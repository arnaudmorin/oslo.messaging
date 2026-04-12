# Copyright 2025 OpenStack Foundation
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import functools
import queue
import threading
import uuid

from oslo_log import log as logging
from oslo_serialization import jsonutils

import oslo_messaging
from oslo_messaging._drivers import base

import blazingmq


LOG = logging.getLogger(__name__)

DEFAULT_DOMAIN = 'bmq.openstack.priority'
DEFAULT_PORT = 30114


class BmqIncomingMessage(base.RpcIncomingMessage):
    """A message received from BlazingMQ.

    Wraps a deserialized RPC message with the ability to send a reply
    back to the caller. The reply is sent via the driver's _post method,
    which handles queue open/write/close lifecycle. This class does not
    hold a direct reference to the BlazingMQ session.
    """

    def __init__(self, ctxt, message, reply_q, msg_id, post):
        super().__init__(ctxt, message, msg_id=msg_id)
        self._reply_q = reply_q
        self._post = post

    def reply(self, reply=None, failure=None):
        """Send an RPC reply back to the caller.

        Serializes the reply (or failure) as JSON and posts it to the
        caller's per-call reply queue. Does nothing if the original
        message had no reply queue (i.e. it was a cast, not a call).
        """
        if not self._reply_q:
            return
        body = {
            '_msg_id': self.msg_id,
            '_reply': reply,
            '_failure': None,
        }
        if failure:
            body['_failure'] = (
                failure[0].__name__ if hasattr(failure[0], '__name__')
                else str(failure[0]),
                str(failure[1]),
            )
        payload = jsonutils.dumps(body).encode()
        try:
            # Send the reply message
            # the _post method is in charge of opening/writing/closing
            # the queue
            self._post(self._reply_q, payload)
        except Exception:
            LOG.exception("Failed to send reply to %s", self._reply_q)

    def requeue(self):
        """Requeue is not supported by BlazingMQ.

        BlazingMQ handles redelivery internally via its own retry
        mechanisms. This method is a no-op.
        """
        LOG.warning("Requeue is not supported by the BlazingMQ driver")

    def heartbeat(self):
        """No-op heartbeat. BlazingMQ manages connection health internally."""
        pass


class BmqListener(base.PollStyleListener):
    """Bridges BlazingMQ's callback-based delivery to oslo.messaging's
    poll-based interface.

    The driver's _on_message callback pushes (msg, handle) tuples into
    this listener's queue. poll() blocks on that queue and returns
    BmqIncomingMessage instances to the oslo.messaging server loop.

    The listener does not open or close BlazingMQ queues itself — that
    is done by the driver. It only holds a reference to a thread-safe
    queue.Queue that the driver populates via _listener_queues.
    """

    def __init__(self, post_fn):
        super().__init__()
        # Public so the driver can register it in _listener_queues
        self.queue = queue.Queue()
        self._post_fn = post_fn
        self._stopped = threading.Event()

    @base.batch_poll_helper
    def poll(self, timeout=None):
        """Block until a message arrives or timeout expires.

        Deserializes the JSON payload, extracts oslo.messaging envelope
        fields (_context, _reply_q, _msg_id), confirms delivery to
        BlazingMQ, and returns a BmqIncomingMessage.
        """
        if self._stopped.is_set():
            return None
        try:
            # This is a blocking call
            msg, handle = self.queue.get(timeout=timeout)
        except queue.Empty:
            return None

        if msg is None and handle is None:
            # Sentinel value from stop()
            return None

        try:
            data = jsonutils.loads(msg.data)
        except Exception:
            LOG.exception("Failed to deserialize BlazingMQ message")
            handle.confirm()
            return None

        ctxt = data.pop('_context', None)
        reply_q = data.pop('_reply_q', None)
        msg_id = data.pop('_msg_id', None)

        # Confirm delivery to BlazingMQ
        handle.confirm()

        return BmqIncomingMessage(
            ctxt, data, reply_q, msg_id, self._post_fn)

    def stop(self):
        """Signal the listener to stop polling.

        Puts a sentinel (None, None) into the queue to unblock any
        thread waiting in poll().
        """
        self._stopped.set()
        # Unblock poll()
        self.queue.put((None, None))

    def cleanup(self):
        """No-op. Queue cleanup is handled by the driver."""
        pass


class BmqDriver(base.BaseDriver):
    """BlazingMQ driver for oslo.messaging.

    Uses BlazingMQ work queues for RPC (round-robin to one consumer) and
    broadcast queues for fanout delivery (all consumers get a copy).

    All BlazingMQ session interactions (open, close, post) are centralized
    in this driver. Listeners and incoming messages interact with the
    broker only through callbacks (_post, _open_queue, _close_queue)
    provided by the driver.

    Transport URL format:
        bmq://host:port/domain_name

    Examples:
        bmq://localhost:30114/bmq.openstack.priority
        bmq://broker.example.com/bmq.prod.priority
    """

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=None):
        super().__init__(conf, url, default_exchange,
                         allowed_remote_exmods)

        # Parse broker address from URL
        if url.hosts:
            host = url.hosts[0]
            hostname = host.hostname or 'localhost'
            port = host.port or DEFAULT_PORT
            self._broker = f"tcp://{hostname}:{port}"
        else:
            self._broker = f"tcp://localhost:{DEFAULT_PORT}"

        # Domain from virtual_host
        if url.virtual_host:
            self._domain = url.virtual_host.strip('/')
        else:
            self._domain = DEFAULT_DOMAIN

        # e.g. bmq.openstack.priority -> bmq.openstack.broadcast
        self._broadcast_domain = self._domain.replace('priority', 'broadcast')

        # Maps queue URIs to Python queue.Queue objects. When _on_message
        # receives a BlazingMQ message, it looks up the URI here to find
        # which listener should receive it.
        self._listener_queues = {}
        self._listener_lock = threading.Lock()

        # Tracks which queues are currently open on the BlazingMQ session
        # and with which flags: uri -> {'read': bool, 'write': bool}.
        # Used to avoid duplicate open_queue calls (which would cause
        # BlazingMQ to hang) and to restore flags after a temporary
        # write-only switch during _post/_send.
        self._queues = {}
        # Note: _queues_lock is technically redundant with _uri_locks
        # (which serialize same-URI operations) and CPython's GIL (which
        # makes dict mutations atomic for different URIs). We keep it as
        # a safety net for non-CPython implementations where the GIL may
        # not exist (e.g. PyPy STM, future free-threaded CPython).
        self._queues_lock = threading.Lock()

        # Per-URI locks to serialize flag-switching operations (_post/_send)
        # on the same queue. Without this, two threads sending to the same
        # queue could step on each other's read/write flag transitions.
        self._uri_locks = {}
        self._uri_locks_lock = threading.Lock()

        # Session is created lazily on first use to avoid connecting
        # to the broker at import/construction time.
        self._session = None
        self._session_lock = threading.Lock()

    def _get_uri_lock(self, uri):
        """Return a per-URI lock, creating it if needed."""
        with self._uri_locks_lock:
            if uri not in self._uri_locks:
                self._uri_locks[uri] = threading.Lock()
            return self._uri_locks[uri]

    def _get_session(self):
        """Return the BlazingMQ session, creating it on first use.

        Uses double-checked locking to ensure the session is created
        exactly once even under concurrent access.
        """
        if self._session is None:
            with self._session_lock:
                if self._session is None:
                    self._session = blazingmq.Session(
                        self._on_session_event,
                        on_message=self._on_message,
                        broker=self._broker,
                    )
                    LOG.info("BlazingMQ session created: broker=%s domain=%s",
                             self._broker, self._domain)
        return self._session

    def _on_session_event(self, event):
        """Handle BlazingMQ session-level events (connect, disconnect, etc).

        Called by the BlazingMQ SDK on a background thread. Currently
        only logs the event.
        """
        # TODO work on events
        LOG.warning("BlazingMQ session event: %s", event)

    def _on_message(self, msg, msg_handle):
        """Central message dispatcher, called by BlazingMQ on a background
        thread when a message arrives on any open queue.

        Looks up the queue URI in _listener_queues and pushes the message
        to the corresponding listener. If no listener is registered
        (e.g. a queue was closed between delivery and dispatch), confirms
        the message to avoid it being redelivered forever.

        IMPORTANT: Only safe operations are performed here — no session
        method calls except confirm(), to avoid BlazingMQ deadlocks.
        """
        LOG.info("Received message on %s", msg.queue_uri)
        with self._listener_lock:
            # Dispatch the message to the correct queue
            q = self._listener_queues.get(msg.queue_uri)
        if q is not None:
            q.put((msg, msg_handle))
        else:
            LOG.warning(
                "No listener for queue %s. Message will not be treated.",
                msg.queue_uri)
            msg_handle.confirm()

    def _open_queue(self, uri, read=False, write=False):
        """Open or reopen a queue with the requested flags.

        A queue must be opened for read OR write, never both. Opening
        for read+write would cause the sender to receive its own messages.

        BlazingMQ does not allow calling open_queue twice on the same URI.
        If the queue is already open with different flags, we close it
        first and reopen with the new flags. If it's already open with
        the exact same flags, this is a no-op.
        """
        if read and write:
            raise ValueError(
                "A queue must be opened for read or write, not both: %s"
                % uri)
        if not read and not write:
            raise ValueError(
                "A queue must be opened for read or write: %s" % uri)
        # Check and update tracking under lock, but perform blocking
        # session calls outside the lock to avoid holding it during
        # broker round-trips (which could deadlock if a callback ever
        # needed _queues_lock).
        need_close = False
        with self._queues_lock:
            existing = self._queues.get(uri)
            if existing and existing['read'] == read and existing['write'] == write:
                return  # already open with exact flags
            if existing:
                need_close = True

        if need_close:
            self._get_session().close_queue(uri)
        LOG.debug('Opening queue uri=%s read=%s write=%s', uri, read, write)
        self._get_session().open_queue(uri, read=read, write=write)
        with self._queues_lock:
            self._queues[uri] = {'read': read, 'write': write}

    def _close_queue(self, uri):
        """Close a queue and remove it from all tracking.

        Also removes the URI from _listener_queues so that any messages
        arriving after close are confirmed and discarded by _on_message
        rather than being pushed to a dead listener.
        """
        if uri is None:
            return
        with self._listener_lock:
            self._listener_queues.pop(uri, None)
        with self._queues_lock:
            if uri not in self._queues:
                return
            del self._queues[uri]
        LOG.debug('Closing queue uri=%s', uri)
        self._get_session().close_queue(uri)
        # Clean up the per-URI lock since the queue is gone
        with self._uri_locks_lock:
            self._uri_locks.pop(uri, None)

    def _post(self, uri, payload, retry=None, timeout=None):
        """Post a message to a queue with retry and ACK support.

        Handles the full write lifecycle:
          1. Saves the current queue flags (if any)
          2. Switches the queue to write-only (to avoid receiving our
             own message if we are also listening on this queue)
          3. Posts the message and waits for broker acknowledgment
          4. Retries on failure according to the retry parameter
          5. Restores the previous queue flags (or closes if it was
             not open before)

        Args:
            uri: BlazingMQ queue URI to post to.
            payload: encoded message bytes.
            retry: None/-1 for unlimited, 0 for no retry, N for N+1 attempts.
            timeout: seconds to wait for broker ACK. None means no timeout.

        Raises:
            MessageDeliveryFailure: if all retry attempts are exhausted.
            MessagingTimeout: if the broker ACK times out.
        """
        max_attempts = 1
        if retry is None or retry == -1:
            max_attempts = 0  # unlimited
        elif retry > 0:
            max_attempts = retry + 1

        with self._get_uri_lock(uri):
            with self._queues_lock:
                existing = self._queues.get(uri)
                # Defensive copy: _open_queue replaces the dict in
                # self._queues, so a reference would still work today,
                # but a copy protects against future in-place mutations.
                previous = existing.copy() if existing else None

            attempt = 0
            try:
                while True:
                    attempt += 1
                    ack_event = threading.Event()
                    ack_status = {}

                    def on_ack(status_dict, event, ack):
                        status_dict['status'] = ack.status
                        event.set()

                    try:
                        self._open_queue(uri, write=True)
                        self._get_session().post(
                            uri, payload,
                            on_ack=functools.partial(
                                on_ack, ack_status, ack_event))
                    except blazingmq.Error as e:
                        if max_attempts and attempt >= max_attempts:
                            raise oslo_messaging.MessageDeliveryFailure(
                                str(e))
                        continue

                    # Wait for broker acknowledgment
                    ack_received = ack_event.wait(timeout=timeout)
                    if not ack_received:
                        raise oslo_messaging.MessagingTimeout(
                            "Timeout waiting for broker ack on %s" % uri)

                    if ack_status.get('status') != \
                            blazingmq.AckStatus.SUCCESS:
                        if max_attempts and attempt >= max_attempts:
                            raise oslo_messaging.MessageDeliveryFailure(
                                "BlazingMQ post failed with status: %s"
                                % ack_status.get('status'))
                        continue

                    # Message delivered successfully
                    break
            finally:
                # Always restore the queue's previous flags
                if previous:
                    self._open_queue(uri, **previous)
                else:
                    self._close_queue(uri)

    def _context_packing(self, context):
        """Serialize an oslo.messaging context to a dict for JSON transport."""
        if hasattr(context, 'to_dict'):
            return context.to_dict()
        return context

    def _send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
              retry=None):
        """Core send implementation for both RPC calls and casts.

        For casts (wait_for_reply=False): posts the message and returns.
        For calls (wait_for_reply=True): creates a temporary reply queue,
        posts the message with the reply URI embedded, then blocks until
        a reply arrives or timeout expires.
        """
        # Determine destination queue URI
        if target.fanout:
            uri = f"bmq://{self._broadcast_domain}/{target.topic}"
        elif target.server is not None:
            uri = f"bmq://{self._domain}/{target.topic}.{target.server}"
        else:
            uri = f"bmq://{self._domain}/{target.topic}"

        # Pack context
        if ctxt:
            message['_context'] = self._context_packing(ctxt)

        # Set up reply queue
        reply_uri = None
        reply_q = None
        if wait_for_reply:
            reply_uri = f"bmq://{self._domain}/reply.{uuid.uuid4().hex}"
            reply_q = queue.Queue()
            self._open_queue(reply_uri, read=True)
            with self._listener_lock:
                self._listener_queues[reply_uri] = reply_q
            message['_reply_q'] = reply_uri

        payload = jsonutils.dumps(message).encode()

        try:
            self._post(uri, payload, retry=retry, timeout=timeout)
        except Exception:
            self._close_queue(reply_uri)
            raise

        if not wait_for_reply:
            return None

        # Wait for reply on per-call queue
        try:
            msg, handle = reply_q.get(timeout=timeout)
        except queue.Empty:
            self._close_queue(reply_uri)
            raise oslo_messaging.MessagingTimeout(
                "Timeout waiting for reply on %s" % target.topic)

        handle.confirm()
        self._close_queue(reply_uri)

        try:
            data = jsonutils.loads(msg.data)
        except Exception:
            LOG.exception("Failed to deserialize reply")
            return None

        if data.get('_failure'):
            raise oslo_messaging.RemoteError(
                data['_failure'][0], data['_failure'][1])

        return data.get('_reply')

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             call_monitor_timeout=None, retry=None, transport_options=None):
        """Send an RPC message (call or cast) to a target.

        This is the public oslo.messaging interface. Delegates to _send
        after logging.
        """
        LOG.debug('Sending message target=%s method=%s wait_for_reply=%s',
                  target, message.get('method'), wait_for_reply)
        return self._send(target, ctxt, message, wait_for_reply, timeout,
                          retry)

    def send_notification(self, target, ctxt, message, version, retry=None):
        """Send a notification message (fire-and-forget, no reply)."""
        self._send(target, ctxt, message, retry=retry)

    def listen(self, target, batch_size, batch_timeout):
        """Create a listener for RPC messages on the given target.

        Opens up to three BlazingMQ queues and wires them to a single
        BmqListener:
          - topic queue: receives round-robin RPC messages for this topic
          - topic.server queue: receives messages targeted at this specific
            server (only if target.server is set)
          - broadcast queue: receives fanout messages sent to all servers
            listening on this topic (uses the broadcast domain)
        """
        listener = BmqListener(self._post)

        # topic
        uri = f"bmq://{self._domain}/{target.topic}"
        self._open_queue(uri, read=True)
        with self._listener_lock:
            self._listener_queues[uri] = listener.queue
        LOG.info("Starting listener on %s", uri)

        # topic.server
        if target.server:
            uri = f"bmq://{self._domain}/{target.topic}.{target.server}"
            self._open_queue(uri, read=True)
            with self._listener_lock:
                self._listener_queues[uri] = listener.queue
            LOG.info("Starting listener on %s", uri)

        # fanout
        uri = f"bmq://{self._broadcast_domain}/{target.topic}"
        self._open_queue(uri, read=True)
        with self._listener_lock:
            self._listener_queues[uri] = listener.queue
        LOG.info("Starting listener on %s", uri)

        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        """Create a listener for notification messages.

        Opens one BlazingMQ queue per (target, priority) pair. For example,
        notifications.info and notifications.error would each get their
        own queue. All queues feed into a single BmqListener.

        Notification pools are not supported by the BlazingMQ driver.
        """
        if pool is not None:
            raise NotImplementedError(
                "Notification pools are not supported by the BlazingMQ driver")

        listener = BmqListener(self._post)

        for target, priority in targets_and_priorities:
            uri = f"bmq://{self._domain}/{target.topic}.{priority}"
            self._open_queue(uri, read=True)
            with self._listener_lock:
                self._listener_queues[uri] = listener.queue
            LOG.info("Starting listener for notifications on %s", uri)

        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def cleanup(self):
        """Tear down the driver and release all resources.

        Stops the BlazingMQ session (which closes all queues and
        disconnects from the broker) and clears internal tracking state.
        """
        if self._session is None:
            return

        # session.stop() closes all queues and disconnects
        try:
            self._session.stop()
        except Exception:
            pass
        self._session = None
        self._queues.clear()
        self._listener_queues.clear()
        self._uri_locks.clear()

    def __del__(self):
        self.cleanup()
