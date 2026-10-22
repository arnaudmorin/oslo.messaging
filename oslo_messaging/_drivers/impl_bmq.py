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
import hashlib
import logging as std_logging
import os
import queue
import re
import threading
import time
import uuid

from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils

import oslo_messaging
from oslo_messaging._drivers import base
from oslo_messaging._drivers import bmq_options
from oslo_messaging._drivers import common as rpc_common

import blazingmq


LOG = logging.getLogger(__name__)

# Session events that are part of a normal life cycle, logged at INFO. The
# others (disconnections, failures, ...) are logged at WARNING.
NORMAL_SESSION_EVENTS = (
    'Connected',
    'Reconnected',
    'StateRestored',
    'HostHealthRestored',
    'QueueResumed',
    'QueueReopened',
    'SlowConsumerNormal',
)

DEFAULT_DOMAIN = 'bmq.openstack.priority'
DEFAULT_PORT = 30114
# Maximum delay between two attempts to post a message, in seconds
MAX_RETRY_DELAY = 5

# Queue names are limited by BlazingMQ to 64 characters, among letters,
# digits, '-', '_' and '.' (see bmqt::Uri)
QUEUE_NAME_MAX_LENGTH = 64
INVALID_QUEUE_NAME_CHARS = re.compile(r'[^-a-zA-Z0-9_.]')
# Domain names: same characters, without consecutive dots
VALID_DOMAIN = re.compile(r'^[-a-zA-Z0-9_]+(\.[-a-zA-Z0-9_]+)*$')


def queue_name(name):
    """Return a valid BlazingMQ queue name for name.

    Valid names are kept. The others (too long, or with characters like
    the '@' of the cinder-volume servers) are made valid, and suffixed
    with a hash of the name, so that two names stay two queues.
    """
    if len(
        name
    ) <= QUEUE_NAME_MAX_LENGTH and not INVALID_QUEUE_NAME_CHARS.search(name):
        return name
    digest = hashlib.sha256(name.encode()).hexdigest()[:16]
    prefix = INVALID_QUEUE_NAME_CHARS.sub('_', name)
    prefix = prefix[: QUEUE_NAME_MAX_LENGTH - len(digest) - 1]
    return f"{prefix}_{digest}"


class BmqIncomingMessage(base.RpcIncomingMessage):
    """A message received from BlazingMQ.

    Wraps a deserialized RPC message with the ability to send a reply
    back to the caller. The reply is sent via the driver's _post method,
    which handles queue open/write/close lifecycle. This class does not
    hold a direct reference to the BlazingMQ session.
    """

    def __init__(
        self, ctxt, message, reply_q, msg_id, post, client_timeout=None
    ):
        super().__init__(ctxt, message, msg_id=msg_id)
        self._reply_q = reply_q
        self._post = post
        # The call monitor timeout of the caller: the RPC dispatcher sends
        # heartbeats while the call runs when it is set
        self.client_timeout = client_timeout

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
            # Serialize the exception like the amqp driver does, so the
            # caller can raise it again with its original class
            body['_failure'] = rpc_common.serialize_remote_exception(failure)
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
        """Tell the caller that the call is still running.

        Called by the RPC dispatcher when the caller set a call monitor
        timeout. The heartbeat is sent to the reply queue of the caller,
        like the reply. Failures are raised, the dispatcher then stops
        sending heartbeats.
        """
        if not self._reply_q:
            return
        body = {'_msg_id': self.msg_id, '_heartbeat': True}
        self._post(self._reply_q, jsonutils.dumps(body).encode())


class BmqListener(base.PollStyleListener):
    """Bridges BlazingMQ's callback-based delivery to oslo.messaging's
    poll-based interface.

    The driver's _on_message callback pushes (msg, handle) tuples into
    this listener's queue. poll() blocks on that queue and returns
    BmqIncomingMessage instances to the oslo.messaging server loop.

    The listener does not open or close BlazingMQ queues itself — that
    is done by the driver. It holds a thread-safe queue.Queue that the
    driver populates via _listener_queues, and the URIs of the queues
    opened for it, which it asks the driver to close when it stops.
    """

    def __init__(self, post_fn, close_fn):
        super().__init__()
        # Public so the driver can register it in _listener_queues
        self.queue = queue.Queue()
        # URIs of the BlazingMQ queues opened for this listener
        self.uris = []
        self._post_fn = post_fn
        self._close_fn = close_fn
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
        client_timeout = data.pop('_timeout', None)

        # Confirm delivery to BlazingMQ
        handle.confirm()

        return BmqIncomingMessage(
            ctxt, data, reply_q, msg_id, self._post_fn, client_timeout
        )

    def stop(self):
        """Signal the listener to stop polling and close its queues.

        Puts a sentinel (None, None) into the queue to unblock any
        thread waiting in poll(). Closing the queues stops the delivery
        to this listener: the broker sends the next messages to the
        other consumers, and delivers again the messages received but
        not confirmed yet.
        """
        self._stopped.set()
        # Unblock poll()
        self.queue.put((None, None))
        self._close_queues()

    def cleanup(self):
        """Close the queues of the listener, if stop() did not."""
        self._close_queues()

    def _close_queues(self):
        while self.uris:
            uri = self.uris.pop()
            try:
                self._close_fn(uri, self.queue)
            except Exception:
                LOG.warning('Failed to close queue %s', uri, exc_info=True)


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

    def __init__(
        self, conf, url, default_exchange=None, allowed_remote_exmods=None
    ):
        super().__init__(conf, url, default_exchange, allowed_remote_exmods)

        # Session is created lazily on first use to avoid connecting
        # to the broker at import/construction time. Set it first, so that
        # cleanup() works when __init__ fails.
        self._session = None
        self._session_lock = threading.Lock()
        # The process which created the session, see _get_session()
        self._session_pid = None
        self._inherited_sessions = []

        # NOTE(arnaud) the BlazingMQ SDK logs every queue operation at INFO,
        # which floods the service logs. Keep its warnings only, unless a
        # level was set for it (e.g. default_log_levels = blazingmq=INFO).
        bmq_logger = std_logging.getLogger('blazingmq')
        if bmq_logger.level == std_logging.NOTSET:
            bmq_logger.setLevel(std_logging.WARNING)

        opt_group = cfg.OptGroup(
            name='oslo_messaging_bmq', title='BlazingMQ driver options'
        )
        conf.register_group(opt_group)
        conf.register_opts(bmq_options.BMQ_OPTS, group=opt_group)
        self._ssl_ca_file = conf.oslo_messaging_bmq.ssl_ca_file

        # Parse broker address (and credentials) from URL
        self._credentials = None
        if url.hosts:
            host = url.hosts[0]
            hostname = host.hostname or 'localhost'
            port = host.port or DEFAULT_PORT
            self._broker = f"tcp://{hostname}:{port}"
            if host.username:
                self._credentials = f"{host.username}:{host.password or ''}"
        else:
            self._broker = f"tcp://localhost:{DEFAULT_PORT}"

        # Domain from virtual_host
        if url.virtual_host:
            self._domain = url.virtual_host.strip('/')
        else:
            self._domain = DEFAULT_DOMAIN

        # e.g. bmq.openstack.priority -> bmq.openstack.broadcast
        self._broadcast_domain = self._domain.replace('priority', 'broadcast')
        # NOTE(arnaud) the SDK does not return when a queue is opened with
        # an invalid URI, so refuse an invalid domain here
        for domain in (self._domain, self._broadcast_domain):
            if not VALID_DOMAIN.match(domain):
                raise oslo_messaging.InvalidTransportURL(
                    url, f'Invalid BlazingMQ domain: {domain!r}'
                )

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

        # RPC replies: a single reply queue for the driver, opened on the
        # first call, and the calls waiting for a reply, by message id
        self._reply_uri = None
        self._reply_lock = threading.Lock()
        self._waiters = {}

    def _queue_uri(self, target, suffix=None, fanout=False):
        """Return the URI of the queue of a target.

        The queue name is <exchange>.<topic>[.<suffix>], the suffix being
        the server of an RPC target or the priority of a notification.
        It starts with the exchange, as services use topics with the same
        name in different exchanges (e.g. notifications).
        """
        exchange = target.exchange or self._default_exchange
        name = f"{exchange}.{target.topic}"
        if fanout:
            return f"bmq://{self._broadcast_domain}/{queue_name(name)}"
        if suffix:
            name = f"{name}.{suffix}"
        return f"bmq://{self._domain}/{queue_name(name)}"

    def _get_uri_lock(self, uri):
        """Return a per-URI lock, creating it if needed."""
        with self._uri_locks_lock:
            if uri not in self._uri_locks:
                self._uri_locks[uri] = threading.Lock()
            return self._uri_locks[uri]

    def _security_kwargs(self):
        """Return the blazingmq.Session arguments for authentication and TLS.

        Only set what is configured, so a blazingmq SDK without
        authentication and TLS support still works when they are not used.
        """
        kwargs = {}
        if self._credentials is not None:
            credential = ('BASIC', self._credentials.encode())
            kwargs['authn_credential_provider'] = lambda: credential
        if self._ssl_ca_file:
            kwargs['tls_certificate_authority'] = self._ssl_ca_file
        return kwargs

    def _get_session(self):
        """Return the BlazingMQ session, creating it on first use.

        Uses double-checked locking to ensure the session is created
        exactly once even under concurrent access.
        """
        if self._session is not None and self._session_pid != os.getpid():
            self._reset_after_fork()
        if self._session is None:
            with self._session_lock:
                if self._session is None:
                    try:
                        self._session = blazingmq.Session(
                            self._on_session_event,
                            on_message=self._on_message,
                            broker=self._broker,
                            **self._security_kwargs(),
                        )
                    except Exception as e:
                        # e.g. BrokerTimeoutError, when the broker is not
                        # reachable or refuses the credentials
                        raise oslo_messaging.MessageDeliveryFailure(
                            f"Failed to connect to the BlazingMQ broker "
                            f"{self._broker}: {e}"
                        ) from e
                    self._session_pid = os.getpid()
                    LOG.info(
                        "BlazingMQ session created: broker=%s domain=%s",
                        self._broker,
                        self._domain,
                    )
        return self._session

    def _reset_after_fork(self):
        """Forget the session inherited from the parent process.

        Services fork their workers after the transport was created, and
        possibly used. The threads of the SDK do not exist in the child,
        and its session shares the connection of the parent: drop it, with
        the queues opened by the parent, and create a new one. The session
        is not stopped, nor destroyed (a reference is kept), as this would
        close the connection of the parent.
        """
        with self._session_lock:
            if self._session is None or self._session_pid == os.getpid():
                return
            LOG.info(
                "Process forked after the BlazingMQ session was created, "
                "creating a new session"
            )
            self._inherited_sessions.append(self._session)
            self._session = None
            self._reply_uri = None
            self._queues.clear()
            self._uri_locks.clear()
            with self._listener_lock:
                self._listener_queues.clear()

    def _on_session_event(self, event):
        """Handle BlazingMQ session-level events (connect, disconnect, etc).

        Called by the BlazingMQ SDK on a background thread. Currently
        only logs the event.
        """
        # TODO(arnaud) work on events
        if type(event).__name__ in NORMAL_SESSION_EVENTS:
            LOG.info("BlazingMQ session event: %s", event)
        else:
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
        LOG.debug("Received message on %s", msg.queue_uri)
        if msg.queue_uri == self._reply_uri:
            self._on_reply(msg, msg_handle)
            return
        with self._listener_lock:
            # Dispatch the message to the correct queue
            q = self._listener_queues.get(msg.queue_uri)
        if q is not None:
            q.put((msg, msg_handle))
        else:
            LOG.warning(
                "No listener for queue %s. Message will not be treated.",
                msg.queue_uri,
            )
            msg_handle.confirm()

    def _on_reply(self, msg, msg_handle):
        """Hand an RPC reply to the call waiting for it.

        Called from _on_message, on a thread of the SDK. Replies of calls
        which are not waiting anymore (e.g. timed out) are dropped.
        """
        msg_handle.confirm()
        try:
            data = jsonutils.loads(msg.data)
        except Exception:
            LOG.exception("Failed to deserialize BlazingMQ reply")
            return
        msg_id = data.get('_msg_id')
        waiter = self._waiters.get(msg_id)
        if waiter is None:
            LOG.debug("No call waiting for reply %s, dropping it", msg_id)
            return
        waiter.put(data)

    def _get_reply_uri(self):
        """Return the reply queue of the driver, opening it on first use."""
        if self._reply_uri is None:
            with self._reply_lock:
                if self._reply_uri is None:
                    uri = f"bmq://{self._domain}/reply.{uuid.uuid4().hex}"
                    self._open_queue(uri, read=True)
                    self._reply_uri = uri
        return self._reply_uri

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
                f"A queue must be opened for read or write, not both: {uri}"
            )
        if not read and not write:
            raise ValueError(
                f"A queue must be opened for read or write: {uri}"
            )
        # Check and update tracking under lock, but perform blocking
        # session calls outside the lock to avoid holding it during
        # broker round-trips (which could deadlock if a callback ever
        # needed _queues_lock).
        need_close = False
        with self._queues_lock:
            existing = self._queues.get(uri)
            if (
                existing
                and existing['read'] == read
                and existing['write'] == write
            ):
                return  # already open with exact flags
            if existing:
                need_close = True

        try:
            if need_close:
                self._get_session().close_queue(uri)
            LOG.debug(
                'Opening queue uri=%s read=%s write=%s', uri, read, write
            )
            self._get_session().open_queue(uri, read=read, write=write)
        except blazingmq.Error as e:
            with self._queues_lock:
                self._queues.pop(uri, None)
            raise oslo_messaging.MessageDeliveryFailure(
                f"Failed to open the BlazingMQ queue {uri}: {e}"
            ) from e
        with self._queues_lock:
            self._queues[uri] = {'read': read, 'write': write}

    def _add_listener_queue(self, listener, uri):
        """Open a queue for reading and deliver its messages to listener."""
        self._open_queue(uri, read=True)
        with self._listener_lock:
            self._listener_queues[uri] = listener.queue
        listener.uris.append(uri)

    def _close_listener_queue(self, uri, listener_queue):
        """Close a queue opened by _add_listener_queue.

        The queue is only closed if it still delivers to listener_queue:
        another listener of the driver may have opened it since.
        """
        with self._listener_lock:
            if self._listener_queues.get(uri) is not listener_queue:
                return
        self._close_queue(uri)

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
        try:
            self._get_session().close_queue(uri)
        except (blazingmq.Error, oslo_messaging.MessageDeliveryFailure):
            # The queue is not used anymore, the broker closes it with the
            # session at the latest
            LOG.warning('Failed to close queue %s', uri, exc_info=True)
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
                            uri,
                            payload,
                            on_ack=functools.partial(
                                on_ack, ack_status, ack_event
                            ),
                        )
                    except (
                        blazingmq.Error,
                        oslo_messaging.MessageDeliveryFailure,
                    ) as e:
                        if max_attempts and attempt >= max_attempts:
                            raise oslo_messaging.MessageDeliveryFailure(str(e))
                        LOG.warning(
                            'Failed to post to %s, retrying: %s', uri, e
                        )
                        time.sleep(min(attempt, MAX_RETRY_DELAY))
                        continue

                    # Wait for broker acknowledgment
                    ack_received = ack_event.wait(timeout=timeout)
                    if not ack_received:
                        raise oslo_messaging.MessagingTimeout(
                            f"Timeout waiting for broker ack on {uri}"
                        )

                    if ack_status.get('status') != blazingmq.AckStatus.SUCCESS:
                        if max_attempts and attempt >= max_attempts:
                            raise oslo_messaging.MessageDeliveryFailure(
                                "BlazingMQ post failed with status: {}".format(
                                    ack_status.get('status')
                                )
                            )
                        LOG.warning(
                            'Post to %s failed with status %s, retrying',
                            uri,
                            ack_status.get('status'),
                        )
                        time.sleep(min(attempt, MAX_RETRY_DELAY))
                        continue

                    # Message delivered successfully
                    break
            finally:
                # Always restore the queue's previous flags
                if previous:
                    try:
                        self._open_queue(uri, **previous)
                    except oslo_messaging.MessageDeliveryFailure:
                        LOG.warning(
                            'Failed to reopen queue %s', uri, exc_info=True
                        )
                else:
                    self._close_queue(uri)

    def _context_packing(self, context):
        """Serialize an oslo.messaging context to a dict for JSON transport."""
        if hasattr(context, 'to_dict'):
            return context.to_dict()
        return context

    def _send(
        self,
        target,
        ctxt,
        message,
        wait_for_reply=None,
        timeout=None,
        call_monitor_timeout=None,
        retry=None,
    ):
        """Core send implementation for both RPC calls and casts.

        For casts (wait_for_reply=False): posts the message and returns.
        For calls (wait_for_reply=True): posts the message with the reply
        queue of the driver and the id of the call, then blocks until
        the reply arrives or timeout expires. With a call monitor timeout,
        the server sends heartbeats while the call runs, and the call
        fails if they stop for longer than call_monitor_timeout.
        """
        # Determine destination queue URI
        if target.fanout:
            uri = self._queue_uri(target, fanout=True)
        else:
            uri = self._queue_uri(target, target.server)

        # Pack context
        if ctxt:
            message['_context'] = self._context_packing(ctxt)

        if not wait_for_reply:
            payload = jsonutils.dumps(message).encode()
            self._post(uri, payload, retry=retry, timeout=timeout)
            return None

        # The timeout covers the whole call: the broker ack of the request,
        # then the reply
        timer = rpc_common.DecayingTimer(duration=timeout)
        timer.start()

        def timed_out():
            raise oslo_messaging.MessagingTimeout(
                f"Timeout waiting for reply on {target.topic}"
            )

        # The reply is sent to the reply queue of the driver, with the id
        # of the call, which routes it to this waiter
        msg_id = uuid.uuid4().hex
        message['_msg_id'] = msg_id
        message['_reply_q'] = self._get_reply_uri()
        if call_monitor_timeout:
            message['_timeout'] = call_monitor_timeout
            cm_timer = rpc_common.DecayingTimer(duration=call_monitor_timeout)
            cm_timer.start()
        else:
            cm_timer = None
        waiter = queue.Queue()
        self._waiters[msg_id] = waiter
        try:
            payload = jsonutils.dumps(message).encode()
            self._post(uri, payload, retry=retry, timeout=timer.check_return())
            while True:
                left = timer.check_return(timed_out)
                if cm_timer:
                    left = cm_timer.check_return(timed_out, maximum=left)
                try:
                    data = waiter.get(timeout=left)
                except queue.Empty:
                    timed_out()
                if not data.get('_heartbeat'):
                    break
                LOG.debug(
                    'Call monitor heartbeat received; renewing timeout timer'
                )
                if cm_timer:
                    cm_timer.restart()
        finally:
            del self._waiters[msg_id]

        if data.get('_failure'):
            # NOTE(arnaud) callers rely on the original exception class
            # (e.g. nova-compute catches ComputeHostNotFound on startup),
            # so do not raise a plain RemoteError
            raise rpc_common.deserialize_remote_exception(
                data['_failure'], self._allowed_remote_exmods
            )

        return data.get('_reply')

    def send(
        self,
        target,
        ctxt,
        message,
        wait_for_reply=None,
        timeout=None,
        call_monitor_timeout=None,
        retry=None,
        transport_options=None,
    ):
        """Send an RPC message (call or cast) to a target.

        This is the public oslo.messaging interface. Delegates to _send
        after logging.
        """
        LOG.debug(
            'Sending message target=%s method=%s wait_for_reply=%s',
            target,
            message.get('method'),
            wait_for_reply,
        )
        return self._send(
            target,
            ctxt,
            message,
            wait_for_reply,
            timeout,
            call_monitor_timeout,
            retry,
        )

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
        listener = BmqListener(self._post, self._close_listener_queue)

        # topic
        uri = self._queue_uri(target)
        self._add_listener_queue(listener, uri)
        LOG.info("Starting listener on %s", uri)

        # topic.server
        if target.server:
            uri = self._queue_uri(target, target.server)
            self._add_listener_queue(listener, uri)
            LOG.info("Starting listener on %s", uri)

        # fanout
        uri = self._queue_uri(target, fanout=True)
        self._add_listener_queue(listener, uri)
        LOG.info("Starting listener on %s", uri)

        return base.PollStyleListenerAdapter(
            listener, batch_size, batch_timeout
        )

    def listen_for_notifications(
        self, targets_and_priorities, pool, batch_size, batch_timeout
    ):
        """Create a listener for notification messages.

        Opens one BlazingMQ queue per (target, priority) pair. For example,
        notifications.info and notifications.error would each get their
        own queue. All queues feed into a single BmqListener.

        Notification pools are not supported by the BlazingMQ driver.
        """
        if pool is not None:
            raise NotImplementedError(
                "Notification pools are not supported by the BlazingMQ driver"
            )

        listener = BmqListener(self._post, self._close_listener_queue)

        for target, priority in targets_and_priorities:
            uri = self._queue_uri(target, priority)
            self._add_listener_queue(listener, uri)
            LOG.info("Starting listener for notifications on %s", uri)

        return base.PollStyleListenerAdapter(
            listener, batch_size, batch_timeout
        )

    def cleanup(self):
        """Tear down the driver and release all resources.

        Stops the BlazingMQ session (which closes all queues and
        disconnects from the broker) and clears internal tracking state.
        """
        if self._session is not None and self._session_pid != os.getpid():
            # the session of the parent process, see _reset_after_fork()
            self._reset_after_fork()
        if self._session is None:
            return

        # session.stop() closes all queues and disconnects
        try:
            self._session.stop()
        except Exception:
            LOG.debug('Failed to stop the BlazingMQ session', exc_info=True)
        self._session = None
        self._reply_uri = None
        self._queues.clear()
        self._listener_queues.clear()
        self._uri_locks.clear()

    def __del__(self):
        self.cleanup()
