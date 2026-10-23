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

import logging
import os
import queue
import sys
import threading
import time
import types
from unittest import mock

import fixtures
from oslo_serialization import jsonutils

import oslo_messaging
from oslo_messaging._drivers import bmq_options
from oslo_messaging.tests import utils as test_utils

# blazingmq is a C extension that may not be installed in the test
# environment. Insert a fake module before importing the driver, only when
# it is missing: the tests patch it anyway, and replacing the real one would
# break the other tests of the process (e.g. the functional tests).
try:
    import blazingmq  # noqa: F401
except ImportError:
    _fake_bmq = types.ModuleType('blazingmq')
    _fake_bmq.Session = None
    _fake_bmq.Error = Exception
    _fake_bmq.AckStatus = types.SimpleNamespace(SUCCESS='SUCCESS')
    sys.modules['blazingmq'] = _fake_bmq

from oslo_messaging._drivers import impl_bmq  # noqa: E402


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeMessage:
    """Mimics a blazingmq.Message."""

    def __init__(self, data, queue_uri='bmq://d/q'):
        self.data = data
        self.queue_uri = queue_uri


class FakeHandle:
    """Mimics a blazingmq.MessageHandle."""

    def __init__(self):
        self.confirmed = False

    def confirm(self):
        self.confirmed = True


class FakeAck:
    def __init__(self, status):
        self.status = status


class FakeSession:
    """Minimal stand-in for blazingmq.Session."""

    def __init__(self, on_session_event=None, on_message=None, broker=None):
        self._on_message = on_message
        self.opened = []  # (uri, read, write)
        self.closed = []
        self.posted = []
        self.stopped = False

    def open_queue(self, uri, read=False, write=False, **kw):
        self.opened.append((uri, read, write))

    def close_queue(self, uri, **kw):
        self.closed.append(uri)

    def post(self, uri, payload, on_ack=None, **kw):
        self.posted.append((uri, payload, on_ack))
        if on_ack:
            on_ack(FakeAck(status='SUCCESS'))

    def stop(self):
        self.stopped = True


class BmqTestBase(test_utils.BaseTestCase):
    """Patches blazingmq so no real broker is needed."""

    def setUp(self):
        super().setUp()
        patcher = mock.patch('oslo_messaging._drivers.impl_bmq.blazingmq')
        self.mock_bmq = patcher.start()
        self.mock_bmq.Session = FakeSession
        self.mock_bmq.Error = Exception
        self.mock_bmq.AckStatus.SUCCESS = 'SUCCESS'
        self.addCleanup(patcher.stop)
        # the driver registers its options, register them before, so the
        # tests can set them
        self.conf.register_opts(
            bmq_options.BMQ_OPTS, group='oslo_messaging_bmq'
        )

    def _get_driver(self, url='bmq://host:30114/bmq.test.priority'):
        transport = oslo_messaging.get_rpc_transport(self.conf, url=url)
        self.addCleanup(transport.cleanup)
        return transport._driver


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------


class TestTransportURL(BmqTestBase):
    def _check(self, url, broker, domain, bcast):
        driver = self._get_driver(url=url)
        self.assertEqual(broker, driver._broker)
        self.assertEqual(domain, driver._domain)
        self.assertEqual(bcast, driver._broadcast_domain)

    def test_full_url(self):
        self._check(
            'bmq://broker:5555/bmq.prod.priority',
            'tcp://broker:5555',
            'bmq.prod.priority',
            'bmq.prod.broadcast',
        )

    def test_default_port(self):
        self._check(
            'bmq://myhost/bmq.test.priority',
            'tcp://myhost:30114',
            'bmq.test.priority',
            'bmq.test.broadcast',
        )

    def test_invalid_pool_domain(self):
        self.config(pool_domain='bmq..fanout', group='oslo_messaging_bmq')
        self.assertRaises(
            oslo_messaging.InvalidTransportURL,
            self._get_driver,
            'bmq://host:30114/bmq.test.priority',
        )

    def test_invalid_domain(self):
        self.assertRaises(
            oslo_messaging.InvalidTransportURL,
            self._get_driver,
            'bmq://host:30114/bmq..priority',
        )

    def test_all_defaults(self):
        self._check(
            'bmq://',
            'tcp://localhost:30114',
            'bmq.openstack.priority',
            'bmq.openstack.broadcast',
        )


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


class TestQueueName(test_utils.BaseTestCase):
    def test_valid_name_kept(self):
        self.assertEqual(
            'nova.compute.host-1', impl_bmq.queue_name('nova.compute.host-1')
        )

    def test_invalid_chars(self):
        name = impl_bmq.queue_name('cinder.cinder-volume.host@lvm')
        self.assertRegex(
            name, r'^cinder\.cinder-volume\.host_lvm_[0-9a-f]{16}$'
        )
        # not the queue of another name with the same valid form
        self.assertNotEqual(
            name, impl_bmq.queue_name('cinder.cinder-volume.host#lvm')
        )

    def test_too_long(self):
        long_name = 'neutron.q-agent-notifier-port-update.' + 'c' * 40
        name = impl_bmq.queue_name(long_name)
        self.assertEqual(64, len(name))
        self.assertTrue(name.startswith('neutron.q-agent-notifier'))
        self.assertNotEqual(name, impl_bmq.queue_name(long_name + 'x'))

    def test_max_length_kept(self):
        name = 'a' * 64
        self.assertEqual(name, impl_bmq.queue_name(name))


class TestSession(BmqTestBase):
    def test_no_session_at_init(self):
        driver = self._get_driver()
        self.assertIsNone(driver._session)

    def test_session_created_once(self):
        driver = self._get_driver()
        s1 = driver._get_session()
        s2 = driver._get_session()
        self.assertIs(s1, s2)


# ---------------------------------------------------------------------------
# _open_queue / _close_queue
# ---------------------------------------------------------------------------


class TestFork(BmqTestBase):
    def test_new_session_after_fork(self):
        driver = self._get_driver()
        parent = driver._get_session()
        driver._open_queue('bmq://d/q', read=True)
        driver._get_reply_uri()

        with mock.patch('os.getpid', return_value=os.getpid() + 1):
            child = driver._get_session()

        self.assertIsNot(parent, child)
        # the session of the parent is kept as is, its queues are forgotten
        self.assertFalse(parent.stopped)
        self.assertIn(parent, driver._inherited_sessions)
        self.assertEqual({}, driver._queues)
        self.assertIsNone(driver._reply_uri)

    def test_cleanup_after_fork_keeps_parent_session(self):
        driver = self._get_driver()
        parent = driver._get_session()

        with mock.patch('os.getpid', return_value=os.getpid() + 1):
            driver.cleanup()

        self.assertFalse(parent.stopped)
        self.assertIsNone(driver._session)

    def test_same_session_without_fork(self):
        driver = self._get_driver()
        self.assertIs(driver._get_session(), driver._get_session())
        self.assertEqual([], driver._inherited_sessions)


class TestOpenCloseQueue(BmqTestBase):
    def test_open_read(self):
        driver = self._get_driver()
        driver._open_queue('bmq://d/q', read=True)
        self.assertIn('bmq://d/q', driver._queues)
        self.assertTrue(driver._queues['bmq://d/q']['read'])

    def test_open_write(self):
        driver = self._get_driver()
        driver._open_queue('bmq://d/q', write=True)
        self.assertTrue(driver._queues['bmq://d/q']['write'])

    def test_open_idempotent(self):
        driver = self._get_driver()
        driver._open_queue('bmq://d/q', read=True)
        driver._open_queue('bmq://d/q', read=True)
        # Only one open_queue call to the session
        session = driver._get_session()
        read_opens = [o for o in session.opened if o[0] == 'bmq://d/q']
        self.assertEqual(1, len(read_opens))

    def test_open_rejects_both_flags(self):
        driver = self._get_driver()
        self.assertRaises(
            ValueError, driver._open_queue, 'bmq://d/q', read=True, write=True
        )

    def test_open_rejects_no_flags(self):
        driver = self._get_driver()
        self.assertRaises(ValueError, driver._open_queue, 'bmq://d/q')

    def test_close_removes_tracking(self):
        driver = self._get_driver()
        driver._open_queue('bmq://d/q', read=True)
        driver._close_queue('bmq://d/q')
        self.assertNotIn('bmq://d/q', driver._queues)

    def test_close_none_is_noop(self):
        driver = self._get_driver()
        driver._close_queue(None)  # should not raise

    def test_reopen_with_different_flags(self):
        driver = self._get_driver()
        driver._open_queue('bmq://d/q', read=True)
        driver._open_queue('bmq://d/q', write=True)
        # Should have closed then reopened
        session = driver._get_session()
        self.assertIn('bmq://d/q', session.closed)
        self.assertTrue(driver._queues['bmq://d/q']['write'])


# ---------------------------------------------------------------------------
# _on_message
# ---------------------------------------------------------------------------


class TestOnMessage(BmqTestBase):
    def test_dispatches_to_listener(self):
        driver = self._get_driver()
        q = queue.Queue()
        driver._listener_queues['bmq://d/q'] = q

        msg = FakeMessage(b'{}', queue_uri='bmq://d/q')
        handle = FakeHandle()
        driver._on_message(msg, handle)

        self.assertFalse(q.empty())
        self.assertFalse(handle.confirmed)

    def test_confirms_when_no_listener(self):
        driver = self._get_driver()
        msg = FakeMessage(b'{}', queue_uri='bmq://d/unknown')
        handle = FakeHandle()
        driver._on_message(msg, handle)
        self.assertTrue(handle.confirmed)


# ---------------------------------------------------------------------------
# BmqListener
# ---------------------------------------------------------------------------


class TestBmqListener(BmqTestBase):
    def test_poll_returns_message(self):
        listener = impl_bmq.BmqListener(
            post_fn=mock.Mock(), close_fn=mock.Mock()
        )
        payload = jsonutils.dumps(
            {
                '_context': {'user': 'admin'},
                '_reply_q': 'bmq://d/reply.abc',
                '_msg_id': 'id-1',
                'method': 'test',
            }
        ).encode()
        handle = FakeHandle()
        listener.queue.put((FakeMessage(payload), handle))

        results = listener.poll(timeout=1)
        self.assertEqual(1, len(results))
        result = results[0]
        self.assertIsInstance(result, impl_bmq.BmqIncomingMessage)
        self.assertEqual({'user': 'admin'}, result.ctxt)
        self.assertEqual({'method': 'test'}, result.message)
        # confirmed when the server acknowledges it
        self.assertFalse(handle.confirmed)
        result.acknowledge()
        self.assertTrue(handle.confirmed)

    def test_poll_timeout(self):
        listener = impl_bmq.BmqListener(
            post_fn=mock.Mock(), close_fn=mock.Mock()
        )
        results = listener.poll(timeout=0.01)
        self.assertEqual([], results)

    def test_stop_unblocks_poll(self):
        listener = impl_bmq.BmqListener(
            post_fn=mock.Mock(), close_fn=mock.Mock()
        )
        listener.stop()
        results = listener.poll(timeout=1)
        self.assertEqual([], results)

    def test_bad_json_skipped(self):
        listener = impl_bmq.BmqListener(
            post_fn=mock.Mock(), close_fn=mock.Mock()
        )
        handle = FakeHandle()
        listener.queue.put((FakeMessage(b'not json'), handle))
        results = listener.poll(timeout=1)
        self.assertEqual([], results)
        self.assertTrue(handle.confirmed)


# ---------------------------------------------------------------------------
# BmqIncomingMessage
# ---------------------------------------------------------------------------


class TestBmqIncomingMessage(BmqTestBase):
    def test_reply_posts_to_reply_queue(self):
        post_fn = mock.Mock()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={},
            message={},
            reply_q='bmq://d/reply.xyz',
            msg_id='msg-1',
            post=post_fn,
        )

        msg.reply(reply={'value': 42})

        post_fn.assert_called_once()
        uri, payload = post_fn.call_args[0]
        self.assertEqual('bmq://d/reply.xyz', uri)
        body = jsonutils.loads(payload)
        self.assertEqual({'value': 42}, body['_reply'])
        self.assertIsNone(body['_failure'])

    def test_reply_with_failure(self):
        post_fn = mock.Mock()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={},
            message={},
            reply_q='bmq://d/reply.abc',
            msg_id='msg-2',
            post=post_fn,
        )

        msg.reply(failure=(ValueError, ValueError('bad'), None))

        body = jsonutils.loads(post_fn.call_args[0][1])
        failure = jsonutils.loads(body['_failure'])
        self.assertEqual('ValueError', failure['class'])
        self.assertEqual('builtins', failure['module'])
        self.assertEqual('bad', failure['message'])

    def test_reply_no_reply_q_is_noop(self):
        post_fn = mock.Mock()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={}, message={}, reply_q=None, msg_id='msg-3', post=post_fn
        )
        msg.reply(reply={'v': 1})
        post_fn.assert_not_called()

    def test_heartbeat_posts_to_reply_queue(self):
        post = mock.Mock()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={},
            message={},
            reply_q='bmq://d/reply.xyz',
            msg_id='msg-1',
            post=post,
            client_timeout=60,
        )
        msg.heartbeat()

        uri, payload = post.call_args[0]
        self.assertEqual('bmq://d/reply.xyz', uri)
        self.assertEqual(
            {'_msg_id': 'msg-1', '_heartbeat': True}, jsonutils.loads(payload)
        )
        self.assertEqual(60, msg.client_timeout)

    def test_requeue_posts_a_copy(self):
        post = mock.Mock()
        handle = FakeHandle()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={},
            message={},
            reply_q=None,
            msg_id=None,
            post=post,
            bmq_message=FakeMessage(b'{"a": 1}', queue_uri='bmq://d/q'),
            handle=handle,
        )
        msg.requeue()

        post.assert_called_once_with('bmq://d/q', b'{"a": 1}', retry=0)
        self.assertTrue(handle.confirmed)

    def test_requeue_failure_keeps_message(self):
        handle = FakeHandle()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={},
            message={},
            reply_q=None,
            msg_id=None,
            post=mock.Mock(side_effect=oslo_messaging.MessageDeliveryFailure),
            bmq_message=FakeMessage(b'{}'),
            handle=handle,
        )
        msg.requeue()  # should not raise

        # not confirmed: BlazingMQ delivers it again
        self.assertFalse(handle.confirmed)

    def test_requeue_of_pool_keeps_message(self):
        post = mock.Mock()
        handle = FakeHandle()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={},
            message={},
            reply_q=None,
            msg_id=None,
            post=post,
            bmq_message=FakeMessage(b'{}', queue_uri='bmq://f/q?id=pool'),
            handle=handle,
        )
        msg.requeue()

        # a copy would be delivered to all the pools again
        post.assert_not_called()
        self.assertFalse(handle.confirmed)

    def test_requeue_supported(self):
        driver = self._get_driver()
        driver.require_features(requeue=True)  # should not raise


# ---------------------------------------------------------------------------
# Send (cast)
# ---------------------------------------------------------------------------


class TestSendCast(BmqTestBase):
    def test_cast_posts_to_topic(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute')
        driver.send(
            target,
            {'user': 'admin'},
            {'method': 'do'},
            wait_for_reply=False,
            timeout=5,
        )

        session = driver._get_producer()
        self.assertTrue(len(session.posted) >= 1)
        uri = session.posted[0][0]
        self.assertEqual('bmq://bmq.test.priority/openstack.compute', uri)

    def test_cast_to_server(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute', server='host1')
        driver.send(
            target, {}, {'method': 'x'}, wait_for_reply=False, timeout=5
        )

        uri = driver._get_producer().posted[0][0]
        self.assertEqual(
            'bmq://bmq.test.priority/openstack.compute.host1', uri
        )

    def test_cast_fanout(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute', fanout=True)
        driver.send(
            target, {}, {'method': 'x'}, wait_for_reply=False, timeout=5
        )

        uri = driver._get_producer().posted[0][0]
        self.assertEqual('bmq://bmq.test.broadcast/openstack.compute', uri)

    def test_cast_to_exchange(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(exchange='nova', topic='compute')
        driver.send(
            target, {}, {'method': 'x'}, wait_for_reply=False, timeout=5
        )

        uri = driver._get_producer().posted[0][0]
        self.assertEqual('bmq://bmq.test.priority/nova.compute', uri)


class TestWriters(BmqTestBase):
    def _cast(self, driver, topic):
        driver.send(
            oslo_messaging.Target(topic=topic),
            {},
            {},
            wait_for_reply=False,
            timeout=5,
        )

    def test_writer_kept_open(self):
        driver = self._get_driver()
        for _ in range(3):
            self._cast(driver, 'compute')

        producer = driver._get_producer()
        uri = 'bmq://bmq.test.priority/openstack.compute'
        self.assertEqual([(uri, False, True)], producer.opened)
        self.assertEqual([], producer.closed)
        self.assertEqual(3, len(producer.posted))

    def test_least_recently_used_writer_closed(self):
        self.useFixture(fixtures.MockPatchObject(impl_bmq, 'MAX_WRITERS', 2))
        driver = self._get_driver()
        for topic in ('a', 'b', 'a', 'c'):
            self._cast(driver, topic)

        self.assertEqual(
            ['bmq://bmq.test.priority/openstack.b'],
            driver._get_producer().closed,
        )

    def test_post_to_listened_queue(self):
        driver = self._get_driver()
        driver.listen(
            oslo_messaging.Target(topic='compute', server='host1'), 1, None
        )
        self._cast(driver, 'compute')

        # the queue of the listener is left as is
        session = driver._get_session()
        self.assertEqual([], session.closed)
        self.assertEqual([], session.posted)
        self.assertTrue(
            driver._queues['bmq://bmq.test.priority/openstack.compute']['read']
        )
        self.assertEqual(1, len(driver._get_producer().posted))


class TestSendCall(BmqTestBase):
    def _call(self, failure_info, allowed_remote_exmods=None):
        driver = self._get_driver()
        driver._allowed_remote_exmods = allowed_remote_exmods or []
        session = driver._get_producer()
        orig_post = session.post

        # Reply with the failure as soon as the call is posted
        def post(uri, payload, on_ack=None, **kw):
            orig_post(uri, payload, on_ack=on_ack, **kw)
            request = jsonutils.loads(payload)
            reply = impl_bmq.BmqIncomingMessage(
                ctxt={},
                message={},
                reply_q=request['_reply_q'],
                msg_id=request['_msg_id'],
                post=lambda reply_uri, data: driver._on_message(
                    FakeMessage(data, queue_uri=reply_uri), FakeHandle()
                ),
            )
            reply.reply(failure=failure_info)

        session.post = post
        target = oslo_messaging.Target(topic='conductor')
        return driver.send(
            target, {}, {'method': 'x'}, wait_for_reply=True, timeout=5
        )

    def _reply_to(self, driver, request, **body):
        body['_msg_id'] = request['_msg_id']
        driver._on_message(
            FakeMessage(
                jsonutils.dumps(body).encode(), queue_uri=request['_reply_q']
            ),
            FakeHandle(),
        )

    def test_calls_share_the_reply_queue(self):
        driver = self._get_driver()
        session = driver._get_producer()
        orig_post = session.post
        requests = []

        def post(uri, payload, on_ack=None, **kw):
            orig_post(uri, payload, on_ack=on_ack, **kw)
            request = jsonutils.loads(payload)
            requests.append(request)
            self._reply_to(driver, request, _reply=request['n'])

        session.post = post
        target = oslo_messaging.Target(topic='conductor')
        for n in range(3):
            self.assertEqual(
                n,
                driver.send(
                    target, {}, {'n': n}, wait_for_reply=True, timeout=5
                ),
            )

        reply_queues = {r['_reply_q'] for r in requests}
        self.assertEqual(1, len(reply_queues))
        self.assertEqual(3, len({r['_msg_id'] for r in requests}))
        # the reply queue is opened once, and stays open
        reply_uri = reply_queues.pop()
        listening = driver._get_session()
        self.assertEqual(
            1, len([o for o in listening.opened if o[0] == reply_uri])
        )
        self.assertNotIn(reply_uri, listening.closed)
        self.assertEqual({}, driver._waiters)

    def test_reply_of_another_call_is_dropped(self):
        driver = self._get_driver()
        session = driver._get_producer()
        orig_post = session.post

        def post(uri, payload, on_ack=None, **kw):
            orig_post(uri, payload, on_ack=on_ack, **kw)
            request = jsonutils.loads(payload)
            # a late reply of a previous call, then the reply of this one
            self._reply_to(driver, dict(request, _msg_id='old'), _reply='late')
            self._reply_to(driver, request, _reply='mine')

        session.post = post
        target = oslo_messaging.Target(topic='conductor')
        self.assertEqual(
            'mine',
            driver.send(target, {}, {}, wait_for_reply=True, timeout=5),
        )

    def test_call_timeout(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='conductor')
        self.assertRaises(
            oslo_messaging.MessagingTimeout,
            driver.send,
            target,
            {},
            {},
            wait_for_reply=True,
            timeout=0.1,
        )
        self.assertEqual({}, driver._waiters)

    def test_call_timeout_covers_ack_and_reply(self):
        driver = self._get_driver()
        session = driver._get_producer()

        # the broker acks after 0.3s, the reply comes 0.3s later: each
        # step is shorter than the timeout, but not the call
        def post(uri, payload, on_ack=None, **kw):
            request = jsonutils.loads(payload)
            time.sleep(0.3)
            on_ack(FakeAck(status='SUCCESS'))
            threading.Timer(
                0.3, self._reply_to, (driver, request), {'_reply': 'late'}
            ).start()

        session.post = post
        target = oslo_messaging.Target(topic='conductor')
        self.assertRaises(
            oslo_messaging.MessagingTimeout,
            driver.send,
            target,
            {},
            {},
            wait_for_reply=True,
            timeout=0.5,
        )

    def _monitored_call(self, heartbeats, reply_after):
        driver = self._get_driver()
        session = driver._get_producer()
        orig_post = session.post
        requests = []

        def post(uri, payload, on_ack=None, **kw):
            orig_post(uri, payload, on_ack=on_ack, **kw)
            request = jsonutils.loads(payload)
            requests.append(request)
            for n in range(heartbeats):
                threading.Timer(
                    0.1 * (n + 1),
                    self._reply_to,
                    (driver, request),
                    {'_heartbeat': True},
                ).start()
            threading.Timer(
                reply_after, self._reply_to, (driver, request), {'_reply': 42}
            ).start()

        session.post = post
        target = oslo_messaging.Target(topic='conductor')
        result = driver.send(
            target,
            {},
            {},
            wait_for_reply=True,
            timeout=60,
            call_monitor_timeout=0.3,
        )
        self.assertEqual(0.3, requests[0]['_timeout'])
        return result

    def test_call_monitor_heartbeats(self):
        # a heartbeat every 0.1s keeps the call alive for 0.6s
        self.assertEqual(42, self._monitored_call(5, 0.6))

    def test_call_monitor_timeout(self):
        # no heartbeat: the call fails after 0.3s, not 60s
        self.assertRaises(
            oslo_messaging.MessagingTimeout, self._monitored_call, 0, 1
        )

    def test_call_raises_original_exception(self):
        exc = ValueError('bad')
        self.assertRaisesRegex(
            ValueError, 'bad', self._call, (ValueError, exc, None)
        )

    def test_call_raises_allowed_exception(self):
        exc = oslo_messaging.MessagingTimeout('slow')
        self.assertRaisesRegex(
            oslo_messaging.MessagingTimeout,
            'slow',
            self._call,
            (type(exc), exc, None),
            ['oslo_messaging.exceptions'],
        )

    def test_call_raises_remote_error_if_not_allowed(self):
        exc = oslo_messaging.MessagingTimeout('slow')
        self.assertRaises(
            oslo_messaging.RemoteError, self._call, (type(exc), exc, None)
        )


class TestLogging(BmqTestBase):
    def setUp(self):
        super().setUp()
        bmq_logger = logging.getLogger('blazingmq')
        self.addCleanup(bmq_logger.setLevel, bmq_logger.level)

    def test_sdk_logs_lowered_to_warning(self):
        logging.getLogger('blazingmq').setLevel(logging.NOTSET)
        self._get_driver()
        self.assertEqual(logging.WARNING, logging.getLogger('blazingmq').level)

    def test_sdk_log_level_kept_if_set(self):
        logging.getLogger('blazingmq').setLevel(logging.INFO)
        self._get_driver()
        self.assertEqual(logging.INFO, logging.getLogger('blazingmq').level)

    def test_session_event_levels(self):
        driver = self._get_driver()
        Connected = type('Connected', (), {})
        ConnectionLost = type('ConnectionLost', (), {})
        with mock.patch.object(impl_bmq, 'LOG') as log:
            driver._on_session_event(Connected())
            log.info.assert_called_once()
            log.warning.assert_not_called()
            driver._on_session_event(ConnectionLost())
            log.warning.assert_called_once()

    def test_disconnected_on_cleanup_is_info(self):
        driver = self._get_driver()
        session = driver._get_session()
        Disconnected = type('Disconnected', (), {})
        session.stop = lambda: driver._on_session_event(Disconnected())
        with mock.patch.object(impl_bmq, 'LOG') as log:
            driver._on_session_event(Disconnected())
            log.warning.assert_called_once()
            driver.cleanup()
            log.info.assert_called_once()
            log.warning.assert_called_once()


class TestSecurity(BmqTestBase):
    def _session_kwargs(self, url):
        driver = self._get_driver(url=url)
        with mock.patch.object(self.mock_bmq, 'Session') as session_cls:
            driver._get_session()
        return session_cls.call_args[1]

    def test_no_credentials_no_tls(self):
        kwargs = self._session_kwargs('bmq://host:30114/bmq.test.priority')
        self.assertNotIn('authn_credential_provider', kwargs)
        self.assertNotIn('tls_certificate_authority', kwargs)

    def test_credentials_from_url(self):
        kwargs = self._session_kwargs(
            'bmq://openstack:s%40cret@host:30114/bmq.test.priority'
        )
        self.assertEqual('tcp://host:30114', kwargs['broker'])
        self.assertEqual(
            ('BASIC', b'openstack:s@cret'),
            kwargs['authn_credential_provider'](),
        )

    def test_tls_from_config(self):
        self.config(ssl_ca_file='/etc/bmq/ca.pem', group='oslo_messaging_bmq')
        kwargs = self._session_kwargs('bmq://host:30114/bmq.test.priority')
        self.assertEqual(
            '/etc/bmq/ca.pem', kwargs['tls_certificate_authority']
        )


# ---------------------------------------------------------------------------
# Listen
# ---------------------------------------------------------------------------


class TestListen(BmqTestBase):
    def test_listen_opens_topic_server_fanout(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute', server='host1')
        driver.listen(target, batch_size=1, batch_timeout=None)

        self.assertIn(
            'bmq://bmq.test.priority/openstack.compute',
            driver._listener_queues,
        )
        self.assertIn(
            'bmq://bmq.test.priority/openstack.compute.host1',
            driver._listener_queues,
        )
        self.assertIn(
            'bmq://bmq.test.broadcast/openstack.compute',
            driver._listener_queues,
        )

    def test_listen_without_server(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute')
        driver.listen(target, batch_size=1, batch_timeout=None)

        self.assertIn(
            'bmq://bmq.test.priority/openstack.compute',
            driver._listener_queues,
        )
        self.assertNotIn(
            'bmq://bmq.test.priority/openstack.compute.host1',
            driver._listener_queues,
        )

    def test_listen_for_notifications(self):
        driver = self._get_driver()
        targets = [
            (oslo_messaging.Target(topic='notifications'), 'info'),
            (oslo_messaging.Target(topic='notifications'), 'error'),
        ]
        driver.listen_for_notifications(
            targets, pool=None, batch_size=1, batch_timeout=None
        )

        self.assertIn(
            'bmq://bmq.test.priority/openstack.notifications.info',
            driver._listener_queues,
        )
        self.assertIn(
            'bmq://bmq.test.priority/openstack.notifications.error',
            driver._listener_queues,
        )

    def test_listen_for_notifications_of_exchanges(self):
        driver = self._get_driver()
        targets = [
            (oslo_messaging.Target(exchange='nova', topic='notif'), 'info'),
            (oslo_messaging.Target(exchange='cinder', topic='notif'), 'info'),
        ]
        driver.listen_for_notifications(
            targets, pool=None, batch_size=1, batch_timeout=None
        )

        self.assertIn(
            'bmq://bmq.test.priority/nova.notif.info', driver._listener_queues
        )
        self.assertIn(
            'bmq://bmq.test.priority/cinder.notif.info',
            driver._listener_queues,
        )

    def test_stop_closes_listener_queues(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute', server='host1')
        listener = driver.listen(target, batch_size=1, batch_timeout=None)
        session = driver._get_session()

        listener.start(lambda incoming: None)
        listener.stop()

        self.assertEqual(
            sorted(
                [
                    'bmq://bmq.test.priority/openstack.compute',
                    'bmq://bmq.test.priority/openstack.compute.host1',
                    'bmq://bmq.test.broadcast/openstack.compute',
                ]
            ),
            sorted(session.closed),
        )
        self.assertEqual({}, driver._listener_queues)
        # cleanup after stop does not close them again
        listener.cleanup()
        self.assertEqual(3, len(session.closed))

    def test_cleanup_closes_notification_queues(self):
        driver = self._get_driver()
        targets = [(oslo_messaging.Target(topic='notifications'), 'info')]
        listener = driver.listen_for_notifications(
            targets, pool=None, batch_size=1, batch_timeout=None
        )
        listener.cleanup()

        self.assertEqual(
            ['bmq://bmq.test.priority/openstack.notifications.info'],
            driver._get_session().closed,
        )

    def test_stop_keeps_queue_of_another_listener(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute')
        first = driver.listen(target, batch_size=1, batch_timeout=None)
        second = driver.listen(target, batch_size=1, batch_timeout=None)

        first.cleanup()

        # the queues now deliver to the second listener
        self.assertEqual([], driver._get_session().closed)
        second.cleanup()
        self.assertEqual(2, len(driver._get_session().closed))

    def test_notification_pool_needs_pool_domain(self):
        driver = self._get_driver()
        targets = [(oslo_messaging.Target(topic='t'), 'info')]
        self.assertRaises(
            NotImplementedError,
            driver.listen_for_notifications,
            targets,
            pool='mypool',
            batch_size=1,
            batch_timeout=None,
        )

    def test_notification_pool(self):
        self.config(pool_domain='bmq.test.fanout', group='oslo_messaging_bmq')
        driver = self._get_driver()
        targets = [(oslo_messaging.Target(topic='notifications'), 'info')]
        driver.listen_for_notifications(
            targets, pool='mypool', batch_size=1, batch_timeout=None
        )

        self.assertEqual(
            ['bmq://bmq.test.fanout/openstack.notifications.info?id=mypool'],
            list(driver._listener_queues),
        )

    def test_invalid_notification_pool(self):
        self.config(pool_domain='bmq.test.fanout', group='oslo_messaging_bmq')
        driver = self._get_driver()
        targets = [(oslo_messaging.Target(topic='t'), 'info')]
        self.assertRaises(
            ValueError,
            driver.listen_for_notifications,
            targets,
            pool='my pool',
            batch_size=1,
            batch_timeout=None,
        )


class TestSendNotification(BmqTestBase):
    def _notify(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='notifications.info')
        driver.send_notification(
            target, {'user': 'admin'}, {'event_type': 'x'}, 2.0
        )
        return driver._get_producer().posted

    def test_notification(self):
        posted = self._notify()

        self.assertEqual(
            ['bmq://bmq.test.priority/openstack.notifications.info'],
            [uri for uri, _, _ in posted],
        )

    def test_notification_with_pool_domain(self):
        self.config(pool_domain='bmq.test.fanout', group='oslo_messaging_bmq')
        posted = self._notify()

        self.assertEqual(
            [
                'bmq://bmq.test.priority/openstack.notifications.info',
                'bmq://bmq.test.fanout/openstack.notifications.info',
            ],
            [uri for uri, _, _ in posted],
        )
        # the same message, with its context
        self.assertEqual(posted[0][1], posted[1][1])
        self.assertEqual(
            {'user': 'admin'}, jsonutils.loads(posted[1][1])['_context']
        )

    def test_rpc_not_posted_to_pool_domain(self):
        self.config(pool_domain='bmq.test.fanout', group='oslo_messaging_bmq')
        driver = self._get_driver()
        driver.send(
            oslo_messaging.Target(topic='compute'),
            {},
            {'method': 'x'},
            wait_for_reply=False,
        )

        self.assertEqual(1, len(driver._get_producer().posted))


class SdkError(Exception):
    """Stands for blazingmq.Error."""


class TestErrors(BmqTestBase):
    def setUp(self):
        super().setUp()
        self.mock_bmq.Error = SdkError
        # do not wait between the attempts
        self.useFixture(fixtures.MockPatch('time.sleep'))

    def test_connection_failure(self):
        driver = self._get_driver()
        self.mock_bmq.Session = mock.Mock(side_effect=SdkError('timeout'))
        self.assertRaisesRegex(
            oslo_messaging.MessageDeliveryFailure,
            'Failed to connect to the BlazingMQ broker',
            driver.listen,
            oslo_messaging.Target(topic='compute', server='host1'),
            1,
            None,
        )

    def test_open_queue_failure(self):
        driver = self._get_driver()
        session = driver._get_session()
        session.open_queue = mock.Mock(side_effect=SdkError('refused'))
        self.assertRaisesRegex(
            oslo_messaging.MessageDeliveryFailure,
            'Failed to open the BlazingMQ queue',
            driver.listen,
            oslo_messaging.Target(topic='compute', server='host1'),
            1,
            None,
        )
        self.assertEqual({}, driver._queues)

    def _cast_with_failures(self, failures, retry):
        driver = self._get_driver()
        session = driver._get_producer()
        orig_post = session.post
        calls = []

        def post(uri, payload, on_ack=None, **kw):
            calls.append(uri)
            if len(calls) <= failures:
                raise SdkError('not connected')
            orig_post(uri, payload, on_ack=on_ack, **kw)

        session.post = post
        driver.send(
            oslo_messaging.Target(topic='compute'),
            {},
            {},
            wait_for_reply=False,
            retry=retry,
        )
        return calls

    def test_post_retried(self):
        self.assertEqual(3, len(self._cast_with_failures(2, retry=None)))

    def test_post_not_retried(self):
        self.assertRaises(
            oslo_messaging.MessageDeliveryFailure,
            self._cast_with_failures,
            1,
            retry=0,
        )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


class TestCleanup(BmqTestBase):
    def test_cleanup_stops_session(self):
        driver = self._get_driver()
        session = driver._get_session()
        driver.cleanup()
        self.assertTrue(session.stopped)
        self.assertIsNone(driver._session)

    def test_cleanup_noop_without_session(self):
        driver = self._get_driver()
        driver.cleanup()  # should not raise
