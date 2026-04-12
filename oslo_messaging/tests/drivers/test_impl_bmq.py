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

import queue
import sys
import threading
import types
from unittest import mock

# blazingmq is a C extension that may not be installed in the test
# environment. Insert a fake module before importing the driver.
_fake_bmq = types.ModuleType('blazingmq')
_fake_bmq.Session = None
_fake_bmq.Error = Exception
_fake_bmq.AckStatus = types.SimpleNamespace(SUCCESS='SUCCESS')
sys.modules.setdefault('blazingmq', _fake_bmq)

from oslo_serialization import jsonutils

import oslo_messaging
from oslo_messaging._drivers import impl_bmq
from oslo_messaging.tests import utils as test_utils



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
        self.opened = []   # (uri, read, write)
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
        self._check('bmq://broker:5555/bmq.prod.priority',
                     'tcp://broker:5555',
                     'bmq.prod.priority',
                     'bmq.prod.broadcast')

    def test_default_port(self):
        self._check('bmq://myhost/bmq.test.priority',
                     'tcp://myhost:30114',
                     'bmq.test.priority',
                     'bmq.test.broadcast')

    def test_all_defaults(self):
        self._check('bmq://',
                     'tcp://localhost:30114',
                     'bmq.openstack.priority',
                     'bmq.openstack.broadcast')


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

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
        self.assertRaises(ValueError,
                          driver._open_queue, 'bmq://d/q',
                          read=True, write=True)

    def test_open_rejects_no_flags(self):
        driver = self._get_driver()
        self.assertRaises(ValueError,
                          driver._open_queue, 'bmq://d/q')

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
        listener = impl_bmq.BmqListener(post_fn=mock.Mock())
        payload = jsonutils.dumps({
            '_context': {'user': 'admin'},
            '_reply_q': 'bmq://d/reply.abc',
            '_msg_id': 'id-1',
            'method': 'test',
        }).encode()
        listener.queue.put((FakeMessage(payload), FakeHandle()))

        results = listener.poll(timeout=1)
        self.assertEqual(1, len(results))
        result = results[0]
        self.assertIsInstance(result, impl_bmq.BmqIncomingMessage)
        self.assertEqual({'user': 'admin'}, result.ctxt)
        self.assertEqual({'method': 'test'}, result.message)

    def test_poll_timeout(self):
        listener = impl_bmq.BmqListener(post_fn=mock.Mock())
        results = listener.poll(timeout=0.01)
        self.assertEqual([], results)

    def test_stop_unblocks_poll(self):
        listener = impl_bmq.BmqListener(post_fn=mock.Mock())
        listener.stop()
        results = listener.poll(timeout=1)
        self.assertEqual([], results)

    def test_bad_json_skipped(self):
        listener = impl_bmq.BmqListener(post_fn=mock.Mock())
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
            ctxt={}, message={},
            reply_q='bmq://d/reply.xyz',
            msg_id='msg-1', post=post_fn)

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
            ctxt={}, message={},
            reply_q='bmq://d/reply.abc',
            msg_id='msg-2', post=post_fn)

        msg.reply(failure=(ValueError, ValueError('bad')))

        body = jsonutils.loads(post_fn.call_args[0][1])
        self.assertEqual(['ValueError', 'bad'], body['_failure'])

    def test_reply_no_reply_q_is_noop(self):
        post_fn = mock.Mock()
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={}, message={},
            reply_q=None, msg_id='msg-3', post=post_fn)
        msg.reply(reply={'v': 1})
        post_fn.assert_not_called()

    def test_requeue_noop(self):
        msg = impl_bmq.BmqIncomingMessage(
            ctxt={}, message={},
            reply_q=None, msg_id=None, post=mock.Mock())
        msg.requeue()  # should not raise


# ---------------------------------------------------------------------------
# Send (cast)
# ---------------------------------------------------------------------------

class TestSendCast(BmqTestBase):

    def test_cast_posts_to_topic(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute')
        driver.send(target, {'user': 'admin'}, {'method': 'do'},
                    wait_for_reply=False, timeout=5)

        session = driver._get_session()
        self.assertTrue(len(session.posted) >= 1)
        uri = session.posted[0][0]
        self.assertEqual('bmq://bmq.test.priority/compute', uri)

    def test_cast_to_server(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute', server='host1')
        driver.send(target, {}, {'method': 'x'},
                    wait_for_reply=False, timeout=5)

        uri = driver._get_session().posted[0][0]
        self.assertEqual('bmq://bmq.test.priority/compute.host1', uri)

    def test_cast_fanout(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute', fanout=True)
        driver.send(target, {}, {'method': 'x'},
                    wait_for_reply=False, timeout=5)

        uri = driver._get_session().posted[0][0]
        self.assertEqual('bmq://bmq.test.broadcast/compute', uri)


# ---------------------------------------------------------------------------
# Listen
# ---------------------------------------------------------------------------

class TestListen(BmqTestBase):

    def test_listen_opens_topic_server_fanout(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute', server='host1')
        driver.listen(target, batch_size=1, batch_timeout=None)

        self.assertIn('bmq://bmq.test.priority/compute',
                       driver._listener_queues)
        self.assertIn('bmq://bmq.test.priority/compute.host1',
                       driver._listener_queues)
        self.assertIn('bmq://bmq.test.broadcast/compute',
                       driver._listener_queues)

    def test_listen_without_server(self):
        driver = self._get_driver()
        target = oslo_messaging.Target(topic='compute')
        driver.listen(target, batch_size=1, batch_timeout=None)

        self.assertIn('bmq://bmq.test.priority/compute',
                       driver._listener_queues)
        self.assertNotIn('bmq://bmq.test.priority/compute.host1',
                          driver._listener_queues)

    def test_listen_for_notifications(self):
        driver = self._get_driver()
        targets = [
            (oslo_messaging.Target(topic='notifications'), 'info'),
            (oslo_messaging.Target(topic='notifications'), 'error'),
        ]
        driver.listen_for_notifications(targets, pool=None,
                                        batch_size=1, batch_timeout=None)

        self.assertIn('bmq://bmq.test.priority/notifications.info',
                       driver._listener_queues)
        self.assertIn('bmq://bmq.test.priority/notifications.error',
                       driver._listener_queues)

    def test_notification_pool_not_supported(self):
        driver = self._get_driver()
        targets = [(oslo_messaging.Target(topic='t'), 'info')]
        self.assertRaises(NotImplementedError,
                          driver.listen_for_notifications,
                          targets, pool='mypool',
                          batch_size=1, batch_timeout=None)


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
