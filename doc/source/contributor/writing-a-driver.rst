============================
 Writing a Transport Driver
============================

This document walks through the creation of a new transport driver for
oslo.messaging: what to do before writing code, how a driver is loaded, the
behavior the rest of the library and the OpenStack services expect from it,
and how to test, document and maintain it.

The :doc:`driver-dev-guide` is the reference of the driver interface; this
document refers to it rather than repeating it. The smallest complete driver
is the fake driver (``oslo_messaging/_drivers/impl_fake.py``), which keeps
everything in memory and is a good place to start reading. The RabbitMQ
driver (``impl_rabbit.py`` and ``amqpdriver.py``) is the reference for the
expected behavior.

.. note::

   The driver interface is internal to oslo.messaging and **private**. It
   can change between releases, which is one of the reasons to maintain a
   driver in the oslo.messaging tree rather than outside of it.

Before writing code
===================

Read the :doc:`supported-messaging-drivers` policy. A driver carried in
oslo.messaging needs unit and functional test coverage, integration jobs
with at least three services, documentation, and at least two people
committed to maintaining it. A new driver has one release cycle (or six
months) to meet these requirements. Plan for them from the start: the CI
jobs and the second maintainer usually take longer than the code.

Propose the driver in `oslo-specs <https://opendev.org/openstack/oslo-specs>`_
and discuss it with the Oslo team, for example at the weekly meeting or on
the openstack-discuss mailing list with the ``[oslo]`` tag. The spec should
explain how the oslo.messaging concepts map to the messaging backend (see
`Addressing`_), which features are not supported, and how the driver will
be tested in the gate.

Check the client library of the backend early:

* its license must be compatible with OpenStack, and it must be accepted
  in the `global requirements
  <https://opendev.org/openstack/requirements>`_;
* it must be installable on every Python version and platform the gate
  uses, which usually means wheels on PyPI;
* it must work with the executors of the services (threads), see
  `Threads, executors and fork`_.

How a driver is loaded
======================

Services do not use drivers directly. They create a transport from a
transport URL, for example::

  [DEFAULT]
  transport_url = mydriver://user:secret@host1:1234,host2:1234/vhost?opt=1

The scheme of the URL selects the driver: it is looked up in the
``oslo.messaging.drivers`` entry point namespace, so a driver is
registered in ``pyproject.toml``:

.. code-block:: toml

  [project.entry-points."oslo.messaging.drivers"]
  mydriver = "oslo_messaging._drivers.impl_mydriver:MyDriver"

The driver class is then instantiated once per transport with::

  MyDriver(conf, url, default_exchange=conf.control_exchange,
           allowed_remote_exmods=allowed_remote_exmods)

* ``conf`` is the ``ConfigOpts`` of the service. Register the options of
  the driver on it, in their own group (see `Configuration`_).
* ``url`` is a ``TransportURL``: ``url.hosts`` is a list of
  ``TransportHost`` with ``hostname``, ``port``, ``username`` and
  ``password``; ``url.virtual_host`` and ``url.query`` hold the path and
  the query string. Credentials belong in the URL, so that the services
  configure every driver the same way.
* ``default_exchange`` is the exchange of the targets that do not set one,
  usually the name of the service (``nova``, ``neutron``...).
* ``allowed_remote_exmods`` is the list of modules of the exceptions the
  driver may raise again on the caller side (see `Replies and failures`_).

The constructor must not connect to the backend: services create
transports at import or configuration time, often before forking. Connect
on first use.

The same driver class serves both the RPC transports
(``get_rpc_transport``) and the notification transports
(``get_notification_transport``), which a service may configure with
different URLs.

The pieces of a driver
======================

A driver is made of three kinds of classes, all defined in
``oslo_messaging/_drivers/base.py``:

* a subclass of ``BaseDriver``: the entry point, which sends messages and
  creates listeners;
* a listener, which hands the received messages to a server. Most drivers
  subclass ``PollStyleListener`` and wrap it in a
  ``PollStyleListenerAdapter``, which runs the polling loop in a thread;
  drivers that are called back by their library can subclass ``Listener``
  instead;
* the incoming messages: a subclass of ``RpcIncomingMessage``, which knows
  how to acknowledge, requeue and reply to the message it represents.

This is how they are used::

  RPCClient.call()/cast()           Notifier.info()...
         |                                |
         v                                v
  driver.send(target, ...)          driver.send_notification(target, ...)
         |                                |
         +--------> backend <-------------+
                       |
                       v
  listener = driver.listen(target)          (RPCServer)
  listener = driver.listen_for_notifications(targets_and_priorities, pool)
                       |                    (NotificationListener)
                       v
  listener.poll() -> [IncomingMessage, ...]
                       |
                       v
  server: message.acknowledge(), dispatch to the endpoint,
          message.heartbeat() while it runs (RPC calls only),
          message.reply(result or failure) / message.requeue()

The ``ctxt`` received by ``send`` and ``send_notification`` is already
serialized by the serializer of the client: it is a dictionary that the
driver sends with the message and gives back, unchanged, as the ``ctxt`` of
the incoming message.

Addressing
==========

A ``Target`` has an ``exchange`` (``default_exchange`` when not set), a
``topic``, an optional ``server``, and a ``fanout`` flag. The driver maps
these to addresses of the backend (queues, subjects, channels...), and the
mapping must be the same in ``send`` and in ``listen``.

An RPC server created with ``listen(target)`` receives:

* the messages sent to the exchange and topic of the target, with each
  message delivered to **one** of the servers listening on the topic,
  ideally balanced between them;
* the messages sent to the exchange, topic and server of the target;
* the fanout messages sent to the exchange and topic, delivered to
  **every** server listening on the topic at that time.

A notification listener created with ``listen_for_notifications`` receives
the messages sent to the topic ``"<topic>.<priority>"`` of each target, for
example ``notifications.info``. Notifications are store and forward: they
must be kept by the backend until a listener consumes them, even when no
listener is running when they are sent. Each notification is delivered to
one listener, unless *pools* are used: listeners with the same ``pool``
share the messages, and each pool receives its own copy. Drivers that do
not support pools raise ``NotImplementedError`` when a pool is given.

Include the exchange in the addresses. Different services use topics with
the same name (all of them send their notifications to the
``notifications`` topic of their own exchange), and listeners select them by
exchange.

Check the naming rules of the backend (length, characters) against real
targets: servers are usually host names, which can be long, and some are
not, for example cinder-volume uses ``host@backend``. Map the names that
break the rules to valid ones, keeping them distinct (for example with a
hash of the name), rather than failing on them.

For example, the RabbitMQ driver maps the exchange to an AMQP exchange, the
topic and the server to routing keys of queues bound to it, and fanout to a
fanout exchange per topic.

Sending
=======

``send`` blocks until the message is handed to the backend, and, for a call
(``wait_for_reply=True``), until the reply is received.

``timeout``
  Raise ``MessagingTimeout`` when the call did not complete within
  ``timeout`` seconds. The timeout covers the whole call, not each step of
  it. A message that could not be processed before the timeout is useless
  to the caller: if the backend allows it, let it expire (the RabbitMQ
  driver sets a per-message TTL), so that a server starting late does not
  process a backlog of abandoned calls.

``retry``
  How many times to retry when the backend is not reachable: ``None`` or
  ``-1`` retries forever, ``0`` does not retry, ``N`` retries ``N`` times,
  then raises ``MessageDeliveryFailure``. Retrying must not duplicate the
  message.

``call_monitor_timeout``
  When set, the caller expects a heartbeat from the server at least every
  ``call_monitor_timeout`` seconds while the call runs, and gives up early
  when they stop: this is how a long call (``timeout`` of an hour) detects a
  dead server in a minute. The driver sends ``call_monitor_timeout`` with
  the message, sets it as ``client_timeout`` on the incoming message, the
  RPC dispatcher then calls ``heartbeat()`` on it every
  ``client_timeout / 2`` seconds, and the driver sends a heartbeat message
  on the reply channel. On the caller side, each heartbeat restarts the
  call monitor timer, and the call fails with ``MessagingTimeout`` when the
  timer expires. See ``ReplyWaiter.wait`` in ``amqpdriver.py``.

``transport_options``
  Options specific to a backend, passed by the caller. Ignore the ones the
  driver does not know.

Raise ``oslo_messaging`` exceptions, never the ones of the backend library:
services catch ``MessagingTimeout``, ``MessageDeliveryFailure`` and
``MessagingException``.

``send_notification`` follows the same rules, without reply. When
``version`` is 2.0 or more, the message is wrapped in an envelope
(``common.serialize_msg``); ``common.deserialize_msg`` unwraps it on the
listener side.

Replies and failures
====================

The driver routes the reply of a call back to the caller. Typically the
caller listens on a reply address, sends it with the request (*reply-to*)
along with a message id, and matches the replies to the waiting calls with
that id. Prefer one reply address per transport, with the calls matched
by message id, to one per call: creating and removing an address for each
call costs round trips to the backend.

``reply`` must not raise: log the failures. An exception raised by the
endpoint is passed as ``failure``. Send it with
``common.serialize_remote_exception(failure)`` and raise it again on the
caller side with ``common.deserialize_remote_exception(data,
allowed_remote_exmods)``. Services depend on getting the original
exception class back: for example nova-compute catches
``ComputeHostNotFound`` on its first start to create its compute node.
Exceptions from modules that are not allowed are raised as
``RemoteError``.

Receiving
=========

``poll`` blocks until messages are available, the timeout expires, or
``stop`` is called from another thread, which must unblock it.
``@base.batch_poll_helper`` turns a ``poll`` that returns one message into
one that supports batches.

``acknowledge`` is called by the server before processing an RPC request,
and after processing a notification. The message is then removed from the
backend. A message that is received but never acknowledged must be
delivered again (to another listener) when the listener goes away.

``requeue`` gives the message back to the backend, to be delivered again.
It is only used by notification listeners created with
``allow_requeue=True``; the driver tells whether it supports it with
``require_features(requeue=True)``, which raises ``NotImplementedError``
by default.

``stop`` must stop the delivery to the listener, and ``cleanup`` must
remove its subscriptions from the backend. Messages must not be delivered
to a listener that was stopped, where they would never be processed: for
example the topic share of a stopped RPC server would be lost.

Connection failures and failover
================================

The backend will restart, and the network will fail. The driver must:

* reconnect, with a backoff, and restore the subscriptions of the
  listeners: a server must keep receiving messages after the backend came
  back, without being restarted;
* fail the pending calls and the sends, according to their ``timeout`` and
  ``retry``, rather than block forever;
* use the other hosts of the URL, if the backend has several nodes and
  the client library does not handle it;
* log the state changes (disconnected, reconnected) at ``WARNING`` and
  ``INFO``, but not every message: services log at ``INFO`` by default,
  and the logs of the backend library usually need to be lowered.

Threads, executors and fork
===========================

Services run the endpoints in a thread pool (the ``threading`` executor).
The driver is called from many threads at the same time: from the
application threads that send, from the listener threads that poll, and
often from threads of the backend library that deliver messages and events.
Protect the shared state, do not hold locks during calls to the backend,
and be careful with the restrictions of the library (for example calls
that are not allowed from its callbacks).

Eventlet is deprecated in OpenStack and being removed from the services;
new drivers should target threads. A library that runs native threads does
not work with eventlet monkey patching.

Services fork: oslo.service and cotyledon fork their workers after the
service, and often its transport, was created in the parent process. A
connection opened before the fork is shared by all the processes, which
then read each other's data. Connect lazily, and if the library cannot be
used after a fork, detect it (compare ``os.getpid()`` with the pid that
opened the connection) and open a new connection in the child. See
:doc:`/reference/transport`.

Configuration
=============

Register the options of the driver in their own group, for example
``[oslo_messaging_mydriver]``, from the constructor of the driver. Keep
them in a module that does not import the backend library, for example
``oslo_messaging/_drivers/mydriver_options.py``, and add them to ``_opts``
in ``oslo_messaging/opts.py``: ``oslo-config-generator`` and the
documentation list the options of every driver, without the libraries of
the drivers installed. Update ``oslo_messaging/tests/test_opts.py``
accordingly.

Do not add options for what can be in the transport URL (hosts,
credentials, virtual host).

Packaging
=========

The backend library is an optional dependency: add an extra in
``pyproject.toml``, so that deployers install it with
``oslo.messaging[mydriver]``, and import it only from the driver module,
which is only imported when the driver is used. The library must be in the
global requirements and the upper constraints of OpenStack.

Testing
=======

Unit tests
  In ``oslo_messaging/tests/drivers/test_impl_mydriver.py``, with the
  backend library mocked. They run on every Python version of the gate.

Functional tests
  ``oslo_messaging/tests/functional`` tests the behavior described in this
  document against a real backend: calls, casts, fanout, timeouts,
  exceptions, notifications, pools, requeue. The tests read the
  ``RPC_TRANSPORT_URL`` and ``NOTIFY_TRANSPORT_URL`` environment variables,
  and skip the features a driver does not support (check the
  ``skipTest`` calls in the tests, and add yours with a reason). They are
  the best way to find what a new driver is missing, run them early::

    export RPC_TRANSPORT_URL=mydriver://127.0.0.1:1234/
    export NOTIFY_TRANSPORT_URL=$RPC_TRANSPORT_URL
    stestr run oslo_messaging.tests.functional

  The tests monkey patch eventlet, and run the servers with the eventlet
  executor. A backend library which runs its own threads, and calls the
  driver back from them, does not work with eventlet: the tests then hang.

  In the gate, a scenario of ``tools/setup-scenario-env.sh`` starts the
  backend and sets the URLs, a ``py3xx-func-scenarioNN`` tox environment
  runs the tests, and a ``oslo.messaging-tox-py3xx-func-scenarioNN`` job
  in ``.zuul.yaml`` runs the environment. The backend is installed from
  ``bindep.txt`` (with a profile) or by the scenario.

Integration tests
  A devstack plugin installs the backend and configures the services to
  use it, like `devstack-plugin-kafka
  <https://opendev.org/openstack/devstack-plugin-kafka>`_. A devstack
  Tempest job runs nova, neutron and cinder with the driver, in the gate of
  oslo.messaging (see the ``oslo.messaging-src-dsvm-full-*`` jobs).

Performance
  ``tools/simulator.py`` sends and receives calls, casts and notifications
  at a given rate, see :doc:`using-simulator`. Compare with the RabbitMQ
  driver on the same hosts.

Documentation
=============

* A page in the :doc:`/admin/index`, like the RabbitMQ and Kafka ones: the
  transport URL, the configuration of the backend that the driver
  expects, the options, security (authentication, TLS), and the
  limitations.
* The options are listed in the configuration reference
  (``doc/source/configuration``).
* A release note (``reno new``).
* The status of the driver in :doc:`supported-messaging-drivers`.

Checklist
=========

* Spec discussed and approved in oslo-specs.
* The backend library is in the global requirements, with wheels for the
  Python versions of the gate.
* Entry point and extra in ``pyproject.toml``.
* No connection in the constructor; reconnection with restored
  subscriptions; safe after a fork.
* ``send``: topic, server and fanout addressing, with the exchange;
  ``timeout`` over the whole call; ``retry`` without duplicates;
  ``call_monitor_timeout`` with heartbeats; ``oslo_messaging``
  exceptions.
* Replies matched by message id; failures serialized with
  ``serialize_remote_exception``.
* Notifications: store and forward, ``"<topic>.<priority>"``, envelope,
  pools or ``NotImplementedError``, ``require_features``.
* Listeners: ``stop`` unblocks ``poll``; ``cleanup`` removes the
  subscriptions.
* Options in their own group and module, listed in ``opts.py``.
* Unit tests, functional tests and job, devstack plugin and Tempest job.
* Admin guide page, configuration reference, release note, two
  maintainers.
