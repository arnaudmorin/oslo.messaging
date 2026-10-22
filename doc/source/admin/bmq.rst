=================================
BlazingMQ Driver Deployment Guide
=================================

Introduction
------------

The BlazingMQ driver is a prospective messaging transport backend in
*oslo.messaging*, for both RPC and notifications. `BlazingMQ`__ is an open
source message queue system, written in C++. Its brokers host queues,
grouped in *domains*, and replicate them in clusters.

__ https://github.com/bloomberg/blazingmq

Overview
--------

The driver uses the BlazingMQ Python SDK. Each transport opens two sessions
to the broker: one receives the messages of the RPC servers and
notification listeners, and the replies of the RPC calls; the other posts
all the messages.

Domains
~~~~~~~

The driver uses two domains, which must be configured on the broker:

* a domain in *priority* mode, where each message of a queue is delivered
  to one of its consumers, for the RPC requests, the replies and the
  notifications. Its name is the virtual host of the transport URL, for
  example ``bmq.openstack.priority``;

* a domain in *broadcast* mode, where each message is delivered to all the
  consumers connected at that time, for the fanout RPC requests. Its name
  is the name of the priority domain, with ``priority`` replaced by
  ``broadcast``, for example ``bmq.openstack.broadcast``.

Queues
~~~~~~

Queues are opened on demand, there is nothing to declare. Their names are
built from the exchange of the target (the ``control_exchange`` of the
service by default), its topic, and its server or priority:

====================  ===========================================
Target                Queue
====================  ===========================================
RPC to a topic        ``bmq://<priority domain>/<exchange>.<topic>``
RPC to a server       ``bmq://<priority domain>/<exchange>.<topic>.<server>``
RPC fanout            ``bmq://<broadcast domain>/<exchange>.<topic>``
RPC replies           ``bmq://<priority domain>/reply.<id>``
Notification          ``bmq://<priority domain>/<exchange>.<topic>.<priority>``
====================  ===========================================

BlazingMQ limits queue names to 64 characters, among letters, digits,
``-``, ``_`` and ``.``. Other names, for example the ones of the
cinder-volume servers (``host@backend``) or long host names, are made valid
and suffixed with a hash of the original name.

Each transport has a single reply queue, opened on its first RPC call. The
replies are matched to the calls by the id of the call. The driver supports
call monitoring (``call_monitor_timeout``): the server sends heartbeats to
the reply queue while the call runs.

Delivery
~~~~~~~~

Each message is acknowledged by the broker before the send returns, and
retried according to the ``retry`` argument of the send. A received
message is confirmed to the broker when the server acknowledges it: before
processing for RPC servers, after processing for notification listeners.
Requeued notifications are posted again to their queue. Messages received
but not confirmed are delivered again when their consumer goes away.

The broker drops the messages which are not consumed within the
``messageTtl`` of their domain.

Limitations
~~~~~~~~~~~

* Notification listener pools are not supported: listeners with a
  ``pool`` raise ``NotImplementedError``.
* Only the first host of the transport URL is used. High availability is
  provided by the BlazingMQ cluster, through a proxy or a load balancer in
  front of its brokers.
* The driver uses native threads, from the SDK: services must use the
  threading executor, eventlet is not supported.

Prerequisites
-------------

The driver requires the ``blazingmq`` Python SDK, which is not installed by
default. Use the ``bmq`` extras tag when installing *oslo.messaging*:

.. code-block:: shell

   $ python -m pip install oslo.messaging[bmq]

Authentication and TLS require a version of the SDK which supports them.

Broker Configuration
--------------------

Refer to the `BlazingMQ documentation`__ for the configuration of the
brokers and clusters. The domains used by the driver are configured with
files in the ``domains`` directory of the broker configuration, named
after the domain. For example, for ``bmq.openstack.priority``:

__ https://bloomberg.github.io/blazingmq/

.. code-block:: json

    {
        "definition": {
            "location": "local",
            "parameters": {
                "maxDeliveryAttempts": 0,
                "deduplicationTimeMs": 300000,
                "consistency": {"eventual": {}},
                "storage": {
                    "config": {"fileBacked": {}},
                    "domainLimits": {"bytes": 67108864, "messages": 100000},
                    "queueLimits": {"bytes": 33554432, "messages": 50000}
                },
                "messageTtl": 300,
                "maxProducers": 0,
                "maxConsumers": 0,
                "maxQueues": 0,
                "maxIdleTime": 0,
                "mode": {"priority": {}}
            }
        }
    }

``bmq.openstack.broadcast`` is configured the same way, with
``"mode": {"broadcast": {}}``. As its messages are only delivered to the
consumers connected when they are posted, its storage can be in memory
(``"inMemory": {}``).

Choose ``messageTtl`` with the longest time the messages may wait for a
consumer, for example the notifications while their consumer is down.

Authentication
~~~~~~~~~~~~~~

The driver authenticates with the ``BASIC`` mechanism, with the username
and password of the transport URL. Configure the basic authenticator of the
broker, and refuse anonymous clients, in the broker configuration
(``bmqbrkrcfg.json``):

.. code-block:: json

    "authentication": {
        "authenticators": [
            {
                "name": "BasicAuthenticator",
                "settings": [
                    {"key": "openstack", "value": {"stringVal": "secret"}}
                ]
            }
        ],
        "anonymousCredential": {"disallow": {}}
    }

The credentials are sent in clear text without TLS: use them with TLS.

TLS
~~~

Configure a TLS listener and its certificate in the broker configuration,
then set :oslo.config:option:`oslo_messaging_bmq.ssl_ca_file` to the
certificate authority which signed the certificate of the broker. The
driver then connects with TLS 1.3 and verifies the certificate.

Configuration
-------------

Transport URL
~~~~~~~~~~~~~

The transport URL is of the form::

    bmq://[user:password@]host[:port]/domain

The port defaults to 30114, and the domain to ``bmq.openstack.priority``.
For example, to use the driver for RPC and notifications::

    [DEFAULT]
    transport_url = bmq://openstack:secret@bmq.example.com:30115/bmq.openstack.priority

    [oslo_messaging_bmq]
    ssl_ca_file = /etc/ssl/certs/bmq-ca.pem

Driver Options
~~~~~~~~~~~~~~

The configuration options are in the
:oslo.config:group:`oslo_messaging_bmq` section of the service
configuration file:

- :oslo.config:option:`oslo_messaging_bmq.ssl_ca_file`

Logging
~~~~~~~

The SDK logs every queue operation at ``INFO``: the driver lowers its
logger, ``blazingmq``, to ``WARNING``, unless a level is set for it, for
example with ``default_log_levels = blazingmq=INFO``.
