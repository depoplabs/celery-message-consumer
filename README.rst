celery-message-consumer
=======================

|PyPI Version| |Build Status|

.. |PyPI Version| image:: http://img.shields.io/pypi/v/celery-message-consumer.svg?style=flat
   :target: https://pypi.python.org/pypi/celery-message-consumer/
   :alt: Latest PyPI version

.. |Build Status| image:: https://circleci.com/gh/depop/celery-message-consumer.svg?style=shield&circle-token=a9ea2909c5cbc4cb32a87f50444ca79b99e3b09c
    :alt: Build Status

Tool for using the ``bin/celery`` worker to consume vanilla AMQP
messages (i.e. not Celery tasks)

While `writing a simple consumer
script <https://medium.com/python-pandemonium/building-robust-rabbitmq-consumers-with-python-and-kombu-part-1-ccd660d17271>`__
using Kombu can be quite easy, the Celery worker provides many features
around process pools, queue/routing connections etc as well as being
known to run reliably over long term.

It seems safer to re-use this battle-tested consumer than try to write
our own and have to learn from scratch all the ways that such a thing
can fail.

Usage
-----

.. code:: bash

    pip install celery-message-consumer


Handlers
~~~~~~~~

In your code, you can define a message handler by decorating a python
function, in much the same way as you would a Celery task:

.. code:: python

    from event_consumer import message_handler

    @message_handler('my.routing.key')
    def process_message(body):
        # `body` has been deserialized for us by the Celery worker
        print(body)

    @message_handler(['my.routing.key1', 'my.routing.key2'])
    def process_messages(body):
        # you can register handler for multiple routing keys

    @message_handler('my.routing.*')
    def process_all_messages(body):
        # or wildcard routing keys, if using a 'topic' exchange

Like a Celery task, the module it is defined in must actually get
imported at some point for the handler to be registered.

A queue (in fact, three queues - see below) will be created to receive
messages matching the routing key.

Celery
~~~~~~

Elsewhere in your code you will need to instantiate a Celery app and
apply our custom 'ConsumerStep' which hooks our message handlers into
the worker. If you are already using Celery *as Celery* in your project
then you probably want separate Celery apps for tasks and for the
message consumer.

.. code:: python

    from celery import Celery
    from event_consumer.handlers import AMQPRetryConsumerStep

    main_app = Celery()

    consumer_app = Celery()
    consumer_app.steps['consumer'].add(AMQPRetryConsumerStep)

You likely will want separate config for each app. See
`Celery docs <http://docs.celeryproject.org/en/latest/userguide/application.html#configuration>`__.

In the config for your message consumer app, add the modules containing
your decorated message handler functions to ``CELERY_IMPORTS``, exactly
as you would for Celery tasks - this ensures they get imported and
registered when the worker starts up.

Then from the command-line, run the Celery worker just like you usually
would, attaching to the consumer app:

.. code:: bash

    bin/celery worker -A myproject.mymodule:consumer_app

Configuration
~~~~~~~~~~~~~

Settings are intended to be configured primarily via a python file, such
as your existing Django ``settings.py`` or Celery ``celeryconfig.py``.
To bootstrap this, there are a couple of env vars to control how config
is loaded:

-  ``EVENT_CONSUMER_APP_CONFIG``
   should be an import path to a python module, for example:
   ``EVENT_CONSUMER_APP_CONFIG=django.conf.settings``
-  ``EVENT_CONSUMER_CONFIG_NAMESPACE``
   Sets the prefix used for loading further config values from env and
   config file. Defaults to ``EVENT_CONSUMER``.

See source of ``event_consumer/conf/`` for more details.

Some useful config keys (all of which are prefixed with
``EVENT_CONSUMER_`` by default):

-  ``SERIALIZER`` this is the name of a Celery serializer name, e.g.
   ``'json'``. The consumer will only accept messages serialized in this
   format.
-  ``QUEUE_NAME_PREFIX`` if using default queue name (routing-key) then
   this prefix will be added to the queue name. If you supply a custom
   queue name in the handler decorator the prefix will not be applied.
-  ``MAX_RETRIES`` defaults to ``4`` (i.e. 1 attempt + 4 retries = 5
   strikes)
-  ``BACKOFF_FUNC`` takes a function ``(int) -> float`` which returns
   the retry delay (in seconds) based on current retry counter for the
   message.
-  ``ARCHIVE_EXPIRY`` time in milliseconds to keep messages in the
   "archive" queue, after which the exchange will delete them. Defaults
   to 24 days.
-  ``USE_DJANGO`` set to ``True`` if your message handler uses the
   Django db connection, so that the worker is able to cope with the
   dreaded *"current transaction is aborted"* error and continue.
-  ``EXCHANGES`` if you need your message handlers to connect their
   queues to specific exchanges then you can provide a dict like:

.. code:: python

    EXCHANGES = {
        # a reference name for this config, used when attaching handlers
        'default': {  
            'name': 'data',  # actual name of exchange in RabbitMQ
            'type': 'topic',  # an AMQP exchange type
        },
        'other': {
            ...
        },
        ...
    }

The ``'default'`` config will be used... by default. You can attach
handler to a specific exchange when decorating:

.. code:: python

    @message_handler('my.routing.key', exchange='other')
    def process_message(body):
        pass

Queue layout
------------

While all of the broker, exchange and queue naming is configurable (see
source code) this project implements a *very specific queue pattern*.

Briefly: for each routing key it listens to, the consumer sets up
*three* queues and a 'dead-letter exchange' (DLX).

#. The "main" message queue
#. If any unhandled exceptions occur, and we have retried less than
   ``settings.MAX_RETRIES``, the message will be put on the "retry"
   queue with a TTL. After the TTL expires, the DLX will put the message
   back on the main queue.
#. If all retries are exhausted (or ``PermanentFailure`` is raised) then
   the consumer will put the message on the "archive" queue. This gives
   opportunity for someone to manually retry the archived messages,
   perhaps after a code fix has been deployed.

| You will of course note that this is *totally different and separate*
  from Celery's own ``task.retry`` mechanism.
| **Pros:** matches pattern we were already using for non-Celery,
  non-Python apps, "archive" queue provides an extra safety net.
| **Cons:** Relies on RabbitMQ-specific feature, more queues (more
  complicated).

Compatibility
-------------

Python 2.7 and 3.6 are both supported.

**Only** RabbitMQ transport is supported.

We depend on Celery and Kombu. Their versioning seems to be loosely in
step so that Celery 3.x goes with Kombu 3.x and Celery 4.x goes with
Kombu 4.x. We test against both v3 and v4.

Django is not required, but when used we have some extra integration
which is needed if your event handlers use the Django db connection.
This must be enabled if required via the ``settings.USE_DJANGO`` flag.

This project is tested against:

=========== ============ ============= ================== ==================
     x       Django 1.4   Django 1.11   Celery/Kombu 3.x   Celery/Kombu 4.x
=========== ============ ============= ================== ==================
Python 2.7       *             *                *                  *
Python 3.6                     *                *                  *                     
=========== ============ ============= ================== ==================

Running the tests
-----------------

CircleCI
~~~~~~~~

| The easiest way to test the full version matrix is to install the
  CircleCI command line app:
| https://circleci.com/docs/2.0/local-jobs/
| (requires Docker)

The cli does not support 'workflows' at the moment so you have to run
the two Python version jobs separately:

.. code:: bash

    circleci build --job python-2.7

.. code:: bash

    circleci build --job python-3.6

py.test (single combination of dependency versions)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's also possible to run the tests locally, allowing for debugging of
errors that occur.

We rely on some RabbitMQ features for our retry queues so we need a
rabbit instance to test against. A ``docker-compose.yml`` file is
provided.

.. code:: bash

    docker-compose up -d
    export BROKER_HOST=$(docker-machine ip default)

(adjust the last line to suit your local Docker installation)

The ``rabbitmqadmin`` web UI is available to aid in debugging queue issues:

.. code:: bash

    http://{BROKER_HOST}:15672/

Now decide which version combination from the matrix you're going to
test and set up your virtualenv accordingly:

.. code:: bash

    pyenv virtualenv 3.6.2 celery-message-consumer

You will need to edit ``requirements.txt`` and ``requirements-test.txt``
for the specific versions of dependencies you want to test against. Then
you can install everything via:

.. code:: bash

    pip install -r requirements-test.txt

Set an env to point to the target Django version's settings in the test
app (for Django-dependent tests) and for general app settings:

.. code:: bash

    export DJANGO_SETTINGS_MODULE=test_app.dj111.settings
    export EVENT_CONSUMER_APP_CONFIG=test_app.settings

Now we can run the tests:

.. code:: bash

    PYTHONPATH=. py.test -v -s --pdb tests/

tox (all version combinations for current Python)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You'll notice in the CircleCI config we run tests against the matrix
dependency versions using ``tox``.

There are `some warts <https://github.com/pyenv/pyenv-virtualenv/issues/202#issuecomment-339624649>`__
around using ``tox`` with ``pyenv-virtualenv`` so if you created a Python 3.6
virtualenv using the instructions above the best thing to do is delete it and
recreate it like this:

.. code:: bash

    pyenv virtualenv -p python3.6 myenv
    pip install tox

(it's actually easier not to use a virtualenv at all - tox creates its
own virtualenvs anyway, but that does mean you'd have to install tox
globally)

You need the Docker container running:

.. code:: bash

    docker-compose up -d
    export BROKER_HOST=$(docker-machine ip default)

You can now run tests for any versions compatible with your virtualenv
python version, e.g.

.. code:: bash

    tox -e py36-dj111-cel4

To run the full version matrix you need to have both Python 2.7 and 3.6. The
easiest way is via ``pyenv``. You will also need to make both Python versions
'global' (or 'local') via pyenv, and then install and run ``tox`` outside of
any virtualenv.

.. code:: bash

    source deactivate
    pyenv global 2.7.14 3.6.2
    pip install tox
    tox
