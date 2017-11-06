import random
import unittest
import warnings

from six.moves import string_letters

import kombu
import kombu.common as common
from kombu import Connection, Exchange
from kombu.pools import producers
import mock

from event_consumer import EventConsumerApp
from event_consumer.conf import settings
from event_consumer.bootsteps import AMQPRetryConsumerStep
from event_consumer.handlers import AMQPRetryHandler


def random_body():
    return dict(body=''.join([random.choice(string_letters) for _ in range(25)]))


class BaseRetryHandlerIntegrationTest(unittest.TestCase):

    # Must not collide with real queue names!
    routing_key = 'RetryHandlerIntegrationTest'
    exchange = 'default'  # see settings.EXCHANGES

    body = staticmethod(random_body)

    def setUp(self):
        super(BaseRetryHandlerIntegrationTest, self).setUp()

        # NOTE:
        # must be a real rabbitmq instance, we rely on rabbitmq
        # features (dead-letter exchange) for our retry queue logic
        self.connection = kombu.Connection(
            settings.BROKER_URL,
            connect_timeout=1,
        )
        self.connection.ensure_connection()
        self.connection.connect()
        self.channel = self.connection.channel()

        self.handler = AMQPRetryHandler(
            self.channel,
            routing_key=self.routing_key,
            queue_name=self.routing_key,
            exchange_key=self.exchange,
            func=lambda body: None,
            backoff_func=lambda attempt: 0,
        )
        self.handler.declare_queues()

        for queue in self.handler.queues.values():
            queue.purge()

        self.archive_consumer = kombu.Consumer(
            channel=self.channel,
            queues=[self.handler.queues[AMQPRetryHandler.ARCHIVE]],
            callbacks=[self.handle_archive]
        )

        for consumer in [self.handler.consumer, self.archive_consumer]:
            consumer.consume()

        self.producer = kombu.Producer(
            self.channel,
            exchange=self.handler.exchange,
            routing_key=self.routing_key,
            serializer='json'
        )
        self.archives = []

        self.app = EventConsumerApp()
        self.app.config_from_object('test_app.celeryconfig')

    def tearDown(self):
        for consumer in [self.handler.consumer, self.archive_consumer]:
            common.ignore_errors(self.connection, consumer.cancel)

        for queue in self.handler.queues.values():
            # Carefully delete test queues which must be empty and have no consumers running.
            queue.delete(
                if_unused=True,
                if_empty=True,
            )

        if self.handler.exchange.name:
            self.handler.exchange.delete(if_unused=True)

        self.connection.close()
        super(BaseRetryHandlerIntegrationTest, self).tearDown()

    def handle_archive(self, body, message):
        self.archives.append((body, message))
        message.ack()


def get_connection():
    # NOTE:
    # must be a real rabbitmq instance, (not a fake 'in-memory' broker)
    # because we rely on rabbitmq features (dead-letter exchange) for our
    # retry queue logic
    connection = kombu.Connection(
        settings.BROKER_URL,
        connect_timeout=1,
    )
    connection.ensure_connection()
    connection.connect()
    return connection


def configure_handlers(channel, parent):
    step = AMQPRetryConsumerStep(parent)
    handlers = step.get_handlers(channel=channel)
    for handler in handlers:
        handler.declare_queues()

        for queue in handler.queues.values():
            queue.purge()

        handler.consumer.consume()
    return handlers


def shut_down(handlers, connection):
    for handler in handlers:
        common.ignore_errors(connection, handler.consumer.cancel)

    for handler in handlers:
        # Carefully delete test queues and exchanges
        # We require them to be empty and unbound to be sure all our cleanup
        # is being done correctly (i.e. nothing got left behind by mistake)
        for queue in handler.queues.values():
            queue.delete(
                if_unused=True,
                if_empty=True,
            )

    for handler in handlers:
        if handler.exchange.name:
            handler.exchange.delete(if_unused=True)

    connection.close()


class BaseConsumerIntegrationTest(unittest.TestCase):

    exchange = 'default'  # see settings.EXCHANGES

    body = staticmethod(random_body)

    def setUp(self):
        super(BaseConsumerIntegrationTest, self).setUp()

        self.connection = get_connection()
        self.channel = self.connection.channel()
        self.app = EventConsumerApp()
        self.app.config_from_object('test_app.celeryconfig')

    def configure_handlers(self):
        """
        Call from inside the test, *after* you have decorated your message handlers
        """
        parent = mock.MagicMock()
        parent.app = self.app
        self.handlers = configure_handlers(self.channel, parent)

    def tearDown(self):
        shut_down(self.handlers, self.connection)
        super(BaseConsumerIntegrationTest, self).tearDown()

    def get_producer(self, handler, routing_key=None):
        return kombu.Producer(
            handler.channel,
            exchange=handler.exchange,
            routing_key=handler.routing_key if routing_key is None else routing_key,
            serializer='json'
        )

    def get_handlers_for_key(self, routing_key):
        return [
            handler
            for handler in self.handlers
            if handler.routing_key == routing_key
        ]


def publish(routing_key, body, exchange_key=settings.DEFAULT_EXCHANGE_KEY, **kwargs):
    """
    Publish a single message, with the specified routing key, to the broker
    and exchange configured in the app `settings` object.
    """
    try:
        exchange = Exchange(**settings.EXCHANGES[exchange_key])
    except KeyError:
        if exchange_key == settings.DEFAULT_EXCHANGE_KEY:
            exchange = Exchange()
        else:
            raise
    connection = Connection(settings.BROKER_URL)

    defaults = {
        'serializer': 'json',
    }
    defaults.update(kwargs)

    with producers[connection].acquire(block=True) as producer:
        producer.publish(
            body,
            exchange=exchange,
            routing_key=routing_key,
            declare=[exchange],
            **defaults
        )


def use_test_databases():
    """
    Adapted from DjangoTestSuiteRunner.setup_databases
    """
    from django.db import connections, DEFAULT_DB_ALIAS
    try:
        from django.test.utils import dependency_ordered  # 1.11
    except ImportError:
        try:
            from django.test.simple import dependency_ordered  # <= 1.5
        except ImportError:
            from django.test.runner import dependency_ordered  # 1.5 < v < 1.11

    # First pass -- work out which databases connections need to be switched
    # and which ones are test mirrors or duplicate entries in DATABASES
    mirrored_aliases = {}
    test_databases = {}
    dependencies = {}
    for alias in connections:
        connection = connections[alias]
        test_mirror = connection.settings_dict.get('TEST_MIRROR')
        if test_mirror:
            # If the database is marked as a test mirror, save
            # the alias.
            mirrored_aliases[alias] = test_mirror
        else:
            # Store a tuple with DB parameters that uniquely identify it.
            # If we have two aliases with the same values for that tuple,
            # they will have the same test db name.
            item = test_databases.setdefault(
                connection.creation.test_db_signature(),
                (connection.settings_dict['NAME'], [])
            )
            item[1].append(alias)

            if 'TEST_DEPENDENCIES' in connection.settings_dict:
                dependencies[alias] = (
                    connection.settings_dict['TEST_DEPENDENCIES'])
            else:
                if alias != DEFAULT_DB_ALIAS:
                    dependencies[alias] = connection.settings_dict.get(
                        'TEST_DEPENDENCIES', [DEFAULT_DB_ALIAS])

    # Second pass -- switch the databases to use test db settings.
    for signature, (db_name, aliases) in dependency_ordered(
            test_databases.items(), dependencies):
        # get test db name from the first connection
        connection = connections[aliases[0]]
        for alias in aliases:
            connection = connections[alias]
            test_db_name = connection.creation._get_test_db_name()
            # NOTE: if using sqlite for tests, be sure to specify a
            # TEST_NAME / TEST:NAME with a real filename to avoid using
            # in-memory db
            if test_db_name == ':memory:':
                # Django converts all sqlite test dbs to :memory: ...but
                # they can't be shared between concurrent processes...
                # in this case it also means our parent test run used an
                # in-memory db that we can't share
                warnings.warn(
                    "In-memory databases can't be shared between concurrent "
                    "test processes. "
                    "{parent} -> {test}".format(parent=db_name, test=test_db_name)
                )
            # we are running late in Django life-cycle so it has already
            # opened connections to default db, need to close and re-open
            # against test db:
            connection.close()
            connection.settings_dict['NAME'] = test_db_name
            connection.cursor()

    for alias, mirror_alias in mirrored_aliases.items():
        # we are running late in Django life-cycle so it has already
        # opened connections to default db, need to close and re-open
        # against test mirror db:
        connection = connections[alias]
        connection.close()
        connection.settings_dict['NAME'] = (
            connections[mirror_alias].settings_dict['NAME'])
        connection.features = connections[mirror_alias].features
        connection.cursor()
