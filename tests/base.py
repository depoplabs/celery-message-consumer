import random
import unittest

from six.moves import string_letters

import kombu.common as common
import kombu

from event_consumer.conf import settings
from event_consumer.handlers import AMQPRetryHandler, AMQPRetryConsumerStep


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
            queue=self.routing_key,
            exchange=self.exchange,
            queue_arguments={},
            func=lambda body: None,
            backoff_func=lambda attempt: 0,
        )
        self.handler.declare_queues()

        queues = [
            self.handler.worker_queue,
            self.handler.retry_queue,
            self.handler.archive_queue,
        ]
        for queue in queues:
            queue.purge()

        self.archive_consumer = kombu.Consumer(
            channel=self.channel,
            queues=[self.handler.archive_queue],
            callbacks=[self.handle_archive]
        )

        for consumer in [self.handler.consumer, self.archive_consumer]:
            consumer.consume()

        self.producer = kombu.Producer(
            self.channel,
            exchange=self.handler.exchanges[self.handler.exchange],
            routing_key=self.routing_key,
            serializer='json'
        )
        self.archives = []

    def tearDown(self):
        queues = [
            self.handler.worker_queue,
            self.handler.retry_queue,
            self.handler.archive_queue,
        ]

        for consumer in [self.handler.consumer, self.archive_consumer]:
            common.ignore_errors(self.connection, consumer.cancel)

        for queue in queues:
            # Carefully delete test queues which must be empty and have no consumers running.
            queue.delete(
                if_unused=True,
                if_empty=True,
            )

        for name, exchange_settings in settings.EXCHANGES.items():
            self.handler.exchanges[name].delete(if_unused=True)

        self.connection.close()
        super(BaseRetryHandlerIntegrationTest, self).tearDown()

    def handle_archive(self, body, message):
        self.archives.append((body, message))
        message.ack()


class BaseConsumerIntegrationTest(unittest.TestCase):

    exchange = 'default'  # see settings.EXCHANGES

    body = staticmethod(random_body)

    def setUp(self):
        super(BaseConsumerIntegrationTest, self).setUp()

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

    def configure_handlers(self):
        """
        Call from inside the test, *after* you have decorated your message handlers
        """
        step = AMQPRetryConsumerStep(None)
        self.handlers = step.get_handlers(channel=self.channel)
        for handler in self.handlers:
            handler.declare_queues()

            queues = [
                handler.worker_queue,
                handler.retry_queue,
                handler.archive_queue,
            ]
            for queue in queues:
                queue.purge()

            handler.consumer.consume()

    def tearDown(self):
        for handler in self.handlers:
            common.ignore_errors(self.connection, handler.consumer.cancel)

        for handler in self.handlers:
            # Carefully delete test queues and exchanges
            # We require them to be empty and unbound to be sure all our cleanup
            # is being done correctly (i.e. nothing got left behind by mistake)
            queues = [
                handler.worker_queue,
                handler.retry_queue,
                handler.archive_queue,
            ]
            for queue in queues:
                queue.delete(
                    if_unused=True,
                    if_empty=True,
                )

        for handler in self.handlers:
            for name, exchange_settings in settings.EXCHANGES.items():
                handler.exchanges[name].delete(if_unused=True)

        self.connection.close()
        super(BaseConsumerIntegrationTest, self).tearDown()

    def get_producer(self, handler, routing_key=None):
        return kombu.Producer(
            handler.channel,
            exchange=handler.exchanges[handler.exchange],
            routing_key=handler.routing_key if routing_key is None else routing_key,
            serializer='json'
        )

    def get_handlers_for_key(self, routing_key):
        return [
            handler
            for handler in self.handlers
            if handler.routing_key == routing_key
        ]
