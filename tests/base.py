from contextlib import contextmanager
import os
import random
import socket
import unittest

from six.moves import string_letters

import kombu.common as common
import kombu

from event_consumer.conf import settings
from event_consumer import handlers as ec


class BaseRetryHandlerIntegrationTest(unittest.TestCase):

    # Must not collide with real queue names!
    routing_key = 'RetryHandlerIntegrationTest'
    exchange = 'default'  # see settings.EXCHANGES

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

        self.handler = ec.AMQPRetryHandler(
            self.channel,
            routing_key=self.routing_key,
            queue=self.routing_key,
            exchange=self.exchange,
            func=lambda body: None,
            backoff_func=lambda attempt: 0,
        )
        self.handler.declare_queues()

        self.archive_consumer = kombu.Consumer(
            channel=self.channel,
            queues=[self.handler.archive_queue],
            callbacks=[self.handle_archive]
        )

        for consumer in [self.handler.consumer, self.archive_consumer]:
            consumer.consume()

        self.producer = kombu.Producer(
            self.channel,
            exchange=self.handler.exchanges['default'],  # default broadcast exchange
            routing_key=self.routing_key,
            serializer='json'
        )
        self.archives = []

        # start fresh, in case previous test run errored and left a msg behind
        try:
            self.connection.drain_events(timeout=0.3)
        except socket.timeout:
            pass

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
        self.connection.close()

    def handle_archive(self, body, message):
        self.archives.append((body, message))
        message.ack()

    @staticmethod
    def body():
        return dict(body=''.join([random.choice(string_letters) for _ in range(25)]))


@contextmanager
def override_environment(**kwargs):
    old_env = os.environ
    new_env = os.environ.copy()
    new_env.update(kwargs)
    os.environ = new_env
    yield
    os.environ = old_env
