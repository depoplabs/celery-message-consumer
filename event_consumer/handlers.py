"""
Apparatus for consuming 'vanilla' AMQP messages (i.e. not Celery tasks) making use
of the battle-tested `bin/celery worker` utility rather than writing our own.

NOTE:
We don't access any Celery config in this file. That is because the config is
loaded by the bin/celery worker itself, according to usual mechanism.
"""
import logging
import traceback

from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Type, Union  # noqa

import amqp  # noqa
import kombu
import kombu.message

from event_consumer.conf import settings
from event_consumer.errors import NoExchange, PermanentFailure

if settings.USE_DJANGO:
    from django.core.signals import request_finished


_logger = logging.getLogger(__name__)


class AMQPRetryHandler(object):
    """
    Implements Depop's try-retry-archive message queue pattern.

    Briefly - messages are processed and may be retried by placing them on a separate retry
    queue on a dead-letter-exchange. Messages on the DLX are automatically re-queued by Rabbit
    once they expire. The expiry is set on a message-by-message basis to allow exponential
    backoff on retries.
    """

    WORKER = 'worker'
    RETRY = 'retry'
    ARCHIVE = 'archive'

    QUEUE_NAME_FORMATS = {
        WORKER: '{}',
        RETRY: '{}.retry',
        ARCHIVE: '{}.archived',
    }

    # keyed by `settings.EXCHANGES` key name
    exchanges = None  # type: Dict[str, kombu.Exchange]

    # keyed by queue name const as used in `QUEUE_NAME_FORMATS`
    queues = None  # type: Dict[str, kombu.Queue]

    def __init__(self,
                 channel,  # type: amqp.channel.Channel
                 routing_key,  # type: str
                 queue_name,  # type: str
                 exchange_key,  # type: str
                 func,  # type: Callable[[Any], Any]
                 backoff_func=None  # type: Optional[Callable[[int], float]]
                 ):
        # type: (...) -> None
        self.channel = channel
        self.routing_key = routing_key
        self.queue_name = queue_name
        self.exchange_key = exchange_key
        self.func = func
        self.backoff_func = backoff_func or self.backoff

        self.exchange = self.setup_exchange(exchange_key, channel)
        self.queues = self.setup_queues(
            queue_name=queue_name,
            routing_key=routing_key,
            exchange=self.exchange,
            channel=channel,
        )

        self.retry_producer = kombu.Producer(
            channel,
            exchange=self.queues[self.RETRY].exchange,
            routing_key=self.queues[self.RETRY].routing_key,
            serializer=settings.SERIALIZER,
        )

        self.archive_producer = kombu.Producer(
            channel,
            exchange=self.queues[self.ARCHIVE].exchange,
            routing_key=self.queues[self.ARCHIVE].routing_key,
            serializer=settings.SERIALIZER,
        )

        self.consumer = kombu.Consumer(
            channel,
            queues=[self.queues[self.WORKER]],
            callbacks=[self],
            accept=settings.ACCEPT,
        )

    @classmethod
    def setup_exchange(cls, key_name, channel=None):
        # type: (str, Optional[kombu.transport.base.StdChannel]) -> kombu.Exchange
        try:
            exchange = kombu.Exchange(
                **settings.EXCHANGES[key_name]
            )
        except KeyError:
            if key_name == settings.DEFAULT_EXCHANGE_KEY:
                # nothing custom configured, set up a default exchange
                exchange = kombu.Exchange()
            else:
                raise NoExchange(
                    "The exchange '{0}' was not found in settings.EXCHANGES. \n"
                    "settings.EXCHANGES = {1}".format(
                        key_name,
                        settings.EXCHANGES
                    )
                )

        # bind
        if channel:
            exchange = exchange(channel)

        return exchange

    @classmethod
    def setup_queues(cls,
                     queue_name,  # type: str
                     routing_key,  # type: str
                     exchange,  # type: kombu.Exchange
                     channel=None,  # type: Optional[kombu.transport.base.StdChannel]
                     ):
        # type: (...) -> Dict[str, kombu.Queue]
        queues = {}

        primary_name = cls.QUEUE_NAME_FORMATS[cls.WORKER].format(queue_name)
        queues[cls.WORKER] = kombu.Queue(
            name=cls.QUEUE_NAME_FORMATS[cls.WORKER].format(queue_name),
            exchange=exchange,
            routing_key=routing_key,
        )

        retry_name = cls.QUEUE_NAME_FORMATS[cls.RETRY].format(queue_name)
        queues[cls.RETRY] = kombu.Queue(
            name=retry_name,
            exchange=exchange,
            routing_key=retry_name,
            # N.B. default exchange automatically routes messages to a queue
            # with the same name as the routing key provided.
            queue_arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": primary_name,
            },
        )

        archive_name = cls.QUEUE_NAME_FORMATS[cls.ARCHIVE].format(queue_name)
        queues[cls.ARCHIVE] = kombu.Queue(
            name=archive_name,
            exchange=exchange,
            routing_key=archive_name,
            queue_arguments={
                "x-expires": settings.ARCHIVE_EXPIRY,  # Messages dropped after this
                "x-max-length": 1000000,  # Maximum size of the queue
                "x-queue-mode": "lazy",  # Keep messages on disk (reqs. rabbitmq 3.6.0+)
            },
        )

        if channel:
            queues = {
                key: queue(channel)
                for key, queue in queues.items()
            }

        return queues

    def __call__(self, body, message):
        # type: (Any, kombu.message.Message) -> None
        """
        Handle a vanilla AMQP message, called by the Celery framework.

        Raising an exception in this method will crash the Celery worker. Ensure
        that all Exceptions are caught and messages acknowledged or rejected
        as they are processed.

        Kwargs:
            body: the message content, which has been deserialized by Kombu
            message: kombu Message object
        """
        retry_count = self.retry_count(message)

        try:
            _logger.debug('Received: (key={}, retry_count={})'.format(
                self.routing_key,
                retry_count,
            ))
            self.func(body)

        except Exception as e:
            if isinstance(e, PermanentFailure):
                self.archive(
                    body,
                    message,
                    "Handler for '{}' raised '{}, {}'\n{}".format(
                        self.routing_key,
                        e.__class__.__name__,
                        e,
                        traceback.format_exc(),
                    )
                )
            elif retry_count >= settings.MAX_RETRIES:
                self.archive(
                    body,
                    message,
                    "Handler for '{}' ran out of retries on exception '{}, {}'\n{}".format(
                        self.routing_key,
                        e.__class__.__name__,
                        e,
                        traceback.format_exc(),
                    )
                )
            else:
                self.retry(
                    body,
                    message,
                    "Handler for '{}' raised the exception '{}, {}', but there are retries "
                    "left\n{}".format(
                        self.routing_key,
                        e.__class__.__name__,
                        e,
                        traceback.format_exc(),
                    )
                )
        else:
            message.ack()
            _logger.debug("Handler for '{}' processed and ack() sent".format(self.routing_key))

        finally:
            if settings.USE_DJANGO:
                # avoid various problems with db connections, due to long-lived
                # worker not automatically participating in Django request lifecycle
                request_finished.send(sender="AMQPRetryHandler")

            if not message.acknowledged:
                message.requeue()
                _logger.critical(
                    "Messages for handler '{}' are not sending an ack() or a reject(). "
                    "This needs attention. Assuming some kind of error and requeueing the "
                    "message.".format(self.routing_key)
                )

    def retry(self, body, message, reason=''):
        """
        Put the message onto the retry queue
        """
        _logger.warning(reason)
        try:
            retry_count = self.retry_count(message)
            headers = message.headers.copy()
            headers.update({
                settings.RETRY_HEADER: retry_count + 1
            })
            self.retry_producer.publish(
                body,
                headers=headers,
                retry=True,
                declares=[self.queues[self.RETRY]],
                expiration=self.backoff_func(retry_count)
            )
        except Exception as e:
            message.requeue()
            _logger.error(
                "Retry failure: retry-reason='{}' exception='{}, {}'\n{}".format(
                    reason,
                    e.__class__.__name__,
                    e,
                    traceback.format_exc(),
                )
            )

        else:
            message.ack()
            _logger.debug("Retry: {}".format(reason))

    def archive(self, body, message, reason=''):
        """
        Put the message onto the archive queue
        """
        try:
            self.archive_producer.publish(
                body,
                headers=message.headers,
                retry=True,
                declares=[self.queues[self.ARCHIVE]],
            )

        except Exception as e:
            message.requeue()
            _logger.error(
                "Archive failure: retry-reason='{}' exception='{}, {}'\n{}".format(
                    reason,
                    e.__class__.__name__,
                    e,
                    traceback.format_exc(),
                )
            )
        else:
            message.ack()
            _logger.debug("Archive: {}".format(reason))

    def declare_queues(self):
        for queue in self.queues.values():
            queue.declare()

    @classmethod
    def retry_count(cls, message):
        return message.headers.get(settings.RETRY_HEADER, 0)

    @staticmethod
    def backoff(retry_count):
        # type: (int) -> float
        """
        Given the number of attempted retries at delivering a message, return
        an increasing TTL for the message for the next retry (in seconds).
        """
        # First retry after 200 ms, then 1s, then 1m, then every 30m
        retry_delay = [0.2, 1, 60, 1800]
        try:
            return retry_delay[retry_count]
        except IndexError:
            return retry_delay[-1]
