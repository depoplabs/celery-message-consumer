"""
Apparatus for consuming 'vanilla' AMQP messages (i.e. not Celery tasks) making use
of the battle-tested `bin/celery worker` utility rather than writing our own.

NOTE:
We don't access any Celery config in this file. That is because the config is
loaded by the bin/celery worker itself, according to usual mechanism.
"""
import logging
import traceback

import six
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Type, Union  # noqa

import amqp  # noqa
import kombu
import kombu.message

from event_consumer.conf import settings
from event_consumer.errors import InvalidQueueRegistration, NoExchange, PermanentFailure
from event_consumer.types import QueueRegistration, UnboundQueue

if settings.USE_DJANGO:
    from django.core.signals import request_finished


_logger = logging.getLogger(__name__)


# Maps routing-keys to handlers
REGISTRY = {}  # type: Dict[QueueRegistration, Callable]

# For use by celeryconfig
UNBOUND_QUEUES = []  # type: List[UnboundQueue]

DEFAULT_EXCHANGE = 'default'  # key in settings.EXCHANGES


def _validate_registration(register_key):  # type: (QueueRegistration) -> None
    """
    Raises:
        InvalidQueueRegistration
    """
    global REGISTRY
    existing = {(r.queue_name, r.exchange_key) for r in REGISTRY.keys()}
    if (register_key.queue_name, register_key.exchange_key) in existing:
        raise InvalidQueueRegistration(
            'Attempted duplicate registrations for messages with the queue name '
            '"{0}" and exchange "{1}"'.format(
                register_key.queue_name,
                register_key.exchange_key,
            )
        )


def message_handler(routing_keys,  # type: Union[str, Iterable]
                    queue=None,  # type: Optional[str]
                    exchange=DEFAULT_EXCHANGE  # type: str
                    ):
    # type: (...) ->  Callable[[Callable], Any]
    """
    Register a function as a handler for messages on a rabbitmq exchange with
    the given routing-key. Default behaviour is to use `routing_key` as the
    queue name and attach it to the 'default' exchange. If this key is not
    present in `settings.EXCHANGES` with your own config then you will get the
    underlying AMQP default exchange - this has some restrictions (you cannot
    bind custom queue names, only auto-bound same-as-routing-key queues are
    possible).Kwargs

    Otherwise Queues and Exchanges are automatically created on the broker
    by Kombu and you don't have to worry about it.

    Kwargs:
        routing_keys: The routing key/s of messages to be handled by the
            decorated task.
        queue: The name of the main queue from which messages
            will be consumed. Defaults to '{QUEUE_NAME_PREFIX}{routing_key}`
            if not supplied. Special case is '' this will give you Kombu
            default queue name without prepending `QUEUE_NAME_PREFIX`.
        exchange: The AMQP exchange config to use. This is a *key name*
            in the `settings.EXCHANGES` dict.

    Returns:
        Callable: function decorator

    Raises:
        InvalidQueueRegistration

    Usage:
        @message_handler('my.routing.key', 'my.queue', 'my.exchange')
        def process_message(body):
            print(body)  # Whatever

    Note that this is an import side-effect (as is Celery's @task decorator).
    In order for the event handler to be registered, its containing module must
    be imported before starting the AMQPRetryConsumerStep.
    """
    if (queue or (queue is None and settings.QUEUE_NAME_PREFIX)) \
            and exchange not in settings.EXCHANGES:
        raise InvalidQueueRegistration(
            "You must use a named exchange from settings.EXCHANGES "
            "if you want to bind a custom queue name."
        )

    if isinstance(routing_keys, six.string_types):
        routing_keys = [routing_keys]
    else:
        if queue is not None:
            raise InvalidQueueRegistration(
                "We need a queue-per-routing-key so you can't specify a "
                "custom queue name when attaching mutiple routes. Use "
                "separate handlers for each routing key in this case."
            )

    def decorator(f):  # type: (Callable) -> Callable
        global REGISTRY, UNBOUND_QUEUES

        for routing_key in routing_keys:
            queue_name = (settings.QUEUE_NAME_PREFIX + routing_key) if queue is None else queue

            # kombu.Consumer has no concept of routing-key (only queue name) so
            # handler registrations must be unique on queue+exchange (otherwise
            # messages from the queue would be randomly sent to the duplicate
            # handlers)
            register_key = QueueRegistration(routing_key, queue_name, exchange)
            _validate_registration(register_key)

            REGISTRY[register_key] = f

            UNBOUND_QUEUES.extend(
                AMQPRetryHandler.unbound_queues_for(
                    queue_name=queue_name,
                    routing_key=routing_key,
                    exchange_key=exchange,
                )
            )

        return f

    return decorator


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
            if key_name == DEFAULT_EXCHANGE:
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
                     queue_cls=None  # type: Optional[Type[kombu.Queue]]
                     ):
        # type: (...) -> Dict[str, Union[kombu.Queue, UnboundQueue]]
        queues = {}

        queue_cls = queue_cls or kombu.Queue

        primary_name = cls.QUEUE_NAME_FORMATS[cls.WORKER].format(queue_name)
        queues[cls.WORKER] = queue_cls(
            name=cls.QUEUE_NAME_FORMATS[cls.WORKER].format(queue_name),
            exchange=exchange,
            routing_key=routing_key,
        )

        retry_name = cls.QUEUE_NAME_FORMATS[cls.RETRY].format(queue_name)
        queues[cls.RETRY] = queue_cls(
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
        queues[cls.ARCHIVE] = queue_cls(
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

    @classmethod
    def unbound_queues_for(cls, queue_name, routing_key, exchange_key):
        # type: (str, str, str) -> List[UnboundQueue]
        exchange = cls.setup_exchange(exchange_key)
        queues = cls.setup_queues(
            queue_name=queue_name,
            routing_key=routing_key,
            exchange=exchange,
            queue_cls=UnboundQueue,
        )
        return list(queues.values())

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
                    "Task '{}' raised '{}, {}'\n{}".format(
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
                    "Task '{}' ran out of retries on exception '{}, {}'\n{}".format(
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
                    "Task '{}' raised the exception '{}, {}', but there are retries left\n{}".format(
                        self.routing_key,
                        e.__class__.__name__,
                        e,
                        traceback.format_exc(),
                    )
                )
        else:
            message.ack()
            _logger.debug("Task '{}' processed and ack() sent".format(self.routing_key))

        finally:
            if settings.USE_DJANGO:
                # avoid various problems with db connections, due to long-lived
                # worker not automatically participating in Django request lifecycle
                request_finished.send(sender="AMQPRetryHandler")

            if not message.acknowledged:
                message.requeue()
                _logger.critical(
                    "Messages for task '{}' are not sending an ack() or a reject(). "
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
