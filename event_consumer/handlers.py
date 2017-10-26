"""
Apparatus for consuming 'vanilla' AMQP messages (i.e. not Celery tasks) making use
of the battle-tested `bin/celery worker` utility rather than writing our own.

NOTE:
We don't access any Celery config in this file. That is because the config is
loaded by the bin/celery worker itself, according to usual mechanism.
"""
import logging
import traceback

from typing import Any, Callable, Dict, List, Optional  # noqa

import celery.bootsteps as bootsteps
import kombu
import kombu.message
import kombu.common as common

from event_consumer.conf import settings
from event_consumer.errors import NoExchange, PermanentFailure
from event_consumer.types import QueueRegistration

if settings.USE_DJANGO:
    from django.core.signals import request_finished


_logger = logging.getLogger(__name__)


# Maps routing-keys to callables
REGISTRY = {}  # type: Dict[str, Callable[[Dict[str, Any]], None]]

DEFAULT_EXCHANGE = 'default'


def message_handler(routing_key, queue=None, exchange=DEFAULT_EXCHANGE):
    """
    Register a function as a handler for messages on a rabbitmq exchange with
    the given routing-key. Default behaviour is to use `routing_key` as the
    queue name and attach it to the 'data' topic exchange

    Kwargs:
        routing_key (str): The routing key of messages to be handled by the
            decorated task.
        queue (Optional[str]): The name of the main queue from which messages
            will be consumed. Defaults to `routing_key` if not supplied.
        exchange (str): The AMQP exchange config to use. This is a *key name*
            in the `settings.EXCHANGES` dict.

    Returns:
        Callable[[Callable], Any]: function decorator

    Usage:
        @message_handler('my.routing.key', 'my.queue', 'my.exchange')
        def process_message(body):
            print(body)  # Whatever

    Note that this is an import side-effect (as is Celery's @task decorator).
    In order for the event handler to be registered, its containing module must
    be imported before starting the AMQPRetryConsumerStep.
    """
    def decorator(f):
        global REGISTRY
        queue_name = routing_key if queue is None else queue
        register_key = QueueRegistration(routing_key, queue_name, exchange)
        existing = REGISTRY.get(register_key, None)
        if existing is not None and existing is not f:
            raise Exception(
                'Multiple registrations for messages with the routing key '
                '"{0}" queue "{1}" and exchange "{2}"'.format(
                    routing_key,
                    queue,
                    exchange,
                )
            )
        REGISTRY[register_key] = f
        return f

    return decorator


class AMQPRetryConsumerStep(bootsteps.StartStopStep):
    """
    This an integration hook with Celery adapted from the built in class
    `bootsteps.ConsumerStep`. Instead of registering a `kombu.Consumer` on
    startup, we create instances of `AMQPRetryHandler` passing in a channel
    which is used to create all the queues/exchanges/etc. needed to
    implement our try-retry-archive scheme.

    See http://docs.celeryproject.org/en/latest/userguide/extending.html
    """

    requires = ('celery.worker.consumer:Connection', )

    def __init__(self, *args, **kwargs):
        self.handlers = []
        self._tasks = kwargs.pop('tasks', REGISTRY)
        super(AMQPRetryConsumerStep, self).__init__(*args, **kwargs)

    def start(self, c):
        channel = c.connection.channel()
        self.handlers = self.get_handlers(channel)

        for handler in self.handlers:
            handler.declare_queues()
            handler.consumer.consume()

    def stop(self, c):
        self._close(c, True)

    def shutdown(self, c):
        self._close(c, False)

    def _close(self, c, cancel_consumers=True):
        channels = set()
        for handler in self.handlers:
            if cancel_consumers:
                common.ignore_errors(c.connection, handler.consumer.cancel)
            if handler.consumer.channel:
                channels.add(handler.consumer.channel)
        for channel in channels:
            common.ignore_errors(c.connection, channel.close)

    def get_handlers(self, channel):
        return [
            AMQPRetryHandler(
                channel,
                queue_registration.routing_key,
                queue_registration.queue,
                queue_registration.exchange,
                func,
                backoff_func=settings.BACKOFF_FUNC,
            )
            for queue_registration, func in self._tasks.items()
        ]


class AMQPRetryHandler(object):
    """
    This implements Depop's try-retry-archive message queue pattern.

    Briefly - messages are processed and may be retried by placing them on a separate retry
    queue on a dead-letter-exchange. Messages on the DLX are automatically re-queued by Rabbit
    once they expire. The expiry is set on a message-by-message basis to allow exponential
    backoff on retries.
    """

    def __init__(self, channel, routing_key, queue, exchange, func, backoff_func=None):
        self.channel = channel
        self.routing_key = routing_key
        self.queue = queue
        self.exchange = exchange
        self.func = func
        self.backoff_func = backoff_func or self.backoff

        self.exchanges = {
            DEFAULT_EXCHANGE: kombu.Exchange(channel=self.channel)
        }

        for name, exchange_settings in settings.EXCHANGES.items():
            self.exchanges[name] = kombu.Exchange(
                channel=self.channel,
                **exchange_settings
            )

        try:
            self.worker_queue = kombu.Queue(
                name=self.queue,
                exchange=self.exchanges[exchange],
                routing_key=self.routing_key,
                channel=self.channel,
            )

            self.retry_queue = kombu.Queue(
                name='{0}.retry'.format(queue),
                exchange=self.exchanges[DEFAULT_EXCHANGE],
                routing_key='{0}.retry'.format(queue),
                # N.B. default exchange automatically routes messages to a queue
                # with the same name as the routing key provided.
                queue_arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": self.queue,
                },
                channel=self.channel,
            )

            self.archive_queue = kombu.Queue(
                name='{0}.archived'.format(queue),
                exchange=self.exchanges[DEFAULT_EXCHANGE],
                routing_key='{0}.archived'.format(queue),
                queue_arguments={
                    "x-expires": settings.ARCHIVE_EXPIRY,  # Messages dropped after this
                    "x-max-length": 1000000,  # Maximum size of the queue
                    "x-queue-mode": "lazy",  # Keep messages on disk (reqs. rabbitmq 3.6.0+)
                },
                channel=self.channel,
            )
        except KeyError as key_exc:
            raise NoExchange(
                "The exchange {0} was not found in settings.EXCHANGES. \n"
                "settings.EXCHANGES = {1}".format(
                    key_exc,
                    settings.EXCHANGES
                )
            )

        self.retry_producer = kombu.Producer(
            channel,
            exchange=self.retry_queue.exchange,
            routing_key=self.retry_queue.routing_key,
            serializer=settings.SERIALIZER,
        )

        self.archive_producer = kombu.Producer(
            channel,
            exchange=self.archive_queue.exchange,
            routing_key=self.archive_queue.routing_key,
            serializer=settings.SERIALIZER,
        )

        self.consumer = kombu.Consumer(
            channel,
            queues=[self.worker_queue],
            callbacks=[self],
            accept=settings.ACCEPT,
        )

    def __call__(self, body, message):
        """
        This is the main Kombu message handler for vanilla AMQP messages.

        Raising an exception in this method will crash the Celery worker. Ensure
        that all Exceptions are caught and messages acknowledged or rejected
        as they are processed.

        Args:
            body (Any): the message content, which has been deserialized by Celery
            message (kombu.message.Message)

        Returns:
            None
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
                declares=[self.retry_queue],
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
                declares=[self.archive_queue],
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
        queues = [self.worker_queue, self.retry_queue, self.archive_queue]
        for queue in queues:
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
