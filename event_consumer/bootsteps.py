from typing import Any, Callable, Dict, List  # noqa

import celery.bootsteps as bootsteps
import kombu  # noqa
import kombu.common as common

from event_consumer import handlers as ec_handlers
from event_consumer.conf import settings
from event_consumer.types import QueueRegistration  # noqa


class AMQPRetryConsumerStep(bootsteps.StartStopStep):
    """
    An integration hook with Celery which is adapted from the built in class
    `bootsteps.ConsumerStep`. Instead of registering a `kombu.Consumer` on
    startup, we create instances of `AMQPRetryHandler` passing in a channel
    which is used to create all the queues/exchanges/etc. needed to
    implement our try-retry-archive scheme.

    See http://docs.celeryproject.org/en/latest/userguide/extending.html
    """

    requires = ('celery.worker.consumer:Connection',)

    handlers = None  # type: List[ec_handlers.AMQPRetryHandler]
    _tasks = None  # type: Dict[QueueRegistration, Callable[[Any], None]]

    def __init__(self, *args, **kwargs):
        self.handlers = []
        self._tasks = kwargs.pop('tasks', ec_handlers.REGISTRY)
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

    # custom methods:
    def get_handlers(self, channel):
        # type: (kombu.transport.base.StdChannel) -> List[ec_handlers.AMQPRetryHandler]
        return [
            ec_handlers.AMQPRetryHandler(
                channel,
                queue_registration.routing_key,
                queue_registration.queue_name,
                queue_registration.exchange_key,
                func,
                backoff_func=settings.BACKOFF_FUNC,
            )
            for queue_registration, func in self._tasks.items()
        ]
