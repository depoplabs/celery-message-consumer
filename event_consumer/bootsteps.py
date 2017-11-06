import logging
from typing import Any, Callable, Dict, List  # noqa

import celery
import celery.bootsteps
import kombu

from event_consumer import handlers as ec_handlers
from event_consumer.conf import settings
from event_consumer.types import QueueRegistration  # noqa


logger = logging.getLogger(__name__)


class ConfigureQueuesStep(celery.bootsteps.Step):
    """
    Ensure that worker only listens to the queues that we have registered
    for our message handlers.

    Blueprint type: 'Worker' step

    The celery app you apply it to must implement our `EventConsumerMixin`
    """

    def __init__(self, parent, **kwargs):
        # type: (celery.worker.WorkController, Any) -> None
        super(ConfigureQueuesStep, self).__init__(parent, **kwargs)
        # add any queues passed to worker with `-Q --queues` cli option
        queues = list(parent.app.amqp.queues.keys())
        queues.extend(parent.app.event_handlers.queue_names)
        parent.setup_queues(include=queues)


class AMQPRetryConsumerStep(celery.bootsteps.StartStopStep):
    """
    An integration hook with Celery which is adapted from the built in class
    `bootsteps.ConsumerStep`. Instead of registering a `kombu.Consumer` on
    startup, we create instances of `AMQPRetryHandler` passing in a channel
    which is used to create all the queues/exchanges/etc that are needed to
    implement our try-retry-archive scheme.

    See http://docs.celeryproject.org/en/latest/userguide/extending.html

    Blueprint type: 'Consumer' step

    The celery app you apply it to must implement our `EventConsumerMixin`
    """

    requires = ('celery.worker.consumer:Connection',)

    handlers = None  # type: List[ec_handlers.AMQPRetryHandler]
    _registry = None  # type: Dict[QueueRegistration, Callable[[Any], None]]

    def __init__(self, parent, **kwargs):
        # type: (celery.worker.consumer.Consumer, **Any) -> None
        self.handlers = []
        self._registry = kwargs.pop('tasks', parent.app.event_handlers.registry)
        super(AMQPRetryConsumerStep, self).__init__(parent, **kwargs)

    def start(self, parent):
        # type: (celery.worker.consumer.Consumer) -> None
        channel = parent.connection.channel()
        self.handlers = self.get_handlers(channel)

        # make it all happen:
        for handler in self.handlers:
            handler.declare_queues()
            handler.consumer.consume()

    def stop(self, parent):
        # type: (celery.worker.consumer.Consumer) -> None
        self._close(parent, True)

    def shutdown(self, parent):
        # type: (celery.worker.consumer.Consumer) -> None
        self._close(parent, False)

    def _close(self, parent, cancel_consumers=True):
        # type: (celery.worker.consumer.Consumer, bool) -> None
        channels = set()
        for handler in self.handlers:
            if cancel_consumers:
                kombu.common.ignore_errors(parent.connection, handler.consumer.cancel)
            if handler.consumer.channel:
                channels.add(handler.consumer.channel)
        for channel in channels:
            kombu.common.ignore_errors(parent.connection, channel.close)

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
            for queue_registration, func in self._registry.items()
        ]
