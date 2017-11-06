import logging
import six
from typing import Any, Callable, Dict, Iterable, Optional, Set, Union  # noqa

from celery import Celery

from event_consumer import bootsteps
from event_consumer.conf import settings
from event_consumer.errors import InvalidQueueRegistration
from event_consumer.types import QueueRegistration, Nonlocal


logger = logging.getLogger(__name__)


class EventHandlers(object):

    # Maps routing-keys to handlers
    registry = None  # type: Dict[QueueRegistration, Callable]

    # For use by `event_consumer.bootsteps.ConfigureQueuesStep`
    queue_names = None  # type: Set[str]

    def __init__(self):
        self.registry = {}
        self.queue_names = set()


class EventConsumerAppMixin(object):

    event_handlers = None  # type: EventHandlers

    def __init__(self, *args, **kwargs):
        super(EventConsumerAppMixin, self).__init__(*args, **kwargs)

        self.event_handlers = EventHandlers()

        self.steps['worker'].add(bootsteps.ConfigureQueuesStep)
        self.steps['consumer'].add(bootsteps.AMQPRetryConsumerStep)

    def message_handler(self,
                        routing_keys,  # type: Union[str, Iterable]
                        queue_name=None,  # type: Optional[str]
                        exchange_key=settings.DEFAULT_EXCHANGE_KEY  # type: str
                        ):
        # type: (...) ->  Callable[[Callable], Any]
        """
        Register a function as a handler for messages on a rabbitmq exchange
        with the given routing-key. (Custom method: Not part of Celery API)

        Default behaviour is to use `routing_key` as the queue name and bind
        it to the 'default' exchange. If this key is not present in
        `settings.EXCHANGES` with your own config then you will get the
        underlying AMQP default exchange - this has some restrictions (you
        cannot bind custom queue names, only auto-bound same-as-routing-key
        queues are possible).

        Otherwise Queues and Exchanges are automatically created on the broker
        by Kombu and you don't have to worry about it.

        Kwargs:
            routing_keys: The routing key/s of messages to be handled by the
                decorated task.
            queue_name: The name of the main queue from which messages
                will be consumed. Defaults to '{QUEUE_NAME_PREFIX}{routing_key}`
                if not supplied. Special case is '' - this will give you Kombu
                default queue name without prepending `QUEUE_NAME_PREFIX`.
            exchange_key: The AMQP exchange config to use. This is a *key name*
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
        if (queue_name or (queue_name is None and settings.QUEUE_NAME_PREFIX)) \
                and exchange_key not in settings.EXCHANGES:
            raise InvalidQueueRegistration(
                "You must use a named exchange from settings.EXCHANGES "
                "if you want to bind a custom queue name."
            )

        if (exchange_key != settings.DEFAULT_EXCHANGE_KEY and
                exchange_key not in settings.EXCHANGES):
            raise InvalidQueueRegistration(
                "Custom exchange config key '{}' not found in "
                "settings.EXCHANGES".format(exchange_key)
            )

        if isinstance(routing_keys, six.string_types):
            routing_keys = [routing_keys]
        else:
            if queue_name is not None:
                raise InvalidQueueRegistration(
                    "We need a queue-per-routing-key so you can't specify a "
                    "custom queue name when attaching mutiple routes. Use "
                    "separate handlers for each routing key in this case."
                )

        nonlocals = Nonlocal(queue_name=queue_name)

        def decorator(f):  # type: (Callable) -> Callable
            for routing_key in routing_keys:
                if nonlocals.queue_name is None:
                    queue_name = (settings.QUEUE_NAME_PREFIX + routing_key)
                else:
                    queue_name = nonlocals.queue_name

                # kombu.Consumer has no concept of routing-key (only queue name) so
                # handler registrations must be unique on queue name (otherwise
                # messages from the queue would be randomly sent to the duplicate
                # handlers)
                register_key = QueueRegistration(routing_key, queue_name, exchange_key)

                # we only need the primary queue, we won't be consuming the 'retry'
                # or 'archive' queues
                if queue_name in self.event_handlers.queue_names:
                    raise InvalidQueueRegistration(
                        "Queue name '{}' is already registered".format(queue_name)
                    )
                self.event_handlers.queue_names.add(queue_name)

                self.event_handlers.registry[register_key] = f

            return f

        return decorator


class EventConsumerApp(EventConsumerAppMixin, Celery):
    pass
