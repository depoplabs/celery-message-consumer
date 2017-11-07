import mock
import pytest

from event_consumer import EventConsumerApp
from event_consumer.bootsteps import AMQPRetryConsumerStep
from event_consumer.conf import settings
from event_consumer.errors import InvalidQueueRegistration
from event_consumer.handlers import AMQPRetryHandler
from event_consumer.test_utils import override_settings
from event_consumer.types import QueueRegistration


@pytest.fixture
def app():
    app = EventConsumerApp()
    app.config_from_object('test_app.celeryconfig')
    return app


@pytest.fixture
def parent(app):
    parent = mock.MagicMock()
    parent.app = app
    return parent


def test_get_handlers_with_defaults(parent):
    """
    Should build handlers from tasks decorated with `@message_handler`
    and use defaults for routing key and exchange if none provided
    """
    app = parent.app
    reg = app.event_handlers.registry

    @app.message_handler('my.routing.key1')
    def f1(body):
        return None

    @app.message_handler('my.routing.key2')
    def f2(body):
        return None

    assert len(reg) == 2

    assert f1 is reg[QueueRegistration('my.routing.key1', 'my.routing.key1', 'default')]
    assert f2 is reg[QueueRegistration('my.routing.key2', 'my.routing.key2', 'default')]

    step = AMQPRetryConsumerStep(parent)
    handlers = step.get_handlers(channel=mock.MagicMock())

    assert len(handlers) == len(reg)

    for handler in handlers:
        assert isinstance(handler, AMQPRetryHandler)
        assert len(handler.consumer.queues) == 1
        assert len(handler.consumer.callbacks) == 1
        assert isinstance(handler.consumer.callbacks[0], AMQPRetryHandler)
        key = (handler.routing_key, handler.queue_name, handler.exchange_key)
        assert handler.consumer.callbacks[0].func is reg[key]


@override_settings(QUEUE_NAME_PREFIX='myapp:')
def test_get_handlers_queue_prefix(parent):
    """
    Should build handlers from tasks decorated with `@message_handler`
    and use defaults for routing key and exchange if none provided
    """
    app = parent.app
    reg = app.event_handlers.registry

    # named exchange is required if using QUEUE_NAME_PREFIX
    with pytest.raises(InvalidQueueRegistration):
        @app.message_handler('my.routing.key1')
        def bad(body):
            return None

    @app.message_handler('my.routing.key1', exchange_key='custom')
    def f1(body):
        return None

    @app.message_handler('my.routing.key2', exchange_key='custom')
    def f2(body):
        return None

    assert len(reg) == 2

    assert f1 is reg[
        QueueRegistration('my.routing.key1', 'myapp:my.routing.key1', 'custom')
    ]
    assert f2 is reg[
        QueueRegistration('my.routing.key2', 'myapp:my.routing.key2', 'custom')
    ]

    step = AMQPRetryConsumerStep(parent)
    handlers = step.get_handlers(channel=mock.MagicMock())

    assert len(handlers) == len(reg)

    for handler in handlers:
        assert isinstance(handler, AMQPRetryHandler)
        assert len(handler.consumer.queues) == 1
        assert len(handler.consumer.callbacks) == 1
        assert isinstance(handler.consumer.callbacks[0], AMQPRetryHandler)
        key = (handler.routing_key, handler.queue_name, handler.exchange_key)
        assert handler.consumer.callbacks[0].func is reg[key]


@override_settings(EXCHANGES={'my.exchange1': {}, 'my.exchange2': {}})
def test_get_handlers_with_queue_and_exchange(parent):
    """
    Should build handlers from tasks decorated with `@message_handler`
    using the specified routing key, queue and exchange
    """
    app = parent.app
    reg = app.event_handlers.registry

    # named exchange is required if using custom queue name
    with pytest.raises(InvalidQueueRegistration):
        @app.message_handler('my.routing.key1', 'my.queue1')
        def bad(body):
            return None

    @app.message_handler('my.routing.key1', 'my.queue1', 'my.exchange1')
    def f1(body):
        return None

    @app.message_handler('my.routing.key2', 'my.queue2', 'my.exchange1')
    def f2(body):
        return None

    # cannot register same queue name on different exchange
    # (must be unique per broker)
    with pytest.raises(InvalidQueueRegistration):
        @app.message_handler('my.routing.key2', 'my.queue2', 'my.exchange2')
        def f3(body):
            return None

    assert len(reg) == 2
    assert f1 is reg[QueueRegistration('my.routing.key1', 'my.queue1', 'my.exchange1')]
    assert f2 is reg[QueueRegistration('my.routing.key2', 'my.queue2', 'my.exchange1')]

    step = AMQPRetryConsumerStep(parent)
    handlers = step.get_handlers(channel=mock.MagicMock())

    assert len(handlers) == len(reg)

    for handler in handlers:
        assert isinstance(handler, AMQPRetryHandler)
        assert len(handler.consumer.queues) == 1
        assert len(handler.consumer.callbacks) == 1
        assert isinstance(handler.consumer.callbacks[0], AMQPRetryHandler)
        key = (handler.routing_key, handler.queue_name, handler.exchange_key)
        assert handler.consumer.callbacks[0].func is reg[key]


@override_settings(EXCHANGES={})
def test_get_handlers_no_exchange(app):
    """
    If a non-default exchange is specified in the `message_handler` decorator
    it must have been configured in the settings.
    """
    # no error for default exchange
    @app.message_handler('my.routing.key1', exchange_key=settings.DEFAULT_EXCHANGE_KEY)
    def f1(body):
        return None

    # error for custom exchange
    with pytest.raises(InvalidQueueRegistration):
        @app.message_handler('my.routing.key1', exchange_key='nonexistent')
        def f2(body):
            return None


def test_get_handlers_same_queue_name_and_exchange(parent):
    """
    Attempt to attach handler with same queue name + exchange should fail.
    """
    app = parent.app
    reg = app.event_handlers.registry

    @app.message_handler('my.routing.key1', 'custom_queue', 'custom')
    def f1(body):
        return None

    with pytest.raises(InvalidQueueRegistration):
        @app.message_handler('my.routing.key2', 'custom_queue', 'custom')
        def f2(body):
            return None

    assert len(reg) == 1

    assert f1 is reg[QueueRegistration('my.routing.key1', 'custom_queue', 'custom')]

    step = AMQPRetryConsumerStep(parent)
    handlers = step.get_handlers(channel=mock.MagicMock())

    assert len(handlers) == len(reg)

    for handler in handlers:
        assert isinstance(handler, AMQPRetryHandler)
        assert len(handler.consumer.queues) == 1
        assert len(handler.consumer.callbacks) == 1
        assert isinstance(handler.consumer.callbacks[0], AMQPRetryHandler)
        key = (handler.routing_key, handler.queue_name, handler.exchange_key)
        assert handler.consumer.callbacks[0].func is reg[key]


@override_settings(EXCHANGES={'my.exchange1': {}, 'my.exchange2': {}})
def test_get_handlers_with_multiple_routes(parent):
    """
    Should build handlers from tasks decorated with `@message_handler`
    using the specified routing key, queue and exchange
    """
    app = parent.app
    reg = app.event_handlers.registry

    # custom queue name is not possible with multiple routes, even with named exchange
    with pytest.raises(InvalidQueueRegistration):
        @app.message_handler(['my.routing.key1', 'my.routing.key2'], 'my.queue1', 'my.exchange1')
        def bad(body):
            return None

    @app.message_handler(['my.routing.key1', 'my.routing.key2'])
    def f1(body):
        return None

    assert len(reg) == 2
    assert f1 is reg[QueueRegistration('my.routing.key1', 'my.routing.key1', 'default')]
    assert f1 is reg[QueueRegistration('my.routing.key2', 'my.routing.key2', 'default')]

    step = AMQPRetryConsumerStep(parent)
    handlers = step.get_handlers(channel=mock.MagicMock())

    assert len(handlers) == len(reg)

    for handler in handlers:
        assert isinstance(handler, AMQPRetryHandler)
        assert len(handler.consumer.queues) == 1
        assert len(handler.consumer.callbacks) == 1
        assert isinstance(handler.consumer.callbacks[0], AMQPRetryHandler)
        key = (handler.routing_key, handler.queue_name, handler.exchange_key)
        assert handler.consumer.callbacks[0].func is reg[key]


def test_registered_queue_names(app):
    """
    We store a registry of (primary) queue names on the app object.
    """
    reg = app.event_handlers.registry
    queue_names = app.event_handlers.queue_names

    @app.message_handler('my.routing.key1')
    def f1(body):
        return None

    assert len(reg) == 1
    assert len(queue_names) == 1

    assert queue_names == {'my.routing.key1'}
