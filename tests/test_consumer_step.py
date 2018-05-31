import mock
import pytest

from flexisettings.utils import override_settings

from event_consumer import message_handler
from event_consumer import handlers as ec
from event_consumer.conf import settings
from event_consumer.errors import InvalidQueueRegistration
from event_consumer.types import QueueRegistration


def test_get_handlers_with_defaults():
    """
    Should build handlers from tasks decorated with `@message_handler`
    and use defaults for routing key and exchange if none provided
    """
    with mock.patch.object(ec, 'REGISTRY', new=dict()) as reg:

        @message_handler('my.routing.key1')
        def f1(body):
            return None

        @message_handler('my.routing.key2')
        def f2(body):
            return None

        assert len(reg) == 2

        assert f1 is reg[QueueRegistration('my.routing.key1', 'my.routing.key1', 'default')]
        assert f2 is reg[QueueRegistration('my.routing.key2', 'my.routing.key2', 'default')]

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = (handler.routing_key, handler.queue, handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key]


@override_settings(settings, QUEUE_NAME_PREFIX='myapp:')
def test_get_handlers_queue_prefix(*mocks):
    """
    Should build handlers from tasks decorated with `@message_handler`
    and use defaults for routing key and exchange if none provided
    """
    with mock.patch.object(ec, 'REGISTRY', new=dict()) as reg:

        # named exchange is required if using QUEUE_NAME_PREFIX
        with pytest.raises(InvalidQueueRegistration):
            @message_handler('my.routing.key1')
            def bad(body):
                return None

        @message_handler('my.routing.key1', exchange='custom')
        def f1(body):
            return None

        @message_handler('my.routing.key2', exchange='custom')
        def f2(body):
            return None

        assert len(reg) == 2

        assert f1 is reg[
            QueueRegistration('my.routing.key1', 'myapp:my.routing.key1', 'custom')
        ]
        assert f2 is reg[
            QueueRegistration('my.routing.key2', 'myapp:my.routing.key2', 'custom')
        ]

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = (handler.routing_key, handler.queue, handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key]


@override_settings(settings, EXCHANGES={'my.exchange1': {}, 'my.exchange2': {}})
def test_get_handlers_with_queue_and_exchange(*mocks):
    """
    Should build handlers from tasks decorated with `@message_handler`
    using the specified routing key, queue and exchange
    """
    with mock.patch.object(ec, 'REGISTRY', new=dict()) as reg:

        # named exchange is required if using custom queue name
        with pytest.raises(InvalidQueueRegistration):
            @message_handler('my.routing.key1', 'my.queue1')
            def bad(body):
                return None

        @message_handler('my.routing.key1', 'my.queue1', 'my.exchange1')
        def f1(body):
            return None

        @message_handler('my.routing.key2', 'my.queue2', 'my.exchange1')
        def f2(body):
            return None

        # can register same queue name on different exchange
        @message_handler('my.routing.key2', 'my.queue2', 'my.exchange2')
        def f3(body):
            return None

        assert len(reg) == 3
        assert f1 is reg[QueueRegistration('my.routing.key1', 'my.queue1', 'my.exchange1')]
        assert f2 is reg[QueueRegistration('my.routing.key2', 'my.queue2', 'my.exchange1')]
        assert f3 is reg[QueueRegistration('my.routing.key2', 'my.queue2', 'my.exchange2')]

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = (handler.routing_key, handler.queue, handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key]


def test_get_handlers_no_exchange():
    """
    If an exchange is specified in the `message_handler` decorator it
    must have been configured in the settings.
    """
    with mock.patch.object(ec, 'REGISTRY', new=dict()):

        @message_handler('my.routing.key1', exchange='nonexistent')
        def f1(body):
            return None

        with pytest.raises(ec.NoExchange):
            step = ec.AMQPRetryConsumerStep(None)
            step.get_handlers(channel=mock.MagicMock())


def test_get_handlers_same_queue_name_and_exchange():
    """
    Attempt to attach handler with same queue name + exchange should fail.
    """
    with mock.patch.object(ec, 'REGISTRY', new=dict()) as reg:

        @message_handler('my.routing.key1', queue='custom_queue', exchange='custom')
        def f1(body):
            return None

        with pytest.raises(InvalidQueueRegistration):
            @message_handler('my.routing.key2', queue='custom_queue', exchange='custom')
            def f2(body):
                return None

        assert len(reg) == 1

        assert f1 is reg[QueueRegistration('my.routing.key1', 'custom_queue', 'custom')]

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = (handler.routing_key, handler.queue, handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key]


@override_settings(settings, EXCHANGES={'my.exchange1': {}, 'my.exchange2': {}})
def test_get_handlers_with_multiple_routes(*mocks):
    """
    Should build handlers from tasks decorated with `@message_handler`
    using the specified routing key, queue and exchange
    """
    with mock.patch.object(ec, 'REGISTRY', new=dict()) as reg:

        # custom queue name is not possible with multiple routes, even with named exchange
        with pytest.raises(InvalidQueueRegistration):
            @message_handler(['my.routing.key1', 'my.routing.key2'], 'my.queue1', 'my.exchange1')
            def bad(body):
                return None

        @message_handler(['my.routing.key1', 'my.routing.key2'])
        def f1(body):
            return None

        assert len(reg) == 2
        assert f1 is reg[QueueRegistration('my.routing.key1', 'my.routing.key1', 'default')]
        assert f1 is reg[QueueRegistration('my.routing.key2', 'my.routing.key2', 'default')]

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = (handler.routing_key, handler.queue, handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key]
