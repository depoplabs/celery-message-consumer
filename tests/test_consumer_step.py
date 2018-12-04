import mock
import pytest

from flexisettings.utils import override_settings

from event_consumer import message_handler
from event_consumer import handlers as ec
from event_consumer.conf import settings
from event_consumer.errors import InvalidQueueRegistration
from event_consumer.types import QueueKey


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

        handler_reg1 = reg[QueueKey(queue='my.routing.key1', exchange='default')]
        assert handler_reg1.handler is f1
        assert handler_reg1.routing_key == 'my.routing.key1'
        assert handler_reg1.queue_arguments == {}

        handler_reg2 = reg[QueueKey(queue='my.routing.key2', exchange='default')]
        assert handler_reg2.handler is f2
        assert handler_reg2.routing_key == 'my.routing.key2'
        assert handler_reg2.queue_arguments == {}

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = QueueKey(queue=handler.queue, exchange=handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key].handler


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

        handler_reg1 = reg[
            QueueKey(queue='myapp:my.routing.key1', exchange='custom')
        ]
        assert handler_reg1.handler is f1
        assert handler_reg1.routing_key == 'my.routing.key1'
        assert handler_reg1.queue_arguments == {}

        handler_reg2 = reg[
            QueueKey(queue='myapp:my.routing.key2', exchange='custom')
        ]
        assert handler_reg2.handler is f2
        assert handler_reg2.routing_key == 'my.routing.key2'
        assert handler_reg2.queue_arguments == {}

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = QueueKey(queue=handler.queue, exchange=handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key].handler


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

        handler_reg1 = reg[
            QueueKey(queue='my.queue1', exchange='my.exchange1')
        ]
        assert handler_reg1.handler is f1
        assert handler_reg1.routing_key == 'my.routing.key1'
        assert handler_reg1.queue_arguments == {}

        handler_reg2 = reg[
            QueueKey(queue='my.queue2', exchange='my.exchange1')
        ]
        assert handler_reg2.handler is f2
        assert handler_reg2.routing_key == 'my.routing.key2'
        assert handler_reg2.queue_arguments == {}

        handler_reg3 = reg[
            QueueKey(queue='my.queue2', exchange='my.exchange2')
        ]
        assert handler_reg3.handler is f3
        assert handler_reg3.routing_key == 'my.routing.key2'
        assert handler_reg3.queue_arguments == {}

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = QueueKey(queue=handler.queue, exchange=handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key].handler


def test_get_handlers_with_queue_arguments():
    """
    Should build handlers from tasks decorated with `@message_handler`
    and pass the `queue_arguments` through to `kombu.Queue` constructor.
    """
    with mock.patch.object(ec, 'REGISTRY', new=dict()) as reg:

        @message_handler('my.routing.key1', queue_arguments={'x-fake-header': 'wtf'})
        def f1(body):
            return None

        handler_reg1 = reg[QueueKey(queue='my.routing.key1', exchange='default')]
        assert handler_reg1.handler is f1
        assert handler_reg1.routing_key == 'my.routing.key1'
        assert handler_reg1.queue_arguments == {'x-fake-header': 'wtf'}

        with mock.patch('kombu.Queue'):
            step = ec.AMQPRetryConsumerStep(None)
            handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(reg) == 1
        assert len(handlers) == len(reg)

        handler = handlers[0]
        assert isinstance(handler, ec.AMQPRetryHandler)
        assert isinstance(handler.worker_queue, mock.MagicMock)
        handler.worker_queue.queue_arguments = {'x-fake-header': 'wtf'}


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

        handler_reg1 = reg[
            QueueKey(queue='custom_queue', exchange='custom')
        ]
        assert handler_reg1.handler is f1
        assert handler_reg1.routing_key == 'my.routing.key1'
        assert handler_reg1.queue_arguments == {}

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = QueueKey(queue=handler.queue, exchange=handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key].handler


@override_settings(settings, EXCHANGES={'my.exchange1': {}, 'my.exchange2': {}})
def test_get_handlers_with_multiple_routes(*mocks):
    """
    Can connect the handler to multiple routing keys, each having a queue.
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

        handler_reg1 = reg[
            QueueKey(queue='my.routing.key1', exchange='default')
        ]
        assert handler_reg1.handler is f1
        assert handler_reg1.routing_key == 'my.routing.key1'
        assert handler_reg1.queue_arguments == {}

        handler_reg2 = reg[
            QueueKey(queue='my.routing.key2', exchange='default')
        ]
        assert handler_reg2.handler is f1
        assert handler_reg2.routing_key == 'my.routing.key2'
        assert handler_reg2.queue_arguments == {}

        step = ec.AMQPRetryConsumerStep(None)
        handlers = step.get_handlers(channel=mock.MagicMock())

        assert len(handlers) == len(reg)

        for handler in handlers:
            assert isinstance(handler, ec.AMQPRetryHandler)
            assert len(handler.consumer.queues) == 1
            assert len(handler.consumer.callbacks) == 1
            assert isinstance(handler.consumer.callbacks[0], ec.AMQPRetryHandler)
            key = QueueKey(queue=handler.queue, exchange=handler.exchange)
            assert handler.consumer.callbacks[0].func is reg[key].handler
