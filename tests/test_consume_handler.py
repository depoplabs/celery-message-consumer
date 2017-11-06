import socket

from flaky import flaky
import mock

from .base import BaseConsumerIntegrationTest


class ConsumeMessageHandlerTest(BaseConsumerIntegrationTest):

    @flaky(max_runs=5, min_passes=5)
    def test_consume_basic(self):
        """
        Should run the wrapped function when a message arrives with its routing key.
        """
        reg = self.app.event_handlers.registry

        f1 = self.app.message_handler('my.routing.key1')(
            mock.MagicMock()
        )
        f2 = self.app.message_handler('my.routing.key2')(
            mock.MagicMock()
        )

        assert len(reg) == 2

        self.configure_handlers()

        assert len(self.handlers) == len(reg)

        h1 = self.get_handlers_for_key('my.routing.key1')[0]
        h2 = self.get_handlers_for_key('my.routing.key2')[0]

        p1 = self.get_producer(h1)
        p2 = self.get_producer(h2)
        body1 = self.body()
        body2 = self.body()

        p1.publish(body1)
        p2.publish(body2)
        for _ in range(2):
            self.connection.drain_events(timeout=0.3)

        f1.assert_called_once_with(body1)
        f2.assert_called_once_with(body2)

        # no retries:
        e = None
        try:
            self.connection.drain_events(timeout=0.3)
        except socket.timeout as exc:
            e = exc
        self.assertIsNotNone(e, msg="e=None here means task was unexpectedly retried")
        # no further calls
        f1.call_count = 1
        f2.call_count = 1

    @flaky(max_runs=5, min_passes=5)
    def test_consume_custom_queue_name(self):
        """
        Should run the wrapped function when a message arrives with its routing key.
        Test that we can connect multiple routing keys on the same queue and the
        appropriate handler will be called in each case.
        """
        reg = self.app.event_handlers.registry

        # we have to use a named exchange to be able to bind a custom queue name
        decorator = self.app.message_handler(
            'my.routing.key1',
            queue_name='custom_queue',
            exchange_key='custom',
        )
        f1 = decorator(
            mock.MagicMock()
        )

        assert len(reg) == 1

        self.configure_handlers()

        assert len(self.handlers) == len(reg)

        h1 = self.get_handlers_for_key('my.routing.key1')[0]

        p1 = self.get_producer(h1)
        body1 = self.body()

        p1.publish(body1)
        self.connection.drain_events(timeout=0.3)

        f1.assert_called_once_with(body1)

        # no retries:
        e = None
        try:
            self.connection.drain_events(timeout=0.3)
        except socket.timeout as exc:
            e = exc
        self.assertIsNotNone(e, msg="e=None here means task was unexpectedly retried")
        # no further calls
        f1.call_count = 1

    @flaky(max_runs=5, min_passes=5)
    def test_consume_wildcard_route(self):
        """
        Should run the wrapped function when a message arrives with its routing key.
        Test that we can connect multiple routing keys on the same queue and the
        appropriate handler will be called in each case.
        """
        reg = self.app.event_handlers.registry

        f1 = self.app.message_handler('my.routing.*', exchange_key='custom')(
            mock.MagicMock()
        )

        assert len(reg) == 1

        self.configure_handlers()

        assert len(self.handlers) == len(reg)

        h1 = self.get_handlers_for_key('my.routing.*')[0]

        p1 = self.get_producer(h1, 'my.routing.key1')
        p2 = self.get_producer(h1, 'my.routing.key2')
        body1 = self.body()
        body2 = self.body()

        p1.publish(body1)
        p2.publish(body2)
        for _ in range(2):
            self.connection.drain_events(timeout=0.3)

        f1.assert_has_calls([mock.call(body1), mock.call(body2)], any_order=True)

        # no retries:
        e = None
        try:
            self.connection.drain_events(timeout=0.3)
        except socket.timeout as exc:
            e = exc
        self.assertIsNotNone(e, msg="e=None here means task was unexpectedly retried")
        # no further calls
        f1.call_count = 2

    @flaky(max_runs=5, min_passes=5)
    def test_consume_multiple_routes(self):
        """
        Should run the wrapped function when a message arrives with its routing key.
        Test that we can connect multiple routing keys on the same handler. Underlying
        implementation is currently queue-per-routing key.
        """
        reg = self.app.event_handlers.registry

        decorator = self.app.message_handler(
            ['my.routing.key1', 'my.routing.key2'],
            exchange_key='custom',
        )
        f1 = decorator(mock.MagicMock())

        assert len(reg) == 2

        self.configure_handlers()

        assert len(self.handlers) == len(reg)

        h1 = self.get_handlers_for_key('my.routing.key1')[0]
        h2 = self.get_handlers_for_key('my.routing.key2')[0]

        p1 = self.get_producer(h1)
        p2 = self.get_producer(h2)
        body1 = self.body()
        body2 = self.body()

        p1.publish(body1)
        p2.publish(body2)
        for _ in range(2):
            self.connection.drain_events(timeout=0.3)

        f1.assert_has_calls([mock.call(body1), mock.call(body2)], any_order=True)

        # no retries:
        e = None
        try:
            self.connection.drain_events(timeout=0.3)
        except socket.timeout as exc:
            e = exc
        self.assertIsNotNone(e, msg="e=None here means task was unexpectedly retried")
        # no further calls
        f1.call_count = 2
