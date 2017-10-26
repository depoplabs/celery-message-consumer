import socket

import mock

from event_consumer.conf import settings
from event_consumer import handlers as ec

from .base import BaseRetryHandlerIntegrationTest


class AMQPRetryHandlerIntegrationTest(BaseRetryHandlerIntegrationTest):

    def test_wrapped_func(self):
        """Should run the wrapped function when a message arrives with its routing key"""
        with mock.patch.object(self.handler, 'func') as f:
            body = self.body()

            self.producer.publish(body)
            self.connection.drain_events(timeout=0.3)
            f.assert_called_once_with(body)
            self.assertEqual(len(self.archives), 0)

            # no retries:
            e = None
            try:
                self.connection.drain_events(timeout=0.3)
            except socket.timeout as exc:
                e = exc
            self.assertIsNotNone(e, msg="e=None here means task was unexpectedly retried")
            f.call_count = 1

    def test_wrapped_func_raises_exception(self):
        """Should archive messages that have been retried too many times"""
        expected_attempts = settings.MAX_RETRIES + 1

        with mock.patch.object(self.handler, 'func') as f:
            f.side_effect = Exception('This should be retried')
            body = self.body()

            self.producer.publish(body)

            for _ in range(expected_attempts):
                self.connection.drain_events(timeout=0.3)
            self.assertEqual(len(self.archives), 0)

            # Expect the final attempt to have placed the message in the
            # archive which we now drain...
            self.connection.drain_events(timeout=0.3)
            self.assertEqual(len(self.archives), 1)

            archived_body, archived_message = self.archives[0]
            self.assertEqual(body, archived_body)
            self.assertEqual(
                archived_message.headers[settings.RETRY_HEADER],
                settings.MAX_RETRIES
            )
            self.assertEqual(f.call_count, expected_attempts)

    def test_wrapped_func_raises_permanent_failure_exception(self):
        """Should catch PermanentFailure exceptions and archive these messages"""
        with mock.patch.object(self.handler, 'func') as f:
            f.side_effect = ec.PermanentFailure('This should be archived first go')
            body = self.body()

            self.assertEqual(len(self.archives), 0)

            # Publish and run handler once
            self.producer.publish(body)
            self.connection.drain_events()

            # Drain from archive once
            self.connection.drain_events()
            self.assertEqual(len(self.archives), 1)

            archived_body, archived_message = self.archives[0]
            self.assertEqual(body, archived_body)
            self.assertEqual(f.call_count, 1)
