import socket

from django.db import transaction
import mock
import pytest

from test_app.factories import UserFactory
from test_app.models import User

from .base import BaseRetryHandlerIntegrationTest


def cleanup():
    User.objects.all().delete()


try:
    transaction_context = transaction.atomic
except AttributeError:
    transaction_context = transaction.commit_on_success


@pytest.mark.django_db(transaction=True)
class DjangoDBTransactionIntegrationTest(BaseRetryHandlerIntegrationTest):

    def setUp(self):
        self.addCleanup(cleanup)
        super(DjangoDBTransactionIntegrationTest, self).setUp()

    def test_wrapped_func_raises_database_error_implicit_transaction(self):
        """
        If consumer encounters a DatabaseError it should be able to re-enqueue
        the message and continue successfully, without falling foul of:
        'current transaction is aborted, commands ignored until end of transaction block'
        """
        user = UserFactory()

        def broken(*args, **kwargs):
            return UserFactory(username=user.username)  # IntegrityError

        def good(*args, **kwargs):
            return UserFactory()

        self.assertEqual(len(User.objects.all()), 1)

        with mock.patch.object(self.handler, 'func') as f:
            # Publish and run handler once, with good func, to prove good works

            f.side_effect = good
            body = self.body()

            self.producer.publish(body)
            self.connection.drain_events(timeout=0.3)
            f.assert_called_once_with(body)
            self.assertEqual(len(self.archives), 0)

            # no retries:
            e1 = None
            try:
                self.connection.drain_events(timeout=0.3)
            except socket.timeout as exc:
                e1 = exc
            self.assertIsNotNone(e1, msg="e1=None here means task was unexpectedly retried")
            f.call_count = 1

        with mock.patch.object(self.handler, 'func') as f:
            # Publish and run handler once, with broken func
            # (expecting retry enqueued due to IntegrityError - if we
            # don't handle it this leads to hanging transaction context
            # causing subsequent failures)

            f.side_effect = broken
            body = self.body()
            self.assertEqual(len(self.archives), 0)

            self.producer.publish(body)
            self.connection.drain_events(timeout=0.3)
            f.assert_called_once_with(body)
            self.assertEqual(len(self.archives), 0)

        with mock.patch.object(self.handler, 'func') as f:
            # attempt to process the retry with good func - should succeed

            f.side_effect = good

            self.connection.drain_events(timeout=0.3)
            f.assert_called_once_with(body)  # previous body, i.e. retry
            self.assertEqual(len(self.archives), 0)

            # no further retries:
            e2 = None
            try:
                self.connection.drain_events(timeout=0.3)
            except socket.timeout as exc:
                e2 = exc
            self.assertIsNotNone(e2, msg="e2=None here means task was unexpectedly retried")
            f.call_count = 1

        self.assertEqual(len(User.objects.all()), 3)

    def test_wrapped_func_raises_database_error_manual_transaction(self):
        """
        Ensure that consumer commit/rollback, to avoid problem with Django
        implicit transactions, does not break code that manages its own tx
        """
        user = UserFactory()

        def broken(*args, **kwargs):
            with transaction_context():
                return UserFactory(username=user.username)  # IntegrityError

        def good(*args, **kwargs):
            with transaction_context():
                return UserFactory()

        self.assertEqual(len(User.objects.all()), 1)

        with mock.patch.object(self.handler, 'func') as f:
            # Publish and run handler once, with good func, to prove good works

            f.side_effect = good
            body = self.body()

            self.producer.publish(body)
            self.connection.drain_events(timeout=0.3)
            f.assert_called_once_with(body)
            self.assertEqual(len(self.archives), 0)

            # no retries:
            e1 = None
            try:
                self.connection.drain_events(timeout=0.3)
            except socket.timeout as exc:
                e1 = exc
            self.assertIsNotNone(e1, msg="e1=None here means task was unexpectedly retried")
            f.call_count = 1

        with mock.patch.object(self.handler, 'func') as f:
            # Publish and run handler once, with broken func
            # (expecting retry enqueued due to IntegrityError - if we
            # don't handle it this leads to hanging transaction context
            # causing subsequent failures)

            f.side_effect = broken
            body = self.body()
            self.assertEqual(len(self.archives), 0)

            self.producer.publish(body)
            self.connection.drain_events(timeout=0.3)
            f.assert_called_once_with(body)
            self.assertEqual(len(self.archives), 0)

        with mock.patch.object(self.handler, 'func') as f:
            # attempt to process the retry with good func - should succeed

            f.side_effect = good

            self.connection.drain_events(timeout=0.3)
            f.assert_called_once_with(body)  # previous body, i.e. retry
            self.assertEqual(len(self.archives), 0)

            # no further retries:
            e2 = None
            try:
                self.connection.drain_events(timeout=0.3)
            except socket.timeout as exc:
                e2 = exc
            self.assertIsNotNone(e2, msg="e2=None here means task was unexpectedly retried")
            f.call_count = 1

        self.assertEqual(len(User.objects.all()), 3)
