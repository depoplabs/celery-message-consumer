import logging

from event_consumer.conf import settings
from event_consumer.errors import PermanentFailure
from event_consumer.handlers import message_handler


_logger = logging.getLogger(__name__)


class IntegrationTestHandlers(object):
    """
    Basic message handlers that log or raise known exceptions to allow
    interactive testing of the RabbitMQ config.
    """

    @staticmethod
    def py_integration_ok(body):
        """
        Should always succeed, never retry, never archive.
        """
        msg = 'py_integration_ok, {}'.format(body)
        _logger.info(msg)

    @staticmethod
    def py_integration_raise(body):
        """
        Should retry until there are no attempts left, then archive.
        """
        msg = 'py_integration_raise, {}'.format(body)
        _logger.info(msg)
        raise Exception(msg)

    @staticmethod
    def py_integration_raise_permanent(body):
        """
        Should cause the message to be archived on first go.
        """
        msg = 'py_integration_raise_permanent, {}'.format(body)
        _logger.info(msg)
        raise PermanentFailure(msg)


if settings.TEST_ENABLED:
    # Add tasks for interactive testing (call decorators directly)
    message_handler('py.integration.ok')(
        IntegrationTestHandlers.py_integration_ok)
    message_handler('py.integration.raise')(
        IntegrationTestHandlers.py_integration_raise)
    message_handler('py.integration.raise.permanent')(
        IntegrationTestHandlers.py_integration_raise_permanent)
