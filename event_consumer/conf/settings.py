from datetime import timedelta
import os
from typing import Optional, Callable

try:
    from django.conf import settings
except ImportError:
    settings = None

CONFIG_NAMESPACE: str = "EVENT_CONSUMER"

# safety var to prevent accidentally enabling the handlers in `test_utils.handlers`
# set to True and then import the module to enable them
TEST_ENABLED: bool = getattr(settings, f"{CONFIG_NAMESPACE}_TEST_ENABLED", False)

# http://docs.celeryproject.org/en/latest/userguide/configuration.html#std:setting-task_serializer
SERIALIZER: str = getattr(settings, f"{CONFIG_NAMESPACE}_SERIALIZER", 'json')
ACCEPT = [SERIALIZER]

QUEUE_NAME_PREFIX: str = getattr(settings, f"{CONFIG_NAMESPACE}_QUEUE_NAME_PREFIX", '')

MAX_RETRIES: int = getattr(settings, f"{CONFIG_NAMESPACE}_MAX_RETRIES", 4)

# By default will use `AMQPRetryHandler.backoff`, otherwise supply your own.
# Should accept a single arg <retry number> and return a delay time (seconds).
BACKOFF_FUNC: Optional[Callable[[int], float]] = getattr(
    settings, f"{CONFIG_NAMESPACE}_BACKOFF_FUNC", None
)

RETRY_HEADER: str = getattr(
    settings, f"{CONFIG_NAMESPACE}_RETRY_HEADER", 'x-retry-count'
)

# Set the consumer prefetch limit
PREFETCH_COUNT: int = getattr(settings, f"{CONFIG_NAMESPACE}_PREFETCH_COUNT", 1)

# to set TTL for archived message (milliseconds)
twenty_four_days = int(timedelta(days=24).total_seconds() * 1000)
ARCHIVE_EXPIRY: int = getattr(
    settings, f"{CONFIG_NAMESPACE}_ARCHIVE_EXPIRY", twenty_four_days
)
ARCHIVE_MAX_LENGTH: int = getattr(
    settings, f"{CONFIG_NAMESPACE}_ARCHIVE_MAX_LENGTH", 1000000
)
ARCHIVE_QUEUE_ARGS = {
    "x-message-ttl": ARCHIVE_EXPIRY,  # Messages dropped after this
    "x-max-length": ARCHIVE_MAX_LENGTH,  # Maximum size of the queue
    "x-queue-mode": "lazy",  # Keep messages on disk (reqs. rabbitmq 3.6.0+)
}


USE_DJANGO: bool = getattr(settings, f"{CONFIG_NAMESPACE}_USE_DJANGO", False)

EXCHANGES: dict[str, dict[str, str]] = getattr(
    settings, f"{CONFIG_NAMESPACE}_EXCHANGES", {}
)
# EXCHANGES = {
#     'default': {  # a reference name for this config, used when attaching handlers
#         'name': 'data',  # actual name of exchange in RabbitMQ
#         'type': 'topic',  # an AMQP exchange type
#     },
#     ...
# }

BROKER_URL = 'amqp://{0}:5672'.format(os.getenv('BROKER_HOST', 'localhost'))
