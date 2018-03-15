from datetime import timedelta
import os
from typing import Callable, Dict, Optional  # noqa


# namespace for config keys loaded from e.g. Django conf or env vars
CONFIG_NAMESPACE = os.getenv('EVENT_CONSUMER_CONFIG_NAMESPACE', 'EVENT_CONSUMER')

# optional import path to file containing namespaced config (e.g. 'django.conf.settings')
APP_CONFIG = os.getenv('EVENT_CONSUMER_APP_CONFIG', None)


# safety var to prevent accidentally enabling the handlers in `test_utils.handlers`
# set to True and then import the module to enable them
TEST_ENABLED = False

# http://docs.celeryproject.org/en/latest/userguide/configuration.html#std:setting-task_serializer
SERIALIZER = 'json'
ACCEPT = [SERIALIZER]

QUEUE_NAME_PREFIX = ''

MAX_RETRIES = 4  # type: int

# By default will use `AMQPRetryHandler.backoff`, otherwise supply your own.
# Should accept a single arg <retry number> and return a delay time (seconds).
BACKOFF_FUNC = None  # type: Optional[Callable[[int], float]]

RETRY_HEADER = 'x-retry-count'
# to generate TTL header for archived message (milliseconds)
ARCHIVE_EXPIRY = int(timedelta(days=24).total_seconds() * 1000)  # type: int


USE_DJANGO = False

EXCHANGES = {}  # type: Dict[str, Dict[str, str]]
# EXCHANGES = {
#     'default': {  # a reference name for this config, used when attaching handlers
#         'name': 'data',  # actual name of exchange in RabbitMQ
#         'type': 'topic',  # an AMQP exchange type
#     },
#     ...
# }
