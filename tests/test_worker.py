import json
import logging
import subprocess
import time
import uuid

import mock
import pytest

from test_app.consumer import app
from test_app.models import Call

from .base import (
    publish,
    use_test_databases,
    get_connection,
    configure_handlers,
    shut_down,
)


logger = logging.getLogger(__name__)


celery_process = None
connection = None
channel = None
handlers = None


@app.task()
def no_op_task():
    return


def setup_module(module):  # pylint: disable=N802
    global celery_process, connection, channel, handlers
    args = [
        'celery',
        'worker',
        '--loglevel=INFO',
        '--app=test_app.consumer:app',
        '--queues=celery',
    ]
    celery_process = subprocess.Popen(args)
    # ensure worker is up and running before continuing
    # so that we can have short sleep time in the tests
    try:
        result = no_op_task.delay()
        result.get(timeout=30)
        logger.info('setup_module: worker ready')
    except:  # noqa
        logger.error('failed to get result for no_op_task')
        teardown_module(module)
    else:
        connection = get_connection()
        channel = connection.channel()
        parent = mock.MagicMock()
        parent.app = app
        handlers = configure_handlers(channel, parent)


def teardown_module(module):  # pylint: disable=N802
    global celery_process, connection, handlers
    if celery_process:
        celery_process.terminate()
        celery_process = None
    time.sleep(0.5)
    shut_down(handlers, connection)


@app.message_handler('test.route.one')
def dummy_handler(body):
    logger.info('dummy_handler: {}'.format(body))
    assert body['event_type'] == 'test'
    assert 'uuid' in body['data']
    use_test_databases()
    call = Call.objects.create(uuid=body['data']['uuid'], body=json.dumps(body))
    logger.info(call.uuid)


@pytest.mark.django_db
def test_consume_event():
    event_id = str(uuid.uuid4())
    publish(
        'test.route.one',
        {
            'event_type': 'test',
            'data': {
                'uuid': event_id,
            }
        }
    )
    time.sleep(1)

    assert Call.objects.all().count() == 1
    call = Call.objects.get(uuid=event_id)
    assert call.body == json.dumps({
        'event_type': 'test',
        'data': {
            'uuid': event_id,
        }
    })
