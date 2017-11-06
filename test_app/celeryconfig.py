from event_consumer.conf import settings


BROKER_URL = settings.BROKER_URL

CELERY_RESULT_BACKEND = 'db+sqlite:///test_app/results.sqlite'

CELERY_IMPORTS = (
    'tests.test_worker',
)

CELERY_TASK_CREATE_MISSING_QUEUES = False
