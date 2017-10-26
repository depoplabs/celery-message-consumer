import os


EVENT_CONSUMER_BACKOFF_FUNC = lambda count: 0.5  # noqa

EVENT_CONSUMER_BROKER_URL = 'amqp://{0}:5672'.format(os.getenv('BROKER_HOST', 'localhost'))

EVENT_CONSUMER_WHATEVER = 'WTF'
