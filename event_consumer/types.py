from typing import NamedTuple

from kombu import Queue


# Used by handlers.REGISTRY as keys
QueueRegistration = NamedTuple('QueueRegistration', [
    ('routing_key', str),
    ('queue_name', str),
    ('exchange_key', str)
])


class UnboundQueue(Queue):
    """
    A Queue that we can pass to Celery `task_queues` setting, that guarantees
    it will never be bound and used (config only). We will instantiate our own
    queues to consume and publish from same config.
    """

    def __call__(self, *args, **kwargs):
        raise NotImplementedError("UnboundQueue can't be bound")
