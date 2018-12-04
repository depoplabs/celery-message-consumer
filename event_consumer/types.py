from typing import NamedTuple, Dict, Callable


# Used by handlers.REGISTRY as keys
QueueKey = NamedTuple(
    'QueueKey',
    [
        ('queue', str),
        ('exchange', str),
    ]
)


HandlerRegistration = NamedTuple(
    'HandlerRegistration',
    [
        ('routing_key', str),
        ('queue_arguments', Dict[str, str]),
        ('handler', Callable),
    ]
)
