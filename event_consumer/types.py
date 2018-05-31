from typing import NamedTuple


# Used by handlers.REGISTRY as keys
QueueRegistration = NamedTuple(
    'QueueRegistration',
    [('routing_key', str), ('queue', str), ('exchange', str)]
)
