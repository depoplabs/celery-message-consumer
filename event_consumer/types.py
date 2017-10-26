from collections import namedtuple

# Used by handlers.REGISTRY as keys
QueueRegistration = namedtuple(
    'QueueRegistration',
    ['routing_key', 'queue', 'exchange']
)
