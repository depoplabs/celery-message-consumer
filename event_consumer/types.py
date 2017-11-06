from typing import NamedTuple


# Used by handlers.REGISTRY as keys
QueueRegistration = NamedTuple('QueueRegistration', [
    ('routing_key', str),
    ('queue_name', str),
    ('exchange_key', str)
])


class Nonlocal(object):
    """
    Helper class to workaround lack of `nonlocal` keyword in Python 2
    https://stackoverflow.com/questions/3190706/nonlocal-keyword-in-python-2-x
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    # these methods are here solely for the benefit of mypy type-checker
    # http://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html#when-you-re-puzzled-or-when-things-are-complicated
    def __setattr__(self, name, value):
        # type: (str, object) -> None
        super(Nonlocal, self).__setattr__(name, value)

    def __getattr__(self, name):
        # type: (str) -> object
        return getattr(self, name)
