from contextlib import contextmanager

from configloader import ConfigLoader


@contextmanager
def override_settings(**kwargs):
    from event_consumer.conf import settings
    old = ConfigLoader(settings.copy())
    new = ConfigLoader(settings.copy())
    new.update(kwargs)
    settings._replace_wrapped(new)
    yield
    settings._replace_wrapped(old)
