from functools import wraps

from configloader import ConfigLoader


class OverrideSettings(object):

    def __init__(self, **kwargs):
        self.overrides = kwargs
        self.__old = None

    def __call__(self, f):
        @wraps(f)
        def decorated(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return decorated

    def __enter__(self):
        self.enable()

    def __exit__(self, exc_type, exc_value, traceback):
        self.disable()

    def enable(self):
        from event_consumer.conf import settings
        self.__old = ConfigLoader(settings.copy())
        new = ConfigLoader(settings.copy())
        new.update(self.overrides)
        settings._replace_wrapped(new)

    def disable(self):
        from event_consumer.conf import settings
        settings._replace_wrapped(self.__old)


override_settings = OverrideSettings
