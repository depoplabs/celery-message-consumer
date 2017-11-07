from configloader import ConfigLoader
from decorator import decorator


class OverrideSettings(object):

    def __init__(self, **kwargs):
        self.overrides = kwargs
        self.__old = None

    def __call__(self, f):
        def wrapper(f, *args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return decorator(wrapper, f)

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
