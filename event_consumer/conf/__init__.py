from configloader import ConfigLoader
from wrapt import ObjectProxy

from event_consumer.conf import defaults

"""
Usage:

    from event_consumer.conf import settings

There are two important env vars:

    `EVENT_CONSUMER_CONFIG_NAMESPACE`
        Sets the prefix used for loading further config values from env
        and config file. Defaults to `EVENT_CONSUMER`.
    `EVENT_CONSUMER_APP_CONFIG`
        Import path to a python object to load futher config values from.
        Defaults to None. e.g. 'django.conf.settings' or 'celeryconfig'.

Although we can load further keys from the env, now prefixed using our
custom namespace, by preference all further keys should be loaded from a
python obj because all values loaded from env will be strings, there is no
way to automatically type cast them.

Keys in the config obj must be prefixed with the namespace, but will be
provided in `settings` without the prefix.

e.g. if you set your env to:

    EVENT_CONSUMER_CONFIG_NAMESPACE=MYAPP_EVENTS
    EVENT_CONSUMER_APP_CONFIG=django.conf.settings

and in your Django settings you have:

    MYAPP_EVENTS_SERIALIZER = 'json'

then:

    from event_consumer.conf import settings

    print(settings.SERIALIZER)
    > json
"""


class Settings(ObjectProxy):

    def _replace_wrapped(self, new):
        self.__wrapped__ = new


def _get_settings():
    config = ConfigLoader()
    config.update_from_object(defaults)

    config.update_from_env_namespace(config.CONFIG_NAMESPACE)
    namespace = config.CONFIG_NAMESPACE

    config.update_from_env_namespace(namespace)

    if config.APP_CONFIG:
        _temp = ConfigLoader()
        _temp.update_from_object(config.APP_CONFIG, lambda key: key.startswith(namespace))
        config.update(_temp.namespace(namespace))

    return Settings(config)


settings = _get_settings()
