from event_consumer.conf import settings, _get_settings
from event_consumer.test_utils.override import override_settings

from .base import override_environment


def test_defaults():
    assert settings.SERIALIZER == 'json'


def test_override_settings():
    assert settings.SERIALIZER == 'json'

    with override_settings(SERIALIZER='yaml'):
        assert settings.SERIALIZER == 'yaml'

    assert settings.SERIALIZER == 'json'


def test_override_environment():
    global settings
    assert settings.SERIALIZER == 'json'

    # env vars are namespaced
    with override_environment(EVENT_CONSUMER_SERIALIZER='yaml'):
        settings = _get_settings()
        assert settings.SERIALIZER == 'yaml'

    settings = _get_settings()
    assert settings.SERIALIZER == 'json'


def test_from_object_namespace():
    # from `test_app.settings.EVENT_CONSUMER_WHATEVER`
    assert settings.WHATEVER == 'WTF'
