from six import text_type


class PermanentFailure(Exception):
    """
    Raising `PermanentFailure` in a handler function causes the Kombu message
    to be archived and not retried.

    Do this when it's obvious a retry will not fix a problem (bad message
    format, programmer error, etc).
    """

    @property
    def message(self):
        try:
            return text_type(self.args[0])
        except IndexError:
            return ''

    def __str__(self):
        return "<{}>: {}".format(self.__class__.__name__, self.message)


class NoExchange(Exception):
    """
    Raised if during the instantiation of an AMQPRetryHandler the requested
    exchange has not been defined in settings.EXCHANGES
    """


class InvalidQueueRegistration(Exception):
    """
    Raised if you try to connect a message handler with a combination of
    queue and exchange that cannot work.
    """
