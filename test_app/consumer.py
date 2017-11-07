from event_consumer import EventConsumerApp


app = EventConsumerApp()
app.config_from_object('test_app.celeryconfig')
