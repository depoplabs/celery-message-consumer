import factory


class UserFactory(factory.django.DjangoModelFactory):

    class Meta:
        model = 'test_app.User'

    username = factory.Sequence(lambda n: 'user_%s' % n)
