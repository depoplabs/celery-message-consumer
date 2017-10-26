from django.db import models


class User(models.Model):
    username = models.CharField(max_length=24, unique=True)
