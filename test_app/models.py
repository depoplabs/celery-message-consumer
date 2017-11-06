from django.db import models


class User(models.Model):

    username = models.CharField(max_length=24, unique=True)


class Call(models.Model):

    uuid = models.CharField(max_length=36, primary_key=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    body = models.TextField()
