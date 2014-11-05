from django.conf import settings
from django.contrib.auth.backends import ModelBackend
from django.contrib.auth.models import User


class LoginAsBackend(ModelBackend):
    """ Authenticate as user specified in configuration file.
        The user should exist in the database.
    """
    def authenticate(self, request=None):
        username = settings.FAKE_LOGIN_AS_USER
        if not username:
            return

        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            return

        return user
