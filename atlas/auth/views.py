
from django.conf import settings

from django.contrib.auth import REDIRECT_FIELD_NAME
from django.views.decorators.cache import never_cache

from . import fake
import fake.views
import shibsso
import shibsso.views


@never_cache
def login(request, redirect_field_name=REDIRECT_FIELD_NAME):
    """Dispatching login action between authentication views."""

    if hasattr(settings, 'FAKE_LOGIN_AS_USER'):
        login_ = fake.views.login
    else:
        login_ = shibsso.views.login

    return login_(request, redirect_field_name)


@never_cache
def logout(request, redirect_field_name=REDIRECT_FIELD_NAME):
    """Logs out the user and redirects to another page."""

    if hasattr(settings, 'FAKE_LOGIN_AS_USER'):
        logout_ = fake.views.logout
    else:
        logout_ = shibsso.views.logout

    return logout_(request, redirect_field_name)
