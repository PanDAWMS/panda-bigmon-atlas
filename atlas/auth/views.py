
from django.conf import settings

from django.contrib.auth import REDIRECT_FIELD_NAME
from django.views.decorators.cache import never_cache

import atlas.auth.fake.views



@never_cache
def login(request, redirect_field_name=REDIRECT_FIELD_NAME):
    """Dispatching login action between authentication views."""

    login_ = atlas.auth.fake.views.login


    return login_(request, redirect_field_name)


@never_cache
def logout(request, redirect_field_name=REDIRECT_FIELD_NAME):
    """Logs out the user and redirects to another page."""

    logout_ = atlas.auth.fake.views.logout
    return logout_(request, redirect_field_name)

