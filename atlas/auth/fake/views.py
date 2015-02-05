
import re

from django.conf import settings
from django.contrib import auth
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.utils.translation import ugettext as _
from django.views.decorators.cache import never_cache

@never_cache
def login(request, redirect_field_name=REDIRECT_FIELD_NAME):
    """Handles the login action for fake authentication."""

    redirect_to = request.REQUEST.get(redirect_field_name, '')

    request.META[settings.META_USERNAME] = settings.FAKE_LOGIN_AS_USER

    user = auth.authenticate(request=request)

    if not redirect_to or ' ' in redirect_to:
        redirect_to = settings.LOGIN_REDIRECT_URL
    elif '//' in redirect_to and re.match(r'[^\?]*//', redirect_to):
        redirect_to = settings.LOGIN_REDIRECT_URL
    auth.login(request, user)

    return HttpResponseRedirect(redirect_to)


@never_cache
def logout(request, redirect_field_name=REDIRECT_FIELD_NAME):
    "Logs out the user and redirects to another page."

    auth.logout(request)

    redirect_to = request.REQUEST.get(redirect_field_name, '')

    if redirect_to:
        return HttpResponseRedirect(redirect_to)

