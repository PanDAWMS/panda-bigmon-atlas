from django.conf import settings

from django.contrib.auth import REDIRECT_FIELD_NAME
from django.http import HttpResponseRedirect
from django.views.decorators.cache import never_cache
import re

@never_cache
def login(request, redirect_field_name=REDIRECT_FIELD_NAME):
    """Dispatching login action between authentication views."""
    redirect_to = request.GET.get(redirect_field_name, '')
    if not redirect_to or ' ' in redirect_to:
        redirect_to = settings.LOGIN_REDIRECT_URL
    elif '//' in redirect_to and re.match(r'[^\?]*//', redirect_to):
        redirect_to = settings.LOGIN_REDIRECT_URL
    return HttpResponseRedirect(redirect_to)


