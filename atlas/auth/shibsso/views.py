#
# Copyright 2010 CERN
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Login and logout views.
Because these are Shibboleth (R) webpages, these views can not be customized.

"""

import re

from django.conf import settings
from django.contrib import auth
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.template import RequestContext
from django.views.decorators.cache import never_cache

def login(request, redirect_field_name=REDIRECT_FIELD_NAME):
    """Displays the login form and handles the login action."""

    redirect_to = request.GET.get(redirect_field_name, '')

    if request.META.get(settings.META_USERNAME):

        user = auth.authenticate(request=request)

        if not redirect_to or ' ' in redirect_to:
            redirect_to = settings.LOGIN_REDIRECT_URL
        elif '//' in redirect_to and re.match(r'[^\?]*//', redirect_to):
            redirect_to = settings.LOGIN_REDIRECT_URL
        auth.login(request, user)

        return HttpResponseRedirect(redirect_to)

    path = request.build_absolute_uri(request.get_full_path())

    return HttpResponseRedirect('https://%s%s%s' %
                                (request.get_host(),
                                settings.SHIB_LOGIN_PATH,
                                path))

login = never_cache(login)

def logout(request, next_page=None, template_name='registration/logged_out.html',
           redirect_field_name=REDIRECT_FIELD_NAME):
    "Logs out the user and displays a message or redirects to another page."

    auth.logout(request)

    if settings.META_USERNAME in request.META:

        next_url = request.build_absolute_uri(request.get_full_path())

        return HttpResponseRedirect('%s%s' %
                                    (settings.SHIB_LOGOUT_URL,
                                    next_url))

    redirect_to = request.GET.get(redirect_field_name, '')

    if redirect_to:
        return HttpResponseRedirect(redirect_to)

    if next_page is None:
        return render(request, template_name, {
            'title': _('Logged out')
            })
    else:
        # Redirect to this page until the session has been cleared.
        return HttpResponseRedirect(next_page or request.path)

logout = never_cache(logout)
