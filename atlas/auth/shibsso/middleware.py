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

"""Middleware for Shibboleth (R).

It logs out the user from the application if the user logged out from SSO.

"""

from django.conf import settings
from django.contrib import auth
from django.contrib.auth import BACKEND_SESSION_KEY
from django.core.exceptions import ImproperlyConfigured
from django.utils.deprecation import MiddlewareMixin

class ShibSSOMiddleware(MiddlewareMixin):
    """Middleware for Shibboleth (R).

    It logs out the user from the application if the user logged out from SSO.

    """

    def process_request(self, request):

        if request.session.get(BACKEND_SESSION_KEY) != 'shibsso.backends.ShibSSOBackend':
            return

        if not hasattr(request, 'user'):
            raise ImproperlyConfigured(
                                       "The Django remote user auth middleware requires the"
                                       " authentication middleware to be installed.  Edit your"
                                       " MIDDLEWARE_CLASSES setting to insert"
                                       " 'django.contrib.auth.middleware.AuthenticationMiddleware'"
                                       " before the RemoteUserMiddleware class.")

        username = request.META.get(settings.META_USERNAME)

        if request.user.is_authenticated():
            if request.user.username != username:
                auth.logout(request)               