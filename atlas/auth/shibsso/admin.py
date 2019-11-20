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

"""Utilities for the administration interface."""

from django.conf import settings
from django.contrib.admin import AdminSite
from django.contrib.auth import BACKEND_SESSION_KEY
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext
from functools import update_wrapper
from django.utils.http import urlquote
from django.utils.translation import ugettext as _
from django.views.decorators.cache import never_cache
from . import views

try:
    # Backwards-compatibility for Django 1.1.1
    from django.views.decorators.csrf import csrf_protect
except:
    pass

class ShibSSOAdminSite(AdminSite):
    """A ShibbolethAdminSite object encapsulates an instance of the Django
    admin application, ready to be hooked in to your URLconf.

    ShibSSOAdminSite presents the Shibboleth (R) login page instead of the default
    login form if SHIB_SSO_ADMIN is set to True.  It is also responsible for
    the SSO logout if the user was authenticated by Shibboleth (R).
    
    """

    def __init__(self, name=None, app_name='admin'):
        """Creates a new ShibSSOAdminSite."""
        super(ShibSSOAdminSite, self).__init__(name)
        if not hasattr(self, 'logout_template'):
            # Backwards-compatibility for Django 1.1.1
            self.logout_template = None

    def admin_view(self, view, cacheable=False):
        """Decorator to create an admin view attached to this AdminSite. This
        wraps the view and provides permission checking by calling
        self.has_permission (except for the logout page).
  
        """

        def inner(request, * args, ** kwargs):
            if not self.has_permission(request) and view != self.logout:
                if not request.user.is_authenticated():
                    return self.login(request)
                else:
                    return render_to_response('shibsso/no_permission.html', {
                                              'title': _('Site administration')
                                              }, context_instance=RequestContext(request))
                                          
            return view(request, * args, ** kwargs)
        if not cacheable:
            inner = never_cache(inner)
        try:
            # Backwards-compatibility for Django 1.1.1
            if not getattr(view, 'csrf_exempt', False):
                inner = csrf_protect(inner)
        except:
            pass
        return update_wrapper(inner, view)

    def login(self, request):
        """Displays the Shibboleth (R) login page for the given HttpRequest if
        SHIB_SSO_ADMIN is set to True. Display the default login form otherwise.
        
        """

        if not settings.SHIB_SSO_ADMIN:
            return super(ShibSSOAdminSite, self).login(request)

        login_url = settings.LOGIN_URL
        redirect_field_name = REDIRECT_FIELD_NAME
        path = urlquote(request.get_full_path())
        tup = login_url, redirect_field_name, path
        return HttpResponseRedirect('%s?%s=%s' % tup)

    login = never_cache(login)

    def logout(self, request):
        """Logs out the user for the given HttpRequest. If the user used
        Shibboleth (R) to login, logs out the user from SSO.

        """

        if request.session.get(BACKEND_SESSION_KEY) != 'shibsso.backends.ShibSSOBackend':
            return super(ShibSSOAdminSite, self).logout(request)

        defaults = {}
        if self.logout_template is not None:
            defaults['template_name'] = self.logout_template

        return views.logout(request, ** defaults)

    logout = never_cache(logout)
