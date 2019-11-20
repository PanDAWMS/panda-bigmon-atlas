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

"""URLs to be used for test purposes for Shibboleth (R) Authentication Backend."""

from django.conf import settings
from django.conf.urls.defaults import *
from django.contrib import admin

admin.autodiscover()

urlpatterns = patterns('',
                       (r'^%s$' % settings.LOGIN_URL[1:], 'shibsso.views.login'),
                       (r'^logout/$', 'shibsso.views.logout'),
                       (r'^logout_default_redirect/$',
                       'shibsso.views.logout', {'next_page': '/goodbye'}),
                       (r'^login_required/$',
                       'shibsso.tests.views.view_login_required'),
                       (r'^admin/', include(admin.site.urls)),
                       (r'^login_auth/$', 'django.contrib.auth.views.login'),
                       )