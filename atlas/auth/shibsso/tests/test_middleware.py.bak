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

"""Tests procedures for ShibSSO middleware."""

from django.conf import settings
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.contrib.auth import SESSION_KEY
from django.test import Client
from django.test import TestCase
from django.utils.http import urlquote
from shibsso.tests import urls as url_tests

class MiddlewareTest(TestCase):

    urls = url_tests
    fixtures = ['shibusers.json']

    def test_user_stay_logged(self):

        # If user is logged in.
        # Session must remain.

        environ = {settings.META_USERNAME: 'shib_super'}

        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)

        request_url = '/login_required/'
        response = client.get(request_url, ** environ)

        self.failUnlessEqual(response.status_code, 200)
        self.failUnlessEqual(response.content, 'shib_super')

        self.failUnlessEqual(client.session[SESSION_KEY], 1)

    def test_user_is_logged_out(self):

        # If user is logged out.
        # Session must be destroyed.

        environ = {settings.META_USERNAME: 'shib_super'}

        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)

        self.failUnlessEqual(client.session[SESSION_KEY], 1)

        request_url = '/login_required/'
        response = client.get(request_url)

        self.failUnlessEqual(response['Location'],
                             'http://testserver%s?%s=%s' % \
                             (settings.LOGIN_URL, REDIRECT_FIELD_NAME,
                             urlquote(request_url)))
        
        self.failUnlessRaises(KeyError, client.session.__getitem__, SESSION_KEY)