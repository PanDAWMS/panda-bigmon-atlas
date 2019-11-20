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

"""Tests procedures for the logout page."""

from django.conf import settings
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.contrib.auth import SESSION_KEY
from django.test import Client
from django.test import TestCase
from django.utils.http import urlquote
from shibsso.tests import urls as url_tests

class LogoutTest(TestCase):

    urls = url_tests
    fixtures = ['shibusers.json']

    def test_shib_redirect(self):
        
        # If user is logged in.
        # Redirect to Shibboleth (R).

        environ = {settings.META_USERNAME: 'shib_super'}

        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)

        request_url = '%s?%s=%s' % ('/logout/', REDIRECT_FIELD_NAME,
                                    '/my_page?my_field=my_value')
        response = client.get(request_url, ** environ)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'],
                             '%s%s%s' % \
                             (settings.SHIB_LOGOUT_URL,
                             urlquote('http://testserver'),
                             urlquote(request_url)))

    def test_session_key_destroyed(self):

        # If user is not logged in.
        # Session must be destroyed.

        environ = {settings.META_USERNAME: 'shib_super'}

        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)

        self.assertEqual(client.session[SESSION_KEY], 1)

        request_url = '/logout/'
        client.get(request_url, ** environ)

        self.assertRaises(KeyError, client.session.__getitem__, SESSION_KEY)

    def test_custom_redirect(self):

        # If user is not logged in.
        # Redirect to custom page.

        client = Client()

        urls = ('%s?%s=%s' % ('/logout/', REDIRECT_FIELD_NAME,
                '/my_goodbye?my_field=my_value'),
                '%s?%s=%s' % ('/logout_default_redirect/', REDIRECT_FIELD_NAME,
                '/my_goodbye?my_field=my_value'))

        for request_url in urls:
            response = client.get(request_url)

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response['Location'],
                                 'http://testserver/my_goodbye?my_field=my_value')

    def test_default_redirect(self):

        # If user is not logged in.
        # Redirect to custom page.

        client = Client()

        request_url = '/logout_default_redirect/'
        response = client.get(request_url)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'],
                             'http://testserver/goodbye')

    def test_no_redirect(self):

        # If user is logged in.
        # Show default template.

        client = Client()

        request_url = '/logout/'
        response = client.get(request_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template[0].name,
                             'registration/logged_out.html')