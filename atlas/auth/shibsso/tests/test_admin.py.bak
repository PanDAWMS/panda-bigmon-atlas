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

"""Tests procedures for the administration site using Shibboleth (R) Authentication
Backend.

"""

from django.conf import settings
from django.conf.urls.defaults import *
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.contrib.auth import SESSION_KEY
from django.test import Client
from django.test import TestCase
from django.utils.http import urlquote
from shibsso.tests import urls as url_tests

class AdminLoginDefaultTest(TestCase):

    urls = url_tests
    fixtures = ['shibusers.json']

    def setUp(self):
        self._shib_sso_admin_backup = settings.SHIB_SSO_ADMIN
        settings.SHIB_SSO_ADMIN = False

    def tearDown(self):
        settings.SHIB_SSO_ADMIN = self._shib_sso_admin_backup

    def test_login(self):

        # If user is not logged in.
        # Show login form.

        client = Client()

        request_url = '/admin/'
        response = client.get(request_url)
        self.failUnlessEqual(response.status_code, 200)
        self.failUnlessEqual(response.template[0].name,
                             'admin/login.html')

    def test_after_login_no_staff_user(self):

        # If user is logged in but no staff.
        # Show login.

        client = Client()

        request_url = '/login_auth/'
        client.post(request_url, {
                    'username': 'user_nostaff',
                    'password': 'password'
                    })

        self.failUnlessEqual(client.session[SESSION_KEY], 4)

        request_url = '/admin/'
        response = client.get(request_url)

        self.failUnlessEqual(response.status_code, 200)
        self.failUnlessEqual(response.template[0].name,
                             'shibsso/no_permission.html')

class AdminLoginShibTest(TestCase):

    urls = url_tests
    fixtures = ['shibusers.json']
    
    def setUp(self):
        self._shib_sso_admin_backup = settings.SHIB_SSO_ADMIN
        settings.SHIB_SSO_ADMIN = True

    def tearDown(self):
        settings.SHIB_SSO_ADMIN = self._shib_sso_admin_backup

    def test_login_redirect(self):

        # If user is not logged in.
        # Redirect to login page.

        client = Client()

        request_url = '/admin/'
        response = client.get(request_url)
        self.failUnlessEqual(response.status_code, 302)
        self.failUnlessEqual(response['Location'],
                             'http://testserver%s?%s=%s' % \
                             (settings.LOGIN_URL,
                             REDIRECT_FIELD_NAME,
                             urlquote(request_url)))

    def test_after_login(self):

        # If user is logged in.
        # Redirect to current page.

        environ = {settings.META_USERNAME: 'user_super'}

        client = Client()

        request_url = "%s?%s=%s" % (settings.LOGIN_URL, REDIRECT_FIELD_NAME,
                                    urlquote('/admin/'))
        response = client.get(request_url, ** environ)

        request_url = response['Location']
        response = client.get(request_url, ** environ)

        self.failUnlessEqual(response.status_code, 200)
        self.failUnlessEqual(response.template[0].name,
                             'admin/index.html')

    def test_after_login_no_staff_user(self):

        # If user is logged in (no staff).
        # Show admin page.

        environ = {settings.META_USERNAME: 'shib_nostaff'}

        client = Client()

        request_url = "%s?%s=%s" % (settings.LOGIN_URL, REDIRECT_FIELD_NAME,
                                    urlquote('/admin/'))
        response = client.get(request_url, ** environ)

        self.failUnlessEqual(client.session[SESSION_KEY], 2)

        request_url = response['Location']
        response = client.get(request_url, ** environ)

        self.failUnlessEqual(response.status_code, 200)
        self.failUnlessEqual(response.template[0].name,
                             'shibsso/no_permission.html')

class AdminLogout(TestCase):

    urls = url_tests
    fixtures = ['shibusers.json']

    def setUp(self):
        self._shib_sso_admin_backup = settings.SHIB_SSO_ADMIN
        settings.SHIB_SSO_ADMIN = False

    def tearDown(self):
        settings.SHIB_SSO_ADMIN = self._shib_sso_admin_backup

    def test_logout_other_user(self):
        # If shib user logs out.
        # Redirect to logout.

        client = Client()

        request_url = '/login_auth/'
        client.post(request_url, {
                    'username': 'user_super',
                    'password': 'password'
                    })

        self.assert_(SESSION_KEY in client.session)

        request_url = '/admin/logout/'
        response = client.get(request_url)

        self.failUnlessEqual(response.template[0].name,
                             'registration/logged_out.html')
        self.assert_(SESSION_KEY not in client.session)

    def test_logout_shib_user(self):
        
        # If shib user logs out.
        # Redirect to shib logout.

        environ = {settings.META_USERNAME: 'user_super'}

        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)

        self.assert_(SESSION_KEY in client.session)

        request_url = '/admin/logout/'
        response = client.get(request_url, ** environ)
        self.failUnlessEqual(response.status_code, 302)
        self.failUnlessEqual(response['Location'],
                             '%s%s%s' % \
                             (settings.SHIB_LOGOUT_URL,
                             urlquote('http://testserver'),
                             urlquote(request_url)))
                             
        self.assert_(SESSION_KEY not in client.session)

        request_url = '/admin/logout/'
        response = client.get(request_url)

        self.failUnlessEqual(response.status_code, 200)
        self.failUnlessEqual(response.template[0].name,
                             'registration/logged_out.html')
        self.assert_(SESSION_KEY not in client.session)