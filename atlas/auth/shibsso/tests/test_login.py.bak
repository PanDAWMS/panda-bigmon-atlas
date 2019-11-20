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

"""Tests procedures for the login page."""

from django.conf import settings
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.contrib.auth import SESSION_KEY
from django.contrib.auth.models import User
from django.test import Client
from django.test import TestCase
from django.utils.http import urlquote
from shibsso.tests import urls as url_tests

class LoginTest(TestCase):

    urls = url_tests
    fixtures = ['shibusers.json']

    def test_shib_redirect(self):

        # If user is not logged in.
        # Redirect to Shibboleth (R).

        client = Client()

        request_url = '%s?%s=%s' % (settings.LOGIN_URL, REDIRECT_FIELD_NAME,
                                    '/my_page?my_field=my_value')
        response = client.get(request_url)
        
        self.failUnlessEqual(response.status_code, 302)
        self.failUnlessEqual(response['Location'],
                             'https://testserver%s%s%s' % \
                             (settings.SHIB_LOGIN_PATH,
                             urlquote('http://testserver'),
                             urlquote(request_url)))

    def test_session_key_created(self):

        # If user is logged in.
        # Session must be created.

        environ = {settings.META_USERNAME: 'shib_super'}

        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)

        self.failUnlessEqual(client.session[SESSION_KEY], 1)

    def test_custom_redirect(self):

        # If user is logged in.
        # Redirect to custom page.

        environ = {settings.META_USERNAME: 'shib_super'}

        client = Client()

        request_url = '%s?%s=%s' % (settings.LOGIN_URL, REDIRECT_FIELD_NAME,
                                    '/my_page?my_field=my_value')
        response = client.get(request_url, ** environ)
        
        self.failUnlessEqual(response.status_code, 302)
        self.failUnlessEqual(response['Location'],
                             'http://testserver/my_page?my_field=my_value')

    def test_default_redirect(self):

        # If user is logged in.
        # Redirect to default page.

        environ = {settings.META_USERNAME: 'shib_super'}

        client = Client()

        request_url = settings.LOGIN_URL
        response = client.get(request_url, ** environ)

        self.failUnlessEqual(response.status_code, 302)
        self.failUnlessEqual(response['Location'],
                             'http://testserver%s' % settings.LOGIN_REDIRECT_URL)

    def test_update_user(self):

        # If user is in the database.
        # User must be updated.

        user = User.objects.get(username='shib_super')

        environ = {settings.META_USERNAME: user.username,
            settings.META_EMAIL: 'new_user_super@example.com',
            settings.META_GROUP: 'group_1;'}
        
        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)
        
        updated_user = User.objects.get(username='shib_super')
        self.failUnlessEqual(updated_user.pk, user.pk)
        self.failUnlessEqual(updated_user.email, 'new_user_super@example.com')
        self.failUnlessEqual(updated_user.first_name, '')
        self.failUnlessEqual(updated_user.last_name, '')
        self.failUnlessEqual(updated_user.is_active, True)
        self.failUnlessEqual(updated_user.is_staff, True)
        self.failUnlessEqual(updated_user.is_superuser, True)
        
        updated_groups = user.groups.all()
        self.failUnlessEqual(len(updated_groups), 1)
        self.failUnlessEqual(updated_groups[0].id, 1)
        
    def test_create_user(self):

        # If user is not in the database.
        # User must be created.

        environ = {settings.META_USERNAME: 'user_new',
            settings.META_EMAIL: 'user_new@example.com',
            settings.META_FIRSTNAME: 'New',
            settings.META_LASTNAME: 'User'}

        client = Client()

        request_url = settings.LOGIN_URL
        client.get(request_url, ** environ)
        
        user = User.objects.get(username=environ[settings.META_USERNAME])
        self.failUnlessEqual(user.username, environ[settings.META_USERNAME])
        self.failUnlessEqual(user.email, environ[settings.META_EMAIL])
        self.failUnlessEqual(user.first_name, environ[settings.META_FIRSTNAME])
        self.failUnlessEqual(user.last_name, environ[settings.META_LASTNAME])
        self.failUnlessEqual(user.is_active, settings.SHIB_SSO_CREATE_ACTIVE)
        self.failUnlessEqual(user.is_staff, settings.SHIB_SSO_CREATE_STAFF)
        self.failUnlessEqual(user.is_superuser, settings.SHIB_SSO_CREATE_SUPERUSER)