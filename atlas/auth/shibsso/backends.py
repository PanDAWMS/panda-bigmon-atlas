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

"""Backend for Shibboleth (R) authentication."""

from django.conf import settings
from django.contrib.auth.backends import ModelBackend
from django.contrib.auth.models import Group
from django.contrib.auth.models import User

def get_group(group_name):
    """Get the group instance present in the database.

       Parameters:
           group_name: name of the group (str).

        Returns an instance of the group (Group). None if the group does not
        exist.

    """

    try:
        group = Group.objects.get(name=group_name)
    except Group.DoesNotExist:
        return None

    return group

class ShibSSOBackend(ModelBackend):
    """Authenticates against Shibboleth (R) data."""

    def authenticate(self, request=None):
        """Authenticates the user against Shibboleth (R) data.

        Parameters:
          request: Http request (HttpRequest).

         Returns an instance of the authenticated user (User). None if the
         authentication credentials are invalid.
         
        """

        username = request.META.get(settings.META_USERNAME)

        if not username:
            return

        email = request.META.get(settings.META_EMAIL, '')
        firstname = request.META.get(settings.META_FIRSTNAME, '')
        lastname = request.META.get(settings.META_LASTNAME, '')
        groups = request.META.get(settings.META_GROUP) or ''

        #groups = map(str.strip, filter(None, groups.split(';')))

        groups_arr = groups.split(';')
        groups = [ x.strip() for x in groups_arr if x ]

        user = ShibSSOBackend._get_updated_user(username, email,
                                                firstname, lastname, groups)

        return user

    @staticmethod
    def _get_updated_user(login, email, firstname, lastname,
                          groups):
        """Get an updated instance of a user. The following fields are updated:
        username, email, first_name, last_name, password (to '(not used)') and
        is_active. Group list are reset, only groups that exist on the Group
        table are set for the user. If the user does not exist in the database,
        a new instance is created.

        Parameters:
           login: login of the authenticated user (str).
           email: email of the authenticated user (str).
           firstname: firstname of the authenticated user (str).
           lastname: lastname of the authenticated user (str).
           groups: group list of the authenticated user (list<str>).

        Returns an instance of the authenticated user (User).

        """

        try:
            user = User.objects.get(username=login)
        except User.DoesNotExist:
            user = User()
            user.username = login
            user.is_active = settings.SHIB_SSO_CREATE_ACTIVE
            user.is_staff = settings.SHIB_SSO_CREATE_STAFF
            user.is_superuser = settings.SHIB_SSO_CREATE_SUPERUSER
#            user.save()

        user.email = email
        user.first_name = firstname
        user.last_name = lastname
        user.password = '(not used)'
        user.save()
        current_groups = [_f for _f in map(get_group, groups) if _f]
        user.groups.set(current_groups)

        user.save()

        return user

#    _get_updated_user = staticmethod(_get_updated_user)
    
