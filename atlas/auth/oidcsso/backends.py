from django.contrib.auth.backends import RemoteUserBackend
import json
import logging
from django.contrib.auth.models import Group
from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token
from django.conf import settings
from django.utils import timezone

_logger = logging.getLogger('prodtaskwebui')

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

class OIDCCernSSOBackend(RemoteUserBackend):
    create_unknown_user = False

    def authenticate(self, request, remote_user):
        try:
            if remote_user:

                user_info = json.loads(request.META.get(settings.OIDC_USERINFO_JSON_PATH))
                user_groups = json.loads(request.META.get(settings.OIDC_GROUPS_CLAIM)).get(settings.OIDC_CLIENT_ID).get('roles')
                _logger.info(f'authenticate {user_info}')
                _logger.info(f'authenticate {user_groups}')
                user = self._get_updated_user(remote_user, user_info.get('email'), user_info.get('given_name'), user_info.get('family_name'), user_groups)
        except:
            pass

        return super().authenticate(request, remote_user)

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
            user.last_login = timezone.now()
            user.email = email
            user.first_name = firstname
            user.last_name = lastname
            user.password = '(not used)'
            user.save()
        existing_groups = [group for group in map(get_group, groups) if group]
        for group in existing_groups:
                user.groups.add(group)
        user.save()
        Token.objects.get_or_create(user=user)
        return user