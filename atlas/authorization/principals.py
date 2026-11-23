"""Identity and membership facts from the existing Django authentication data."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class DelegatedIdentity:
    """Identity asserted by a service authorized to delegate this action.

    This principal is never saved. Local accounts keep their status and roles;
    unknown subjects have no group, superuser or direct-permission assignments.
    """
    username: str
    asserted_fullname: str
    delegated_by: str
    local_user: object = None

    @property
    def is_authenticated(self):
        return True

    @property
    def is_active(self):
        return self.local_user.is_active if self.local_user is not None else True

    @property
    def is_superuser(self):
        return self.local_user.is_superuser if self.local_user is not None else False

    @property
    def first_name(self):
        return self.local_user.first_name if self.local_user is not None else ''

    @property
    def last_name(self):
        return self.local_user.last_name if self.local_user is not None else ''


def group_codes(user):
    if not user.is_authenticated:
        return []
    if isinstance(user, DelegatedIdentity):
        if user.local_user is None:
            return []
        user = user.local_user
    codes = set()
    for group in user.groups.all():
        match = re.fullmatch(r'IAM:atlas/(.+)/production', group.name)
        if match:
            codes.add(match.group(1))
        for permission in group.permissions.all():
            if 'has_' in permission.name and '_permissions' in permission.name:
                codes.add(permission.codename)
    return sorted(codes)


def roles_for(user, groups=(), editor=False):
    if not user.is_authenticated or not user.is_active:
        return []
    roles = [role for role in ('DPD', 'REPR', 'MCCOORD', 'ALL') if role in groups]
    roles.append('subject')
    if user.is_superuser:
        roles.append('prodsys:superuser')
    if editor:
        roles.append('gdpconfig-editor')
    return roles


def user_for_username(username):
    from django.contrib.auth.models import AnonymousUser, User
    try:
        return User.objects.get(username=username)
    except User.DoesNotExist:
        return AnonymousUser()


def delegated_task_user_rights(actor, username, action, fullname=None):
    from types import SimpleNamespace
    from django.core.exceptions import PermissionDenied
    from .resources import can_delegate
    if not can_delegate(SimpleNamespace(user=actor), action):
        raise PermissionDenied('Service is not allowed to delegate this action')
    local_user = user_for_username(username)
    principal = DelegatedIdentity(username, fullname or '', actor.username,
                                  local_user if local_user.is_authenticated else None)
    return principal, group_codes(principal)
