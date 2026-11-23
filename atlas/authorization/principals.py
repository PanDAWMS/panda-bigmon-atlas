"""Identity and membership facts from the existing Django authentication data."""

import re


def group_codes(user):
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
