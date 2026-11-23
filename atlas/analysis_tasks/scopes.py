from atlas.authorization.resources import can_scope
import re

from django.contrib.auth.models import User


def container_scopes_by_user(username: str) -> list[str]:
    scopes = [f'user.{username}']
    user = User.objects.get(username=username)
    group_names = []
    for group in user.groups.all():
        group_names.append(group.name)
        match = re.match(r'^IAM:atlas/(.+)/production$', group.name)
        if match:
            scopes.append(f'group.{match.group(1)}')
    return [scope for scope in scopes if can_scope(user, scope, action='list', group_names=group_names, audit=False)]
