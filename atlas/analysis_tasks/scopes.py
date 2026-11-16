from atlas.authorization.stage2 import observe_scope_list
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
    observe_scope_list(user, scopes, group_names)
    return scopes
