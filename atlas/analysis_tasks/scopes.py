import re

from django.contrib.auth.models import User


def container_scopes_by_user(username: str) -> list[str]:
    scopes = [f'user.{username}']
    for group in User.objects.get(username=username).groups.all():
        match = re.match(r'^IAM:atlas/(.+)/production$', group.name)
        if match:
            scopes.append(f'group.{match.group(1)}')
    return scopes
