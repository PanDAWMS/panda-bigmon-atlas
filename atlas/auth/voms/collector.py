#!/bin/env python

import re
from django.conf import settings

from .interface import VomsInterface
from .models import VomsUser

def run():
    """
    Update VOMS mapping nickname <-> (DN, CA) in the database
    :return: dictionary, 'added', 'removed' - records added/removed,
             'details' - more information on operations performed
    """
    options = VomsInterface.get_identity_options(proxy_cert=settings.VOMS_PROXY_CERT)
    options.update(settings.VOMS_OPTIONS)

    voms = VomsInterface(options)

    voms_users = {}

    for user_info in voms.list_users():
        dn = user_info["DN"]
        ca = user_info["CA"]
        try:
            nickname = voms.get_user_nickname(dn, ca)
        except:
            # TODO: log the error (e.g. user was removed during data collection)
            continue
        if not re.match(r"^\w+$", nickname):
            # TODO: log the warning
            continue

        if not voms_users.get(nickname):
            voms_users[nickname] = {}
        voms_users[nickname].update({dn: ca})

    result = {'added': 0, 'removed': 0, 'detailed': []}

    for user in VomsUser.objects.all():
        info = voms_users.get(user.username)
        if not info or not user.dn in info:
            try:
                user.delete()
            except:
                # TODO: log the error
                continue

            result['removed'] += 1
            result['detailed'].append({
                'action': 'remove', 'username': user.username,
                'dn': user.dn, 'ca': user.ca,
            })
            # TODO: log operation
            continue
        else:
            del voms_users[user.username][user.dn]

    for (nickname, info) in voms_users.items():
        for (dn, ca) in info.items():
            user = VomsUser()

            try:
                user = VomsUser.objects.get(username=nickname, dn=dn)
            except:
                user.username = nickname
                user.dn = dn
                user.ca = ca
                try:
                    user.save()
                except:
                    # TODO: log the error
                    continue
                result['added'] += 1
                result['detailed'].append({
                    'action': 'add', 'username': user.username,
                    'dn': user.dn, 'ca': user.ca,
                })

    return result


if __name__ == "__main__":
    run()
