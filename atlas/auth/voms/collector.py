from django.conf import settings

from .interface import VomsInterface

from .models import VomsUser

def run():
    options = VomsInterface.get_identity_options()
    options.update(settings.VOMS_OPTIONS)

    voms = VomsInterface(options)

    voms_users = {}

    for user_info in voms.list_users():
        dn = user_info["DN"]
        ca = user_info["CA"]
        nickname = voms.get_user_nickname(dn, ca)

        if not voms_users.get(nickname):
            voms_users[nickname] = {}
        voms_users[nickname].update({dn: ca})

    for user in VomsUser.objects.all():
        info = voms_users.get(user.username)
        if not info or not (dn in voms_users[user.username]):
            user.delete()
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
                user.save()


if __name__ == "__main__":
    run()

