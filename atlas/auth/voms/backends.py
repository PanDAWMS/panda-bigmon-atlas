from django.conf import settings
from django.contrib.auth.backends import ModelBackend
from django.contrib.auth.models import Group
from django.contrib.auth.models import User
import logging

from .interface import VomsInterface
from .models import VomsUser

_logger = logging.getLogger('prodtaskwebui')

def add_group(group_name):
    try:
        group = Group.objects.get(name=group_name)
    except Group.DoesNotExist:
        group = Group(name=group_name)
        group.save()

    return group


class VomsBackend(ModelBackend):
    """ Adding VOMS-based authentication groups for user. """

    def authenticate(self, request=None):
        """ Checking user against VOMS data and adding corresponding authentication groups.

        Parameters:
          request: Http request (HttpRequest).

         Returns always None to pass further handling to ShibSSO module.

        """

        username = request.META.get(settings.META_USERNAME)

        if not username:
            return

        dn_map = {}
        for record in VomsUser.objects.filter(username=username):
            dn_map[record.dn] = record.ca

        if not dn_map:
            return
        try:
            voms = VomsInterface(settings.VOMS_OPTIONS)
            voms_groups = []

            for (dn, ca) in dn_map.items():
                vo_roles = voms.list_user_roles(dn, ca) or []
                vo_groups = voms.list_user_groups(dn, ca) or []
                vo_roles = ["vomsrole:" + x for x in vo_roles]
                vo_groups = ["vomsgroup:" + x for x in vo_groups]

                voms_groups = vo_groups + vo_roles


            for group in voms_groups:
                add_group(group)

            meta_groups = request.META.get(settings.META_GROUP, '')
            groups = ';'.join( [x for x in (voms_groups + [meta_groups]) if x] )
            request.META[settings.META_GROUP] = groups
        except SystemExit as e:
            _logger.error("Auth problem  %s" % e)

        return
