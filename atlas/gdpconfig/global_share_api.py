"""Read API for the Angular global shares page; legacy endpoints stay unchanged."""

import logging

from rest_framework.authentication import BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from atlas.prodtask.models import GlobalShare
from atlas.task_action.task_management import task_user_rights


_logger = logging.getLogger('prodtaskwebui')
MC_LEVEL2_SHARES = {'MC evgen', 'MC merge', 'MC root', 'MC simul'}


def manageable_global_shares(user, shares):
    """Mirror global_share_change permissions for each individual share.

    The legacy writer authorizes whole batches. Clients should submit siblings
    together, rather than combine changes across permission scopes or MC levels.
    """
    if user.user_permissions.filter(name='Can edit GDPConfig').exists():
        return [share['name'] for share in shares]

    _, allowed_groups = task_user_rights(user.username)
    return [
        share['name'] for share in shares
        if ('DPD' in allowed_groups and share['parent'] == 'Derivations')
        or ('REPR' in allowed_groups and share['parent'] == 'Reprocessing')
        or ('MCCOORD' in allowed_groups and (
            share['name'] in MC_LEVEL2_SHARES or share['parent'] in MC_LEVEL2_SHARES
        ))
    ]


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def global_share_tree_v2(request):
    """Return the complete hierarchy as parent links plus editable share names."""
    try:
        shares = [
            {'name': share['name'], 'parent': share['parent'] or None, 'value': int(share['value'])}
            for share in GlobalShare.objects.order_by('name').values('name', 'parent', 'value')
        ]
        return Response({
            'shares': shares,
            'manageable_shares': manageable_global_shares(request.user, shares),
        })
    except Exception:
        _logger.exception('Unable to load global shares for %s', request.user.username)
        return Response({'detail': 'Unable to load global shares.'}, status=500)
