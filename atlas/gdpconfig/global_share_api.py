"""Read API for the Angular global shares page; legacy endpoints stay unchanged."""

from atlas.authorization.resources import manageable_shares

import logging

from rest_framework.authentication import BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from atlas.prodtask.models import GlobalShare


_logger = logging.getLogger('prodtaskwebui')
MC_LEVEL2_SHARES = {'MC evgen', 'MC merge', 'MC root', 'MC simul'}


def manageable_global_shares(user, shares):
    """Evaluate policy capabilities for each individual share.

    The legacy writer authorizes whole batches. Clients should submit siblings
    together, rather than combine changes across permission scopes or MC levels.
    """
    return manageable_shares(user, shares)


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
