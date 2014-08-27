from django.conf import settings
from django.contrib import auth


class VomsMiddleware(object):
    """
    Updates user's groups once a session.
    """

    def process_request(self, request):
        username = request.META.get(settings.META_USERNAME)

        if username and not request.session.get('VOMS_GROUPS_INITIALIZED'):
            if auth.authenticate(request=request):
                request.session['VOMS_GROUPS_INITIALIZED'] = 1
