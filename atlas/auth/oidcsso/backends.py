from django.contrib.auth.backends import RemoteUserBackend
import logging
_logger = logging.getLogger('prodtaskwebui')

class OIDCCernSSOBackend(RemoteUserBackend):
    create_unknown_user = False

    def authenticate(self, request, remote_user):
        _logger.info(f'authenticate {remote_user}')
        _logger.info(f'authenticate {request.META}')
        return super().authenticate(request, remote_user)