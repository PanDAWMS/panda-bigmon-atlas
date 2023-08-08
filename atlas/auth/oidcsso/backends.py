from django.contrib.auth.backends import RemoteUserBackend
import json
import logging
_logger = logging.getLogger('prodtaskwebui')

class OIDCCernSSOBackend(RemoteUserBackend):
    create_unknown_user = False

    def authenticate(self, request, remote_user):
        try:
            user_info = json.loads(request.META.get('OIDC_userinfo_json'))
            user_groups = json.loads(request.META.get('OIDC_CLAIM_resource_access')).get('atlas-prodtask').get('roles')
            _logger.info(f'authenticate {user_info}')
            _logger.info(f'authenticate {user_groups}')
        except:
            pass

        return super().authenticate(request, remote_user)