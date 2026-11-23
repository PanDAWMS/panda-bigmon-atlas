from django.contrib.admin.apps import AdminConfig


class AuthorizationAdminConfig(AdminConfig):
    default_site = 'atlas.authorization.admin.PolicyAdminSite'

    def ready(self):
        # Refuse to start with a missing dependency, invalid model or bad bundle.
        from .service import get_engine
        get_engine()
        super().ready()
