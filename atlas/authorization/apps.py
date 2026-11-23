from django.apps import AppConfig


class AuthorizationConfig(AppConfig):
    name = 'atlas.authorization'

    def ready(self):
        # Refuse to start with a missing dependency, invalid model or bad bundle.
        from .service import get_engine
        get_engine()
