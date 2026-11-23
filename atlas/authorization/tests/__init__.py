"""Minimal Django setup for standalone authorization tests."""

from django.conf import settings

if not settings.configured:
    settings.configure(SECRET_KEY='test', INSTALLED_APPS=[
        'django.contrib.contenttypes', 'django.contrib.auth', 'django.contrib.admin',
        'atlas.authorization.apps.AuthorizationConfig'], DATABASES={})
    import django
    django.setup()
