
from os.path import dirname, join

import atlas
import atlas.common
from .local import MY_SECRET_KEY, dbaccess, MY_CELERY, DEVELOPMENT, ADMIN_MAILS

ALLOWED_HOSTS = [
    ### cern.ch
    '.cern.ch',  # Allow domain and subdomains
    '.cern.ch.',  # Also allow FQDN and subdomains
    ### bigpanda.cern.ch
    'bigpanda.cern.ch',  # Allow domain and subdomains
    'bigpanda.cern.ch.',  # Also allow FQDN and subdomains
    ### pandawms.org
    '.pandawms.org',  # Allow domain and subdomains
    '.pandawms.org.',  # Also allow FQDN and subdomains

    '127.0.0.1', '.localhost'
]

admin_mails = ADMIN_MAILS

defaultDatetimeFormat = "%Y-%m-%d %H:%M:%S"


DATABASE_ROUTERS = ['atlas.dbrouter.ProdMonDBRouter']


STATICFILES_DIRS = [
        join(dirname(atlas.common.__file__), 'static'),
]

# if DEVELOPMENT:
#     STATICFILES_DIRS.append(join(dirname(atlas.__file__), 'frontendjs','prodsysjs','dist','prodsysjs'))

TEMPLATE_DIRS = (
    join(dirname(atlas.__file__), 'templates'),
)

STATIC_ROOT = join(dirname(atlas.__file__), 'static_media/')
#STATIC_ROOT = None
MEDIA_ROOT = join(dirname(atlas.__file__), 'static_media2/')
STATIC_URL_BASE = '/static/'
#STATIC_URL_BASE = '/jedimonstatic/'
MEDIA_URL_BASE = '/media/'
#MEDIA_URL_BASE = '/jedimonmedia/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = MY_SECRET_KEY

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases
DATABASES = dbaccess

### URL_PATH_PREFIX for multi-developer apache/wsgi instance
### on EC2: URL_PATH_PREFIX = '/bigpandamon' or URL_PATH_PREFIX = '/developersprefix'
#URL_PATH_PREFIX = '/atlas'
#URL_PATH_PREFIX = '/jedimon'
URL_PATH_PREFIX = ''
### on localhost:8000: URL_PATH_PREFIX = '/.'
#URL_PATH_PREFIX = ''
MEDIA_URL = URL_PATH_PREFIX + MEDIA_URL_BASE
STATIC_URL = URL_PATH_PREFIX + STATIC_URL_BASE

## init logger
## A sample logging configuration. The only tangible logging
## performed by this configuration is to send an email to
## the site admins on every HTTP 500 error when DEBUG=False.
## See http://docs.djangoproject.com/en/dev/topics/logging for
## more details on how to customize your logging configuration.
from .logconfig import LOGGING

AUTHENTICATION_BACKENDS = (
    # 'atlas.auth.shibsso.backends.ShibSSOBackend',
    "atlas.auth.oidcsso.backends.OIDCCernSSOBackend",
)


if DEVELOPMENT:
    DEBUG=True
    AUTHENTICATION_BACKENDS = (
     #  'atlas.auth.fake.backends.LoginAsBackend',

        "atlas.auth.oidcsso.backends.OIDCCernSSOBackend",
    )




SHIB_SSO_ADMIN = True
SHIB_SSO_CREATE_ACTIVE = True
SHIB_SSO_CREATE_STAFF = False
SHIB_SSO_CREATE_SUPERUSER = False
SHIB_LOGIN_PATH = '/Shibboleth.sso/?target='
SHIB_LOGOUT_URL = 'https://login.cern.ch/adfs/ls/?wa=wsignout1.0&returnurl='
OIDC_LOGIN_URL= '/sso/login/'
OIDC_USERNAME_FIELD = 'sub'
OIDC_USERINFO_JSON_PATH = 'OIDC_userinfo_json'
OIDC_GROUPS_CLAIM = 'OIDC_CLAIM_resource_access'
OIDC_CLIENT_ID = 'atlas-prodtask'
META_EMAIL = 'ADFS_EMAIL'
META_FIRSTNAME = 'ADFS_FIRSTNAME'
META_GROUP = 'ADFS_GROUP'
META_LASTNAME = 'ADFS_LASTNAME'
META_USERNAME = 'ADFS_LOGIN'
LOGIN_REDIRECT_URL = '/'
SYSTEM_HEALTH_STATUS_FILE = '/tmp/prodsys_status.txt'
#FAKE_LOGIN_AS_USER = 'username'

# SESSION

