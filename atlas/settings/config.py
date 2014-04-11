
from os.path import dirname, join

import core
import atlas

from .local import MY_SECRET_KEY, dbaccess

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
]


### VIRTUALENV
VIRTUALENV_PATH = '/data/virtualenv/django1.6.1__python2.6.6__jedimon'

### WSGI
WSGI_PATH = VIRTUALENV_PATH + '/pythonpath'

DATABASE_ROUTERS = ['atlas.dbrouter.ProdMonDBRouter']

STATICFILES_DIRS = (
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(dirname(core.common.__file__), 'static'),
#    join(join(dirname(core.__file__),'datatables'), 'static'),
#    join(dirname(atlas.__file__), 'static'),
)

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(dirname(core.common.__file__), 'templates'),
    join(dirname(atlas.__file__), 'templates'),
)

STATIC_ROOT = join(dirname(atlas.__file__), 'static')
#STATIC_ROOT = None
MEDIA_ROOT = join(dirname(atlas.__file__), 'media')
STATIC_URL_BASE = '/jedimonstatic/'
MEDIA_URL_BASE = '/jedimonmedia/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = MY_SECRET_KEY

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases
DATABASES = dbaccess

### URL_PATH_PREFIX for multi-developer apache/wsgi instance
### on EC2: URL_PATH_PREFIX = '/bigpandamon' or URL_PATH_PREFIX = '/developersprefix'
#URL_PATH_PREFIX = '/atlas'
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
#LOG_SIZE = 1000000000
#LOGGING = {
#    'version': 1,
#    'disable_existing_loggers': False,
##    'disable_existing_loggers': True,
#    'filters': {
#        'require_debug_false': {
#            '()': 'django.utils.log.RequireDebugFalse'
#        }
#    },
#    'handlers': {
#        'null': {
#            'level':'DEBUG',
#            'class':'django.utils.log.NullHandler',
#        },
#        'logfile-bigpandamon': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.bigpandamon",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-prodtaskwebui': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.prodtaskwebui",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-django': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.django",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-viewdatatables': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.viewdatatables",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-rest': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.rest",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-jedi_jobsintask': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.jedi_jobsintask",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-jedi_extra': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.jedi_extra",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-todoview': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.todoview",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-table_datatable': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.table_datatable",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'logfile-django_datatable': {
#            'level':'DEBUG',
#            'class':'logging.handlers.RotatingFileHandler',
#            'filename': LOG_ROOT + "/logfile.django_datatable",
#            'maxBytes': LOG_SIZE,
#            'backupCount': 2,
#            'formatter': 'verbose',
#        },
#        'mail_admins': {
#            'level': 'ERROR',
#            'filters': ['require_debug_false'],
##            'class': 'django.utils.log.AdminEmailHandler'
#            'class':'logging.StreamHandler',
#        }
#    },
#    'loggers': {
#        'django.request': {
#            'handlers': ['mail_admins'],
##            'level': 'ERROR',
#            'level': 'DEBUG',
#            'propagate': True,
#        },
#        'django': {
#            'handlers':['logfile-django'],
#            'propagate': True,
#            'level':'DEBUG',
#        },
#        'django_datatables_view': {
#            'handlers':['logfile-viewdatatables'],
#            'propagate': True,
#            'level':'DEBUG',
#        },
#        'rest_framework': {
#            'handlers':['logfile-rest'],
#            'propagate': True,
#            'level':'DEBUG',
#        },
#        'bigpandamon': {
#            'handlers': ['logfile-bigpandamon'],
#            'level': 'DEBUG',
#        },
#        'prodtaskwebui': {
#            'handlers': ['logfile-prodtaskwebui'],
#            'level': 'DEBUG',
#        },
#        'jedi_jobsintask': {
#            'handlers': ['logfile-jedi_jobsintask'],
#            'level': 'DEBUG',
#        },
#        'postproduction': {
#            'handlers': ['logfile-postproduction'],
#            'level': 'DEBUG',
#        },
#        'jedi_extra': {
#            'handlers': ['logfile-jedi_extra'],
#            'level': 'DEBUG',
#        },
#        'todoview': {
#            'handlers': ['logfile-todoview'],
#            'level': 'DEBUG',
#        },
#        'table_datatable': {
#            'handlers': ['logfile-table_datatable'],
#            'level': 'DEBUG',
#        },
#        'django_datatable': {
#            'handlers': ['logfile-django_datatable'],
#            'level': 'DEBUG',
#        },
#    },
#    'formatters': {
#        'verbose': {
##            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
#            'format': '%(asctime)s %(module)s %(name)-12s:%(lineno)d %(levelname)-5s %(message)s'
#        },
#        'simple': {
#            'format': '%(levelname)s %(name)-12s:%(lineno)d %(message)s'
#        },
#    },
#    'logfile': {
#        'level':'DEBUG',
#        'class':'logging.handlers.RotatingFileHandler',
#        'filename': LOG_ROOT + "/logfile",
#        'maxBytes': LOG_SIZE,
#        'backupCount': 5,
#        'formatter': 'verbose',
#    },
#}


