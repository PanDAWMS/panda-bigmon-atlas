"""
    atlas logconfig
"""
import datetime
from logging.handlers import TimedRotatingFileHandler

from .local import LOG_ROOT
LOG_SIZE = 1000000000
import logging, socket

class ContextFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = ContextFilter.hostname
        record.logName = '.'.join(record.pathname.split('/')[-2:])
        record.logName = record.logName[:record.logName.rfind('.')]
        return True

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
#    'disable_existing_loggers': True,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        },
        'context_filter':{
            '()': 'atlas.settings.logconfig.ContextFilter'
        }
    },
    'handlers': {
        'null': {
            'level':'DEBUG',
            'class':'logging.NullHandler',
        },
        'logfile-django': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.django",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-viewdatatables': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.viewdatatables",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-rest': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.rest",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-bigpandamon': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.bigpandamon",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-jedi_jobsintask': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.jedi_jobsintask",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-jedi_extra': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.jedi_extra",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-users_extra': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.users_extra",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-user_views': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.user_views",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-api_reprocessing': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.api_reprocessing",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
#            'class': 'django.utils.log.AdminEmailHandler'
            'class':'logging.StreamHandler',
        }
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins'],
#            'level': 'ERROR',
            'level': 'DEBUG',
            'propagate': True,
        },
        'django': {
            'handlers':['logfile-django'],
            'propagate': True,
            'level':'DEBUG',
        },
        'django_datatables_view': {
            'handlers':['logfile-viewdatatables'],
            'propagate': True,
            'level':'DEBUG',
        },
        'rest_framework': {
            'handlers':['logfile-rest'],
            'propagate': True,
            'level':'DEBUG',
        },
        'bigpandamon': {
            'handlers': ['logfile-bigpandamon'],
            'level': 'DEBUG',
        },
        'jedi_jobsintask': {
            'handlers': ['logfile-jedi_jobsintask'],
            'level': 'DEBUG',
        },
        'jedi_extra': {
            'handlers': ['logfile-jedi_extra'],
            'level': 'DEBUG',
        },
        'users_extra': {
            'handlers': ['logfile-users_extra'],
            'level': 'DEBUG',
        },
        'user_views': {
            'handlers': ['logfile-user_views'],
            'level': 'DEBUG',
        },
        'api_reprocessing':{
            'handlers': ['logfile-api_reprocessing'],
            'level': 'DEBUG',
        }
    },
    'formatters': {
        'verbose': {
#            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
            'format': '%(asctime)s %(module)s %(name)-12s:%(lineno)d %(levelname)-5s %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(name)-12s:%(lineno)d %(message)s'
        },
        "json": {"()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                 'format': '%(asctime)s %(levelname)s %(hostname)s %(logName)s %(funcName)s %(lineno)d %(message)s'}
    },
    'logfile': {
        'level':'DEBUG',
        'class':'logging.handlers.RotatingFileHandler',
        'filename': LOG_ROOT + "/logfile",
        'maxBytes': LOG_SIZE,
        'backupCount': 5,
        'formatter': 'verbose',
    },
}


def appendLogger(loggername, loggerlevel='DEBUG', \
                 loggerclass='logging.handlers.RotatingFileHandler', backupCount=2):
    """
        appendLogger - append new logger properties

        :param loggername: name of the logger, e.g. 'bigpandamon'
        :type loggername: string
    """
    global LOGGING, LOG_SIZE, LOG_ROOT

    handler = 'logfile-' + str(loggername)
    filename = LOG_ROOT + '/logfile.' + str(loggername)

    LOGGING['handlers'][handler] = \
    {
        'level':loggerlevel, \
        'class':loggerclass, \
        'filename': filename, \
        'maxBytes': LOG_SIZE, \
        'backupCount': backupCount, \
        'formatter': 'verbose', \
    }
    LOGGING['loggers'][loggername] = \
    {
        'handlers': [handler], \
        'level': loggerlevel, \
    }

def appendJsonLogger(loggername, loggerlevel='DEBUG',  loggerclass='logging.handlers.TimedRotatingFileHandler'):

    global LOGGING, LOG_SIZE, LOG_ROOT

    handler = 'logfile-' + str(loggername)
    filename = LOG_ROOT + '/json/logfile.' + str(loggername)

    LOGGING['handlers'][handler] = {
            'level':loggerlevel,
            'class':loggerclass,
            'filename': filename,
            'when':"midnight",
            'backupCount':5,
            'formatter': 'json',
            'filters':['context_filter']
            }
    LOGGING['loggers'][loggername] = {'handlers': [handler], 'level': loggerlevel}



### More Django related logging
### table_datatable
appendLogger('table_datatable')

### django_datatable
appendLogger('django_datatable')


### ProdSys2 logging
### prodtaskwebui
appendLogger('prodtaskwebui')

### postproduction
appendLogger('postproduction')

appendLogger('prodtask_messaging', backupCount=0)

appendJsonLogger('prodtask_ELK')

