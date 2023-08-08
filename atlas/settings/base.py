
from os.path import dirname, join

from .local import MY_CELERY, DEVELOPMENT
import atlas

VERSIONS = {
    'atlas': atlas.__versionstr__,
}

STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

AUTH_MIDDL = "django.contrib.auth.middleware.PersistentRemoteUserMiddleware"
if DEVELOPMENT:
    AUTH_MIDDL = "atlas.auth.fake.middleware.FakeRemoteUserMiddleware"


MIDDLEWARE = (

    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
     AUTH_MIDDL,
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)


TEMPLATE_DIRS = (

    join(dirname(atlas.__file__), 'templates'),
    join(dirname(atlas.__file__), 'datatables/templates'),

)

INSTALLED_APPS_BIGPANDAMON_ATLAS = (
    'django.contrib.auth',
    'django.contrib.staticfiles',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.admin',
    'django.contrib.admindocs',
    'atlas.auth.voms',
    'rest_framework.authtoken',
    'rest_framework',
    'atlas.prodtask',
    'atlas.prodjob',
    'atlas.reqtask',
    'atlas.datatables',
    'atlas.gdpconfig',
    'atlas.dkb',
    'atlas.prestage',
    'atlas.request_pattern',
    'atlas.special_workflows',
    'atlas.getdatasets',
    # 'atlas.auth.shibsso',
    'atlas.ami',
    'django_tables2',
    'atlas.celerybackend',
    'django_celery_results',
    'django_celery_beat',
    'atlas.frontendjs',
    'atlas.frontenddjango',
    'atlas.prodtask_api',
    'atlas.gpdeletion'


)
INSTALLED_APPS =  INSTALLED_APPS_BIGPANDAMON_ATLAS
JS_I18N_APPS_EXCLUDE = INSTALLED_APPS_BIGPANDAMON_ATLAS + ('django_tables2',)

TEMPLATE_CONTEXT_PROCESSORS = (
    'django.contrib.auth.context_processors.auth',
    'django.contrib.messages.context_processors.messages',
    'django.template.context_processors.request'
)

TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
#     'django.template.loaders.eggs.Loader',
)

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': TEMPLATE_DIRS,
        'OPTIONS': {
            'context_processors': TEMPLATE_CONTEXT_PROCESSORS,
            'loaders': TEMPLATE_LOADERS,

        },
    },

]

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
        'LOCATION': 'prodtask_cache',
        'OPTIONS': {
            'MAX_ENTRIES': 3000
        }
    }
}

LANGUAGE_CODE = 'en-us'
LANGUAGE_NAME = 'English'
LANGUAGE_NAME_LOCAL = 'English'

TIME_ZONE = 'UTC'

USE_TZ = True
USE_L10N = True
USE_I18N = True

ROOT_URLCONF = 'atlas.urls'

SITE_ID = 2

# email
EMAIL_SUBJECT_PREFIX = 'bigpandamon-atlas: '


CELERY_RESULT_BACKEND = 'django-db'
CELERY_BROKER_URL = MY_CELERY
if DEVELOPMENT:
    CELERY_TASK_ROUTES = {'atlas.prodtask.tasks.test_async_progress': {'queue': 'test'},
                          'atlas.prodtask.step_manage_views.async_obsolete_old_task_for_slices': {'queue': 'test'}}

DATA_UPLOAD_MAX_MEMORY_SIZE = 50214400

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'