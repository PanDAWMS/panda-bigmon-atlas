
from os.path import dirname, join


import atlas


VERSIONS = {
    'atlas': atlas.__versionstr__,
}
# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
#    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
#     'django.template.loaders.eggs.Loader',
)


MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',  # for AJAX POST protection with csrf
    'django.contrib.auth.middleware.AuthenticationMiddleware',
### added
    'django.contrib.auth.middleware.RemoteUserMiddleware',  # for APIs: htcondorapi
### END added
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
#### django-debug-toolbar
#    'debug_toolbar.middleware.DebugToolbarMiddleware',
###
#    'django.middleware.common.CommonMiddleware',
#    'django.contrib.sessions.middleware.SessionMiddleware',
#    'django.middleware.csrf.CsrfViewMiddleware',  # for AJAX POST protection with csrf
#    'django.contrib.auth.middleware.AuthenticationMiddleware',
#    'django.contrib.messages.middleware.MessageMiddleware',
#    # Uncomment the next line for simple clickjacking protection:
#    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
)


TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(dirname(atlas.__file__), 'templates'),

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

    'atlas.prodtask',
    'atlas.prodjob',
    'atlas.reqtask',
    'atlas.datatables',
    'atlas.gdpconfig',
     'djangojs',
    'django_datatables_view',
    ### atlas.todoview: Placeholder for views which need to be implemented
    ### as part of cross-linking between jobs and tasks monitoring
    'atlas.getdatasets',
    'django_tables2',#pip install django_tables2
)
INSTALLED_APPS =  INSTALLED_APPS_BIGPANDAMON_ATLAS
JS_I18N_APPS_EXCLUDE = INSTALLED_APPS_BIGPANDAMON_ATLAS + ('django_tables2',)

TEMPLATE_CONTEXT_PROCESSORS = (
    'django.contrib.auth.context_processors.auth',
    'django.contrib.messages.context_processors.messages',
    'django.core.context_processors.request', #django-tables2
)


ROOT_URLCONF = 'atlas.urls'

SITE_ID = 2

# email
EMAIL_SUBJECT_PREFIX = 'bigpandamon-atlas: '


