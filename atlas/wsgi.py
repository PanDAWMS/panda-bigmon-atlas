"""
WSGI config for bigpandamon project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/howto/deployment/wsgi/

further doc: http://thecodeship.com/deployment/deploy-django-apache-virtualenv-and-mod_wsgi/

"""

import os
import sys
import site
from os.path import join, pardir, abspath, dirname, split

### dummy settings settings_bigpandamon file with VIRTUALENV_PATH, WSGI_PATH
baseSettingsPath = '/data/bigpandamon_settings'
sys.path.append(baseSettingsPath)

#virtualenvPath = '/data/virtualenv/django1.6.1__python2.6.6__jedimon'
#virtualenvPath = '/data/virtualenv/django1.6.1__python2.6.6__atlas'
virtualenvPath = '/data/wenaus/virtualenv/twrpm'
path = virtualenvPath + '/pythonpath'
try:
#    from settings_bigpandamon_jedimon import VIRTUALENV_PATH
#    from settings_bigpandamon_jedimon import WSGI_PATH
    from settings_bigpandamon_twrpm import VIRTUALENV_PATH
    from settings_bigpandamon_twrpm import WSGI_PATH
    virtualenvPath = VIRTUALENV_PATH
    path = WSGI_PATH
except:
    print "Something went wrong with import of WSGI_PATH from settings."
    print "Staying with default path: %s" % path

# Add the site-packages of the chosen virtualenv to work with
site.addsitedir(virtualenvPath + '/lib/python2.6/site-packages')

# Add the app's directory to the PYTHONPATH
sys.path.append(path)
sys.path.append(path + '/pythonpath')

#os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bigpandamon.settings")
#os.environ["DJANGO_SETTINGS_MODULE"] = "atlas.settings"

# django settings module
DJANGO_SETTINGS_MODULE = '%s.%s' % (split(abspath(dirname(__file__)))[1], 'settings')
# pythonpath dirs
PYTHONPATH = [
    join(dirname(__file__), pardir),
]

# inject few paths to pythonpath
for p in PYTHONPATH:
    if p not in sys.path:
        sys.path.insert(0, p)


os.environ['DJANGO_SETTINGS_MODULE'] = DJANGO_SETTINGS_MODULE


# Activate your virtual env
activate_env = os.path.expanduser(virtualenvPath + '/bin/activate_this.py')
execfile(activate_env, dict(__file__=activate_env))

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

## Apply WSGI middleware here.
## from helloworld.wsgi import HelloWorldApplication
## application = HelloWorldApplication(application)
#


