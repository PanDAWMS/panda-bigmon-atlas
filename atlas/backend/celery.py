from __future__ import absolute_import

import os
import sys
from celery import Celery

# set the default Django settings module for the 'celery' program.
sys.path.insert(0,'..//')
sys.path.insert(0,'..//..//bigpandamon-core')
print sys.path
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'atlas.settings')

from django.conf import settings  # noqa

app = Celery('backend',
             broker='amqp://guest:guest@borodin-dev2.cern.ch//',
             backend='djcelery.backends.database:DatabaseBackend',)

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))