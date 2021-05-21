import os
from functools import wraps

from celery import Celery, Task
from celery.result import AsyncResult
from celery.worker.request import Request
import logging


import sys


_jsonLogger = logging.getLogger('prodtask_ELK')

sys.path.insert(0,'..//')
# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'atlas.settings')

app = Celery('celerybackend')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))


class ProdSysCeleryRequest(Request):
    'A minimal custom request to log task start'

    def on_accepted(self, pid, time_accepted):
        super(ProdSysCeleryRequest, self).on_accepted(pid, time_accepted)
        _jsonLogger.info('Celery task accepted', extra={'celery_task_id':self.task_id,'celery_task_name':self.task_name})


class ProdSysTask(Task):
    Request = ProdSysCeleryRequest

    _prodsys_celery__task_name = 'default'

    @property
    def prodsys_celery_task_name(self):
        return self._prodsys_celery__task_name

    @prodsys_celery_task_name.setter
    def prodsys_celery_task_name(self, value):
        self._prodsys_celery__task_name = value

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        super(ProdSysTask, self).after_return(status, retval, task_id, args, kwargs, einfo)
        if einfo:
            _jsonLogger.error('Celery task failed', extra={'celery_task_id':task_id,'celery_task_name':self.name,'celery_status':status,'celery_error':einfo})
        else:
            _jsonLogger.info('Celery task finsihed', extra={'celery_task_id':task_id,'celery_task_name':self.name,'celery_status':status})


    def progress_message_update(self, value, total=None):
            if not self.request.called_directly:
                if total:
                    self.update_state(state="PROGRESS", meta={'processed': value, 'total':total, 'name':self.prodsys_celery_task_name})
                else:
                    self.update_state(state="PROGRESS", meta={'progress': value, 'name':self.prodsys_celery_task_name})


    @staticmethod
    def set_task_name(task_name):
        def decorator(function):
            @wraps(function)
            def wrapper(self, *args, **kwargs):
                self.prodsys_celery_task_name = task_name
                result = function(self, *args, **kwargs)
                return result
            return wrapper
        return decorator




