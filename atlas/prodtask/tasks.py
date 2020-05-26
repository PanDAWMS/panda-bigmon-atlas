from __future__ import absolute_import, unicode_literals

from atlas.celerybackend.celery import app
from atlas.prodtask.task_views import sync_old_tasks

import logging
_logger = logging.getLogger('prodtaskwebui')


@app.task
def test_celery():
    _logger.info('test celery')
    return 2

@app.task(ignore_result=True)
def sync_tasks():
    sync_old_tasks(-1)
    return 2