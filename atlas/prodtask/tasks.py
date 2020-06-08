from __future__ import absolute_import, unicode_literals

from atlas.celerybackend.celery import app
from atlas.prestage.views import find_action_to_execute, submit_all_tapes_processed
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
    return None


@app.task(ignore_result=True)
def step_actions():
    find_action_to_execute()
    return None

@app.task(ignore_result=True)
def data_carousel():
    submit_all_tapes_processed()
    return None
