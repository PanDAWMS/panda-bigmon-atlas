from __future__ import absolute_import, unicode_literals

import time

from atlas.auth.oidcsso.utils import fill_user_groups_from_iam
from atlas.celerybackend.celery import app, ProdSysTask
from atlas.gpdeletion.views import collect_datasets, redo_all, do_gp_deletion_update, clean_superceeded
from atlas.prestage.views import find_action_to_execute, submit_all_tapes_processed_with_shares, \
    delete_done_staging_rules, \
    sync_cric_deft, find_repeated_tasks_to_follow, find_stage_task_replica_to_delete, remove_stale_rules, \
    clean_stale_actions, find_stale_stages, fill_staging_destination, check_stale_staging_tasks
from atlas.prodtask.hashtag import hashtag_request_to_tasks
from atlas.prodtask.mcevgen import sync_cvmfs_db
from atlas.prodtask.models import ProductionTask
from atlas.prodtask.open_ended import check_open_ended
from atlas.prodtask.patch_reprocessing import find_done_patched_tasks
from atlas.prodtask.task_actions import do_new_action
from atlas.prodtask.task_views import sync_old_tasks, check_merge_container
from functools import wraps

import logging

from atlas.production_request.views import fill_mc_stats_trend
from atlas.task_action.task_management import TaskActionExecutor

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
    submit_all_tapes_processed_with_shares()
    return None


@app.task(ignore_result=True)
def open_ended():
    check_open_ended()
    return None


@app.task(ignore_result=True)
def request_hashtags():
    hashtag_request_to_tasks()
    return None


@app.task(ignore_result=True)
def sync_evgen_jo():
    sync_cvmfs_db()
    return None

@app.task(ignore_result=True)
def remove_done_staging():
    find_stage_task_replica_to_delete()
    return None


@app.task(ignore_result=True)
def cric_profile_sync():
    sync_cric_deft()
    return None

@app.task
def async_tasks_action(username, task_ids, action, comment, *args):
    result = [do_new_action(username, x, action, comment, *args) for x in task_ids]
    return result

@app.task(ignore_result=True)
def find_DC_existsed_replica_tasks():
    find_repeated_tasks_to_follow()
    return None


@app.task(time_limit=86400)
def collect_gp(data, exclude_list):
    redo_all(data, exclude_list)
    return True


@app.task(time_limit=7200)
def gp_deletion_update():
    do_gp_deletion_update()
    return True

@app.task(time_limit=10800)
def gp_deletion_update_with_cleaning():
    do_gp_deletion_update()
    clean_superceeded()
    return True




@app.task(bind=True, base=ProdSysTask)
@ProdSysTask.set_task_name('test task')
def test_async_progress(self, a):
    for i in range(10):
        time.sleep(10)
        self.progress_message_update(i*10)
    if a == 'bad':
        raise Exception('Something Wrong')
    return 'finished: '+str(a)


@app.task(ignore_result=True)
def check_single_tag_containers():
    check_merge_container(3)
    return None


@app.task(ignore_result=True)
def remove_stale_staging_rules():
    remove_stale_rules(9)
    return None


@app.task(ignore_result=True)
def clean_stale_action_task():
    clean_stale_actions()
    return None

@app.task(ignore_result=True)
def fill_mc_subcampaign_trend():
    fill_mc_stats_trend()
    return None


@app.task(ignore_result=True)
def rebalance_tape_carousel():
    find_stale_stages(8)
    return None

@app.task(ignore_result=True)
def log_external_task_action(action, username, body, status):
    if 'task_id' in body:
        try:
            task_id = int(body['task_id'])
            if ProductionTask.objects.filter(id=task_id).exists:
                task = ProductionTask.objects.get(id=task_id)
                jedi_info = status['jedi_info']
                args = []
                for key,value in body.items():
                    if key != 'task_id':
                        args.append(value)
                TaskActionExecutor._log_production_task_action_message(username, '', task.request_id, task.id, action, jedi_info['return_code'],
                                                     jedi_info['return_info'] or '', *args)
        except Exception as ex:
            _logger.error(f'Problem action logging {ex}')
    return None


@app.task(ignore_result=True)
def fill_staging_rse():
    fill_staging_destination()
    return None

@app.task(ignore_result=True)
def resume_staling_staging_tasks():
    check_stale_staging_tasks()
    return None

@app.task(ignore_result=True)
def find_reprocessing_patched_tasks():
    find_done_patched_tasks()
    return None

@app.task(ignore_result=True)
def sync_users_with_IAM(update_only_new=False):
    fill_user_groups_from_iam(update_only_new)
    return None