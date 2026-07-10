from __future__ import absolute_import, unicode_literals

import time

from atlas.auth.oidcsso.utils import fill_user_groups_from_iam
from atlas.celerybackend.celery import app, ProdSysTask
from atlas.gpdeletion.views import collect_datasets, redo_all, do_gp_deletion_update, clean_superceeded
from atlas.prestage.views import find_action_to_execute, submit_all_tapes_processed_with_shares, \
    delete_done_staging_rules, \
    sync_cric_deft, find_repeated_tasks_to_follow, find_stage_task_replica_to_delete, remove_stale_rules, \
    clean_stale_actions, find_stale_stages, fill_staging_destination, check_stale_staging_tasks, cache_bad_rses, \
    get_stuck_requests
from atlas.prodtask.dataset_recovery import check_running_recovery_requests, check_submitted_recovery_requests
from atlas.prodtask.hashtag import hashtag_request_to_tasks
from atlas.prodtask.mcevgen import sync_cvmfs_db, set_pmg_hashtags
from atlas.prodtask.models import ProductionTask, DistributedLock
from atlas.prodtask.open_ended import check_open_ended
from atlas.prodtask.patch_reprocessing import find_done_patched_tasks
from atlas.prodtask.postproduction import check_all_tasks_post_production_actions
from atlas.prodtask.task_actions import do_new_action
from atlas.prodtask.task_views import sync_old_tasks, check_merge_container, find_filter_bkg_tasks

import logging

from atlas.production_request.views import fill_mc_stats_trend
from atlas.task_action.task_management import TaskActionExecutor, reload_analysis_tasks

_logger = logging.getLogger('prodtaskwebui')


@app.task(queue='test')
def test_celery():
    _logger.info('test celery')
    return 2

@app.task(ignore_result=True)
def sync_tasks():
    sync_old_tasks(-1)
    return None

@app.task(ignore_result=True)
def clean_expired_locks():
    DistributedLock.clean_locks()
    return None

@app.task(ignore_result=True)
def cache_dc_stats():
    cache_bad_rses(True)
    get_stuck_requests()
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
def create_filter_bkg():
    find_filter_bkg_tasks()
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

@app.task(ignore_result=True)
def run_reload_tasks():
    reload_analysis_tasks()
    return None

@app.task(ignore_result=True)
def check_submitted_recovery_requests_task():
    check_submitted_recovery_requests()
    return None

@app.task(ignore_result=True)
def check_running_recovery_requests_task():
    check_running_recovery_requests()
    return None

@app.task(ignore_result=True)
def postproduction():
    check_all_tasks_post_production_actions()
    return None

@app.task(ignore_result=True)
def check_pmg_merge_evgen():
    set_pmg_hashtags()
    return None

@app.task(bind=True, base=ProdSysTask)
@ProdSysTask.set_task_name('poll_jedi_async_result')
def poll_jedi_async_result(self, request_id: str, poll_interval: int = 5, max_polls=None,
                           original_func_name: str = '',
                           action_type: str = TaskActionExecutor.ActionType.GENERAL.value,
                           *original_args):
    """
    Poll JEDI for async request results and log completion.
    
    This task polls the get_result endpoint until the overall_status is 'complete',
    then logs using the action-type specific logger in TaskActionExecutor.
    
    Args:
        request_id: UUID from JEDI async submit endpoint
        poll_interval: seconds to wait between polls
        max_polls: maximum number of polls before giving up (None = no limit)
        original_func_name: name of the original JEDI method for logging
        action_type: action type used for lock selection and logging
        *original_args: arguments passed to the original method (first should be task_id if available)
    
    Returns:
        dict with final result or raises exception on failure
    """
    action_name = f'jedi_async_{original_func_name}'
    normalized_action_type = action_type.upper() if isinstance(action_type, str) else action_type
    try:
        resolved_action_type = TaskActionExecutor.ActionType(normalized_action_type)
    except ValueError as exc:
        raise ValueError(f"Unknown action_type '{action_type}' for request_id={request_id}") from exc

    lock_name = None
    if resolved_action_type in [TaskActionExecutor.ActionType.TASK, TaskActionExecutor.ActionType.DATASET]:
        if not original_args:
            raise ValueError(
                f"action_type={resolved_action_type.value} requires first positional arg to build lock key"
            )
        lock_name = f"async_jedi_tasks_{str(original_args[0])}"

    def _release_lock():
        if lock_name:
            try:
                DistributedLock.release_lock(lock_name)
                _logger.info(f"Released distributed lock '{lock_name}'")
            except Exception as lock_err:
                _logger.warning(f"Failed to release lock '{lock_name}': {lock_err}")

    action_executor = TaskActionExecutor('celery', 'async')

    def _log_by_action_type(return_code, return_message):
        if resolved_action_type == TaskActionExecutor.ActionType.TASK:
            from atlas.prodtask.models import ProductionTask
            task_id = original_args[0]
            task = ProductionTask.objects.filter(id=task_id).first()
            prod_request_id = None
            if task and task.request_id > 300:
                prod_request_id = task.request.reqid
            action_executor._log_production_task_action_message(
                'system',
                f'Async JEDI task: {original_func_name}',
                prod_request_id,
                task_id,
                action_name,
                return_code,
                return_message
            )
            return

        if resolved_action_type == TaskActionExecutor.ActionType.DATASET:
            dataset = original_args[0]
            action_executor._log_rule_action_message(dataset, action_name, return_code, return_message)
            return

        action_executor._log_general_action_message(action_name, return_code, return_message)

    poll_count = 0
    try:
        while True:
            poll_count += 1

            try:
                # Poll for results
                result = action_executor.get_result(request_id)

                _logger.debug(f"Poll #{poll_count} for request_id={request_id}: {result}")

                # Check if complete
                if isinstance(result, dict):
                    data = result.get('data', {})
                    overall_status = data.get('overall_status')
                    results = data.get('results', [])
                    message = data.get('message', '')

                    if overall_status == 'complete':
                        return_code = 0
                        return_message = f'JEDI async request completed successfully: {message}'
                        _log_by_action_type(return_code, return_message)
                        return {
                            'request_id': request_id,
                            'overall_status': overall_status,
                            'results': results,
                            'message': message,
                            'poll_count': poll_count
                        }
                    elif overall_status == 'pending':
                        # Not done yet, check if we've exceeded max polls
                        if max_polls and poll_count >= max_polls:
                            error_msg = f"Max polls ({max_polls}) exceeded for request_id={request_id}"
                            _logger.error(error_msg)
                            raise TimeoutError(error_msg)

                        # Sleep and retry
                        _logger.debug(f"Request {request_id} still pending, retrying in {poll_interval}s...")
                        self.progress_message_update(
                            min(poll_count * 10, 90),
                            additional_info={'request_id': request_id, 'status': 'pending'}
                        )
                        time.sleep(poll_interval)
                    else:
                        error_msg = f"Unknown overall_status '{overall_status}' for request_id={request_id}"
                        _logger.error(error_msg)
                        raise ValueError(error_msg)
                else:
                    error_msg = f"Unexpected result format from get_result: {result}"
                    _logger.error(error_msg)
                    raise ValueError(error_msg)

            except Exception as e:
                _logger.error(f"Error polling request_id={request_id}: {e}")
                _log_by_action_type(1, str(e))
                raise
    finally:
        _release_lock()
