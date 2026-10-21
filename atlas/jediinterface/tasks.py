"""Celery polling task for asynchronous JEDI requests."""

import logging
import time

from atlas.celerybackend.celery import app, ProdSysTask

_logger = logging.getLogger('prodtaskwebui')


@app.task(bind=True, base=ProdSysTask, name='atlas.prodtask.tasks.poll_jedi_async_result')
@ProdSysTask.set_task_name('poll_jedi_async_result')
def poll_jedi_async_result(self, request_id: str, poll_interval: int = 5, max_polls=None,
                           original_func_name: str = '',
                           action_type: str = 'GENERAL',
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
    # These dependencies are only needed by the worker, not when queuing from a console.
    from atlas.prodtask.models import DistributedLock
    from atlas.task_action.task_management import TaskActionExecutor

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
                    if 'async_meta' in result:
                        message = result.get('message', '')
                        results = result.get('data', [])
                        overall_status = result['async_meta']['status']
                    else:
                        data = result.get('data', {})
                        overall_status = data.get('overall_status')
                        results = data.get('results', [])
                        message = data.get('message', '')

                    if overall_status in ['complete', 'done']:
                        return_code = 0
                        return_message = f'JEDI async request completed successfully: {message}'
                        _log_by_action_type(return_code, return_message)
                        return {
                            'request_id': request_id,
                            'overall_status': 'done',
                            'results': results,
                            'message': message,
                            'poll_count': poll_count
                        }
                    elif overall_status in ['pending','running']:
                        # Not done yet, check if we've exceeded max polls
                        if max_polls and poll_count >= max_polls:
                            error_msg = f"Max polls ({max_polls}) exceeded for request_id={request_id}"
                            _logger.error(error_msg)
                            raise TimeoutError(error_msg)

                        # Sleep and retry
                        _logger.debug(f"Request {request_id} still {overall_status}, retrying in {poll_interval}s...")
                        self.progress_message_update(
                            min(poll_count * 10, 90),
                            additional_info={'request_id': request_id, 'status': 'pending'}
                        )
                        time.sleep(poll_interval)
                    elif overall_status == 'failed':
                        return_code = 1
                        return_message = f'JEDI async failed: {message}'
                        _log_by_action_type(return_code, return_message)
                        return {
                            'request_id': request_id,
                            'overall_status': 'failed',
                            'results': results,
                            'message': message,
                            'poll_count': poll_count
                        }
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
