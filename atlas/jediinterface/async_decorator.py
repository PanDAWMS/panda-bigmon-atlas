"""
Decorator for running JEDIClient methods asynchronously with Celery task polling.
"""

from functools import wraps
from typing import Callable, Any, Optional
import logging

_logger = logging.getLogger('prodtaskwebui')


def jedi_async_task(poll_interval: int = 5, action_type: str = 'GENERAL', max_polls: Optional[int] = None):
    """
    Decorator to run a JEDIClient async method as a Celery task.
    
    This decorator creates a Celery task that:
    1. Calls the decorated JEDIClient method to submit the async request
    2. Polls for results using get_result() with the specified interval
    3. Logs the completion using the action-type specific logger
    
    Args:
        poll_interval: seconds to wait between result polls (default: 5)
        max_polls: maximum number of polls before giving up (default: None, no limit)
        action_type: action type passed to poll_jedi_async_result (TASK, DATASET, GENERAL).
                     Locking is enabled automatically for TASK and DATASET, and disabled for GENERAL.

    Returns:
        The decorated function which returns the Celery task ID on success,
        or raises an exception on failure.
        
    Example:
        @jedi_async_task(poll_interval=10, action_type='TASK')
        def submit_some_request(self, task_id, params):
            # Must return a dict with 'request_id' key from JEDI
            return self._post_new_api_command('api/v1/...', {...})
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs) -> Any:
            # Import here to avoid circular imports
            from atlas.prodtask.tasks import poll_jedi_async_result

            lock_name = None
            resolved_action_type = action_type.upper() if isinstance(action_type, str) else action_type
            needs_lock = resolved_action_type in {'TASK', 'DATASET'}

            if needs_lock:
                if not args:
                    raise ValueError(
                        f"action_type={resolved_action_type} requires at least one positional argument as the lock key in {func.__name__}"
                    )
                lockkey = str(args[0])
                lock_name = f"async_jedi_tasks_{lockkey}"

                from atlas.prodtask.models import DistributedLock
                if not DistributedLock.acquire_lock(lock_name, lock_timeout=86400):
                    raise RuntimeError(f"A single async action for {lockkey} is allowed at a time")

            result = {'success':False, 'message':''}
            try:
                # Call the original function to get the request_id
                result = func(self, *args, **kwargs)
            except Exception:
                # Release lock immediately if the initial call fails
                if lock_name:
                    from atlas.prodtask.models import DistributedLock
                    DistributedLock.release_lock(lock_name)
                raise

            # Extract request_id from result
            if isinstance(result, dict):
                request_id = result.get('data', {}).get('request_id')
            else:
                request_id = None
                
            if not request_id:
                if lock_name:
                    from atlas.prodtask.models import DistributedLock
                    DistributedLock.release_lock(lock_name)
                raise ValueError(f"Expected dict with data.request_id from {func.__name__}, got {result}")
            
            # Submit async polling task to Celery
            # Note: We don't pass the jedi_client instance directly since Celery needs to serialize args
            task = poll_jedi_async_result.apply_async(
                args=(request_id, poll_interval, max_polls, func.__name__, resolved_action_type) + args,
            )
            
            _logger.info(f"Submitted async JEDI task {func.__name__} with request_id={request_id}, "
                        f"celery_task_id={task.id}, lock_name={lock_name}, action_type={resolved_action_type}")

            return result
        
        return wrapper
    return decorator
