from atlas.prodtask.models import PostProductionActions, ProductionTask, DistributedLock

from abc import ABC, abstractmethod
from pprint import pprint
import logging

from atlas.prodtask.task_views import set_task_sample_container, set_production_container, post_bad_state_action

_logger = logging.getLogger('prodtaskwebui')


class BaseAction(ABC):
    """Base abstract class for all actions."""

    @property
    @abstractmethod
    def action_name(self) -> str:
        """Two-letter action name identifier."""
        pass

    @abstractmethod
    def execute(self, task_id: int) -> bool:
        """Execute the action for a given task.

        Args:
            task_id: The identifier of the task to execute the action on.

        Returns:
            True if action completed successfully and should be removed,
            False if action should be kept for retry.
        """
        pass


class TestAction(BaseAction):
    """Test action that prints ProductionTask details."""

    @property
    def action_name(self) -> str:
        return PostProductionActions.ACTIONS.TEST_ACTION

    def execute(self, task_id: int) -> bool:
        """Print ProductionTask details.

        Args:
            task_id: The identifier of the task to print.

        Returns:
            True if task was successfully printed, False otherwise.
        """
        try:
            task = ProductionTask.objects.get(id=task_id)
            _logger.info(f"Executing TestAction for task {task_id}")
            pprint(vars(task))
            return True
        except ProductionTask.DoesNotExist:
            _logger.error(f"Task {task_id} does not exist")
            return False
        except Exception as e:
            _logger.error(f"Error executing TestAction for task {task_id}: {e}")
            return False


class SampleContainerAction(BaseAction):
    """Sample container action placeholder."""

    @property
    def action_name(self) -> str:
        return PostProductionActions.ACTIONS.SAMPLE_CONTAINER

    def execute(self, task_id: int) -> bool:
        """Execute sample container action.

        Args:
            task_id: The identifier of the task.

        Returns:
            True when action is complete.
        """
        production_task = ProductionTask.objects.get(id=task_id)
        if production_task.status in ProductionTask.RED_STATUS+[ProductionTask.STATUS.OBSOLETE]:
            post_bad_state_action(task_id)
            return True
        _logger.info(f"Executing SampleContainerAction for task {task_id}")
        set_production_container(task_id)
        set_task_sample_container(task_id, True)
        return True


# Action registry: maps action codes to action classes
ACTION_REGISTRY = {
    PostProductionActions.ACTIONS.TEST_ACTION: TestAction,
    PostProductionActions.ACTIONS.SAMPLE_CONTAINER: SampleContainerAction,
}


def execute_post_production_actions(task_id: int) -> None:
    """Execute all post-production actions for a task.

    This function retrieves all actions for a task from PostProductionActions,
    executes each action, and removes completed actions from the list.
    If no actions remain, the PostProductionActions record is deleted.

    Args:
        task_id: The identifier of the task to process.
    """
    try:
        pp_actions = PostProductionActions.objects.get(id=task_id)
    except PostProductionActions.DoesNotExist:
        _logger.debug(f"No post-production actions found for task {task_id}")
        return

    actions_to_remove = []

    for action_code in pp_actions.action_list:
        action_class = ACTION_REGISTRY.get(action_code)

        if not action_class:
            _logger.warning(f"Unknown action code '{action_code}' for task {task_id}")
            actions_to_remove.append(action_code)
            continue

        try:
            action = action_class()
            _logger.info(f"Executing action '{action_code}' for task {task_id}")

            if action.execute(task_id):
                _logger.info(f"Action '{action_code}' completed for task {task_id}")
                actions_to_remove.append(action_code)
            else:
                _logger.info(f"Action '{action_code}' needs retry for task {task_id}")

        except Exception as e:
            _logger.error(f"Error executing action '{action_code}' for task {task_id}: {e}")

    # Remove completed actions
    for action_code in actions_to_remove:
        pp_actions.remove_action(action_code)
    pp_actions = PostProductionActions.objects.get(id=task_id)
    # Delete record if no actions remain
    if not pp_actions.action_list:
        _logger.info(f"All actions completed for task {task_id}, deleting record")
        pp_actions.delete()


def check_all_tasks_post_production_actions() -> None:
    """Check and execute post-production actions for all tasks."""
    task_ids = list(PostProductionActions.objects.all().values_list('id', flat=True))
    # split by 1000 ids and take ProductionTask in chunks
    chunk_size = 1000
    task_id_to_process = []
    for i in range(0, len(task_ids), chunk_size):
        chunk = task_ids[i:i + chunk_size]
        task_id_to_process += list(ProductionTask.objects.filter(id__in=chunk, status__in=ProductionTask.NOT_RUNNING).values_list('id', flat=True))
    try:
        if DistributedLock.acquire_lock(f'postproduction', 3600):
            for task_id in task_id_to_process:
                execute_post_production_actions(task_id)
    except Exception as e:
        _logger.error(f"Error in check_all_tasks_post_production_actions: {e}")
    finally:
        DistributedLock.release_lock(f'postproduction')


