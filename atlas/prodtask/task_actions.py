import json

from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from .models import ProductionTask, MCPriority
import atlas.deftcore.api.client as deft


_deft_client = deft.Client(settings.DEFT_AUTH_USER, settings.DEFT_AUTH_KEY)

# Mapping between task actions and DEFT task actions
_deft_actions = {
    'kill': 'abort_task',
    'finish': 'finish_task',
    'change_priority': 'change_task_priority',
    'reassign_to_site': 'reassign_task_to_site',
    'reassign_to_cloud': 'reassign_task_to_cloud',
    'retry': 'retry_task',
    'change_ram_count': 'change_task_ram_count',
    'change_wall_time': 'change_task_wall_time',
}

supported_actions = _deft_actions.keys()
supported_actions.extend(['obsolete', 'increase_priority', 'decrease_priority'])


def do_action(owner, task_id, action, *args):
    result = dict(owner=owner, task=task_id, action=action, args=args,
                  status=None, accepted=False, registered=False,
                  exception=None, exception_source=None)

    if not action in supported_actions:
        result['exception'] = "Action '%s' is not supported" % action
        return result

    if action in _deft_actions:
        result.update(_do_deft_action(owner, task_id, action, *args))
    elif action == 'increase_priority':
        result.update(increase_task_priority(owner, task_id, *args))
    elif action == 'decrease_priority':
        result.update(decrease_task_priority(owner, task_id, *args))
    elif action == 'obsolete':
        result.update(obsolete_task(owner, task_id))

    return result


def _do_deft_action(owner, task_id, action, *args):
    """
    Perform task action using DEFT API
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :param action: action name
    :param args: additional arguments for the action (if needed)
    :return: dictionary with action execution details
    """

    result = dict(owner=owner, task=task_id, action=action, args=args,
                  status=None, accepted=False, registered=False,
                  exception=None, exception_source=None)

    if not action in _deft_actions:
        result['exception'] = "Action '%s' is not supported" % action
        return result

    try:
        func = getattr(_deft_client, _deft_actions[action])
    except AttributeError as e:
        result.update(exception=str(e))
        return result

    try:
        request_id = func(owner, task_id, *args)
    except Exception as e:
        result.update(exception=str(e),
                      exception_source=_deft_client.__class__.__name__)
        return result

    result['accepted'] = True

    try:
        status = _deft_client.get_status(request_id)
    except Exception as e:
        result.update(exception=str(e),
                      exception_source=_deft_client.__class__.__name__)
        return result

    result.update(registered=True, status=status)

    return result


def obsolete_task(owner, task_id):
    """
    Mark task as 'obsolete'
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :return: dict with action status
    """
    result = dict(owner=owner, task_id=task_id,
                  accepted=True, registered=False, exception=None)

    # TODO: add logging
    # TODO: add logging with PandaLog (using DEFT API)
    try:
        task = ProductionTask.objects.get(id=task_id)
    except ObjectDoesNotExist:
        result['exception'] = "Task '%s' does not exist" % task_id
        return result
    except Exception as error:
        result['exception'] = str(error)
        return result

    if task.status not in ['done', 'finished']:
        result['exception'] = "Task '%s' is in the state '%s', not 'done' or 'finished'" % (task_id, task.status)
        return result

    #TODO: log action
    ProductionTask.objects.filter(id=task_id).update(status='obsolete', timestamp=timezone.now())
    result['registered'] = True
    return result


def change_task_priority(owner, task_id, priority):
    """
    Set task JEDI priority.
    :param task_id: task ID
    :param priority: JEDI task priority
    :return: dict with action status
    """
    # TODO: add status checking and logging
    return _do_deft_action(owner, task_id, 'change_priority', priority)


def get_task_priority_levels(task_id):
    """
    Get task priority levels (if any) for the task
    :param task_id: task ID
    :return: dict containing available levels, current level and task priority
    """

    def get_priority_levels():
        """ Get JEDI priority levels from the step template
        :return: dict of JEDI priority of the step { name: {level: priority, ...}, ...}
        """
        levels_ = {}
        for prio in MCPriority.objects.all():
            try:
                named_priorities = json.loads(prio.priority_dict)
            except:
                continue
            for name, priority in named_priorities.items():
                if not levels_.get(name):
                    levels_[name] = {}
                levels_[name][int(prio.priority_key)] = priority
        return levels_

    result = dict(id=task_id, current_level=None, levels={}, current_priority=None)

    try:
        task = ProductionTask.objects.get(id=task_id)
    except ObjectDoesNotExist:
        result.update(reason="Task not found")
        return result

    step = task.step
    slice_priority = step.slice.priority

    result["current_priority"] = int(task.current_priority or task.priority)

    if slice_priority < 100:  # having a priority level here
        step_name = step.step_template.step
        levels = get_priority_levels()
        result["levels"] = levels.get(step_name, {})

    result["successful"] = True
    return result


def shift_task_priority(owner, task_id, level_shift, priority_shift=None):
    """
    Shifting task priority up or down
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :param level_shift: if > 0, increasing the priority, otherwise decreasing.
    Has precedence over priority_shift.
    :param priority_shift: value of priority shift to apply
    :return:
    """

    levels_info = get_task_priority_levels(task_id)
    levels = levels_info.get("levels")
    current_prio = levels_info.get("current_priority")

    result = dict()

    if levels:
        levels = levels.values()

    if not levels and (priority_shift is not None):
        return change_task_priority(owner, task_id, current_prio+priority_shift)

    if level_shift > 0:
        next_priorities = sorted([x for x in levels if x > current_prio])
    else:
        next_priorities = sorted([x for x in levels if x < current_prio])

    if not next_priorities:  # limit value is reached
        return result

    new_priority = next_priorities[0]
    return change_task_priority(owner, task_id, new_priority)


def increase_task_priority(owner, task_id, delta=None):
    """
    Increase task priority for one level or specified value
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :param delta: value to change priority on
    :return:
    """
    if isinstance(delta, int):
        return shift_task_priority(owner=owner, task_id=task_id, level_shift=-1, priority_shift=delta)
    else:
        return shift_task_priority(owner=owner, task_id=task_id, level_shift=-1)


def decrease_task_priority(owner, task_id, delta=None):
    """
    Decrease task priority for one level or specified value
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :param delta: value to change priority on
    :return:
    """
    if isinstance(delta, int):
        return shift_task_priority(owner=owner, task_id=task_id, level_shift=1, priority_shift=-delta)
    else:
        return shift_task_priority(owner=owner, task_id=task_id, level_shift=1)


