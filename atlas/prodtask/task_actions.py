import json

from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from atlas.prodtask.hashtag import add_or_get_request_hashtag
from atlas.prodtask.models import ProductionTask, MCPriority, ProductionDataset

import atlas.deftcore.api.client as deft
from atlas.prodtask.task_views import sync_deft_jedi_task

from atlas.prodtask.views import task_clone_with_skip_used

_deft_client = deft.Client(auth_user=settings.DEFT_AUTH_USER, auth_key=settings.DEFT_AUTH_KEY,base_url=settings.BASE_DEFT_API_URL)

# Mapping between task actions and DEFT task actions
_deft_actions = {
    'abort': 'abort_task',
    'finish': 'finish_task',
    'change_priority': 'change_task_priority',
    'reassign_to_site': 'reassign_task_to_site',
    'reassign_to_cloud': 'reassign_task_to_cloud',
    'reassign_to_nucleus': 'reassign_task_to_nucleus',
    'reassign_to_share': 'reassign_task_to_share',
    'retry': 'retry_task',
    'change_ram_count': 'change_task_ram_count',
    'change_wall_time': 'change_task_wall_time',
    'change_cpu_time': 'change_task_cpu_time',
    'increase_attempt_number': 'increase_attempt_number',
    'abort_unfinished_jobs': 'abort_unfinished_jobs',
    'delete_output': 'clean_task_carriages',
    'kill_job': 'kill_job',
    'obsolete': 'obsolete_task',
    'change_core_count': 'change_task_attribute',
    'change_split_rule': 'change_task_split_rule',
    'pause_task': 'pause_task', 'resume_task':'resume_task', 'trigger_task':'trigger_task_brokerage',
    'avalanche_task':'avalanche_task',
    'reload_input':'reload_input',
    'obsolete_entity': 'obsolete_entity'
}

supported_actions = list(_deft_actions.keys())
# Non-DEFT actions here
#supported_actions.extend(['obsolete', 'increase_priority', 'decrease_priority'])
supported_actions.extend(['increase_priority', 'decrease_priority'])
supported_actions.extend(['retry_new'])
supported_actions.extend(['set_hashtag','remove_hashtag','sync_jedi'])


def do_action(owner, task_id, action, *args):

    result = dict(owner=owner, task=task_id, action=action, args=args,
                  status=None, accepted=False, registered=False,
                  exception=None, exception_source=None)

    if action not in supported_actions:
        result['exception'] = "Action '%s' is not supported" % action
        return result

    # TODO: add a check whether action is allowed for task in this state

    if action == 'delete_output':
        output_datasets = ProductionDataset.objects.filter(task_id=task_id)
        output_formats = [x.get('name').split('.')[4] for x in output_datasets.values('name')]
        if set(args).issubset(output_formats):
        #OK
            result.update(_do_deft_action(owner, task_id, action, args))
        else:
        #Exception
            result['exception'] = "Task {0} doesn't have formats: {1}".format(task_id,args)
        #_deft_client.clean_task_carriages(owner,task_id,args)
        return result
    if action in _deft_actions:
        if action == 'change_core_count':
            args = ('coreCount',)+args
        result.update(_do_deft_action(owner, task_id, action, *args))
    elif action == 'increase_priority':
        result.update(increase_task_priority(owner, task_id, *args))
    elif action == 'decrease_priority':
        result.update(decrease_task_priority(owner, task_id, *args))
    elif action == 'set_hashtag':
        result.update(set_hashtag(owner, task_id, args))
    elif action == 'sync_jedi':
        result.update(sync_jedi(owner, task_id))
    elif action == 'remove_hashtag':
        result.update(remove_hashtag(owner, task_id, args))
    #elif action == 'obsolete':
    #    result.update(obsolete_task(owner, task_id))
    elif action == 'retry_new':
        try:
            step_id = task_clone_with_skip_used(task_id, owner)
            result.update(dict(step_id=int(step_id)))
        except:
            result['exception'] = "Can't retry task {0}".format(task_id)

    return result


def set_hashtag(owner, task_id, hashtag_name):
    try:
        task = ProductionTask.objects.get(id=task_id)
        hashtag = add_or_get_request_hashtag(hashtag_name[0])
        task.set_hashtag(hashtag)
        #print hashtag_name[0]
        return {'status':'success'}
    except Exception as e:
        return {'exception':str(e)}

def sync_jedi(owner, task_id):
    try:
        task = ProductionTask.objects.get(id=task_id)
        sync_deft_jedi_task(task_id)
        return {'status':'success'}
    except Exception as e:
        return {'exception':str(e)}


def remove_hashtag(owner, task_id, hashtag_name):
    try:
        task = ProductionTask.objects.get(id=task_id)
        hashtag = add_or_get_request_hashtag(hashtag_name[0])
        task.remove_hashtag(hashtag)
        return {'status':'success'}
    except Exception as e:
        return {'exception':str(e)}


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
            for name, priority in list(named_priorities.items()):
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

    current_priority = int(task.current_priority or task.priority)
    result["current_priority"] = current_priority

    step = task.step
    slice_priority = step.slice.priority

    if slice_priority < 100:  # having a priority level here
        step_name = step.step_template.step
        result["step_name"] = step_name
        levels = get_priority_levels()
        step_levels = levels.get(step_name, {})
        result["levels"] = step_levels
        for level, prio in list(step_levels.items()):
            if prio == current_priority:
                result["current_level"] = level

    result["successful"] = True
    return result


def shift_task_priority(owner, task_id, level_shift, priority_shift=None):
    """
    Shifting task priority up or down
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :param level_shift: if < 0, increasing the priority, otherwise decreasing.
    Has precedence over priority_shift.
    :param priority_shift: value of priority shift to apply
    :return:
    """

    levels_info = get_task_priority_levels(task_id)
    levels = levels_info.get("levels")
    current_prio = levels_info.get("current_priority")

    result = dict()

    if levels:
        levels = list(levels.values())

    if not levels and (priority_shift is not None):
        return change_task_priority(owner, task_id, current_prio+priority_shift)

    # Assuming that lower level always has higher priority (as set in the DB)
    if level_shift<0:
        next_priorities = sorted([x for x in levels if x>current_prio], reverse=True)
    else:
        next_priorities = sorted([x for x in levels if x<current_prio], reverse=True)

    if not next_priorities:  # limit value is reached, nothing to do
        result["Outer value of priority level already, nothing to do"]
        return result

    new_priority = next_priorities[0]
    return change_task_priority(owner, task_id, new_priority)


def increase_task_priority(owner, task_id, delta=None):
    """
    Increase task priority to next level or on specified value
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
    Decrease task priority to next level or on specified value
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :param delta: value to change priority on
    :return:
    """
    if isinstance(delta, int):
        return shift_task_priority(owner=owner, task_id=task_id, level_shift=1, priority_shift=-delta)
    else:
        return shift_task_priority(owner=owner, task_id=task_id, level_shift=1)


