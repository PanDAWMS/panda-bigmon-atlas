import json
from functools import partial

from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from atlas.prodtask.hashtag import add_or_get_request_hashtag
from atlas.prodtask.models import ProductionTask, MCPriority, ProductionDataset, StepAction, ActionStaging

from atlas.prodtask.task_views import sync_deft_jedi_task

from atlas.prodtask.ddm_api import DDM
from atlas.task_action.task_management import TaskActionExecutor, do_jedi_action


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
supported_actions.extend(['disable_idds'])
supported_actions.extend(['finish_plus_reload'])










def do_new_action(owner, task_id, action, comment, *args):
    action_executor = TaskActionExecutor(owner, comment)
    return do_jedi_action(action_executor, task_id, action, comment, *args)




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
    raise NotImplementedError("Deprecated")


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






