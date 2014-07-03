
from django.http import HttpResponse
from django.template.response import TemplateResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie
from django.core.exceptions import ObjectDoesNotExist

import core.datatables as datatables

from .models import ProductionTask, TRequest, StepExecution

from .task_views import ProductionTaskTable, get_clouds, get_sites

from .task_actions import kill_task, finish_task, obsolete_task, change_task_priority, reassign_task_to_site, reassign_task_to_cloud

import json


_task_actions = {
    'kill': kill_task,
    'finish': finish_task,
    'obsolete': obsolete_task,
    'change_priority': change_task_priority,
    'reassign_to_site': reassign_task_to_site,
    'reassign_to_cloud': reassign_task_to_cloud,
}


def do_tasks_action(tasks, action, *args):
    """
    Performing task actions
    :param tasks: list of tasks affected
    :param action: name of action
    :param args: additional arguments
    :return: array of actions' statuses
    """
    if (not tasks) or not (action in _task_actions):
        return

    result = []
    for task in tasks:
        response = _task_actions[action](task, *args)
        req_info = dict(task_id=task, action=action, response=response)
        result.append(req_info)

    return result


def tasks_action(request, action):
    """
    Handling task actions requests
    :param request: HTTP request object
    :param action: action name
    :return: HTTP response with action status (JSON)
    """
    empty_response = HttpResponse('')

    if request.method != 'POST' or not (action in _task_actions):
        return empty_response

    data_json = request.body
    if not data_json:
        return empty_response
    data = json.loads(data_json)

    tasks = data.get("tasks")
    if not tasks:
        return empty_response

    params = data.get("parameters", [])
    response = do_tasks_action(tasks, action, *params)
    return HttpResponse(json.dumps(response))


def get_same_slice_tasks(request, tid):
    """ Getting all the tasks ids from the slice where specified task is
    :tid request: task ID
    :return: tasks of the same slice as specified (dict)
    """
    empty_response = HttpResponse('')

    if not tid:
        return empty_response

    try:
        task = ProductionTask.objects.get(id=tid)
    except:
        return empty_response

    step_id = task.step.id
    slice_id = StepExecution.objects.get(id=step_id).slice.id
    steps = [ str(x.get('id')) for x in StepExecution.objects.filter(slice=slice_id).values("id") ]
    tasks = {}
    for task in ProductionTask.objects.filter(step__in=steps).only("id", "status"):
        tasks[str(task.id)] = { "id": str(task.id), "status": task.status }

    response = dict(tasks=tasks)
    return HttpResponse(json.dumps(response))


@ensure_csrf_cookie
@csrf_protect
@never_cache
@datatables.datatable(ProductionTaskTable, name='fct')
def task_manage(request):
    """

    :param request: HTTP request
    :return: rendered HTTP response
    """
    qs = request.fct.get_queryset()
    last_task_submit_time = ProductionTask.objects.order_by('-submit_time')[0].submit_time


    return TemplateResponse(request, 'prodtask/_task_manage.html',
                            {'title': 'Manage Production Tasks',
                             'active_app': 'prodtask/task_manage',
                             'table': request.fct,
                             'parent_template': 'prodtask/_index.html',
                             'last_task_submit_time': last_task_submit_time,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'edit_mode': True,
                            })

