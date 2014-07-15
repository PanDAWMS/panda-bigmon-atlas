
from django.http import HttpResponse
from django.template.response import TemplateResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect, csrf_exempt, ensure_csrf_cookie
from django.core.exceptions import ObjectDoesNotExist

import core.datatables as datatables

from .models import ProductionTask, TRequest, StepExecution

from .task_views import ProductionTaskTable, get_clouds, get_sites

from .task_actions import kill_task, finish_task, obsolete_task,\
                          change_task_priority, increase_task_priority, decrease_task_priority, \
                          reassign_task_to_site, reassign_task_to_cloud, retry_task


import json


_task_actions = {
    'kill': kill_task,
    'finish': finish_task,
    'obsolete': obsolete_task,
    'change_priority': change_task_priority,
    'increase_priority': increase_task_priority,
    'decrease_priority': decrease_task_priority,
    'reassign_to_site': reassign_task_to_site,
    'reassign_to_cloud': reassign_task_to_cloud,
    'retry': retry_task,
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


@never_cache
@csrf_exempt
def get_same_slice_tasks(request):
    """
    Getting all the tasks' ids from the slices where specified tasks are
    :param request: HTTP request in form of JSON { "tasks": [id1, ..idN] }
    :return: information on tasks of the same slices as given ones (dict)
    """
    empty_response = HttpResponse('')

    if request.method != 'POST':
        return empty_response

    data_json = request.body
    if not data_json:
        return empty_response
    data = json.loads(data_json)

    tasks = data.get("tasks")
    if not tasks:
        return empty_response

    tasks_slices = {}

    for task_id in list(set(tasks)):
        try:
            task = ProductionTask.objects.get(id=task_id)
        except ObjectDoesNotExist:
            continue

        slice_id = task.step.slice.id
        steps = [str(x.get('id')) for x in StepExecution.objects.filter(slice=slice_id).values("id")]
        slice_tasks = {}
        for task_ in ProductionTask.objects.filter(step__in=steps).only("id", "step", "status"):
            slice_tasks[str(task_.id)] = dict(step=str(task_.step.id), status=task_.status)

        tasks_slices[task_id] = dict(tasks=slice_tasks, slice=str(slice_id))

    return HttpResponse(json.dumps(tasks_slices))



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

