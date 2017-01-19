import json

from django.http import HttpResponse
from django.template.response import TemplateResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect, csrf_exempt, ensure_csrf_cookie
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authentication import TokenAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser
from django.http import HttpResponseForbidden
import atlas.datatables as datatables
import logging

from .models import ProductionTask, TRequest, StepExecution, JediTasks

from .task_views import ProductionTaskTable, Parameters, get_clouds, get_sites, get_nucleus, get_permission_analy
from .task_views import get_permissions
from .task_actions import do_action

logger = logging.getLogger('prodtaskwebui')

def do_tasks_action(owner, tasks, action, *args):
    """
    Performing tasks actions
    :param tasks: list of tasks IDs affected
    :param action: name of the action
    :param args: additional arguments
    :return: array of per-task actions' statuses
    """
    # TODO: add local logging
    if not tasks:
        return

    #result = {}
    result = []

    for task in tasks:
        req_info = do_action(owner, task, action, *args)
        #if req_info['exception']:
            #return req_info
        #result =req_info
        result.append(req_info)


    return result



def _http_json_response(data):
    """
    Wrap dictionary JSON dump to a HTTP response
    :param data: JSON contents of the response
    :return: HTTP response with the data dumped to string
    """
    return HttpResponse(json.dumps(data))

ALLOWED_FOR_EXTERNAL_API = ['abort','finish']

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def task_action_ext(request, action=None):
    """
    Sending task action to JEDI.
    \narguments:
       \n * username: user from whom action will be made. Required
       \n * task: task id. Required
       \n * userfullname: user full name for analysis tasks.
       \n * parameters: dict of parameters. Required

    """
    #TODO: change to privileges check
    if not action:
        content = {
            'Actions': ', '.join(ALLOWED_FOR_EXTERNAL_API),
        }
        return content
    if request.user.username != 'bigpanda_api':
        return HttpResponseForbidden()
    error_message = []
    is_permitted = False
    action_username = ''
    task_id = None
    try:
        data = json.loads(request.body)
        if action not in ALLOWED_FOR_EXTERNAL_API:
            error_message += '%s is not allowed' % action
        action_username = data.get('username')
        if not action_username:
            error_message.append('username is required for task action')

        task_id = data.get('task')
        is_analy = False
        task = None
        if not task_id:
            error_message.append('task is required for task action')
        else:
            params = data.get('parameters', None)
            if not ProductionTask.objects.filter(id=int(task_id)).exists():
                if JediTasks.objects.filter(id=int(task_id)).exists():
                    is_analy = True
                else:
                    error_message.append("task %s doesn't exists" % task_id)
            else:
                task = ProductionTask.objects.get(id=int(task_id))
                if task.request_id == 300:
                    is_analy = True

            denied_tasks = [task_id]
            # Analysis
            if is_analy:
                userfullname = data.get('userfullname')
                if not userfullname:
                    error_message.append('userfullname is required for analysis task action')
        if not error_message:
            if not is_analy:
                is_permitted, denied_tasks = get_permissions(action_username, [int(task_id)])
            else:
                is_permitted, denied_tasks = get_permission_analy(action_username, [int(task_id)], userfullname)
    except Exception,e:
        error_message.append(str(e))
    if  error_message:
        content = {
            'exception': '; '.join(error_message),
            'result': 'FAILED'
        }
        logger.error( "Task action problems: %s" % ('; '.join(error_message)))

    else:
        if not is_permitted:
            content = {
                'exception': "User '%s' don't have permissions to make action '%s' with task(s) '%s'" %
                             (action_username,action,','.join([str(x) for x in denied_tasks])),
                'result': 'FAILED'
             }
            logger.error( "User '%s' don't have permissions to make action '%s' with task(s) '%s'" %
                             (action_username,action,','.join([str(x) for x in denied_tasks])))
        else:
            content = {
                'details': "Action '%s' will be perfomed for task %s with parameters %s" %
                             (action,task_id,str(params)),
                'result': 'OK'
             }

            logger.debug("Action '%s' will be perfomed for task %s with parameters %s" %
                             (action,task_id,str(params)))

    return Response(content)



def tasks_action(request, action):
    """
    Handling task actions requests
    :param request: HTTP request object
    :param action: action name
    :return: HTTP response with action status (JSON)
    """

    response = {"action": action}

    if request.method != 'POST':
        response["exception"] = \
            "Request method %s is not supported" % request.method
        return _http_json_response(response)

    # TODO: rewrite with django auth system
    #if not request.user.groups.filter(name='vomsrole:/atlas/Role=production'):
    #    return empty_response
    owner = request.user.username
    if not owner:
        response["exception"] = "Username is empty"
        return _http_json_response(response)

    data_json = request.body
    if not data_json:
        response["exception"] = "Request data is empty"
        return _http_json_response(response)

    data = json.loads(data_json)

    tasks = data.get("tasks")
    if not tasks:
        response["exception"] = "Tasks list is empty"
        return _http_json_response(response)

    params = data.get("parameters", [])

    is_permitted, denied_tasks = get_permissions(request.user.username,tasks)

    #if is_permitted is False:
    if denied_tasks:
            denied_tasks_string = ", ".join(denied_tasks)
            response["exception"] = "User '%s' don't have permissions to make action '%s' with task(s) '%s'" % (owner,action,denied_tasks_string)     
    else:
            response = do_tasks_action(owner, tasks, action, *params)
    return _http_json_response(response)


@never_cache
@csrf_exempt
def get_same_slice_tasks(request):
    """
    Getting all the tasks' ids from the slices where specified tasks are
    :param request: HTTP request in form of JSON { "tasks": [id1, ..idN] }
    :return: information on tasks of the same slices as given ones (dict)
    """

    if request.method != 'POST':
        return _http_json_response(
            {"exception": "Request method %s is not supported" % request.method}
        )

    data_json = request.body
    if not data_json:
        return _http_json_response({"exception": "Request data is empty"})

    data = json.loads(data_json)

    tasks = data.get("tasks")
    if not tasks:
        return _http_json_response({"exception": "Tasks list is empty"})

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

    return _http_json_response(tasks_slices)


@ensure_csrf_cookie
@csrf_protect
@never_cache
@datatables.parametrized_datatable(ProductionTaskTable, Parameters, name='fct')
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
                             'parametrized': request.parametrized,
                             'parent_template': 'prodtask/_index.html',
                             'last_task_submit_time': last_task_submit_time,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'nucleus': get_nucleus(),
                             'edit_mode': True,
                            })



