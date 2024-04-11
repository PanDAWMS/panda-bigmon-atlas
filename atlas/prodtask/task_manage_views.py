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
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser
from django.http import HttpResponseForbidden
import atlas.datatables as datatables
import logging

from .models import ProductionTask, TRequest, StepExecution, JediTasks

from .task_views import ProductionTaskTable, Parameters, get_clouds, get_sites, get_nucleus, get_global_shares, \
    check_action_allowed
from .task_views import get_permissions
from .task_actions import do_action
from ..jedi.client import JEDIClientTest
from ..task_action.task_management import TaskManagementAuthorisation, TaskActionAllowed, TaskActionExecutor, \
    do_jedi_action

logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')


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
        _jsonLogger.info("Tasks action executed",extra={'task':str(task),'user':owner,'action':action,'params':json.dumps(args)})
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

ALLOWED_FOR_EXTERNAL_API = ['abort','finish', 'change_priority', 'retry']

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
    userfullname = ''
    tasks_allowed: [TaskActionAllowed] = []
    params = []
    try:
        data = request.data
        if action not in ALLOWED_FOR_EXTERNAL_API:
            error_message += '%s is not allowed' % action
        action_username = data.get('username')
        if not action_username:
            error_message.append('username is required for task action')

        task_id = data.get('task')
        if not task_id:
            error_message.append('task is required for task action')
        else:
            params = data.get('parameters', [])
            userfullname = data.get('userfullname','')
        is_permitted = False
        if not error_message:
            authentification_management = TaskManagementAuthorisation()
            tasks_allowed = authentification_management.tasks_action_authorisation([task_id], action_username, action, params,
                                                                                   userfullname)
            is_permitted = tasks_allowed[0].user_allowed and tasks_allowed[0].action_allowed
    except Exception as e:
        error_message.append(str(e))
    if  error_message:
        content = {
            'exception': '; '.join(error_message),
            'result': 'FAILED'
        }
        logger.error( "Task action problems: %s" % ('; '.join(error_message)))
        _jsonLogger.info( "Task action problems: %s" % ('; '.join(error_message)),extra={'user':action_username,'action':action})

    else:
        if not is_permitted:
            msg = "User '%s' can't perform %s:"%(action_username,action)
            if tasks_allowed[0].user_allowed:
                msg += " no permissions to make action with task(s) '%s';" %(','.join([str(x) for x in [task_id]]))
            if tasks_allowed[0].action_allowed:
                msg += "action isn't allowed for %s"%(','.join([str(x) for x in [task_id]]))
            content = {
                'exception': msg,
                'result': 'FAILED'
             }
            logger.error(msg)
            _jsonLogger.info(msg,extra={'user':action_username,'action':action})
        else:
            try:
                executor = TaskActionExecutor(action_username, '')
                if params:
                    return_code, return_info = do_jedi_action(executor, task_id, action, *params)
                else:
                    return_code, return_info = do_jedi_action(executor, task_id, action, None)
                content = {
                    'details': "Action '%s' will be performed by %s for task %s with parameters %s" %
                                 (action,action_username,task_id,str(params)),
                    'result': 'OK'

                 }

                logger.debug("Action '%s' will be performed  by %s for task %s with parameters %s, response %s" %
                                 (action,action_username,task_id,str(params),str(return_info)))
            except Exception as e:
                    content = {
                        'exception': '; '.join(error_message),
                        'result': 'FAILED'
                    }
                    logger.error( "Task action problems: %s" % ('; '.join(error_message)))

    return Response(content)



@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def task_chain_obsolete_action(request):
    error_message = []
    try:
       data = request.data
       tasks_id = data.get('tasks')
       if len(tasks_id)>100:
           raise ValueError('Too much tasks to obsolete. Please contact DPA')
       params = data.get('parameters', [])
       user = request.user
       obsolete_list = []
       abort_list = []
       tasks = ProductionTask.objects.filter(id__in=tasks_id)
       for task in tasks:
           if task.status in ProductionTask.NOT_RUNNING:
                obsolete_list.append(int(task.id))
           else:
                abort_list.append(int(task.id))
       if not user.user_permissions.filter(name='Can obsolete chain').exists():
           raise LookupError('User %s has no permission for chain task obsoleting '% user.username)
       else:
            if  obsolete_list:
                response = do_tasks_action(user.username, [obsolete_list], "obsolete_entity", *params)
                logger.info("Tasks action - tasks:%s user:%s action:%s params:%s response:%s"%(str(obsolete_list),user,"obsolete_entity",str(params),str(response)))
            if abort_list:
                params = []
                response = do_tasks_action(user.username, abort_list, "abort", *params)
                logger.info("Tasks action - tasks:%s user:%s action:%s params:%s response:%s"%(str(abort_list),user,"abort",str(params),str(response)))
            #print user, tasks_id, params

    except Exception as e:
        error_message.append(str(e))

    if not error_message:
        content = {'result': 'OK'}
    else:
        content = {
                   'result': 'FAILED',
                   'exception': '; '.join(error_message)
                   }

    return Response(content)


@csrf_protect
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
    tasks_ids = list(map(int, tasks))
    params = data.get("parameters", [])
    try:
        denied_tasks, not_allowed_tasks = check_action_allowed(owner, tasks_ids, action, params)
        if denied_tasks or not_allowed_tasks:
                msg = "User '%s' can't perform %s:"%(owner,action)
                if denied_tasks:
                    msg += " no permissions to make action with task(s) '%s';" %(','.join([str(x) for x in denied_tasks]))
                if not_allowed_tasks:
                    msg += " action isn't allowed for %s"%(','.join([str(x) for x in not_allowed_tasks]))
                logger.error(msg)
                response["exception"] = msg
                _jsonLogger.info(msg,extra={'user':owner,'action':action})

        else:
                response = do_tasks_action(owner, tasks, action, *params)
                logger.info("Tasks action - tasks:%s user:%s action:%s params:%s"%(str(tasks),owner,action,str(params)))
    except Exception as e:
        response["exception"] = str(e)
        logger.error("Tasks action error: %s" % (str(e)))
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
                             'shares': get_global_shares(),
                             'nucleus': get_nucleus(),
                             'edit_mode': True,
                             'show_sync': True

                             })



