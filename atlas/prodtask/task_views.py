from copy import deepcopy
from typing import Dict

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.template.response import TemplateResponse
from django.urls import reverse, resolve
from atlas.celerybackend.celery import app
from atlas.prodtask.check_duplicate import find_downstreams_by_task, create_task_chain
from atlas.prodtask.ddm_api import DDM, name_without_scope
from atlas.prodtask.hashtag import add_or_get_request_hashtag
from atlas.prodtask.models import StepExecution, GlobalShare, StepTemplate
import logging

from ..cric.client import CRICClient
from ..settings import defaultDatetimeFormat, OIDC_LOGIN_URL

import atlas.datatables as datatables

from .forms import ProductionTaskForm, ProductionTaskCreateCloneForm, ProductionTaskUpdateForm
from .models import ProductionTask, TRequest, TTask, ProductionDataset, Site, JediTasks, JediDatasets

from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.http import HttpResponseForbidden
from django.contrib.auth.models import User
from django.db.models import Count, Q
from django.utils import timezone
from django.core.cache import cache

from django.utils.timezone import utc
from datetime import datetime, timedelta
import pytz
import locale
import time
import json
OUTPUTS_TYPES = ["AOD","EVNT","HITS"]


_logger = logging.getLogger('prodtaskwebui')

from django.views.decorators.csrf import csrf_protect, csrf_exempt, ensure_csrf_cookie

# Allowed task actions per status
allowed_task_actions = {
    'waiting': ['set_hashtag','remove_hashtag','abort','retry', 'reassign', 'change_priority', 'change_parameters', 'increase_attempt_number','kill_job',  'abort_unfinished_jobs','sync_jedi'],
    'registered': ['kill_job','retry'],
    'assigning': ['kill_job','retry'],
    'submitting': ['kill_job','retry'],
    'ready': ['kill_job','retry'],
    'running': ['kill_job','retry'],
    'exhausted': ['kill_job','retry','retry_new', 'reassign'],
    'done': ['obsolete', 'delete_output', 'obsolete_entity','set_hashtag','remove_hashtag','ctrl','reassign', 'finish'],
    'finished': ['set_hashtag','remove_hashtag','retry', 'retry_new', 'change_parameters', 'obsolete', 'ctrl', 'delete_output','change_priority', 'obsolete_entity', 'finish'],
    'broken': ['set_hashtag','remove_hashtag','sync_jedi', 'abort'],
    'aborted': ['set_hashtag','remove_hashtag','sync_jedi', 'abort'],
    'failed': ['set_hashtag','remove_hashtag','sync_jedi', 'abort'],
    'scouting':['set_hashtag','remove_hashtag','sync_jedi'],
    'obsolete':['set_hashtag','remove_hashtag','sync_jedi'],
    'paused': ['retry'],
    'staging':['retry'],
    'toretry':['retry'],
    'pending':['retry'],
    'toabort':['abort']
}

# Actions for tasks in "active" states
for _status in ['registered', 'assigning', 'submitting', 'ready', 'running','exhausted', 'paused', 'scouting', 'toretry', 'staging', 'pending']:
    allowed_task_actions[_status].extend(['abort', 'finish', 'change_priority',
                                          'change_parameters', 'reassign',
                                          'increase_attempt_number', 'abort_unfinished_jobs','set_hashtag','remove_hashtag',
                                          'ctrl','sync_jedi','disable_idds', 'finish_plus_reload'])



# Extending actions by groups of them
for _status in allowed_task_actions:
    if 'change_priority' in allowed_task_actions[_status]:
        allowed_task_actions[_status].extend(['increase_priority', 'decrease_priority'])
    if 'change_parameters' in allowed_task_actions[_status]:
        allowed_task_actions[_status].extend(['change_ram_count', 'change_wall_time', 'change_cpu_time', 'change_core_count', 'change_split_rule'])
    if 'reassign' in allowed_task_actions[_status]:
        allowed_task_actions[_status].extend(['reassign_to_site', 'reassign_to_cloud', 'reassign_to_nucleus', 'reassign_to_share'])
    if 'ctrl' in allowed_task_actions[_status]:
        allowed_task_actions[_status].extend(['pause_task', 'resume_task', 'trigger_task' , 'avalanche_task','reload_input','sync_jedi'])

@csrf_protect
def descent_tasks(request, task_id):
    try:
        child_tasks = find_downstreams_by_task(task_id)
        request.session['selected_tasks'] = [int(task_id)] + [int(x['id']) for x in child_tasks]
        return render(request, 'reqtask/_task_table.html',{'reqid':None,'title': 'Descent tasks for task %s'%str(task_id)})
    except Exception as e:
        _logger.error("Problem with retrieving descends tasks for %s: %s"%(str(task_id),str(e)))
        return HttpResponseRedirect('/')


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def form_task_chain(request, task_id):
    def update_parent(chain, task_childs, parent):
            task_childs[parent] = task_childs.get(parent,0) + 1
            if chain[parent]['parent']!=parent:
                update_parent(chain, task_childs, chain[parent]['parent'])
    try:
        chain = create_task_chain(int(task_id))
        levels = [{} for x in range(15)]

        for task in chain:
            dict_to_send = {'id':int(chain[task]['task']['id']),'etag':chain[task]['task']['name'].split('.')[-1].split('_')[-1],
                            'status':chain[task]['task']['status'],'provenance':chain[task]['task']['provenance'],
                            'request':chain[task]['task']['request_id']}
            if 'eventIndex' in chain[task]['task']['name']:
                dict_to_send['provenance'] = 'EI'
            levels[chain[task]['level']][chain[task]['parent']] = levels[chain[task]['level']].get(chain[task]['parent'],[]) + [dict_to_send]

        result = {}
        for index, level in enumerate(levels[1:]):
            if level:
                result.update({index:level})

        content = result
    except Exception as e:
        content = str(e)
        return Response(content,status=500)
    return Response(content)


@login_required(login_url=OIDC_LOGIN_URL)
def task_chain_view(request, task_id):
    if request.method == 'GET':
        task = ProductionTask.objects.get(id = int(task_id))
        return render(request, 'prodtask/_task_chain.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Task chain obsolete',
                'submit_url': 'prodtask:task_chain_view',
                'parent_template': 'prodtask/_index.html',
                'taskid': int(task.id),
                'taskName': task.name,
                'total_events':task.total_events
            })



@login_required(login_url=OIDC_LOGIN_URL)
def task_old_details(request, rid=None):
    if rid:
        try:
            same_tasks = []
            same_tasks_with_status = []
            output_datasets = []
            output_formats = []
            if ProductionTask.objects.filter(id=rid).exists():
                task = ProductionTask.objects.get(id=rid)
                output_datasets = ProductionDataset.objects.filter(task_id=rid)
                output_formats = [x.get('name').split('.')[4] for x in output_datasets.values('name')]
                if task.is_extension:
                    dataset_pat=output_datasets[0].name.split("tid")[0]
                    datasets_extension = ProductionDataset.objects.filter(name__icontains=dataset_pat)
                    same_tasks = [int(x.name.split("tid")[1].split("_")[0]) for x in datasets_extension]
                    for same_task in same_tasks:
                        same_tasks_with_status.append({'id': same_task, 'status': ProductionTask.objects.get(id=same_task).status})
            else:
                task = TTask.objects.get(id=rid)
            ttask = TTask.objects.get(id=rid)



            tasks = []
            tasks.append(rid)
            # is_permitted,denied_task = get_permissions(request.user.username,tasks)
            denied_task, tmp = check_action_allowed(request.user.username, tasks)
            is_permitted = denied_task == []
            permissions = {}
            if task.status in allowed_task_actions:
                for action in allowed_task_actions[task.status]:
                    permissions[action] = is_permitted

            # TODO: these actions are needed from DEFT and JEDI (SB)
            for action in ['edit', 'clone']:
                permissions[action] = False
            try:
                hashtags = [str(x) for x in task.hashtags]
            except:
                hashtags = []
            request_parameters = {
                'active_app': 'prodtask',
                'pre_form_text': 'ProductionTask details with ID = %s' % rid,
                'task': task,
                'ttask': ttask,
                'output_datasets': output_datasets,
                'clouds': get_clouds(),
                'sites': get_sites(),
                'nucleus': get_nucleus(),
                'shares': get_global_shares(),
                'outputs': output_formats,
                'extasks': same_tasks_with_status,
                'hashtags': hashtags,
                'parent_template': 'prodtask/_index.html',
                'show_sync': True
            }
            for action, perm in list(permissions.items()):
                request_parameters['can_' + action + '_task'] = perm

            return render(request, 'prodtask/_task_detail.html', request_parameters)
        except:
            return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/')

def task_details(request, rid=None):
    if rid:
        return HttpResponseRedirect(f'/ng/task/{rid}')
        # try:
        #     from atlas.settings.local import FIRST_ADOPTERS
        #     if (request.user.username in FIRST_ADOPTERS):
        #         return HttpResponseRedirect(f'/ng/task/{rid}')
        # except:
        #     pass
        # try:
        #     same_tasks = []
        #     same_tasks_with_status = []
        #     output_datasets = []
        #     output_formats = []
        #     if ProductionTask.objects.filter(id=rid).exists():
        #         task = ProductionTask.objects.get(id=rid)
        #         output_datasets = ProductionDataset.objects.filter(task_id=rid)
        #         output_formats = [x.get('name').split('.')[4] for x in output_datasets.values('name')]
        #         if task.is_extension:
        #             dataset_pat=output_datasets[0].name.split("tid")[0]
        #             datasets_extension = ProductionDataset.objects.filter(name__icontains=dataset_pat)
        #             same_tasks = [int(x.name.split("tid")[1].split("_")[0]) for x in datasets_extension]
        #             for same_task in same_tasks:
        #                 same_tasks_with_status.append({'id': same_task, 'status': ProductionTask.objects.get(id=same_task).status})
        #     else:
        #         task = TTask.objects.get(id=rid)
        #     ttask = TTask.objects.get(id=rid)
        #
        #
        #
        #     tasks = []
        #     tasks.append(rid)
        #     # is_permitted,denied_task = get_permissions(request.user.username,tasks)
        #     denied_task, tmp = check_action_allowed(request.user.username, tasks)
        #     is_permitted = denied_task == []
        #     permissions = {}
        #     if task.status in allowed_task_actions:
        #         for action in allowed_task_actions[task.status]:
        #             permissions[action] = is_permitted
        #
        #     # TODO: these actions are needed from DEFT and JEDI (SB)
        #     for action in ['edit', 'clone']:
        #         permissions[action] = False
        #     try:
        #         hashtags = [str(x) for x in task.hashtags]
        #     except:
        #         hashtags = []
        #     request_parameters = {
        #         'active_app': 'prodtask',
        #         'pre_form_text': 'ProductionTask details with ID = %s' % rid,
        #         'task': task,
        #         'ttask': ttask,
        #         'output_datasets': output_datasets,
        #         'clouds': get_clouds(),
        #         'sites': get_sites(),
        #         'nucleus': get_nucleus(),
        #         'shares': get_global_shares(),
        #         'outputs': output_formats,
        #         'extasks': same_tasks_with_status,
        #         'hashtags': hashtags,
        #         'parent_template': 'prodtask/_index.html',
        #         'show_sync': True
        #     }
        #     for action, perm in list(permissions.items()):
        #         request_parameters['can_' + action + '_task'] = perm
        #
        #     return render(request, 'prodtask/_task_detail.html', request_parameters)
        # except:
        #     return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/')




@login_required(login_url=OIDC_LOGIN_URL)
def task_clone(request, rid=None):
    return HttpResponseRedirect('/')


@login_required(login_url=OIDC_LOGIN_URL)
def task_update(request, rid=None):
    return HttpResponseRedirect('/')


@login_required(login_url=OIDC_LOGIN_URL)
def task_create(request):
    return HttpResponseRedirect('/')


class ProductionTaskTable(datatables.DataTable):

    name = datatables.Column(
        label='Task Name',
        sClass='breaked_word',
        )

    request = datatables.Column(
        label='Request',
        model_field='request__reqid',
        sClass='numbers',
 #       bVisible='false',
        )

    step = datatables.Column(
        label='Step',
        model_field='step__id',
        sClass='centered',
  #      bVisible='false',
        )

    parent_id = datatables.Column(
        label='Parent id',
        bVisible='false',
        )

    id = datatables.Column(
        label='Task ID',
        sClass='numbers taskid',
    #    asSorting=[ "desc" ],
        )

    current_priority = datatables.Column(
        label='Priority',
        sClass='numbers',
        )

    project = datatables.Column(
        label='Project',
        bVisible='false',
#        sSearch='user',
        )

    chain_id = datatables.Column(
        label='Chain',
        bVisible='false',
#        sSearch='user',
        )

    total_req_jobs = datatables.Column(
        label='Total Jobs',
        sClass='numbers',
        )

    total_done_jobs = datatables.Column(
        label='Done Jobs',
        sClass='numbers',
        )

    failure_rate = datatables.Column(
        label='Failure %',
        sClass='numbers',
        #bSortable=False,
        model_field='failure_rate'
        )

    total_events = datatables.Column(
        label='Events',
        sClass='numbers',
        )

    status = datatables.Column(
        label='Status',
        sClass='centered',
        )

    submit_time = datatables.Column(
        label='Submit time',
        sClass='px100 datetime centered',
   #     bVisible='false',
        )

    timestamp = datatables.Column(
        label='Timestamp',
        sClass='px100 datetime centered',
        )

    start_time = datatables.Column(
        label='Start time',
        bVisible='false',
        )

    provenance = datatables.Column(
        label='P-e',
        #bVisible='false',
        )

    phys_group = datatables.Column(
        label='Phys group',
        bVisible='false',
        )

    reference = datatables.Column(
        label='JIRA',
        sClass='numbers',
        )

    comments = datatables.Column(
        label='Comments',
        bVisible='false',
        )

#    inputdataset = datatables.Column(
#        label='Inputdataset',
#        )

    physics_tag = datatables.Column(
        label='Physics tag',
        bVisible='false',
        )

    username = datatables.Column(
        label='Owner',
        bVisible='false',
        )

    update_time = datatables.Column(
        label='Update time',
        bVisible='false',
        )

    step_name = datatables.Column(
        label='Step',
        model_field='step__step_template__step',
  #      bVisible='false',
        )

    priority = datatables.Column(
        label='SPriority',
        sClass='numbers',
        )
        
    campaign = datatables.Column(
        bVisible='false',
        )

    class Meta:
        model = ProductionTask

        id = 'task_table'
        var = 'taskTable'

        bSort = True
        bPaginate = True
        bJQueryUI = True

        bProcessing = True

        sDom = '<"top-toolbar"lf><"table-content"rt><"bot-toolbar"ip>'

        bAutoWidth = False
        bScrollCollapse = False

        aaSorting = [[4, "desc"]]
        aLengthMenu = [[100, 1000, -1], [100, 1000, "All"]]
        iDisplayLength = 100

        fnServerParams = "taskServerParams"

        fnPreDrawCallback = "taskDrawCallback"

        fnClientTransformData = "prepareData"

        bServerSide = True

        def __init__(self):
            self.sAjaxSource = reverse('task_table')

    def additional_data(self, request, qs):
        """
        Overload DataTables method for adding statuses info at the page
        :return: dictionary of data should be added to each server response of table data
        """
        status_stat = get_status_stat(qs)
        return { 'task_stat' : status_stat }


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def outside_task_action(request, action):
    result = {}
    return Response({"load": result})

def get_status_stat(qs):
    """
    Compute ProductionTasks statuses by query set
    :return: list of statuses with count of task in corresponding state
    """
    return [ { 'status':'total', 'count':qs.count() } ] +\
            [ { 'status':str(x['status']), 'count':str(x['count']) }
              for x in qs.values('status').annotate(count=Count('id')) ]


@csrf_protect
def task_status_stat_by_request(request, rid):
    """
    ProductionTasks statuses for specific request
    :return: line with statuses
    """
    qs = ProductionTask.objects.filter(request__reqid=rid)

    stat = get_status_stat(qs)
    wrong_stat = get_status_stat(ProductionTask.objects.filter(request__reqid=rid, step__slice__is_hide=True))
    result_stat = []
    if len(wrong_stat)>1:
        result_stat.append(stat[0])
        result_stat[0]['count'] -= wrong_stat[0]['count']
        wrong_stat_dict = {}
        for x in wrong_stat[1:]:
            wrong_stat_dict[x['status']] = int(x['count'])
        for index in range(1,len(stat)):
            if stat[index]['status'] in wrong_stat_dict:
                stat[index]['count'] = int(stat[index]['count']) - wrong_stat_dict[stat[index]['status']]
            if int(stat[index]['count']) > 0:
                result_stat.append(stat[index])
    else:
        result_stat = stat
    return TemplateResponse(request, 'prodtask/_task_status_stat.html', { 'stat': result_stat, 'reqid': rid})


class Parameters(datatables.Parametrized):

    task_id = datatables.Parameter(label='Task ID', get_Q=lambda v: Q( **{ 'id__iregex' : v } ) )
    task_id_gt = datatables.Parameter(label='Task ID <=', get_Q=lambda v: Q( **{ 'id__lte' : v } ) )
    task_id_lt = datatables.Parameter(label='Task ID >=', get_Q=lambda v: Q( **{ 'id__gte' : v } ) )

    project = datatables.Parameter(label='Project')
    username = datatables.Parameter(label='Username')
    campaign = datatables.Parameter(label='Campaign')

    request = datatables.Parameter(label='Request ID', model_field='request__reqid')
    request_id_gt = datatables.Parameter(label='Request ID <=', get_Q=lambda v: Q( **{ 'request__reqid__lte' : v } ) )
    request_id_lt = datatables.Parameter(label='Request ID >=', get_Q=lambda v: Q( **{ 'request__reqid__gte' : v } ) )
    chain = datatables.Parameter(label='Chain', model_field='chain_id')
    provenance = datatables.Parameter(label='Provenance')

    phys_group = datatables.Parameter(label='Phys Group')
    step_name = datatables.Parameter(label='Step Name', model_field='step__step_template__step')
    step_output_format = datatables.Parameter(label='Step output format', get_Q=lambda v: Q( **{ 'step__step_template__output_formats__iregex' : v } ) )


    def _get_ctag_Q(value):
        tags=value.split()
        return Q( step__step_template__ctag__in=tags )

    ctag = datatables.Parameter(label='AMI tag exact', get_Q=_get_ctag_Q)
    #ctag = datatables.Parameter(label='AMI tag exact', get_Q=lambda v: Q( **{ 'step__step_template__ctag__iexact' : v } ) )

    task_name = datatables.Parameter(label='Task name', name='taskname', id='taskname', get_Q=lambda v: Q( **{ 'name__iregex' : v } ) )

    #type = datatables.Parameter(label='Type', model_field='request_type')
    type = datatables.Parameter(label='Type', model_field='request__request_type')

    class Meta:
        SetParametersToURL = 'SetParametersToURL'
        ParseParametersFromURL = 'ParseParametersFromURL'
        var = 'parameters_list'

    def _task_status_Q(value):
        if value == 'active':
            return Q( status__in=['done','finished','failed','broken','aborted','obsolete'] ).__invert__()
        elif value == 'ended':
            return Q( status__in=['done','finished'] )
        elif value == 'regular':
            return Q( status__in=['failed','broken','aborted','obsolete'] ).__invert__()
        elif value == 'irregular':
            return Q( status__in=['failed','broken','aborted'] )
        elif value:
            return Q( status__iexact=value )
        return Q()

    task_status = datatables.Parameter(label='Status', name='status', id='status', get_Q=_task_status_Q )
    task_type = datatables.Parameter(label='Task type', get_Q=lambda v: (Q(project='user').__invert__() if (v=='production') else (Q(project='user') if (v=='analysis') else Q()) ) )


    time_from = datatables.Parameter(label='Last update time period from', get_Q=lambda v: Q(timestamp__gt=datetime.utcfromtimestamp(float(v)/1000.).replace(tzinfo=utc).strftime(defaultDatetimeFormat)))
    time_to = datatables.Parameter(label='Last update time period to', get_Q=lambda v: Q(timestamp__lt=datetime.utcfromtimestamp(float(v)/1000.).replace(tzinfo=utc).strftime(defaultDatetimeFormat)))


@login_required(login_url=OIDC_LOGIN_URL)
@datatables.parametrized_datatable(ProductionTaskTable, Parameters)
def task_table(request):
    """
    ProductionTask table
    :return: table page or data for it
    """
    last_task_submit_time = ProductionTask.objects.order_by('-submit_time')[0].submit_time
    return TemplateResponse(request, 'prodtask/_task_table.html', { 'title': 'Production Tasks Table',
                                                                    'active_app' : 'prodtask',
                                                                    'table': request.datatable,
                                                                    'parametrized': request.parametrized,
                                                                    'parent_template': 'prodtask/_index.html',
                                                                    'last_task_submit_time' : last_task_submit_time
                                                                    })


def get_clouds():
    """
    !Deprecated Get list of clouds
    :return: list of clouds names
    """
    clouds = [ ]

    return clouds


def get_sites(update_cache=False):
    """
    Get list of site names
    :return: list of site names
    """
    if not update_cache and cache.get('panda_queues'):
        return cache.get('panda_queues')
    cric_client = CRICClient()
    queues = cric_client.get_panda_queues()
    result = list(queues.keys())
    result.sort()
    cache.set('panda_queues', result, 3600*24)
    return result

def get_nucleus(update_cache=False):
    """
    Get list of nuclei names
    :return: list of nuclei names
    """
    if not update_cache and cache.get('panda_nucleus'):
        return cache.get('panda_nucleus')
    nucleus = [ x.get('site_name') for x in Site.objects.filter(role='nucleus').order_by('site_name').values('site_name').distinct() ]
    cache.set('panda_nucleus', nucleus, 3600*24)
    return nucleus

def slice_by_task(request, task_id):
    try:
        task = ProductionTask.objects.get(id=task_id)
        request_id = task.request_id
        slice = task.step.slice.slice
    except:
        return HttpResponseRedirect('/')
    return HttpResponseRedirect(reverse('prodtask:input_list_approve_full', args=[request_id])+'#inputList'+str(slice))

def slice_by_task_short(request, task_id):
    try:
        task = ProductionTask.objects.get(id=task_id)
        request_id = task.request_id
        slice = task.step.slice.slice
    except:
        return HttpResponseRedirect('/')
    return HttpResponseRedirect(reverse('prodtask:input_list_approve', args=[request_id])+'#inputList'+str(slice))

ALWAYS_ALLOWED = ['set_hashtag','remove_hashtag']


def check_action_allowed(username, tasks, action=None, params=None, userfullname=''):
    is_superuser=False
    group_permissions = []
    denied_tasks=[]
    not_allowed_tasks = []
    allowed_groups = []
    if User.objects.filter(username=username).exists():
        user = User.objects.get(username=username)
        user_groups = user.groups.all()
        is_superuser = user.is_superuser
        for gp in user_groups:
            group_permissions += list(gp.permissions.all())
        for gp in group_permissions:
            if "has_" in gp.name and "_permissions" in gp.name:
                allowed_groups.append(gp.codename)
    for task in tasks:
            physgroup = ''
            request_phys_group = ''
            if ProductionTask.objects.filter(id=task).exists():
                task_dict = ProductionTask.objects.values('username','name','status','request_id','phys_group','request__phys_group').get(id=task)
                task_owner =task_dict.get('username')
                task_name = task_dict.get('name')
                task_status = task_dict.get('status')
                physgroup = task_dict.get('phys_group')
                request_phys_group = task_dict.get('request__phys_group')
                is_analy = task_dict.get('request_id') == 300
            else:
                task_dict = JediTasks.objects.values('username','taskname','status').get(id=task)
                task_owner = task_dict.get('username')
                task_name = task_dict.get('taskname')
                task_status = task_dict.get('status')
                is_analy = True

            if action and (action not in allowed_task_actions[task_status]):
                not_allowed_tasks.append(task)
            else:
                if action not in ALWAYS_ALLOWED:
                    if not is_analy:
                        if is_superuser or (username==task_owner):
                                pass
                        elif "DPD" in allowed_groups:
                                pass
                        elif "MCCOORD" in  allowed_groups:
                                pass
                        elif ((physgroup in allowed_groups) or (request_phys_group in allowed_groups)) and (action not in ['increase_priority','reassign_to_site', 'reassign_to_cloud', 'reassign_to_nucleus', 'reassign_to_share']):
                                if (action=='change_priority' and int(params[0])>570):
                                    denied_tasks.append(task)
                                else:
                                    pass
                        else:
                                denied_tasks.append(task)
                    else:
                        if is_superuser or (username==task_owner):
                            pass
                        elif (userfullname == task_owner) and (task_name.split('.')[1] == username):
                            pass
                        else:
                            denied_tasks.append(task)
    return denied_tasks, not_allowed_tasks


def get_permissions(username,tasks):
    """

    :param request: HTTP request
    :return: is_permitted: True/False
    """


    is_superuser=False
    user = ""
    user_groups = ""
    user_permissions = []
    group_permissions = []
    task_owner = ""

    try:
            user = User.objects.get(username=username)
            user_groups = user.groups.all()
            is_superuser = user.is_superuser
            user_permissions = user.user_permissions.all()
            for gp in user_groups:
                    group_permissions += list(gp.permissions.all())
    except:
            pass

    is_permitted=False
    denied_tasks=[]

    allowed_groups = []
    for gp in group_permissions:
            if "has_" in gp.name and "_permissions" in gp.name:
            #        phg=code[(code.find("has_"))+4:code.find("_rights")]
                     allowed_groups.append(gp.codename)
    #print "allowed_groups:", allowed_groups

    for task in tasks:
            task_owner = ProductionTask.objects.values('username').get(id=task).get('username')
            physgroup = ProductionTask.objects.values('phys_group').get(id=task).get('phys_group')

            #print "phys_group:", physgroup
   
            if is_superuser is True or user==task_owner:
                    is_permitted=True
            elif physgroup in allowed_groups:
                    is_permitted=True 
            elif "DPD" in allowed_groups:
                    is_permitted=True
            elif "MCCOORD" in  allowed_groups:
                    is_permitted=True
            else:
                    denied_tasks.append(task)

    if len(denied_tasks)>0: 
           is_permitted=False          

    return (is_permitted,denied_tasks)


def create_fake_task(step_id,task_id):
    new_fake_task = ProductionTask()
    step = StepExecution.objects.get(id=step_id)
    new_fake_task.id = task_id
    new_fake_task.step = step
    new_fake_task.request = step.request
    new_fake_task.parent_id = task_id
    new_fake_task.chain_id = task_id
    new_fake_task.submit_time = timezone.now()
    new_fake_task.reference = ''
    new_fake_task.campaign = ''
    new_fake_task.jedi_info = ''
    new_fake_task.save()
    return new_fake_task



def sync_deft_rucio_nevents(task_id):
    task = ProductionTask.objects.get(id=task_id)
    ddm = DDM()
    rucio_nEvents = ddm.dataset_metadata(task.output_dataset)['events']
    if rucio_nEvents > task.total_events:
        print(task_id, rucio_nEvents, task.total_events)
        if task.status in ProductionTask.NOT_RUNNING:
            ttask = TTask.objects.get(id=task.id)
            setattr(ttask,'total_events',rucio_nEvents)
            ttask.save()
        setattr(task,'total_events',rucio_nEvents)
        task.save()


def sync_request(request_id):
    tasks = ProductionTask.objects.filter(request=request_id)
    for task in tasks:
        if task.status not in ['finished','done','obsolete']+ProductionTask.RED_STATUS:
            sync_deft_jedi_task(task.id)


def sync_old_tasks(task_number, time_interval = 14400):
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    if task_number>0:
        tasks = list(ProductionTask.objects.filter(id__lte=task_number,status__in=ProductionTask.SYNC_STATUS, request_id__gte=2000))
    else:
        tasks = list(ProductionTask.objects.filter(status__in=ProductionTask.SYNC_STATUS, request_id__gte=2000))
    for task_chunk in chunks(tasks, 5000):
        task_ids = [task.id for task in task_chunk]
        jedi_tasks =  list(TTask.objects.filter(id__in=task_ids).values())
        jedi_tasks_by_id = {}
        for jedi_task in jedi_tasks:
            jedi_tasks_by_id[int(jedi_task['id'])] = jedi_task
        print(datetime.utcnow().replace(tzinfo=pytz.utc))
        for task in task_chunk:
            jedi_task = jedi_tasks_by_id[int(task.id)]
            if (task.status != jedi_task['status']) or (jedi_task['timestamp'] > task.timestamp):
                if (task.status != 'toretry') or (jedi_task['status']!='finished'):
                    sync_deft_jedi_task_from_db(task,jedi_task)



def sync_deft_jedi_task_from_db(deft_task,t_task):
    jedi_values = {}
    _logger.info("Sync task between deft and jedi task id:%s"%str(deft_task.id))
    sync_keys = ['status', 'total_done_jobs', 'start_time', 'total_req_jobs','total_events','current_priority']
    for item in sync_keys:
        jedi_values.update({item:t_task[item]})
    #post production status
    if deft_task.status in ['obsolete']:
        jedi_values['status'] = deft_task.status
    if (deft_task.status == 'toabort') and (jedi_values['status'] != 'aborted'):
        jedi_values['status'] = deft_task.status
    jedi_task = JediTasks.objects.filter(id=deft_task.id).values('start_time','errordialog','status')[0]
    if not jedi_values['start_time']:
        jedi_values['start_time'] = jedi_task['start_time']
    jedi_values['jedi_info'] = jedi_task['errordialog'][0:255]
    if not jedi_values['jedi_info']:
        jedi_values['jedi_info'] = 'no info from JEDI'
    if jedi_task['status'] == 'aborting':
        jedi_values['status'] = 'toabort'
    do_update = False
    for item in list(jedi_values.keys()):
        if jedi_values[item] != deft_task.__getattribute__(item):
            do_update = True
            break
    if do_update:
        nfiles_stat = ['total_files_tobeused','total_files_used','total_files_finished','total_files_failed','total_files_onhold']
        for nfiles_name in nfiles_stat:
            jedi_values.update({nfiles_name:0})
        jedi_datasets = list(JediDatasets.objects.filter(id=deft_task.id,type__in=['input','pseudo_input']).values())
        for jedi_dataset in jedi_datasets:
            if (jedi_dataset['masterid'] == None) and (not jedi_dataset['datasetname'].startswith('ddo')) \
                    and (not jedi_dataset['datasetname'].startswith('panda')) and ('.log.' not in jedi_dataset['datasetname']):
                for nfiles_name in nfiles_stat:
                    if jedi_dataset[nfiles_name]:
                        jedi_values[nfiles_name] = jedi_values[nfiles_name] + jedi_dataset[nfiles_name]
    for item in list(jedi_values.keys()):
        if jedi_values[item] != deft_task.__getattribute__(item):
            _logger.info("Field %s updated: %s - %s"%(item, jedi_values[item], deft_task.__getattribute__(item)))
            setattr(deft_task, item, jedi_values[item])
    if do_update:
        deft_task.timestamp=timezone.now()
        deft_task.save()

def create_user_task(task_id: int) -> int:
    if not TTask.objects.filter(id=task_id).exists():
        raise ValueError(f'Task with id {task_id} does not exist in JEDI')
    t_task = TTask.objects.get(id=task_id)
    if ProductionTask.objects.filter(id=task_id).exists():
        return task_id
    prod_task = ProductionTask(id=t_task.id,
                               step=201,
                               request=300,
                               parent_id=t_task.parent_tid,
                               name=t_task.name,
                               project=t_task.name.split('.')[0],
                               phys_group='',
                               provenance='',
                               status=t_task.status,
                               total_events=0,
                               total_req_jobs=0,
                               total_done_jobs=0,
                               submit_time=t_task.submit_time,
                               bug_report=0,
                               priority=t_task.priority,
                               inputdataset='',
                               timestamp=timezone.now(),
                               vo='atlas',
                               prodSourceLabel='user',
                               username=t_task.username,
                               chain_id=t_task.chain_id,
                               campaign='',
                               subcampaign='',
                               bunchspacing='',
                               total_req_events=-1,
                               simulation_type='notMC',
                               primary_input='',
                               ami_tag='',
                               output_formats='')
    prod_task.save()
    return task_id
def sync_deft_jedi_task(task_id):
    """
    Sync task between JEDI and DEFT DB.
    :param task_id: task id for sync
    :return:
    """


    deft_task = ProductionTask.objects.get(id=task_id)
    if deft_task.request_id != 300:
        t_task =  TTask.objects.filter(id=task_id).values('status','total_done_jobs','start_time','total_req_jobs',
                                                       'total_events','current_priority')[0]
        sync_deft_jedi_task_from_db(deft_task, t_task)
    else:
        sync_deft_user_task(task_id)


def sync_deft_user_task(task_id):
    deft_task = ProductionTask.objects.get(id=task_id)
    if deft_task.request.reqid == 300:
        t_task = TTask.objects.get(id=task_id)
        if t_task.status != deft_task.status:
            deft_task.status = t_task.status
            deft_task.timestamp = timezone.now()
            deft_task.save()

def create_mc_exhausted_hashtag():
    tasks = ProductionTask.objects.filter(status__in=['assigning','running','ready','submitting','registered','exhausted','waiting'],provenance='AP',project__startswith='mc')
    exhausted_tasks = []
    for task in tasks:
        if task.status == 'exhausted':
            exhausted_tasks.append(task)
        else:
            jedi_task = JediTasks.objects.get(id=task.id)
            if (jedi_task.status == 'exhausted') or (jedi_task.superstatus == 'exhausted'):
                exhausted_tasks.append(task)
    hashtag = add_or_get_request_hashtag('MC_exhausted')
    hashtag_tasks = hashtag.tasks
    for task in hashtag.tasks:
        if task not in exhausted_tasks:
            task.remove_hashtag(hashtag)
    for task in exhausted_tasks:
        if task not in hashtag_tasks:
            task.set_hashtag(hashtag)

def create_nucleus_hashtag(nucleus,hashtag):
    tasks = ProductionTask.objects.filter(status__in=['assigning','running','ready','submitting','registered','exhausted','waiting'],request_id__gte=1000)
    selcted_tasks = []
    for task in tasks:
            jedi_task = JediTasks.objects.get(id=task.id)
            if (jedi_task.nucleus == nucleus) :
                selcted_tasks.append(task)
    hashtag = add_or_get_request_hashtag(hashtag)
    hashtag_tasks = hashtag.tasks
    for task in hashtag.tasks:
        if task not in selcted_tasks:
            task.remove_hashtag(hashtag)
    for task in selcted_tasks:
        if task not in hashtag_tasks:
            task.set_hashtag(hashtag)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def sync_request_tasks(request, reqid):

    try:
        tasks = list(ProductionTask.objects.filter(request=reqid))
        for task in tasks:
            sync_deft_jedi_task(task.id)
    except Exception as e:
        content = str(e)
        return Response(content,status=500)

    return Response({'success':True})

def mc_ami_tags_reduction(postfix):
    if 'tid' in postfix:
        postfix = postfix[:postfix.find('_tid')]

    new_postfix = []
    first_letter = ''
    for token in postfix.split('_')[:-1]:
        if token[0] != first_letter and not (token[0] == 's' and first_letter == 'a'):
            new_postfix.append(token)
        first_letter = token[0]
    new_postfix.append(postfix.split('_')[-1])
    return '_'.join(new_postfix)

def get_mc_container_name(dataset_name):
    return '.'.join(dataset_name.split('.')[:-1] + [mc_ami_tags_reduction(dataset_name.split('.')[-1])])




def fill_main_container(production_request_id):
    tasks = ProductionTask.objects.filter(request_id=production_request_id, status__in=[ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED])
    ddm = DDM()
    for task in tasks:
        if 'merge' in task.name:
            for dataset in task.output_non_log_datasets():
                if 'tid' in dataset :
                    container_name = dataset.split('_tid')[0]
                    if not ddm.dataset_exists(container_name):
                        ddm.register_container(container_name, [dataset])
                        print(f"Container {container_name} does not exist {dataset}")
                    else:
                        if not ddm.dataset_is_in_container(dataset, container_name):
                            ddm.register_datasets_in_container(container_name, [dataset])
                            print(f"Dataset {dataset} is not in container {container_name}")




def check_merge_container(days, days_till=1):
    time_since = timezone.now() - timedelta(days=days)
    time_till = timezone.now() - timedelta(days=days_till)
    ddm = DDM()
    tasks = ProductionTask.objects.filter(timestamp__gte=time_since, timestamp__lte=time_till, provenance='AP', status__in=['done','finished'],
                                          project__startswith='mc')
    total = 0
    for task in tasks:
        if (task.phys_group not in ['SOFT','VALI']) and ('merge' in task.name):
            parent_dataset = task.primary_input
            if 'tid' not in parent_dataset:
                _logger.warning('Warning no tid in parent {task_id}'.format(task_id=task.id))
            else:
                container_name = get_mc_container_name(parent_dataset)
                container_datasets = ddm.dataset_in_container(container_name)
                container_datasets = list(map(name_without_scope,container_datasets))
                datasets = ProductionDataset.objects.filter(task_id=task.id)
                dataset_name = None
                for dataset in datasets:
                    if '.log.' not in dataset.name:
                        dataset_name = name_without_scope(dataset.name)
                        break
                if (parent_dataset in container_datasets) and (dataset_name in container_datasets):
                    _logger.error('Error for {task_id} both datasets are in container {container}. {parent} will be removed'.format(task_id=task.id,container=container_name,parent=parent_dataset))
                    ddm.delete_datasets_from_container(container_name,[parent_dataset])
                    total += 1
    return total


@app.task(time_limit=10800, ignore_result=True)
def find_merge_dataset_to_delete(is_mc = True):
    ddm = DDM()
    prefix = 'mc'
    if not is_mc:
        prefix = 'data'
    result = {}
    for x in OUTPUTS_TYPES:
        result[x] = []
    tasks = ProductionTask.objects.filter(timestamp__gte=timezone.now() - timedelta(days=60),provenance='AP',
                                          status__in=['done','finished'],name__startswith=prefix )
    merge_tasks = [x for x in tasks if x.phys_group not in ['SOFT','VALI'] and 'merge' in x.name and x.output_formats in OUTPUTS_TYPES]
    deleted_datasets = []
    for task in merge_tasks:
        parent_dataset = task.primary_input
        if 'tid' in parent_dataset:
            production_dataset = ProductionDataset.objects.get(name=name_without_scope(parent_dataset))
            if production_dataset.status != 'Deleted':
                if ddm.dataset_exists(parent_dataset):
                    dataset_details = ddm.dataset_metadata(parent_dataset)
                    if ((dataset_details['datatype'] in OUTPUTS_TYPES) and dataset_details['expired_at']
                            and (dataset_details['expired_at'] - timezone.now().replace(tzinfo=None) > timedelta(days=1))):
                        result[dataset_details['datatype']].append({'name':dataset_details['name'],
                                                                    'bytes':dataset_details['bytes'],
                                                                    'daysLeft':(dataset_details['expired_at'] - timezone.now().replace(tzinfo=None)).days,
                                                                    'task_id':dataset_details['task_id'], 'parent_task_id': task.id,
                                                                    'parentPer':task.total_files_failed / (task.total_files_finished+task.total_files_failed)})

                else:
                    deleted_datasets.append(parent_dataset)

        else:
            _logger.warning('Warning no tid in parent {task_id}'.format(task_id=task.id))

    for x in OUTPUTS_TYPES:
        cache.set('del_merge_%s_%s'% (prefix, x),result[x],None)
    cache.set('deleted_datasets', deleted_datasets,None)
    cache.set('merge_deletion_update_time',timezone.now(),None)

@app.task(time_limit=10800, ignore_result=True)
def find_merge_dataset_not_delete(is_mc = True, days_from=356, days_to=60):
    ddm = DDM()
    prefix = 'mc'
    if not is_mc:
        prefix = 'data'
    result = {}
    for x in OUTPUTS_TYPES:
        result[x] = []
    tasks = ProductionTask.objects.filter(timestamp__lte=timezone.now() - timedelta(days=days_to),
                                          timestamp__gte=timezone.now() - timedelta(days=days_from),provenance='AP',
                                          status__in=['done','finished'],name__startswith=prefix )
    merge_tasks = [x for x in tasks if x.phys_group not in ['SOFT','VALI'] and 'merge' in x.name and x.output_formats in OUTPUTS_TYPES]
    datasets = 0
    size = 0
    for task in merge_tasks:
        parent_dataset = task.primary_input
        if 'tid' in parent_dataset:
            production_dataset = ProductionDataset.objects.get(name=name_without_scope(parent_dataset))
            if production_dataset.status != 'Deleted':
                if ddm.dataset_exists(parent_dataset):
                    dataset_details = ddm.dataset_metadata(parent_dataset)
                    if (dataset_details['datatype'] in OUTPUTS_TYPES):
                        result[dataset_details['datatype']].append({'name':dataset_details['name'],
                                                                    'bytes':dataset_details['bytes'],
                                                                    'task_id':dataset_details['task_id'], 'parent_task_id': task.id,
                                                                    'parentPer':task.total_files_failed / (task.total_files_finished+task.total_files_failed)})
                        datasets+=1
                        size+=dataset_details['bytes']


        else:
            _logger.warning('Warning no tid in parent {task_id}'.format(task_id=task.id))

    for x in OUTPUTS_TYPES:
        cache.set('not_deleted_unmerge_%s_%s_%s'% (prefix, x, str(days_from)),result[x],None)
    return datasets, size


@app.task(time_limit=10800, ignore_result=True)
def find_special_deletion(parent_ami_tag, child_ami_tag):
    tasks = ProductionTask.objects.filter(timestamp__gte=timezone.now() - timedelta(days=120),provenance='AP',
                                          status__in=['done','finished'],name__startswith='mc',ami_tag=parent_ami_tag)
    ddm = DDM()
    filtered_tasks =  [x for x in tasks if x.phys_group not in ['VALI'] and child_ami_tag in x.name ]
    deleted_datasets = cache.get('deleted_datasets',[])
    result = []
    for task in filtered_tasks:
        parent_dataset = task.primary_input
        if 'tid' in parent_dataset:
            production_dataset = ProductionDataset.objects.get(name=name_without_scope(parent_dataset))
            if (production_dataset.status != 'Deleted') and (parent_dataset not in deleted_datasets):
                if ddm.dataset_exists(parent_dataset):
                    dataset_details = ddm.dataset_metadata(parent_dataset)
                    result.append({'name':dataset_details['name'],
                                                                'bytes':dataset_details['bytes'],
                                                                'daysLeft':-1,
                                                                'task_id':dataset_details['task_id'],'parent_task_id': task.id,
                                                                'parentPer':task.total_files_failed / (task.total_files_finished+task.total_files_failed)})

                else:
                    deleted_datasets.append(parent_dataset)
    cache.set('del_special_%s_%s'% (parent_ami_tag, child_ami_tag),result,None)
    cache.set('deleted_datasets', deleted_datasets,None)
    cache.set('special_deletion_update_time',timezone.now(),None)


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def unmerged_datasets_to_delete(request):
    """
    """
    result = {}
    result['outputs'] = {}
    prefix = 'mc'
    if request.query_params.get('perfix'):
        prefix = request.query_params.get('perfix')
    for output in OUTPUTS_TYPES:
        result['outputs'][output] = cache.get('del_merge_%s_%s'% (prefix, output))
    result['timestamp'] = cache.get('merge_deletion_update_time')

    return Response(result)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def unmerge_datasets_not_deleted(request):
    """
    """
    result = {}
    result['outputs'] = {}
    prefix = 'mc'
    if request.query_params.get('prefix'):
        prefix = request.query_params.get('prefix')
    for output in OUTPUTS_TYPES:
        result['outputs'][output] = cache.get('not_deleted_unmerge_%s_%s'% (prefix, output))
    result['timestamp'] = timezone.now()

    return Response(result)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def special_datasets_to_delete(request):
    """
    """
    result = {}
    parent_ami_tag = ''
    child_ami_tag = ''
    if request.query_params.get('parent_tag'):
        parent_ami_tag = request.query_params.get('parent_tag')
    if request.query_params.get('child_tag'):
        child_ami_tag = request.query_params.get('child_tag')
    result['outputs'] = {"special": cache.get('del_special_%s_%s'% (parent_ami_tag, child_ami_tag))}
    result['timestamp'] = cache.get('special_deletion_update_time')

    return Response(result)


def recover_obsolete(task_id):
    task = ProductionTask.objects.get(id=task_id)
    if task.status == 'obsolete':
        ddm = DDM()
        tt_task = TTask.objects.get(id=task_id)
        task.status = tt_task.status
        task.save()
        datasets = ProductionDataset.objects.filter(task_id=task.id)
        for dataset in datasets:
            if not ddm.dataset_exists(dataset.name):
                print(f"Recover {dataset.name}")
                continue
            if 'log' not in dataset.name and dataset.status.lower() != 'done':
                dataset.status = 'done'
                dataset.save()


def get_global_shares(update_cache=False):
    if not update_cache and cache.get('global_share'):
        return cache.get('global_share')
    all_global_share = list(GlobalShare.objects.all())
    leaf_shares = {}
    for share in all_global_share:
        leaf_shares.pop(share.parent, '')
        leaf_shares[share.name] = share.parent
    leaf_shares_list = list(leaf_shares.items())
    leaf_shares_list.sort(key=lambda x:x[1]+'-'+x[0])
    result = [x[0] for x in leaf_shares_list]
    cache.set('global_share', result, 3600*24)
    return result


def tasks_serialisation(tasks: [ProductionTask], hashtags: Dict[int, str] = None) -> [dict]:
    step_from_name = {
        '.evgen.': 'Evgen',
        '.simul.': 'Simul',
        '.recon.': 'Reco',
        '.deriv.': 'Deriv',
    }
    tasks_serial = []
    step_template_name = {}
    for task in tasks:
        serial_task = task.__dict__
        if '_state' in serial_task:
            del serial_task['_state']
        step_name = ''
        for key in step_from_name:
            if key in task.name:
                step_name = step_from_name[key]
                break
        if not step_name:
            if task.name.startswith('data') and '.merge.' in task.name:
                step_name = 'Merge'
            else:
                if task.ami_tag not in step_template_name:
                    step_id = StepExecution.objects.filter(id=task.step_id).values("step_template_id").get()['step_template_id']
                    step_template_name[task.ami_tag] = StepTemplate.objects.filter(id=step_id).values("step").get()['step']
                step_name = step_template_name[task.ami_tag]
        serial_task.update(dict(step_name=step_name))
        serial_task['failureRate'] = task.failure_rate or 0
        if hashtags and task.id in hashtags:
            serial_task['hashtags'] = hashtags[task.id]
        tasks_serial.append(serial_task)
    return tasks_serial