from copy import deepcopy

from django.forms.models import model_to_dict
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response, redirect
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.urls import reverse, resolve

from atlas.prodtask.check_duplicate import find_downstreams_by_task, create_task_chain
from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.hashtag import add_or_get_request_hashtag
from atlas.prodtask.models import StepExecution
import logging

from ..settings import defaultDatetimeFormat

import atlas.datatables as datatables
from .models import Schedconfig

from .forms import ProductionTaskForm, ProductionTaskCreateCloneForm, ProductionTaskUpdateForm
from .models import ProductionTask, TRequest, TTask, ProductionDataset, Site, JediTasks, JediDatasets
from .task_actions import allowed_task_actions

from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authentication import TokenAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.http import HttpResponseForbidden
from django.contrib.auth.models import User
from django.db.models import Count, Q
from django.utils import timezone

from django.utils.timezone import utc
from datetime import datetime
import pytz
import locale
import time
import json


_logger = logging.getLogger('prodtaskwebui')

from django.views.decorators.csrf import csrf_protect, csrf_exempt, ensure_csrf_cookie

GLOBAL_SHARES = [
    'Express',
    'Validation',
    'Test',
    'Special',
    'MC 16',
    'MC Other',
    'MC 16 evgen',
    'MC Other evgen',
    'MC 16 simul',
    'MC Other simul',
    'MC merge',
    'Reprocessing default',
    'Heavy Ion',
    'Spillover',
    'MC Derivations',
    'Data Derivations',
    'Overlay',
    'User Analysis',
    'Group Higgs',
    'Group SM',
    'Group Exotics',
    'Group Susy',
    'Group Analysis',
    'Upgrade',
    'HLT Reprocessing',
    'Event Index',
    'Frontier'

]
def descent_tasks(request, task_id):
    try:
        child_tasks = find_downstreams_by_task(task_id)
        request.session['selected_tasks'] = [int(task_id)] + [int(x['id']) for x in child_tasks]
        return render(request, 'reqtask/_task_table.html',{'reqid':None,'title': 'Descent tasks for task %s'%str(task_id)})
    except Exception as e:
        _logger.error("Problem with retrieving descends tasks for %s: %s"%(str(task_id),str(e)))
        return HttpResponseRedirect('/')


@api_view(['GET'])
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

def get_permission_analy(action_username, tasks, userfullname):
    is_superuser=False
    user = ""
    group_permissions = []

    if  User.objects.filter(username=action_username).exists():
        user = User.objects.get(username=action_username)
        user_groups = user.groups.all()
        is_superuser = user.is_superuser
        for gp in user_groups:
            group_permissions += list(gp.permissions.all())


    is_permitted=False
    denied_tasks=[]

    allowed_groups = []
    for gp in group_permissions:
            if "has_" in gp.name and "_permissions" in gp.name:
                     allowed_groups.append(gp.codename)


    for task in tasks:
            if ProductionTask.objects.filter(id=task).exists():
                task_owner = ProductionTask.objects.values('username').get(id=task).get('username')
                task_name = ProductionTask.objects.values('name').get(id=task).get('name')
            else:
                task_owner = JediTasks.objects.values('username').get(id=task).get('username')
                task_name = JediTasks.objects.values('taskname').get(id=task).get('taskname')
            #print "phys_group:", physgroup

            if is_superuser is True or user==task_owner:
                is_permitted=True
            elif (userfullname == task_owner) and (task_name.split('.')[1] == action_username):
                is_permitted=True
            else:
                denied_tasks.append(task)

    if len(denied_tasks)>0:
           is_permitted=False

    return (is_permitted,denied_tasks)
    pass






def task_details(request, rid=None):
    if rid:
        try:
            task = ProductionTask.objects.get(id=rid)
            ttask = TTask.objects.get(id=rid)
            output_datasets = ProductionDataset.objects.filter(task_id=rid)
            output_formats = [x.get('name').split('.')[4] for x in output_datasets.values('name')]

            same_tasks = []
            if task.is_extension:
                dataset_pat=output_datasets[0].name.split("tid")[0]
                datasets_extension = ProductionDataset.objects.filter(name__icontains=dataset_pat)
                same_tasks = [int(x.name.split("tid")[1].split("_")[0]) for x in datasets_extension]
            #    ds_pattern=re.search('/.+?(?=tid)/',dataset.name)
        except:
            return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/')

    tasks=[]
    tasks.append(rid)
    #is_permitted,denied_task = get_permissions(request.user.username,tasks)
    denied_task,tmp = check_action_allowed(request.user.username, tasks)
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
        'active_app' : 'prodtask',
        'pre_form_text' : 'ProductionTask details with ID = %s' % rid,
        'task': task,
        'ttask': ttask,
        'output_datasets': output_datasets,
        'clouds': get_clouds(),
        'sites': get_sites(),
        'nucleus': get_nucleus(),
        'shares': GLOBAL_SHARES,
        'outputs': output_formats,
        'extasks': same_tasks,
        'hashtags': hashtags,
        'parent_template' : 'prodtask/_index.html',
        }
    print(permissions)
    for action, perm in list(permissions.items()):
        request_parameters['can_' + action + '_task'] = perm

    return render(request, 'prodtask/_task_detail.html', request_parameters)


def task_clone(request, rid=None):
    return HttpResponseRedirect('/')


def task_update(request, rid=None):
    return HttpResponseRedirect('/')


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

    chain_tid = datatables.Column(
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


def task_status_stat_by_request(request, rid):
    """
    ProductionTasks statuses for specific request
    :return: line with statuses
    """
    qs = ProductionTask.objects.filter(request__reqid=rid)
    stat = get_status_stat(qs)
    return TemplateResponse(request, 'prodtask/_task_status_stat.html', { 'stat': stat, 'reqid': rid})


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
    chain = datatables.Parameter(label='Chain', model_field='chain_tid')
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
                                                                    'last_task_submit_time' : last_task_submit_time,
                                                                    })


def get_clouds():
    """
    Get list of clouds
    :return: list of clouds names
    """
    clouds = [ x.get('cloud') for x in Schedconfig.objects.values('cloud').distinct() ]
    locale.setlocale(locale.LC_ALL, '')
    clouds = sorted(clouds, key=locale.strxfrm)
    return clouds


def get_sites():
    """
    Get list of site names
    :return: list of site names
    """
    sites = [ x.get('siteid') for x in Schedconfig.objects.values('siteid').distinct() ]
    locale.setlocale(locale.LC_ALL, '')
    sites = sorted(sites, key=locale.strxfrm)
    return sites

def get_nucleus():
    """
    Get list of nuclei names
    :return: list of nuclei names
    """
    nucleus = [ x.get('site_name') for x in Site.objects.filter(role='nucleus').values('site_name').distinct() ]
    locale.setlocale(locale.LC_ALL, '')
    nucleus = sorted(nucleus, key=locale.strxfrm)


    return nucleus


def slice_by_task(request, task_id):
    try:
        task = ProductionTask.objects.get(id=task_id)
        request_id = task.request_id
        slice = task.step.slice.slice
    except:
        return HttpResponseRedirect('/')
    return HttpResponseRedirect(reverse('prodtask:input_list_approve_full', args=[request_id])+'#inputList'+str(slice))


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
            if ProductionTask.objects.filter(id=task).exists():
                task_dict = ProductionTask.objects.values('username','name','status','request_id','phys_group').get(id=task)
                task_owner =task_dict.get('username')
                task_name = task_dict.get('name')
                task_status = task_dict.get('status')
                physgroup = task_dict.get('phys_group')
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
                        elif (physgroup in allowed_groups) and (action not in ['increase_priority']):
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
    new_fake_task.chain_tid = task_id
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

    if task_number>0:
        tasks = list(ProductionTask.objects.filter(id__lte=task_number,status__in=ProductionTask.SYNC_STATUS, request_id__gte=2000))
    else:
        tasks = list(ProductionTask.objects.filter(status__in=ProductionTask.SYNC_STATUS, request_id__gte=2000))
    task_ids = [task.id for task in tasks]
    jedi_tasks =  list(TTask.objects.filter(id__in=task_ids).values())
    jedi_tasks_by_id = {}
    for jedi_task in jedi_tasks:
        jedi_tasks_by_id[int(jedi_task['id'])] = jedi_task
    print(datetime.utcnow().replace(tzinfo=pytz.utc))
    for task in tasks:
        jedi_task = jedi_tasks_by_id[int(task.id)]
        if (task.status != jedi_task['status']) or (jedi_task['timestamp'] > task.timestamp):
            if (task.status != 'toretry') or (jedi_task['status']!='finished'):
                sync_deft_jedi_task_from_db(task,jedi_task)



def sync_deft_jedi_task_from_db(deft_task,t_task):
    jedi_values = {}
    _logger.info("Sync task between deft and jedi task id:%s"%str(deft_task.id))
    sync_keys = ['status', 'total_done_jobs', 'start_time', 'total_req_jobs','total_events']
    for item in sync_keys:
        jedi_values.update({item:t_task[item]})
    #post production status
    if deft_task.status in ['obsolete']:
        jedi_values['status'] = deft_task.status
    jedi_task = JediTasks.objects.filter(id=deft_task.id).values('start_time','errordialog')[0]
    if not jedi_values['start_time']:
        jedi_values['start_time'] = jedi_task['start_time']
    jedi_values['jedi_info'] = jedi_task['errordialog'][0:255]
    if not jedi_values['jedi_info']:
        jedi_values['jedi_info'] = 'no info from JEDI'
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


def sync_deft_jedi_task(task_id):
    """
    Sync task between JEDI and DEFT DB.
    :param task_id: task id for sync
    :return:
    """


    deft_task = ProductionTask.objects.get(id=task_id)
    t_task =  TTask.objects.filter(id=task_id).values('status','total_done_jobs','start_time','total_req_jobs',
                                                   'total_events')[0]
    sync_deft_jedi_task_from_db(deft_task, t_task)


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
def sync_request_tasks(request, reqid):

    try:
        tasks = list(ProductionTask.objects.filter(request=reqid))
        for task in tasks:
            sync_deft_jedi_task(task.id)
    except Exception as e:
        content = str(e)
        return Response(content,status=500)

    return Response({'success':True})