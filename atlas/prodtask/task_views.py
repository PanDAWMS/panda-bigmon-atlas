

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.core.urlresolvers import reverse

from ..settings import defaultDatetimeFormat

import core.datatables as datatables
from core.resource.models import Schedconfig

from .forms import ProductionTaskForm, ProductionTaskCreateCloneForm, ProductionTaskUpdateForm
from .models import ProductionTask, TRequest, TTask, ProductionDataset

from django.db.models import Count, Q


from django.utils.timezone import utc
from datetime import datetime

import locale
import time



def task_details(request, rid=None):
   if rid:
       try:
           task = ProductionTask.objects.get(id=rid)
           ttask = TTask.objects.get(id=rid)
           output_datasets = ProductionDataset.objects.filter(task_id=rid)
          # form = ProductionTaskForm(instance=req)
       except:
           return HttpResponseRedirect('/')
   else:
       return HttpResponseRedirect('/')

   # TODO: check user permissions on the task (SB)
   # TODO: handle actions for 'waiting' tasks (they're in DEFT, not yet in JEDI) (SB)
   task_not_ended = (task.status in ['registered', 'assigning', 'submitting', 'running'])

   permissions = {}
   # TODO: these actions are needed from DEFT and JEDI (SB)
   for action in ['edit', 'clone']:
       permissions[action] = False

   permissions['obsolete'] = task.status in ['done', 'finished']
   permissions['retry'] = task.status in ['finished']

   for action in ['abort', 'finish', 'change_prio', 'reassign']:
       permissions[action] = task_not_ended


   request_parameters = {
       'active_app' : 'prodtask',
       'pre_form_text' : 'ProductionTask details with ID = %s' % rid,
       'task': task,
       'ttask': ttask,
       'output_datasets': output_datasets,
       'clouds': get_clouds(),
       'sites': get_sites(),
       'parent_template' : 'prodtask/_index.html',
   }

   for action, perm in permissions.items():
       request_parameters['can_' + action + '_task'] = perm

   return render(request, 'prodtask/_task_detail.html', request_parameters)


def task_clone(request, rid=None):
   if request.method == 'POST':
       form = ProductionTaskCreateCloneForm(request.POST)
       if form.is_valid():
          # Process the data in form.cleaned_data
           req = ProductionTask(**form.cleaned_data)
           req.save()
           return HttpResponseRedirect(reverse('task', args=(req.id,))) # Redirect after POST
   else:
       try:
           values = ProductionTask.objects.values().get(id=rid)
       except:
           return HttpResponseRedirect('/')
       del values['id']
       form = ProductionTaskCreateCloneForm(values)

   return render(request, 'prodtask/_form.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'Clonning of ProductionTask with ID = %s' % rid,
       'form': form,
       'submit_url': 'prodtask:task_clone',
       'url_args'  : rid,
       'parent_template' : 'prodtask/_index.html',
   })

def task_update(request, rid=None):
   if request.method == 'POST':
       try:
           req = ProductionTask.objects.get(id=rid)
           form = ProductionTaskUpdateForm(request.POST, instance=req) # A form bound to the POST data
       except:
           return HttpResponseRedirect('/')
       if form.is_valid():
          # Process the data in form.cleaned_data
           req = ProductionTask(**form.cleaned_data)
           req.save()
           return HttpResponseRedirect(reverse('task', args=(req.id,))) # Redirect after POST
   else:
       try:
           req = ProductionTask.objects.get(id=rid)
           form = ProductionTaskUpdateForm(instance=req)
       except:
           return HttpResponseRedirect('/')
   return render(request, 'prodtask/_form.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'Updating of ProductionTask with ID = %s' % rid,
       'form': form,
       'submit_url': 'prodtask:task_update',
       'url_args': rid,
       'parent_template' : 'prodtask/_index.html',
   })

def task_create(request):
   if request.method == 'POST': # If the form has been submitted...
       form = ProductionTaskCreateCloneForm(request.POST) # A form bound to the POST data
       if form.is_valid(): # All validation rules pass
           # Process the data in form.cleaned_data
           req = ProductionTask(**form.cleaned_data)
           req.save()
           return HttpResponseRedirect( reverse('prodtask:task', req.id) ) # Redirect after POST
   else:
       form = ProductionTaskCreateCloneForm() # An unbound form
   return render(request, 'prodtask/_form.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'Create ProductionTask',
       'form': form,
       'submit_url': 'prodtask:task_create',
       'parent_template' : 'prodtask/_index.html',
   })


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
        label='Req Jobs',
        sClass='numbers',
        )

    total_done_jobs = datatables.Column(
        label='Done Jobs',
        sClass='numbers',
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
        label='Provenance',
        bVisible='false',
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
    step_output_format = datatables.Parameter(label='Step output format', get_Q=lambda v: Q( **{ 'step__step_template__output_formats__iexact' : v } ) )

    ctag = datatables.Parameter(label='AMI tag exact', get_Q=lambda v: Q( **{ 'step__step_template__ctag__iexact' : v } ) )

    task_name = datatables.Parameter(label='Task name', name='taskname', id='taskname', get_Q=lambda v: Q( **{ 'name__iregex' : v } ) )

    class Meta:
        SetParametersToURL = 'SetParametersToURL'
        ParseParametersFromURL = 'ParseParametersFromURL'
        var = 'parameters_list'

    def _task_status_Q(value):
        if value == 'active':
            return Q( status__in=['done','finished','failed','broken','aborted'] ).__invert__()
        elif value == 'ended':
            return Q( status__in=['done','finished'] )
        elif value == 'regular':
            return Q( status__in=['failed','broken','aborted'] ).__invert__()
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
