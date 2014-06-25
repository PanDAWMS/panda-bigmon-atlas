

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
from .models import ProductionTask, TRequest

from django.db.models import Count, Q


from django.utils.timezone import utc
from datetime import datetime

import locale
import time



def task_details(request, rid=None):
   if rid:
       try:
           task = ProductionTask.objects.get(id=rid)
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

   for action in ['abort', 'finish', 'change_prio', 'reassign']:
       permissions[action] = task_not_ended


   request_parameters =  {
       'active_app' : 'prodtask',
       'pre_form_text' : 'ProductionTask details with ID = %s' % rid,
       'task': task,
       'clouds': get_clouds(),
       'sites': get_sites(),
       'parent_template' : 'prodtask/_index.html',
   }

   for action, perm in permissions.items():
       request_parameters['can_' + action + '_task' ] = perm

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
        
    class Meta:
        model = ProductionTask

        id = 'task_table'
        var = 'taskTable'
        bSort = True
        bPaginate = True
        bJQueryUI = True

        bAutoWidth = False
      #  width = "1200px"

      #  sScrollX = '100%'
      #  sScrollY = '25em'
        bScrollCollapse = False

        aaSorting = [[4, "desc"]]
        aLengthMenu = [[100, 1000, -1], [100, 1000, "All"]]
        iDisplayLength = 100

        fnServerParams = "taskServerParams"

        fnDrawCallback = "taskDrawCallback"

        fnServerData =  "taskServerData"


        bServerSide = True

        def __init__(self):
            self.sAjaxSource = reverse('task_table')


    def apply_first_page_filters(self, request):

        self.apply_filters(request)

        qs = self.get_queryset()

        parameters = [ ('project','project'), ('username','username'), ('request','request__reqid'), ('chain','chain_tid'), ('status','status')]
        for param in parameters:
            value = request.GET.get(param[0], 0)
            if value:
                if value != 'None':
                    qs = qs.filter(Q( **{ param[1]+'__icontains' : value } ))
                else:
                    qs = qs.filter(Q( **{ param[1]+'__exact' : '' } ))

        self.update_queryset(qs)

    def apply_filters(self, request):
        qs = self.get_queryset()

        parameters = [   ('project','project'), ('username','username'), ('taskname','name'),
                            ('request','request__reqid'), ('chain','chain_tid'),
                            ('provenance', 'provenance'), ('phys_group','phys_group'),
                            ('step_name', 'step__step_template__step'), ('step_output_format', 'step__step_template__output_formats') ]

        for param in parameters:
            value = request.GET.get(param[0], 0)
            if value:
                if value != 'None':
                    qs = qs.filter(Q( **{ param[1]+'__icontains' : value } ))
                else:
                    qs = qs.filter(Q( **{ param[1]+'__exact' : '' } ))

        task_type = request.GET.get('status', 0)
        if task_type == 'active':
            qs = qs.exclude( status__in=['done','finished','failed','broken','aborted'] )
        elif task_type == 'ended':
            qs = qs.filter( status__in=['done','finished'] )
        elif task_type == 'regular':
            qs = qs.exclude( status__in=['failed','broken','aborted'] )
        elif task_type == 'irregular':
            qs = qs.filter( status__in=['failed','broken','aborted'] )
        elif task_type:
            qs = qs.filter(Q( **{ param[1]+'__icontains' : value } ))

        task_type = request.GET.get('task_type', 'production')
        if task_type == 'production':
            qs = qs.exclude(project='user')
        elif task_type == 'analysis':
            qs = qs.filter(project='user')

        time_from = request.GET.get('time_from', 0)
        time_to = request.GET.get('time_to', 0)

        if time_from:
            time_from = float(time_from)/1000.
        else:
            time_from = time.time() - 3600 * 24 * 60

        if time_to:
            time_to = float(time_to)/1000.
        else:
            time_to = time.time()

        time_from = datetime.utcfromtimestamp(time_from).replace(tzinfo=utc).strftime(defaultDatetimeFormat)
        time_to = datetime.utcfromtimestamp(time_to).replace(tzinfo=utc).strftime(defaultDatetimeFormat)

        qs = qs.filter(timestamp__gt=time_from).filter(timestamp__lt=time_to)

        self.update_queryset(qs)

    def prepare_ajax_data(self, request):

        self.apply_filters(request)

        params = request.fct.parse_params(request)

        qs = request.fct.get_queryset()

        qs = request.fct._handle_ajax_global_search(qs, params)
        qs = request.fct._handle_ajax_column_specific_search(qs, params)

   #     qs = request.fct.apply_sort_search(qs, params)

        status_stat = [ { 'status':'total', 'count':qs.count() } ] + [ { 'status':str(x['status']), 'count':str(x['count']) }  for x in qs.values('status').annotate(count=Count('id')) ]

        data = datatables.DataTable.prepare_ajax_data(request.fct, request)

        data['task_stat'] = status_stat

        return data




@datatables.datatable(ProductionTaskTable, name='fct')
def task_table(request):

    qs = request.fct.get_queryset()

    last_task_submit_time = ProductionTask.objects.order_by('-submit_time')[0].submit_time

  #  request.fct.apply_first_page_filters(request)

    return TemplateResponse(request, 'prodtask/_task_table.html', { 'title': 'Production Tasks Table',
                                                                    'active_app' : 'prodtask',
                                                                    'table': request.fct,
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

