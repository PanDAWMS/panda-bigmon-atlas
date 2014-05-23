

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse

from ..settings import defaultDatetimeFormat

import core.datatables as datatables

from .forms import ProductionTaskForm, ProductionTaskCreateCloneForm, ProductionTaskUpdateForm
from .models import ProductionTask, TRequest

from django.db.models import Count, Q


from django.utils.timezone import utc
from datetime import datetime

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

   return render(request, 'prodtask/_task_detail.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'ProductionTask details with ID = %s' % rid,
       'task': task,
       'parent_template' : 'prodtask/_index.html',
   })

def task_clone(request, rid=None):
   if request.method == 'POST':
       form = ProductionTaskCreateCloneForm(request.POST)
       if form.is_valid():
          # Process the data in form.cleaned_data
           req = ProductionTask(**form.cleaned_data)
           req.save()
           return HttpResponseRedirect('/prodtask/task/%s' % req.id) # Redirect after POST
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
           return HttpResponseRedirect('/prodtask/task/%s' % req.id) # Redirect after POST
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
           return HttpResponseRedirect('/prodtask/trequest/%s' % req.id) # Redirect after POST
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
  #      sClass='px100',
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

    priority = datatables.Column(
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
        )

    submit_time = datatables.Column(
        label='Submit time',
        sClass='px100',
   #     bVisible='false',
        )

    timestamp = datatables.Column(
        label='Timestamp',
        sClass='px100',
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

    class Meta:
        model = ProductionTask

        id = 'task_table'
        var = 'taskTable'
        bSort = True
        bPaginate = True
        bJQueryUI = True

        sScrollX = '100%'
      #  sScrollY = '25em'
        bScrollCollapse = True

        aaSorting = [[3, "desc"]]
        aLengthMenu = [[100, 1000, -1], [100, 1000, "All"]]
        iDisplayLength = 100

        fnServerParams = "taskServerParams"

        fnDrawCallback = "taskDrawCallback"

        fnServerData =  "taskServerData"


        bServerSide = True
        sAjaxSource = '/prodtask/task_table/'

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
                            ('request','request__reqid'), ('chain','chain_tid'), ('status','status'),
                            ('provenance', 'provenance'), ('phys_group','phys_group') ]

        for param in parameters:
            value = request.GET.get(param[0], 0)
            if value:
                if value != 'None':
                    qs = qs.filter(Q( **{ param[1]+'__icontains' : value } ))
                else:
                    qs = qs.filter(Q( **{ param[1]+'__exact' : '' } ))

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

