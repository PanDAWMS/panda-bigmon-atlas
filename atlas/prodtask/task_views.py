

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
import core.datatables as datatables

from .forms import ProductionTaskForm, ProductionTaskCreateCloneForm, ProductionTaskUpdateForm
from .models import ProductionTask, TRequest

from django.db.models import Count

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
       'fields': task._meta.get_all_field_names(),
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

    id = datatables.Column(
        label='ID',
        )

    step = datatables.Column(
        label='StepEx',
        model_field='step__id',
  #      bVisible='false',        
        )

    request = datatables.Column(
        label='Request',
        model_field='request__reqid',
 #       bVisible='false',
        )

    parent_id = datatables.Column(
        label='Parent id',
        bVisible='false',
        )

    name = datatables.Column(
        label='Name',
        )

    project = datatables.Column(
        label='Project',
#        bVisible='false',
#        sSearch='user',
        )
        
    chain_tid = datatables.Column(
        label='Chain',
        bVisible='false',
#        sSearch='user',
        )
        
    status = datatables.Column(
        label='Status',
        )
        
    phys_group = datatables.Column(
        label='Phys group',
        )
        
    provenance = datatables.Column(
        label='Provenance',
        )

    total_events = datatables.Column(
        label='TEvent',
        )

    total_req_jobs = datatables.Column(
        label='TReq job',
        )

    total_done_jobs = datatables.Column(
        label='TDone job',
        )

    submit_time = datatables.Column(
        label='Submit time',
        )
        
    start_time = datatables.Column(
        label='Start time',
        )
        
    timestamp = datatables.Column(
        label='Timestamp',
        )

    bug_report = datatables.Column(
        label='Bug report',
        )

    priority = datatables.Column(
        label='Priority',
        )
        
    comments = datatables.Column(
        label='Comments',
        )
        
#    inputdataset = datatables.Column(
#        label='Inputdataset',
#        )
        
    physics_tag = datatables.Column(
        label='Physics tag',
        )
        
    username = datatables.Column(
        label='Owner',
        bVisible='false',
        )
        
    class Meta:
        model = ProductionTask
        
        id = 'task_table'
        var = 'taskTable'
        bSort = True
        bPaginate = True
        bJQueryUI = True

        sScrollX = '100%'
        sScrollY = '25em'
        bScrollCollapse = True

        aaSorting = [[0, "desc"]]
        aLengthMenu = [[20, 100, -1], [20, 100, "All"]]
        iDisplayLength = 20

        fnServerParams = """
                            function ( aoData ) {
                                    var time_from = $( "#time_from" ).datepicker( "getDate" );
                                    var time_to = $( "#time_to" ).datepicker( "getDate" );
                                    
                                    time_from = new Date(time_from.getUTCFullYear(), time_from.getUTCMonth(), time_from.getUTCDate()+2, -17);
                                    time_to = new Date(time_to.getUTCFullYear(), time_to.getUTCMonth(), time_to.getUTCDate()+3, -17);
                                    
                                    aoData.push( { "name": "task_type", "value": $("#task_type").val() } );
                                    aoData.push( { "name": "time_from", "value": time_from.getTime() } );
                                    aoData.push( { "name": "time_to",   "value": time_to.getTime() } );
                                }
                        """ 
        
        fnServerData =  """
                        function ( sSource, aoData, fnCallback, oSettings ) {
                            
                          function prepareData( data, textStatus, jqXHR )
                          {
                            for(var i in data['aaData'])
                            {
                                var row = data['aaData'][i];
                                
                                row[0] = '<a href="/prodtask/task/'+row[0]+'/">'+row[0]+'</a>&nbsp;&nbsp;'; /*+
                                                     '<span style="float: right;" ><a href="/prodtask/task_update/'+row[0]+'/">Update</a>&nbsp;'+
                                                     '<a href="/prodtask/task_clone/'+row[0]+'/">Clone</a></span>'*/

                                row[1] = '<a href="/prodtask/stepex/'+row[1]+'/">'+row[1]+'</a>';
	                            row[2] = '<a href="/prodtask/request/'+row[2]+'/">'+row[2]+'</a>';
                                                     
                                row[7] = '<span class="'+row[7]+'">'+row[7]+'</span>';
                                
							    row[13] = row[13]=='None'? 'None' : row[13].slice(0,19) ;
                                row[14] = row[14]=='None'? 'None' : row[14].slice(0,19) ;
                                row[15] = row[15]=='None'? 'None' : row[15].slice(0,19) ;
							}
                            fnCallback( data, textStatus, jqXHR );
                          }
                        
                          oSettings.jqXHR = $.ajax( {
                            "dataType": 'json',
                            "type": "GET",
                            "url": sSource,
                            "data": aoData,
                            "success": prepareData
                          } )
                          }
                          
                          """
                          

        bServerSide = True
        sAjaxSource = '/prodtask/task_table/'
        



@datatables.datatable(ProductionTaskTable, name='fct')
def task_table(request):
   
    qs = request.fct.get_queryset()
    
    task_type = request.GET.get('task_type', '')
    if task_type == 'production':
        qs = qs.exclude(project='user')
    elif task_type == 'analysis':
        qs = qs.filter(project='user')
        
    task_count_by_type = {  'production': qs.exclude(project='user').count(),
                            'analysis': qs.filter(project='user').count(),
                            }
        
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

    time_from = datetime.utcfromtimestamp(time_from).replace(tzinfo=utc)
    time_to = datetime.utcfromtimestamp(time_to).replace(tzinfo=utc)
    
    qs = qs.filter(submit_time__gt=time_from).filter(submit_time__lt=time_to)
    
    request.fct.update_queryset(qs)
    
    status_stat = ProductionTask.objects.values('status').annotate(count=Count('id'))
    total_task = ProductionTask.objects.count()
    projects = ProductionTask.objects.values('project').annotate(count=Count('id')).order_by('project')
    usernames = ProductionTask.objects.values('username').annotate(count=Count('id')).order_by('username')
    parents = ProductionTask.objects.values('parent_id').annotate(count=Count('id')).order_by('-parent_id')
    requests = ProductionTask.objects.values('request__reqid').annotate(count=Count('id')).order_by('-request__reqid')
    chains = ProductionTask.objects.values('chain_tid').annotate(count=Count('id')).order_by('-chain_tid')
    
    return TemplateResponse(request, 'prodtask/_task_table.html', { 'title': 'Production Tasks Table',
                                                                    'active_app' : 'prodtask',
                                                                    'table': request.fct,
                                                                    'parent_template': 'prodtask/_index.html',
                                                                    'status_stat' : status_stat,
                                                                    'total_task' : total_task,
                                                                    'task_count_by_type': task_count_by_type,
                                                                    'projects'  : projects,
                                                                    'usernames'  : usernames,
                                                                    'requests'  : requests,
                                                                    'parents'   : parents,
                                                                    'chains'    : chains,
                                                                    })

