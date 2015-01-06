from datetime import datetime, timedelta
import json
import copy
import logging
import pytz
from .models import TRequest, ProductionTask, StepExecution

from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.core.urlresolvers import reverse
from django.http import HttpResponse, HttpResponseRedirect, HttpRequest
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.utils import timezone
from django.db.models import Count, Q
from django.views.decorators.csrf import csrf_protect
from django.db import transaction


_logger = logging.getLogger('prodtaskwebui')

def make_default_report(request):
    error_list = []
    not_started = []
    stale_list = []
    warning_list = []
    return render(request, 'prodtask/_main_report.html', {
                'active_app': 'mcprod',
                'parent_template': 'prodtask/_index.html',
                'error_list': error_list,
                'not_started': not_started,
                'stale_list': stale_list,
                'warning_list': warning_list,
                'type':'MC',
                'days':2

             })

def make_report(request, production_request_type, number_of_days):
    if request.method == 'GET':
        try:
            type_provenance = {'MC':'AP','GROUP':'GP','REPROCESSING':'AP'}
            # Optimize by status-time
            if production_request_type not in type_provenance:
                raise ValueError('Unsupprorted type: %s'%production_request_type)
            if int(number_of_days) not in range(5):
                raise ValueError('Wrong day number: %s'%number_of_days)
            requests_ids = list(TRequest.objects.filter(request_type = production_request_type).values('reqid','is_error'))
            requests_list = dict([(x['reqid'],x['is_error']) for x in requests_ids if int(x['reqid']) > 1000 ])
            start_date = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=int(number_of_days)*24)
            #end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
            failed_tasks = ProductionTask.objects.filter(Q(status__in=['failed','broken','aborted']),
                                                         Q(timestamp__gte = start_date),
                                                         Q(provenance = type_provenance[production_request_type])).order_by('request__reqid').values()
            end_date =  datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=2)
            approved_steps = list(StepExecution.objects.filter(Q(status='Approved'),Q(step_appr_time__range=[start_date,end_date])).values('id','request_id'))


            error_list = []
            warning_list = []
            stale_list = []
            not_started = []
            for step in approved_steps:
                if (step['request_id'] in requests_list.keys()) and (step['request_id'] not in not_started):
                    if (requests_list[step['request_id']]):
                        tasks_started = ProductionTask.objects.filter(step_id = step['id']).count()
                        if tasks_started == 0:
                            not_started.append(step['request_id'])
            for failed_task in failed_tasks:
                if failed_task['request_id'] in requests_list.keys():

                    error_list.append({'request':failed_task['request_id'],'task_id':failed_task['id'],
                                       'status':failed_task['status'],'group':failed_task['phys_group'],'time':failed_task['timestamp']})
            running_tasks = []
            running_tasks = list(ProductionTask.objects.filter(Q(status__in=['failed','broken','aborted','done','finished','obsolete']).__invert__(),
                                                           Q(provenance = type_provenance[production_request_type])).order_by('request__reqid').values())
            for running_task in running_tasks:
                if running_task['request_id'] in requests_list:
                    stale_time = datetime.utcnow().replace(tzinfo=pytz.utc) - running_task['timestamp']
                    to_append = {'request':running_task['request_id'],'task_id':running_task['id'],
                                       'status':running_task['status'],'group':running_task['phys_group'],'time':running_task['timestamp']}
                    if stale_time > timedelta(hours=3*24):
                        to_append.update({'reason':'stale more than 3 days'})
                        stale_list.append(to_append)
                    elif stale_time > timedelta(hours=24):
                        warning_list.append(to_append)
                        to_append.update({'reason':'stale more than 1 day'})
                    # else:
                    #     info_list.append(to_append)



            return render(request, 'prodtask/_main_report.html', {
                            'active_app': 'mcprod',
                            'parent_template': 'prodtask/_index.html',
                            'error_list': error_list,
                            'not_started': not_started,
                            'stale_list': stale_list,
                            'warning_list': warning_list,
                            'type':production_request_type,
                            'days':number_of_days

                         })
        except Exception,e:
            _logger.error("Problem with report making  %s"%e)
            return make_default_report(request)

