from datetime import datetime, timedelta
import json
import copy
import logging
import pytz
from .models import TRequest, ProductionTask

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

def make_report(request, production_request_type, number_of_days):
    if request.method == 'GET':
        type_provenance = {'MC':'AP','GROUP':'GP','REPROCESSING':'AP'}
        # Optimize by status-time
        requests_ids = list(TRequest.objects.filter(request_type = production_request_type).values('reqid'))
        requests_list = [x['reqid'] for x in requests_ids if int(x['reqid']) > 1000 ]
        start_date = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=int(number_of_days)*24)
        #end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
        failed_tasks = ProductionTask.objects.filter(Q(status__in=['failed','broken','aborted']),
                                                     Q(timestamp__gte = start_date),
                                                     Q(provenance = type_provenance[production_request_type])).order_by('request__reqid').values()
        error_list = []
        warning_list = []
        info_list = []
        for failed_task in failed_tasks:
            if failed_task['request_id'] in requests_list:
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
                    error_list.append(to_append)
                elif stale_time > timedelta(hours=24):
                    warning_list.append(to_append)
                else:
                    info_list.append(to_append)



        return render(request, 'prodtask/_main_report.html', {
                        'active_app': 'mcprod',
                        'parent_template': 'prodtask/_index.html',
                        'error_list':error_list,
                        'info_list':info_list,
                        'warning_list':warning_list,

                     })

