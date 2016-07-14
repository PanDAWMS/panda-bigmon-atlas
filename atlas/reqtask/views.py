import json
import requests
from django.core import serializers
from atlas.prodtask.models import ProductionTask, StepExecution, StepTemplate
# import logging
# import os
from atlas.prodtask.task_views import get_clouds, get_sites, get_nucleus

from decimal import Decimal
from datetime import datetime

from django.http import HttpResponse
from django.shortcuts import render


# _logger = logging.getLogger('prodtaskwebui')


def request_tasks(request, rid = None):

    # def decimal_default(obj):
    #     if isinstance(obj, Decimal):
    #         return int(obj)
    #     raise TypeError
    #
    # task_array = []
    #
    # if rid:
    #     qs = ProductionTask.objects.filter(request__reqid = rid).values('id')
    #     task_array = [decimal_default( x.get('id')) for x in qs]

    return render(request, 'reqtask/_task_table.html',
                            {'reqid':rid,
                             #'clouds': get_clouds(),
                             #'sites': get_sites(),
                             #'nucleus': get_nucleus()
                             })


def tasks_action(request):
    """

    :type request: object
    """
    user = request.user.username

    is_superuser = request.user.is_superuser
    #print request.body
    if not is_superuser:
        return HttpResponse('Permission denied')

    return HttpResponse('OK')


def get_task_array(request):

    # task_array = json.loads(request.body)
    #
    # if len(task_array)==0:
    #     try:
    #         task_array=request.session['selected_tasks']
    #         del request.session['selected_tasks']
    #     except:
    #         task_array=[]

    try:
        task_array=request.session['selected_tasks']
        del request.session['selected_tasks']
    except:
        task_array=[]

    return task_array


def get_tasks(request):

    reqid = json.loads(request.body)
    if not reqid:
        task_array = get_task_array(request)
        #qs = ProductionTask.objects.filter(id__in=task_array).values()
        qs = ProductionTask.objects.filter(id__in=task_array)
    else:
        #qs = ProductionTask.objects.filter(request__reqid = reqid).values()
        qs = ProductionTask.objects.filter(request__reqid = reqid)

    data_list = []
    for task in list(qs):
        task_dict = task.__dict__
        #step_id = StepExecution.objects.filter(id = task["step_id"]).values("step_template_id").get()['step_template_id']
        step_id = StepExecution.objects.filter(id = task.step_id).values("step_template_id").get()['step_template_id']

        #task.update(dict(step_name=StepTemplate.objects.filter(id = step_id).values("step").get()['step'] ))
        task_dict.update(dict(step_name=StepTemplate.objects.filter(id = step_id).values("step").get()['step'] ))

        task_dict.update(dict(failure_rate=task.failure_rate))
        del task_dict['_state']
        data_list.append(task_dict)

    def decimal_default(obj):
        if isinstance(obj, Decimal):

            return float(obj)
        if isinstance(obj, datetime):

            return obj.isoformat()

        raise TypeError

    data= json.dumps(list(data_list),default = decimal_default)
    #data = json.dumps(list(qs.values()),default = decimal_default)
    #data = json.dumps(list(qs),default = decimal_default)

    return HttpResponse(data)
