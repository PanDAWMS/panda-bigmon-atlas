import json
import requests
from django.core import serializers

from atlas.prodtask.hashtag import tasks_from_string
from atlas.prodtask.models import ProductionTask, StepExecution, StepTemplate, InputRequestList
# import logging
# import os
from atlas.prodtask.task_views import get_clouds, get_sites, get_nucleus

from decimal import Decimal
from datetime import datetime

from django.http import HttpResponse
from django.shortcuts import render


# _logger = logging.getLogger('prodtaskwebui')


def str_to_slices_range(range_str):
    token = ''
    slices = []
    chain_start = -1
    for ch in range_str:
        if ch not in ['x','y']:
            token += ch
        else:
            current_value = int(token,16)
            token = ''
            if ch == 'x':
                if chain_start != -1:
                    raise ValueError('Wrong sequence to convert')
                chain_start = current_value
            if ch == 'y':
                if chain_start != -1:
                    slices += range(chain_start,current_value+1)
                    chain_start =-1
                else:
                    slices += [current_value]
    return slices


def tasks_hashtags(request, hashtag_formula):
    task_ids = []
    try:
        task_ids = tasks_from_string(hashtag_formula)
        request.session['selected_tasks'] = map(int,task_ids)
    except Exception,e:
        pass
    return render(request, 'reqtask/_task_table.html',
                            {'reqid':None,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'nucleus': get_nucleus()
                             })

def request_tasks_slices(request, rid, slices):


    if (rid and slices):
            ordered_slices = str_to_slices_range(slices)
            slice_ids = list(InputRequestList.objects.filter(request=rid, slice__in=ordered_slices).values_list('id',flat=True))
            steps = list( StepExecution.objects.filter(slice__in=slice_ids).values_list('id',flat=True))
            task_ids = list(ProductionTask.objects.filter(step__in=steps).values_list('id',flat=True))
            request.session['selected_tasks'] = map(int,task_ids)
    return render(request, 'reqtask/_task_table.html',
                            {'reqid':None,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'nucleus': get_nucleus()
                             })


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
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'nucleus': get_nucleus()
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
