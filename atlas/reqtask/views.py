import json
import re

import requests
from django.contrib.auth.decorators import login_required
from django.core import serializers

from atlas.dkb.views import tasks_from_string
from atlas.prodtask.models import ProductionTask, StepExecution, StepTemplate, InputRequestList
# import logging
# import os
from atlas.prodtask.task_views import get_clouds, get_sites, get_nucleus, get_global_shares

from decimal import Decimal
from datetime import datetime, timedelta

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render

from atlas.settings import OIDC_LOGIN_URL


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
                    slices += list(range(chain_start,current_value+1))
                    chain_start =-1
                else:
                    slices += [current_value]
    return slices

@login_required(login_url=OIDC_LOGIN_URL)
def tasks_hashtags(request, hashtag_formula):
    try:
        from atlas.settings.local import FIRST_ADOPTERS
        if (request.user.username in FIRST_ADOPTERS):
            return HttpResponseRedirect(f'/ng/tasks-by-hashtags/{hashtag_formula}')
    except:
        pass
    task_ids = []
    try:
        task_ids = tasks_from_string(hashtag_formula)
        request.session['selected_tasks'] = list(map(int,task_ids))
    except Exception as e:
        request.session['selected_tasks'] = []
    return render(request, 'reqtask/_task_table.html',
                            {'reqid':None,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'shares' : get_global_shares(),
                             'nucleus': get_nucleus(),
                             'search_string':'Hashtags: %s'%hashtag_formula,
                             'title':'Hashtags: %s'%hashtag_formula
                             })


@login_required(login_url=OIDC_LOGIN_URL)
def request_recent_tasks(request, days=3):
    try:
        task_ids = list(ProductionTask.objects.filter(timestamp__gte=datetime.now() - timedelta(days=int(days)),request_id__gt=1000).values_list('id',flat=True))
        request.session['selected_tasks'] = list(map(int,task_ids))
    finally:
        pass
    return render(request, 'reqtask/_task_table.html',
                            {'reqid':None,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'shares': get_global_shares(),
                             'nucleus': get_nucleus(),
                             'search_string':'Tasks for the last %s days'%str(days),
                             'title': 'Tasks for the last %s days'%str(days)
                             })

@login_required(login_url=OIDC_LOGIN_URL)
def request_tasks_slices(request, rid, slices):
    try:
        from atlas.settings.local import FIRST_ADOPTERS
        if (request.user.username in FIRST_ADOPTERS):
            return HttpResponseRedirect(f'/ng/request-tasks/{rid}/{slices}')
    except:
        pass

    if (rid and slices):
            ordered_slices = str_to_slices_range(slices)
            slice_ids = list(InputRequestList.objects.filter(request=rid, slice__in=ordered_slices).values_list('id',flat=True))
            steps = list( StepExecution.objects.filter(slice__in=slice_ids).values_list('id',flat=True))
            task_ids = list(ProductionTask.objects.filter(step__in=steps).values_list('id',flat=True))
            request.session['selected_tasks'] = list(map(int,task_ids))
    return render(request, 'reqtask/_task_table.html',
                            {'reqid':None,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'shares': get_global_shares(),
                             'nucleus': get_nucleus(),
                             'search_string':'Slices for request: %s'%str(rid),
                             'title': 'Slices for request: %s'%str(rid)
                             })


@login_required(login_url=OIDC_LOGIN_URL)
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
    try:
        from atlas.settings.local import FIRST_ADOPTERS
        if (request.user.username in FIRST_ADOPTERS):
            return HttpResponseRedirect(f'/ng/request-tasks/{rid}')
    except:
        pass
    return render(request, 'reqtask/_task_table.html',
                            {'reqid':rid,
                             'clouds': get_clouds(),
                             'sites': get_sites(),
                             'shares': get_global_shares(),
                             'nucleus': get_nucleus()
                             })


@login_required(login_url=OIDC_LOGIN_URL)
def request_tasks_by_url(request):
    request_path = request.META['QUERY_STRING']
    if request_path:
        return HttpResponseRedirect(f'/ng/tasks-by-url?{request_path}')
    return HttpResponseRedirect(f'/ng/tasks-by-url')








@login_required(login_url=OIDC_LOGIN_URL)
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



def  get_tasks_by_url(url):
    url=re.sub('&display_limit=(\d+)','',url)
    url = url.replace('https','http')
    if 'json' not in url:
        if url[-1]=='&':
            url=url+'&'
        else:
            url=url+'&json'
    headers = {'content-type': 'application/json', 'accept': 'application/json'};
    resp = requests.get(url, headers=headers)
    data = resp.json()
    return [x['jeditaskid'] for x in data]


@login_required(login_url=OIDC_LOGIN_URL)
def get_tasks(request):

    FAILED = ['failed','broken','aborted','obsolete']
    NOT_RUNNING = ['done','finished','failed','broken','aborted','obsolete']
    STEPS_ORDER = ['Evgen',
             'Evgen Merge',
             'Simul',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Atlfast',
             'Atlf Merge',
             'TAG',
             'Deriv',
             'Deriv Merge']
    input_data = json.loads(request.body)
    if 'reqid' in input_data:
        reqid = input_data['reqid']
        qs = ProductionTask.objects.filter(request__reqid = reqid)
    elif 'site' in input_data:
        try:
            task_array = get_tasks_by_url(input_data['site'])
        except:
            task_array = []
        qs = ProductionTask.objects.filter(id__in=task_array)
    else:
        task_array = get_task_array(request)
        qs = ProductionTask.objects.filter(id__in=task_array)

    data_list = []
    status_dict = {}
    steps_dict = {}
    not_failed_count = 0
    running_count = 0
    for task in list(qs):
        task_dict = task.__dict__
        step_id = StepExecution.objects.filter(id = task.step_id).values("step_template_id").get()['step_template_id']

        task_dict.update(dict(step_name=StepTemplate.objects.filter(id = step_id).values("step").get()['step'] ))

        task_dict.update(dict(failure_rate=task.failure_rate))
        del task_dict['_state']
        data_list.append(task_dict)
        status_dict[task.status] = status_dict.get(task.status,0) + 1
        steps_dict[task_dict['step_name']] = steps_dict.get(task_dict['step_name'], 0) + 1
        if task.status not in FAILED:
            not_failed_count += 1
        if task.status not in NOT_RUNNING:
            running_count += 1

    def decimal_default(obj):
        if isinstance(obj, Decimal):

            return float(obj)
        if isinstance(obj, datetime):

            return obj.isoformat()

        raise TypeError

    status_stat = [{'name':'total','count':len(data_list),'property':{'active':False,'good':False}}]

    status_stat.append({'name':'active','count':running_count,'property':{'active':False,'good':False}})
    status_stat.append({'name':'good','count':not_failed_count,'property':{'active':False,'good':False}})
    for status in ProductionTask.STATUS_ORDER:
        if status in status_dict:
            status_stat.append({'name':status,'count':status_dict[status],'property':{'active':status not in NOT_RUNNING,'good':status not in FAILED}})
    steps_stat = []
    if len(list(steps_dict.keys()))>1:
        steps_stat = [{'name': 'total', 'count': len(data_list), 'property': {}}]
        for step in STEPS_ORDER:
            if step in steps_dict:
                steps_stat.append({'name':step,'count':steps_dict[step],'property':{}})
    data= json.dumps({'data':list(data_list),'status_stat':status_stat, 'steps_stat':steps_stat},default = decimal_default)
    return HttpResponse(data)
