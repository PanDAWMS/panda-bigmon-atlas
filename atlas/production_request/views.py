import json
import logging

import os
import re
import time
from dataclasses import asdict, dataclass, field
from functools import reduce
from pprint import pprint
from typing import Dict, List

import math
import requests
from celery.result import AsyncResult
from django.core.exceptions import ObjectDoesNotExist
from django.core.handlers.wsgi import WSGIRequest
from django.http import HttpRequest
from django.utils import timezone
from rest_framework.request import Request

from atlas.atlaselastic.views import get_tasks_action_logs, get_task_stats, get_campaign_nevents_per_amitag
from atlas.celerybackend.celery import ProdSysTask, app
from atlas.dkb.views import tasks_from_string, es_task_search_all
from atlas.jediinterface.client import JEDIClientTest
from atlas.prodtask.helper import form_json_request_dict
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats, TRequest, \
    ProductionDataset, GroupProductionDeletionExtension, InputRequestList, StepExecution, StepTemplate, SliceError, \
    JediTasks, JediDatasetContents, JediDatasets, SliceSerializer, ParentToChildRequest, SystemParametersHandler, \
    MCWorkflowTransition, MCWorkflowChanges, MCWorkflowRequest, days_ago, TProject, ProductionRequestSerializer, \
    HashTag, HashTagToRequest, get_bulk_hashtags_by_task, MCWorkflowSubCampaign, ETAGRelease, MCPriority

from rest_framework import serializers, generics, status
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes

from atlas.prodtask.spdstodb import fill_steptemplate_from_gsprd
from atlas.prodtask.task_views import get_sites, get_nucleus, get_global_shares, tasks_serialisation
from atlas.prodtask.views import clone_slices, request_clone_slices, form_existed_step_list, get_full_patterns, \
    form_step_in_page, create_steps, fill_request_events, get_pattern_name, set_request_status, get_all_patterns, \
    single_request_action_celery_task
from atlas.production_request.derivation import find_all_inputs_by_tag
from atlas.task_action.task_management import TaskActionExecutor
from django.core.cache import cache


_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')




@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def prepare_slice(request):
    slice = InputRequestList.objects.get(request=36122,slice=0)
    slice_serial = SliceSerializer(slice).data
    steps = StepExecution.objects.filter(slice=slice, request=36122)
    steps_serial = []
    step_parent = None
    for step in steps:
        steps_serial.append({'id':step.id,'ami_tag':step.step_template.ctag,'status':step.status,'step':step.step_template.step,
                             'step_parent':step.step_parent_id, 'request':step.request_id,'task_config':step.get_task_config(),
                             'priority':step.priority,'input_events':step.input_events, 'project_mode': step.get_task_config('project_mode')
                             })
        if step.step_parent.request_id !=  36122:
            step_parent = step.step_parent
    if step_parent:
        step = step_parent
        steps_serial.append({'id':step.id,'ami_tag':step.step_template.ctag,'status':step.status,'step':step.step_template.step,
                             'step_parent':step.step_parent_id,'request':step.request_id,'task_config':step.get_task_config(),
                             'project_mode': step.get_task_config('project_mode'),
                             'priority':step.priority,'input_events':step.input_events})
    # for step in steps_serial:
    #     tasks = ProductionTask.objects.filter(step=step['id'])
    #     current_tasks = []
    #     for task in tasks:
    #         current_tasks.append({'id':task.id,'status':task.status})
    #     step['tasks'] = current_tasks
    slice_serial['steps'] = steps_serial
    print(slice_serial)
    return Response(slice_serial)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_steps(request):
    slice = InputRequestList.objects.get(request=36122,slice=0)
    slice_serial = SliceSerializer(slice).data
    steps = StepExecution.objects.filter(slice=slice, request=36122)
    steps_serial = []
    step_parent = None
    for step in steps:
        steps_serial.append({'id':step.id,'ami_tag':step.step_template.ctag,'status':step.status,'step':step.step_template.step,
                             'step_parent':step.step_parent_id, 'request':step.request_id,'task_config':step.get_task_config(),
                             'priority':step.priority,'input_events':step.input_events, 'project_mode': step.get_task_config('project_mode')
                             })
        if step.step_parent.request_id !=  36122:
            step_parent = step.step_parent
    if step_parent:
        step = step_parent
        steps_serial.append({'id':step.id,'ami_tag':step.step_template.ctag,'status':step.status,'step':step.step_template.step,
                             'step_parent':step.step_parent_id,'request':step.request_id,'task_config':step.get_task_config(),
                             'project_mode': step.get_task_config('project_mode'),
                             'priority':step.priority,'input_events':step.input_events})
    # for step in steps_serial:
    #     tasks = ProductionTask.objects.filter(step=step['id'])
    #     current_tasks = []
    #     for task in tasks:
    #         current_tasks.append({'id':task.id,'status':task.status})
    #     step['tasks'] = current_tasks
    slice_serial['steps'] = steps_serial
    print(slice_serial)
    return Response(slice_serial)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_steps_api(request):
    if request.query_params.get('requests_list'):
        return Response(get_steps_for_requests(list(map(int, request.query_params.get('requests_list').split(',')))))

class ProductionTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProductionTask
        fields = '__all__'

class JediTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = JediTasks
        fields = '__all__'
@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_task(request):
    try:
        task_id = int(request.query_params.get('task_id'))
        jedi_parameters_task = TTask.objects.get(id=task_id)
        task_parameters = jedi_parameters_task.jedi_task_parameters
        job_parameters = task_parameters.pop('jobParameters', {})
        output_datasets = []
        jedi_task = JediTaskSerializer(JediTasks.objects.get(id=task_id)).data
        if ProductionTask.objects.filter(id=task_id).exists():
            production_task = ProductionTask.objects.get(id=task_id)
            task_data = ProductionTaskSerializer(production_task).data
            task_data['subcampaign'] = production_task.request.subcampaign
            task_data['request_id'] = production_task.request_id
            task_data['coreCount'] = jedi_parameters_task.jedi_task_parameters.get('coreCount',1)
            output_datasets = [x.name for x in ProductionDataset.objects.filter(task_id=task_id)]
            task_data['failureRate'] = production_task.failure_rate
            if production_task.request_id > 300:
                task_data['projectMode'] = production_task.step.get_task_config('project_mode')
                task_data['inputEvents'] = production_task.step.input_events
                if DatasetStaging.objects.filter(dataset=production_task.input_dataset).exists():
                    dataset_staging = DatasetStaging.objects.filter(dataset=production_task.input_dataset).last()
                    if ((dataset_staging.status in DatasetStaging.ACTIVE_STATUS) or
                            (dataset_staging.status==DatasetStaging.STATUS.DONE and dataset_staging.start_time>production_task.submit_time)):
                        task_data['staging'] = {'status':dataset_staging.status,
                                    'staged_files':dataset_staging.staged_files,'total_files':dataset_staging.total_files,
                                                'rule':dataset_staging.rse,'source':dataset_staging.source_expression, 'dataset':production_task.input_dataset}
            task_data['hashtags'] = [x.hashtag for x in production_task.hashtags]
        else:
            task_data = {'id': task_id, 'username': jedi_parameters_task.username,
                         'name': jedi_parameters_task.name, 'status': jedi_parameters_task.status }
        return Response({'task':task_data, 'task_parameters': task_parameters, 'job_parameters': job_parameters,
                         'jedi_task': jedi_task, 'output_datasets':output_datasets})
    except ObjectDoesNotExist:
        return Response(f"Task doesn't exist", status=400)
    except Exception as ex:
        return Response(f"Problem with task loading: {ex}", status=400)




def child_derivation_tasks(request_id: int, steps: [int]) -> [ProductionTask]:
    child_tasks = []
    if ParentToChildRequest.objects.filter(parent_request=request_id, relation_type='DP').exists():
        for child_request in ParentToChildRequest.objects.filter(parent_request=request_id, relation_type='DP').values_list('child_request_id', flat=True):
            steps = list(StepExecution.objects.filter(request=child_request, step_parent_id__in=steps).values_list('id', flat=True))
            child_tasks += list(ProductionTask.objects.filter(step__in=steps))
    return child_tasks


def filter_hidden(tasks: [ProductionTask], hashtag: str|None = None) -> [ProductionTask]:
    filtered_tasks = []
    request_to_skip = []
    hidden_slices_per_request = {}
    steps_slices = {}
    hashtag_id = None
    if hashtag is not None:
        hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    for task in tasks:
        if task.request_id  in request_to_skip:
            continue
        if task.request_id not in hidden_slices_per_request:
            if hashtag is not None and not HashTagToRequest.objects.filter(request_id=task.request_id, hashtag_id=hashtag_id).exists():
                request_to_skip.append(task.request_id)
                continue
            hidden_slices_per_request[task.request_id] = list(InputRequestList.objects.filter(is_hide=True, request=task.request_id).values_list('id', flat=True))
            steps = list(StepExecution.objects.filter(request=task.request_id).values_list('id', 'slice_id'))
            steps_slices.update({step:slice_id for step, slice_id in steps})
        if steps_slices[task.step_id] not in hidden_slices_per_request[task.request_id]:
            filtered_tasks.append(task)
    return filtered_tasks


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_task_for_request(request: Request) -> Response:
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    DERIVAITON_USERS = ['atlas-phys-dpd-production']

    try:
        hashtags = None
        if 'hashtagString' in request.data and request.data['hashtagString']:
            if 'source' in request.data and request.data['source'] == 'dkb':
                tasks, _ ,_ = es_task_search_all({'search_string' :request.data['hashtagString']}, 'prod')
                tasks_id = [x['taskid'] for x in tasks]
                tasks = ProductionTask.objects.filter(id__in=tasks_id)
            elif 'source' in request.data and request.data['source'] == 'jira':
                production_requests = TRequest.objects.filter(ref_link__endswith='/'+request.data['hashtagString'])
                tasks= ProductionTask.objects.filter(request__in=production_requests)
                tasks = filter_hidden(tasks)
            elif 'source' in request.data and request.data['source'] == 'taskStatus':
                task_staus = request.data['hashtagString']
                days = 5
                if 'days' in request.data:
                    days = min([10,int(request.data['days'])])
                if task_staus == 'active':
                    ACTIVE_STATUS = [status for status in ProductionTask.ALL_STATUS if status not in ProductionTask.NOT_RUNNING]
                    tasks = ProductionTask.objects.filter(status__in=ACTIVE_STATUS, request__reqid__gte=1000)
                elif task_staus == 'active+':
                    ACTIVE_STATUS = [status for status in ProductionTask.ALL_STATUS if status not in ProductionTask.NOT_RUNNING]
                    tasks = list(ProductionTask.objects.filter(status__in=ACTIVE_STATUS, request__reqid__gte=1000))
                    tasks +=  list(ProductionTask.objects.filter(status__in=ProductionTask.RED_STATUS,timestamp__gt=days_ago(2),  request__reqid__gte=1000))
                    hashtags = get_bulk_hashtags_by_task([x.id for x in tasks])
                elif task_staus == 'recent_derivation':
                    ACTIVE_STATUS = [status for status in ProductionTask.ALL_STATUS if status not in ProductionTask.NOT_RUNNING]
                    tasks = list(ProductionTask.objects.filter(status__in=ACTIVE_STATUS,  username__in=DERIVAITON_USERS))
                    tasks +=  list(ProductionTask.objects.filter(status__in=ProductionTask.NOT_RUNNING,timestamp__gt=days_ago(7), username__in=DERIVAITON_USERS ))
                    hashtags = get_bulk_hashtags_by_task([x.id for x in tasks])
                    tasks = filter_hidden(tasks)
                elif task_staus not in ProductionTask.NOT_RUNNING:
                    tasks = ProductionTask.objects.filter(status=task_staus, request__reqid__gte=1000)
                else:
                    tasks = ProductionTask.objects.filter(status=task_staus, timestamp__gt=days_ago(days), request__reqid__gte=1000)
            else:
                tasks_id = tasks_from_string(request.data['hashtagString'])
                # split on 10000 chunks tasks = list(ProductionTask.objects.filter(id__in=tasks_id))
                tasks = sum([list(ProductionTask.objects.filter(id__in=chunk)) for chunk in chunks(tasks_id, 1000)], [])



        else:
            request_id = int(request.data['requestID'])
            if request_id < 1000:
                raise TRequest.DoesNotExist
            if request.data['slices']:
                slices = request.data['slices']
                slice_ids = list(InputRequestList.objects.filter(request=request_id, slice__in=slices).values_list('id',flat=True))
                steps = list( StepExecution.objects.filter(slice__in=slice_ids).values_list('id',flat=True))
                tasks = list(ProductionTask.objects.filter(step__in=steps)) + child_derivation_tasks(request_id, steps)
            else:
                tasks = list(ProductionTask.objects.filter(request_id=request_id))
            hashtags = get_bulk_hashtags_by_task([x.id for x in tasks])
        tasks_serial = tasks_serialisation(tasks, hashtags)
        return Response(tasks_serial)
    except Exception as ex:
        return Response(f"Problem with task loading: {ex}", status=400)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_tasks_by_bigpanda_url(request: Request) -> Response:
    try:
        bigpanda_url_parameters = request.data['tasksURL']
        bigpanda_url = bigpanda_url_parameters
        if not bigpanda_url_parameters.startswith('http'):
            bigpanda_url = f'https://bigpanda.cern.ch/tasks/?{bigpanda_url_parameters}'
        url = re.sub('&display_limit=(\d+)', '', bigpanda_url)
        url = url.replace('https', 'http')
        if 'json' not in url:
            if url[-1] == '&':
                url = url + '&'
            else:
                url = url + '&json'
        resp = requests.get(url, headers= {'content-type': 'application/json', 'accept': 'application/json'})
        data = resp.json()
        tasks =  [ProductionTask.objects.get(id=x['jeditaskid']) for x in data]
        tasks_serial = tasks_serialisation(tasks)
        return Response(tasks_serial)
    except Exception as ex:
        return Response(f"Problem with task loading: {ex}", status=400)
@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_task_action_logs(request):
    if request.query_params.get('task_id'):
        return Response(get_tasks_action_logs(int(request.query_params.get('task_id'))))


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_task_hs06(request):
    if request.query_params.get('task_id'):
        task_stats = get_task_stats(int(request.query_params.get('task_id')))
        task=ProductionTask.objects.get(id=request.query_params.get('task_id'))
        finished_hpes06 = 0
        failed_hpes06 = 0
        running_files = -1

        input_dataset = task.inputdataset
        input_dataset = input_dataset[input_dataset.find(':')+1:]
        for dataset in task_stats:
            if dataset.task_hs06sec_finished + dataset.task_hs06sec_failed>0:
                finished_hpes06 = dataset.task_hs06sec_finished
                failed_hpes06 = dataset.task_hs06sec_failed
                if task.request_id > 300:
                    running_files = 0
                break
        total_output = 0
        input_events = 0
        input_bytes = 0
        output_datasets = set()
        for dataset in task_stats:
            if (dataset.type == 'output') and (dataset.dataset_id not in output_datasets):
                if dataset.bytes > 0:
                    output_datasets.add(dataset.dataset_id)
                total_output += dataset.bytes or 0
        if  task.request_id > 300:
            dataset_id = None
            for dataset in task_stats:
                dataset_name = dataset.dataset
                dataset_name = dataset_name[dataset_name.find(':') + 1:]
                if (input_dataset.endswith('.py') and dataset_name=='pseudo_dataset') or (input_dataset==dataset_name):
                    dataset_id = dataset.dataset_id
                    input_events = dataset.events or 0
                    input_bytes = dataset.bytes or 0
                    if input_events>0:
                        break
            if dataset_id is None:
                for dataset in JediDatasets.objects.filter(id=task.id):
                    dataset_name = dataset.datasetname
                    dataset_name = dataset_name[dataset_name.find(':') + 1:]
                    if (input_dataset.endswith('.py') and dataset_name == 'pseudo_dataset') or (
                            input_dataset == dataset_name):
                        dataset_id = dataset.datasetid
                        break
            if  (task.status not in ProductionTask.NOT_RUNNING) and dataset_id:
                running_files = JediDatasetContents.objects.filter(jeditaskid=task.id, datasetid=dataset_id,
                                                               status='running').count()

        parent_percent = 1.0
        parent_task = task
        while (parent_task.status not in ProductionTask.NOT_RUNNING) and (parent_task.parent_id != parent_task.id):
            parent_task = ProductionTask.objects.get(id=parent_task.parent_id)
            if (parent_task.status not in ProductionTask.NOT_RUNNING):
                parent_percent = parent_percent * float(parent_task.total_files_finished) / float(parent_task.total_files_tobeused)
        return Response({'finished': finished_hpes06,'failed': failed_hpes06, 'running': running_files, 'parentPercent':parent_percent,
                         'total_output_size':total_output, 'input_events':input_events, 'input_bytes':input_bytes})


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_error_logs(request):
    try:
        if request.query_params.get('task_id'):
            error_log = JediTasks.objects.get(id=request.query_params.get('task_id')).errordialog
            if error_log:
                link_re = re.match('^.*"([^"]+)"', error_log)
                if link_re:
                    log_url = link_re.group(1)
                    log_response = requests.get(log_url,verify=False)
                    if log_response.status_code == requests.codes.ok:
                        log_content = log_response.content
                        log_lines = [x for x in log_content.decode().splitlines() if x]
                        log_lines.reverse()
                        return Response({'log': '<br/>'.join(log_lines)})
    except Exception as ex:
        _logger.error(f'Problem reading logs: {ex}')
    return Response({'log':None})

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_task_extensions(request):
    same_tasks_with_status = []
    try:
        if request.query_params.get('task_id'):
            task = ProductionTask.objects.get(id=int(request.query_params.get('task_id')))
            output_datasets = ProductionDataset.objects.filter(task_id=int(request.query_params.get('task_id')))
            if task.is_extension:
                dataset_pat = output_datasets[0].name.split("tid")[0]
                datasets_extension = ProductionDataset.objects.filter(name__icontains=dataset_pat)
                same_tasks = [int(x.name.split("tid")[1].split("_")[0]) for x in datasets_extension]
                for same_task in same_tasks:
                    same_tasks_with_status.append(
                        {'id': same_task, 'status': ProductionTask.objects.get(id=same_task).status})
    except Exception as ex:
        _logger.error(f'Problem this extensions searching: {ex}')
    return Response(same_tasks_with_status)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def task_action(request):

    return Response(None)

def get_steps_for_requests(requests_ids):
    def split_list(a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
    production_requests = list(TRequest.objects.filter(reqid__in=requests_ids).values())

    time_start = timezone.now()
    steps = []
    slices = []
    for production_request_id in requests_ids:
        current_steps = list(StepExecution.objects.filter(request=production_request_id).order_by('id').values('id','status','slice_id','step_parent_id','request_id','task_config','priority','input_events','step_template_id'))
        slices += list(InputRequestList.objects.filter(request=production_request_id).order_by('id').values())
        tasks = list(ProductionTask.objects.filter(request=production_request_id).values('id','step_id','status','total_events','total_files_tobeused','total_files_finished'))
        # steps_action = list(StepAction.objects.filter(request=production_request_id).values())
        # slice_errors = list(SliceError.objects.filter(request=production_request_id))
        tasks_dict = {}
        for current_task in tasks:
            tasks_dict[current_task['step_id']] = tasks_dict.get(current_task['step_id'],[]) + [current_task]
        for step in current_steps:
            step['tasks'] = tasks_dict.get(step['id'],[])
        steps += current_steps
    step_template_set = set()
    slices = [x for x in slices if not x['is_hide']]
    slice_ids = [x['id'] for x in slices]
    steps = [x for x in steps if x['slice_id'] in slice_ids]
    for step in steps:
        step_template_set.add(step['step_template_id'])
    step_templates = []
    for step_template_ids in split_list(list(step_template_set), len(step_template_set)//100 + 1):
        step_templates += list(StepTemplate.objects.filter(id__in=step_template_ids).values())
    step_template_dict = {}
    for step_template in step_templates:
        step_template_dict[step_template['id']] = step_template
    for step in steps:
        step['step_name'] = step_template_dict[step['step_template_id']]['step']
        step['ami_tag'] = step_template_dict[step['step_template_id']]['ctag']
        step['output_formats'] = step_template_dict[step['step_template_id']]['output_formats']
        step['task_config'] = json.loads(step['task_config'])
    result = {'production_requests':production_requests,'slices':slices,'steps':steps}

    return result


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def save_slice(request):
    print('test')
    slice = InputRequestList.objects.get(id=request.data['id'])
    for key in ['input_data','input_events','comment']:
        if key in request.data:
            if slice.__getattribute__(key) != request.data[key]:
                print(key, slice.__getattribute__(key), request.data[key])
                slice.__setattr__(key, request.data[key])
                slice.save()
    return Response(request.data)


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def collect_steps_by_jira(request):
    production_requests = TRequest.objects.filter(ref_link='https://its.cern.ch/jira/browse/'+request.query_params.get('jira')).order_by('reqid').values_list('reqid', flat=True)
    return Response(get_steps_for_requests(list( production_requests)))


def longest_common_substring(s1, s2):
    m = [[0] * (1 + len(s2)) for i in range (1 + len(s1))]
    longest, x_longest = 0, 0
    for x in range(1, 1 + len(s1)):
        for y in range(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return s1[x_longest - longest: x_longest]

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def info_by_jira(request):
    jira = 'https://its.cern.ch/jira/browse/'+request.query_params.get('jira')
    production_requests = list(TRequest.objects.filter(ref_link='https://its.cern.ch/jira/browse/'+request.query_params.get('jira')).order_by('reqid'))
    req_ids = [x.reqid for x in production_requests]
    description = reduce(longest_common_substring, [x.description for x in production_requests])
    if not description or len(description)<5:
        description = production_requests[0].description
    manager = production_requests[0].manager
    group = production_requests[0].phys_group
    return Response({'description': description, 'requests_number': len(production_requests), 'jira_reference': jira,
                     'manager': manager, 'phys_group': group, 'reqIDs': req_ids})

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_request_info(request):
    production_request = TRequest.objects.get(reqid=request.query_params.get('prodcution_request_id'))
    return Response(ProductionRequestSerializer(production_request).data)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_reassign_entities(request):
    return Response( {
    'sites': get_sites(),
    'nucleus': get_nucleus(),
    'shares': get_global_shares()})


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def derivation_input(request):
    try:
        if request.query_params.get('ami_tag'):
            ami_tag = request.query_params.get('ami_tag')
            cached_result = cache.get(f'derivation_input_{ami_tag}')
            if cached_result is None:
                result, requests_ids, outputs, projects = find_all_inputs_by_tag(ami_tag)
                production_requests = list(TRequest.objects.filter(reqid__in=requests_ids).values())
                return_value = {'containers': map(asdict, result), 'requests': production_requests, "format_outputs": outputs,
                             "projects": projects}
                cache.set(f'derivation_input_{ami_tag}',return_value,24*3600)
            else:
                return_value = cached_result
            return Response(return_value)
    except Exception as ex:
        return Response(f"Problem with derivation loading: {ex}", status=400)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def pattern_slices_derivation_request(request):
    try:
        requestID = int(request.data['requestID'])
        slicesIDs = request.data['slices']
        based_input = ''
        pattern_slices = []
        for sliceID in slicesIDs:
            slice = InputRequestList.objects.get(request_id=requestID, slice=sliceID)
            if not based_input:
                if ProductionTask.objects.filter(step__slice_id=slice, request_id=requestID).exists():
                    if not ProductionTask.objects.filter(step__slice_id=slice, request_id=requestID, status__in=ProductionTask.BAD_STATUS).exists():
                        based_input = slice.dataset
                        pattern_slices.append(sliceID)
                else:
                    based_input = slice.dataset
                    pattern_slices.append(sliceID)
            else:
                if slice.dataset == based_input:
                    pattern_slices.append(sliceID)
        pattern_steps = find_derivation_pattern_steps(requestID, pattern_slices)

        return Response({'pattern_slices':','.join([x['slice'] for x in pattern_steps])})

    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def find_derivation_pattern_steps(requestID: int, sliceIDs: [int]) -> [dict]:
    pattern_steps = []
    output_formats = set()
    for sliceID in sliceIDs:
        step = StepExecution.objects.get(slice=InputRequestList.objects.get(slice=sliceID, request=requestID),
                                         request_id=requestID)
        step_output_formats = step.step_template.output_formats.split('.')
        if not [x for x in step_output_formats if x in output_formats]:
            output_formats = output_formats.union(set(step_output_formats))
            pattern_steps.append({'slice': sliceID, 'ami_tag': step.step_template.ctag,
                                  'output_formats': step_output_formats})
    return pattern_steps

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def form_pattern_for_derivation_request_extension(request):
    try:
        return Response(find_derivation_pattern_steps(int(request.data['requestID']),  request.data['slices']))
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def extend_derivation_request(request):
    try:
        requestID = int(request.data['requestID'])
        sliceIDs = request.data['slices']
        datasets = request.data['containerList']
        pattern_steps = find_derivation_pattern_steps(requestID, sliceIDs)
        pattern_slices = [x['slice'] for x in pattern_steps]
        for dataset in datasets:
            new_slices = clone_slices(requestID, requestID, pattern_slices, -1, False)
            for sliceNumber in new_slices:
                slice = InputRequestList.objects.get(request_id=requestID, slice=sliceNumber)
                slice.dataset = dataset
                slice.save()
        return Response(str(requestID))

    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def mc_subcampaign_stats(request):
    try:
        total_stats = []
        # mc_subcampaigns = [SystemParametersHandler.MCSubCampaignStats('MC23:MC23a',':25ns'),
        #                    SystemParametersHandler.MCSubCampaignStats('MC23:MC23c',':25ns'),
        #                    SystemParametersHandler.MCSubCampaignStats('MC23:MC23d',':25ns'),]
        mc_subcampaigns = SystemParametersHandler.get_mc_sub_campaigns_stats()
        trends = cache.get('mc_stats_trend')
        trends.reverse()
        current_time = timezone.now()
        for mc_subcampaign in mc_subcampaigns:
            stats = get_campaign_nevents_per_amitag(mc_subcampaign.campaign,{'pile':mc_subcampaign.pile_suffix})
            current_trend = [(trend['time'],list(filter(lambda x: x['mc_subcampaign']==mc_subcampaign.campaign, trend['stats']))) for trend in trends]
            trend = [{'seconds':math.ceil((current_time- x[0]).total_seconds()),'stats':x[1][0]['stats']} for x in current_trend if len(x)>0 and len(x[1])>0]
            total_stats.append({'mc_subcampaign':mc_subcampaign.campaign, 'stats':stats, 'trend':trend})
        return Response(total_stats)
    except Exception as ex:
        _logger.error(f'Problem with mc sub campaign loading: {ex}')
        return Response(f"Problem with mc sub campaign loading: {ex}", status=400)



def fill_default_campaigns():
    mc_subcampaigns = [SystemParametersHandler.MCSubCampaignStats('MC23:MC23e', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC23:MC23d', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC23:MC23c', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC23:MC23a', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC20:MC20e', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC20:MC20d', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC20:MC20a', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC16:MC16e', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC16:MC16d', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC16:MC16c', ':25ns'),
                       SystemParametersHandler.MCSubCampaignStats('MC16:MC16a', ':25ns'),
                       ]
    SystemParametersHandler.set_mc_sub_campaigns_stats(mc_subcampaigns)
    fill_mc_stats_trend()

def fill_mc_stats_trend():
    total_stats = []
    mc_subcampaigns = SystemParametersHandler.get_mc_sub_campaigns_stats()
    for mc_subcampaign in mc_subcampaigns:
        stats = get_campaign_nevents_per_amitag(mc_subcampaign.campaign, {'pile': mc_subcampaign.pile_suffix})
        total_events = {}
        for step in ['evgen','simul', 'pile']:
            total_events[step] = sum([x['nevents'] for x in stats[step]])
        total_stats.append({'mc_subcampaign': mc_subcampaign.campaign, 'stats': total_events})
    existing_stats = cache.get('mc_stats_trend')
    if existing_stats is None:
        existing_stats = []
    if len(existing_stats) < 200:
        existing_stats.append({'time': timezone.now(), 'stats': total_stats})
    else:
        existing_stats.pop(0)
        existing_stats.append({'time': timezone.now(), 'stats': total_stats})
    cache.set('mc_stats_trend', existing_stats, None)

def fill_default_workflows():
    workflows : Dict[str, MCWorkflowSubCampaign] = {}
    mc16a_changes = [MCWorkflowChanges(value='mc16', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                     MCWorkflowChanges(value='(sim+reco)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc16a_evgen_simul_transition = MCWorkflowTransition(new_request='MC16a simul', parent_step='Evgen',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc16a_changes,
                                                        pattern={MCWorkflowTransition.SimulationType.FASTSIM_BYRELEASE: [(21,72),(23,116)],
                                                                 MCWorkflowTransition.SimulationType.FULLSIM_BYRELEASE: [(21,77),(23,111)]} )
    mc16a_mc16d_changes = [MCWorkflowChanges(value='MC16c', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc16a_mc16d_transition = MCWorkflowTransition(new_request='MC16c evgen', parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc16a_mc16d_changes, event_ratio=1.25)
    mc16a_mc16e_changes = [MCWorkflowChanges(value='MC16e', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc16a_mc16e_transition = MCWorkflowTransition(new_request='MC16e evgen', parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc16a_mc16e_changes, event_ratio=1.6)
    workflows['MC16a simul'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16a', project_base='mc16', transitions=[])
    mc16c_changes = [MCWorkflowChanges(value='mc16', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                     MCWorkflowChanges(value='(sim)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc16d_changes = [MCWorkflowChanges(value='(reco)', type=MCWorkflowChanges.ChangeType.DESCRIPTION),
                     MCWorkflowChanges(value='MC16d', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc16c_evgen_simul_transition = MCWorkflowTransition(new_request='MC16c simul', parent_step='Evgen',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc16c_changes,
                                                        pattern={MCWorkflowTransition.SimulationType.FASTSIM_BYRELEASE: [(21,17),(23,117)],
                                                                 MCWorkflowTransition.SimulationType.FULLSIM_BYRELEASE:[(21,52),(23, 112)]} )
    mc16d_simul_reco_transition = MCWorkflowTransition(new_request='MC16d reco', parent_step='Simul',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc16d_changes,
                                                        pattern={MCWorkflowTransition.SimulationType.FASTSIM: 73,
                                                                 MCWorkflowTransition.SimulationType.FULLSIM:76} )
    workflows['MC16d reco'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16d', project_base='mc16', transitions=[])
    workflows['MC16c simul'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16c', project_base='mc16', transitions=[mc16d_simul_reco_transition])
    workflows['MC16c evgen'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16c', project_base='mc15', transitions=[mc16c_evgen_simul_transition])
    mc16e_changes = [MCWorkflowChanges(value='mc16', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                     MCWorkflowChanges(value='(sim+reco)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc16e_evgen_simul_transition = MCWorkflowTransition(new_request='MC16e simul', parent_step='Evgen',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc16e_changes,
                                                        pattern={MCWorkflowTransition.SimulationType.FASTSIM_BYRELEASE: [(21,75),(23,118)],
                                                                 MCWorkflowTransition.SimulationType.FULLSIM_BYRELEASE: [(21,80),(23,113)]})
    workflows['MC16e simul'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16e', project_base='mc16', transitions=[])
    workflows['MC16e evgen'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16e', project_base='mc15', transitions=[mc16e_evgen_simul_transition])

    mc16a_mc20a_changes = [MCWorkflowChanges(value='(mc20)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc16a_mc20a_transition = MCWorkflowTransition(new_request='MC20a evgen', parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc16a_mc20a_changes, event_ratio=1)
    workflows['MC16a evgen'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16a', project_base='mc15',
                                                     transitions=[mc16a_evgen_simul_transition, mc16a_mc16d_transition, mc16a_mc16e_transition, mc16a_mc20a_transition])
    mc20a_mc20d_changes = [MCWorkflowChanges(value='MC16c', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc20a_mc20d_transition = MCWorkflowTransition(new_request='MC20c evgen', parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc20a_mc20d_changes, event_ratio=1.25)
    mc20a_mc20e_changes = [MCWorkflowChanges(value='MC16e', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc20a_mc20e_transition = MCWorkflowTransition(new_request='MC20e evgen', parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc20a_mc20e_changes, event_ratio=1.6)
    mc20a_simul_reco_changes = [MCWorkflowChanges(value='MC20a', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN),
                           MCWorkflowChanges(value='mc20', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                                MCWorkflowChanges(value='MC20', type=MCWorkflowChanges.ChangeType.CAMPAIGN),
                                MCWorkflowChanges(value='(reco)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    workflows['MC20a reco'] = MCWorkflowSubCampaign(campaign='MC20', subcampaign='MC20a', project_base='mc20',
                                                     transitions=[])
    mc20a_simul_reco_transition = MCWorkflowTransition(new_request='MC20a reco', parent_step='Simul',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc20a_simul_reco_changes,
                                                        pattern={} )
    workflows['MC20a simul'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16a', project_base='mc16',
                                                     transitions=[mc20a_simul_reco_transition])
    mc20a_changes = [MCWorkflowChanges(value='mc16', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                     MCWorkflowChanges(value='(sim)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc20a_evgen_simul_transition = MCWorkflowTransition(new_request='MC20a simul', parent_step='Evgen',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc20a_changes,
                                                        pattern={} )
    workflows['MC20a evgen'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16a', project_base='mc15',
                                                     transitions=[mc20a_evgen_simul_transition, mc20a_mc20d_transition, mc20a_mc20e_transition])
    workflows['MC20d reco'] = MCWorkflowSubCampaign(campaign='MC20', subcampaign='MC20d', project_base='mc20', transitions=[])
    mc20d_changes = [MCWorkflowChanges(value='(reco)', type=MCWorkflowChanges.ChangeType.DESCRIPTION),
                     MCWorkflowChanges(value='mc20', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                     MCWorkflowChanges(value='MC20', type=MCWorkflowChanges.ChangeType.CAMPAIGN),
                     MCWorkflowChanges(value='MC20d', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc20d_simul_reco_transition = MCWorkflowTransition(new_request='MC20d reco', parent_step='Simul',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc20d_changes,
                                                        pattern={} )
    workflows['MC20c simul'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16c', project_base='mc16', transitions=[mc20d_simul_reco_transition])

    mc20c_changes = [MCWorkflowChanges(value='mc16', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                     MCWorkflowChanges(value='(sim)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc20c_evgen_simul_transition = MCWorkflowTransition(new_request='MC20c simul', parent_step='Evgen',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc20c_changes,
                                                        pattern={} )
    workflows['MC20c evgen'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16c', project_base='mc15', transitions=[mc20c_evgen_simul_transition])
    mc20e_simul_reco_changes = [MCWorkflowChanges(value='MC20e', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN),
                                MCWorkflowChanges(value='MC20', type=MCWorkflowChanges.ChangeType.CAMPAIGN),
                           MCWorkflowChanges(value='mc20', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                                MCWorkflowChanges(value='(reco)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    workflows['MC20e reco'] = MCWorkflowSubCampaign(campaign='MC20', subcampaign='MC20e', project_base='mc20',
                                                     transitions=[])
    mc20e_simul_reco_transition = MCWorkflowTransition(new_request='MC20e reco', parent_step='Simul',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc20e_simul_reco_changes,
                                                        pattern={} )
    workflows['MC20e simul'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16e', project_base='mc16',
                                                     transitions=[mc20e_simul_reco_transition])
    mc20e_changes = [MCWorkflowChanges(value='mc16', type=MCWorkflowChanges.ChangeType.PROJECT_BASE),
                     MCWorkflowChanges(value='(sim)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc20e_evgen_simul_transition = MCWorkflowTransition(new_request='MC20e simul', parent_step='Evgen',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc20e_changes,
                                                        pattern={} )
    workflows['MC20e evgen'] = MCWorkflowSubCampaign(campaign='MC16', subcampaign='MC16e', project_base='mc15', transitions=[mc20e_evgen_simul_transition])
    mc23a_mc23d_changes = [MCWorkflowChanges(value='MC23c', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc23a_mc23d_transition = MCWorkflowTransition(new_request='MC23c evgen', parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc23a_mc23d_changes, event_ratio=1.5)
    mc23a_mc23e_changes = [MCWorkflowChanges(value='MC23e', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN)]
    mc23a_mc23e_transition = MCWorkflowTransition(new_request='MC23e evgen', parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc23a_mc23e_changes, event_ratio=2.5)
    workflows['MC23a evgen'] = MCWorkflowSubCampaign(campaign='MC23', subcampaign='MC23a', project_base='mc23',
                                                     transitions=[mc23a_mc23d_transition, mc23a_mc23e_transition])
    workflows['MC23e evgen'] = MCWorkflowSubCampaign(campaign='MC23', subcampaign='MC23e', project_base='mc23',
                                                     transitions=[])
    workflows['MC23d reco'] = MCWorkflowSubCampaign(campaign='MC23', subcampaign='MC23d', project_base='mc23',
                                                     transitions=[])
    mc23d_changes = [MCWorkflowChanges(value='MC23d', type=MCWorkflowChanges.ChangeType.SUBCAMPAIGN),
                     MCWorkflowChanges(value='(reco)', type=MCWorkflowChanges.ChangeType.DESCRIPTION)]
    mc23d_simul_reco_transition = MCWorkflowTransition(new_request='MC23d reco', parent_step='Simul',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc23d_changes,
                                                        pattern={} )
    workflows['MC23c evgen'] = MCWorkflowSubCampaign(campaign='MC23', subcampaign='MC23c', project_base='mc23', transitions=[mc23d_simul_reco_transition])

    workflows_requests = MCWorkflowRequest(workflows=workflows)
    print(workflows_requests.model_dump_json())
    SystemParametersHandler.set_mc_workflow_request(workflows_requests)


def build_workflows_tree():
    workflows = SystemParametersHandler.get_mc_workflow_request()
    workflows_tree = {}
    for workflow in workflows.workflows:
        workflows_tree[workflow] = {}
        for transition in workflows.workflows[workflow]:
            workflows_tree[workflow][transition.new_request] = transition
    return workflows_tree

def build_workflow_vertical(base_subcampaign: str):
    workflows = SystemParametersHandler.get_mc_workflow_request().workflows
    base_step = f'{base_subcampaign} evgen'
    base_request = workflows[base_step]
    sub_campaigns = {base_request.campaign: [base_step]}
    request_to_process = [base_step]
    while request_to_process:
        current_request = request_to_process.pop()
        base_request = workflows[current_request]
        for transition in base_request.transitions:
            if transition.transition_type == MCWorkflowTransition.TransitionType.VERTICAL:
                current_campaign = workflows[transition.new_request].campaign
                sub_campaigns[current_campaign] = sub_campaigns.get(current_campaign, []) + [transition.new_request]
                request_to_process.append(transition.new_request)
    result = []
    pprint(sub_campaigns)


class WorkflowActions:

    @dataclass
    class AsyncUpdates:
        count: int = 1
        total: int = 0
        requests_ids: List[int] = field(default_factory=list)


    def __init__(self, base_request_id: int, workflows_tree=None, async_task:ProdSysTask|None=None):
        if workflows_tree is None:
            self.workflows_tree = SystemParametersHandler.get_mc_workflow_request().workflows
        self.base_request_id = base_request_id
        self.base_workflow = None
        self.base_request = TRequest.objects.get(reqid=base_request_id)
        self.print_result = []
        self.found_patterns = {}
        self.async_task = async_task
        self.async_update_values = self.AsyncUpdates()
        for name, workflow in self.workflows_tree.items():
            subcmapaign_to_look = self.base_request.subcampaign
            if 'mc20' in self.base_request.description.lower():
                subcmapaign_to_look = subcmapaign_to_look.replace('MC16', 'MC20')
            if (workflow.campaign == self.base_request.campaign and workflow.subcampaign == self.base_request.subcampaign and
                name == f'{subcmapaign_to_look} {workflow.BASE_REQUEST}'):
                self.base_workflow = workflow
                self.base_workflow_name = name
                break


    @staticmethod
    def clone_request(request_id: int, new_description: str, new_project: str) -> int:
        production_request = TRequest.objects.get(reqid=request_id)
        slices = [slice.slice for slice in InputRequestList.objects.filter(request=request_id).order_by("slice") if not slice.is_hide]
        new_request_id = request_clone_slices(request_id, production_request.manager, new_description, production_request.ref_link, slices,
                             new_project)
        return new_request_id


    @staticmethod
    def change_request_campaign(request_id: int, new_campaign: str, new_subcampaign: str):
        production_request = TRequest.objects.get(reqid=request_id)
        production_request.campaign = new_campaign
        production_request.subcampaign = new_subcampaign
        production_request.save()

    @staticmethod
    def change_input_events_number(request_id: int, ratio: float):
        for slice in InputRequestList.objects.filter(request=request_id):
            if not slice.is_hide:
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(slice=slice, request=request_id))
                first_step = ordered_existed_steps[0]
                input_events = first_step.input_events
                new_input_events = math.ceil(input_events*ratio)
                first_step.input_events = new_input_events
                first_step.save()
                slice.input_events = new_input_events
                slice.save()
        fill_request_events(request_id, request_id)



    @staticmethod
    def apply_pattern(request_id: int, pattern_name: str):
        pattern = filter(lambda x: x[0] == pattern_name, get_full_patterns()).__next__()[1]
        slices_steps = {}
        first_pattern_step = None
        for slice in InputRequestList.objects.filter(request=request_id):
            if not slice.is_hide:
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(slice=slice, request=request_id))
                step_as_in_page = form_step_in_page(ordered_existed_steps, StepExecution.STEPS, existed_foreign_step)
                slice_steps = []
                for index, step in enumerate(step_as_in_page):
                    value = ''
                    is_skipped = False
                    changes = {}
                    for element in pattern[index][1]:
                        changes[element[0]] = str(element[1])
                    if not pattern[index][0]:
                        is_skipped = True
                        if step:
                            value = step.step_template.ctag
                    else:
                        value = pattern[index][0]
                        if not first_pattern_step:
                            first_pattern_step = index
                    slice_steps.append({'value': value, 'is_skipped': is_skipped, 'formats': '', 'changes': changes})
                if existed_foreign_step:
                    slice_steps.append({'foreign_id': str(existed_foreign_step.id)})
                else:
                    slice_steps.append({'foreign_id': '0'})
                slices_steps[slice.slice] = slice_steps
        for steps_status in list(slices_steps.values()):
            for index, steps in enumerate(steps_status[:-2]):
                if (StepExecution.STEPS[index] == 'Reco') or (StepExecution.STEPS[index] == 'Atlfast'):
                    if not steps['formats']:
                        steps['formats'] = 'AOD'
        error_slices, _ = create_steps(None, slices_steps, request_id, STEPS=StepExecution.STEPS, approve_level=-1)
        if error_slices:
            raise Exception(f'Error in slices: {request_id} {error_slices} for pattern {pattern_name}')
        return first_pattern_step

    @staticmethod
    def connect_requests(request_id: int, new_request_id: int, step_number: int):
        parent_slices = [slice for slice in InputRequestList.objects.filter(request=request_id).order_by("slice") if not slice.is_hide]
        for index, slice in enumerate(InputRequestList.objects.filter(request=new_request_id).order_by("slice")):
            if not slice.is_hide:
                parent_ordered_existed_steps, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(slice=parent_slices[index], request=request_id))
                parent_step_as_in_page = form_step_in_page(parent_ordered_existed_steps, StepExecution.STEPS, existed_foreign_step)
                parent_step = [x for index, x in enumerate(parent_step_as_in_page) if x and index < step_number][-1]
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(slice=slice, request=new_request_id))
                step_as_in_page = form_step_in_page(ordered_existed_steps, StepExecution.STEPS, existed_foreign_step)
                step_to_delete = []
                for index, step in enumerate(step_as_in_page):
                    if step and index < step_number:
                        step_to_delete.append(step)
                    if index == step_number:
                        step.step_parent = parent_step
                        step.set_task_config({'nEventsPerInputFile':''})
                        step.save()
                        break
                for step in step_to_delete:
                    step.delete()

    def approve_request(self, request_id: int):
        slices = [x for x in InputRequestList.objects.filter(request=request_id).order_by("slice") if not x.is_hide]
        for slice in slices:
            for step in StepExecution.objects.filter(slice=slice, request=request_id):
                if step.status == StepExecution.STATUS.NOT_CHECKED:
                    step.status = StepExecution.STATUS.APPROVED
                    step.save()
        set_request_status('cron', request_id, 'approved', 'Automatic approve',
                           'Request was approved')

    def apply_transition(self, request_id: int, old_request_base: MCWorkflowSubCampaign, transition: MCWorkflowTransition, description:str,
                         pattern_id: int| None = None, event_ratio: float | None = None):
        original_request = TRequest.objects.get(reqid=request_id)
        new_subcampaign = original_request.subcampaign
        new_campaign = original_request.campaign
        new_project = str(original_request.project)
        for change in transition.changes:
            if change.type == MCWorkflowChanges.ChangeType.SUBCAMPAIGN:
                new_subcampaign = change.value
            if change.type == MCWorkflowChanges.ChangeType.CAMPAIGN:
                new_campaign = change.value
            if change.type == MCWorkflowChanges.ChangeType.PROJECT_BASE:
                new_project = new_project.replace(old_request_base.project_base, change.value)
            if  change.type == MCWorkflowChanges.ChangeType.DESCRIPTION:
                description = f'{change.value} {description}'
        new_request = self.clone_request(request_id, description, new_project)
        if self.async_task is not None:
            self.lock_request(new_request)
        self.change_request_campaign(new_request, new_campaign, new_subcampaign)
        self.async_update()
        if event_ratio is not None:
            self.change_input_events_number(new_request, event_ratio)
        if pattern_id is not None:
            first_pattern_step = self.apply_pattern(new_request, get_pattern_name(pattern_id))
            self.async_update()
            self.connect_requests(request_id, new_request, first_pattern_step)
        return new_request

    def print_transition(self, request_id: str, old_request_base: MCWorkflowSubCampaign, transition: MCWorkflowTransition, description:str,
                          new_project: str=''):
        if not request_id:
            request_id = 'created on the previous step'
        result = [f"Clone {request_id} "]
        for change in transition.changes:
            if change.type == MCWorkflowChanges.ChangeType.SUBCAMPAIGN:
                result += [f'Change subcampaign to "{change.value}"']
            if change.type == MCWorkflowChanges.ChangeType.CAMPAIGN:
                result += [f'Change campaign to "{change.value}"']
            if change.type == MCWorkflowChanges.ChangeType.PROJECT_BASE:
                result += [f'Change project  to "{new_project.replace(old_request_base.project_base, change.value)}"']
                new_project = new_project.replace(old_request_base.project_base, change.value)
            if  change.type == MCWorkflowChanges.ChangeType.DESCRIPTION:
                result += [f'Change description to "{change.value} {description}"']
        result += [f'Apply pattern']
        result += [f'Connect requests {request_id} with the new request']
        return result, new_project


    def async_update(self):
        if self.async_task is not None:
            self.async_task.progress_message_update(self.async_update_values.count, self.async_update_values.total,
                                                    {'reqids':self.async_update_values.requests_ids})
            self.async_update_values.count += 1
    def submit_horizontal_transition(self, approve: bool = False, just_print: bool = False, selected_patterns: Dict[str, int]|None = None):
        horizontal_transitions = [x for x in self.base_workflow.transitions if x.transition_type == MCWorkflowTransition.TransitionType.HORIZONTAL]
        description = self.base_request.description
        current_request_id = self.base_request_id
        current_workflow = self.base_workflow
        current_workflow_name = self.base_workflow_name
        requests_to_approve = [current_request_id]
        transition_number = -1
        while horizontal_transitions:
            transition_number += 1
            transition = horizontal_transitions.pop()
            if len(horizontal_transitions) > 0:
                raise Exception(f'More than one horizontal transition in {current_request_id}')
            horizontal_transitions += [x for x in self.workflows_tree[transition.new_request].transitions if x.transition_type == MCWorkflowTransition.TransitionType.HORIZONTAL]
        horizontal_transitions = [x for x in self.base_workflow.transitions if x.transition_type == MCWorkflowTransition.TransitionType.HORIZONTAL]
        self.async_update_values.total = transition_number * 3 + 3
        if approve:
            self.async_update_values.total += 1
        self.async_update_values.requests_ids = requests_to_approve
        self.async_update()
        self.print_result = []
        self.found_patterns = {}
        current_project = str(self.base_request.project)
        while horizontal_transitions:
            transition = horizontal_transitions.pop()
            if just_print:
                try:
                    self.found_patterns[current_workflow_name] = self.find_pattern( self.base_request_id, transition.pattern)
                except:
                    pass
                current_transition, current_project  = self.print_transition(str(current_request_id),
                                                                               current_workflow, transition, description, current_project)
                self.print_result.append({'name':current_workflow_name,
                                          'transitions':current_transition})
                current_request_id = ''
            else:
                if selected_patterns is not None and current_workflow_name in selected_patterns:
                    pattern_id = selected_patterns[current_workflow_name]
                else:
                    try:
                        pattern_id = self.find_pattern(current_request_id, transition.pattern)
                    except Exception as ex:
                        raise Exception(f'Error in pattern {current_request_id} {ex}')
                current_request_id = self.apply_transition(current_request_id, current_workflow, transition, description, pattern_id, None)
                requests_to_approve.append(current_request_id)
                self.async_update_values.requests_ids = requests_to_approve
                self.async_update()
            current_workflow = self.workflows_tree[transition.new_request]
            current_workflow_name = transition.new_request
            horizontal_transitions += [x for x in current_workflow.transitions if x.transition_type == MCWorkflowTransition.TransitionType.HORIZONTAL]
        if approve and not just_print:
            for request_id in requests_to_approve:
                self.approve_request(request_id)
            self.async_update()

        return requests_to_approve




    def find_pattern(self, request_id, patterns):
        def get_request_evgen_release(slices, request_id):
            release = None
            tags = set()
            for slice in slices:
                steps = StepExecution.objects.filter(slice=slice, request=request_id)
                for step in steps:
                    if step.step_template.ctag not in tags:
                        tags.add(step.step_template.ctag)
                        if 'evgen' in step.step_template.step.lower():
                            new_release = ETAGRelease.objects.get(ami_tag=step.step_template.ctag).sw_release
                            if release and release != new_release:
                                raise Exception(f'Evgen releases are different {request_id}')
                            release = new_release
            if not release:
                raise Exception(f'No evgen steps {request_id}')
            return release


        slices = [x for x in InputRequestList.objects.filter(request=request_id).order_by("slice") if not x.is_hide]
        is_full = False
        if len(slices)>200:
            slices = slices[:200]
        for slice in slices:
            if 'fullsim' in slice.comment.lower() or '(fs)' in slice.comment.lower():
                is_full = True
            else:
                if is_full:
                    raise Exception(f'Fullsim and fastsim slices are mixed {request_id}')
        for pattern_type, pattern_id in patterns.items():
            if not is_full and pattern_type == MCWorkflowTransition.SimulationType.FASTSIM:
                return pattern_id
            if is_full and pattern_type == MCWorkflowTransition.SimulationType.FULLSIM:
                return pattern_id
            if ((not is_full and pattern_type == MCWorkflowTransition.SimulationType.FASTSIM_BYRELEASE) or
                (is_full and pattern_type == MCWorkflowTransition.SimulationType.FULLSIM_BYRELEASE)):
                release = get_request_evgen_release(slices, request_id)
                for pattern_by_request in pattern_id:
                   if release.startswith(str(pattern_by_request[0])):
                       return pattern_by_request[1]
        raise Exception(f'No pattern for {request_id}')

    def lock_request(self, new_request: int):
        cache_key = 'celery_request_action' + str(new_request)
        if cache.get(cache_key) and (type(cache.get(cache_key)) is dict):
            async_task = cache.get(cache_key)
            celery_task = AsyncResult(async_task.get('id'))
            if celery_task.status in ['FAILURE', 'SUCCESS']:
                cache.delete(cache_key)
        celery_task = self.async_task
        cache.set(cache_key, {'id': celery_task.request.id, 'name': 'Subcampaign split', 'user': 'auto'},
                  TRequest.DEFAULT_ASYNC_ACTION_TIMEOUT)
        
@app.task(bind=True, base=ProdSysTask, time_limit=3600*4)
@ProdSysTask.set_task_name('Split by sub campaigns')
def split_horizontal_by_subcampaign(self, reqid, approve=False, selected_patterns=None):
    action = WorkflowActions(reqid, async_task=self)
    return  action.submit_horizontal_transition(approve=approve, just_print=False, selected_patterns=selected_patterns)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def submit_horizontal_transition(request):
    try:
        action = WorkflowActions(int(request.data['requestID']))
        approve = request.data.get('approve', False)
        patterns = request.data.get('patterns', None)
        _jsonLogger.info('Split request by subcampaign', extra=form_json_request_dict(int(request.data['requestID']), request,
                                                                                           {'patterns': str(patterns)}))
        produced_requests = action.submit_horizontal_transition(approve=approve, just_print=False, selected_patterns=patterns)
        return Response(produced_requests)
    except Exception as ex:
        _logger.error(f'Problem with horizontal transition: {ex}')
        return Response(str(ex), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def submit_horizontal_transition_async(request):
    try:
        production_request_id = int(request.data['requestID'])
        approve = request.data.get('approve', False)
        patterns = request.data.get('patterns', None)
        _jsonLogger.info('Split request by subcampaign', extra=form_json_request_dict(production_request_id, request,
                                                                                           {'patterns': str(patterns)}))
        result = single_request_action_celery_task(production_request_id, split_horizontal_by_subcampaign, 'Split by sub campaigns',
                                  request.user.username, production_request_id, approve, patterns)

        # result = single_request_action_celery_task(production_request_id, test_async_progress_split, 'test task split', 'mborodin', [55974])
        return Response(result['task_id'])
    except Exception as ex:
        _logger.error(f'Problem with horizontal async transition: {ex}')
        return Response(str(ex), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.task(bind=True, base=ProdSysTask)
@ProdSysTask.set_task_name('test task split')
def test_async_progress_split(self, requests):
    for i in range(10):
        time.sleep(10)
        self.progress_message_update(i*10, 100, additional_info={'reqids':requests})
    if requests == []:
        raise Exception('Something Wrong')
    return requests

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def prepare_horizontal_transition(request):
    try:
        production_request_id = int(request.data['requestID'])
        production_request = TRequest.objects.get(reqid=production_request_id)
        action = WorkflowActions(production_request_id)
        if not action.base_workflow:
            raise Exception(f'No workflow found for {production_request_id}')
        action.submit_horizontal_transition(approve=False, just_print=True)
        async_task_id = ''
        cache_key = 'celery_request_action' + str(production_request_id)
        async_task = cache.get(cache_key, None)
        if async_task and (type(async_task) is dict):
            async_task_id = async_task.get('id')
            celery_task = AsyncResult(async_task.get('id'))
            if celery_task.status in ['FAILURE', 'SUCCESS']:
                cache.delete(cache_key)
                async_task_id = ''
        return Response({'request': ProductionRequestSerializer(production_request).data, 'print_results': action.print_result,
                         'patterns': action.found_patterns, 'long_description': production_request.long_description,
                         'number_of_slices': len([x for x in InputRequestList.objects.filter(request=production_request_id) if not x.is_hide]),
                                                 'all_patterns': get_all_patterns(), 'async_task_id': async_task_id})
    except Exception as ex:

        return Response(str(ex), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def read_from_spreadsheet(google_sprd: str):
    spreadsheet_dict = fill_steptemplate_from_gsprd(google_sprd, '3.0')
    if not spreadsheet_dict:
        raise Exception('No good data find in the spreadsheet. Please check that all required rows are filled.')
    for priority in [x['input_dict']['priority'] for x in spreadsheet_dict]:
        try:
            MCPriority.objects.get(priority_key=int(priority))
        except ObjectDoesNotExist as e:
            raise Exception("Priority %i doesn't exist in the system" % int(priority))
        except Exception as e:
            raise Exception("Error during priority check: %s" % str(e))
    return spreadsheet_dict


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_request_outputs(request, requestID):
    """
    Get the outputs of the request - last step of each slice which is not hidden.
    return: dictionary with keys as base slice and values as list of dictionaries with task, status, ami_tag and outputs
    """
    try:
        slices = InputRequestList.objects.filter(request=requestID).order_by('slice')
        outputs = {}
        output_step = {}
        for slice in slices:
            if not slice.is_hide:
                steps = StepExecution.objects.filter(slice=slice, request=requestID).order_by('id')
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(steps)
                if ordered_existed_steps:
                    output_key = slice.slice
                    for step in reversed(ordered_existed_steps):
                        if ProductionTask.objects.filter(step=step, request=requestID).exists():
                            outputs[output_key] = []
                            for task in ProductionTask.objects.filter(step=step, request=requestID):
                                outputs[output_key].append({'task': task.id, 'status': task.status, 'outputs': task.output_non_log_datasets(), 'ami_tag': task.ami_tag})
                            break
        return Response(outputs)

    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

