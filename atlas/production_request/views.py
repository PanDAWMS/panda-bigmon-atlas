import json
import logging

import os
import re
from dataclasses import asdict
from functools import reduce

import math
import requests
from django.core.exceptions import ObjectDoesNotExist
from django.core.handlers.wsgi import WSGIRequest
from django.http import HttpRequest
from django.utils import timezone
from rest_framework.request import Request

from atlas.atlaselastic.views import get_tasks_action_logs, get_task_stats, get_campaign_nevents_per_amitag
from atlas.dkb.views import tasks_from_string, es_task_search_all
from atlas.jedi.client import JEDIClientTest
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats, TRequest, \
    ProductionDataset, GroupProductionDeletionExtension, InputRequestList, StepExecution, StepTemplate, SliceError, \
    JediTasks, JediDatasetContents, JediDatasets, SliceSerializer, ParentToChildRequest, SystemParametersHandler, \
    MCWorkflowTransition, MCWorkflowChanges, MCWorkflowRequest

from rest_framework import serializers, generics, status
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes

from atlas.prodtask.task_views import get_sites, get_nucleus, get_global_shares, tasks_serialisation
from atlas.prodtask.views import clone_slices
from atlas.production_request.derivation import find_all_inputs_by_tag
from atlas.task_action.task_management import TaskActionExecutor
from django.core.cache import cache


_logger = logging.getLogger('prodtaskwebui')




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

class ProductionRequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = TRequest
        fields = '__all__'


def child_derivation_tasks(request_id: int, steps: [int]) -> [ProductionTask]:
    child_tasks = []
    if ParentToChildRequest.objects.filter(parent_request=request_id, relation_type='DP').exists():
        for child_request in ParentToChildRequest.objects.filter(parent_request=request_id, relation_type='DP').values_list('child_request_id', flat=True):
            steps = list(StepExecution.objects.filter(request=child_request, step_parent_id__in=steps).values_list('id', flat=True))
            child_tasks += list(ProductionTask.objects.filter(step__in=steps))
    return child_tasks


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def production_task_for_request(request: Request) -> Response:
    try:
        if 'hashtagString' in request.data and request.data['hashtagString']:
            if 'dkb' in request.data and request.data['dkb']:
                tasks, _ ,_ = es_task_search_all({'search_string' :request.data['hashtagString']}, 'prod')
                tasks_id = [x['taskid'] for x in tasks]
                tasks = ProductionTask.objects.filter(id__in=tasks_id)
            else:
                tasks_id = tasks_from_string(request.data['hashtagString'])
                tasks = ProductionTask.objects.filter(id__in=tasks_id)
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
        tasks_serial = tasks_serialisation(tasks)
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
            trend = [{'seconds':math.ceil((current_time- x[0]).total_seconds()),'stats':x[1][0]['stats']} for x in current_trend if len(x)>0]
            total_stats.append({'mc_subcampaign':mc_subcampaign.campaign, 'stats':stats, 'trend':trend})
        return Response(total_stats)
    except Exception as ex:
        _logger.error(f'Problem with mc sub campaign loading: {ex}')
        return Response(f"Problem with mc sub campaign loading: {ex}", status=400)



def fill_default_campaigns():
    mc_subcampaigns = [SystemParametersHandler.MCSubCampaignStats('MC23:MC23d', ':25ns'),
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
    workflows = {('MC16a','evgen'): [], ('MC16a','simul'): [], ('MC16b','evgen'):[]}
    mc16a_changes = [MCWorkflowChanges(name='project_base', value='mc16', type=MCWorkflowChanges.ChangeType.REPLACE),
                     MCWorkflowChanges(name='description', value='(sim)', type=MCWorkflowChanges.ChangeType.APPEND)]
    mc16a_evgen_simul_transition = MCWorkflowTransition(new_request=('MC16a','simul'), parent_step='Evgen',
                                                        transition_type=MCWorkflowTransition.TransitionType.HORIZONTAL,
                                                        changes=mc16a_changes,
                                                        pattern={'AF2':116,'FS':76} )
    print(mc16a_evgen_simul_transition.model_dump_json())
    workflows[('MC16a','evgen')].append(mc16a_evgen_simul_transition)
    mc16a_mc16b_changes = [MCWorkflowChanges(name='subcampaign', value='mc16b', type=MCWorkflowChanges.ChangeType.REPLACE)]
    mc16a_mc16b_transition = MCWorkflowTransition(new_request=('MC16b','evgen'), parent_step='Evgen', transition_type=MCWorkflowTransition.TransitionType.VERTICAL,
                                                  changes=mc16a_mc16b_changes, event_ratio=1.2)
    workflows[('MC16a','evgen')].append(mc16a_mc16b_transition)
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