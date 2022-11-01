import json
import logging

import os
import re
from functools import reduce

import requests
from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone

from atlas.atlaselastic.views import get_tasks_action_logs, get_task_stats
from atlas.jedi.client import JEDIClientTest
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats, TRequest, \
    ProductionDataset, GroupProductionDeletionExtension, InputRequestList, StepExecution, StepTemplate, SliceError, \
    JediTasks, JediDatasetContents, JediDatasets

from rest_framework import serializers, generics
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes

from atlas.prodtask.task_views import get_sites, get_nucleus, get_global_shares
from atlas.task_action.task_management import TaskActionExecutor

_logger = logging.getLogger('prodtaskwebui')

class SliceSerializer(serializers.ModelSerializer):
    class Meta:
        model = InputRequestList
        fields = '__all__'


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
        if task_stats:
            finished_hpes06 = task_stats[0].task_hs06sec_finished
            failed_hpes06 = task_stats[0].task_hs06sec_failed
            if task.request_id > 300:
                running_files = 0
        total_output = 0
        input_events = 0
        input_bytes = 0
        output_datasets = set()
        for dataset in task_stats:
            if (dataset.type == 'output') and (dataset.dataset_id not in output_datasets):
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
def get_reassign_entities(request):
    return Response( {
    'sites': get_sites(),
    'nucleus': get_nucleus(),
    'shares': get_global_shares()})