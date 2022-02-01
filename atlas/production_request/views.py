import json

import os
from functools import reduce

from django.utils import timezone

from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats, TRequest, \
    ProductionDataset, GroupProductionDeletionExtension, InputRequestList, StepExecution, StepTemplate, SliceError

from rest_framework import serializers, generics
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes

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