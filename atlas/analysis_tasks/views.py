import json
import logging
import string
import time
from copy import deepcopy
import datetime
from pprint import pprint
from time import sleep
from typing import Dict

import requests
from attr import dataclass
from django.contrib.auth.models import User, Group
from django.core.mail import send_mail
from django.core.cache import cache

from django.http import HttpResponseBadRequest
from django.urls import reverse
from django.utils import timezone
from rest_framework.response import Response

from jinja2.nativetypes import NativeEnvironment
from rest_framework import status
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated

from atlas.analysis_tasks.source_handling import submit_source_to_rucio, modify_and_submit_task, \
    submit_task_for_rucio_file, check_tag_source, check_source_exists
from atlas.atlaselastic.views import get_task_stats, TaskDatasetStats
from atlas.prestage.views import step_action
from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.hashtag import _set_request_hashtag, remove_hashtag_from_request
from atlas.prodtask.helper import form_json_request_dict
from atlas.prodtask.models import AnalysisTaskTemplate, TTask, TemplateVariable, InputRequestList, AnalysisStepTemplate, \
    ProductionTask, StepExecution, TRequest, SliceSerializer, JediDatasetContents, JediDatasets, SliceError, \
    HashTagToRequest, HashTag, SystemParametersHandler, StepAction, DistributedLock
from atlas.prodtask.settings import APP_SETTINGS
from atlas.prodtask.spdstodb import fill_template
from atlas.prodtask.step_manage_views import hide_slice
from atlas.prodtask.task_views import tasks_serialisation
from rest_framework import serializers
from atlas.celerybackend.celery import app
from atlas.prodtask.views import set_request_status
from atlas.production_request.derivation import get_container_name

_jsonLogger = logging.getLogger('prodtask_ELK')

OFFICIAL_HASHTAG = 'CentralisedNTUP'
def find_output_base(task_params: dict) -> str:
    output_base = ''
    if 'outDS' in task_params['cliParams']:
        output_base = [x for x in task_params['cliParams'].split('outDS')[1].replace('=', ' ').split(' ') if x][0]
    return output_base


def replace_template_by_variable(task_params: dict, variable: TemplateVariable, root: [str] = None) -> dict:
    if root is None:
        root = []
    for key, value in task_params.items():
        if type(value) == dict:
            task_params[key] = replace_template_by_variable(value, variable, root+[key])
        elif type(value) == str:
            if variable.value in value:
                task_params[key] = value.replace(variable.value, TemplateVariable.get_key_template(variable.name))
                variable.keys.append(TemplateVariable.KEYS_SEPARATOR.join(root+[key]))
        elif type(value) == list:
            for i in range(len(value)):
                if type(value[i]) == str:
                    if variable.value in value[i]:
                        value[i] = value[i].replace(variable.value, TemplateVariable.get_key_template(variable.name))
                        variable.keys.append(TemplateVariable.KEYS_SEPARATOR.join(root+[key, str(i)]))
                elif type(value[i]) == dict:
                    value[i] = replace_template_by_variable(value[i], variable, root+[key+TemplateVariable.KEYS_SEPARATOR+str(i)])
    return task_params

def move_job_paramas_to_task_params(task_params: dict) -> dict:
    if TemplateVariable.KEY_NAMES.JOB_PARAMETERS in task_params:
        for job_parameter in task_params[TemplateVariable.KEY_NAMES.JOB_PARAMETERS]:
            for key, value in job_parameter.items():
                if key in TemplateVariable.JOB_TO_TASK_PARAMETERS:
                    task_params[TemplateVariable.JOB_TO_TASK_PARAMETERS[key]] = value
    return task_params


def get_task_params(task_id: int, task_params: dict = None) -> [dict, [TemplateVariable]]:
    task = TTask.objects.get(id=task_id)
    template_variables = []
    if task_params is None:
        task_params = task.jedi_task_parameters
    output_base = find_output_base(task_params)
    task_name = task.name
    task_params['taskName'] = task_name.replace(task.name, TemplateVariable.get_key_template(TemplateVariable.KEY_NAMES.TASK_NAME))
    template_variables.append(TemplateVariable(TemplateVariable.KEY_NAMES.TASK_NAME, task_name, ['taskName']))
    task_params['userName'] = task_params['userName'].replace(task.username, TemplateVariable.get_key_template(TemplateVariable.KEY_NAMES.USER_NAME))
    template_variables.append(TemplateVariable(TemplateVariable.KEY_NAMES.USER_NAME, task.username, ['userName']))
    task_input = task_params.get('dsForIN')
    output_variable = TemplateVariable(TemplateVariable.KEY_NAMES.OUTPUT_BASE, output_base)
    task_params = replace_template_by_variable(task_params, output_variable, [])
    template_variables.append(output_variable)
    input_variable = TemplateVariable(TemplateVariable.KEY_NAMES.INPUT_BASE, task_input)
    task_params = replace_template_by_variable(task_params, input_variable, [])
    template_variables.append(input_variable)
    if ':' in output_base:
        output_scope = output_base.split(':')[0]
    else:
        if output_base.startswith('user') or output_base.startswith('group'):
            output_scope = '.'.join(output_base.split('.')[0:2])
        else:
            output_scope = output_base.split('.')[0]
    scope_variable = TemplateVariable(TemplateVariable.KEY_NAMES.OUTPUT_SCOPE, output_scope)
    task_params = replace_template_by_variable(task_params, scope_variable, [])
    template_variables.append(scope_variable)
    template_variables.append(TemplateVariable(TemplateVariable.KEY_NAMES.TASK_PRIORITY, task_params.get('taskPriority','1000'),
                                               ['taskPriority'], TemplateVariable.VariableType.INTEGER))
    if 'parent_tid' in task_params:
        template_variables.append(
            TemplateVariable(TemplateVariable.KEY_NAMES.PARENT_ID, task_params['parent_tid'],
                             ['parent_tid'], TemplateVariable.VariableType.INTEGER))
    task_params = move_job_paramas_to_task_params(task_params)
    return task_params, template_variables

def create_pattern_from_task(task_id: int, pattern_description: str, task_params: dict = None, source_action: str = '',
                             username: str = '') -> AnalysisTaskTemplate:
    task_template, template_variables = get_task_params(task_id, TTask.objects.get(id=task_id).jedi_task_parameters)
    new_pattern = AnalysisTaskTemplate()
    new_pattern.description = pattern_description
    new_pattern.task_parameters = task_params
    new_pattern.software_release = task_params.get('transHome','')
    new_pattern.physics_group = task_params.get('workingGroup','VALI')
    new_pattern.variables_data = template_variables
    new_pattern.build_task = task_id
    new_pattern.username = username
    if source_action:
        new_pattern.source_action = source_action
    new_pattern.save()
    return new_pattern


def create_task_from_pattern(pattern_id: int, task_name: str, task_params: dict) -> TTask:
    pattern = AnalysisTaskTemplate.objects.get(id=pattern_id)
    new_task = TTask()
    return new_task


def check_user_group(group: str, username: str, working_group: str) -> bool:
    user = User.objects.get(username=username)
    user_groups = [group.name for group in user.groups.all()]
    group_name = group.split('.')[1]
    iam_group = f'IAM:atlas/{group_name}/production'
    if iam_group not in user_groups and not user.is_superuser:
        raise Exception(f'User {username} is not in group {iam_group}')
    if working_group != group_name:
        raise Exception(f'Working group {working_group} does not match group {group_name}')
    return True


def send_new_request_mail(request_id: int, template: AnalysisTaskTemplate , username: str, link: str):
        production_request = TRequest.objects.get(reqid=request_id)
        short_description = ''.join([x for x in production_request.description if x in string.printable]).replace('\n',
                                                                                                                  '').replace(
            '\r', '')
        subject = f'Analysis request {short_description} {template.physics_group}'
        mail_body = f"""
    New analysis request was created by {username} for the template {template.description}
    
    Best,


    Details:
    - Link to Request: {link}
    """

        mail_from = APP_SETTINGS['prodtask.email.from']
        send_mail(subject,
                  mail_body,
                  mail_from,
                  SystemParametersHandler.get_analysis_request_email().emails,
                   fail_silently=True)

def verify_step(step: AnalysisStepTemplate, ddm: DDM, username: str):
    prod_step = step.step_production_parent
    if not prod_step.step_parent or (prod_step.step_parent == prod_step):
        if not ddm.dataset_exists(step.get_variable(TemplateVariable.KEY_NAMES.INPUT_BASE)):
            raise Exception(f'Input dataset {step.get_variable(TemplateVariable.KEY_NAMES.INPUT_BASE)} does not exist')
        input_dataset = step.get_variable(TemplateVariable.KEY_NAMES.INPUT_BASE)
        if ddm.dataset_metadata(input_dataset)['did_type'] == 'CONTAINER':
            datasets_in_container = ddm.dataset_in_container(input_dataset)
            if len(datasets_in_container) == 0:
                raise Exception(f'Container {input_dataset} is empty')
    if step.get_variable(TemplateVariable.KEY_NAMES.OUTPUT_SCOPE).startswith('group'):
        check_user_group(step.get_variable(TemplateVariable.KEY_NAMES.OUTPUT_SCOPE), username, step.step_parameters.get(TemplateVariable.KEY_NAMES.WORKING_GROUP))
    else:
        if  step.step_parameters.get(TemplateVariable.KEY_NAMES.WORKING_GROUP):
            raise Exception(f'Working group {step.step_parameters.get(TemplateVariable.KEY_NAMES.WORKING_GROUP)} does not match user scope')
    if step.step_parameters.get(TemplateVariable.JOB_TO_TASK_PARAMETERS[TemplateVariable.KEY_NAMES.DESTINATION]):
        try:
            rse_exists = ddm.list_rses(f"rse={step.step_parameters.get(TemplateVariable.JOB_TO_TASK_PARAMETERS[TemplateVariable.KEY_NAMES.DESTINATION])}")
        except Exception as e:
            raise Exception(f'RSE {step.step_parameters.get(TemplateVariable.JOB_TO_TASK_PARAMETERS[TemplateVariable.KEY_NAMES.DESTINATION])} does not exist')

def possible_on_tape(dataset_name: str) -> bool:
    if '.AOD.' in dataset_name:
        return True
    if '.DAOD_' in dataset_name and dataset_name.startswith('data') and 'tid' not in dataset_name:
        return True
    return False



def create_data_carousel(analysis_step: AnalysisStepTemplate):
    ddm = DDM()
    input_dataset = analysis_step.get_variable(TemplateVariable.KEY_NAMES.INPUT_BASE)
    if ddm.dataset_metadata(input_dataset)['did_type'] == 'CONTAINER':
        datasets = ddm.dataset_in_container(input_dataset)
    else:
        datasets = [input_dataset]
    verify_on_tape = []
    for dataset in datasets:
        if possible_on_tape(dataset):
            verify_on_tape.append(dataset)

    for dataset in verify_on_tape:
        full_replicas = ddm.full_replicas_per_type(dataset)
        if len(full_replicas['data']) == 0 and len(full_replicas['tape']) > 0:
            if not StepAction.objects.filter(step=int(analysis_step.step_production_parent.id), action=StepAction.STAGING_ACTION,
                                             status__in=['active', 'executing', 'verify']).exists():
                sa = StepAction()
                sa.request = analysis_step.request
                sa.step = analysis_step.step_production_parent.id
                sa.attempt = 0
                sa.create_time = timezone.now()
                sa.execution_time = timezone.now() + datetime.timedelta(minutes=12)
                sa.status = 'active'
                sa.action = StepAction.STAGING_ACTION
                sa.save()
            analysis_step.change_variable(TemplateVariable.KEY_NAMES.TO_STAGING, 'True')
            analysis_step.step_parameters[TemplateVariable.KEY_NAMES.TO_STAGING] = 'True'
            analysis_step.save()
            return analysis_step
    return analysis_step






def create_analy_task_for_slice(requestID: int, slice: int, username: str ) -> [int]:
    new_tasks = []
    ddm = DDM()
    steps = AnalysisStepTemplate.objects.filter(request=requestID, slice=InputRequestList.objects.get(slice=slice, request=requestID))
    for step in steps:
        if (step.status == AnalysisStepTemplate.STATUS.APPROVED) and (not ProductionTask.objects.filter(step=step.step_production_parent).exists()):
            verify_step(step, ddm, username)
            task_id = TTask().get_id()
            prod_step = step.step_production_parent
            if not prod_step.step_parent or (prod_step.step_parent == prod_step):
                t_task, prod_task = register_analysis_task(step, task_id, task_id)
                t_task.save()
                prod_task.save()
                new_tasks.append(task_id)
            else:
                input_tasks = ProductionTask.objects.filter(step=prod_step.step_parent)
                for input_task in input_tasks:
                    current_step = AnalysisStepTemplate.objects.get(id=step.id)
                    if input_task.status not in ProductionTask.BAD_STATUS:
                        for dataset in input_task.output_non_log_datasets():
                            if current_step.slice.dataset ==  get_container_name(dataset):
                                # current_step.change_step_input(dataset)
                                current_step.change_variable(TemplateVariable.KEY_NAMES.INPUT_BASE, dataset)
                                task_id = TTask().get_id()
                                t_task, prod_task = register_analysis_task(current_step, task_id, input_task.id)
                                t_task.save()
                                prod_task.save()
                                new_tasks.append(task_id)
                    current_step = None

    return new_tasks

def monk_create_analy_task_for_slice(requestID: int, slice: int ):
    new_tasks = []
    steps = AnalysisStepTemplate.objects.filter(request=requestID, slice=InputRequestList.objects.get(slice=slice, request=requestID))
    for step in steps:
        if (step.status == AnalysisStepTemplate.STATUS.APPROVED) and (not ProductionTask.objects.filter(step=step.step_production_parent).exists()):
            task_id = TTask().get_id()
            t_task, prod_task = register_analysis_task(step, task_id, task_id)
            return t_task, prod_task


def print_rendered_steps_in_slice(requestID: int, slice: int):
    steps = AnalysisStepTemplate.objects.filter(request=requestID, slice=InputRequestList.objects.get(slice=slice, request=requestID))
    for step in steps:
        pprint(step.render_task_template())


def check_name_version(step_template: AnalysisStepTemplate):
    current_task_name = step_template.get_variable(TemplateVariable.KEY_NAMES.TASK_NAME)
    input_dataset = step_template.get_variable(TemplateVariable.KEY_NAMES.INPUT_BASE)
    if TTask.objects.filter(name=current_task_name).exists():
        name_base = current_task_name.replace('/','')
        current_postfix = name_base.split('.')[-1]
        current_postfix_number = 1
        if current_postfix.startswith('v'):
            current_postfix_number = int(current_postfix[1:])
            name_base = '.'.join(name_base.split('.')[:-1])
        for version_number in range(current_postfix_number, 99):
            new_name = name_base + '.v' + f'{version_number:02d}'
            if TTask.objects.filter(name=new_name+ '/').exists():
                previous_task = TTask.objects.filter(name=new_name+ '/').last()
                if ((previous_task.status not in ProductionTask.BAD_STATUS) and
                        input_dataset == previous_task.input_dataset):
                    if (ProductionTask.objects.filter(id=previous_task.id).exists() and
                        ProductionTask.objects.get(id=previous_task.id).status in [ProductionTask.STATUS.TOABORT]):
                        continue
                    raise Exception(f'Task already exists: {previous_task.id}')
                continue
            step_template.change_variable(TemplateVariable.KEY_NAMES.TASK_NAME, new_name+ '/')
            step_template.change_variable(TemplateVariable.KEY_NAMES.OUTPUT_BASE, new_name)
            step_template.save()
            return step_template
        raise Exception('Too many versions of task')
    return step_template


def check_input_source_exists(step_template: AnalysisStepTemplate):
    if not check_source_exists(step_template.step_parameters['buildSpec']['archiveName'], step_template.step_parameters['sourceURL']):
            task_template =  AnalysisTaskTemplate.objects.get(id=step_template.task_template.id)
            if task_template.task_parameters['buildSpec']['archiveName'] != step_template.step_parameters['buildSpec']['archiveName']:
                if not check_source_exists(task_template.task_parameters['buildSpec']['archiveName'], task_template.task_parameters['sourceURL']):
                        raise Exception(f'Source tarball {task_template.task_parameters["buildSpec"]["archiveName"]} does not exist')
                step_template.step_parameters['buildSpec']['archiveName'] = task_template.task_parameters['buildSpec']['archiveName']
                step_template.step_parameters['sourceURL'] = task_template.task_parameters['sourceURL']
                step_template.save()
                return step_template
            _jsonLogger.error(f'Failed to reinitialise tag {step_template.task_template.tag}')
            raise Exception(f"Source tarball {step_template.step_parameters['buildSpec']['archiveName']} does not exist")
    return step_template

def collect_all_output_datasets(request_id: int) -> [str]:
    output_datasets = []
    for slice in InputRequestList.objects.filter(request=request_id):
        for step in AnalysisStepTemplate.objects.filter(request=request_id, slice=slice):
            if ProductionTask.objects.filter(step=step.step_production_parent).exists():
                tasks = ProductionTask.objects.filter(step=step.step_production_parent)
                for task in tasks:
                    if task.status in [ProductionTask.STATUS.FINISHED, ProductionTask.STATUS.DONE]:
                        datasets = JediDatasets.objects.filter(id=task.id)
                        for dataset in datasets:
                            if dataset.type == 'tmpl_output':
                                output_datasets.append(dataset.datasetname)
    return output_datasets


def check_parameters_for_task(step_template: AnalysisStepTemplate) -> bool:
    if (step_template.step_parameters.get(TemplateVariable.KEY_NAMES.INPUT_DS) is not None and
        step_template.step_parameters.get(TemplateVariable.KEY_NAMES.nEVENTS) is not None):
        raise Exception('Input dataset and nEvents are not allowed in the same task, please use nFiles instead')
    return True


def register_analysis_task(step_template: AnalysisStepTemplate, task_id: int, parent_tid: int) -> [TTask, ProductionTask]:
    step_template = check_name_version(step_template)
    #step_template = create_data_carousel(step_template)
    step_template = check_input_source_exists(step_template)
    step_template.step_parameters = deepcopy(step_template.render_task_template())
    check_parameters_for_task(step_template)
    task = TTask(id=task_id,
                          parent_tid=parent_tid,
                          status=ProductionTask.STATUS.WAITING,
                          total_done_jobs=0,
                          submit_time=timezone.now(),
                          vo=step_template.vo,
                          prodSourceLabel=step_template.prodSourceLabel,
                          name=step_template.step_parameters[step_template.get_variable_key(TemplateVariable.KEY_NAMES.TASK_NAME)],
                          username=step_template.step_parameters[step_template.get_variable_key(TemplateVariable.KEY_NAMES.USER_NAME)],
                          chain_id=parent_tid,
                          _jedi_task_parameters=json.dumps(step_template.step_parameters))

    prod_task = ProductionTask(id=task.id,
                               step=step_template.step_production_parent,
                               request=step_template.request,
                               parent_id=parent_tid,
                               name=task.name,
                               project=step_template.project,
                               phys_group='',
                               provenance='',
                               status=ProductionTask.STATUS.WAITING,
                               total_events=0,
                               total_req_jobs=0,
                               total_done_jobs=0,
                               submit_time=timezone.now(),
                               bug_report=0,
                               priority=task.priority,
                               inputdataset=step_template.input_dataset,
                               timestamp=timezone.now(),
                               vo=step_template.vo,
                               prodSourceLabel=step_template.prodSourceLabel,
                               username=task.username,
                               chain_id=task.id,
                               #dynamic_jobdef=None,
                               campaign='',
                               subcampaign='',
                               bunchspacing='',
                               total_req_events=-1,
                               #pileup=None,
                               simulation_type='notMC',
                               #is_extension=None,
                               #ttcr_timestamp=None,
                               primary_input=step_template.input_dataset,
                               ami_tag=step_template.task_template.tag,
                               output_formats='')
    return task, prod_task



def create_step_from_template(slice: InputRequestList, template: AnalysisTaskTemplate, parent_step: StepExecution|None=None) -> AnalysisStepTemplate:
    prod_step = StepExecution()
    prod_step.step_template = fill_template(AnalysisStepTemplate.ANALYSIS_STEP_NAME.GROUP_ANALYSIS, template.tag, slice.priority)
    prod_step.request = slice.request
    prod_step.slice = slice
    prod_step.status = StepExecution.STATUS.NOT_CHECKED
    prod_step.priority = slice.priority
    prod_step.step_def_time = timezone.now()
    prod_step.input_events = -1
    if parent_step:
        prod_step.step_parent = parent_step
    prod_step.save()
    step = AnalysisStepTemplate()
    step.name = AnalysisStepTemplate.ANALYSIS_STEP_NAME.GROUP_ANALYSIS
    step.status = AnalysisStepTemplate.STATUS.NOT_CHECKED
    step.task_template = template
    step.step_parameters = template.task_parameters
    step.slice = slice
    step.request = slice.request
    step.variables_data = deepcopy(template.variables_data)
    step.change_step_input(slice.dataset)
    step.template = template
    step.step_production_parent = prod_step
    step.save()
    prod_step.analysis_step_id = step.id
    prod_step.save()
    return step


def add_analysis_slices_to_request(production_request: TRequest, template: AnalysisTaskTemplate, containers: [str]) -> [int]:
    slices = []
    for container in containers:
        slice = InputRequestList()
        new_slice_number = InputRequestList.objects.filter(request=production_request).count()
        slice.request = production_request
        slice.dataset = container
        slice.priority = template.get_variable(TemplateVariable.KEY_NAMES.TASK_PRIORITY)
        slice.brief = 'Analysis slice'
        slice.slice = new_slice_number
        slice.save()
        step = create_step_from_template(slice, template)
        slices.append(new_slice_number)
    return slices

def add_child_analysis_slices_to_request(production_request: TRequest, template: AnalysisTaskTemplate,
                                         parent_steps: [StepExecution], parent_output: str) -> [int]:
    slices = []
    for parent_step in parent_steps:
        task = ProductionTask.objects.filter(step=parent_step).last()
        if task:
            for dataset in task.output_non_log_datasets():
                if parent_output in dataset.split('.'):
                    slice = InputRequestList()
                    new_slice_number = InputRequestList.objects.filter(request=production_request).count()
                    slice.request = production_request
                    slice.dataset =  get_container_name(dataset)
                    slice.priority = template.get_variable(TemplateVariable.KEY_NAMES.TASK_PRIORITY)
                    slice.brief = 'Analysis slice'
                    slice.slice = new_slice_number
                    slice.save()
                    step = create_step_from_template(slice, template, parent_step)
                    slices.append(new_slice_number)
    return slices


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def prepare_template_from_task(request):
    try:
        task_id = int(request.query_params.get('task_id'))
        task_template, _ = get_task_params(task_id, TTask.objects.get(id=task_id).jedi_task_parameters)
        task_template[TemplateVariable.KEY_NAMES.NO_EMAIL] = True
        return Response(task_template)
    except TTask.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def create_template(request):
    try:
        _jsonLogger.info('Create template from task',extra={'user':request.user.username, 'template_task_id':request.data['taskID']})

        new_pattern = create_pattern_from_task(int(request.data['taskID']), request.data['description'],
                                               request.data['taskTemplate'], request.data['sourceAction'], request.user.username)
        try:
            _jsonLogger.info('Submit source to rucio',
                             extra={'user': request.user.username, 'template_task_id': request.data['taskID']})
            submit_source_to_rucio.delay(new_pattern.id)
        except Exception as e:
            _jsonLogger.error(f'Submit source to rucio failed {e}',
                             extra={'user': request.user.username, 'template_task_id': request.data['taskID']})
        return Response(new_pattern.tag,status=status.HTTP_201_CREATED)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class AnalysisTaskTemplateSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalysisTaskTemplate
        fields = '__all__'

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_template(request):
    try:
        template_tag = request.query_params.get('template_tag')
        template = AnalysisTaskTemplate.objects.filter(tag=template_tag).last()
        return Response(AnalysisTaskTemplateSerializer(template).data)
    except AnalysisTaskTemplate.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def tasks_analysis_serialisation(tasks: [ProductionTask], step_name) -> [dict]:
    tasks_serial = []
    for task in tasks:
        serial_task = task.__dict__
        del serial_task['_state']
        serial_task.update(dict(step_name=step_name))
        serial_task['failureRate'] = task.failure_rate or 0
        tasks_serial.append(serial_task)
    return tasks_serial
def analysis_steps_serializer(analysis_steps: [AnalysisStepTemplate], steps_by_id: dict = None, tasks_by_id: dict = None, all_analysis_patterns: dict = None) -> [dict]:
    serialized_steps = []
    for step in analysis_steps:
        production_step = steps_by_id[step.step_production_parent_id]
        if production_step.step_parent_id == production_step.id:
            step_production_parent_id = production_step.id
            production_step_parent_request_id = production_step.request_id
            production_step_parent_slice = None
        else:
            step_production_parent_id = production_step.step_parent_id
            production_step_parent_slice = production_step.step_parent.slice.slice
            production_step_parent_request_id = production_step.step_parent.request_id
        serialized_production_step = {'id':production_step.id,'status':production_step.status, 'step':AnalysisStepTemplate.ANALYSIS_STEP_NAME.GROUP_ANALYSIS,
                             'production_step_parent_id':step_production_parent_id,
                                      'request':production_step.request_id,'task_config':'',
                             'priority':production_step.priority,'input_events':production_step.input_events, 'project_mode': '',
                                      'production_step_parent_request_id':production_step_parent_request_id,
                                        'production_step_parent_slice':production_step_parent_slice,
                                      }
        tasks = tasks_by_id.get(production_step.id, [])
        serialized_tasks = []
        if tasks:
            serialized_tasks = tasks_analysis_serialisation(tasks, AnalysisStepTemplate.ANALYSIS_STEP_NAME.GROUP_ANALYSIS)
        serialized_step = {
            'id': step.id,
            'name': step.name,
            'status': step.status,
            'step_parameters': step.step_parameters,
            'slice_id': step.slice_id,
            'request_id': step.request_id,
            'step_production_parent_id': step_production_parent_id,
            'step_analysis_parent_id': step.id,
            'template_name': all_analysis_patterns[step.task_template_id].tag,
        }

        serialized_steps.append({'analysis_step': serialized_step, 'step': serialized_production_step, 'tasks': serialized_tasks})
    return serialized_steps


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_all_patterns(request):
    try:
        if request.query_params.get('status') == 'all':
            patterns = AnalysisTaskTemplate.objects.all()
        else:
            patterns = AnalysisTaskTemplate.objects.filter(status=AnalysisTaskTemplate.STATUS.ACTIVE)
        serialized_patterns = []
        for pattern in patterns:
            serialized_patterns.append(AnalysisTaskTemplateSerializer(pattern).data)
        return Response(serialized_patterns)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def create_analysis_request(request):
    try:
        if request.data['requestExtID']:
            new_request = TRequest.objects.get(reqid=int(request.data['requestExtID']))
            if new_request.request_type != 'ANALYSIS':
                return Response(f'Request {request.data["requestExtID"]} is not an analysis request', status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            new_request = TRequest.objects.get(reqid=48149)
            new_request.reqid = None
            new_request.description = request.data['description']
            new_request.save()

        analysis_template_base = request.data['templateBase']
        new_scope = request.data['scope']
        analysis_template = AnalysisTaskTemplate.objects.get(id=analysis_template_base['id'])
        check_tag_source.delay(analysis_template.tag)
        analysis_template.task_parameters = analysis_template_base['task_parameters']
        if analysis_template.get_variable(TemplateVariable.KEY_NAMES.OUTPUT_SCOPE) != new_scope:
            old_scope = analysis_template.get_variable(TemplateVariable.KEY_NAMES.OUTPUT_SCOPE)
            if old_scope.startswith('user'):
                analysis_template.change_variable(TemplateVariable.KEY_NAMES.USER_NAME, f"{request.user.first_name} {request.user.last_name}")
            if new_scope.startswith('group'):
                analysis_template.task_parameters[TemplateVariable.KEY_NAMES.WORKING_GROUP] = new_scope.split('.')[1]
                analysis_template.change_variable(TemplateVariable.KEY_NAMES.WORKING_GROUP, new_scope.split('.')[1])
            analysis_template.change_variable(TemplateVariable.KEY_NAMES.OUTPUT_SCOPE, new_scope)
            analysis_template.change_variable(TemplateVariable.KEY_NAMES.TASK_NAME, analysis_template.get_variable(TemplateVariable.KEY_NAMES.TASK_NAME).replace(old_scope, new_scope))
            analysis_template.change_variable(TemplateVariable.KEY_NAMES.OUTPUT_BASE, analysis_template.get_variable(TemplateVariable.KEY_NAMES.OUTPUT_BASE).replace(old_scope, new_scope))
        if len( f"{request.user.first_name} {request.user.last_name}") < 32:
            new_request.manager = f"{request.user.first_name} {request.user.last_name}"
        else:
            new_request.manager = request.user.username
        new_request.save()
        if 'inputContainers' in request.data:
            add_analysis_slices_to_request(TRequest.objects.get(reqid=new_request.reqid),analysis_template , request.data['inputContainers'])
        elif 'inputSlices' in request.data:
            steps = []
            input_request_id = None
            output_format = None
            for slice in request.data['inputSlices']:
                if not input_request_id:
                    input_request_id = int(slice['requestID'])
                    output_format = slice['outputFormat']
                steps.append(StepExecution.objects.get(request=input_request_id, slice=InputRequestList.objects.get(slice=slice['slice'], request=input_request_id)))
            add_child_analysis_slices_to_request(TRequest.objects.get(reqid=new_request.reqid),analysis_template , steps, output_format)
        send_new_request_mail(new_request.reqid, analysis_template, request.user.username, request.build_absolute_uri(reverse('prodtask:input_list_approve',args=(new_request.reqid,))))
        return Response(str(new_request.reqid))

    except Exception as e:
        _jsonLogger.error(f'Create request problem {e}',
                          extra={'user': request.user.username})

        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_request(request):
    try:
        request_id = int(request.query_params.get('request_id'))
        if request_id < 1000:
            raise TRequest.DoesNotExist
        request = TRequest.objects.get(reqid=request_id)
        slices = list(InputRequestList.objects.filter(request=request))
        all_analysis_steps =  list(AnalysisStepTemplate.objects.filter(request=request).order_by('id'))

        analysis_steps_by_slice = {}
        analysis_pattern_ids = set()
        for step in all_analysis_steps:
            if step.slice_id not in analysis_steps_by_slice:
                analysis_steps_by_slice[step.slice_id] = []
            analysis_steps_by_slice[step.slice_id].append(step)
            analysis_pattern_ids.add(step.task_template_id)
        all_analysis_patterns = {pattern.id: pattern for pattern in list(AnalysisTaskTemplate.objects.filter(id__in=analysis_pattern_ids))}
        all_production_steps = { step.id: step for step in list(StepExecution.objects.filter(request=request))}
        all_tasks = list(ProductionTask.objects.filter(request=request))
        all_tasks_by_step = {}
        for task in all_tasks:
            if task.step_id not in all_tasks_by_step:
                all_tasks_by_step[task.step_id] = []
            all_tasks_by_step[task.step_id].append(task)
        serialized_slices_with_steps = []
        all_slice_errors = {error.slice_id: error for error in list(SliceError.objects.filter(request=request, is_active=True))}

        for slice in slices:
            if slice.is_hide is None or slice.is_hide == False:
                analysis_steps = analysis_steps_by_slice.get(slice.id, [])
                serialized_steps = analysis_steps_serializer(analysis_steps, all_production_steps, all_tasks_by_step, all_analysis_patterns)
                slice_error = ''
                if all_slice_errors.get(slice.id):
                    slice_error = all_slice_errors.get(slice.id).message
                serialized_slices_with_steps.append({'slice': SliceSerializer(slice).data, 'steps': serialized_steps, 'slice_error': slice_error})

        return Response(serialized_slices_with_steps)

    except TRequest.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_request_stat(request):
    try:
        request_id = int(request.query_params.get('request_id'))
        if request_id < 1000:
            raise TRequest.DoesNotExist
        tasks = ProductionTask.objects.filter(request=request_id)
        tasks_stat = {'hs06sec_finished': 0, 'hs06sec_failed': 0, 'bytes': 0}
        for task in tasks:
            all_datasets_stats = get_task_stats(task.id)
            hs_set = False
            for dataset_stat in all_datasets_stats:
                if dataset_stat.type == 'output':
                    if not hs_set:
                        tasks_stat['hs06sec_finished'] += dataset_stat.task_hs06sec_finished
                        tasks_stat['hs06sec_failed'] += dataset_stat.task_hs06sec_failed
                        hs_set = True
                    if task.status not in ProductionTask.RED_STATUS:
                        tasks_stat['bytes'] += dataset_stat.bytes
        return Response(tasks_stat)

    except TRequest.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_request_output_datasets_names(request):
    try:
        request_id = int(request.query_params.get('requestID'))
        if request_id < 1000:
            raise TRequest.DoesNotExist
        output_datasets = collect_all_output_datasets(request_id)
        return Response(output_datasets)
    except TRequest.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)
def clone_analysis_request_slices(request):
    slices = request.data['slices']
    for slice in slices:
        simple_analy_slice_clone(request.data['requestID'], slice)
    return Response({'result':f"{len(slices)} cloned"}, status=status.HTTP_200_OK)


def get_slices_template(request):
    try:
        request_id = int(request.data['requestID'])
        if request_id < 1000:
            raise TRequest.DoesNotExist
        production_request = TRequest.objects.get(reqid=request_id)
        slices = request.data['slices']
        result = {'slicesToModify':[], 'template':None}
        for slice_number in slices:
            slice = InputRequestList.objects.filter(request=production_request).get(slice=slice_number)
            step = AnalysisStepTemplate.objects.filter(request=production_request, slice=slice).order_by('id').first()
            if step and step.status == AnalysisStepTemplate.STATUS.NOT_CHECKED:
                result['slicesToModify'].append(slice_number)
                if not result['template']:
                    result['template'] = step.step_parameters
        return Response(result, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def modify_slices_template(request):
    try:
        request_id = int(request.data['requestID'])
        if request_id < 1000:
            raise TRequest.DoesNotExist
        production_request = TRequest.objects.get(reqid=request_id)
        slices = request.data['slices']
        template = request.data['template']
        input_dataset = request.data['inputDataset'].strip()
        result = {'slicesModified':[]}
        for slice_number in slices:
            slice = InputRequestList.objects.filter(request=production_request).get(slice=slice_number)
            step = AnalysisStepTemplate.objects.filter(request=production_request, slice=slice).order_by('id').first()
            if step and step.status == AnalysisStepTemplate.STATUS.NOT_CHECKED:
                step.step_parameters = template
                if input_dataset:
                    slice.dataset = input_dataset
                    step.change_step_input(input_dataset)
                    slice.save()
                step.save()
                result['slicesModified'].append(slice_number)
        return Response({'result':f"{len(result['slicesModified'])} modified"}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def set_analysis_request_status(request):
    production_request_id = request.data['requestID']
    new_status = request.data['status']
    set_request_status(request.user.username, production_request_id, new_status, 'Request status was changed to %s by %s' % (new_status, request.user.username),
                          'Request status is changed to %s by WebUI' % new_status, request)


    return Response({'status': TRequest.objects.get(reqid=production_request_id).cstatus}, status=status.HTTP_200_OK)


def hide_analysis_slices(request):
    try:
        request_id = int(request.data['requestID'])
        if request_id < 1000:
            raise TRequest.DoesNotExist
        production_request = TRequest.objects.get(reqid=request_id)
        slices = request.data['slices']
        for sliceNumber in slices:
            slice = InputRequestList.objects.get(request=production_request, slice=sliceNumber)
            hide_slice(slice)
        return Response({'result':f"{len(slices)} hidden"}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def add_delete_analysis_request_hashtag(request):
    try:
        action = request.data['action']
        request_id = int(request.data['requestID'])
        hashtag = request.data['hashtag']
        if request_id < 1000:
            raise TRequest.DoesNotExist
        if action == 'add':
            _set_request_hashtag(request_id, hashtag)
        elif action == 'delete':
            if hashtag != OFFICIAL_HASHTAG:
                remove_hashtag_from_request(request_id, hashtag)
        return Response({'result':f"{hashtag} {action}ed"}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)
@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def analysis_request_action(request):
    try:
        action = request.data['action']
        _jsonLogger.info('Do analysis request action', extra=form_json_request_dict(request.data['requestID'], request, extra={'action':action}))
        if action == 'submit':
            return submit_analysis_slices(request)
        elif action == 'clone':
            return clone_analysis_request_slices(request)
        elif action == 'hide':
            return hide_analysis_slices(request)
        elif action == 'getSlicesTemplate':
            return get_slices_template(request)
        elif action == 'modifySlicesTemplate':
            return modify_slices_template(request)
        elif action == 'setStatus':
            return set_analysis_request_status(request)
        else:
            return Response('Unknown action', status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.task()
def prepare_eventloop_analysis_task(request_id: int):
    slices = InputRequestList.objects.filter(request=request_id)
    input_datasets = {}
    slice_to_modify = {}
    original_input = {}
    for slice in slices:
        if not slice.is_hide:
            step = AnalysisStepTemplate.objects.filter(request=request_id, slice=slice).order_by('id').first()
            if (step.task_template.source_action == AnalysisTaskTemplate.SOURCE_ACTION.EVENTLOOP and
                    not ProductionTask.objects.filter(step=step.step_production_parent).exists()):
                if step.task_template.source_tar not in input_datasets:
                    input_datasets[step.task_template.source_tar] = []
                    slice_to_modify[step.task_template.source_tar] = []
                    original_task_id = step.task_template.build_task
                    original_task = TTask.objects.get(id=original_task_id)
                    original_task_parameters = original_task.jedi_task_parameters
                    original_input[step.task_template.source_tar] = original_task_parameters['dsForIN']
                input_datasets[step.task_template.source_tar].append(slice.dataset)
                slice_to_modify[step.task_template.source_tar].append(slice.slice)
    if input_datasets:
        for source, datasets in input_datasets.items():
            new_task = modify_and_submit_task(source, original_input[source] , datasets)
            sleep(5)
            _jsonLogger.info(f'Change input source for eventloop tasks with {new_task} for {len(slice_to_modify[source])} slices',
                             extra=form_json_request_dict(request_id, None, extra={'slices':str(slice_to_modify[source])}))
            for slice in slice_to_modify[source]:
                modify_analysis_step_input_source(request_id, slice, new_task)


def unset_slice_error(request, slice):
    try:
        if SliceError.objects.filter(request=request, slice=slice, is_active=True).exists():
            slice_error = SliceError.objects.filter(request=request, slice=slice)[0]
            slice_error.is_active = False
            slice_error.save()
    except Exception as ex:
        _jsonLogger.warning('Slice error saving failed: {0}'.format(ex))


def set_slice_error(request, slice, exception_type, message):
    try:
        slice_error = SliceError(request=TRequest.objects.get(reqid=request), slice=InputRequestList.objects.get(id=slice))
        if SliceError.objects.filter(request=request, slice=slice).exists():
            slice_error = SliceError.objects.filter(request=request, slice=slice)[0]
        slice_error.exception_type = exception_type
        slice_error.message = message
        slice_error.exception_time = timezone.now()
        slice_error.is_active = True
        slice_error.save()
    except Exception as ex:
        _jsonLogger.warning('Slice error saving failed: {0}'.format(ex))


def set_analysis_request_hashtags(requestID, new_tasks):
    hashtags = [x.hashtag for x in HashTagToRequest.objects.filter(request_id=requestID)]
    official_hashtag = HashTag.objects.get(hashtag=OFFICIAL_HASHTAG)
    if official_hashtag not in hashtags:
        hashtags.append(official_hashtag)
    for task in new_tasks:
        production_task = ProductionTask.objects.get(id=task)
        for hashtag in hashtags:
            production_task.set_hashtag(hashtag.hashtag)


def check_request_status(production_request: TRequest):
     if production_request.cstatus in [TRequest.STATUS.WAITING, TRequest.STATUS.WORKING]:
         new_status = TRequest.STATUS.MONITORING
         slices = InputRequestList.objects.filter(request=production_request)
         for slice in slices:
            if slice.is_hide is None or slice.is_hide == False and not slice.tasks_in_slice().exists():
                new_status = TRequest.STATUS.WORKING
                break
         set_request_status('auto', production_request.reqid, new_status,
                            'Request status was changed to %s by %s' % (new_status, 'auto'),
                            'Request status is changed to %s by backend' % new_status, None)



def submit_analysis_slices(request):
    try:
        request_id = int(request.data['requestID'])
        if request_id < 1000:
            raise TRequest.DoesNotExist
        slices = request.data['slices']
        submitted_slices = []
        new_tasks = []
        production_request = TRequest.objects.get(reqid=request_id)
        if production_request.cstatus in [TRequest.STATUS.CANCELLED]:
            raise Exception('Request is cancelled')
        for slice_number in slices:
            slice = InputRequestList.objects.get(slice=slice_number, request_id=request_id)
            analysis_steps = list(AnalysisStepTemplate.objects.filter(request=request_id, slice=slice).order_by('id'))
            for step in analysis_steps:
                step.status = AnalysisStepTemplate.STATUS.APPROVED
                prod_step = step.step_production_parent
                prod_step.status = StepExecution.STATUS.APPROVED
                step.save()
                prod_step.save()
                try:
                    # from atlas.settings.local import FIRST_ADOPTERS
                    # if (request.user.username in FIRST_ADOPTERS):
                        _jsonLogger.info('Submit analysis task for slice',
                            extra=form_json_request_dict( request_id, request, extra={'slice': slice.slice}))
                        new_tasks += create_analy_task_for_slice(request_id, slice.slice, request.user.username)
                        submitted_slices.append(slice)
                        unset_slice_error(request.data['requestID'], slice.id)
                except Exception as e:
                    _jsonLogger.error(f'Submit analysis task for slice failed {e}',
                                      extra=form_json_request_dict(request_id, request, extra={'slice': slice.slice}))
                    step.status = AnalysisStepTemplate.STATUS.NOT_CHECKED
                    prod_step = step.step_production_parent
                    prod_step.status = StepExecution.STATUS.NOT_CHECKED
                    step.save()
                    prod_step.save()
                    set_slice_error(request_id, slice.id, 'default', str(e))
        if new_tasks:
            set_analysis_request_hashtags(request_id, new_tasks)
        check_request_status(production_request)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return Response({'result':f"{len(slices)} submitted"}, status=status.HTTP_200_OK)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def save_template_changes(request):
    try:
        _jsonLogger.info('Save template changes',extra={'user':request.user.username, 'template_id':request.data['templateID']})
        template_id= request.data['templateID']
        if request.data['templateBase'] is not None:
            AnalysisTaskTemplate.objects.filter(tag=template_id).update(**request.data['templateBase'])
        template = AnalysisTaskTemplate.objects.filter(tag=template_id).last()
        changes = request.data['params']
        for key in changes['removedFields']:
            template.task_parameters.pop(key)
        for key, value in changes['changes'].items():
            template.task_parameters[key] = value
        template.save()
        return Response(status=status.HTTP_200_OK)
    except AnalysisTaskTemplate.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)
def key_type_from_dict(original_dict, typed_result):

    for key, value in original_dict.items():
        if key not in typed_result:
            typed_result[key] = {}
            typed_result[key]['type'] = set()
            typed_result[key]['nullable'] = False
        if value is None:
            typed_result[key]['nullable'] = True
        if isinstance(value, dict):
            raise Exception(f'Nested dictionaries are not supported {key}')
        elif isinstance(value, list):
            raise Exception('Nested lists are not supported')
        elif isinstance(value, str):
            typed_result[key]['type'].add('str')
        elif isinstance(value, bool):
            typed_result[key]['type'].add('bool')
        elif isinstance(value, int):
            typed_result[key]['type'].add('int')
        elif isinstance(value, float):
            typed_result[key]['type'].add('float')
        else:
            typed_result[key]['type'].add('str')

def get_keys_types_from_task_parametrers(keys_types: dict, array_keys: dict, dict_keys:dict, mse: dict, task_parameters: dict):
    for key, value in task_parameters.items():
        if key != 'multiStepExec':
            if key not in keys_types:
                keys_types[key] = {}
                keys_types[key]['type'] = set()
                keys_types[key]['nullable'] = False

            if value is None:
                keys_types[key]['nullable'] = True

            if isinstance(value, dict):
                if key not in dict_keys:
                    dict_keys[key] = {}
                key_type_from_dict(value, dict_keys[key])
            elif isinstance(value, list):
                if key not in array_keys:
                    array_keys[key] = {}
                for item in value:
                    if isinstance(item, dict):
                        key_type_from_dict(item, array_keys[key])
            else:
                key_type_from_dict({key: value}, keys_types)
        elif key == 'multiStepExec':
                get_keys_types_from_task_parametrers(mse[0], mse[1], mse[2], {}, value)

def modify_analysis_step_input_source(request_id: int, slice: int, new_input_task_id: int):
    analy_step = AnalysisStepTemplate.objects.get(request=request_id, slice=InputRequestList.objects.get(slice=slice, request=request_id))
    task = TTask.objects.get(id=new_input_task_id)
    analy_step.step_parameters['buildSpec']['archiveName'] = task.jedi_task_parameters['buildSpec']['archiveName']
    analy_step.step_parameters['sourceURL'] = task.jedi_task_parameters['sourceURL']
    analy_step.save()

def simple_analy_slice_clone(request_id: int, slice_number: int) -> int:
    try:
        new_slice = InputRequestList.objects.get(request_id=request_id, slice=slice_number)
        original_slice = InputRequestList.objects.get(request_id=request_id, slice=slice_number)
        step_exec = StepExecution.objects.get(slice=original_slice, request=request_id)
        analy_step = AnalysisStepTemplate.objects.get(step_production_parent=step_exec)
        new_slice.id = None
        new_slice.slice = TRequest.objects.get(reqid=request_id).get_next_slice()
        new_slice.save()
        if step_exec.step_parent == step_exec:
            step_exec.step_parent = None
        step_exec.id = None
        step_exec.slice = new_slice
        if step_exec.status == StepExecution.STATUS.APPROVED:
            step_exec.status = StepExecution.STATUS.NOT_CHECKED
        step_exec.save()
        analy_step.id = None
        analy_step.slice = new_slice
        analy_step.step_production_parent = step_exec
        if analy_step.status == AnalysisStepTemplate.STATUS.APPROVED:
            analy_step.status = AnalysisStepTemplate.STATUS.NOT_CHECKED
        analy_step.save()
        return new_slice.slice

    except Exception as e:
        raise Exception(f'Failed to clone slice {slice} of request {request_id}: {str(e)}')


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_task_preview(request):
    try:
        request_id = int(request.query_params.get('requestID'))
        if request_id < 1000:
            raise TRequest.DoesNotExist
        slice = int(request.query_params.get('sliceNumber'))
        step = AnalysisStepTemplate.objects.filter(request=request_id,
                                                    slice=InputRequestList.objects.get(slice=slice, request=request_id)).last()
        if step.step_production_parent.step_parent and step.step_production_parent.step_parent != step.step_production_parent:
            input_tasks = ProductionTask.objects.filter(step=step.step_production_parent.step_parent)
            pattern = None
            parent_tasks = []
            for input_task in input_tasks:
                if input_task.status not in ProductionTask.BAD_STATUS:
                    for dataset in input_task.output_non_log_datasets():
                        if step.slice.dataset == get_container_name(dataset):
                            if not pattern:
                                step.change_variable(TemplateVariable.KEY_NAMES.INPUT_BASE, dataset)
                                pattern = step.render_task_template()
                            parent_tasks.append(input_task.id)

            return Response(json.dumps({'parent_tasks_id':parent_tasks, 'rendered_first_task':step.render_task_template()}, indent=4, sort_keys=True))
        return Response(json.dumps(step.render_task_template(), indent=4, sort_keys=True))
    except (TRequest.DoesNotExist, InputRequestList.DoesNotExist, AnalysisStepTemplate.DoesNotExist):
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_pattern_view(request):
    try:
        tag = request.query_params.get('tag')
        template = AnalysisTaskTemplate.objects.filter(tag=tag).last()
        return Response(json.dumps(template.task_parameters, indent=4, sort_keys=True))
    except (TRequest.DoesNotExist, InputRequestList.DoesNotExist, AnalysisStepTemplate.DoesNotExist):
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)



@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_derivation_slices(request):
    try:
        request_id = int(request.query_params.get('requestID'))
        if request_id < 1000:
            raise TRequest.DoesNotExist
        slices = InputRequestList.objects.filter(request=request_id)
        result_slices = []
        output_formats = set()
        for slice in slices:
            if slice.is_hide is None or slice.is_hide == False:
                step = StepExecution.objects.filter(slice=slice, request=request_id).last()
                if step and step.status == StepExecution.STATUS.APPROVED and ProductionTask.objects.filter(step=step).exists():
                    for dataset in ProductionTask.objects.filter(step=step).last().output_non_log_datasets():
                        result_slices.append({'slice': slice.slice, 'outputFormat': dataset.split('.')[-2], 'container': get_container_name(dataset)})
                        output_formats.add(dataset.split('.')[-2])
        return Response({'slices': result_slices, 'outputFormats': list(output_formats)})
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_request_hashtags(request):
    try:
        request_id = int(request.query_params.get('requestID'))
        if request_id < 1000:
            raise TRequest.DoesNotExist
        hashtags = [x.hashtag.hashtag for x in HashTagToRequest.objects.filter(request_id=request_id)]
        if OFFICIAL_HASHTAG in hashtags:
            hashtags.pop(hashtags.index(OFFICIAL_HASHTAG))
        return Response(hashtags)
    except (TRequest.DoesNotExist, InputRequestList.DoesNotExist, AnalysisStepTemplate.DoesNotExist):
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def all_groups() -> [str]:
    if cache.get('all_group_scopes'):
        return cache.get('all_group_scopes')
    groups = []
    for group in Group.objects.all():
        if group.name.startswith('IAM:atlas/') and group.name.endswith('/production') and 'phys' in group.name:
            groups.append(f'group.{group.name.split("/")[-2]}')
    groups.sort()
    cache.set('all_group_scopes', groups, 3600*5*24)
    return groups
def scopes_by_user(username: str) -> [str]:
    scopes = [f'user.{username}']
    for group in User.objects.get(username=username).groups.all():
        if group.name.startswith('IAM:atlas/') and group.name.endswith('/production'):
            scopes.append(f'group.{group.name.split("/")[-2]}')
    return scopes + [scope for scope in all_groups() if scope not in scopes]



@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_scopes_by_user(request):
    try:

        username = request.user.username
        groups = scopes_by_user(username)
        return Response(groups)
    except (TRequest.DoesNotExist, InputRequestList.DoesNotExist, AnalysisStepTemplate.DoesNotExist):
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)