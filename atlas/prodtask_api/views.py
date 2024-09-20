
import json
import logging
from dataclasses import asdict
from time import sleep

from django.http.response import HttpResponseForbidden, HttpResponseBadRequest
from rest_framework import status, serializers
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser

from atlas.atlaselastic.monit_views import prepare_staging_task_info
from atlas.cric.client import CRICClient
from atlas.dkb.views import datasets_by_campaign
from atlas.prestage.views import staging_rule_verification
from atlas.prodtask.dataset_recovery import get_unavalaible_daod_input_datasets, TaskDatasetRecover, \
    register_recreation_request, get_unavaliaible_dataset_info, submit_dataset_recovery_requests
from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import TRequest, InputRequestList, StepExecution, DatasetStaging, \
    ProductionRequestSerializer, RequestStatus, TTask, ProductionTask, JediTasks, DatasetRecovery, DatasetRecoveryInfo
from atlas.prodtask.patch_reprocessing import clone_fix_reprocessing_task, find_reprocessing_to_fix, \
    ReprocessingTaskFix, patched_containers
from atlas.prodtask.spdstodb import fill_template
from atlas.prodtask.step_manage_views import recreate_output
from atlas.prodtask.views import form_existed_step_list, set_request_status
from atlas.task_action.task_management import TaskManagementAuthorisation

_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')



def change_step_from_dict(step, changes_dict):
    if step.task_config:
        task_config = json.loads(step.task_config)
    else:
        task_config = {}
    change_project_mode = {}
    remove_project_mode =[]
    if 'update_project_mode' in changes_dict:
        change_project_mode = changes_dict.pop('update_project_mode')
    if 'remove_project_mode' in changes_dict:
        remove_project_mode = changes_dict.pop('remove_project_mode')
    for x in StepExecution.TASK_CONFIG_PARAMS:
        if x in changes_dict:
            if changes_dict[x] and x in StepExecution.INT_TASK_CONFIG_PARAMS:
                task_config[x] = int(changes_dict[x])
            else:
                task_config[x] = changes_dict[x]
    step.set_task_config(task_config)
    for project_mode_addition in change_project_mode:
        step.update_project_mode(project_mode_addition,change_project_mode[project_mode_addition])
    for project_mode_key in remove_project_mode:
        step.remove_project_mode(project_mode_key)
    change_template = False
    ctag = step.step_template.ctag
    if 'ami_tag' in changes_dict:
        ctag = changes_dict['ami_tag']
        change_template = True
    output_formats = step.step_template.output_formats
    if 'output_formats' in changes_dict:
        output_formats = changes_dict['output_formats']
        change_template = True

    if change_template:
        step.step_template = fill_template(step.step_template.step, ctag, step.step_template.priority,
                                                 output_formats, step.step_template.memory)
    if 'priority' in changes_dict:
        step.priority = step['priority']


def clone_pattern_slice(production_request, pattern_request, pattern_slice, steps, slice_dict):
    production_request = TRequest.objects.get(reqid=production_request)
    original_slice = InputRequestList.objects.filter(request=pattern_request,slice=pattern_slice)
    original_steps = StepExecution.objects.filter(slice=original_slice[0],request=pattern_request).order_by('id')
    ordered_existed_steps, parent_step = form_existed_step_list(original_steps)
    clone_slice = False
    for step in ordered_existed_steps:
            if step.step_template.step in steps:
                clone_slice = True
                break
    if clone_slice:
        new_slice = list(original_slice.values())[0]
        new_slice_number = InputRequestList.objects.filter(request=production_request).count()
        del new_slice['id']
        del new_slice['request_id']
        for key in slice_dict:
            if key not in ['id','request_id']:
                new_slice[key] = slice_dict[key]
        new_slice['slice'] = new_slice_number
        new_slice['request'] = production_request
        new_input_data = InputRequestList(**new_slice)
        new_input_data.save()
        original_steps = StepExecution.objects.filter(slice=original_slice[0],request=pattern_request).order_by('id')
        ordered_existed_steps, parent_step = form_existed_step_list(original_steps)
        parent_step = None
        for index, step in enumerate(ordered_existed_steps):
                if step.step_template.step in steps:
                    step.id = None
                    step.step_appr_time = None
                    step.step_def_time = None
                    step.step_exe_time = None
                    step.step_done_time = None
                    step.slice = new_input_data
                    step.request = production_request
                    change_step_from_dict(step, steps[step.step_template.step])
                    step.status = 'Approved'
                    if parent_step:
                        step.step_parent = parent_step
                    step.save_with_current_time()
                    if not parent_step:
                        step.step_parent = step
                        step.save()
                    parent_step = step
        return new_slice_number
    else:
        return None


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def create_slice(request):
    """
    Create a slice based on a pattern. User has to be the "production_request" owner. Steps should contain dictionary of step
    names which should be copied from the pattern slice as well as modified fields, e.g. {'Simul':{'container_name':'some.container.name'}}
       :param production_request: Prodcution request ID. Required
       :param pattern_slice: Pattern slice number. Required
       :param pattern_request: Pattern slice number. Required
       :param steps: Dictionary of steps to be copied from pattern slice. Required
       :param slice: Dictionary of parameters to be changed in a slice. optional

    """


    try:
        data = request.data
        production_request = TRequest.objects.get(reqid=data['production_request'])
        if request.user.username != production_request.manager:
            return HttpResponseForbidden()

        pattern_slice = int(data['pattern_slice'])
        pattern_request = int(data['pattern_request'])
        steps = data['steps']
        slice_dict = data.get('slice')
        new_slice_number = None
        if InputRequestList.objects.filter(request=production_request).count()<1000:
            new_slice_number = clone_pattern_slice(production_request.reqid, pattern_request, pattern_slice, steps, slice_dict)
            if new_slice_number and (production_request.cstatus not in ['test', 'approved']):
                set_request_status('cron',production_request.reqid,'approved','Automatic approve by api', 'Request was automatically extended')
    except Exception as e:
        return HttpResponseBadRequest(e)
    return Response({'slice': new_slice_number,'request':production_request.reqid})






@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def test_api(request):
    """
        Return data which was sent or error
       :param any valid json


    """

    return Response({'user':request.user.username,'data': request.data})


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def is_stage_rule_stuck_because_of_tape(request):
    """
        Check that "dataset" is stuck for "days" due to a tape problem\n
       * dataset: dataset name. Required\n
       * days: Days request should not being updated to be stuck, 7 is minimum, 10 is default. optional

    """
    try:
        dataset = request.query_params.get('dataset')
        days = int(request.query_params.get('days', 10))
        if not dataset:
            raise ValueError('dataset name is required')
        if not DatasetStaging.objects.filter(dataset=dataset).exists():
            if ':' not in dataset:
                if DatasetStaging.objects.filter(dataset=dataset.split('.')[0]+':'+dataset).exists():
                    dataset = dataset.split('.')[0]+':'+dataset
                else:
                    raise  ValueError(f'Staging for {dataset} is not found')
        if days < 7:
            raise ValueError('7 days is a minimum')
        stuck, result = staging_rule_verification(dataset, days)
        _jsonLogger.info("Check staging rule to be stuck",
                         extra={'dataset': dataset, 'days': days, 'result': str((stuck, result))})
        return Response({'dataset': dataset, 'days': days, 'stuck': stuck, 'tape_errors': result})
    except Exception as e:
        _jsonLogger.error(f"Check staging rule to be stuck failed {e}")
        return HttpResponseBadRequest(f"Check staging rule to be stuck failed {e}")

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def dsid_info(request):
    """
        Return information about dataset by dsid\n
       * dsid: dataset id. Required\n

    """
    try:
        dsid = request.query_params.get('dsid')

        return Response(datasets_by_campaign(dsid))
    except Exception as e:
        _jsonLogger.error(f"Taken dsid info error {e}")
        return HttpResponseBadRequest(f"Taken dsid info error {e}")

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def recreate_delete_dataset(request):

    try:
        dataset = request.query_params.get('dataset')
        if not dataset:
            raise ValueError('dataset name is required')
        ddm = DDM()
        if ddm.dataset_exists(dataset):
            raise ValueError(f'dataset {dataset} still exists')
        task_id = int(dataset[dataset.rfind('tid')+3:dataset.rfind('_')])
        output_type = dataset.split('.')[-2]
        result_task_id, result_dataset = recreate_output(task_id, output_type)
        return Response({'dataset': result_dataset, 'task_id': result_task_id})
    except Exception as e:
        error_message = f"Recreate deleted dataset failed {e}"
        _jsonLogger.error(error_message)
        return HttpResponseBadRequest(error_message)


def deft_legacy_request_api_data(request_id: int) -> dict:
    production_request = TRequest.objects.get(reqid=request_id)
    request_data = ProductionRequestSerializer(production_request).data
    request_data['reference'] = request_data.pop('jira_reference')
    request_data.pop('is_fast', None)
    request_data.pop('info_fields', None)

    request_data['id'] = request_data.pop('reqid')

    first_created_status = RequestStatus.objects.filter(request=request_id).order_by('timestamp')
    if first_created_status:
        request_data['creation_time'] = first_created_status[0].timestamp.strftime('%d-%m-%Y %H:%M:%S')
    last_approved_status = RequestStatus.objects.filter(request=request_id, status='approved').order_by(
        '-timestamp')
    if last_approved_status:
        request_data['approval_time'] = last_approved_status[0].timestamp.strftime('%d-%m-%Y %H:%M:%S')
    try:
        evgen_steps = []
        input_slices = InputRequestList.objects.filter(request=request_id).order_by('slice')
        for input_slice in input_slices:
            try:
                if input_slice.input_data and not input_slice.is_hide:
                    if '/' not in input_slice.input_data:
                        dsid = int(input_slice.input_data.split('.')[1])
                        brief = input_slice.input_data.split('.')[2]
                    else:
                        dsid = int(input_slice.input_data.split('/')[0])
                        brief = input_slice.input_data.split('/')[1].split('.')[1]
                    evgen_request_steps = StepExecution.objects.filter(request=request_id,
                                                               step_template__step__iexact='evgen',
                                                               slice__slice=input_slice.slice)
                    if evgen_request_steps:
                        for evgen_step in evgen_request_steps:
                            evgen_steps.append({'dsid': dsid,
                                                'brief': brief,
                                                'input_events': evgen_step.input_events,
                                                'jo': evgen_step.slice.input_data,
                                                'ctag': evgen_step.step_template.ctag,
                                                'slice': int(evgen_step.slice.slice)})
                    else:
                        evgen_steps.append({'dsid': dsid,
                                            'brief': brief,
                                            'jo': input_slice.input_data,
                                            'slice': int(input_slice.slice)})
            except Exception as ex:
                _logger.error(f'Exception occurred: {ex}')
    except Exception as ex:
        _logger.error(f'Exception occurred: {ex}')
        evgen_steps = None
    request_data['evgen_steps'] = evgen_steps
    return request_data

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def deft_legacy_request(request, request_id):
    """
        Return data which was sent or error
       :param any valid json


    """

    try:

        return Response(deft_legacy_request_api_data(int(request_id)))
    except Exception as e:
        error_message = f"DEFT request {e}"
        _jsonLogger.error(error_message)
        return HttpResponseBadRequest(error_message)


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def reprocessing_request_patch_info(request, requestID):
    try:
        pathed_tasks = patched_containers(requestID)
        selected_task = request.query_params.get('selectedTask')
        tasks_to_fix = find_reprocessing_to_fix(requestID, pathed_tasks)
        if selected_task:
            tasks_to_fix = [x for x in tasks_to_fix if x.original_task_id == int(selected_task)]
        return Response( {'request': ProductionRequestSerializer(TRequest.objects.get(reqid=requestID)).data,
                          'patchedTasks': pathed_tasks, "tasksToFix": [asdict(x) for x in tasks_to_fix]})
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def stage_profile(request, taskID):
    try:
        dataset = request.query_params.get('dataset')
        source = request.query_params.get('source')
        if dataset and source:
            result = prepare_staging_task_info(taskID, dataset, source)
        else:
            result = prepare_staging_task_info(taskID)


        return Response(asdict(result) )
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def patch_reprocessing_request(request, requestID):
    try:
        ami_tag = request.data.get('amiTag')
        tasks_to_patch = request.data.get('tasksToPatch')
        pathed_tasks = patched_containers(requestID)
        tasks_to_fix: [ReprocessingTaskFix] = find_reprocessing_to_fix(requestID, pathed_tasks)
        result = 0
        for task in tasks_to_fix:
            if task.original_task_id in tasks_to_patch:
                clone_fix_reprocessing_task(task, ami_tag)
                result += 1
        if result > 0:
            set_request_status('cron', requestID, 'approved', 'Reprocessing patch',
                               'Request was automatically approved')
        return Response(result)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def unavailable_datasets_info(request):
    try:
        dataset = request.query_params.get('dataset')
        task_id = request.query_params.get('taskID')
        username = request.query_params.get('username')
        result = []
        if dataset:
            result = get_unavaliaible_dataset_info(dataset)
        elif task_id:
            result = get_unavalaible_daod_input_datasets([int(task_id)])
        elif username:
            if username == 'all':

                tasks = JediTasks.objects.filter(id__gte=40700000, status__in=[ProductionTask.STATUS.PENDING, ProductionTask.STATUS.RUNNING], prodsourcelabel='user')
                tasks_id = [x.id for x in tasks if 'missing at online endpoints' in x.errordialog]
            else:
                tasks = JediTasks.objects.filter(username=username, status__in=[ProductionTask.STATUS.PENDING, ProductionTask.STATUS.RUNNING])
                tasks_id = [x.id for x in tasks if 'missing at online endpoints' in x.errordialog]
            result = get_unavalaible_daod_input_datasets(tasks_id)
        sites = set()
        for dataset_info in result:
            sites.update(dataset_info.replicas)
        cric_client = CRICClient()
        downtimes = [cric_client.get_ddm_endpoint_wan(x) for x in sites]

        return Response({'datasets': [asdict(x) for x in result], 'downtimes':downtimes} )
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def request_recreation(request):
    try:
        datasets_info: [TaskDatasetRecover] = request.data.get('datasets')
        username = request.user.username
        comment = request.data.get('comment')
        requests_registered = register_recreation_request([TaskDatasetRecover(**x) for x in datasets_info], username, comment)
        return Response(len(requests_registered))
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DatasetRecoverySerializer(serializers.ModelSerializer):
    class Meta:
        model = DatasetRecovery
        fields = '__all__'

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_all_recovery_requests(request):
    try:
        result = []
        requests = DatasetRecovery.objects.all()
        for recovery_request in requests:
            serialized_request = DatasetRecoverySerializer(recovery_request).data
            serialized_request['comment'] = ''
            serialized_request['error'] = ''
            serialized_request['containers'] = []
            if recovery_request.status != DatasetRecovery.STATUS.DONE:
                if DatasetRecoveryInfo.objects.filter(dataset_recovery=recovery_request).exists():
                    dataset_recovery_info = DatasetRecoveryInfo.objects.get(dataset_recovery=recovery_request)
                    serialized_request['error'] = dataset_recovery_info.error
                    if recovery_request.status not in [DatasetRecovery.STATUS.RUNNING, DatasetRecovery.STATUS.SUBMITTED]:
                        serialized_request['comment'] = dataset_recovery_info.info_obj.comment
                    if recovery_request.status in [DatasetRecovery.STATUS.DONE]:
                        serialized_request['containers'] = dataset_recovery_info.info_obj.containers
            result.append(serialized_request)

        return Response(result)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def submit_recreation(request):
    try:
        task_management = TaskManagementAuthorisation()
        user, allowed_groups = task_management.task_user_rights(request.user.username)
        if request.user.is_superuser or 'DPD' in allowed_groups:
            ids: [int] = map(lambda x: int(x), request.data.get('IDs'))
            requests_submitted = submit_dataset_recovery_requests(ids)
            return Response(len(requests_submitted))
        else:
            return Response('User is not allowed to submit requests', status=status.HTTP_403_FORBIDDEN)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)