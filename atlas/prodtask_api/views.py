
import json
import logging

from django.http.response import HttpResponseForbidden, HttpResponseBadRequest
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser

from atlas.prestage.views import staging_rule_verification
from atlas.prodtask.models import TRequest, InputRequestList, StepExecution, DatasetStaging
from atlas.prodtask.spdstodb import fill_template
from atlas.prodtask.views import form_existed_step_list, set_request_status

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