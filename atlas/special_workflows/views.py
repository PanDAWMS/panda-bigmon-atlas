import logging
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response

from django.shortcuts import render
import json

from atlas.messaging.consumer import Payload
from atlas.prodtask.models import StepTemplate, StepExecution, InputRequestList, TRequest, MCPattern, ProductionTask, \
    get_priority_object, ProductionDataset, RequestStatus, get_default_project_mode_dict, get_default_nEventsPerJob_dict, \
    OpenEndedRequest, TrainProduction, ParentToChildRequest, TProject
from atlas.prodtask.views import form_existed_step_list, clone_slices, set_request_status
from django.contrib.auth.decorators import login_required
from rest_framework import status
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.parsers import JSONParser
from rest_framework.decorators import parser_classes


_logger = logging.getLogger('prodtaskwebui')

@login_required(login_url='/prodtask/login/')
def index(request):
    if request.method == 'GET':

        return render(request, 'special_workflows/_index_special_workflows.html', {
                'active_app': 'special_workflows',
                'pre_form_text': 'Special workflows',
                'title': 'Special workflows',
                'parent_template': 'prodtask/_index.html',
            })

@api_view(['GET'])
def idds_postproc(request,production_request):
    slices = InputRequestList.objects.filter(request=production_request).order_by('-slice')
    last_slice = None
    for slice in slices:
        if not slice.is_hide:
            last_slice = slice
            break

    step = StepExecution.objects.get(request=production_request, slice=last_slice)
    # Check steps which already exist in slice, and change them if needed
    step_submitted = step.status in StepExecution.STEPS_APPROVED_STATUS
    terminations = step.get_task_config('terminations')
    if not terminations:
        terminations = [{'name':'','comparison':'gt','value':0}]

    return Response({'step':{'submitted':step_submitted,'ami_tag':step.step_template.ctag,'step_id':step.id},
                     'outputPostProcessing':step.get_task_config('outputPostProcessing'),
                     'template_input':step.get_task_config('template_input'),'terminations':terminations})


@api_view(['GET'])
def idds_tasks(request,production_request):
    slices = InputRequestList.objects.filter(request=production_request).order_by('slice')
    tasks = []
    for slice in slices:
        if not slice.is_hide:
            step = StepExecution.objects.get(request=production_request, slice=slice)
            if ProductionTask.objects.filter(step=step,request=production_request).exists():
                task = ProductionTask.objects.get(step=step,request=production_request)
                parameter = ''
                value = ''
                if step.get_task_config('template_input'):
                    parameter = step.get_task_config('template_input')['name']
                    value = step.get_task_config('template_input')['value']
                tasks.append({'id':task.id,'status':task.status,'parameter':parameter,'value':value ,'started':task.submit_time})
    return Response({'tasks':tasks})

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def request_results(request, production_request):
    """
    Returns list of datasets for the last step of the chain with a status
    :param production_request: Production request id
    :return: List of dict {chain_input: ,outputs:,  tasks: }
    """
    try:
        slices = InputRequestList.objects.filter(request=production_request).order_by('slice')
        result = []
        for slice in slices:
            if not slice.is_hide and StepExecution.objects.filter(request=production_request, slice=slice).exists():
                steps,_ = form_existed_step_list(StepExecution.objects.filter(request=production_request, slice=slice))
                step = steps[-1]
                outputs = step.step_template.output_formats
                result_tasks = []
                if ProductionTask.objects.filter(step=steps[-1]).exists():
                    for task in ProductionTask.objects.filter(step=steps[-1]):
                            datasets = []
                            if task.status not in ProductionTask.RED_STATUS:
                                for dataset in list(ProductionDataset.objects.filter(task_id=task.id)):
                                    if ('log' not in dataset.name) and ('LOG' not in dataset.name):
                                        datasets.append(dataset.name)
                            result_tasks.append({'task_id':task.id,'status':task.status, 'datasets': datasets})
                result.append({'chain_input':slice.input_data,'outputs':outputs,'tasks':result_tasks})
    except Exception as e:
        return Response('Problem %s'%str(e), status.HTTP_400_BAD_REQUEST)
    return Response(result)

@api_view(['GET'])
def idds_get_patterns(request):
    slices = InputRequestList.objects.filter(request=30687).order_by('slice')
    patterns = []
    for slice in slices:
        if not slice.is_hide:
            step = StepExecution.objects.get(request=30687, slice=slice)
            patterns.append({'name':slice.brief,'outputPostProcessing':step.get_task_config('outputPostProcessing'),
                     'template_input':step.get_task_config('template_input'),'terminations':step.get_task_config('terminations')})
    return Response({'patterns':patterns})


@api_view(['POST'])
def idds_postproc_save(request,step_id):
    try:
        step = StepExecution.objects.get(id=step_id)
        outputPostProcessing = request.data['outputPostProcessing']
        template_input = request.data['template_input']
        terminations = []
        for termination in request.data['terminations']:
            if termination.get('name'):
                terminations.append(termination)
        step.set_task_config({'outputPostProcessing':outputPostProcessing})
        step.set_task_config({'template_input':template_input})
        if terminations:
            step.set_task_config({'terminations': terminations})
        step.save()
    except Exception as e:
        _logger.error("Problem with saving idds postprocessing %s" % str(e))
        return Response({'error': str(e)}, status=400)
    return Response({'sucess':True})


def idds_action_on_message(task_id, output):
    try:

        task = ProductionTask.objects.get(id = task_id)
        step = task.step
        terminations = step.get_task_config('terminations')
        to_continue = True
        _logger.info("Extending active learning task: %s %s" %(task_id, str(output)))
        for condition in terminations:
            if condition['name'] in output:
                if condition['comparison'] == 'gt':
                    to_continue &= (condition['value'] > output[condition['name']])
                if condition['comparison'] == 'lte':
                    to_continue &= (condition['value'] <= output[condition['name']])
            else:
                to_continue = False
                break
        if to_continue:
                template_input = step.get_task_config('template_input')
                template_input['value'] = output[template_input['name']]
                slice = step.slice
                new_slice = clone_slices(step.request_id, step.request_id, [slice.slice], -1, False)[0]
                new_step = StepExecution.objects.get(slice=InputRequestList.objects.get(request=step.request_id,slice=new_slice))
                new_step.set_task_config({'template_input':template_input})
                new_step.status = 'Approved'
                new_step.save()
                production_request = step.request
                if production_request.cstatus != 'test':
                    set_request_status('auto', production_request.reqid, 'approved', 'Automatic idds approve',
                                       'Request was automatically approved')
        else:
            _logger.info("Finish workflow for task: %s" % (task_id))
    except Exception as e:
        _logger.error("Problem during action applying: %s" % e)


def idds_recive_message(payload: Payload) -> None:

    #_logger.info( str(payload.body))
    try:
        if payload.body['msg_type'] ==  "collection_activelearning":
            task_id = payload.body['workload_id']
            output = payload.body['output']
            idds_action_on_message(int(task_id),output)
        payload.ack()
    except Exception as e:
        _logger.error("Problem during iDDS message consumption: %s"%e)
        payload.nack()