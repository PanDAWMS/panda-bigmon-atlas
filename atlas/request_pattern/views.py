import logging

from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from django.shortcuts import render
import json
from atlas.prodtask.models import StepTemplate, StepExecution, InputRequestList, TRequest, MCPattern, ProductionTask, \
    get_priority_object, ProductionDataset, RequestStatus, get_default_project_mode_dict, get_default_nEventsPerJob_dict, \
    OpenEndedRequest, TrainProduction, ParentToChildRequest, TProject
from atlas.prodtask.views import form_existed_step_list, clone_slices
from django.contrib.auth.decorators import login_required

from atlas.settings import OIDC_LOGIN_URL

_logger = logging.getLogger('prodtaskwebui')


@login_required(login_url=OIDC_LOGIN_URL)
def index(request):
    if request.method == 'GET':

        return render(request, 'request_pattern/_index_request_pattern.html', {
                'active_app': 'request_pattern',
                'pre_form_text': 'MC pattern',
                'title': 'MC pattern',
                'parent_template': 'prodtask/_index.html',
            })



def pattern_total_list(all=False):
    result = []
    task_configs = {}
    patterns = list(InputRequestList.objects.filter(request=29269).order_by('slice'))
    steps = list(StepExecution.objects.filter(request=29269).order_by('id'))
    for step in steps:
        ctag = step.get_task_config('tag')
        if ctag == 'x9999':
            ctag = ''
        task_configs[int(step.slice_id)] = task_configs.get(int(step.slice_id),[]) + [ctag]
    for pattern in patterns[1:]:
        if all or (not pattern.is_hide):
            result.append({'slice':int(pattern.slice),'name':pattern.brief,'tags':task_configs[int(pattern.id)],
                           'obsolete':pattern.is_hide or False })
    result.sort(key=lambda x: x['name'])
    return result


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def pattern_list_with_obsolete(request):
    return Response(pattern_total_list(True))


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def pattern_list(request):
    return Response(pattern_total_list())


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def clone_pattern(request):
    try:
        origin_slice = request.data['slice']
        new_name = request.data['new_name']
        new_slice_id = clone_slices(29269,29269,[int(origin_slice)],-1,False)[0]
        new_slice = InputRequestList.objects.get(request=29269,slice=new_slice_id)
        new_slice.brief = new_name
        new_slice.save()
        result = {'new_pattern': new_slice_id}

    except Exception as e:
        _logger.error("Problem with pattern cloning %s" % str(e))
        return Response({'error': str(e)}, status=400)
    return Response(result)


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def slice_pattern_steps(request,slice):
    pattern = InputRequestList.objects.get(request=29269, slice=slice)
    existed_steps = StepExecution.objects.filter(request=29269, slice=pattern)
    # Check steps which already exist in slice, and change them if needed
    ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
    result_list = []
    for index, step in enumerate(ordered_existed_steps):
        task_config = json.loads(step.task_config)
        ctag = task_config.get('tag','')
        if not ctag and step.step_template.ctag == 'x9999':
            ctag = ''
        result_list.append(
            {'tag': ctag, 'id':step.id, 'step_name': step.step_template.step,
             'nEventsPerJob': task_config.get('nEventsPerJob', ''),
             'nEventsPerInputFile': task_config.get('nEventsPerInputFile', ''),
             'project_mode': task_config.get('project_mode', ''),
             'input_format': task_config.get('input_format', ''),
              'output_formats': task_config.get('output_formats', ''),
              'merging_tag': task_config.get('merging_tag', ''),
             'nFilesPerMergeJob': task_config.get('nFilesPerMergeJob', ''),
             'nGBPerMergeJob': task_config.get('nGBPerMergeJob', ''),
             'nMaxFilesPerMergeJob': task_config.get('nMaxFilesPerMergeJob', ''),
             'nFilesPerJob': task_config.get('nFilesPerJob', ''), 'nGBPerJob': task_config.get('nGBPerJob', ''),
             'maxFailure': task_config.get('maxFailure', ''),
             'nEventsPerMergeJob': task_config.get('nEventsPerMergeJob', ''),
             'PDA': task_config.get('PDA', ''),
             'PDAParams': task_config.get('PDAParams', ''),'container_name':task_config.get('container_name',''),
             'onlyTagsForFC':task_config.get('onlyTagsForFC',None)
             })

    return Response({'steps':result_list,'pattern_name':pattern.brief,'pattern_in_use':not(pattern.is_hide or False)})

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def slice_pattern_save_steps(request,slice):
    result = {'sucess':True}
    CHANGABLE = ['input_format', 'output_formats', 'tag','nEventsPerJob','project_mode','nFilesPerJob','nGBPerJob','maxFailure','container_name','onlyTagsForFC']
    slice = InputRequestList.objects.get(request=29269, slice=slice)
    if request.data['pattern_in_use']:
        slice.is_hide = False
    else:
        slice.is_hide = True
    if request.data['pattern_name'] != slice.brief:
        slice.brief = request.data['pattern_name']
    slice.save()
    try:
        steps = request.data['steps']
        step_dict = {}
        for step in steps:
            step_dict[int(step['id'])] = step
        steps = StepExecution.objects.filter(request=29269, slice=slice)
        message = []
        for step in steps:
            if int(step.id) in step_dict:
                step_changed = False
                for x in CHANGABLE:
                    if step.get_task_config(x) != step_dict[int(step.id)][x]:
                        message.append('{'+str(x) + ':' + str(step.get_task_config(x))+'} => {'+str(x) + ':' + str(step_dict[int(step.id)][x])+'}')
                        if step_dict[int(step.id)][x] is None:
                            step.set_task_config({x:''})
                        else:
                            step.set_task_config({x:step_dict[int(step.id)][x]})
                        step_changed = True
                if (not step.get_task_config('container_name')) and (step.get_task_config('onlyTagsForFC') is not None):
                    step.remove_task_config('onlyTagsForFC')
                    step_changed = True
                if step_changed:
                    _logger.info('Pattern %i changed: %s'%(int(slice.slice),' '.join(message)))
                    step.save()

    except Exception as e:
        _logger.error("Problem with pattern saving %s" % str(e))
        return Response({'error': str(e)}, status=400)
    return Response(result)


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def slice_pattern(request,slice):
    pattern = InputRequestList.objects.get(request=29269, slice=slice)

    result  = pattern.brief

    return Response(result)