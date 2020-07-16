import logging
from _ast import In

from django.http.response import HttpResponseForbidden
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser

from atlas.prodtask.models import TRequest, InputRequestList, StepExecution
from atlas.prodtask.views import form_existed_step_list

_logger = logging.getLogger('prodtaskwebui')


TEST_PATTERN_REQUEST = 29269

def clone_pattern_slice(production_request, pattern_request, pattern_slice, slice, steps):
    original_slice = InputRequestList.objects.filter(request=pattern_request,slice=pattern_slice)
    new_slice = list(original_slice.values())[0]
    new_slice_number = InputRequestList.objects.filter(request=production_request).count()
    new_slice['slice'] = new_slice_number
    del new_slice['id']
    del new_slice['request_id']
    new_slice['request'] = production_request
    for key in slice:
        new_slice[key] = slice[key]
    new_input_data = InputRequestList(**new_slice)
    new_input_data.save()
    original_steps = StepExecution.objects.filter(slice=original_slice,request=pattern_request)
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
                for key in steps[step.step_template.step]:
                    pass
                if (step.status == 'Skipped'):
                    step.status = 'NotCheckedSkipped'
                elif step.status in ['Approved', 'Waiting']:
                    step.status = 'NotChecked'
                if parent_step:
                    step.step_parent = parent_step
                step.save_with_current_time()
                if not parent_step:
                    step.step_parent = step
                    step.save()


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def create_slice(request):
    """
    Create a slice based on a pattern. User has to be the "production_request_ owner. Steps should contain list of step
    name which should be copied from pattern slice as well as modified fields, e.g. [{'step':'simul','container_name':'some.container.name'}]
       :param production_request: Prodcution request ID. Required
       :param pattern_slice: Pattern slice number. Required
       :param steps: List of steps to be copied from pattern slice. required

    """

    return HttpResponseForbidden()

    data = request.data
    production_request = TRequest.objects.get(reqid=data.get('production_request'))

    if request.user.username != production_request.manager:
        return HttpResponseForbidden()




@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def test_api(request):
    """
        Return data which was sent or error
       :param any valid json


    """

    data = request.data

    return Response({'user':request.user.username,'data':data})