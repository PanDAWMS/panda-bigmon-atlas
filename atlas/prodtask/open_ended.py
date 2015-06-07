import json
import logging

from django.http import HttpResponse, HttpResponseRedirect

from django.views.decorators.csrf import csrf_protect
from time import sleep
from copy import deepcopy
from atlas.prodtask.ddm_api import DDM
from ..prodtask.models import RequestStatus
from ..prodtask.spdstodb import fill_template
from ..prodtask.request_views import clone_slices, set_request_status
from ..prodtask.helper import form_request_log
from ..prodtask.task_actions import do_action
from .views import form_existed_step_list, form_step_in_page, fill_dataset

from .models import StepExecution, InputRequestList, TRequest, OpenEndedRequest

_logger = logging.getLogger('prodtaskwebui')

@csrf_protect
def close_open_ended(request,reqid):
    if request.method == 'POST':
        try:
            open_ended_requests = OpenEndedRequest.objects.filter(request=reqid,status='open')
            for open_ended_request in open_ended_requests:
                open_ended_request.status = 'closed'
                open_ended_request.save()
        except Exception,e:
           return HttpResponse(json.dumps({'success': False,'message':str(e)}), content_type='application/json')
        return  HttpResponse(json.dumps({'success':True}), content_type='application/json')

@csrf_protect
def make_open_ended(request,reqid):
    if request.method == 'POST':
        # check that production request is not open ended
        try:
            if OpenEndedRequest.objects.filter(request=reqid).exists():
                return  HttpResponse(json.dumps({'success': False,'message':'Already open ended'}), content_type='application/json')
            #Make open ended from slice 0
            slices = list(InputRequestList.objects.filter(request=reqid))
            if not slices:
                HttpResponse(json.dumps({'success': False,'message':'No input'}), content_type='application/json')

            first_slice = slices[0]

            # Skip all steps in zero slice
            steps = StepExecution.objects.filter(request=reqid,slice=first_slice)
            for step in steps:
                step.status = 'Skipped'
                step.save()

            #Register open ended request
            open_ended_request = OpenEndedRequest()
            open_ended_request.request = TRequest.objects.get(reqid=reqid)
            open_ended_request.container = slices[0].dataset.name
            open_ended_request.status = 'open'
            open_ended_request.save()
        except Exception,e:
            return HttpResponse(json.dumps({'success': False,'message':str(e)}), content_type='application/json')
        return  HttpResponse(json.dumps({'success':True}), content_type='application/json')



def check_open_ended():
    """
    try to extend all request with status 'open'
    :return:
    """
    open_requests = OpenEndedRequest.objects.filter(status='open')
    extended_requests = []
    for open_production_request in open_requests:
        try:
            if extend_open_ended_request(open_production_request.request_id):
                extended_requests.append(open_production_request.request_id)
            open_production_request.save_last_update()
        except Exception,e:
            _logger.error('Container extension failed: request:%s %s'%(str(open_production_request.request_id),str(e)))
    return extended_requests




def extend_open_ended_request(reqid):
    """
    To extend request by adding dataset which are not yet processed. Container is taken from first slice,
    steps should be skipped on it.
    :param reqid: ID of request to extend
    :return:
    True if request is extended
    """

    slices = list(InputRequestList.objects.filter(request=reqid).order_by('slice'))
    container_name = slices[0].dataset.name
    datasets = []
    slices_to_extend = [0]
    for index, slice in enumerate(slices[1:]):
        if slice.dataset.name == container_name:
            slices_to_extend.append(int(slice.slice))
        else:
            datasets.append(slice.dataset.name)

    ddm = DDM()
    datasets_in_container = ddm.dataset_in_container(container_name)
    is_extended = False
    for dataset in datasets_in_container:
        if (dataset not in datasets) and (dataset[dataset.find(':')+1:] not in datasets):
            is_extended = True
            for slice_number in slices_to_extend:
                new_slice_number = clone_slices(reqid,reqid,[slice_number],-1,False)
                new_slice = InputRequestList.objects.get(request=reqid,slice=new_slice_number)
                new_slice.dataset = fill_dataset(dataset)
                new_slice.save()
                steps = StepExecution.objects.filter(request=reqid,slice=new_slice)
                for step in steps:
                    step.status = 'Approved'
                    step.save()
    if is_extended:
        set_request_status('cron',reqid,'approved','Automatic openended approve', 'Request was automatically extended')
    return is_extended



    pass
