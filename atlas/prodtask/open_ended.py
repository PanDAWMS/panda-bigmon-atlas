import datetime
from django.db.models import Q
import json
import logging

from django.http import HttpResponse, HttpResponseRedirect

from django.views.decorators.csrf import csrf_protect
from time import sleep, time
from copy import deepcopy
import pytz
from atlas.prodtask.ddm_api import DDM
from ..prodtask.models import RequestStatus, ProductionTask
from ..prodtask.spdstodb import fill_template

from atlas.prodtask.views import set_request_status, clone_slices
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
            open_ended_request = None
            if OpenEndedRequest.objects.filter(request=reqid).exists():
                if OpenEndedRequest.objects.get(request=reqid).status == 'open':
                    return  HttpResponse(json.dumps({'success': False,'message':'Already open ended'}), content_type='application/json')
                if OpenEndedRequest.objects.get(request=reqid).status == 'closed':
                    open_ended_request = OpenEndedRequest.objects.get(request=reqid)

            #Make open ended from slice 0
            slices = list(InputRequestList.objects.filter(request=reqid))
            if not slices:
                HttpResponse(json.dumps({'success': False,'message':'No input'}), content_type='application/json')

            first_slice = slices[0]
            old_name =  slices[0].dataset.name
            new_name = old_name
            if old_name.find(':') == -1:
                new_name = old_name[:old_name.find('.')]+':'+old_name
            if new_name[-1:] != '/':
                new_name = new_name + '/'
            new_dataset = None
            if old_name != new_name:
                new_dataset = fill_dataset(new_name)
            #Register open ended request

            for slice in slices:
                if not slice.is_hide:
                    if slice.dataset.name == old_name:
                        if new_dataset:
                            slice.dataset = new_dataset
                            slice.save()
                        # Skip all steps in zero slice
                        steps = StepExecution.objects.filter(request=reqid,slice=slice)
                        for step in steps:
                            step.status = 'Skipped'
                            step.save()
            if not (open_ended_request):
                open_ended_request = OpenEndedRequest()
            open_ended_request.request = TRequest.objects.get(reqid=reqid)
            open_ended_request.container = new_name
            open_ended_request.status = 'open'
            open_ended_request.save()
            extend_open_ended_request(int(reqid))

        except Exception,e:
            _logger.error(form_request_log(reqid,None,'Problem with making open ended: %s'%str(e)))
            return HttpResponse(json.dumps({'success': False,'message':str(e)}), content_type='application/json')
        return  HttpResponse(json.dumps({'success':True}), content_type='application/json')


@csrf_protect
def push_check(request,reqid):
    if request.method == 'GET':
        try:
            if OpenEndedRequest.objects.filter(request=reqid).exists():
                open_ended_request = OpenEndedRequest.objects.get(request=reqid)
                if open_ended_request.status=='open':
                    if (datetime.utcnow().replace(tzinfo=pytz.utc) - open_ended_request.last_update).seconds > 600:
                        extend_open_ended_request(reqid)
                        open_ended_request.save_last_update()
        except Exception,e:
            return HttpResponse(json.dumps({'success': False,'message':str(e)}), content_type='application/json')


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

SIMULTANEOUS_TASKS_NUMBER = 300

def do_task_start(reqid):
    not_approved_steps = list(StepExecution.objects.filter(request=reqid,status='NotChecked'))
    is_extended = False
    if len(not_approved_steps) > 0:
        approved_steps_number = StepExecution.objects.filter(request=reqid,status='Approved').count()
        finished_tasks = ProductionTask.objects.filter(Q(status__in=['failed','broken','aborted','obsolete','done','finished']),Q(request=reqid)).count()
        if (approved_steps_number - finished_tasks) < SIMULTANEOUS_TASKS_NUMBER:
            is_extended = True
            task_to_start =  SIMULTANEOUS_TASKS_NUMBER - (approved_steps_number - finished_tasks)
            if task_to_start > len(not_approved_steps):
                task_to_start = len(not_approved_steps)
            for step in not_approved_steps[:task_to_start]:
                if step.step_parent == step:
                    step.status = 'Approved'
                    step.save()
                else:
                    if step.step_parent.status ==  'Approved':
                        step.status = 'Approved'
                        step.save()
    return is_extended


def clean_open_ended(reqid):
    slices = list(InputRequestList.objects.filter(request=reqid).order_by('slice'))
    for slice in slices:
        if not slice.is_hide:
            if not StepExecution.objects.filter(slice=slice).exists():
                print slice.request_id,slice.slice
                slice.is_hide = True
                slice.save()


def extend_open_ended_request(reqid):
    """
    To extend request by adding dataset which are not yet processed. Container is taken from first slice,
    steps should be skipped on it.
    :param reqid: ID of request to extend
    :return:
    True if request is extended
    """
    request = TRequest.objects.get(reqid=reqid)
    old_status = request.cstatus
    if old_status == 'extending':
        return False
    else:
        request.cstatus = 'extending'
        request.save()
    slices = list(InputRequestList.objects.filter(Q(request=reqid),~Q(is_hide=True)).order_by('slice'))
    _logger.debug(form_request_log(reqid,None,'Start request extending'))
    container_name = slices[0].dataset_id
    datasets = []
    slices_to_extend = [0]
    for index, slice in enumerate(slices[1:]):
        if not slice.is_hide:
            if slice.dataset_id == container_name:
                slices_to_extend.append(int(slice.slice))
            else:
                datasets.append(slice.dataset_id)
    tasks_count_control = False
    if request.request_type == 'EVENTINDEX':
        tasks_count_control = True
    try:
        ddm = DDM()
        datasets_in_container = ddm.dataset_in_container(container_name)
        _logger.debug(form_request_log(reqid,None,'Datasets in container: %i'%len(datasets_in_container)))
    except Exception, e:
        datasets_in_container = []
        _logger.error(form_request_log(reqid, None, 'error during request extension: %s' % str(e)))

    is_extended = False
    for dataset in datasets_in_container:
        if (dataset not in datasets) and (dataset[dataset.find(':')+1:] not in datasets):
            is_extended = True
            _logger.debug(form_request_log(reqid,None,'New dataset %s'%dataset))
            for slice_number in slices_to_extend:
                new_slice_number = clone_slices(reqid,reqid,[slice_number],-1,False)[0]
                new_slice = InputRequestList.objects.get(request=reqid,slice=new_slice_number)
                try:
                    if len(dataset)>150:
                        dataset = dataset[dataset.find(':')+1:]
                    new_slice.dataset = fill_dataset(dataset)
                    new_slice.save()
                    steps = StepExecution.objects.filter(request=reqid,slice=new_slice)
                    if len(steps) == 1:
                        if steps[0].step_template.output_formats == '':
                            output_format = dataset.split('.')[4]
                            step_new_template = fill_template(steps[0].step_template.step,steps[0].step_template.ctag,
                                                              steps[0].step_template.priority,output_format,
                                                              steps[0].step_template.memory)
                            step = steps[0]
                            step.step_template = step_new_template
                            step.save()
                    for step in steps:
                        if not tasks_count_control:
                            step.status = 'Approved'
                        else:
                            step.status = 'NotChecked'
                        step.save()

                except Exception,e:
                    new_slice.dataset = None
                    new_slice.is_hide = True
                    new_slice.save()
    if tasks_count_control:
        do_task = do_task_start(reqid)
        is_extended = is_extended or do_task
    request.cstatus = old_status
    request.save()
    if is_extended:
        set_request_status('cron',reqid,'approved','Automatic openended approve', 'Request was automatically extended')

    return is_extended



    pass
