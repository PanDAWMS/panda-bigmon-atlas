import functools
import json
import logging

from django.http import HttpResponse, HttpResponseBadRequest
from django.http.response import HttpResponseNotAllowed, HttpResponseForbidden

from django.views.decorators.csrf import csrf_protect
from django.utils import timezone
from copy import deepcopy

from atlas.prodtask.ddm_api import dataset_events_ddm, DDM
#from atlas.prodtask.googlespd import GSP
from atlas.prodtask.models import RequestStatus, WaitingStep, TrainProduction, MCPattern, SliceError
#from ..prodtask.spdstodb import fill_template
from atlas.prodtask.task_actions import _do_deft_action
from atlas.prodtask.views import set_request_status, clone_slices, egroup_permissions, single_request_action_celery_task
from atlas.prodtask.spdstodb import fill_template
from .hashtag import _set_request_hashtag
from ..prodtask.helper import form_request_log, form_json_request_dict
from .ddm_api import dataset_events
#from ..prodtask.task_actions import do_action
from .views import form_existed_step_list, form_step_in_page, fill_dataset, make_child_update
from django.db.models import Q
from rest_framework.response import Response
from rest_framework.decorators import api_view
from .models import StepExecution, InputRequestList, TRequest, ProductionTask, ProductionDataset, \
    ParentToChildRequest, TTask
from functools import reduce
from time import time
from atlas.celerybackend.celery import app, ProdSysTask
from celery.result import AsyncResult
from atlas.prodtask.tasks import test_async_progress

_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')


@csrf_protect
def tag_info(request, tag_name):
    if request.method == 'GET':
        results = {'success':False}
        try:
            trtf = None
            if trtf:
                results.update({'success':True,'name':tag_name,'output':trtf[0].formats,'transformation':trtf[0].trf,
                                'input':trtf[0].input,'step':trtf[0].step})
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')


def check_waiting_steps():

    def remove_waiting(steps,new_status='NotChecked'):
        for step in  steps:
            step.status = new_status
            step.save()


    APPROVE_LEVEL = 0.5
    all_waiting_steps = list(StepExecution.objects.filter(status='Waiting'))
    slices = set()
    steps_by_slices = {}
    requests = set()
    slices_by_request = {}
    for step in all_waiting_steps:
        slices.add(step.slice_id)
        steps_by_slices[step.slice_id] = steps_by_slices.get(step.slice_id,[])+[step]
    for slice in slices:
        approved_steps = list(StepExecution.objects.filter(status='Approved',slice=slice))
        if len(approved_steps) == 0:
            # No approved steps -> remove waiting
            remove_waiting(steps_by_slices[slice])

        else:
            # Check only first task of the first step

            tasks_to_check = ProductionTask.objects.filter(step=approved_steps[0])
            if tasks_to_check:
                task_to_check = tasks_to_check[0]
                if task_to_check.status in ProductionTask.RED_STATUS:
                    # Parent failed -> remove waiting
                    remove_waiting(steps_by_slices[slice])
                else:
                    total_files_tobeused = 0
                    total_files_finished = 0
                    if task_to_check.total_files_tobeused:
                        total_files_tobeused = task_to_check.total_files_tobeused
                    if task_to_check.total_files_finished:
                        total_files_finished = task_to_check.total_files_finished
                    if total_files_tobeused != 0:
                        if ((float(total_files_finished)/float(total_files_tobeused))>APPROVE_LEVEL):
                            remove_waiting(steps_by_slices[slice],'Approved')
                            requests.add(approved_steps[0].request_id)
                            slices_by_request[approved_steps[0].request_id] = slices_by_request.get(approved_steps[0].request_id,[]) + [slice]
                            _logger.debug("Slice %s has been approved after evgen"%str(slice))

    for request_id in requests:
        set_request_status('cron',request_id,'approved','Automatic waiting approve', 'Request was automatically approved')
        slice_numbers = []
        for slice_id in slices_by_request[request_id]:
              slice_numbers.append(int(InputRequestList.objects.get(id=slice_id).slice))
        make_child_update(request_id,'cron',slice_numbers)






@csrf_protect
def clone_slices_in_req(request, reqid, step_from, make_link_value):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = list(map(int,slices))
            _logger.debug(form_request_log(reqid,request,'Clone slices: %s' % str(ordered_slices)))
            ordered_slices.sort()
            if make_link_value == '1':
                make_link = True
            else:
                make_link = False
            step_from = int(step_from)
            cloned_slices = clone_slices(reqid,reqid,ordered_slices,step_from,make_link,True)
            results = {'success':True}
            request.session['selected_slices'] = list(map(int,cloned_slices))
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')



def fill_dataset_names(reqid):
    slices = list(InputRequestList.objects.filter(request=reqid).order_by('slice'))
    for slice in slices:
        if not slice.dataset:
            steps = StepExecution.objects.filter(request=reqid,slice=slice)
            if steps[0].step_parent.slice.dataset:
                slice.dataset = steps[0].step_parent.slice.dataset
                slice.save()


def extend_request_by_train(reqid,request_donor,slices):
            slices_donor = list(InputRequestList.objects.filter(request=request_donor).order_by('slice'))
            container_name = slices_donor[0].dataset
            slices_to_get_info = [slices_donor[0]]
            for index, slice_donor in enumerate(slices_donor[1:]):
                if (slice_donor.dataset == container_name) and (not slice_donor.is_hide):
                    slices_to_get_info.append(slice_donor)
            for slice in slices:
                for slice_donor in slices_to_get_info:
                    new_slice_number = clone_slices(reqid,reqid,[slice],-1,False)[0]
                    new_slice = InputRequestList.objects.get(request=reqid, slice=new_slice_number)
                    step = StepExecution.objects.get(request=reqid,slice=new_slice)
                    step_donor = StepExecution.objects.get(request=request_donor,slice=slice_donor)
                    step.priority = step_donor.priority
                    step.step_template = step_donor.step_template
                    step.task_config = step_donor.task_config
                    step.save_with_current_time()

@csrf_protect
def extend_by_train_pattern_slices_in_req(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            request_donor = input_dict['requestDonor']
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Extend slices by train: %s' % str(slices)))
            extend_request_by_train(reqid, request_donor, slices)
        except Exception as e:
            pass

        return HttpResponse(json.dumps(results), content_type='application/json')


def reject_steps_in_slice(current_slice):
    step_execs = StepExecution.objects.filter(slice=current_slice,request=current_slice.request)
    ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
    for step in ordered_existed_steps:
        if ProductionTask.objects.filter(step=step).count() == 0:
            step.step_appr_time = None
            if step.status == 'Skipped':
                step.status = 'NotCheckedSkipped'
            elif (step.status == 'Approved') or (step.status == 'Waiting'):
                step.status = 'NotChecked'
            step.save()
            try:
                for pre_definition_action in WaitingStep.objects.filter(step=step.id, status__in=['active','executing','failed']):
                    pre_definition_action.status = 'cancelled'
                    pre_definition_action.done_time = timezone.now()
                    pre_definition_action.save()
            except Exception as e:
                pass

def get_steps_for_update(reqid, slices, step_to_check, ami_tag):
        if step_to_check:
            if step_to_check in StepExecution.STEPS:
                step_to_check_index = StepExecution.STEPS.index(step_to_check)
                STEPS = StepExecution.STEPS
            else:
                STEPS = [None]*len(StepExecution.STEPS)
                step_to_check_index = int(step_to_check)
        else:
            step_to_check_index = -1
        filtered_steps = []
        for slice in slices:
            steps = StepExecution.objects.filter(request=reqid,slice=slice)
            if not step_to_check:
                for step in steps:
                    if (not ami_tag) or (ami_tag == step.step_template.ctag):
                        filtered_steps.append(step)
            else:
                ordered_existed_steps, parent_step = form_existed_step_list(steps)
                steps_as_shown = form_step_in_page(ordered_existed_steps,STEPS,parent_step)
                if steps_as_shown[step_to_check_index]:
                   if (not ami_tag) or (steps_as_shown[step_to_check_index].step_template.ctag == ami_tag):
                    filtered_steps.append(steps_as_shown[step_to_check_index])
        return filtered_steps


@csrf_protect
def find_parent_slices(request, reqid, parent_request):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            start_time = time()
            slices = input_dict['slices']
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = list(map(int,slices))
            _logger.debug(form_request_log(reqid,request,'Find parent slices: %s, parent request %s'% (str(ordered_slices),parent_request)))
            ordered_slices.sort()
            changed_slices = set_parent_step(ordered_slices,int(reqid),int(parent_request))
            results = {'success':True}
            request.session['selected_slices'] = list(map(int,changed_slices))
            _jsonLogger.info('Finish parent steps for MC slices', extra=form_json_request_dict(reqid,request,
                                                                                                      {'duration':time()-start_time,'slices':json.dumps(slices)}))
        except Exception as e:
            _logger.error(str(e))
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def async_find_parent_slices(request, reqid, parent_request):
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            ordered_slices = list(map(int,slices))
            _logger.debug(form_request_log(reqid,request,'Find parent slices: %s, parent request %s'% (str(ordered_slices),parent_request)))
            ordered_slices.sort()
            return_value = single_request_action_celery_task(reqid,find_parent_slices_task,'find parent slices',
                                                             request.user.username,ordered_slices,int(reqid),int(parent_request))
            return HttpResponse(json.dumps(return_value), content_type='application/json')
        except Exception as e:
            _logger.error(str(e))
            return HttpResponseBadRequest(e)


@app.task(bind=True, base=ProdSysTask)
@ProdSysTask.set_task_name('find parent slices')
def find_parent_slices_task(self, ordered_slices,reqid,parent_request):
    parent_slices = list(InputRequestList.objects.filter(request=parent_request).order_by('slice'))
    parent_slice_dict = {}
    slices_updated = []
    for parent_slice in parent_slices:
        if (parent_slice.input_data) and (not parent_slice.is_hide):
            parent_slice_dict[parent_slice.input_data] = parent_slice_dict.get(parent_slice.input_data,[])+[parent_slice]
    self.progress_message_update(0,len(ordered_slices))
    for slices_processed, slice_number in enumerate(ordered_slices):
        slice = InputRequestList.objects.get(slice=slice_number,request=reqid)
        steps = StepExecution.objects.filter(slice=slice,request=reqid)
        ordered_existed_steps, parent_step = form_existed_step_list(steps)
        if ((not parent_step) or (parent_step.request_id != parent_request)) and slice.input_data:
            first_not_skipped = None
            step_to_delete = []
            tags = []
            for step in ordered_existed_steps:
                if step.status in ['NotCheckedSkipped']:
                    tags.append(step.step_template.ctag)
                    step_to_delete.append(step)
                else:
                    first_not_skipped = step
                    break
            step_to_delete.reverse()
            if tags:
                parent_found = False
                parent_slices = parent_slice_dict.get(slice.input_data,[])
                for slice_index, parent_slice in enumerate(parent_slices):
                    parent_slice_steps = StepExecution.objects.filter(slice=parent_slice,request=parent_request)
                    parent_ordered_existed_steps, parent_step = form_existed_step_list(parent_slice_steps)
                    for index,step in enumerate(parent_ordered_existed_steps):
                        if step.status in ['Approved']:
                            if step.step_template.ctag == tags[index]:
                                if index == (len(tags)-1):
                                    first_not_skipped.step_parent = step
                                    first_not_skipped.set_task_config({'nEventsPerInputFile':''})
                                    first_not_skipped.save()
                                    for x in step_to_delete:
                                        x.delete()
                                    parent_slice_dict[slice.input_data].pop(slice_index)
                                    parent_found = True
                                    slices_updated.append(slice_number)
                                    break
                            else:
                                break
                    if parent_found:
                        break
        elif parent_step and  (parent_step.request_id == parent_request) and slice.input_data:
            if parent_slice_dict.get(slice.input_data,[]):
                parent_slice_dict[slice.input_data].pop(0)
        self.progress_message_update(slices_processed,len(ordered_slices))
    return slices_updated

def set_parent_step(slices, request, parent_request):
    parent_slices = list(InputRequestList.objects.filter(request=parent_request).order_by('slice'))
    parent_slice_dict = {}
    slices_updated = []
    for parent_slice in parent_slices:
        if (parent_slice.input_data) and (not parent_slice.is_hide):
            parent_slice_dict[parent_slice.input_data] = parent_slice_dict.get(parent_slice.input_data,[])+[parent_slice]
    for slice_number in slices:
        slice = InputRequestList.objects.get(slice=slice_number,request=request)
        steps = StepExecution.objects.filter(slice=slice,request=request)
        ordered_existed_steps, parent_step = form_existed_step_list(steps)
        if ((not parent_step) or (parent_step.request_id != parent_request)) and slice.input_data:
            first_not_skipped = None
            step_to_delete = []
            tags = []
            for step in ordered_existed_steps:
                if step.status in ['NotCheckedSkipped']:
                    tags.append(step.step_template.ctag)
                    step_to_delete.append(step)
                else:
                    first_not_skipped = step
                    break
            step_to_delete.reverse()
            if tags:
                parent_found = False
                parent_slices = parent_slice_dict.get(slice.input_data,[])
                for slice_index, parent_slice in enumerate(parent_slices):
                    parent_slice_steps = StepExecution.objects.filter(slice=parent_slice,request=parent_request)
                    parent_ordered_existed_steps, parent_step = form_existed_step_list(parent_slice_steps)
                    for index,step in enumerate(parent_ordered_existed_steps):
                        if step.status in ['Approved']:
                            if step.step_template.ctag == tags[index]:
                                if index == (len(tags)-1):
                                    first_not_skipped.step_parent = step
                                    first_not_skipped.set_task_config({'nEventsPerInputFile':''})
                                    first_not_skipped.save()
                                    for x in step_to_delete:
                                        x.delete()
                                    parent_slice_dict[slice.input_data].pop(slice_index)
                                    parent_found = True
                                    slices_updated.append(slice_number)
                                    break
                            else:
                                break
                    if parent_found:
                        break
        elif parent_step and  (parent_step.request_id == parent_request) and slice.input_data:
            if parent_slice_dict.get(slice.input_data,[]):
                parent_slice_dict[slice.input_data].pop(0)
    return slices_updated


@csrf_protect
def get_steps_bulk_info(request, reqid):
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            slice_numbers = input_dict['slices']
            step_to_check = input_dict['step']
            ami_tag = input_dict['amiTag']
            all_slices = list(InputRequestList.objects.filter(request=reqid))
            slices = all_slices
            if '-1' not in slice_numbers:
                slices = [x.id for x in all_slices if str(x.slice) in slice_numbers]
            _logger.debug(form_request_log(reqid,request,'Take steps info: %s' % str(slice_numbers)))
            steps = get_steps_for_update(reqid,slices,step_to_check,ami_tag)
            result_dict = {'multivalues':{},'singlevalue':{}}
            steps_approved = 0
            for step in steps:
                if step.status in StepExecution.STEPS_APPROVED_STATUS:
                    steps_approved +=1

                current_step_dict = {'ctag':step.step_template.ctag}
                current_step_dict['output_formats'] = step.step_template.output_formats
                if step.step_template.memory:
                    current_step_dict['memory'] = int(step.step_template.memory)
                else:
                    current_step_dict['memory'] = ''
                current_step_dict['priority'] = int(step.priority)
                if step.input_events:
                    current_step_dict['input_events'] = int(step.input_events)
                else:
                    current_step_dict['input_events'] = ''
                task_config = step.get_task_config()
                for x in StepExecution.TASK_CONFIG_PARAMS:
                    if x in task_config:
                        if (x not in ['PDA']) or task_config[x]:
                            current_step_dict[x]=task_config[x]
                        else:
                            current_step_dict[x] = 'none'
                    else:
                        if x not in ['PDA']:
                            current_step_dict[x]=''
                        else:
                            current_step_dict[x] = 'none'
                for key in current_step_dict:
                    if key in result_dict['multivalues']:
                        if current_step_dict[key] not in result_dict['multivalues'][key]:
                            result_dict['multivalues'][key].append(current_step_dict[key])
                    else:
                        if key in result_dict['singlevalue']:
                            if current_step_dict[key] != result_dict['singlevalue'][key]:
                                result_dict['multivalues'][key] = [result_dict['singlevalue'][key],current_step_dict[key]]
                                del result_dict['singlevalue'][key]
                        else:
                            result_dict['singlevalue'][key] = current_step_dict[key]

            return HttpResponse(json.dumps({'result':result_dict,'stepsApproved':steps_approved,'stepsToChange':len(steps)-steps_approved}), content_type='application/json')
        except Exception as e:
            pass
    return HttpResponse(json.dumps({'status':'failed'}), content_type='application/json')


@csrf_protect
def get_slices_bulk_info(request, reqid):
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            slice_numbers = input_dict['slices']
            all_slices = list(InputRequestList.objects.filter(request=reqid))
            slices = all_slices
            if '-1' not in slice_numbers:
                slices = [x for x in all_slices if str(x.slice) in slice_numbers]
            _logger.debug(form_request_log(reqid,request,'Take slices info: %s' % str(slice_numbers)))
            result_dict = {'multivalues':{},'singlevalue':{}}
            for slice in slices:
                current_slice_dict = {'datasetName':slice.dataset,'comment':slice.comment,'jobOption':slice.input_data,
                                      'eventsNumber':int(slice.input_events),'priority':int(slice.priority)}

                for key in current_slice_dict:
                    if key in result_dict['multivalues']:
                        if current_slice_dict[key] not in result_dict['multivalues'][key]:
                            result_dict['multivalues'][key].append(current_slice_dict[key])
                    else:
                        if key in result_dict['singlevalue']:
                            if current_slice_dict[key] != result_dict['singlevalue'][key]:
                                result_dict['multivalues'][key] = [result_dict['singlevalue'][key],current_slice_dict[key]]
                                del result_dict['singlevalue'][key]
                        else:
                            result_dict['singlevalue'][key] = current_slice_dict[key]

            return HttpResponse(json.dumps({'result':result_dict}), content_type='application/json')
        except Exception as e:
            pass
    return HttpResponse(json.dumps({'status':'failed'}), content_type='application/json')

@csrf_protect
def set_steps_bulk_info(request, reqid):
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            slice_numbers = input_dict['slices']
            step_to_check = input_dict['step']
            ami_tag = input_dict['amiTag']
            new_values = input_dict['newValues']
            all_slices = list(InputRequestList.objects.filter(request=reqid))
            slices = all_slices
            if '-1' not in slice_numbers:
                slices = [x.id for x in all_slices if str(x.slice) in slice_numbers]
            _logger.debug(form_request_log(reqid,request,'Change steps info: %s %s' % (str(slice_numbers),str(new_values))))
            steps = get_steps_for_update(reqid,slices,step_to_check,ami_tag)
            new_step_template = None
            if new_values:
                for step in steps:
                    step_modified = False
                    template_modified = False
                    if step.status not in StepExecution.STEPS_APPROVED_STATUS:
                        output_formats = step.step_template.output_formats
                        if 'output_formats' in new_values:
                            if new_values['output_formats'] != step.step_template.output_formats:
                                output_formats = new_values['output_formats']
                                template_modified = True
                        memory = step.step_template.memory
                        if 'memory' in new_values:
                            if new_values['memory'] != step.step_template.memory:
                                memory = new_values['memory']
                                template_modified = True
                        if template_modified:
                            if not new_step_template:
                                new_step_template = fill_template(step.step_template.step,step.step_template.ctag,
                                                                  step.step_template.priority,output_formats,memory)
                            step.step_template = new_step_template
                            step_modified = True
                        if 'priority' in new_values:
                            step_modified = True
                            step.priority = new_values['priority']
                        if 'input_events' in new_values:
                            step_modified = True
                            step.input_events = new_values['input_events']
                        for x in StepExecution.TASK_CONFIG_PARAMS:
                            if x in new_values:
                                if x in ['PDA']:
                                    if new_values[x] == 'none':
                                        new_values[x] = ''
                                step_modified = True
                                if x in StepExecution.INT_TASK_CONFIG_PARAMS:
                                    if new_values[x]:
                                        step.set_task_config({x:int(new_values[x])})
                                    else:
                                        step.set_task_config({x:new_values[x]})
                                else:
                                    if type(new_values[x]) is not bool:
                                        step.set_task_config({x:new_values[x].strip()})
                                    else:
                                        step.set_task_config({x: new_values[x]})
                        if step_modified:
                            step.save()
                return HttpResponse(json.dumps({'status':'success'}), content_type='application/json')
        except Exception as e:
            return HttpResponse(json.dumps({'status':'failed','error':str(e)}), content_type='application/json')
    return HttpResponse(json.dumps({'status':'failed'}), content_type='application/json')

@csrf_protect
def get_ami_tag_list(request, reqid):
    if request.method == 'POST':
        data = request.body
        input_dict = json.loads(data)
        slice_numbers = input_dict['slices']
        step_to_check = input_dict['step']
        all_slices = list(InputRequestList.objects.filter(request=reqid))
        slices = all_slices
        if '-1' not in slice_numbers:
            slices = [x.id for x in all_slices if str(x.slice) in slice_numbers]
        _logger.debug(form_request_log(reqid,request,'Take tags list: %s' % str(slice_numbers)))
        steps = get_steps_for_update(reqid,slices,step_to_check,'')
        ami_tags = set()
        for step in steps:
            ami_tags.add(step.step_template.ctag)
        return HttpResponse(json.dumps({'AMITags':list(ami_tags)}), content_type='application/json')
    return HttpResponse(json.dumps({'AMITags':[],'status':'failed'}), content_type='application/json')


def find_child_request_slices(production_request_id,parent_slices):
    """
    Find all slices which related to the parent parent_slices in production_request_id
    :param production_request_id: parent production request id
    :param parent_slices: list of the slice numbers in parent production request
    :return: list of the slice id in child requests
    """

    # find child requests
    requests_relations = ParentToChildRequest.objects.filter(status='active',parent_request=production_request_id)
    child_requests = [item.child_request for item in requests_relations if item.child_request]
    child_slices = []
    if child_requests:
        # find all parent steps
        parent_steps = []
        for slice_number in parent_slices:
            current_slice = InputRequestList.objects.get(request=production_request_id,slice=int(slice_number))
            parent_steps += StepExecution.objects.filter(slice=current_slice,request=production_request_id).values_list('id',flat=True)
        # for each child request find linked slices
        for child_request in child_requests:
            current_child_slice_set = set()
            child_steps =  list(StepExecution.objects.filter(request=child_request).values('id','slice_id','step_parent_id'))
            for child_step in child_steps:
                if child_step['step_parent_id'] in parent_steps:
                    current_child_slice_set.add(child_step['slice_id'])
            child_slices += list(current_child_slice_set)
    return child_slices


@csrf_protect
def reject_slices_in_req(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if slices == ['-1']:
                slices = ['0']
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Reject slices: %s' % str(slices)))
            slices_numbers = list(map(int, slices))
            for slice_number in slices_numbers:
                current_slice = InputRequestList.objects.get(request=reqid,slice=slice_number)
                reject_steps_in_slice(current_slice)
            for slice_id in find_child_request_slices(reqid, slices_numbers):
                current_slice = InputRequestList.objects.get(id=slice_id)
                reject_steps_in_slice(current_slice)

            request.session['selected_slices'] = list(map(int,slices))
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def hide_slices_in_req(request, reqid):

    def hide_slice(slice):
        if not slice.is_hide:
            slice.is_hide = True
            reject_steps_in_slice(slice)
            for slice_error in SliceError.objects.filter(request=slice.request,slice=slice, is_active=True):
                slice_error.is_active = False
                slice_error.save()
        else:
            slice.is_hide = False
        slice.save()

    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Hide slices: %s' % str(slices)))
            slices_numbers = list(map(int, slices))
            for slice_number in slices_numbers:
                current_slice = InputRequestList.objects.get(request=reqid,slice=slice_number)
                hide_slice(current_slice)
            #reject child slices
            for slice_id in find_child_request_slices(reqid, slices_numbers):
                current_slice = InputRequestList.objects.get(id=slice_id)
                hide_slice(current_slice)
            results = {'success':True}
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def add_request_comment(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            comment = input_dict['comment']
            _logger.debug(form_request_log(reqid,request,'Add comment: %s' % str(comment)))
            new_comment = RequestStatus()
            new_comment.comment = comment
            new_comment.owner = request.user.username
            new_comment.request = TRequest.objects.get(reqid=reqid)
            new_comment.status = 'comment'
            new_comment.save_with_current_time()
            results = {'success':True}
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

# def find_double_task(request_from,request_to,showObsolete=True,checkMode=True,obsoleteOnly=True):
#     total = 0
#     total_steps = 0
#     total_tasks = 0
#     nothing = 0
#     alreadyObsolete = 0
#     obsolets = []
#     aborts = []
#     status_list = ['obsolete','aborted','broken','failed','submitting','submitted','assigning','registered','ready','running','finished','done']
#     for request_id in range(request_from,request_to):
#         try:
#             total1 = total
#             total_steps1 = total_steps
#             steps = StepExecution.objects.filter(request=request_id)
#             tasks = list(ProductionTask.objects.filter(request=request_id))
#             total_tasks +=  len(tasks)
#             tasks_by_step = {}
#             for task in tasks:
#                 tasks_by_step[task.step_id] = tasks_by_step.get(task.step_id,[])+[task]
#
#             for current_step in steps:
#                 input_dict = {}
#                 if tasks_by_step.get(current_step.id,None):
#
#                     for current_task in tasks_by_step[current_step.id]:
#                         real_name = current_task.input_dataset[current_task.input_dataset.find(':')+1:]
#                         input_dict[real_name]=input_dict.get(real_name,[])+[current_task]
#                     # if len(input_dict.keys())>1:
#                     #     long_step += 1
#                     #     print 'check - ', current_step.id,input_dict[input_dict.keys()[0]][0].inputdataset
#                     for input_dataset in input_dict.keys():
#                         if len(input_dict[input_dataset])>1:
#                             total_steps += 1
#                             total += len(input_dict[input_dataset])
#                             dataset_to_stay = 0
#                             max_status_index = status_list.index(input_dict[input_dataset][0].status)
#                             #print '-'
#                             for index,ds in enumerate(input_dict[input_dataset][1:]):
#                                 if status_list.index(ds.status) > max_status_index:
#                                     dataset_to_stay = index+1
#                                     max_status_index = status_list.index(ds.status)
#                             if input_dict[input_dataset][dataset_to_stay].status != 'done':
#                                 print 'To stay:', input_dict[input_dataset][dataset_to_stay].status,input_dict[input_dataset][dataset_to_stay].id,input_dataset
#                             for index,ds in enumerate(input_dict[input_dataset]):
#
#                                 if ds.status == 'obsolete':
#                                     if showObsolete:
#                                         print ds.output_dataset
#                                     alreadyObsolete += 1
#                                 if index != dataset_to_stay:
#                                     if ds.status in ['obsolete','broken','failed','aborted']:
#                                         #print 'Do nothing:',ds.status,ds.id
#                                         nothing += 1
#                                         pass
#                                     elif ds.status in ['finished','done']:
#                                         print 'Obsolete:',ds.status,ds.id
#                                         obsolets.append(ds.id)
#                                         print dataset_to_stay,'-',[(x.status,x.id) for x in input_dict[input_dataset]]
#                                     else:
#                                         print 'Abort:',ds.status,ds.id
#                                         aborts.append(ds.id)
#
#                             #print current_step.id,'-',input_dataset,'-',len(input_dict[input_dataset]),[x.status for x in input_dict[input_dataset]]
# #            print request_id, '-',len(tasks), (total-total1),(total_steps-total_steps1)
#
#         except Exception,e:
#             print e
#             pass
#     if (not checkMode):
#         #pass
#         for task_id in obsolets:
#             res = do_action('mborodin',task_id,'obsolete')
#         if not obsoleteOnly:
#             for task_id in aborts:
#                 res = do_action('mborodin',str(task_id),'abort')
#                 try:
#                     if res['status']['jedi_info']['status_code']!=0:
#                         print res
#                 except:
#                     pass
#                 print res
#                 sleep(1)
#     print total_tasks,total_steps,total
#     print 'obsoletes:',len(obsolets),'abort:',len(aborts),'Already obsolete:',alreadyObsolete




@csrf_protect
def step_params_from_tag(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            checkecd_tag_format = json.loads(data)
            tag = checkecd_tag_format['tag_format'].split(':')[0]
            output_format, slice_from = checkecd_tag_format['tag_format'].split('-')
            output_format = output_format[len(tag)+1:]
            project_mode = ''
            input_events = ''
            priority = ''
            nEventsPerJob = ''
            nEventsPerInputFile = ''
            destination_token = ''
            req = TRequest.objects.get(reqid=reqid)
            slices = InputRequestList.objects.filter(request=req).order_by("slice")
            for slice in slices:
                if slice.slice>=int(slice_from):
                    step_execs = StepExecution.objects.filter(slice=slice, request=req)
                    for step_exec in step_execs:
                        if(tag == step_exec.step_template.ctag)and(output_format == step_exec.step_template.output_formats):
                            task_config = json.loads(step_exec.task_config)
                            if 'project_mode' in task_config:
                                project_mode = task_config['project_mode']
                            if 'nEventsPerJob' in task_config:
                                nEventsPerJob = task_config['nEventsPerJob']
                            if 'nEventsPerInputFile' in task_config:
                                nEventsPerInputFile = task_config['nEventsPerInputFile']
                            if 'token' in task_config:
                                destination_token = task_config['token']
                            input_events = step_exec.input_events
                            priority = step_exec.priority
            results.update({'success':True,'project_mode':project_mode,'input_events':str(input_events),
                            'priority':str(priority),'nEventsPerJob':str(nEventsPerJob),
                            'nEventsPerInputFile':str(nEventsPerInputFile),'destination':destination_token})
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def test_auth_for_api(request, param):
    if request.method == 'POST':
        return HttpResponse(json.dumps({'user':request.user.username,'arg':param}), content_type='application/json')
    if request.method == 'GET':
        return HttpResponse(json.dumps({'user':request.user.username,'arg':param}), content_type='application/json')


def test_auth_for_api2(request, param):
    if request.method == 'POST':
        return HttpResponse(json.dumps({'user':request.user.username,'arg':param}), content_type='application/json')
    if request.method == 'GET':
        return HttpResponse(json.dumps({'user':'test','arg':param}), content_type='application/json')

@csrf_protect
def update_project_mode(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        updated_slices = []
        try:
            data = request.body
            checkecd_tag_format = json.loads(data)
            _logger.debug(form_request_log(reqid,request,'Update steps: %s' % str(checkecd_tag_format)))
            tag = checkecd_tag_format['tag_format'].split(':')[0]
            output_format, slice_from = checkecd_tag_format['tag_format'].split('-')
            output_format = output_format[len(tag)+1:]
            slice_from = 0
            new_project_mode = checkecd_tag_format['project_mode']
            new_input_events = int(checkecd_tag_format['input_events'])
            new_priority = int(checkecd_tag_format['priority'])
            if checkecd_tag_format['nEventsPerInputFile']:
                new_nEventsPerInputFile = int(checkecd_tag_format['nEventsPerInputFile'])
            else:
                new_nEventsPerInputFile = ''
            if checkecd_tag_format['nEventsPerJob']:
                new_nEventsPerJob = int(checkecd_tag_format['nEventsPerJob'])
            else:
                new_nEventsPerJob = ''
            new_destination = None
            if checkecd_tag_format['destination_token']:
                new_destination = checkecd_tag_format['destination_token']
            req = TRequest.objects.get(reqid=reqid)
            slices = InputRequestList.objects.filter(request=req).order_by("slice")
            for slice in slices:
                if slice.slice>=int(slice_from):
                    step_execs = StepExecution.objects.filter(slice=slice, request=req)
                    for step_exec in step_execs:
                        if(tag == step_exec.step_template.ctag)and(output_format == step_exec.step_template.output_formats):
                            if step_exec.status != 'Approved':
                                task_config = json.loads(step_exec.task_config)
                                task_config['project_mode'] = new_project_mode
                                step_exec.task_config = ''
                                step_exec.set_task_config(task_config)
                                step_exec.set_task_config({'nEventsPerInputFile':new_nEventsPerInputFile})
                                step_exec.set_task_config({'nEventsPerJob':new_nEventsPerJob})
                                if new_destination:
                                    step_exec.set_task_config({'token':'dst:'+new_destination.replace('dst:','')})
                                if step_exec.get_task_config('token') and (not new_destination):
                                    step_exec.set_task_config({'token':''})
                                step_exec.input_events = new_input_events
                                step_exec.priority = new_priority
                                step_exec.save()
                                if slice.slice not in updated_slices:
                                   updated_slices.append(str(slice.slice))
            results.update({'success':True,'slices':updated_slices})
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')




@csrf_protect
def get_tag_formats(request, reqid):
    if request.method == 'GET':
        results = {'success':False}
        try:
            tag_formats = []
            #slice_from = []
            #project_modes = []
            req = TRequest.objects.get(reqid=reqid)
            slices = InputRequestList.objects.filter(request=req).order_by("slice")
            for slice in slices:
                step_execs = StepExecution.objects.filter(slice=slice, request=req)
                for step_exec in step_execs:
                    tag_format = step_exec.step_template.ctag + ":" + step_exec.step_template.output_formats
                    task_config = '{}'
                    if step_exec.task_config:
                        task_config = step_exec.task_config
                    task_config = json.loads(task_config)
                    project_mode = ''
                    if 'project_mode' in task_config:
                        project_mode = task_config['project_mode']
                    do_update = True
                    for existed_tag_format in tag_formats:
                        if (existed_tag_format[0] == tag_format) and (existed_tag_format[1] == project_mode):
                            do_update = False
                    if do_update:
                        tag_formats.append((tag_format,project_mode,slice.slice))
            results.update({'success':True,'data':[x[0]+'-'+str(x[2]) for x in tag_formats]})
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def slice_steps(request, reqid, slice_number):
    if request.method == 'GET':
        results = {'success':False}
        try:
            if slice_number == '-1':
                slice_number = 0
            req = TRequest.objects.get(reqid=reqid)
            input_list = InputRequestList.objects.get(request=req,slice=slice_number)
            existed_steps = StepExecution.objects.filter(request=req, slice=input_list)
            # Check steps which already exist in slice, and change them if needed
            ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            result_list = []
            foreign_step_dict_index = -1
            if req.request_type == 'MC':
                step_as_in_page = form_step_in_page(ordered_existed_steps,StepExecution.STEPS, None)
            else:
                step_as_in_page = form_step_in_page(ordered_existed_steps,['']*len(StepExecution.STEPS),existed_foreign_step)
            if existed_foreign_step:
                    if req.request_type != 'MC':
                        foreign_step_dict = {'step':existed_foreign_step.step_template.ctag,'step_name':existed_foreign_step.step_template.step,'step_type':'foreign',
                                            'nEventsPerJob':'','nEventsPerInputFile':'','nFilesPerJob':'',
                                            'project_mode':'','input_format':'',
                                            'priority':'', 'output_formats':'','input_events':'',
                                            'token':'','nGBPerJob':'','maxAttempt':'','maxFailure':'','evntFilterEff':'',
                                             'PDA':'','PDAPArams':''}
                        foreign_step_dict_index = 0
                    else:
                        foreign_step_dict = {'step':existed_foreign_step.step_template.ctag,'step_name':existed_foreign_step.step_template.step,'step_type':'foreign',
                                            'nEventsPerJob':'','nEventsPerInputFile':'','nFilesPerJob':'',
                                            'project_mode':'','input_format':'',
                                            'priority':'', 'output_formats':'','input_events':'',
                                            'token':'','nGBPerJob':'','maxAttempt':'','maxFailure':'','evntFilterEff':'',
                                             'PDA':'','PDAPArams':''}
                        foreign_step_dict_index = StepExecution.STEPS.index(existed_foreign_step.step_template.step)

            for index,step in enumerate(step_as_in_page):
                if not step:
                    if index == foreign_step_dict_index:
                        result_list.append(foreign_step_dict)
                    else:
                        result_list.append({'step':'','step_name':'','step_type':''})
                else:
                    is_skipped = 'not_skipped'
                    if step.status == 'NotCheckedSkipped' or step.status == 'Skipped':
                        is_skipped = 'is_skipped'
                    task_config = json.loads(step.task_config)
                    result_list.append({'step':step.step_template.ctag,'step_name':step.step_template.step,'step_type':is_skipped,
                                        'nEventsPerJob':task_config.get('nEventsPerJob',''),'nEventsPerInputFile':task_config.get('nEventsPerInputFile',''),
                                        'project_mode':task_config.get('project_mode',''),'input_format':task_config.get('input_format',''),
                                        'priority':str(step.priority), 'output_formats':step.step_template.output_formats,'input_events':str(step.input_events),
                                        'token':task_config.get('token',''),'merging_tag':task_config.get('merging_tag',''),
                                        'nFilesPerMergeJob':task_config.get('nFilesPerMergeJob',''),'nGBPerMergeJob':task_config.get('nGBPerMergeJob',''),
                                        'nMaxFilesPerMergeJob':task_config.get('nMaxFilesPerMergeJob',''),
                                        'nFilesPerJob':task_config.get('nFilesPerJob',''),'nGBPerJob':task_config.get('nGBPerJob',''),
                                        'maxFailure':task_config.get('maxFailure',''),'nEventsPerMergeJob':task_config.get('nEventsPerMergeJob',''),
                                        'evntFilterEff':task_config.get('evntFilterEff',''),
                                        'PDA':task_config.get('PDA',''),
                                         'PDAParams':task_config.get('PDAParams',''),'container_name':task_config.get('container_name',''),
                                         'onlyTagsForFC':task_config.get('onlyTagsForFC',''),
                                        'previousTasks':','.join(map(str,task_config.get('previous_task_list',[])))})

            dataset = ''
            if input_list.dataset:
                dataset = input_list.dataset
            jobOption = ''
            if input_list.input_data:
                jobOption = input_list.input_data
            results = {'success':True,'step_types':result_list, 'dataset': dataset, 'jobOption':jobOption,
                       'totalEvents':int(input_list.input_events),'comment':input_list.comment}
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def reject_steps(request, reqid, step_filter):
    if request.method == 'GET':
        results = {'success':False}
        try:
            changed_steps = 0
            if step_filter == 'all':
                req = TRequest.objects.get(reqid=reqid)
                steps = StepExecution.objects.filter(request=req)
                for step in steps:
                    if step.status == 'Approved':
                        step.status = 'NotChecked'
                        step.save()
                        changed_steps += 1
                    elif step.status == 'Skipped':
                        step.status = 'NotCheckedSkipped'
                        step.save()
                        changed_steps += 1
            results = {'success':True,'step_changed':str(changed_steps)}
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')


def prepare_slices_to_retry(production_request, ordered_slices):
    """
    Function to prepare action to recovery broken tasks. If task are broken it creates new slice with broken task setting
    in step previous_task. If task was already restarted it's not go again.
    :param production_request: request id
    :param ordered_slices: ordered slice numbers
    :return: dictionary of action {slice number: [list of task per step] }
    """
    def remove_task_chain(list_to_remove, tasks_checked, task_id, tasks_parent):
        slice_steps = tasks_checked.pop(task_id)
        for slice_step in slice_steps:
            if task_id in list_to_remove[slice_step[0]][slice_step[1]]:
                list_to_remove[slice_step[0]][slice_step[1]].remove(task_id)
            if tasks_parent[task_id] in tasks_checked:
                remove_task_chain(list_to_remove, tasks_checked, tasks_parent[task_id], tasks_parent)



    tasks_db = list(ProductionTask.objects.filter(request=production_request).order_by('-submit_time').values())
    tasks = {}
    tasks_parent = {}
    tasks_checked = {}
    result_dict = {}
    for current_task in tasks_db:
        tasks[current_task['step_id']] = tasks.get(current_task['step_id'],[]) + [current_task]
        tasks_parent[current_task['id']] = current_task['parent_id']
    for slice in ordered_slices:
        current_slice = InputRequestList.objects.get(slice=slice,request=production_request)
        step_execs = StepExecution.objects.filter(slice=current_slice,request=production_request)
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(step_execs)
        result_dict[slice] = []
        for index, step in enumerate(ordered_existed_steps):
            tasks_to_fix = []
            previous_tasks = step.get_task_config('previous_task_list')
            if previous_tasks:
                for task in previous_tasks:
                    remove_task_chain(result_dict,tasks_checked,task,tasks_parent)
            if step.id in tasks:
                for task in tasks[step.id]:
                    if task['status'] in ['broken','failed','aborted']:
                        tasks_to_fix.append(task['id'])
                        tasks_checked[task['id']] = tasks_checked.get(task['id'],[])+[(slice, index)]
                    else:
                        if task['id'] in tasks_checked:
                            remove_task_chain(result_dict,tasks_checked,task['id'],tasks_parent)
                        if tasks_parent[task['id']] in tasks_checked:
                            remove_task_chain(result_dict,tasks_checked,tasks_parent[task['id']],tasks_parent)

            result_dict[slice].append(tasks_to_fix)
    return result_dict


def apply_retry_action(production_request, retry_action):
    new_slice_number = (InputRequestList.objects.filter(request=production_request).order_by('-slice')[0]).slice + 1
    request_source = TRequest.objects.get(reqid=production_request)
    old_new_step = {}
    for slice_number in sorted(retry_action):
        if reduce(lambda x,y: x+y,retry_action[slice_number]):
            current_slice = InputRequestList.objects.get(request=production_request,slice=int(slice_number))
            step_execs = StepExecution.objects.filter(slice=current_slice,request=production_request)
            ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
            if len(ordered_existed_steps) == len(retry_action[slice_number]):
                new_slice = list(current_slice.values())[0]
                new_slice['slice'] = new_slice_number
                new_slice_number += 1
                del new_slice['id']
                new_input_data = InputRequestList(**new_slice)
                new_input_data.cloned_from = InputRequestList.objects.get(request=production_request,slice=int(slice_number))
                new_input_data.save()
                if request_source.request_type == 'MC':
                    STEPS = StepExecution.STEPS
                else:
                    STEPS = ['']*len(StepExecution.STEPS)
                step_as_in_page = form_step_in_page(ordered_existed_steps,STEPS,parent_step)
                real_step_index = -1
                first_changed = False
                for index,step in enumerate(step_as_in_page):
                    if step:
                        real_step_index += 1
                        if retry_action[slice_number][real_step_index] or first_changed:
                            self_looped = step.id == step.step_parent.id
                            old_step_id = step.id
                            step.id = None
                            step.step_appr_time = None
                            step.step_def_time = None
                            step.step_exe_time = None
                            step.step_done_time = None
                            step.slice = new_input_data
                            step.set_task_config({'previous_task_list':list(map(int,retry_action[slice_number][real_step_index]))})
                            if step.status == 'Skipped':
                                step.status = 'NotCheckedSkipped'
                            elif (step.status == 'Approved') or (step.status == 'Waiting'):
                                step.status = 'NotChecked'
                            if first_changed and (step.step_parent.id in old_new_step):
                                step.step_parent = old_new_step[int(step.step_parent.id)]
                            step.save_with_current_time()
                            if self_looped:
                                step.step_parent = step
                            first_changed = True
                            step.save()
                            old_new_step[old_step_id] = step


@csrf_protect
def retry_slices(request, reqid):
    """
    :param request:
    :param reqid:
    :return:
    """
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = list(map(int,slices))
            _logger.debug(form_request_log(reqid,request,'Retry slices: %s' % str(ordered_slices)))
            ordered_slices.sort()
            retry_action = prepare_slices_to_retry(reqid, ordered_slices)
            apply_retry_action(reqid, retry_action)
            results = {'success':True}
        except Exception as e:
            pass
    return HttpResponse(json.dumps(results), content_type='application/json')


def create_tier0_split_slice(slice_dict, steps_list):
    """
    Create slice in last tier0 request.
    :param slice_dict: Dict, possible keys ['dataset','comment','priority']
    :param steps_list: Dict, possinle keys ['ctag','output_formats','memory','priority'] + StepExecution.TASK_CONFIG_PARAMS
    :return: request number if succeed
    """

    def make_new_slice(slice_dict, last_request):

        if InputRequestList.objects.filter(request=last_request).count() == 0:
            new_slice_number = 0
        else:
            new_slice_number = (InputRequestList.objects.filter(request=last_request).order_by('-slice')[0]).slice + 1
        new_slice = InputRequestList()
        if slice_dict.get('dataset',''):
            new_slice.dataset = fill_dataset(slice_dict['dataset'])
        else:
            raise ValueError('Dataset has to be defined')
        new_slice.input_events = -1
        new_slice.slice = new_slice_number
        new_slice.request = last_request
        new_slice.comment = slice_dict.get('comment','')
        new_slice.priority = slice_dict.get('priority',950)
        new_slice.brief = ' '
        new_slice.save()
        return new_slice
    #create_steps({new_slice_number:step_list}, last_request.reqid, len(StepExecution.STEP)S*[''], 99)
    last_request = (TRequest.objects.filter(request_type='TIER0').order_by('-reqid'))[0]

    parent = None
    # dict of slice and step which are correspond to output
    output_slice_step = {}
    current_slice = make_new_slice(slice_dict, last_request)
    # dict of last step for each slice
    slice_last_step = {}
    for step_dict in steps_list:
        new_step = StepExecution()
        new_step.request = last_request

        new_step.input_events = -1
        if step_dict.get('ctag',''):
            ctag = step_dict.get('ctag','')
        else:
            raise ValueError('Ctag has to be defined for step')
        if step_dict.get('input_format',''):
            if step_dict['input_format'] not in output_slice_step:
                raise ValueError('no parent step found for %s' % step_dict['input_format'])
            else:
                # Check that last step on parent slice is the parent step for this output
                if slice_last_step[output_slice_step[step_dict['input_format']][0].slice] != output_slice_step[step_dict['input_format']][1]:
                    # create a new slice and set a parent step
                    current_slice = make_new_slice(slice_dict,last_request)
                else:
                    current_slice = output_slice_step[step_dict['input_format']][0]
                parent = output_slice_step[step_dict['input_format']][1]
        new_step.slice = current_slice
        if step_dict.get('output_formats',''):
            output_formats = step_dict.get('output_formats','')
        else:
            raise ValueError('output_formats has to be defined for step')
        new_step.priority = step_dict.get('priority', 950)
        memory = step_dict.get('memory', 0)
        new_step.step_template = fill_template('Reco',ctag, new_step.priority, output_formats, memory)
        if ('nFilesPerJob' not in step_dict) and ('nGBPerJob' not in step_dict):
            raise ValueError('nFilesPerJob or nGBPerJob have to be defined')
        for parameter in StepExecution.TASK_CONFIG_PARAMS:
            if parameter in step_dict:
                new_step.set_task_config({parameter:step_dict[parameter]})
        if parent:
            new_step.step_parent = parent
        new_step.status = 'Approved'
        new_step.save_with_current_time()
        if not parent:
            new_step.step_parent = new_step
            new_step.save()
        #fill dict of output-slice dict
        for output_format in output_formats.split('.'):
                output_slice_step[output_format] = (current_slice,new_step)
        parent = new_step

        slice_last_step[current_slice.slice] = new_step
    last_request.cstatus = 'approved'
    last_request.save()
    request_status = RequestStatus(request=last_request,comment='Request approved by Tier0',owner='tier0',
                                   status=last_request.cstatus)
    request_status.save_with_current_time()
    return last_request.reqid



def split_slice_by_tid(reqid, slice_number):
    production_request = TRequest.objects.get(reqid=reqid)
    slice_to_split = InputRequestList.objects.get(request=production_request, slice=slice_number)
    if not slice_to_split.dataset:
        raise  ValueError("Can't split slice by datasets - container name should be saved")
    if 'tid' in slice_to_split.dataset:
        raise  ValueError("Can't split slice by datasets - container name should be saved")
    datasets_events = dataset_events(slice_to_split.dataset)
    datasets_events.sort(key=lambda x:x['events'])
    datasets_events.reverse()
    events_to_proceed = slice_to_split.input_events
    for index, dataset in enumerate(datasets_events):
        new_slice_slice = clone_slices(reqid,reqid,[slice_to_split.slice],-1,False)[0]
        new_event_number = 0
        new_slice = InputRequestList.objects.get(request=production_request, slice=new_slice_slice)
        new_slice.dataset = fill_dataset(dataset['dataset'])
        if events_to_proceed != -1:
            old_event_number = new_slice.input_events
            if index == (len(datasets_events)-1):
                new_event_number = events_to_proceed
            elif dataset['events'] <= events_to_proceed:
                new_event_number = dataset['events']
                events_to_proceed -= dataset['events']
            elif dataset['events'] > events_to_proceed:
                new_event_number = events_to_proceed
                events_to_proceed = 0
            new_slice.input_events = new_event_number
            steps = StepExecution.objects.filter(slice=new_slice,request=production_request)
            for step in steps:
                if step.input_events == old_event_number:
                    step.input_events = new_event_number
                    step.save()
        new_slice.save()



@csrf_protect
def split_slices_by_tid(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Split slices by tid: %s' % str(slices)))
            good_slices = []
            bad_slices = []
            for slice_number in slices:
                try:
                    split_slice_by_tid(reqid,slice_number)
                    good_slices.append(slice_number)
                    splitted_slice = InputRequestList.objects.get(request = reqid, slice = slice_number)
                    splitted_slice.is_hide = True
                    splitted_slice.comment = 'Splitted'
                    splitted_slice.save()
                except  Exception as e:
                    bad_slices.append(slice_number)
                    _logger.error("Problem with slice splitting : %s"%( e))
            if len(bad_slices) > 0:
                results = {'success':False,'badSlices':bad_slices,'goodSlices':good_slices}
            else:
                results = {'success':True,'badSlices':bad_slices,'goodSlices':good_slices}
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')


@csrf_protect
def split_slices_by_output(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Split slices by output: %s' % str(slices)))
            good_slices = []
            bad_slices = []
            for slice_number in slices:
                try:
                    split_by_output(reqid,slice_number)
                    good_slices.append(slice_number)
                except  Exception as e:
                    bad_slices.append(slice_number)
                    _logger.error("Problem with slice splitting : %s"%( e))
            if len(bad_slices) > 0:
                results = {'success':False,'badSlices':bad_slices,'goodSlices':good_slices}
            else:
                results = {'success':True,'badSlices':bad_slices,'goodSlices':good_slices}
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

def split_by_output(reqid,slice_number):
    slice = InputRequestList.objects.get(request=reqid,slice=slice_number)
    ordered_existed_steps, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(slice=slice))
    if not existed_foreign_step and (len(ordered_existed_steps)==2):
        outputs = ordered_existed_steps[0].step_template.output_formats.split('.')
        for output in outputs:
            new_slice = clone_slices(reqid, reqid, [slice.slice], 1, True)[0]
            step = StepExecution.objects.get(slice=InputRequestList.objects.get(request=reqid,slice=new_slice),request=reqid)
            step_new_template = fill_template(step.step_template.step, step.step_template.ctag,
                                              step.step_template.priority, output,
                                              step.step_template.memory)
            step.step_template = step_new_template
            step.set_task_config({'input_format':output})
            step.status = 'NotChecked'
            step.save()
        old_step = ordered_existed_steps[1]
        old_step.status = 'NotCheckedSkipped'
        old_step.save()


def split_slice(reqid, slice_number, divider):
    """
    Create a new slices in reqid with number of events in each slice = total_events in slice / divider
    :param reqid: request id
    :param slice_number: slice number to split
    :param divider: divider count

    """

    def prepare_splitted_slice(slice_to_split, new_slice_number, ordered_existed_steps, index,  new_event_number,
                               output_dataset, nEventsPerInputFile=None):
            new_slice = list(slice_to_split.values())[0]
            new_slice['slice'] = new_slice_number
            del new_slice['id']
            new_slice['input_events'] = new_event_number
            comment = new_slice['comment']
            new_slice['comment'] = comment[:comment.find(')')+1] + '('+str(index)+')' + comment[comment.find(')')+1:]
            new_input_data = InputRequestList(**new_slice)
            if output_dataset:
                new_input_data.dataset = fill_dataset(output_dataset)
            new_input_data.save()
            parent = None
            first_step = True
            for step_dict in ordered_existed_steps:
                current_step = deepcopy(step_dict)
                current_step.slice = new_input_data
                if parent:
                    current_step.step_parent = parent
                current_step.save()
                if first_step:
                    if current_step.status not in ['NotCheckedSkipped','Skipped']:
                        first_step = False
                    current_step.input_events = new_input_data.input_events
                    if nEventsPerInputFile:
                        current_step.set_task_config({'nEventsPerInputFile':nEventsPerInputFile})
                    current_step.save()
                if not parent:

                    current_step.step_parent = current_step
                    current_step.save()
                parent = current_step

    production_request = TRequest.objects.get(reqid=reqid)
    slice_to_split = InputRequestList.objects.filter(request = production_request, slice = slice_number)
    new_slice_number = (InputRequestList.objects.filter(request=production_request).order_by('-slice')[0]).slice + 1
    step_execs = StepExecution.objects.filter(slice=slice_to_split[0],request = production_request)
    ordered_existed_steps, existed_foreign_step = form_existed_step_list(step_execs)
    output_dataset = ''
    nEventsPerInputFile = None
    if existed_foreign_step:
        # get dataset from parent step
        task = ProductionTask.objects.get(step=existed_foreign_step)
        if task.status not in ['finished','done']:
            raise ValueError("Can't split slice - parent task should be finished" )
        output_dataset = task.output_dataset
        nEventsPerInputFile = existed_foreign_step.get_task_config('nEventsPerJob')
    for step in ordered_existed_steps:
            step.id = None
            step.step_parent = step
    if (slice_to_split[0].input_events != -1) and (slice_to_split[0].input_events > divider) and \
            ((int(slice_to_split[0].input_events) // divider) < 200):
        for step_dict in ordered_existed_steps:
            if (step_dict.input_events != slice_to_split[0].input_events) and (step_dict.input_events != -1):
                raise ValueError("Can't split slice wrong event in step %s" % str(step_dict.input_events))
            if step_dict.status in StepExecution.STEPS_APPROVED_STATUS:
                raise ValueError("Can't split slice step %s is approved" % str(step_dict.status))
        for i in range(int(slice_to_split[0].input_events) // int(divider)):
            prepare_splitted_slice(slice_to_split,new_slice_number,ordered_existed_steps, i, divider, output_dataset, nEventsPerInputFile)
            new_slice_number += 1
        if (slice_to_split[0].input_events % divider) != 0:
            prepare_splitted_slice(slice_to_split,new_slice_number,ordered_existed_steps,
                                   int(slice_to_split[0].input_events) // int(divider), slice_to_split[0].input_events % divider, output_dataset, nEventsPerInputFile)
            new_slice_number += 1
    else:
        raise ValueError("Can't split slice total events: %s on %s" % (str(slice_to_split[0].input_events),str(divider)))



@csrf_protect
def split_slices_in_req(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            divider = int(input_dict['divider'])
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Split slices: %s' % str(slices)))
            good_slices = []
            bad_slices = []
            for slice_number in slices:
                try:
                    split_slice(reqid,slice_number,divider)
                    good_slices.append(slice_number)
                    splitted_slice = InputRequestList.objects.get(request = reqid, slice = slice_number)
                    splitted_slice.is_hide = True
                    splitted_slice.comment = 'Splitted'
                    splitted_slice.save()
                except  Exception as e:
                    bad_slices.append(slice_number)
                    _logger.error("Problem with slice splitting : %s"%( e))
            if len(bad_slices) > 0:
                results = {'success':False,'badSlices':bad_slices,'goodSlices':good_slices}
            else:
                results = {'success':True,'badSlices':bad_slices,'goodSlices':good_slices}
        except Exception as e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')



def fix_dima_error():
    inputSlice = InputRequestList.objects.filter(request=1240)
    counter =0
    for i in inputSlice:
        if i.slice > 622:
            step = StepExecution.objects.get(slice=i)
            parent = StepExecution.objects.get(id=step.step_parent.id)
            tasks = ProductionTask.objects.filter(step=parent)
            datasets = None
            for task in tasks:
                if task.status == 'done':
                    datasets= ProductionDataset.objects.filter(task_id=task.id)

            for dataset in datasets:
                if  dataset.name.find('log')==-1:
                    counter +=1
                    print(dataset.name)
                    i.dataset = dataset
                    i.save()
                    step.step_parent = step
                    step.save()
    print(counter)

def console_bulk_requst_steps_update(requests, new_task_config, add_project_mode=None, remove_project_mode=None):
    for request_id in requests:

        steps = StepExecution.objects.filter(request=request_id)
        print(steps)
        for step in steps:
                if new_task_config:
                    step.set_task_config(new_task_config)
                if add_project_mode:
                    new_project_modes = []
                    new_project_modes.append(add_project_mode)
                    for token in step.get_task_config('project_mode').split(';'):
                        if add_project_mode not in token:
                            new_project_modes.append(token)
                    step.set_task_config({'project_mode':';'.join(new_project_modes)})
                if remove_project_mode:
                    new_project_modes = []
                    for token in step.get_task_config('project_mode').split(';'):
                        if remove_project_mode != token:
                            new_project_modes.append(token)
                    step.set_task_config({'project_mode':';'.join(new_project_modes)})
                step.save()


def get_offset_from_jedi(id):
    jedi_task = TTask.objects.get(id=id)
    offset = None
    for value  in jedi_task.jedi_task_parameters['jobParameters']:
        if (value.get('value') == '--randomSeed=${SEQNUMBER}')or(value.get('value') =='randomSeed=${SEQNUMBER}'):
          offset =  value.get('offset')
    return offset

def find_simul_duplicates():
    simul_tasks = list(ProductionTask.objects.filter(Q( status__in=['done','finished'] )&Q(name__startswith='mc')&Q(name__contains='simul')).values('name','id','total_events','inputdataset', 'request_id'))
    sorted_simul_tasks = sorted(simul_tasks, key=lambda x: x['name'])
    first=sorted_simul_tasks[0]
    result_list=[]
    current_list=[]
    for simul_task in sorted_simul_tasks[1:]:
        if simul_task['name']==first['name']:
            if current_list:
                current_list+=[simul_task]
            else:
                current_list=[first,simul_task]
        else:
            if current_list:
               result_list+=[current_list]
               current_list=[]
        first=simul_task
    bad_list = []
    task_id_list = []
    for simul_same_tasks in result_list:
        is_container = False
        is_dataset = False
        requests_set = set()
        for simul_same_task in simul_same_tasks:
            requests_set.add(int(simul_same_task['request_id']))
            if '/' in simul_same_task['inputdataset']:
                is_container = True
            else:
                is_dataset = True
            if (is_container and is_dataset) and (len(requests_set)>1) and (max(requests_set)>1000):
                bad_list.append(simul_same_tasks)
                break
    #print bad_list
    total_events = 0
    requests = set()
    for tasks in bad_list:
        print(tasks[0]['name']+ ' - '+','.join([str(x['id']) for x in tasks])+' - '+','.join([str(x['request_id']) for x in tasks]))
        total_events += sum([x['total_events'] for x in tasks])
        for task in tasks:
            requests.add(task['request_id'])
    print(total_events)
    print(len(bad_list))
    print(requests)


def find_evgen_duplicates():
    evgen_tasks = list(ProductionTask.objects.filter(Q( status__in=['done','finished'] )&Q(name__startswith='mc')&Q(name__contains='evgen')).values('name','id','total_events', 'request_id'))
    sorted_evgen_tasks = sorted(evgen_tasks, key=lambda x: x['name'])
    first=sorted_evgen_tasks[0]
    result_list=[]
    current_list=[]
    for evgen_task in sorted_evgen_tasks[1:]:
        if evgen_task['name']==first['name']:
            if current_list:
                current_list+=[evgen_task]
            else:
                current_list=[first,evgen_task]
        else:
            if current_list:
               result_list+=[current_list]
               current_list=[]
        first=evgen_task
    bad_list = []
    task_id_list = []
    for evgen_same_tasks in result_list:
        offset = [get_offset_from_jedi(evgen_same_tasks[0]['id'])]

        evgen_same_tasks[0]['offset'] = offset[0]
        bad = False
        for evgen_same_task in evgen_same_tasks[1:]:
            current_offset = get_offset_from_jedi(evgen_same_task['id'])
            evgen_same_task['offset'] = current_offset
            if (current_offset!=None) and (current_offset in offset) and (not bad):
                bad_list.append(evgen_same_tasks)
                task_id_list.append(evgen_same_task['id'])
                bad = True
    #print bad_list
    total_events = 0
    requests = set()
    for_group = {}
    reuqest_by_name = {}
    for tasks in bad_list:
        #print tasks[0]['name']+ ' - '+','.join([str(x['id']) for x in tasks])+' - '+','.join([str(x['request_id']) for x in tasks])+' - '+','.join([str(x['offset']) for x in tasks])
        total_events += sum([x['total_events'] for x in tasks])
        task_id_list += [x['id'] for x in tasks if x['offset']==0]
        reuqest_by_name[tasks[0]['name']]=tasks[0]['request_id']
        if tasks[0]['name'].find('Sherpa')>-1:
            for_group[tasks[0]['name']]=[]
        for task in tasks:
            requests.add(task['request_id'])
            if task['offset'] == 0:
                if task['name'] in for_group:
                    for_group[task['name']]+=[task['id']]
    #print for_group

    #task_id_list.sort()
    #print len(bad_list)
    #print requests
    file_for_result_AP=open('/tmp/duplicationDescendAP.txt','w')
    file_for_result_GP=open('/tmp/duplicationDescendGP.txt','w')
    GP_requests = set()
    AP_requests = set()
    for task_group in for_group:
        duplicates, strange = find_task_by_input(for_group[task_group],task_group,reuqest_by_name[task_group])
        for duplicate in duplicates:
            if duplicates[duplicate][1] == 'AP':
                file_for_result_AP.write(str(duplicate)+','+str(duplicates[duplicate][2])+'\n')
                AP_requests.add(duplicates[duplicate][2])
            else:
                file_for_result_GP.write(str(duplicate)+','+str(duplicates[duplicate][2])+'\n')
                GP_requests.add(duplicates[duplicate][2])
        if strange:
            print(strange)
    print(GP_requests)
    print(AP_requests)
    file_for_result_AP.close()
    file_for_result_GP.close()

def find_task_by_input(task_ids, task_name, request_id):
    result_duplicate = []
    print(task_ids)
    for task_id in task_ids:
        task_pattern = '.'.join(task_name.split('.')[:-2]) + '%'+task_name.split('.')[-1] +'%'
        similare_tasks = list(ProductionTask.objects.extra(where=['taskname like %s'], params=[task_pattern]).filter(Q( status__in=['done','finished'] )).values('id','name','inputdataset','provenance','request_id').order_by('id'))
        task_chains = [int(task_id)]
        current_duplicates={int(task_id):(task_name,'AP',request_id)}
        for task in similare_tasks:
            task_input = task['inputdataset']
            #print task_input,'-',task['id']
            if 'py' not in task_input:
                if '/' in task_input:
                    task_chains.append(int(task['id']))
                else:
                    if 'tid' in task_input:
                        task_input_id = int(task_input[task_input.rfind('tid')+3:task_input.rfind('_')])
                        if (task_input_id in task_chains) :
                            task_chains.append(int(task['id']))
                            current_duplicates.update({int(task['id']):(task['name'],task['provenance'],task['request_id'])})
                    else:
                        print('NOn tid:',task_input,task_name)
        result_duplicate.append(current_duplicates)
    first_tasks = result_duplicate[0]
    second_tasks = result_duplicate[1]
    name_set = set()
    not_duplicate = []
    for task_id in first_tasks:
        name_set.add(first_tasks[task_id][0])
    for task_id in list(second_tasks.keys()):
        if second_tasks[task_id][0] not in name_set:
            not_duplicate.append({task_id:second_tasks[task_id]})
    return second_tasks,not_duplicate

def find_downstreams_by_task(task_id):
    result_duplicate = []
    original_task = ProductionTask.objects.get(id=task_id)
    task_name = original_task.name
    task_pattern = '.'.join(task_name.split('.')[:-2]) + '%'+task_name.split('.')[-1] +'%'
    similare_tasks = list(ProductionTask.objects.extra(where=['taskname like %s'], params=[task_pattern]).
                          filter(Q( status__in=['done','finished','obsolete'] )).values('id','name','inputdataset','provenance','request_id','status').
                          order_by('id'))
    task_chains = [int(task_id)]
    current_duplicates={int(task_id):(task_name,original_task.provenance,original_task.request_id,'done')}
    for task in similare_tasks:
        task_input = task['inputdataset']
        #print task_input,'-',task['id']
        if 'py' not in task_input:
            if ('/' in task_input) and (int(task['id'])>int(task_id)):
                task_chains.append(int(task['id']))
                current_duplicates.update({int(task['id']):(task['name'],task['provenance'],task['request_id'],task['status'])})
                if (task['request_id'] != original_task.request_id) and (task['provenance']=='AP'):
                    print('Simul problem' +'-'+ task_name + '-' + task['name'])
                #print task_input,int(task['id'])
            else:
                if 'tid' in task_input:
                    task_input_id = int(task_input[task_input.rfind('tid')+3:task_input.rfind('_')])
                    #print task_input_id,task['id'], len(task_chains)

                    if (task_input_id in task_chains) :
                        task_chains.append(int(task['id']))
                        current_duplicates.update({int(task['id']):(task['name'],task['provenance'],task['request_id'],task['status'])})
                else:
                    print('NOn tid:',task_input,task_name)

    return current_duplicates


@csrf_protect
def test_tasks_from_slices(request):
    results = {'success':False}
    if request.method == 'POST':
        try:
            tasks = request.session['selected_tasks']
            print(tasks)
            results = {'success':True}
        except Exception as e:
            pass
    return HttpResponse(json.dumps(results), content_type='application/json')



def slices_range_to_str(slices):
    return_string = hex(slices[0])[2:]
    last_value = slices[0]
    is_chain = False
    for slice in slices[1:]:
        if (slice-last_value) == 1:
            is_chain = True
        else:
            if is_chain:
                return_string += 'x'+hex(last_value)[2:]
            is_chain = False
            return_string += 'y'+hex(slice)[2:]
        last_value = slice
    if is_chain:
        return_string += 'x'+hex(last_value)[2:]
    return_string += 'y'
    return return_string

@csrf_protect
def form_tasks_from_slices(request,request_id):
    results = {'success':False}
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = list(map(int,slices))
            ordered_slices.sort()


            results = {'success':True, 'slices_range': slices_range_to_str(ordered_slices)}
        except Exception as e:
            pass
    return HttpResponse(json.dumps(results), content_type='application/json')



def find_identical_step(step):
    pass


def bulk_obsolete_from_file(file_name):
    with open(file_name,'r') as input_file:
        tasks = (int(line.split(',')[0]) for line in input_file if line)
        print(timezone.now())
        for task_id in tasks:
            task = ProductionTask.objects.get(id=task_id)
            if task.status in ['finished','done']:
                task.status='obsolete'
                task.timestamp=timezone.now()
                task.save()
                print(task.name, task.status)

def bulk_find_downstream_from_file(file_name, output_file_name, provenance='AP', start_request=0):
    with open(file_name,'r') as input_file:
        tasks = (int(line.split(',')[0]) for line in input_file if line)
        output_file = open(output_file_name,'w')
        for task_id in tasks:
            downstream_tasks  = find_downstreams_by_task(task_id)
            for duplicate in sorted(downstream_tasks):
                if ((downstream_tasks[duplicate][1] == provenance) and (downstream_tasks[duplicate][3] != 'obsolete')) \
                        and (int(downstream_tasks[duplicate][2])>start_request):
                    output_file.write(str(task_id)+','+str(downstream_tasks[duplicate][0])+','
                                      +str(duplicate)+','+str(downstream_tasks[duplicate][2])+'\n')
        output_file.close()


def fix_wrong_parent(reqid):
    steps = StepExecution.objects.filter(request=reqid)
    for step in steps:
        if step.step_parent.step_template.ctag[0] == 't':
            new_step_parent = step.step_parent.step_parent
            step.step_parent = new_step_parent
            step.save()


def find_retried(file_name):
    with open(file_name,'r') as input_file:
        tasks = (int(line.split(',')[0]) for line in input_file if line)
        request_slices = {}

        for task_id in tasks:
            task = ProductionTask.objects.get(id=task_id)
            if task.request_id not in request_slices:
                request_slices[task.request_id] = []
                current_slices = list(InputRequestList.objects.filter(request=task.request_id ))
                for slice in current_slices:
                    try:
                        step = StepExecution.objects.get(slice=slice)
                        project_mode = step.get_task_config('project_mode')
                        if 'skipFilesUsedBy' in project_mode:
                            request_slices[task.request_id].append({'id':slice.id,
                                                                    'parent_task':int(project_mode[project_mode.find('skipFilesUsedBy')+len('skipFilesUsedBy='):project_mode.find('skipFilesUsedBy')+len('skipFilesUsedBy=')+7])})
                    except:
                        pass



            print(task_id,task.status, end=' ')
            slice_ids = [x['id'] for x in request_slices[task.request_id] if x['parent_task']==int(task_id)]
            for slice_id in slice_ids:
                try:
                    child_task = ProductionTask.objects.get(step=StepExecution.objects.get(slice=slice_id))
                    print(int(child_task.id),child_task.status, end=' ')
                except:
                    pass
            print('')

def reshuffle_slices(reqid,starting_slice=0):
    inputs = list(InputRequestList.objects.filter(request=reqid,slice__gte=starting_slice).order_by('slice'))
    i=starting_slice
    for slice in inputs:
        if slice.slice != i:
            slice.slice = i
            slice.save()
        i+=1



def clean_open_request(reqid,starting_slice=0):
    inputs = list(InputRequestList.objects.filter(request=reqid))
    uniq_datasets = set()
    duplicated_slices = []
    slices = []
    for current_slice in inputs:
        if (current_slice.dataset[current_slice.dataset.find(':')+1:] in uniq_datasets) and (current_slice.slice>=starting_slice):
            slices.append(current_slice.id)
            duplicated_slices.append(current_slice)
        uniq_datasets.add(current_slice.dataset[current_slice.dataset.find(':')+1:])
    slices.sort()
    #print slices[:30]
    if not duplicated_slices:
        return False
    for slice in duplicated_slices:
        steps = list(StepExecution.objects.filter(slice=slice))
        if len(steps) == 0:
            slice.delete()
        elif len(steps) == 1:
            step=steps[0]
            if (step.status == 'NotChecked') or (step.status == 'NotCheckedSkipped'):
                step.delete()
                slice.delete()
                print(slice.slice)
    reshuffle_slices(reqid,starting_slice)
    return True


@csrf_protect
def change_parent(request, reqid, new_parent):
    """
    :param request:
    :param reqid:
    :return:
    """
    results = {'success':False}
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = list(map(int,slices))
            _logger.debug(form_request_log(reqid,request,'Change parent for slices: %s to %s' % (str(ordered_slices),str(new_parent))))
            ordered_slices.sort()
            for slice in ordered_slices:
                change_slice_parent(reqid,slice,new_parent)
            request.session['selected_slices'] = list(map(int,slices))
            results = {'success':True}
        except Exception as e:
            _logger.error('Problem with set new parent: %s',str(e))
    return HttpResponse(json.dumps(results), content_type='application/json')


def change_slice_parent(request,slice,new_parent_slice):
    step_execs = StepExecution.objects.filter(slice=InputRequestList.objects.get(request=request,slice=slice),request=request)
    ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
    step = ordered_existed_steps[0]
    if  int(new_parent_slice) == -2:
        slice = InputRequestList.objects.get(request=request, slice=slice)
        slice.dataset = None
        slice.save()
        return
    if parent_step and (step.status not in StepExecution.STEPS_APPROVED_STATUS):
        if int(new_parent_slice) == -1:
            step.step_parent = step
            step.save()
            return
        step_execs_old_parent = StepExecution.objects.filter(slice=parent_step.slice,request=parent_step.request)
        ordered_existed_steps_old_parent, parent_step_temp = form_existed_step_list(step_execs_old_parent)
        step_index = ordered_existed_steps_old_parent.index(parent_step)
        step_execs_parent = StepExecution.objects.filter(slice=InputRequestList.objects.get(request=parent_step.request,slice=int(new_parent_slice)))
        ordered_existed_steps_parent, parent_step_parent = form_existed_step_list(step_execs_parent)
        if step_index < len(ordered_existed_steps_parent):
            step.step_parent = ordered_existed_steps_parent[step_index]
            step.save()
            changed_slice = step.slice
            changed_slice.dataset = step.step_parent.slice.dataset
            changed_slice.input_data = step.step_parent.slice.input_data
            changed_slice.input_events = step.step_parent.slice.input_events
            changed_slice.save()
    else:
        if int(new_parent_slice) == -1:
            slice = InputRequestList.objects.get(request=request, slice=slice)
            slice.dataset = None
            slice.save()
            return

def delete_empty_slice(slice):
    steps = list(StepExecution.objects.filter(slice=slice))
    if len(steps) == 0:
        slice.delete()
    elif len(steps) == 1:
        step=steps[0]
        if (step.status == 'NotChecked') or (step.status == 'NotCheckedSkipped'):
            step.delete()
            slice.delete()

def remove_dubl_slices(reqid):
    slices = list(InputRequestList.objects.filter(request=reqid).order_by('-slice'))
    prev_slice = slices[0]
    for slice in slices[1:]:
        if slice.slice == prev_slice.slice:
            print(slice.slice)
            #delete_empty_slice(prev_slice)
            #delete_empty_slice(slice)


def find_project_mode(project_mode, request_type):
    result = {}
    ignoreStep = StepExecution.objects.filter(task_config__contains=project_mode)
    for step in ignoreStep:
        if not int(step.request_id) in result:
            if step.request.request_type == request_type:
                result[int(step.request_id)] = []
        if int(step.request_id) in result:
            if ProductionTask.objects.filter(step=step).exists():
                if not step.slice.is_hide:
                    result[step.request_id].append((step.step_template.ctag,step.slice.slice))
    return result


def merge_rest_events(original_task_id, new_request_id):
    task = ProductionTask.objects.get(id=original_task_id)
    new_request = TRequest.objects.get(reqid=new_request_id)
    original_request = task.request
    if (original_request.project == new_request.project):
        new_slice = clone_slices(original_request.reqid,new_request_id,[task.step.slice.slice],0,True,False,False,{},2)[0]
        slice = InputRequestList.objects.get(request=new_request,slice=new_slice)
        slice.dataset = fill_dataset(task.primary_input)
        slice.input_events = -1
        slice.save()
        steps = StepExecution.objects.filter(slice=slice)
        for step in steps:
            step.input_events = -1
            step.save()
    else:
        return original_task_id


def check_campaign(reqid, rucio_campaign):
    request = TRequest.objects.get(reqid=reqid)
    if (request.request_type != 'MC'):
        return True
    if not rucio_campaign:
        rucio_campaign='mc15:mc15a'
    subcampaign = request.subcampaign.lower()
    if 'mc16' not in subcampaign:
        return True
    if ('mc16' not in rucio_campaign.lower()) or (':' not in rucio_campaign):
        if subcampaign == 'mc16a':
            return True
        else:
            return False
    else:
        rucio_subcampaign = rucio_campaign.split(':')[1].lower()
        if subcampaign == rucio_subcampaign:
            return True
        elif (subcampaign == 'mc16d') and (rucio_subcampaign == 'mc16c'):
            return True
        else:
            return False


@csrf_protect
def dataset_slice_info(request, reqid, slice_number):
    if request.method == 'GET':
        results = {'success':False}
        try:

            results = {'success': True,'data': find_slice_dataset_info(reqid, slice_number)}
        except Exception as e:
            _logger.error("Problem with getting dataset info #%i %i: %s"%(int(reqid),int(slice_number),e))
        return HttpResponse(json.dumps(results), content_type='application/json')



def find_slice_dataset_info(reqid, slice_number):
    slice = InputRequestList.objects.get(request=reqid,slice=slice_number)
    container = slice.dataset
    dataset_events_list = dataset_events_ddm(container)
    steps = list(StepExecution.objects.filter(slice=slice,request=reqid))
    # ordered_existed_steps, parent_step = form_existed_step_list(steps)
    # tag = None
    # for step in ordered_existed_steps:
    #     if step.status  not in ['NotCheckedSkipped','Skipped']:
    #         tag = step.step_template.ctag
    #         break
    tasks = list(ProductionTask.objects.filter(step__in=steps))
    result_data = []
    for dataset in dataset_events_list:

        current_tasks = []
        for task in tasks:
            if (task.primary_input.split('.')[0]+':'+task.primary_input) == dataset['dataset']:
                current_tasks.append(int(task.id))
        other_tasks = []
        # if tag:
        #     for task in ProductionTask.objects.filter(ami_tag=tag,primary_input=dataset['dataset'].split(':')[1]):
        #         if (task.status not in (['obsolete']+ProductionTask.RED_STATUS)) and (int(task.id) not in current_tasks):
        #             other_tasks.append(int(task.id))
        current_campaign = 'NotCurrentCampaign'
        if check_campaign(reqid,dataset['metadata']['campaign']):
            current_campaign = 'CurrentCampaign'
        result_data.append({'dataset':dataset['dataset'],'events':dataset['events'],'input_task':dataset['metadata']['task_id'],'SubCampaing':dataset['metadata']['campaign'],
        'Tasks':current_tasks,'otherTasks':other_tasks,'currentCampaign':current_campaign})
    return result_data



def find_missing_tasks(req_id):
    slices = list(InputRequestList.objects.filter(request=req_id))
    slice_to_copy = []
    for slice in slices:
        if(not slice.is_hide)and(slice.slice != 155):
            dataset_info = find_slice_dataset_info(req_id, slice.slice)
            for slice_dataset in dataset_info:
                if (slice_dataset['currentCampaign']=='CurrentCampaign') :
                    steps  = list(StepExecution.objects.filter(slice=slice))
                    ordered_existed_steps, parent_step = form_existed_step_list(steps)
                    tag = None
                    events_per_input = 0
                    for step in ordered_existed_steps:
                        if step.status  not in ['NotCheckedSkipped','Skipped']:
                            tag = step.step_template.ctag
                            events_per_input = step.get_task_config('nEventsPerInputFile')
                            break
                    task_exists = False
                    for task in ProductionTask.objects.filter(ami_tag=tag,primary_input=slice_dataset['dataset'].split(':')[1]):
                        if (task.status not in (['obsolete']+ProductionTask.RED_STATUS)):
                           task_exists = True

                    if not task_exists:

                        task_id = int(slice_dataset['dataset'][slice_dataset['dataset'].rfind('tid')+3:slice_dataset['dataset'].rfind('_')])
                        if int(ProductionTask.objects.get(id=task_id).step.get_task_config('nEventsPerJob')) == int(events_per_input):
                            slice_to_copy.append((int(slice.slice),slice_dataset['dataset']))
    return slice_to_copy

def clone_missing_tasks_slices(req_id):
    result = find_missing_tasks(req_id)
    for slice_dataset in result:

        new_slice_number = clone_slices(req_id,req_id,[slice_dataset[0]],-1,False)[0]
        new_slice = InputRequestList.objects.get(request=req_id, slice=new_slice_number)
        new_slice.dataset = fill_dataset(slice_dataset[1])
        new_slice.save()


def set_sample_offset(parent_request, child_request, slices=None):
    if slices:
        child_slices = InputRequestList.objects.filter(request=child_request, slice__in=slices)
    else:
        child_slices = InputRequestList.objects.filter(request=child_request)
    parent_slices = list(InputRequestList.objects.filter(request=parent_request))
    parent_slice_dict = {}
    for slice in parent_slices:
        if slice.dataset and ProductionTask.objects.filter(step__in=list(StepExecution.objects.filter(slice=slice))).exists():
            parent_slice_dict[slice.dataset] = parent_slice_dict.get(slice.dataset,[]) + [slice]
    for slice in child_slices:
        if not slice.is_hide and not(ProductionTask.objects.filter(step__in=list(StepExecution.objects.filter(slice=slice))).exists()):
            offset = 0
            steps = StepExecution.objects.filter(slice=slice)
            ordered_steps, parent_step = form_existed_step_list(steps)
            first_step_index = -1
            step_to_change = None
            for index,step in enumerate(ordered_steps):
                if step.status == 'NotChecked':
                    first_step_index = index
                    step_to_change = step
                    break
            for parent_slice in parent_slice_dict.get(slice.dataset,[]):
                steps = StepExecution.objects.filter(slice=parent_slice)
                ordered_parent_steps, parent_step = form_existed_step_list(steps)
                if (first_step_index != -1) and (first_step_index < len(ordered_parent_steps)):
                    if ProductionTask.objects.filter(step=ordered_parent_steps[first_step_index]).exists():
                        for task in ProductionTask.objects.filter(step=ordered_parent_steps[first_step_index]):
                            jedi_task = TTask.objects.get(id=task.id)
                            offset += jedi_task.jedi_task_parameters.get('nFiles',0)
            if offset > 0:
                print(step_to_change.step_template.ctag, offset+1, slice.slice)
                step_to_change.update_project_mode('primaryInputOffset',offset+1)
                step_to_change.save()

def recursive_delete(step_id, do_delete=False):
    step_tree = []
    not_root = True
    while not_root:
        step=StepExecution.objects.get(id=step_id)
        step_tree.append(step.id)
        step_id = step.step_parent_id
        not_root = not(step.step_parent_id == step.id)
    print(step_tree)
    if do_delete:
        for step_id in step_tree:
            step = StepExecution.objects.get(id=step_id)
            step.step_parent=step
            step.save()
            step.delete()

#
# def find_old_transient(from_id, to_id, gsp_key, sheet):
#     tasks = list(ProductionTask.objects.filter(id__lte=to_id, id__gte=from_id, status__in=['done', 'finished'],
#                                           provenance='AP', name__contains='merge', name__startswith='mc'))
#     full_tasks = []
#     for task in tasks:
#         if task.total_files_finished == task.total_files_tobeused:
#             if task.primary_input.split('.')[-2] in task.output_formats.split('.'):
#                 full_tasks.append(task.primary_input)
#     to_delete = []
#     suspicios = []
#     not_in_db = 0
#     deleted = 0
#     ddm = DDM()
#     for dataset in full_tasks:
#         try:
#             metadata = ddm.dataset_metadata(dataset)
#             to_delete.append((dataset,metadata['transient'],metadata['bytes']))
#         except DataIdentifierNotFound:
#             deleted +=1
#             # if 'tid' not in dataset:
#             #     print dataset
#             # else:
#             #     if not ProductionDataset.objects.get(name=dataset).ddm_timestamp:
#             #         not_in_db +=1
#         except Exception,e:
#             print e
#             suspicios.append(dataset)
#     gsp = GSP()
#     print 'Deleted %s/%s/%s'%(len(to_delete),not_in_db,deleted)
#     gsp.update_spreadsheet(gsp_key, sheet, 'A:C', to_delete, True)
#
#
# def set_transient(gsp_key, sheet, start_liftime, increase_delta):
#     gsp = GSP()
#     values =  gsp.get_spreadsheet(gsp_key,sheet,'A:A')
#     datasets = [x[0] for x in values['values']]
#     liftime = start_liftime
#     counter = 0
#     ddm = DDM()
#     for dataset in datasets:
#         counter += 1
#         if counter > increase_delta:
#             liftime += 5
#             counter = 0
#         print dataset,liftime
#         try:
#             ddm.setLifeTimeTransientDataset(dataset,liftime)
#         except Exception,e:
#             print dataset,str(e)
@api_view(['GET'])
def request_train_patterns(request, reqid):
    train_pattern_list= []
    try:
        cur_request = TRequest.objects.get(reqid=reqid)
        if cur_request.request_type == 'MC':
            pattern_type = 'mc_pattern'
        else:
            pattern_type = 'data_pattern'
        trains = TrainProduction.objects.filter(status=pattern_type).order_by('id')
        for train in trains:
            train_pattern_list.append({'train_id': train.id, 'name': '(' + str(train.pattern_request.reqid) +
                                                                             ')' +
                                                                             train.pattern_request.description})
    except Exception as e:
        content = str(e)
        return Response(content,status=500)

    return Response(train_pattern_list)


def convert_old_patterns(request_id):
    def unwrap(pattern_dict):
        return_list = []
        if type(pattern_dict) == dict:
            for key in pattern_dict:
                if key != 'ctag':
                    return_list.append((key,pattern_dict[key]))
            return pattern_dict.get('ctag',''), return_list
        else:
            return pattern_dict,[('ctag',pattern_dict)]
    def check_empty_pattern(pattern, default_pattern):
        for x in list(pattern.keys()):
            if pattern[x]:
                return pattern
        return default_pattern
    production_request = TRequest.objects.get(reqid=request_id)
    pattern_list = MCPattern.objects.filter(pattern_status='IN USE').order_by('pattern_name')
    pattern_list_name = [(x.pattern_name,unwrap(json.loads(x.pattern_dict))) for x in pattern_list]
    #return pattern_list_name
    for pattern in pattern_list_name:
        new_slice_id = clone_slices(request_id,request_id,[0],-1,False)[0]
        slice = InputRequestList.objects.get(id =new_slice_id)
        slice.comment = pattern[0]
        for step in pattern[1]:
            pass


def obsolete_deleted_tasks(production_request):
    tasks = ProductionTask.objects.filter(request=production_request,status__in = ['finished','done'])
    ddm = DDM()
    for task in tasks:
        output_datasets = ProductionDataset.objects.filter(task_id=task.id)
        to_delete = True
        for output_datset in output_datasets:
            if ('log' not in output_datset.name) and ddm.dataset_exists(output_datset.name):
                to_delete = False
        if to_delete:
            print(task.id,task.name)
            _do_deft_action('mborodin', int(task.id), 'obsolete')


def fix_fahui_error(request_id, tasks):
    request = TRequest.objects.get(reqid=request_id)
    cloned_slices = [x.cloned_from_id for x in InputRequestList.objects.filter(request=request) if not(x.is_hide)]
    request_to_approve = False
    tasks.sort()
    fixed_slices  = []
    print('Request:',request_id)
    for task_id in tasks:
        task = ProductionTask.objects.get(request=request, id=task_id)
        slice = task.step.slice
        # if slice.id in cloned_slices:
        #     print('Aready cloned:',request.reqid)
        #     continue
        # if slice.id in fixed_slices:
        #     continue
        step_execs = StepExecution.objects.filter(request=request, slice=slice)
        ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
        if request.request_type == 'MC':
            STEPS = StepExecution.STEPS
        else:
            STEPS = [''] * len(StepExecution.STEPS)
        step_as_in_page = form_step_in_page(ordered_existed_steps, STEPS, parent_step)
        step_index = step_as_in_page.index(task.step)
        # print(slice.slice,step_index)
        fixed_slices.append(slice.id)
        new_slice = clone_slices(request.reqid,request.reqid,[slice.slice],step_index,True, True)[0]
        new_steps = StepExecution.objects.filter(request=request, slice=InputRequestList.objects.get(request=request,slice=new_slice))
        for new_step in new_steps:
            new_step.status = 'Approved'
            new_step.save()
        request_to_approve = True
    if request_to_approve:
        set_request_status('cron', request.reqid, 'approved', 'Automatic fix approve',
                           'Request was automatically approved')


def fix_clone_for_step(request):
    for slice in InputRequestList.objects.filter(request=request):
        if not slice.is_hide:
            step_execs = StepExecution.objects.filter(request=request, slice=slice)
            ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
            if not slice.cloned_from and parent_step:
                if(parent_step.request==slice.request):
                    print(slice.slice,parent_step.slice.slice)
                    slice.cloned_from = parent_step.slice
                    slice.save()



def compare_two_slices(production_request, slice1, slice2):
    steps1 = StepExecution.objects.filter(request=production_request, slice=InputRequestList.objects.get(request=production_request,slice=slice1))
    ordered_existed_steps1, parent_step = form_existed_step_list(steps1)
    steps2 = StepExecution.objects.filter(request=production_request, slice=InputRequestList.objects.get(request=production_request,slice=slice2))
    ordered_existed_steps2, parent_step = form_existed_step_list(steps2)
    if len(ordered_existed_steps1)!=len(ordered_existed_steps2):
        print('different step number')
        return False
    is_equael = True
    for index, step1 in enumerate(ordered_existed_steps1):
        step1_dict = step1.__dict__
        step2_dict =  ordered_existed_steps2[index].__dict__
        for x in step1_dict.keys():
            if x not in ['id','task_config','_state','slice_id','step_def_time','step_parent_id']:
                if step1_dict[x] != step2_dict[x]:
                   print('%s different for %i step'%(x,index))
                   is_equael = False
        task_config1 = step1.get_task_config()
        task_config2 = ordered_existed_steps2[index].get_task_config()
        if len(task_config1.keys()) !=  len(task_config2.keys()) :
             print('different task config length for step %i,%i,%i'%(index,step1_dict['id'],step2_dict['id']))
             is_equael = False
        for x in   task_config1.keys():
            if (x not in task_config2) or ( task_config1[x] !=   task_config2[x] ) :
                print('%s different for %i step %i,%i'%(x,index, step1_dict['id'],step2_dict['id']))
                is_equael = False
    return   is_equael


def check_all_outputs_deleted(task, ddm):
    number_of_outputs = len(task.output_formats.split('.'))
    output_datasets = ProductionDataset.objects.filter(task_id=task.id)
    to_delete = True
    for output_datset in output_datasets:
        if '.log.' not in output_datset.name:
            number_of_outputs -= 1
            if ddm.dataset_exists(output_datset.name):
                to_delete = False
    return to_delete and number_of_outputs == 0

def obsolete_old_task_for_slice(request_id, slice_number, ddm):
    slice = InputRequestList.objects.get(slice=slice_number, request=request_id)
    input_container = slice.dataset
    step_execs = StepExecution.objects.filter(request=request_id, slice=slice)
    ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
    first_step_tag = None
    number_of_obsolete_tasks = 0
    for step in ordered_existed_steps:
        if step.status in ['NotChecked','Approved']:
            if ProductionTask.objects.filter(request=request_id, step=step).exists():
                return 0
            first_step_tag = step.step_template.ctag
            break
    datasets = ddm.dataset_in_container(input_container)
    tasks = []
    if first_step_tag:
        tasks = list(ProductionTask.objects.filter(primary_input=input_container, ami_tag=first_step_tag))
        for dataset in datasets:
            tasks += list(ProductionTask.objects.filter(primary_input=dataset.split(':')[1], ami_tag=first_step_tag))
    for task in tasks:
        if task.status in ['finished','done','obsolete']:
            to_delete = check_all_outputs_deleted(task, ddm)
            if to_delete:
                merge_is_empty = True
                for child_task in ProductionTask.objects.filter(parent_id=task.id):
                    if child_task.status in ['finished','done'] and '.merge.' in child_task.name:
                        if check_all_outputs_deleted(child_task, ddm):
                            _logger.info('Obsolecence: {taskid} is obsolete because all output is deleted'.format(taskid=task.id))
                            number_of_obsolete_tasks += 1
                            _do_deft_action('mborodin', int(child_task.id), 'obsolete')
                        else:
                            merge_is_empty = False
                            break
                if merge_is_empty:
                    if task.status != 'obsolete':
                        _logger.info('Obsolecence: {taskid} is obsolete because all output is deleted'.format(taskid=task.id))
                        number_of_obsolete_tasks += 1
                        _do_deft_action('mborodin', int(task.id), 'obsolete')
    return number_of_obsolete_tasks

@csrf_protect
def obsolete_old_deleted_tasks(request, reqid):
    if request.method == 'POST':
        try:
            if ('MCCOORD' in egroup_permissions(request.user.username)) or request.user.is_superuser :
                _set_request_hashtag(reqid,'MCDeletedReprocessing')
                data = request.body
                input_dict = json.loads(data)
                slices = input_dict['slices']
                if '-1' in slices:
                    del slices[slices.index('-1')]
                _logger.debug(form_request_log(reqid,request,'Obsolete old tasks with deleted output slices: %s' % str(slices)))
                ddm = DDM()
                number_of_tasks = 0
                for slice_number in slices:
                    try:
                        number_of_tasks+= obsolete_old_task_for_slice(reqid,slice_number,ddm)
                    except Exception as e:
                        _logger.error("Problem with deleted output tasks obsolete : %s"%( e))
                results = {'success':True,'tasksObsolete':number_of_tasks}
            else:
                return HttpResponseForbidden('This action is only for MC COORD')
        except Exception as e:
            _logger.error("Problem with deleted output tasks obsolete : %s"%( e))
            return HttpResponseBadRequest(e)
        return HttpResponse(json.dumps(results), content_type='application/json')


@api_view(['GET'])
def celery_task_status(request, celery_task_id):
    celery_task = AsyncResult(celery_task_id)
    progress = 0
    result = None
    celery_task_name = 'Unknown'
    if (celery_task.status == 'PROGRESS') and celery_task.info:
        if 'name' in celery_task.info:
            celery_task_name = celery_task.info.get('name')
        if 'progress' in celery_task.info:
            progress = celery_task.info.get('progress')
        if ('processed' in celery_task.info) and ('total' in celery_task.info):
            progress = celery_task.info.get('processed') * 100 // celery_task.info.get('total')
    if (celery_task.status == 'SUCCESS'):
        result = celery_task.result
    return Response({'status':celery_task.status, 'progress': progress, 'result':result, 'celery_task_name':celery_task_name})

@api_view(['POST'])
def test_celery_task(request, reqid):
    return_value = single_request_action_celery_task(reqid,test_async_progress,'test',request.user.username,request.data['text'])
    return Response(return_value)



@api_view(['GET'])
def input_with_slice_errors(request, reqid):
    slice_erros = SliceError.objects.filter(request=reqid,is_active=True)
    result_list = []
    for slice_error in slice_erros:
        result_list.append(slice_error.slice.dataset)
    return Response({'input_errors':result_list})