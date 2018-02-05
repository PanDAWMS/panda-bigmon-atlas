from _ast import In

from django.core.mail import send_mail
from django.forms import model_to_dict
import json
import logging
import os
from copy import deepcopy
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response, redirect
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie
from django.core.exceptions import ObjectDoesNotExist
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from django.utils import timezone
import time

from atlas.getdatasets.models import TRequest
from atlas.prodtask.ddm_api import tid_from_container
from atlas.prodtask.helper import form_request_log
from atlas.prodtask.models import TRequest, RequestStatus, InputRequestList, StepExecution, get_priority_object, \
    HashTagToRequest, ProductionTask, MCPattern, ParentToChildRequest, HashTag

from atlas.prodtask.spdstodb import fill_template
from ..prodtask.helper import form_request_log
from ..prodtask.settings import APP_SETTINGS
from ..prodtask.ddm_api import find_dataset_events



import atlas.datatables as datatables

from .models import StepTemplate, StepExecution, InputRequestList, TRequest, MCPattern, Ttrfconfig, ProductionTask, \
    get_priority_object, ProductionDataset, RequestStatus, get_default_project_mode_dict, get_default_nEventsPerJob_dict, \
    OpenEndedRequest, TrainProduction, ParentToChildRequest, TProject
from .spdstodb import fill_template

from django.db.models import Count, Q
from django.contrib.auth.decorators import login_required

_logger = logging.getLogger('prodtaskwebui')






def step_approve(request, stepexid=None, reqid=None, sliceid=None):
    if request.method == 'GET':
        try:
            choosen_step = StepExecution.objects.get(id=stepexid)
            if (choosen_step.step_template.step != 'Evgen'):
                steps_for_approve = [choosen_step]
            else:
                cur_slice = InputRequestList.objects.get(id=sliceid)
                cur_request = TRequest.objects.get(reqid=reqid)
                steps_for_approve = StepExecution.objects.all().filter(request=cur_request, slice=cur_slice)
            for st in steps_for_approve:
                st.status = 'Approved'
                st.save()
        except Exception, e:
            #print e
            return HttpResponseRedirect(reverse('prodtask:step_execution_table'))
    return HttpResponseRedirect(reverse('prodtask:step_execution_table'))


def find_missing_tags(tags):
    return_list = []
    for tag in tags:
        try:
            if int(tag[1:])==9999:
                return_list.append(tag)
            else:
                trtf = Ttrfconfig.objects.all().filter(tag=tag.strip()[0], cid=int(tag.strip()[1:]))
                if not trtf:
                    if (tag[0]=='r') and (int(tag[1:])<6000):
                        return_list.append(tag)
                    else:
                        pass
        except ObjectDoesNotExist,e:
                pass
        except Exception,e:
            raise e

    return return_list


def step_status_definition(is_skipped, is_approve=True, is_waiting=False):
    if is_waiting:
        return 'Waiting'
    if is_skipped and is_approve:
        return 'Skipped'
    if not(is_skipped) and is_approve:
        return 'Approved'
    if is_skipped and not(is_approve):
        return 'NotCheckedSkipped'
    if not(is_skipped) and not(is_approve):
        return 'NotChecked'


def form_existed_step_list(step_list):
    result_list = []
    temporary_list = []
    another_chain_step = None
    for step in step_list:
        if step.step_parent == step:
            if result_list:
                raise ValueError('Not linked chain')
            else:
                result_list.append(step)
        else:
           temporary_list.append(step)
    if not result_list:
        for index,current_step in enumerate(temporary_list):
            if current_step.step_parent not in temporary_list:
                # step in other chain
                another_chain_step = current_step.step_parent
                result_list.append(current_step)
                temporary_list.pop(index)
                break
    for i in range(len(temporary_list)):
        j = 0
        while (temporary_list[j].step_parent!=result_list[-1]):
            j+=1
            if j >= len(temporary_list):
                raise ValueError('Not linked chain')
        result_list.append(temporary_list[j])
    return (result_list,another_chain_step)


def similar_status(status, is_skipped):
    return ((((status ==  'Skipped') or (status ==  'NotCheckedSkipped')) and is_skipped) or
            (((status ==  'Approved') or (status ==  'NotChecked')) and not is_skipped))



def step_is_equal(step_value, existed_step):
    if step_value['formats']:
        return (existed_step.step_template.output_formats == step_value['formats']) and \
               (existed_step.step_template.ctag == step_value['value']) and similar_status(existed_step.status,step_value['is_skipped'])
    else:
        return (existed_step.step_template.ctag == step_value['value']) and similar_status(existed_step.status,step_value['is_skipped'])


def approve_existed_step(step, new_status):
    if not (step.status == 'Approved') and not (step.status == 'Skipped'):
        if step.status != new_status:
            step.status = new_status
            step.save_with_current_time()
    pass


#TODO: Change it to real dataset workflow
def fill_dataset(ds):
    dataset = None
    try:
        dataset = ProductionDataset.objects.all().filter(name=ds)[0]
    except:
        pass
    finally:
        if dataset:
            return dataset
        else:
            dataset = ProductionDataset.objects.create(name=ds, files=-1, timestamp=timezone.now())
            dataset.save()
            return dataset

def form_step_in_page(ordered_existed_steps,STEPS, is_foreign):
    if STEPS[0]:
        return_list = []
        i = 0
        if len(ordered_existed_steps)==0:
            return [None]*len(STEPS)

        for STEP_NAME in STEPS:
            if i >= len(ordered_existed_steps):
                return_list.append(None)
            else:
                if STEP_NAME == ordered_existed_steps[i].step_template.step:
                    return_list.append(ordered_existed_steps[i])
                    i += 1
                else:
                    return_list.append(None)
        if len(ordered_existed_steps)!=i:
            raise ValueError('Not consistent chain')
        return return_list
    else:
        if is_foreign:
            return [None]+ordered_existed_steps+[None]*(len(STEPS)-len(ordered_existed_steps)-1)
        else:
            return ordered_existed_steps+[None]*(len(STEPS)-len(ordered_existed_steps))




#TODO: FIX it. Make one commit
def create_steps(slice_steps, reqid, STEPS=StepExecution.STEPS, approve_level=99, waiting_level=99):
    """
    Creating/saving steps

     :param slice_steps: dict of slices this element {Slice number:[step tag,is_skipped]}
     :param reqid: request id
     :param is_approve: approve if true, save if false

    """
    def events_per_input_file(index, STEPS, task_config, parent_step):
        if index == 0:
            task_config.update({'nEventsPerInputFile':get_default_nEventsPerJob_dict().get(STEPS[index],'')})
        else:
            if parent_step:
                if 'nEventsPerJob' in json.loads(parent_step.task_config):
                    task_config.update({'nEventsPerInputFile': json.loads(parent_step.task_config)['nEventsPerJob']})
                else:
                    task_config.update({'nEventsPerInputFile': get_default_nEventsPerJob_dict().get(parent_step.step_template.step,'')})
            else:
                task_config.update({'nEventsPerInputFile':get_default_nEventsPerJob_dict().get(STEPS[index-1],'')})

    try:
        APPROVED_STATUS = ['Skipped','Approved']
        SKIPPED_STATUS = ['NotCheckedSkipped','Skipped']
        error_slices = []
        no_action_slices = []
        cur_request = TRequest.objects.get(reqid=reqid)
        for slice, steps_status in slice_steps.items():
            input_list = InputRequestList.objects.filter(request=cur_request, slice=int(slice))[0]
            existed_steps = StepExecution.objects.filter(request=cur_request, slice=input_list)
            if input_list.priority is None:
                priority_obj = get_priority_object(850)
            else:
                priority_obj = get_priority_object(input_list.priority)
            # Check steps which already exist in slice, and change them if needed
            try:
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            except ValueError,e:
                ordered_existed_steps, existed_foreign_step = [],None

            parent_step = None
            no_action = True
            status_changed = False
            foreign_step = 0
            if int(steps_status[-1]['foreign_id']) !=0:
                foreign_step = int(steps_status[-1]['foreign_id'])
                parent_step = StepExecution.objects.get(id=foreign_step)
            steps_status.pop()
            step_as_in_page = form_step_in_page(ordered_existed_steps,STEPS,existed_foreign_step)
            # if foreign_step !=0 :
            #     step_as_in_page = [None] + step_as_in_page
            first_not_approved_index = 0
            total_events = -1
            if not parent_step:
                total_events = input_list.input_events
            still_skipped = True
            new_step = False
            for index,step in enumerate(step_as_in_page):
                if step:
                    if step.status in APPROVED_STATUS:
                        first_not_approved_index = index + 1
                        parent_step = step
                        if step.status not in SKIPPED_STATUS:
                            total_events = -1
                            still_skipped = False
                            if (index>=waiting_level):
                               waiting_level = 99
                        else:
                            total_events = step.input_events
            try:
                to_delete = []
                for index,step_value in enumerate(steps_status[first_not_approved_index:],first_not_approved_index):

                    step_in_db = step_as_in_page[index]
                    if not step_value['value'] and not step_in_db:
                        continue
                    if not step_value['value'] and step_in_db:
                        to_delete.append(step_in_db)
                        continue
                    if step_value['value'] and (not step_in_db) and existed_foreign_step and (index == 0):
                         raise ValueError("Part of child chain before linked step can't be overridden")
                    no_action = False
                    if step_value['changes']:
                        for key in step_value['changes'].keys():
                            if type(step_value['changes'][key]) != dict:
                                step_value['changes'][key].strip()
                            else:
                                for key_second_level in step_value['changes'][key].keys():
                                    step_value['changes'][key][key_second_level].strip()
                    if step_in_db:
                        if (len(to_delete)==0)and(step_in_db.step_template.ctag == step_value['value']) and \
                                (not step_value['changes']) and (total_events==step_in_db.input_events) and \
                                similar_status(step_in_db.status,step_value['is_skipped']) and (not new_step):
                            if still_skipped and (index>=waiting_level):
                                 waiting_level = 99
                            approve_existed_step(step_in_db,step_status_definition(step_value['is_skipped'],
                                                                                   index<=approve_level,
                                                                                   index>=waiting_level))
                            if step_in_db.status not in SKIPPED_STATUS:
                                total_events = -1
                                still_skipped = False
                            parent_step = step_in_db
                        else:

                            if step_in_db.task_config:
                                task_config = json.loads(step_in_db.task_config)
                            else:
                                task_config = {}
                            for x in ['input_format','nEventsPerJob','token','merging_tag','nEventsPerMergeJob',
                                      'nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob','project_mode',
                                      'nFilesPerJob','nGBPerJob','maxAttempt','maxFailure','evntFilterEff']:
                                if x in step_value['changes']:
                                    task_config[x] = step_value['changes'][x]
                            for x in ['nEventsPerInputFile']:
                                if x in step_value['changes']:
                                    if step_value['changes'][x]:
                                        task_config[x] = int(step_value['changes'][x])
                                    else:
                                        task_config[x] = ''
                                elif new_step:
                                    task_config[x] = ''
                            if 'maxFailure' in task_config:
                                if task_config['maxFailure']:
                                    task_config['maxAttempt'] = int(task_config['maxFailure']) + 10
                            change_template = False
                            ctag = step_value['value']
                            if ctag != step_in_db.step_template.ctag:
                                change_template = True
                            output_formats = step_in_db.step_template.output_formats
                            if 'output_formats' in step_value['changes']:
                                output_formats = step_value['changes']['output_formats']
                                change_template = True
                            memory = step_in_db.step_template.memory
                            if 'memory' in step_value['changes']:
                                change_template = True
                                memory = step_value['changes']['memory']
                            if change_template:
                                step_in_db.step_template = fill_template(step_in_db.step_template.step,ctag, step_in_db.step_template.priority, output_formats, memory)
                            if 'priority' in step_value['changes']:
                                step_in_db.priority = step_value['changes']['priority']
                            if parent_step:
                                step_in_db.step_parent = parent_step
                            else:
                                step_in_db.step_parent = step_in_db
                            if not similar_status(step_in_db.status,step_value['is_skipped']):
                                status_changed = True
                            if still_skipped and (index>=waiting_level):
                                 waiting_level = 99
                            step_in_db.status = step_status_definition(step_value['is_skipped'], index<=approve_level,
                                                                       index>=waiting_level)

                            if 'input_events' in step_value['changes']:
                                step_in_db.input_events = step_value['changes']['input_events']
                                total_events = step_in_db.input_events
                            else:
                                if (step_in_db.input_events == -1):
                                    if status_changed:
                                        step_in_db.input_events = total_events
                                else:
                                    if not still_skipped:
                                        step_in_db.input_events = -1
                                    else:
                                        total_events = step_in_db.input_events

                            if ('nEventsPerInputFile' not in step_value['changes']) and (not task_config.get('nEventsPerInputFile','')) and still_skipped:
                                events_per_input_file(index,STEPS,task_config,parent_step)
                            if step_in_db.status not in SKIPPED_STATUS:
                                total_events = -1
                                still_skipped = False
                            step_in_db.set_task_config(task_config)
                            step_in_db.remove_task_config('spreadsheet_original')
                            step_in_db.step_def_time = None
                            step_in_db.save_with_current_time()
                            parent_step = step_in_db
                    else:
                            status_changed = True
                            task_config = {'maxFailure':10,'maxAttempt':30}
                            if not input_list.project_mode:
                                task_config.update({'project_mode':get_default_project_mode_dict().get(STEPS[index],'')})
                                task_config.update({'nEventsPerJob':get_default_nEventsPerJob_dict().get(STEPS[index],'')})
                                if still_skipped:
                                    events_per_input_file(index,STEPS,task_config,parent_step)
                            else:
                                task_config.update({'project_mode':input_list.project_mode})
                            for x in ['input_format','nEventsPerJob','token','merging_tag','nEventsPerMergeJob',
                                      'nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob','project_mode','nFilesPerJob',
                                      'nGBPerJob','maxAttempt','maxFailure','evntFilterEff']:
                                if x in step_value['changes']:
                                    task_config[x] = step_value['changes'][x]
                            for x in ['nEventsPerInputFile']:
                                if x in step_value['changes']:
                                    if step_value['changes'][x]:
                                        task_config[x] = int(step_value['changes'][x])
                                    else:
                                        task_config[x] = ''
                            ctag = step_value['value']
                            output_formats = step_value['formats']
                            if 'output_formats' in step_value['changes']:
                                output_formats = step_value['changes']['output_formats']
                            memory = None
                            if 'memory' in step_value['changes']:
                                memory = step_value['changes']['memory']
                            if(STEPS[index]):
                                temp_priority = priority_obj.priority(STEPS[index], step_value['value'])
                            else:
                                temp_priority = priority_obj.priority('Evgen', step_value['value'])

                            step_template = fill_template(STEPS[index], ctag, temp_priority, output_formats, memory)
                            if 'priority' in step_value['changes']:
                                temp_priority = step_value['changes']['priority']
                            st_exec = StepExecution(request=cur_request,slice=input_list,step_template=step_template,
                                        priority=temp_priority)
                            no_parent = True
                            st_exec.set_task_config(task_config)
                            st_exec.remove_task_config('spreadsheet_original')
                            if parent_step:
                                st_exec.step_parent = parent_step
                                no_parent = False
                            if still_skipped and (index>=waiting_level):
                                 waiting_level = 99
                            st_exec.status = step_status_definition(step_value['is_skipped'], index<=approve_level,
                                                                    index>=waiting_level)
                            if 'input_events' in step_value['changes']:
                                st_exec.input_events = step_value['changes']['input_events']
                            else:
                                st_exec.input_events = total_events
                            if st_exec.status not in SKIPPED_STATUS:
                                total_events = -1
                                still_skipped = False
                            st_exec.save_with_current_time()
                            if no_parent:
                                st_exec.step_parent = st_exec
                                st_exec.save()
                            parent_step = st_exec
                            new_step = True
                for step in to_delete:
                            step.step_parent = step
                            step.save()
                            step.delete()
            except Exception,e:
                _logger.error("Problem step save/approval %s"%str(e))
                error_slices.append(int(slice))
            else:
                if no_action:
                    no_action_slices.append(int(slice))
    except Exception, e:
        _logger.error("Problem step save/approval %s"%str(e))
        raise e
    return error_slices,no_action_slices


def get_step_input_type(ctag):
    trtf = Ttrfconfig.objects.all().filter(tag=ctag.strip()[0], cid=int(ctag.strip()[1:]))[0]
    return trtf.input


def filter_mc_campaign(cur_request, tasks):
    result = tasks
    events = -1
    if cur_request.campaign == 'MC16':
        result = []
        events = 0
        subcampaign = cur_request.subcampaign
        for task_id in tasks:
            task = ProductionTask.objects.get(id=task_id)
            if task.request.campaign == cur_request.campaign:
                if ((subcampaign=='MC16d') and (task.request.subcampaign=='MC16c')) or (task.request.subcampaign == subcampaign):
                    result.append(task_id)
                    events += task.total_events
            else:
                if (subcampaign == 'MC16a') or ('valid' in task.name):
                    result.append(task_id)
                    events += task.total_events
    return result, events


def filter_input_datasets(dataset_events, reqid, filter_type):
    result = []
    for item in dataset_events:
        if filter_type:
            if filter_type not in item['dataset_name']:
                continue
        cur_request = TRequest.objects.get(reqid=reqid)
        new_tasks, new_events = filter_mc_campaign(cur_request,item['tasks'])
        if len(new_tasks)<len(item['tasks']):
            if len(new_tasks)>0:
                result.append({'dataset_name':item['dataset_name'],'events':str(new_events), 'excluded':True})
        else:
            result.append({'dataset_name':item['dataset_name'],'events':item['events'], 'excluded':False})

    return result


def form_skipped_slice(slice, reqid):
    cur_request = TRequest.objects.get(reqid=reqid)
    input_list = InputRequestList.objects.filter(request=cur_request, slice=int(slice))[0]
    existed_steps = StepExecution.objects.filter(request=cur_request, slice=input_list)
    # Check steps which already exist in slice
    try:
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
    except ValueError,e:
        ordered_existed_steps, existed_foreign_step = [],None
    if ordered_existed_steps[0].status == 'Skipped' and input_list.dataset:
        return {slice:[]}
    processed_tags = []
    last_step_name = ''
    for step in ordered_existed_steps:
        if step.status == 'NotCheckedSkipped' or step.status == 'Skipped':
            processed_tags.append(step.step_template.ctag)
            last_step_name = step.step_template.step

        else:
            input_step_format = step.get_task_config('input_format')
            break
    if input_list.input_data and processed_tags:
        try:
            input_type = ''
            default_input_type_prefix = {
                'Evgen': {'format':'EVNT','prefix':''},
                'Evgen Merge': {'format':'EVNT','prefix':'.'},
                'Simul': {'format':'HITS','prefix':'.', 'filter':'simul'},
                'Merge': {'format':'HITS','prefix':'.','filter':'merge'},
                'Reco': {'format':'AOD','prefix':'recon.'},
                'Rec Merge': {'format':'AOD','prefix':'merge.'}
            }
            filter_type = ''
            if last_step_name in default_input_type_prefix:
                if input_step_format:
                    input_type = default_input_type_prefix[last_step_name]['prefix'] + input_step_format
                else:
                    input_type = default_input_type_prefix[last_step_name]['prefix'] + default_input_type_prefix[last_step_name]['format']
                if 'filter' in default_input_type_prefix[last_step_name]:
                    filter_type = default_input_type_prefix[last_step_name]['filter']
            dsid = input_list.input_data.split('.')[1]
            job_option_pattern = input_list.input_data.split('.')[2]
            dataset_events = find_skipped_dataset(dsid,job_option_pattern,processed_tags,input_type)

            # if input_type=='merge.HITS':
            #     dataset_events += find_skipped_dataset(dsid,job_option_pattern,processed_tags,'simul.HITS')
            # if input_type=='simul.HITS':
            #     dataset_events += find_skipped_dataset(dsid,job_option_pattern,processed_tags,'merge.HITS')
            #print dataset_events
            #return {slice:[x for x in dataset_events if x['events']>=input_list.input_events ]}

            return {slice:filter_input_datasets(dataset_events, reqid, filter_type)}
        except Exception,e:
            logging.error("Can't find skipped dataset: %s" %str(e))
            return {slice:[]}
    return {slice:[]}


def get_skipped_steps(production_request, slice):
    existed_steps = StepExecution.objects.filter(request=production_request, slice=slice)
    # Check steps which already exist in slice
    try:
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
    except ValueError,e:
        ordered_existed_steps, existed_foreign_step = [],None
    processed_tags = []
    last_step_name = ''
    last_step = None
    for step in ordered_existed_steps:
        if step.status == 'NotCheckedSkipped' or step.status == 'Skipped':
            processed_tags.append(step.step_template.ctag)
            last_step_name = step.step_template.step
        else:
            last_step = step
            break
    return last_step_name, processed_tags, last_step


@csrf_protect
def find_input_datasets(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        slice_dataset_dict = {}
        data = request.body
        slices = json.loads(data)
        for slice_number in slices:
            try:
                slice_dataset_dict.update(form_skipped_slice(slice_number,reqid))
            except Exception,e:
                pass
        results.update({'success':True,'data':slice_dataset_dict})

        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def change_request_priority(request, reqid, old_priority, new_priority):
    if request.method == 'POST':
        slices = list(InputRequestList.objects.filter(request=reqid,priority=int(old_priority)))

        slice_steps = {}
        for slice in slices:
            slice_steps.update({str(slice.slice) : {'changes':{'priority':str(int(new_priority))}}})

        try:
                save_slice_changes(reqid, slice_steps)
        except Exception,e:
                logging.error("Can't update slice priority: %s" %str(e))
        results = {}
        results.update({'success':True})
        fill_request_priority(reqid,reqid)
        return HttpResponse(json.dumps(results), content_type='application/json')


MC_COORDINATORS= ['cgwenlan','jzhong','jgarcian','mcfayden','jferrand','mehlhase','schwanen','lserkin','jcosta','boeriu',
                  'onofrio','jmonk','kado']



def request_approve_status(production_request, request):
    if (production_request.request_type == 'MC') and (production_request.phys_group != 'VALI'):
        user_name=''
        is_superuser=False
        try:
            user_name = request.user.username
        except:
            pass

        try:
            is_superuser = request.user.is_superuser
        except:
            pass
        # change to VOMS
        _logger.debug("request:%s is registered by %s" % (str(production_request.reqid),user_name))
        if (user_name in MC_COORDINATORS) or ('MCCOORD' in egroup_permissions(request.user.username)) or is_superuser:
            return 'approved'

    else:
        if (production_request.request_type == 'GROUP') and (production_request.cstatus == 'waiting'):
            if ParentToChildRequest.objects.filter(parent_request=production_request).exists():
                if ParentToChildRequest.objects.filter(parent_request=production_request)[0].train_id:
                    if HashTag.objects.filter(hashtag__iexact='PatternToMerge').exists():
                        patterns = list(HashTagToRequest.objects.filter(hashtag=HashTag.objects.filter(hashtag__iexact='PatternToMerge')[0]).values_list('request_id',flat=True))
                        if TrainProduction.objects.get(id=ParentToChildRequest.objects.filter(parent_request=production_request)[0].train_id).pattern_request_id in patterns:
                            return 'registered'
        return 'approved'


def remove_input(good_slices, reqid):
    removed_input_slices = []
    for slice_number in good_slices:
        input_list = InputRequestList.objects.get(request=reqid, slice=int(slice_number))
        existed_steps = StepExecution.objects.filter(request=reqid, slice=input_list)
        try:
            ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            if (ordered_existed_steps[0].step_template.step == 'Evgen') and (ordered_existed_steps[0].status in ['NotChecked','Approved']):
                if input_list.dataset:
                    if 'evgen' not in input_list.dataset:
                        input_list.dataset = None
                        input_list.save()
                        removed_input_slices.append(slice_number)
        except:
            pass
    return removed_input_slices


def fill_all_slices_from_0_slice(reqid):
    slices = InputRequestList.objects.filter(request=reqid,is_hide=False).order_by('slice')
    steps_slice_0 = list(StepExecution.objects.filter(request = reqid,slice=slices[0]))
    steps_total_count = StepExecution.objects.filter(request = reqid).count()
    if len(steps_slice_0) == steps_total_count:
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(steps_slice_0)
        #steps_dict = [model_to_dict(x) for x in ordered_existed_steps]
        for step in ordered_existed_steps:
            step.id = None
            step.step_parent = step

        for slice in slices:
            if slice.slice != 0:
                parent = None
                for step_dict in ordered_existed_steps:
                    current_step = deepcopy(step_dict)
                    current_step.slice = slice
                    if parent:
                        current_step.step_parent = parent
                    current_step.save()
                    if not parent:
                        current_step.step_parent = current_step
                        current_step.input_events = slice.input_events
                        current_step.save()
                    parent = current_step


def save_slice_changes(reqid, slice_steps):
    not_changed = []
    for slice_number, steps_status in slice_steps.items():
        if slice_number != '-1':
            if steps_status['changes']:
                do_action = False
                for field in ['jobOption','datasetName','eventsNumber','comment','priority']:
                    if steps_status['changes'].get(field):
                        do_action = True
                if do_action:
                    try:
                        current_slice = InputRequestList.objects.get(request=reqid,slice=int(slice_number))
                        if StepExecution.objects.filter(slice=current_slice,status = 'Approved').count() > 0:
                            not_changed.append(slice)
                        else:
                            if steps_status['changes'].get('jobOption'):
                                current_slice.input_data = steps_status['changes'].get('jobOption')
                                current_slice.save()
                            if steps_status['changes'].get('datasetName'):
                                change_dataset_in_slice(reqid,slice_number,steps_status['changes'].get('datasetName'))
                            if steps_status['changes'].get('eventsNumber'):
                                current_slice.input_events = steps_status['changes'].get('eventsNumber')
                                current_slice.save()
                                if int(steps_status['changes'].get('eventsNumber')) == -1:
                                    for step in steps_status['sliceSteps']:
                                        if step['value']:
                                            if step['changes']:
                                                if not step['changes'].has_key('input_events'):
                                                    step['changes'].update({'input_events':'-1'})
                                            else:
                                                step['changes'] = {'input_events':'-1'}
                            if steps_status['changes'].get('comment'):
                                new_comment = steps_status['changes'].get('comment')
                                if ('(Fullsim)' not in new_comment) and ('(Atlfast)' not in new_comment):
                                    if '(Fullsim)' in current_slice.comment:
                                        new_comment = '(Fullsim)' + new_comment
                                    elif '(Atlfast)' in current_slice.comment:
                                        new_comment = '(Atlfast)' + new_comment
                                current_slice.comment = new_comment
                                current_slice.save()
                            if steps_status['changes'].get('priority'):
                                current_slice.priority = steps_status['changes'].get('priority')
                                current_slice.save()
                                priority_dict = get_priority_object(current_slice.priority)
                                for step in StepExecution.objects.filter(slice=current_slice):
                                        step.priority = priority_dict.priority(step.step_template.step, step.step_template.ctag)
                                        step.save()
                    except Exception,e:
                        not_changed.append(slice)
    return []


def find_input_per_file(dataset_name):
    if 'tid' not in dataset_name:
        to_search = dataset_name.replace('/','')[dataset_name.find(':')+1:]+'_tid%'
    else:
        to_search = dataset_name.replace('/','')[dataset_name.find(':')+1:]
    try:
        datasets = ProductionDataset.objects.extra(where=['name like %s'], params=[to_search])
        for dataset in datasets:
            current_task = ProductionTask.objects.get(id=dataset.task_id)
            if current_task.status not in ProductionTask.RED_STATUS:
                return current_task.step.get_task_config('nEventsPerJob')
        return ''
    except Exception,e:
        return ''



def find_evgen_missing(evgen_tag,job_options, energy = '13TeV'):
    task_name = job_options.split('.')[0].lower() + '_'+energy+'.'+job_options.split('.')[1]+'.' +\
                 job_options.split('.')[2] + '.' + 'evgen' + '.' + evgen_tag
    tasks = ProductionTask.objects.filter(name=task_name)
    if tasks:
        total_events = 0
        for task in tasks:
            if task.status not in (ProductionTask.RED_STATUS + ['obsolete']):
                total_events += task.total_events
        return total_events
    else:
        return 0



@csrf_protect
def check_slices_for_request_split(request, production_request):
    results = {'success':False}
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            slices_evgen = input_dict
            do_split = False
            no_events = False
            project = TRequest.objects.get(reqid=production_request).project_id
            if project > 'mc16':
                for slice_evgen in slices_evgen:
                    if slice_evgen[0] != -1:
                        slice = InputRequestList.objects.get(request=production_request,slice=slice_evgen[0])
                        evgen_events = find_evgen_missing(slice_evgen[1],slice.input_data)
                        if evgen_events <= (0.95 * float(slice.input_events)):
                            do_split = True
                            break
            results = {'success':True, 'do_split': do_split, 'no_events': no_events }
        except Exception,e:
            pass
    return HttpResponse(json.dumps(results), content_type='application/json')







def split_slice_between_projects(slice, parent_request, child_request, step_to_split_number):
    #Clone slice to two requests
    _logger.debug('Clone slice %s from %s to %s'%(str(slice),str(parent_request.reqid),str(child_request.reqid)))
    new_slice_number = clone_slices(parent_request.reqid,child_request.reqid,[slice.slice],-1,True,False,False,{},step_to_split_number)[0]
    ordered_existed_steps, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(request=parent_request, slice=slice))
    new_slice = InputRequestList.objects.get(request=child_request,slice=new_slice_number)
    child_steps, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(request=child_request,
                                                                                            slice=new_slice))
    new_slice.input_events = child_steps[0].get_task_config('split_events')
    new_slice.save()
    if  slice.input_events > new_slice.input_events:
        slice.input_events = slice.input_events - new_slice.input_events
        slice.save()
    parent_steps = {ordered_existed_steps[step_to_split_number].step_parent.id:child_steps[step_to_split_number-1]}
    new_parent_slice_number = clone_slices(parent_request.reqid,parent_request.reqid,[slice.slice],step_to_split_number,True,False,False,parent_steps)[0]
    new_parent_slice = InputRequestList.objects.get(request=parent_request,slice=new_parent_slice_number)
    new_parent_slice.input_events = new_slice.input_events
    new_parent_slice.save()
    # Approve child request:
    for step in child_steps:
        if step:
            step.status = 'Approved'
            step.input_events = step.get_task_config('split_events')
            step.remove_task_config('split_events')
            step.save()
    set_request_status('cron',child_request.reqid,'approved','Automatic cloned approve', 'Request was automatically approved')
    # Waiting new slice
    cloned_parent_slice, existed_foreign_step = form_existed_step_list(StepExecution.objects.filter(request=parent_request,
                                                                                                    slice=new_parent_slice))
    for index, step in enumerate(cloned_parent_slice):
        if step:
            if ordered_existed_steps[index+step_to_split_number].status in ['Approved','Waiting']:
                step.status = 'Waiting'
                step.save()
    for index, step in enumerate(ordered_existed_steps):
        if step and (index<step_to_split_number):
            step.status = 'Skipped'
            step.save()
    # work with original slice
    containers = form_skipped_slice(slice.slice, parent_request.reqid)[slice.slice]
    if containers:
        _logger.debug('Find containers: %s'%(str(containers)))
        containers.sort(key= lambda x: x['events'])
        change_dataset_in_slice(parent_request.reqid,slice.slice,containers[0]['dataset_name'])
        for index, step in enumerate(ordered_existed_steps):
            if step:
                if step.status == 'Waiting':
                    step.status = 'Approved'
                    step.save()
    else:
        for index, step in enumerate(ordered_existed_steps):
            if step:
                if (step.status == 'Approved') or (step.status == 'Waiting'):
                    step.status = 'NotApproved'
                    step.save()

    pass


def split_request(production_request_number, slice_numbers):
    production_request = TRequest.objects.get(reqid=production_request_number)
    slices = InputRequestList.objects.filter(request=production_request, slice__in=slice_numbers).order_by('slice')
    # Create evgen request if doesn't exist
    child_requests = {}
    related_requests = ParentToChildRequest.objects.filter(parent_request = production_request, relation_type='SP')
    for related_request in related_requests:
        child_requests[str(related_request.child_request.project)] = related_request.child_request

    for slice in slices:
        existed_steps = StepExecution.objects.filter(request=production_request, slice=slice)
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
        if existed_foreign_step:
            continue
        split_number = -1
        for index,step in enumerate(ordered_existed_steps):
            if step.get_task_config('split_events'):
                split_number=index
        if split_number == -1:
            step = ordered_existed_steps[0]
            events_done = find_evgen_missing(step.step_template.ctag, slice.input_data)
            if float(events_done)<(0.95*float(step.input_events)):
                    split_number = 0
                    step.set_task_config({'split_events':int(step.input_events-events_done)})
                    step.save()
        if split_number != -1:
            if (ordered_existed_steps[split_number] and ordered_existed_steps[split_number+1]):
                campaign = slice.input_data.split('.')[0].lower()
                child_project = campaign + '_' + str(production_request.project).split('_')[1]
                if child_project in child_requests:
                    child_request = child_requests[child_project]
                else:
                    _logger.debug('Create new request for split steps for %s'%str(production_request_number))
                    evgen_request_id = request_clone_slices(production_request_number, production_request.manager,
                                                            'Evgen for '+production_request.description,
                                                            production_request.ref_link,[],child_project)
                    request_relation = ParentToChildRequest()
                    request_relation.child_request = child_request
                    request_relation.parent_request = production_request
                    request_relation.relation_type = 'SP'
                    request_relation.status = 'active'
                    request_relation.save()
                    child_requests[child_project] = child_request
                    _logger.debug('New request for split steps is %s'%str(child_request.reqid))
                split_slice_between_projects(slice, production_request, child_request, split_number+1)
    # Create a new evgen slice in evgen request
    # Create a new slice in simul-request
        # Find containers
    # Modify original slice in a simul request


def change_dataset_in_slice(req, slice, new_dataset_name):
    input_list = InputRequestList.objects.get(request=req, slice=int(slice))
    events_per_file = find_input_per_file(new_dataset_name)
    dataset = fill_dataset(new_dataset_name)
    input_list.dataset = dataset
    input_list.save()
    if events_per_file:
        temp1, pattern_tags, approved_step = get_skipped_steps(req,input_list)
        if approved_step.step_parent != approved_step:
            if approved_step.step_parent.status in ['NotCheckedSkipped','Skipped']:
                if approved_step.step_parent.get_task_config('nEventsPerJob') != events_per_file:
                    approved_step.step_parent.set_task_config({'nEventsPerJob':events_per_file})
                    approved_step.step_parent.save()
        if approved_step.get_task_config('nEventsPerInputFile') != events_per_file:
            approved_step.set_task_config({'nEventsPerInputFile':events_per_file})
            approved_step.save()


def find_child_steps(parent_request_id, slice_steps):
    requests_relations = ParentToChildRequest.objects.filter(status='active',parent_request=parent_request_id)
    child_requests = [item.child_request for item in requests_relations if item.child_request]
    parent_position = {}
    if child_requests:
        steps = list(StepExecution.objects.filter(request=parent_request_id))
        parent_steps_id = [step.id for step in steps]
        step_relation = {}
        for child_request in child_requests:
            child_steps = list(StepExecution.objects.filter(request=child_request).values('id','slice_id',
                                                                                           'step_parent_id'))
            for child_step in child_steps:
                if child_step['step_parent_id'] in parent_steps_id:
                    step_relation[child_step['step_parent_id']] = step_relation.get(parent_steps_id,[]) + [child_step['id']]
        for step in steps:
            if step.id in step_relation:
                #take position in chain
                parent_position[step.id] = {'slice':step.slice.slice,'child_steps':step_relation[step.id]}


def create_request_for_pattern(parent_request_id, short_description, manager):
            parent_request = TRequest.objects.get(reqid=parent_request_id)
            new_request = TRequest()
            new_request.campaign = parent_request.project
            new_request.project = parent_request.project
            new_request.description = short_description
            new_request.phys_group = 'PHYS'
            new_request.provenance = 'GP'
            new_request.request_type = 'GROUP'
            new_request.energy_gev = parent_request.energy_gev
            new_request.manager =  'atlas-dpd-production'
            new_request.cstatus = 'waiting'
            new_request.save()
            request_status = RequestStatus(request=new_request,comment='Request created as child train WebUI',owner=manager,
                                                       status='waiting')

            request_status.save_with_current_time()

            return new_request


def find_parent_for_train_steps(ordered_slices, parent_request, step_number = -1):
    not_approved = []
    parent_steps = []
    is_mc = False
    if step_number == -1:
        is_mc = True
    for slice_number in ordered_slices:
        input_list = InputRequestList.objects.get(request=parent_request,slice=int(slice_number))
        existed_steps = StepExecution.objects.filter(request=parent_request, slice=input_list)
        # Check steps which already exist in slice, and change them if needed
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
        if is_mc:
            step_as_in_page = form_step_in_page(ordered_existed_steps,StepExecution.STEPS, None)
            if 'Fullsim' not in input_list.comment:
                step_number = 8
            else:
                step_number = 5
        else:
            step_as_in_page = form_step_in_page(ordered_existed_steps,['']*len(StepExecution.STEPS),existed_foreign_step)
        if step_as_in_page[step_number]:
            if step_as_in_page[step_number].status != 'Approved':
                not_approved.append(slice_number)
            else:
                parent_steps.append(step_as_in_page[step_number])
        else:
            not_approved.append(slice_number)
    return parent_steps,not_approved


def form_output_pattern(phys_group, train):
    #take trains:
    train_outputs = json.loads(train.outputs)
    result_outut = []
    for slice_outputs in train_outputs:
        for output in slice_outputs[1]:
            if phys_group in output:
                result_outut.append(slice_outputs)
                break
    return result_outut


def make_child_update(parent_request_id, manager, slices):
    child_requests = list(ParentToChildRequest.objects.filter(status='active',parent_request=parent_request_id))
    parent_request = TRequest.objects.get(reqid=parent_request_id)
    if child_requests:
        steps = list(StepExecution.objects.filter(request=parent_request_id))
        steps_by_slice = {}

        for current_step in steps:
            steps_by_slice[current_step.slice_id] = steps_by_slice.get(current_step.slice_id,[])+[current_step]
        for child_request in child_requests:
            output_pattern = {}
            # if child request is not exist yet
            step_relation = {}
            if child_request.child_request:
                # find steps which already exist and which should be created
                parent_steps_id = [step.id for step in steps]

                child_steps = list(StepExecution.objects.filter(request=child_request.child_request))
                for child_step in child_steps:
                    if child_step.step_parent_id in parent_steps_id:
                        step_relation[child_step.step_parent_id] = step_relation.get(child_step.step_parent_id,[]) + [child_step]

            used_slices = set()
            do_request_approve = False
            if step_relation:
                for slice_number in slices:
                    slice = InputRequestList.objects.get(request=parent_request,slice=slice_number)
                    for step in steps_by_slice[slice.id]:
                        if step.id in step_relation:
                            used_slices.add(slice.slice)
                            if step.status == 'Approved':
                                for child_step in step_relation[step.id]:
                                    if child_step.status != 'Approved':
                                        child_step.status = 'Approved'
                                        child_step.save()
                                        do_request_approve = True
            if child_request.relation_type == 'BC':
                slices_to_proceed = [slice_number for slice_number in slices if int(slice_number) not in used_slices]
                parent_steps, slices_not_approved = find_parent_for_train_steps(slices_to_proceed, parent_request)
                if parent_steps and (not output_pattern):
                    output_pattern = form_output_pattern(parent_request.phys_group, child_request.train)
                if parent_steps and output_pattern:
                    if not child_request.child_request:
                        child_request.child_request = create_request_for_pattern(parent_request_id, manager, "Automatic derivation for request #%s"%str(parent_request_id))
                        child_request.save()
                    create_steps_in_child_pattern(child_request.child_request,parent_steps,child_request.train.pattern_request,output_pattern,'Approved')
                    do_request_approve = True
            if do_request_approve:
                 set_request_status(manager,child_request.child_request_id,'approved','Automatic child approve', 'Request was automatically approved')
                            # parent_position[step.id] = {'slice':step.slice.slice,'child_steps':step_relation[step.id]}
                # find steps which already exist


def request_steps_approve_or_save(request, reqid, approve_level, waiting_level=99, do_split=False):
    results = {'success':False}
    try:
        data = request.body
        slice_steps = json.loads(data)
        _logger.debug(form_request_log(reqid,request,"Steps modification for: %s" % slice_steps))
        slices = slice_steps.keys()
        fail_slice_save = save_slice_changes(reqid, slice_steps)
        try:
            fill_request_priority(reqid,reqid)
        except:
            pass
        for slice, steps_status in slice_steps.items():
            slice_steps[slice] = steps_status['sliceSteps']
        for steps_status in slice_steps.values():
            for steps in steps_status[:-2]:
                steps['value'] = steps['value'].strip()
        slice_new_input = {}
        for slice, steps_status in slice_steps.items():
            if steps_status[-1]:
                slice_new_input.update({slice:steps_status[-1]['input_dataset']})
            slice_steps[slice]= steps_status[:-1]

        # Check input on missing tags, wrong skipping
        missing_tags,wrong_skipping_slices,old_double_trf = step_validation(slice_steps)
        error_approve_message = False
        req = TRequest.objects.get(reqid=reqid)
        owner = request.user.username
        if (owner != req.manager) and (req.request_type == 'MC') and (req.phys_group != 'VALI'):
            if (not request.user.is_superuser) and ('MCCOORD' not in egroup_permissions(req.manager)):
                error_approve_message = True
        results = {'missing_tags': missing_tags,'slices': [],'no_action_slices' :slices,'wrong_slices':wrong_skipping_slices,
                   'double_trf':old_double_trf, 'success': True, 'new_status':'', 'fail_slice_save': fail_slice_save,
                   'error_approve_message': error_approve_message}
        if (not missing_tags) and (not error_approve_message):

            _logger.debug("Start steps save/approval")

            if req.request_type == 'MC':
                for steps_status in slice_steps.values():
                    for index,steps in enumerate(steps_status[:-2]):
                        if (StepExecution.STEPS[index] == 'Reco') or (StepExecution.STEPS[index] == 'Atlfast'):
                                if not steps['formats']:
                                    steps['formats'] = 'AOD'
            removed_input = []
            error_slices = []
            if ['-1'] == slice_steps.keys():
                slice_0 = deepcopy(slice_steps['-1'])
                if req.request_type == 'MC':
                    error_slices, no_action_slices = create_steps({0:slice_steps['-1']},reqid,StepExecution.STEPS, approve_level,waiting_level)
                else:
                    error_slices, no_action_slices = create_steps({0:slice_steps['-1']},reqid,['']*len(StepExecution.STEPS), approve_level,waiting_level)
                if req.request_type == 'MC':
                    approved_steps = StepExecution.objects.filter(request=reqid, status='Approved').count()
                    if (0 not in error_slices) and (approved_steps == 0):
                        fill_all_slices_from_0_slice(reqid)
                    else:
                        slice_count = InputRequestList.objects.filter(request=reqid).count()
                        extended_slice_steps = {}
                        for i in range(1,slice_count):
                            extended_slice_steps.update({str(i):deepcopy(slice_0)})
                        error_slices, no_action_slices = create_steps(extended_slice_steps,reqid,StepExecution.STEPS, approve_level)
            else:
                if '-1' in  slice_steps.keys():
                    del slice_steps['-1']
                if not (req.manager) or (req.manager == 'None'):
                    missing_tags.append('No manager name!')
                else:
                    removed_input = []
                    #child_steps_before_update = find_child_steps(reqid,slice_steps)
                    if req.request_type == 'MC':
                        error_slices, no_action_slices = create_steps(slice_steps,reqid,StepExecution.STEPS, approve_level, waiting_level)
                        good_slices = [int(x) for x in slices if int(x) not in error_slices]
                        removed_input = remove_input(good_slices,reqid)
                    else:
                        error_slices, no_action_slices = create_steps(slice_steps,reqid,['']*len(StepExecution.STEPS), approve_level, waiting_level)
                    make_child_update(reqid,owner,slice_steps)
            if (req.cstatus.lower() not in  ['test','cancelled']) and (approve_level>=0):
                    if not owner:
                        owner = req.manager
                    req.cstatus = request_approve_status(req,request)
                    req.save()

                    request_status = RequestStatus(request=req,comment='Request approved by WebUI',owner=owner,
                                                   status=req.cstatus)
                    request_status.save_with_current_time()
            if req.request_type == 'MC':
                if do_split:
                    split_request(reqid,[x for x in map(int,slices) if x not in error_slices])
                else:
                    for slice, new_dataset in slice_new_input.items():
                        if new_dataset:
                            change_dataset_in_slice(req, int(slice), new_dataset)

            results = {'missing_tags': missing_tags,
                       'slices': [x for x in map(int,slices) if x not in (error_slices + no_action_slices)],
                       'wrong_slices':wrong_skipping_slices,
                       'double_trf':old_double_trf, 'error_slices':error_slices,
                       'no_action_slices' :no_action_slices,'success': True, 'new_status': req.cstatus,
                       'removed_input':removed_input, 'fail_slice_save':'',
                       'error_approve_message': error_approve_message}
        else:
                _logger.debug("Some tags are missing: %s" % missing_tags)
    except Exception, e:
            _logger.error("Problem with step modifiaction: %s" % e)

    return HttpResponse(json.dumps(results), content_type='application/json')



def find_skipped_dataset(DSID,job_option,tags,data_type):
    """
    Find a datasets and their events number for first not skipped step in chain
    :param DSID: dsid of the chain
    :param job_option: job option name of the chain input
    :param tags: list of tags which were already proceeded
    :param data_type: expected data type
    :return: list of dict {'dataset_name':'...','events':...}
    """
    return_list = []
    for base_value in ['valid','mc']:
        dataset_pattern = base_value+"%"+str(DSID)+"%"+job_option+"%"+data_type+"%"+"%".join(tags)+"%"
        _logger.debug("Search dataset by pattern %s"%dataset_pattern)
        return_list += find_dataset_events(dataset_pattern, tags)
    return return_list







def find_old_double_trf(tags):
    double_trf_tags = set()
    for tag in tags:
        try:
            trtf = Ttrfconfig.objects.all().filter(tag=tag.strip()[0], cid=int(tag.strip()[1:]))
            if ',' in trtf[0].trf:
                double_trf_tags.add(tag)
        except:
            pass
    return list(double_trf_tags)


def step_validation(slice_steps):
    tags = []
    # Slices with skipped
    wrong_skipping_slices = set()
    for slice, steps_status in slice_steps.items():
        is_skipped = True
        is_not_skipped = False
        for steps in steps_status[:-1]:
            if steps['value'] and (steps['value'] not in tags):
                tags.append(steps['value'])
            if steps['value']:
                if steps['is_skipped'] == True:
                    is_skipped = True
                else:
                    if is_not_skipped and is_skipped:
                        wrong_skipping_slices.add(slice)
                    else:
                        is_skipped = False
                        is_not_skipped = True

    missing_tags = find_missing_tags(tags)
    old_double_trf = find_old_double_trf(tags)
    return missing_tags,list(wrong_skipping_slices),old_double_trf



@csrf_protect
def request_steps_save(request, reqid):
    if request.method == 'POST':
        return request_steps_approve_or_save(request, reqid, -1)
    return HttpResponseRedirect(reverse('prodtask:input_list_approve', args=(reqid,)))


@csrf_protect
def request_steps_approve(request, reqid, approve_level, waiting_level):
    if request.method == 'POST':
        return request_steps_approve_or_save(request, reqid, int(approve_level)-1, int(waiting_level))
    return HttpResponseRedirect(reverse('prodtask:input_list_approve', args=(reqid,)))


@csrf_protect
def request_steps_approve_split(request, reqid, approve_level, waiting_level):
    if request.method == 'POST':
        return request_steps_approve_or_save(request, reqid, int(approve_level)-1, int(waiting_level), True)
    return HttpResponseRedirect(reverse('prodtask:input_list_approve', args=(reqid,)))

def form_step_hierarchy(tags_formats_text):
    step_levels = []
    for line in tags_formats_text.split('\n'):
        step_levels.append([])
        step_levels[-1] = [(x.split(':')[0],x.split(':')[1]) for x in line.split(' ') if x]
    step_hierarchy = []
    for level_index,level in enumerate(step_levels):
        step_hierarchy.append([])
        # find if tag on some previous level already exist, then make a link
        for i in range(level_index):
            if level[0] == step_levels[i][-1]:
                step_hierarchy[-1].insert(0,{'level':i,'step_number':len(step_levels[i])-1,'ctag':'','formats':''})
        # no link
        if len(step_hierarchy[-1]) == 0:
            step_hierarchy[-1].append({'level':level_index,'step_number':0,'ctag':level[0][0],'formats':level[0][1]})
        for j in range(1,len(level)):
            step_hierarchy[-1].append({'level':level_index,'step_number':j-1,'ctag':level[j][0],'formats':level[j][1]})
    return step_hierarchy



@csrf_protect
def request_reprocessing_steps_create(request, reqid=None):
    if request.method == 'POST':
        cur_request = TRequest.objects.get(reqid=reqid)
        result = {}
        try:
            data = request.body
            input_dict = json.loads(data)
            tags_formats_text = input_dict['tagsFormats']
            slices = input_dict['slices']
            #form levels from input text lines
            step_levels = form_step_hierarchy(tags_formats_text)
            #create chains for each input
            new_slice_number = InputRequestList.objects.filter(request=reqid).count()
            for slice_number in slices:
                real_steps_hierarchy=[]
                input_skeleton = {}
                for level_index,level in enumerate(step_levels):
                    current_slice = {}
                    real_steps_hierarchy.append([])
                    if level_index == 0:
                        current_slices = InputRequestList.objects.filter(request=reqid,slice=slice_number)
                        input_skeleton = current_slices.values('brief','phys_comment','comment','project_mode',
                                                             'priority','input_events')[0]
                        input_skeleton['request'] = cur_request
                        current_slice = current_slices[0]
                    else:
                        input_skeleton['slice'] = new_slice_number
                        new_slice_number += 1
                        current_slice = InputRequestList(**input_skeleton)
                        current_slice.save()


                    for i,current_tag in enumerate(level):
                        if current_tag['ctag'] == '':
                            real_steps_hierarchy[-1].append(real_steps_hierarchy[current_tag['level']][current_tag['step_number']])
                        else:
                            step_template = fill_template('',current_tag['ctag'],current_slice.priority,current_tag['formats'])
                            new_step_exec = StepExecution(request=cur_request, step_template=step_template,status='NotChecked',
                                                          slice=current_slice,priority=current_slice.priority,
                                                          input_events=-1)
                            new_step_exec.save_with_current_time()
                            if (current_tag['level'] == level_index) and (current_tag['step_number'] == i):
                                new_step_exec.step_parent = new_step_exec
                            else:
                                new_step_exec.step_parent = real_steps_hierarchy[current_tag['level']][current_tag['step_number']]
                            if current_slice.project_mode:
                                new_step_exec.set_task_config({'project_mode' : current_slice.project_mode})
                            new_step_exec.save()
                            real_steps_hierarchy[-1].append(new_step_exec)
        except Exception,e:
            return HttpResponse(json.dumps(result), content_type='application/json',status=500)
        return HttpResponse(json.dumps(result), content_type='application/json')
    return HttpResponseRedirect(reverse('prodtask:input_list_approve', args=(reqid,)))

@csrf_protect
def make_test_request(request, reqid):
    results = {}
    if request.method == 'POST':
        try:
            _logger.debug(form_request_log(reqid,request,'Make as test'))
            cur_request = TRequest.objects.get(reqid=reqid)
            cur_request.cstatus = 'test'
            cur_request.save()
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def make_request_fast(request, reqid):
    results = {}
    if request.method == 'POST':
        try:
            _logger.debug(form_request_log(reqid,request,'Make request fast'))
            cur_request = TRequest.objects.get(reqid=reqid)
            cur_request.is_fast = True
            cur_request.save()
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

def home(request):
    tmpl = get_template('prodtask/_index.html')
    c = Context({'active_app' : 'prodtask', 'title'  : 'Monte Carlo Production Home'})
    return HttpResponse(tmpl.render(c))

def about(request):
    tmpl = get_template('prodtask/_about.html')
    c = Context({'active_app' : 'prodtask', 'title'  : 'Monte Carlo Production about', })
    return HttpResponse(tmpl.render(c))

def step_skipped(step):
    return (step.status=='Skipped')or(step.status=='NotCheckedSkipped')

def fixPattern(pattern):
    pattern_d = json.loads(pattern.pattern_dict)
    for step in pattern_d.keys():
        if not pattern_d[step]['ctag']:
            if not pattern_d[step]['project_mode']:
                pattern_d[step]['project_mode'] = get_default_project_mode_dict()[step]
            if not pattern_d[step]['nEventsPerJob']:
                pattern_d[step]['nEventsPerJob'] = get_default_nEventsPerJob_dict()[step]
    pattern.pattern_dict = json.dumps(pattern_d)
    pattern.save()



@ensure_csrf_cookie
def input_list_approve(request, rid=None):
    return request_table_view(request, rid, show_hidden=False)

@ensure_csrf_cookie
def input_list_approve_full(request, rid=None):
    return request_table_view(request, rid, show_hidden=True)


NUMBER_EVENTS_TO_SPLIT = 2000000

def redirect_to_value(request, site_name):
    return redirect("http://%s.cern.ch/"%site_name)

def egroup_permissions(username):
    return_list = []
    try:
        current_user = User.objects.get(username = username )
        user_groups = current_user.groups.all()
        group_permissions = []
        for group in user_groups:
             group_permissions += list(group.permissions.all())
        for group_permission in group_permissions:
              if "has_" in group_permission.name and "_permissions" in group_permission.name:
                return_list.append(group_permission.codename)
    except:
        return []
    return return_list



def request_table_view(request, rid=None, show_hidden=False):
    # Prepare data for step manipulation page

    def get_approve_status(ste_task_list,slice=None):
        if slice:
            if slice.is_hide:
                return 'hidden'
        return_status = {'submitted':'not_submitted','original':'changed','split':'no'}
        exist_approved = False
        exist_not_approved = False
        exist_spreadsheet_original = False
        exist_to_split = False
        for step_task in ste_task_list:
            if step_task['step']:
                if (step_task['step']['status'] == 'Approved')or(step_task['step']['status'] == 'Skipped'):
                    exist_approved = True
                else:
                    exist_not_approved = True
                if 'spreadsheet_original' in step_task['step']['task_config']:
                    return_status['original'] = 'original'
                if ('split_events' in step_task['step']['task_config']) and (step_task['step']['status'] not in ['Skipped','NotCheckedSkipped']):
                    return_status['split'] = 'split'

        if exist_approved and exist_not_approved:
            return_status['submitted'] = 'partially_submitted'
        if exist_approved and not(exist_not_approved):
            return_status['submitted'] = 'submitted'

        return return_status

    def approve_level(step_task_list):
        max_level = -1
        for index,step_task in enumerate(step_task_list):
            if step_task['step']:
                if (step_task['step']['status'] == 'Approved')or(step_task['step']['status'] == 'Skipped'):
                    max_level=index
        return max_level+1

    def has_waiting(step_task_list):
        for step_task in step_task_list:
            if step_task['step']:
                if (step_task['step']['status'] == 'Waiting'):
                    return  True
        return False




    BIG_PANDA_TASK_BASE = 'http://bigpanda.cern.ch/task/'
    FAKE_TASK_NUMBER = '123456'
    PRODTASK_TASK_BASE = ''

    def form_step_obj(step,tasks,input_slice,foreign=False,another_request=None):
        skipped = 'skipped'
        tag = ''
        slice = ''
        output_format = ''
        is_another_request = False
        if step:
            if foreign:
                if another_request:
                    is_another_request = True
                slice={'slice':str(input_slice),'request':another_request}
                skipped = 'foreign'
            elif (step['status'] =='Skipped')or(step['status']=='NotCheckedSkipped'):
                skipped = 'skipped'
            else:

                skipped = 'run'
            tag = step['ctag']
            output_format = step.get('output_format','')
        task_short = ''
        return_tasks = []
        if tasks:

            for task in tasks:
                task['short'] = task['status'][0:8]
                task['finished_rate'] = 'full_finished'
                if task['status']=='finished':
                        if (task['total_files_tobeused']!=0) and (task['total_files_finished']!=0):
                            try:
                                if task['total_files_finished']!=task['total_files_tobeused']:
                                    task['finished_rate'] = 'finished90'
                                task_rate = (float(task['total_files_finished']))/(float(task['total_files_tobeused']))
                                if task_rate < 0.7:
                                    task['finished_rate'] = 'finished60'
                                elif task_rate < 0.9:
                                    task['finished_rate'] = 'finished80'
                            except:
                                pass

                task['href'] = BIG_PANDA_TASK_BASE + str(task['id'])
                task['href_local'] = PRODTASK_TASK_BASE.replace(FAKE_TASK_NUMBER,str(task['id']))
                return_tasks.append(task)
                if task['is_extension']:
                    ext_task = deepcopy(task)
                    ext_task['short'] =(' ^'+ 'ext.' + '^ ')
                    ext_task['href'] = PRODTASK_TASK_BASE.replace(FAKE_TASK_NUMBER,str(ext_task['id']))

                    return_tasks.append(ext_task)
        return {'step':step, 'tag':tag, 'skipped':skipped, 'tasks_real':tasks, 'tasks':return_tasks, 'slice':slice,
                'is_another_request':is_another_request,'output_format':output_format}

    def unwrap(pattern_dict):
        return_list = []
        if type(pattern_dict) == dict:
            for key in pattern_dict:
                if key != 'ctag':
                    return_list.append((key,pattern_dict[key]))
            return pattern_dict.get('ctag',''), return_list
        else:
            return pattern_dict,[('ctag',pattern_dict)]

    def get_last_step_format(slice_data):
        return_value = ''
        real_events = ''
        for step in slice_data[1]:
            if step.get('step'):
                if (step['skipped'] not in ['skipped','foreign']) and (not real_events):
                    real_events = step['step']['input_events']
                return_value = step['output_format']
        return return_value, real_events

    def check_empty_pattern(pattern, default_pattern):
        for x in pattern.keys():
            if pattern[x]:
                return pattern
        return default_pattern

    if request.method == 'GET':
        try:
            PRODTASK_TASK_BASE = request.build_absolute_uri(reverse('prodtask:task',args=[FAKE_TASK_NUMBER,]))
            cur_request = TRequest.objects.get(reqid=rid)
            #steps_db =
            _logger.debug(form_request_log(rid,request,'Start prepare data fro request page'))
            long_description = cur_request.info_field('long_description')
            original_spreadsheet = cur_request.info_field('data_source')
            if cur_request.request_type != 'MC':
                STEPS_LIST = [str(x) for x in range(len(StepExecution.STEPS))]
                pattern_list_name = [('Empty', [unwrap({'ctag':'','project_mode':'','nEventsPerJob':''}) for step in STEPS_LIST])]
            else:
                pattern_list_name = []
                STEPS_LIST = StepExecution.STEPS
                # Load patterns which are currently in use
                pattern_list = MCPattern.objects.filter(pattern_status='IN USE').order_by('pattern_name')
                pattern_list_name = [(x.pattern_name,
                                      [unwrap(check_empty_pattern(json.loads(x.pattern_dict).get(step,{}),{'ctag':'','project_mode':get_default_project_mode_dict()[step],'nEventsPerJob':get_default_nEventsPerJob_dict()[step]})) for step in StepExecution.STEPS]) for x in pattern_list]
                # Create an empty pattern for color only pattern
                pattern_list_name += [('Empty', [unwrap({'ctag':'','project_mode':get_default_project_mode_dict()[step],'nEventsPerJob':get_default_nEventsPerJob_dict()[step]}) for step in StepExecution.STEPS])]
                pattern_list_name += [('Initial', [{} for step in STEPS_LIST])]
            show_reprocessing = (cur_request.request_type == 'REPROCESSING') or (cur_request.request_type == 'HLT')
            input_lists_pre = list(InputRequestList.objects.filter(request=cur_request).order_by('slice'))
            input_list_count = InputRequestList.objects.filter(request=cur_request).count()
            # input_lists - list of tuples for end to form.
            # tuple format:
            # first element - InputRequestList object
            # second element - List of step dict in order
            # third element - approve slice string
            # fourth element - boolean, true if some task already related for steps
            input_lists = []
            approved_count = 0
            total_slice = 0
            slice_pattern = []
            edit_mode = True
            fully_approved = 0
            hidden_slices = 0
            input_list_index = {}
            total_task_dict = {}
            total_steps_count = 0
            approved_steps_count = 0
            comment_author = ' '
            last_comment = ' '
            autorized_change_request = True
            needed_management_approve = False
            first_approval_message = ''
            show_is_fast = False
            show_split = False
            slice_priorities = set()
            manage_slice_priorities = {}
            is_open_ended = False
            try:
                is_open_ended = OpenEndedRequest.objects.filter(request=cur_request,status='open').exists()
            except:
                pass
            if (cur_request.request_type in ['HLT','REPROCESSING', 'GROUP']) or (cur_request.phys_group == 'VALI'):
                show_is_fast = True
            if (cur_request.request_type == 'MC') and (cur_request.phys_group!='VALI'):
                try:
                    if (not request.user.is_superuser) and (request.user.username not in MC_COORDINATORS) and \
                            ('MCCOORD' not in egroup_permissions(request.user.username)):
                        autorized_change_request = False
                    if cur_request.cstatus == 'waiting':
                        needed_management_approve = True
                    else:
                        request_registration = RequestStatus.objects.filter(request=cur_request,status='registered')
                        request_cancellation = RequestStatus.objects.filter(request=cur_request,status='cancelled')
                        if request_registration:
                            first_approval_message = 'Request was approved for processing by %s at %s'%(request_registration[0].owner,
                                                                                                        request_registration[0].timestamp.ctime() )
                        elif request_cancellation:
                            first_approval_message = 'Request was cancelled by %s at %s'%(request_cancellation[0].owner,
                                                                                                        request_cancellation[0].timestamp.ctime() )
                        else:
                            approved_by = RequestStatus.objects.filter(request=cur_request,status='approved')
                            if approved_by:
                                first_approval_message = 'Request was started by %s at %s without PMG approval '%(approved_by[0].owner,approved_by[0].timestamp.ctime())
                except:
                    autorized_change_request = False
            comments = RequestStatus.objects.filter(request=cur_request,status='comment').order_by('-timestamp').first()
            if comments:
                comment_author = comments.owner
                last_comment = comments.comment
            show_as_huge = False
            if not input_lists_pre:
                edit_mode = True
                use_input_date_for_pattern = True
            else:
                # choose how to form input data pattern: from jobOption or from input dataset
                slice_pattern = '*'
                use_input_date_for_pattern = True
                if not input_lists_pre[0].input_data:
                        use_input_date_for_pattern = False
                if input_list_count < 50:
                    if use_input_date_for_pattern:
                        slice_pattern = input_lists_pre[0].input_data.split('.')
                    else:
                        if input_lists_pre[0].dataset:
                            slice_pattern = input_lists_pre[0].dataset.name.split('.')
                        else:
                            slice_pattern = ''
                failed_slices = set()
                cloned_slices = []
                do_all = True
                do_cloned_and_failed = False
                if ((input_list_count>800) and (not show_hidden) and ((cur_request.request_type == 'MC')or(cur_request.request_type == 'EVENTINDEX'))) or \
                        ((input_list_count>200)and (not show_hidden) and (cur_request.request_type == 'MC')and('pMSSM' in cur_request.description)):

                    show_as_huge = True
                    do_all = False
                    pattern_steps = StepExecution.objects.filter(request=rid, slice=input_lists_pre[0])
                    total_task_dict['red'] = 0
                    total_task_dict['done'] = 0
                    total_task_dict['finished'] = 0
                    total_task_dict['blue'] = 0
                    total_task_dict['running'] = 0
                    total_task_dict['total'] = ProductionTask.objects.filter(request=rid).count()
                    if total_task_dict['total'] != 0:
                        failed_task_list = ProductionTask.objects.filter(Q(status__in=['failed','broken','aborted','obsolete']),Q(request=cur_request))
                        total_task_dict['red'] = len(failed_task_list)
                        total_task_dict['done'] = ProductionTask.objects.filter(Q(status='done'),Q(request=cur_request)).count()
                        finished_task_list = ProductionTask.objects.filter(Q(status='finished'),Q(request=cur_request))
                        total_task_dict['finished'] = len(finished_task_list)
                        total_task_dict['running'] = ProductionTask.objects.filter(Q(status='running'),Q(request=cur_request)).count()
                        total_task_dict['blue'] = total_task_dict['total'] - (total_task_dict['red'] +  total_task_dict['done'] + total_task_dict['finished'] + total_task_dict['running'])
                        #Find slices with broken slices
                        if ((total_task_dict['red']+total_task_dict['finished'])>0)and(total_task_dict['red']<600):
                            map(lambda x: failed_slices.add(x.step.slice.id),failed_task_list)
                            map(lambda x: failed_slices.add(x.step.slice.id),finished_task_list)
                            cloned_slices = [x.id for x in input_lists_pre if x.cloned_from]
                            do_cloned_and_failed = True
                    input_lists_pre_pattern = deepcopy(input_lists_pre[0])
                    total_steps_count = StepExecution.objects.filter(request=rid).count()
                    approved_steps_count = StepExecution.objects.filter(request=rid, status='Approved').count()
                    steps = {}
                    for step in pattern_steps:
                        steps[step.step_template.step] = step
                    slice_steps_ordered = []
                    input_lists_pre_pattern.input_data = '=> %i slices <=' % input_list_count
                    input_lists_pre_pattern.slice = '-1'
                    if (cur_request.request_type == 'MC'):
                        for step_name in StepExecution.STEPS:
                            if step_name not in steps:
                                slice_steps_ordered.append(form_step_obj({},{},-1))
                            else:
                                step_dict = model_to_dict(steps[step_name])
                                step_dict.update({'ctag':steps[step_name].step_template.ctag})
                                step_dict.update({'slice':input_lists_pre_pattern})
                                slice_steps_ordered.append(form_step_obj(step_dict,{},-1))
                    else:
                        for step in pattern_steps:
                                step_dict = model_to_dict(step)
                                step_dict.update({'ctag':step.step_template.ctag})
                                step_dict.update({'slice':input_lists_pre_pattern})
                                slice_steps_ordered.append(form_step_obj(step_dict,{},-1))
                        slice_steps_ordered += [form_step_obj({},{},-1)] * (len(StepExecution.STEPS) - len(pattern_steps))
                    if (cur_request.request_type == 'MC'):
                        approved = (total_steps_count >= input_list_count) and ((approve_level(slice_steps_ordered)*input_list_count)==(approved_steps_count))
                    else:
                        approved = True
                    slice_dict = model_to_dict(input_lists_pre_pattern)
                    if (cur_request.request_type == 'MC'):
                        slice_dict['dataset'] = ''
                    if approved:
                        input_lists.append((slice_dict, slice_steps_ordered, get_approve_status(slice_steps_ordered,input_lists_pre_pattern),
                                            False,'',approve_level(slice_steps_ordered),'no',has_waiting(slice_steps_ordered)))
                    else:
                        input_lists.append((slice_dict, slice_steps_ordered, 'not_submitted',
                                            False,'',-1,'no',False))
                if do_all or do_cloned_and_failed:
                    if do_all or ((len(cloned_slices)+len(failed_slices))>80):
                        steps_db = list(StepExecution.objects.filter(request=rid).values())

                    else:
                        steps_db = list(StepExecution.objects.filter(Q(request=rid),Q(slice_id__in=cloned_slices+list(failed_slices))).values())
                    tasks_db = list(ProductionTask.objects.filter(request=rid).order_by('-submit_time').values())
                    step_templates_set = set()
                    steps = {}
                    for current_step in steps_db:
                        steps[current_step['slice_id']] = steps.get(current_step['slice_id'],[])+[current_step]
                        step_templates_set.add(current_step['step_template_id'])
                    tasks = {}
                    for current_task in tasks_db:
                        tasks[current_task['step_id']] =  tasks.get(current_task['step_id'],[]) + [current_task]
                    step_templates = {}
                    for step_template in step_templates_set:
                        step_templates[step_template] = StepTemplate.objects.get(id=step_template)
                    for slice in input_lists_pre:
                        if (not show_hidden) and slice.is_hide:
                            hidden_slices += 1
                            continue
                        if (do_cloned_and_failed):
                            if slice.id not in (cloned_slices+list(failed_slices)):
                                continue
                        #step_execs = StepExecution.objects.filter(slice=slice)

                        #step_execs = [x for x in steps if x.slice == slice]
                        try:
                            step_execs = steps[slice.id]
                        except:
                            step_execs = []
                        slice_priorities.add(str(slice.priority))
                        manage_slice_priorities[str(slice.priority)] = manage_slice_priorities.get(str(slice.priority),0) + 1
                        slice_steps = {}
                        total_slice += 1
                        show_task = False
                        # creating a pattern
                        # if input_list_count < 50:
                        #     if use_input_date_for_pattern:
                        #         if slice.input_data:
                        #             current_slice_pattern = slice.input_data.split('.')
                        #         else:
                        #             current_slice_pattern=''
                        #     else:
                        #         if slice.dataset:
                        #             current_slice_pattern = slice.dataset.name.split('.')
                        #         else:
                        #             current_slice_pattern=''
                        #
                        #     if current_slice_pattern:
                        #         for index,token in enumerate(current_slice_pattern):
                        #             if index >= len(slice_pattern):
                        #                 slice_pattern.append(token)
                        #             else:
                        #                 if token!=slice_pattern[index]:
                        #                     slice_pattern[index] = os.path.commonprefix([token,slice_pattern[index]])
                        #                     slice_pattern[index] += '*'
                        # Creating step dict
                        slice_steps_list = []
                        temp_step_list = []
                        another_chain_step = None
                        for step in step_execs:
                            step_task = []
                            try:
                                step_task = tasks[step['id']]

                            except Exception,e:
                                step_task = []

                            if step_task:
                                show_task = True
                            ctag = step_templates[step['step_template_id']].ctag
                            step_name = step_templates[step['step_template_id']].step
                            step_format = step_templates[step['step_template_id']].output_formats
                            step.update({'ctag':ctag,'output_format':step_format})
                            if cur_request.request_type == 'MC':

                                slice_steps.update({step_name:form_step_obj(step,step_task,slice.slice)})

                            else:

                                if step['id'] == step['step_parent_id']:
                                    slice_steps_list.append((step['id'],form_step_obj(step,step_task,slice.slice)))
                                else:
                                    temp_step_list.append((step,step_task))
                        if cur_request.request_type == 'MC':
                            show_split = True
                            first_step = True
                            slice_steps_ordered = []
                            another_chain_step_dict = {}
                            for step_name in StepExecution.STEPS:
                                slice_steps_ordered.append(form_step_obj({},{},slice.slice))
                            for index,step_name in enumerate(StepExecution.STEPS):
                                if step_name not in slice_steps:
                                    pass
                                else:
                                    slice_steps_ordered[index] = slice_steps[step_name]
                                    if first_step:
                                        first_step = False
                                        if slice_steps[step_name]['step']['id'] != slice_steps[step_name]['step']['step_parent_id']:
                                            another_chain_step = StepExecution.objects.get(id=slice_steps[step_name]['step']['step_parent_id'])
                                            another_chain_step_dict = model_to_dict(another_chain_step)
                                            another_chain_step_dict.update({'ctag':another_chain_step.step_template.ctag})
                                            another_chain_index = StepExecution.STEPS.index(another_chain_step.step_template.step)
                                            if another_chain_step_dict['request'] != int(rid):
                                                slice_steps_ordered[another_chain_index] = form_step_obj(another_chain_step_dict
                                                                                                         ,{}, another_chain_step.slice.slice,
                                                                                                      True,another_chain_step_dict['request'])
                                            else:
                                                slice_steps_ordered[another_chain_index] = form_step_obj(another_chain_step_dict
                                                         ,{}, another_chain_step.slice.slice,
                                                      True)
                            #slice_steps_ordered = [slice_steps.get(x,form_step_obj({},{},slice.slice)) for x in StepExecution.STEPS]
                            approved = get_approve_status(slice_steps_ordered)

                            if (approved['submitted'] == 'submitted')or(approved['submitted'] == 'partially_submitted'):
                                    approved_count += 1
                            if (approved['submitted'] == 'submitted'):
                                fully_approved +=1
                            slice_dict = model_to_dict(slice)
                            if not slice_dict['dataset']:
                                slice_dict['dataset'] = ''
                            input_list_index.update({slice_dict['id']:len(input_lists)})
                            cloned = 'no'
                            if slice_dict['cloned_from']:
                                if slice_dict['cloned_from'] in input_list_index:
                                   temp_list = list(input_lists[input_list_index[slice_dict['cloned_from']]])
                                   temp_list[6] = str(slice_dict['slice'])
                                   input_lists[input_list_index[slice_dict['cloned_from']]] = tuple(temp_list)
                            if another_chain_step_dict:
                                input_lists.append((slice_dict, slice_steps_ordered, get_approve_status(slice_steps_ordered,slice),
                                                    show_task,another_chain_step_dict['id'],approve_level(slice_steps_ordered),cloned,has_waiting(slice_steps_ordered)))
                            else:
                                input_lists.append((slice_dict, slice_steps_ordered, get_approve_status(slice_steps_ordered,slice),
                                                    show_task,'',approve_level(slice_steps_ordered),cloned,has_waiting(slice_steps_ordered)))

                            if (not show_task)or(fully_approved<total_slice):
                                edit_mode = True
                        else:
                            i = 0
                            if not(slice_steps_list) and (len(temp_step_list) == 1):
                                if temp_step_list[0][0]['step_parent_id'] != temp_step_list[0][0]['id']:
                                    # step in other chain
                                    another_chain_step_obj = StepExecution.objects.get(id=temp_step_list[0][0]['step_parent_id'])
                                    another_chain_step = model_to_dict(another_chain_step_obj)
                                    another_chain_step.update({'ctag':another_chain_step_obj.step_template.ctag})
                                    if another_chain_step['request'] != int(rid):
                                        slice_steps_list.append((another_chain_step['id'], form_step_obj(another_chain_step,{},
                                                                                                      another_chain_step_obj.slice.slice,
                                                                                                      True,another_chain_step['request'])))
                                    else:
                                        slice_steps_list.append((another_chain_step['id'], form_step_obj(another_chain_step,{},
                                                                                                          another_chain_step_obj.slice.slice,
                                                                                                          True)))
                                slice_steps_list.append((temp_step_list[0][0]['id'],form_step_obj(temp_step_list[0][0],temp_step_list[0][1],slice.slice)))
                                temp_step_list.pop(0)
                            if not slice_steps_list:
                                step_id_list = [x[0]['id'] for x in temp_step_list]
                                # find a root of chain
                                for index,current_step in enumerate(temp_step_list):
                                    if current_step[0]['step_parent_id'] not in step_id_list:
                                        # step in other chain
                                        another_chain_step_obj = StepExecution.objects.get(id=temp_step_list[index][0]['step_parent_id'])
                                        another_chain_step = model_to_dict(another_chain_step_obj)
                                        another_chain_step.update({'ctag':another_chain_step_obj.step_template.ctag})
                                        if another_chain_step['request'] != int(rid):
                                            slice_steps_list.append((another_chain_step['id'], form_step_obj(another_chain_step,{},
                                                                                                          another_chain_step_obj.slice.slice,
                                                                                                          True,another_chain_step['request'])))
                                        else:
                                            slice_steps_list.append((another_chain_step['id'], form_step_obj(another_chain_step,{},
                                                                                                          another_chain_step_obj.slice.slice,
                                                                                                          True)))
                                        slice_steps_list.append((current_step[0]['id'],form_step_obj(current_step[0],current_step[1],slice.slice)))
                                        temp_step_list.pop(index)


                            for i in range(len(temp_step_list)):
                                j = 0
                                while (temp_step_list[j][0]['step_parent_id']!=slice_steps_list[-1][0]):
                                    j+=1
                                    if j >= len(temp_step_list):
                                        raise ValueError('Not linked chain')
                                        #break
                                slice_steps_list.append((temp_step_list[j][0]['id'],form_step_obj(temp_step_list[j][0],temp_step_list[j][1],slice.slice)))

                            edit_mode = True
                            slice_steps = [x[1] for x in slice_steps_list] + [form_step_obj({},{},slice.slice)]*(len(STEPS_LIST)-len(slice_steps_list))
                            approved = get_approve_status(slice_steps[:len(slice_steps_list)])
                            if (approved['submitted'] == 'submitted')or(approved['submitted'] == 'partially_submitted'):
                                    approved_count += 1
                            slice_dict =  model_to_dict(slice)
                            if not slice_dict['dataset']:
                                slice_dict['dataset'] = ''
                            input_list_index.update({slice_dict['id']:len(input_lists)})
                            cloned = 'no'
                            if slice_dict['cloned_from']:
                                if slice_dict['cloned_from'] in input_list_index:
                                   temp_list = list(input_lists[input_list_index[slice_dict['cloned_from']]])
                                   temp_list[6] = str(slice_dict['slice'])
                                   input_lists[input_list_index[slice_dict['cloned_from']]] = tuple(temp_list)
                            if another_chain_step:
                                input_lists.append((slice_dict, slice_steps, get_approve_status(slice_steps,slice),  show_task,
                                                    another_chain_step['id'], approve_level(slice_steps),cloned,has_waiting(slice_steps)))
                            else:
                                input_lists.append((slice_dict, slice_steps, get_approve_status(slice_steps,slice),  show_task, '',
                                                    approve_level(slice_steps),cloned,has_waiting(slice_steps)))


            step_list = [{'name':x,'idname':x.replace(" ",'')} for x in STEPS_LIST]
            jira_problem_link = ''
            if cur_request.jira_reference:
                jira_problem_link = cur_request.jira_reference
            for cur_slice in range(len(input_lists)):
                temp_list = list(input_lists[cur_slice])
                slice_output, real_events = get_last_step_format(temp_list)
                events_str = str(temp_list[0]['input_events'])
                if real_events and (real_events != temp_list[0]['input_events']):
                    events_str = "%s (%s)"%(str(real_events), str(temp_list[0]['input_events']))
                if (len(slice_output) > 60) and (cur_request.request_type == 'REPROCESSING'):
                    temp_list+=[slice_output[:60]+"...",slice_output,events_str]
                else:
                    temp_list+=[slice_output,slice_output,events_str]

                input_lists[cur_slice] = tuple(temp_list)
            if cur_request.is_error:
                has_deft_problem = True
            else:
                has_deft_problem = False
            _logger.debug(form_request_log(rid,request,'Finish prepare data fro request page'))
            train_pattern_list = []
            try:
                if cur_request.request_type == 'MC':
                    pattern_type = 'mc_pattern'
                else:
                    pattern_type = 'data_pattern'
                trains = TrainProduction.objects.filter(status=pattern_type).order_by('id')
                for train in trains:
                    train_pattern_list.append({'train_id':train.id,'request_name':'('+str(train.pattern_request.reqid)+
                                                                                  ')'+
                                                                                  train.pattern_request.description})
            except:
                pass
            child_requests = []
            parent_request_id = ''
            try:
                related_requests = ParentToChildRequest.objects.filter(parent_request=rid,relation_type__in=['BC,MA','CL','MR'],status='active')
                for related_request in related_requests:
                    pattern_name = ''
                    pattern_id = rid
                    if related_request.train:
                        pattern_name = related_request.train.pattern_request.description
                        pattern_id = int(related_request.train.pattern_request_id)
                    if related_request.child_request:
                        child_requests.append({'request':int(related_request.child_request_id),'pattern_id':pattern_id,
                                           'pattern_name':pattern_name,
                                           'type':dict(ParentToChildRequest.RELATION_TYPE)[related_request.relation_type]})
                    else:
                        child_requests.append({'request':None,'pattern_id':pattern_id,
                                           'pattern_name':pattern_name,
                                           'type':dict(ParentToChildRequest.RELATION_TYPE)[related_request.relation_type]})
                related_requests = ParentToChildRequest.objects.filter(child_request=rid,relation_type__in=['SP','CL'],status='active')
                for related_request in related_requests:
                    child_requests.append({'request':int(related_request.parent_request_id),'pattern_id':None,
                                           'pattern_name':'Parent',
                                           'type':dict(ParentToChildRequest.RELATION_TYPE)[related_request.relation_type]})
                if ParentToChildRequest.objects.filter(child_request=rid,relation_type ='CL',status='active').exists():
                    parent_request_id = str(ParentToChildRequest.objects.filter(child_request=rid,relation_type ='CL',status='active').last().parent_request_id)

            except:
                pass
            hashtags_string = ''
            hashtags_path = ''
            try:
                hashtags_string,hashtags_path = form_hashtag_string(cur_request.reqid)
            except:
                pass
            selected_slices = json.dumps([])
            manage_priorities = []
            priorities_list = []
            PMG_PRIORITIES = ['-2','0','1','2','3','4']
            if manage_slice_priorities:
                for item in manage_slice_priorities.keys():
                    if item in PMG_PRIORITIES:
                        manage_priorities.append({'slices':manage_slice_priorities[item], 'value':item, 'name':item.replace('-2','0+')})

                priorities_list = [{'value':x,'name':x.replace('-2','0+')} for x in PMG_PRIORITIES]
            try:
                if 'selected_slices' in request.session:
                    selected_slices = json.dumps(map(int,request.session['selected_slices']))
                    del request.session['selected_slices']
            except:
                pass
            project_list = [str(x) for x in list(TProject.objects.all())]
            return   render(request, 'prodtask/_reqdatatable.html', {
               'active_app' : 'prodtask',
               'parent_template' : 'prodtask/_index.html',
                'pre_form_text': 'Reuqest ' + str(cur_request.reqid),
               'trequest': cur_request,
               'inputLists': input_lists,
               'step_list': step_list,
               'pattern_list': pattern_list_name,
               'pr_id': rid,
               'approvedCount': approved_count,
               'pattern': '.'.join(list(slice_pattern)),
               'totalSlice':total_slice,
               'edit_mode':edit_mode,
               'show_reprocessing':show_reprocessing,
               'not_use_input_date_for_pattern':not use_input_date_for_pattern,
               'has_deft_problem':has_deft_problem,
               'jira_problem_link':jira_problem_link,
               'hidden_slices':hidden_slices,
               'total_tasks': total_task_dict,
               'show_as_huge': show_as_huge,
               'approved_steps': approved_steps_count,
               'total_steps' : total_steps_count,
               'long_description':long_description,
               'last_comment':last_comment,
               'comment_author':comment_author,
               'autorized_change_request':autorized_change_request,
               'show_is_fast':show_is_fast,
               'show_split':show_split,
               'needed_management_approve':needed_management_approve,
               'first_approval_message':first_approval_message,
               'is_open_ended':is_open_ended,
                'page_title':'%s - Request'%str(rid),
                'train_pattern_list':train_pattern_list,
                'selected_slices':selected_slices,
                'original_spreadsheet':original_spreadsheet,
                'child_requests':child_requests,
                'hashtags_string':hashtags_string,
                'hashtags_path':hashtags_path,
                'manage_priorities':manage_priorities,
                'priorities_list':priorities_list,
                'priorities_str':','.join([x.replace('-2','0+') for x in slice_priorities]),
                'project_list':project_list,
                'parent_request_id':parent_request_id,
                'bigpanda_base':BIG_PANDA_TASK_BASE
               })
        except Exception, e:
            _logger.error("Problem with request list page data forming: %s" % e)
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    return HttpResponseRedirect(reverse('prodtask:request_table'))


def step_template_details(request, rid=None):
    if rid:
        try:
            step_template = StepTemplate.objects.get(id=rid)
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    else:
        return HttpResponseRedirect(reverse('prodtask:request_table'))

    return render(request, 'prodtask/_step_template_detail.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'StepTemplate details with ID = %s' % rid,
       'step': step_template,
       'parent_template' : 'prodtask/_index.html',
   })

class StepTemlateTable(datatables.DataTable):

    id = datatables.Column(
        label='Step Template ID',
        model_field='id',
        )

    step = datatables.Column(
        label='Step',
        )

    ctag = datatables.Column(
        label='C-tag',
        )

    def_time = datatables.Column(
        label='Definition time',
        )

    priority = datatables.Column(
        label='Priority',
        )
    swrelease = datatables.Column(
        label='SWRelease',
        )


    class Meta:
        model = StepTemplate
        bSort = True
        bPaginate = True
        bJQueryUI = True
        sScrollX = '100em'
        sScrollY = '20em'
        bScrollCollapse = True

        aaSorting = [[0, "desc"]]
        aLengthMenu = [[10, 50, 1000], [10, 50, 1000]]
        iDisplayLength = 10

        bServerSide = True
        
        def __init__(self):
            self.sAjaxSource = reverse('prodtask:step_template_table')

@datatables.datatable(StepTemlateTable, name='fct')
def step_template_table(request):
    qs = request.fct.get_queryset()
    request.fct.update_queryset(qs)
    return TemplateResponse(request, 'prodtask/_datatable.html', {  'title': 'StepTemlates Table', 'active_app' : 'prodtask', 'table': request.fct,
                'parent_template': 'prodtask/_index.html'})



def stepex_details(request, rid=None):
    if rid:
        try:
            step_ex = StepExecution.objects.get(id=rid)
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    else:
        return HttpResponseRedirect(reverse('prodtask:request_table'))

    return render(request, 'prodtask/_step_ex_detail.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'StepExecution details with ID = %s' % rid,
       'step_ex': step_ex,
       'parent_template' : 'prodtask/_index.html',
   })

class StepExecutionTable(datatables.DataTable):


    id = datatables.Column(
        label='STEP_EX',
        )
    slice = datatables.Column(
        label='SliceID',
        model_field='slice__id'
        )

    request = datatables.Column(
        label='Request',
        model_field='request__reqid'
        )

    step_template = datatables.Column(
        label='Step Template',
        model_field='step_template__step'
        )

    status = datatables.Column(
        label='Status',
        )

    priority = datatables.Column(
        label='Priority',
        )
    def_time = datatables.Column(
        label='Definition time',
        )


    class Meta:
        model = StepExecution
        bSort = True
        bPaginate = True
        bJQueryUI = True
        sScrollX = '100em'
        sScrollY = '20em'
        bScrollCollapse = True

        aaSorting = [[0, "desc"]]
        aLengthMenu = [[10, 50, 1000], [10, 50, 1000]]
        iDisplayLength = 10

        bServerSide = True

        def __init__(self):
            self.sAjaxSource = reverse('prodtask:step_execution_table')


@datatables.datatable(StepExecutionTable, name='fct')
def step_execution_table(request):
    qs = request.fct.get_queryset()
    request.fct.update_queryset(qs)
    return TemplateResponse(request, 'prodtask/_datatable.html', {  'title': 'StepExecutions Table', 'active_app' : 'prodtask', 'table': request.fct,
                                                                'parent_template': 'prodtask/_index.html'})


def production_dataset_details(request, name=None):
   if name:
       try:
           dataset = ProductionDataset.objects.get(name=name)
       except:
           return HttpResponseRedirect(reverse('prodtask:request_table'))
   else:
       return HttpResponseRedirect(reverse('prodtask:request_table'))

   return render(request, 'prodtask/_dataset_detail.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'ProductionDataset details with Name = %s' % name,
       'dataset': dataset,
       'parent_template' : 'prodtask/_index.html',
   })

class ProductionDatasetTable(datatables.DataTable):

    name = datatables.Column(
        label='Dataset',
        sClass='breaked_word',
        )

    task_id = datatables.Column(
        label='TaskID',
        sClass='numbers',
        )

    parent_task_id = datatables.Column(
        label='ParentTaskID',
        bVisible='false',
        )

    rid = datatables.Column(
        label='ReqID',
        bVisible='false',
        )

    phys_group = datatables.Column(
        label='Phys Group',
        sClass='px180 centered',
        )

    events = datatables.Column(
        label='Events',
        bVisible='false',
        )
        
    files = datatables.Column(
        label='Files',
        bVisible='false',
        )

    status = datatables.Column(
        label='Status',
        sClass='px100 centered',
        )
        
    timestamp = datatables.Column(
        label='Timestamp',
        sClass='px140 centered',
        )


    class Meta:
        model = ProductionDataset

        id = 'dataset_table'
        var = 'datasetTable'

        bSort = True
        bPaginate = True
        bJQueryUI = True

        sDom = '<"top-toolbar"lf><"table-content"rt><"bot-toolbar"ip>'

        bAutoWidth = False
        bScrollCollapse = False

        fnServerParams = "datasetServerParams"

        fnClientTransformData = "prepareData"

        aaSorting = [[1, "desc"]]
        aLengthMenu = [[100, 1000, -1], [100, 1000, "All"]]
        iDisplayLength = 100

        bServerSide = True

        def __init__(self):
            sAjaxSource = reverse('production_dataset_table')

    def apply_filters(self, request):
        qs = self.get_queryset()

#        qs = qs.filter( status__in=['aborted','broken','failed','deleted',
#                'toBeDeleted','toBeErased','waitErased','toBeCleaned','waitCleaned'] )
        filters = 0
        for status in ['aborted','broken','failed','deleted', 'toBeDeleted','toBeErased','waitErased','toBeCleaned','waitCleaned']:
            if filters:
                filters |= Q(status__iexact=status)
            else:
                filters = Q(status__iexact=status)
        qs = qs.filter(filters)

        parameters = [ ('datasetname','name'), ('status','status'), ('campaign','campaign'), ]

        for param in parameters:
            value = request.GET.get(param[0], 0)
            if value and value != '':
                if param[0] == 'datasetname':
                    qs = qs.filter(Q( **{ param[1]+'__iregex' : value } ))
                else:
                    qs = qs.filter(Q( **{ param[1]+'__iexact' : value } ))

        self.update_queryset(qs)


@datatables.datatable(ProductionDatasetTable, name='fct')
def production_dataset_table(request):
#    qs = request.fct.get_queryset()
    request.fct.apply_filters(request)
#    request.fct.update_queryset(qs)

    return TemplateResponse(request, 'prodtask/_dataset_table.html', {  'title': 'Aborted and Obsolete Production Dataset Status Table', 'active_app' : 'prodtask', 'table': request.fct,
                                                                'parent_template': 'prodtask/_index.html'})


@never_cache
def userinfo(request):
    return TemplateResponse(request, "prodtask/_userinfo.html",
            {
                 'title': 'User info',
                 'active_app' : 'prodtask',
                 'parent_template': 'prodtask/_index.html',
            })


def set_request_status(username, reqid, status, message, comment, request=None):
    STATUS_ORDER = ['registered','approved','canceled']

    _logger.debug(form_request_log(reqid,request,message))

    cur_request = TRequest.objects.get(reqid=reqid)
    request_status = RequestStatus(request=cur_request,comment=comment,owner=username,
                                   status=status)
    request_status.save_with_current_time()
    if (status in STATUS_ORDER) and (cur_request.cstatus in STATUS_ORDER):
        if STATUS_ORDER.index(status) >= STATUS_ORDER.index(cur_request.cstatus):
            cur_request.cstatus = status
    else:
        cur_request.cstatus = status
    cur_request.save()


def create_steps_in_child_pattern(new_request, parent_steps, pattern_request, outputs_slices, status='NotChecked'):
    spreadsheet_dict = []
    pattern_slices = set()
    for output_slices in outputs_slices:
        pattern_slices.add(output_slices[0])
    step_pattern = {}
    for pattern_slice_number in pattern_slices:
        pattern_slice = InputRequestList.objects.get(request=pattern_request,slice=int(pattern_slice_number))
        step_pattern[pattern_slice_number] = StepExecution.objects.filter(slice=pattern_slice)[0]
    slice_index = 0
    for parent_step in parent_steps:
        #parent_step = StepExecution.objects.get(id=int(parent_step_id))
        for output_slice in outputs_slices:
            current_step_pattern = step_pattern[output_slice[0]]
            current_output_formats = []
            for output in output_slice[1]:
                if output in current_step_pattern.step_template.output_formats.split('.'):
                    current_output_formats.append(output)
            if current_output_formats:
                st_sexec_list = []
                irl = dict(slice=slice_index, brief=current_step_pattern.slice.brief, comment=current_step_pattern.slice.comment, dataset=parent_step.slice.dataset_id,
                           input_data=parent_step.slice.input_data,
                           project_mode=current_step_pattern.slice.project_mode,
                           priority=int(current_step_pattern.slice.priority),
                           input_events=-1)
                slice_index += 1
                sexec = dict(status=status, priority=int(current_step_pattern.priority),
                             input_events=-1)
                task_config =  current_step_pattern.get_task_config()
                nEventsPerJob = task_config.get('nEventsPerJob','')
                task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
                st_sexec_list.append({'step_name': current_step_pattern.step_template.step, 'tag': current_step_pattern.step_template.ctag
                                         , 'step_exec': sexec,
                                  'memory': int(current_step_pattern.step_template.memory),
                                  'formats': '.'.join(current_output_formats),
                                  'task_config':task_config,'parent_step_id':parent_step.id})
                spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
    make_slices_from_dict(new_request,spreadsheet_dict)


def make_slices_from_dict(req, file_dict):
        step_parent_dict = {}
        for current_slice in file_dict:
            input_data = current_slice["input_dict"]
            input_data['request'] = req
            priority_obj = get_priority_object(input_data['priority'])
            if input_data.get('dataset'):
                    input_data['dataset'] = fill_dataset(input_data['dataset'])
            _logger.debug("Filling input data: %s" % input_data)
            input_data['slice'] = req.get_next_slice()
            irl = InputRequestList(**input_data)
            irl.save()
            for step in current_slice.get('step_exec_dict'):
                st = fill_template(step['step_name'], step['tag'], input_data['priority'],
                                   step.get('formats', None), step.get('memory', None))
                task_config= {}
                upadte_after = False
                if 'task_config' in step:
                    if 'nEventsPerJob' in step['task_config']:
                        if step['task_config']['nEventsPerJob'].get(step['step_name'],''):
                            task_config.update({'nEventsPerJob':int(step['task_config']['nEventsPerJob'].get(step['step_name']))})

                            if step['step_name']=='Evgen':
                                task_config.update({'nEventsPerInputFile':int(step['task_config']['nEventsPerJob'].get(step['step_name'],0))})
                        else:
                            task_config.update({'nEventsPerJob':step['task_config']['nEventsPerJob'].get(step['step_name'])})
                    task_config_options = ['project_mode','input_format','token','nFilesPerMergeJob','nEventsPerMergeJob',
                                           'nGBPerMergeJob','nMaxFilesPerMergeJob','merging_tag','nFilesPerJob',
                                           'nGBPerJob','maxAttempt','maxFailure','split_slice','evntFilterEff']
                    for task_config_option in task_config_options:
                        if task_config_option in step['task_config']:
                            task_config.update({task_config_option:step['task_config'][task_config_option]})
                step['step_exec']['request'] = req
                step['step_exec']['slice'] = irl
                step['step_exec']['step_template'] = st
                step['step_exec']['priority'] = priority_obj.priority(st.step,st.ctag)
                _logger.debug("Filling step execution data: %s" % step['step_exec'])
                st_exec = StepExecution(**step['step_exec'])
                if ('parent_step_id' in step):
                    st_exec.step_parent = StepExecution.objects.get(id=step['parent_step_id'])
                else:
                    if step_parent_dict:
                        if ('step_parent' in step) and ('step_order' in step):
                            if (step['step_parent']==step['step_order']):
                                upadte_after = True
                            else:
                                st_exec.step_parent = step_parent_dict[step['step_parent']]
                        else:
                            upadte_after = True
                    else:
                        upadte_after = True
                if task_config:
                    st_exec.set_task_config(task_config)
                st_exec.save_with_current_time()
                if ('step_parent' in step) and ('step_order' in step):
                    step_parent_dict.update({step['step_order']:st_exec})
                else:
                    step_parent_dict.update({0:st_exec})
                if upadte_after:
                    if ('step_parent' in step) and ('step_order' in step):
                        st_exec.step_parent = step_parent_dict[step['step_parent']]
                    else:
                        st_exec.step_parent = step_parent_dict[0]
                    st_exec.save()
        return True


def tasks_progress(all_tasks):
    def get_all_tasks_from_request(parent_task_id):
        parent_task = ProductionTask.objects.get(id=parent_task_id)
        parent_request_tasks = list(ProductionTask.objects.filter(request=parent_task.request_id).values('id','total_events'))
        result_dict = {}
        for parent_request_task in parent_request_tasks:
            result_dict.update({int(parent_request_task['id']):int(parent_request_task['total_events'])})
        return result_dict

    def get_step_name(task_input, task_name):
        return '.'.join(task_input.split('.')[3:5])+'.'+\
               task_input[:task_input.rfind('_tid')].split('.')[-1].split('_')[-1][0]+'.'+task_name.split('.')[-1].split('_')[-1][0]


    step_by_name = {}
    step_statistic = {}
    #key - task id, {"input_events":int, "processed_events":int, "chain_id":id}
    processed_tasks = {}
    other_requests_tasks = {}
    chains = {}

    for task in all_tasks:
        if task.status not in  ProductionTask.RED_STATUS:
            task_input_events = 0
            task_step = get_step_name(task.inputdataset, task.name)
            if task_step not in step_by_name:
                step_by_name.update({task_step:task.step.step_template.step})
            parent_tasks_id = get_parent_tasks(task)
            chain_id = 0
            task_input_events = -1

            if step_by_name[task_step] in ['Simul']:
                task_input_events = task.step.input_events
            #Count number of events for parent task
            #evgen
            if (len(parent_tasks_id) == 0) or (task_input_events > -1):
                task_input_events = task.step.input_events
                if task_input_events == -1:
                    if int(task.step.get_task_config('nEventsPerInputFile'))<int(task.step.get_task_config('nEventsPerJob')):
                        task_input_events = task.total_files_tobeused*int(task.step.get_task_config('nEventsPerInputFile'))
                    else:
                        task_input_events = task.total_files_tobeused*int(task.step.get_task_config('nEventsPerJob'))
                chain_id = int(task.id)
                chains.update({int(task.id):[int(task.id)]})
            else:
                if len(parent_tasks_id) == 1:
                    parent_task_id = parent_tasks_id[0]
                    if parent_task_id in processed_tasks:
                        task_input_events = processed_tasks[parent_task_id]["processed_events"]
                        chains[processed_tasks[parent_task_id]["chain_id"]] = chains[processed_tasks[parent_task_id]["chain_id"]] + [int(task.id)]
                        chain_id = processed_tasks[parent_task_id]["chain_id"]
                    else:
                        if task.status == 'done':
                            task_input_events = task.total_events
                        else:
                            task_input_events = task.step.input_events
                            if task_input_events == -1:
                                if task.status=='finished':
                                    if int(task.step.get_task_config('nEventsPerInputFile'))<int(task.step.get_task_config('nEventsPerJob')):
                                        task_input_events = task.total_files_tobeused*int(task.step.get_task_config('nEventsPerInputFile'))
                                    else:
                                        task_input_events = task.total_files_tobeused*int(task.step.get_task_config('nEventsPerJob'))
                                else:
                                    if parent_task_id not in other_requests_tasks:
                                        other_requests_tasks.update(get_all_tasks_from_request(parent_task_id))
                                    task_input_events = other_requests_tasks[parent_task_id]
                        chains.update({int(task.id):[int(task.id)]})
                        chain_id = int(task.id)
                if len(parent_tasks_id) > 1:
                    chains.update({int(task.id):[int(task.id)]})

                    for parent_task_id in parent_tasks_id:
                        if parent_task_id in processed_tasks:
                            task_input_events += processed_tasks[parent_tasks_id]
                        else:
                            if parent_task_id not in other_requests_tasks:
                                other_requests_tasks.update(get_all_tasks_from_request(parent_task_id))
                            task_input_events += other_requests_tasks[parent_task_id]
                    chain_id = int(task.id)
            processed_tasks.update({int(task.id):{"input_events":task_input_events, "processed_events":task.total_events,
                                                  "chain_id":chain_id, 'status':task.status,
                                                  'step':step_by_name[task_step], 'request':task.request_id,
                                                  'name':task.name, 'tag': task.name.split('.')[-1].split('_')[-1]}})
            events_done = 0
            running_events = 0
            pending = 0
            if task.status in ProductionTask.NOT_RUNNING:
                events_done = task.total_events
            elif task.status in ['running','paused','exhausted']:
                running_events = task_input_events - task.total_events
                events_done = task.total_events
            else:
                pending = task_input_events
            if step_by_name[task_step] in step_statistic:


                step_statistic[step_by_name[task_step]] = {'input_events':step_statistic[step_by_name[task_step]]['input_events']+task_input_events,
                                             'processed_events':step_statistic[step_by_name[task_step]]['processed_events']+task.total_events,
                                                           'events_done':step_statistic[step_by_name[task_step]]['events_done']+events_done,
                                                           'running_events':step_statistic[step_by_name[task_step]]['running_events']+running_events,
                                                           'pending_events':step_statistic[step_by_name[task_step]]['pending_events']+pending}
            else:
                step_statistic[step_by_name[task_step]] = {'input_events':task_input_events,
                                             'processed_events':task.total_events,
                                                           'events_done':events_done,
                                                           'running_events':running_events,
                                                           'pending_events':pending}
    #print {'chains':chains,'processed_tasks':processed_tasks,'step_statistic':step_statistic}
    return {'chains':chains,'processed_tasks':processed_tasks,'step_statistic':step_statistic}


def prepare_step_statistic(request_statistics):
        ordered_step_statistic = []
        for step_statistic in request_statistics['step_statistic']:
            percent_done = 0.0
            percent_finished = 0.0
            percent_runnning = 0.0
            percent_paending = 0.0
            if request_statistics['step_statistic'][step_statistic]["input_events"] == 0:
                step_status = 'Unknown'
            else:
                percent_done = float(request_statistics['step_statistic'][step_statistic]['processed_events']) / float(request_statistics['step_statistic'][step_statistic]["input_events"])
                if (percent_done>0.90):
                   step_status = 'StepDone'
                elif (percent_done>0.10):
                   step_status = 'StepProgressing'
                else:
                   step_status =  'StepNotStarted'
                percent_finished = float(request_statistics['step_statistic'][step_statistic]['events_done']) / float(request_statistics['step_statistic'][step_statistic]['input_events'])
                percent_runnning = float(request_statistics['step_statistic'][step_statistic]['running_events']) / float(request_statistics['step_statistic'][step_statistic]['input_events'])
                percent_paending = float(request_statistics['step_statistic'][step_statistic]['pending_events']) / float(request_statistics['step_statistic'][step_statistic]['input_events'])
            ordered_step_statistic.append({'statistic':request_statistics['step_statistic'][step_statistic],
                                           'step_name':step_statistic,'order':MCPattern.STEPS.index(step_statistic),
                                           'step_status':step_status,'percent':str(round(percent_done*100,2))+'%',
                                           'percent_status':str(round(percent_runnning*100,2))+'%'+'/'+str(round(percent_paending*100,2))+'%'+'/'+str(round(percent_finished*100,2))+'%'})

        ordered_step_statistic.sort(key=lambda x:x['order'])
        return ordered_step_statistic


def form_hashtag_string(reqid):
    hashtags_string = ''
    request_hashtags = HashTagToRequest.objects.filter(request=reqid)
    hashtags = []
    hashtag_path = ''
    for request_hashtag in request_hashtags:
        if request_hashtag.hashtag.hashtag not in hashtags:
            hashtags.append(request_hashtag.hashtag.hashtag)
    if hashtags:
        hashtags_string = ' '.join(['#'+x for x in hashtags])
        hashtag_path = ','.join(hashtags)
    return hashtags_string, hashtag_path


def get_parent_tasks(task):
    if task.parent_id != task.id:
        return [int(task.parent_id)]
    if 'evgen'in task.name:
        return []
    task_input = task.primary_input
    if not task_input:
        return []
    if 'tid' in task_input:
        return [int(task_input[task_input.rfind('tid')+3:task_input.rfind('_')])]
    else:
        return [] #tid_from_container(task_input)


def clone_request_hashtags(old_request_id, new_request):
    old_request_hashatgs = HashTagToRequest.objects.filter(request=old_request_id)
    for old_request_hashatg in old_request_hashatgs:
        new_request_hashatg = HashTagToRequest()
        new_request_hashatg.request = new_request
        new_request_hashatg.hashtag = old_request_hashatg.hashtag
        new_request_hashatg.save()



def request_clone_slices(reqid, owner, new_short_description, new_ref,  slices, project, do_parent = True):
    request_destination = TRequest.objects.get(reqid=reqid)
    request_destination.reqid = None
    request_destination.cstatus = 'waiting'
    request_destination.description = new_short_description
    request_destination.jira_reference = None
    request_destination.is_error = None
    request_destination.manager = owner
    request_destination.ref_link = new_ref
    request_destination.project = TProject.objects.get(project=project)
    if request_destination.info_fields:
        info_field = json.loads(request_destination.info_fields)
        info_field['long_description'] = 'Cloned from request %s'%str(reqid)
        request_destination.info_fields=json.dumps(info_field)
    request_destination.save()
    request_status = RequestStatus(request=request_destination,comment='Request cloned from %i'%int(reqid),owner=owner,
                                                       status='waiting')
    request_status.save_with_current_time()
    if do_parent:
        new_parent_child = ParentToChildRequest()
        new_parent_child.parent_request = TRequest.objects.get(reqid=reqid)
        new_parent_child.child_request = request_destination
        new_parent_child.relation_type = 'CL'
        new_parent_child.status = 'active'
        new_parent_child.save()
    _logger.debug("New request: #%i"%(int(request_destination.reqid)))
    try:
        clone_request_hashtags(reqid, request_destination)
    except:
        pass
    clone_slices(reqid,request_destination.reqid,slices,0,False)
    return request_destination.reqid


def clone_slices(reqid_source,  reqid_destination, slices, step_from, make_link, fill_slice_from=False, do_smart=False, predefined_parrent={}, step_before=99):
        ordered_slices = map(int,slices)
        ordered_slices.sort()
        #form levels from input text lines
        #create chains for each input
        request_source = TRequest.objects.get(reqid=reqid_source)
        if reqid_source == reqid_destination:
            request_destination = request_source
        else:
            request_destination = TRequest.objects.get(reqid=reqid_destination)
        #TODO: fix race condition
        new_slice_numbers = []
        new_slice_number = InputRequestList.objects.filter(request=request_destination).count()
        old_new_step = {}
        for slice_number in ordered_slices:
            current_slice = InputRequestList.objects.filter(request=request_source,slice=int(slice_number))
            new_slice = current_slice.values()[0]
            new_slice['slice'] = new_slice_number
            new_slice_numbers.append(new_slice_number)
            new_slice_number += 1
            del new_slice['id']
            del new_slice['request_id']
            new_slice['request'] = request_destination
            new_input_data = InputRequestList(**new_slice)
            new_input_data.save()
            if fill_slice_from:
                new_input_data.cloned_from = InputRequestList.objects.get(request=request_source,slice=int(slice_number))
                new_input_data.save()
            step_execs = StepExecution.objects.filter(slice=current_slice)
            ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
            if request_source.request_type == 'MC':
                STEPS = StepExecution.STEPS
            else:
                STEPS = ['']*len(StepExecution.STEPS)
            step_as_in_page = form_step_in_page(ordered_existed_steps,STEPS,parent_step)
            first_changed = not make_link
            for index,step in enumerate(step_as_in_page):
                if step:
                    if ((index >= step_from) and (index < step_before)) or (not make_link):
                        self_looped = step.id == step.step_parent.id
                        old_step_id = step.id
                        step.id = None
                        step.step_appr_time = None
                        step.step_def_time = None
                        step.step_exe_time = None
                        step.step_done_time = None
                        step.slice = new_input_data
                        step.request = request_destination
                        if do_smart:
                            problematic_tasks = get_problematic_task_list(old_step_id)
                            if problematic_tasks:
                                step.set_task_config({'previous_task_list':problematic_tasks})
                        if (step.status == 'Skipped') or (index < step_from):
                            step.status = 'NotCheckedSkipped'
                        elif step.status in ['Approved','Waiting']:
                            step.status = 'NotChecked'
                        if step.step_parent.id in predefined_parrent:
                            step.step_parent = predefined_parrent[step.step_parent.id]
                        else:
                            if first_changed and (step.step_parent.id in old_new_step):
                                    step.step_parent = old_new_step[int(step.step_parent.id)]
                        step.save_with_current_time()
                        if self_looped:
                            step.step_parent = step
                        first_changed = True
                        step.save()
                        old_new_step[old_step_id] = step
        clone_child_slices(reqid_source,old_new_step)
        return new_slice_numbers


def get_problematic_task_list(step_id):
    tasks = ProductionTask.objects.filter((Q(status__in=['failed','broken','aborted']),Q(step=step_id)))
    return_list = []
    for task in tasks:
        return_list.append(task.id)
    return return_list


def clone_child_slices(parent_request, parent_steps):
    child_requests = list(ParentToChildRequest.objects.filter(status='active',parent_request=parent_request).values_list('child_request',flat=True))
    for child_request in child_requests:
        if child_request:
            current_child_slice_set = set()
            child_steps =  list(StepExecution.objects.filter(request=child_request))
            for child_step in child_steps:
                if child_step.step_parent_id in parent_steps.keys():
                    current_child_slice_set.add(child_step.slice.slice)
            clone_slices(child_request, child_request, list(current_child_slice_set), -1, False, False, False, parent_steps)


def fill_request_priority(request_from, request_to):
    requests = TRequest.objects.filter(reqid__gte=request_from,reqid__lte=request_to,request_type='MC')
    for request in requests:
        slices_priority = list(InputRequestList.objects.filter(request=request).values('priority').distinct())
        priorities = [int(x['priority']) for x in slices_priority]
        priorities.sort()
        request.update_priority(priorities)
        request.save()

def fill_request_events(request_from, request_to):
    requests = TRequest.objects.filter(reqid__gte=request_from,reqid__lte=request_to,request_type='MC')
    for request in requests:
        slices_events = list(InputRequestList.objects.filter(request=request).values('input_events').distinct())
        total_events = sum([int(x['input_events']) for x in slices_events])
        request.set_info_field('request_events',total_events)
        request.save()