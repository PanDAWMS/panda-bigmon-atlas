from django.core.mail import send_mail
from django.forms import model_to_dict
import json
import logging
import os
from copy import deepcopy
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie
from django.core.exceptions import ObjectDoesNotExist
from django.core.urlresolvers import reverse
from django.utils import timezone
import time
from ..prodtask.helper import form_request_log
from ..prodtask.settings import APP_SETTINGS
from ..prodtask.ddm_api import find_dataset_events



import core.datatables as datatables

from .models import StepTemplate, StepExecution, InputRequestList, TRequest, MCPattern, Ttrfconfig, ProductionTask, \
    get_priority_object, ProductionDataset, RequestStatus, get_default_project_mode_dict, get_default_nEventsPerJob_dict
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
            if int(tag[1:])>9000:
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


def step_status_definition(is_skipped, is_approve=True):
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
def create_steps(slice_steps, reqid, STEPS=StepExecution.STEPS, approve_level=99):
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
            priority_obj = get_priority_object(input_list.priority)
            # Check steps which already exist in slice, and change them if needed
            try:
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            except ValueError,e:
                ordered_existed_steps, existed_foreign_step = [],None

            parent_step = None
            no_action = True
            foreign_step = 0
            if int(steps_status[-1]['foreign_id']) !=0:
                foreign_step = int(steps_status[-1]['foreign_id'])
                parent_step = StepExecution.objects.get(id=foreign_step)
            steps_status.pop()
            step_as_in_page = form_step_in_page(ordered_existed_steps,STEPS,existed_foreign_step)
            # if foreign_step !=0 :
            #     step_as_in_page = [None] + step_as_in_page
            first_not_approved_index = 0
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
                            approve_existed_step(step_in_db,step_status_definition(step_value['is_skipped'], index<=approve_level))
                            if step_in_db.status not in SKIPPED_STATUS:
                                total_events = -1
                                still_skipped = False
                            parent_step = step_in_db
                        else:

                            if step_in_db.task_config:
                                task_config = json.loads(step_in_db.task_config)
                            else:
                                task_config = {}
                            for x in ['input_format','nEventsPerJob','token','merging_tag',
                                      'nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob','project_mode',
                                      'nFilesPerJob','nGBPerJob','maxAttempt']:
                                if x in step_value['changes']:
                                    task_config[x] = step_value['changes'][x]
                            for x in ['nEventsPerInputFile']:
                                if x in step_value['changes']:
                                    if step_value['changes'][x]:
                                        task_config[x] = int(step_value['changes'][x])
                                    else:
                                        task_config[x] = ''
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
                            step_in_db.status = step_status_definition(step_value['is_skipped'], index<=approve_level)
                            if 'input_events' in step_value['changes']:
                                step_in_db.input_events = step_value['changes']['input_events']
                                total_events = step_in_db.input_events
                            else:
                                if step_in_db.input_events==-1:
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
                            step_in_db.step_def_time = None
                            step_in_db.save_with_current_time()
                            parent_step = step_in_db
                    else:
                            task_config = {'maxAttempt':15}
                            if not input_list.project_mode:
                                task_config.update({'project_mode':get_default_project_mode_dict().get(STEPS[index],'')})
                                task_config.update({'nEventsPerJob':get_default_nEventsPerJob_dict().get(STEPS[index],'')})
                                events_per_input_file(index,STEPS,task_config,parent_step)
                            else:
                                task_config.update({'project_mode':input_list.project_mode})
                            for x in ['input_format','nEventsPerJob','token','merging_tag',
                                      'nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob','project_mode','nFilesPerJob',
                                      'nGBPerJob','maxAttempt']:
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
                            if parent_step:
                                st_exec.step_parent = parent_step
                                no_parent = False
                            st_exec.status = step_status_definition(step_value['is_skipped'], index<=approve_level)
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
        return {}
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
                'Simul': {'format':'HITS','prefix':'simul.'},
                'Merge': {'format':'HITS','prefix':'merge.'},
                'Reco': {'format':'AOD','prefix':'recon.'},
                'Rec Merge': {'format':'AOD','prefix':'merge.'}
            }
            if last_step_name in default_input_type_prefix:
                if input_step_format:
                    input_type = default_input_type_prefix[last_step_name]['prefix'] + input_step_format
                else:
                    input_type = default_input_type_prefix[last_step_name]['prefix'] + default_input_type_prefix[last_step_name]['format']
            dsid = input_list.input_data.split('.')[1]
            job_option_pattern = input_list.input_data.split('.')[2]
            dataset_events = find_skipped_dataset(dsid,job_option_pattern,processed_tags,input_type)
            #print dataset_events
            #return {slice:[x for x in dataset_events if x['events']>=input_list.input_events ]}
            return {slice:dataset_events}
        except Exception,e:
            logging.error("Can't find skipped dataset: %s" %str(e))
            return {}
    return {}


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

MC_COORDINATORS= ['cgwenlan','jzhong','jgarcian','mcfayden','jferrand','mehlhase','schwanen','lserkin','jcosta','boeriu']

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
        if (user_name in MC_COORDINATORS) or is_superuser:
            return 'approved'
        else:
            current_uri = request.build_absolute_uri(reverse('prodtask:input_list_approve',args=(production_request.reqid,)))
            mess = '''
Request %i has been registered by %s and is waiting approval:
%s
            '''%(production_request.reqid,user_name,current_uri)
            send_mail("Request %i was registered"%production_request.reqid,mess,APP_SETTINGS['prodtask.email.from'],['atlas-phys-mcprod-team@cern.ch','mborodin@cern.ch'],
                      fail_silently=True)
            return 'registered'

    else:
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
                    input_list.dataset = None
                    input_list.save()
                    removed_input_slices.append(slice_number)
        except:
            pass
    return removed_input_slices


def fill_all_slices_from_0_slice(reqid):
    slices = InputRequestList.objects.filter(request=reqid).order_by('slice')
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
                for field in ['jobOption','datasetName','eventsNumber','comment']:
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
                    except Exception,e:
                        not_changed.append(slice)
    return []


def find_input_per_file(dataset_name):
    if 'tid' not in dataset_name:
        to_search = dataset_name.replace('/','')[dataset_name.find(':')+1:]+'_tid%'
    else:
        to_search = dataset_name.replace('/','')[dataset_name.find(':')+1:]
    try:
        dataset = ProductionDataset.objects.extra(where=['name like %s'], params=[to_search]).first()
        if dataset:
            current_task = ProductionTask.objects.get(id=dataset.task_id)
            return json.loads(current_task.step.task_config).get('nEventsPerJob','')
    except Exception,e:
        return ''




def change_dataset_in_slice(req, slice, new_dataset_name):
    input_list = InputRequestList.objects.get(request=req, slice=int(slice))
    events_per_file = find_input_per_file(new_dataset_name)
    dataset = fill_dataset(new_dataset_name)
    input_list.dataset = dataset
    input_list.save()
    if events_per_file:
        temp1, pattern_tags, approved_step = get_skipped_steps(req,input_list)
        if json.loads(approved_step.task_config).get('nEventsPerInputFile','') != events_per_file:
            approved_step.set_task_config({'nEventsPerInputFile':events_per_file})
            approved_step.save()



def request_steps_approve_or_save(request, reqid, approve_level):
    results = {'success':False}
    try:
        data = request.body
        slice_steps = json.loads(data)
        _logger.debug(form_request_log(reqid,request,"Steps modification for: %s" % slice_steps))
        slices = slice_steps.keys()
        fail_slice_save = save_slice_changes(reqid, slice_steps)
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
        results = {'missing_tags': missing_tags,'slices': slices,'wrong_slices':wrong_skipping_slices,
                   'double_trf':old_double_trf, 'success': True, 'new_status':'', 'fail_slice_save': fail_slice_save}
        if not missing_tags:

            _logger.debug("Start steps save/approval")
            req = TRequest.objects.get(reqid=reqid)
            if req.request_type == 'MC':
                for steps_status in slice_steps.values():
                    for index,steps in enumerate(steps_status[:-2]):
                        if (StepExecution.STEPS[index] == 'Reco') or (StepExecution.STEPS[index] == 'Atlfast'):
                                if not steps['formats']:
                                    steps['formats'] = 'AOD'
            removed_input = []
            if ['-1'] == slice_steps.keys():
                slice_0 = deepcopy(slice_steps['-1'])
                error_slices, no_action_slices = create_steps({0:slice_steps['-1']},reqid,StepExecution.STEPS, approve_level)
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
                    if req.request_type == 'MC':
                        error_slices, no_action_slices = create_steps(slice_steps,reqid,StepExecution.STEPS, approve_level)
                        good_slices = [int(x) for x in slices if int(x) not in error_slices]
                        removed_input = remove_input(good_slices,reqid)
                    else:
                        error_slices, no_action_slices = create_steps(slice_steps,reqid,['']*len(StepExecution.STEPS), approve_level)

            if (req.cstatus.lower() != 'test') and (approve_level>=0):
                req.cstatus = request_approve_status(req,request)
                req.save()
                owner='default'
                try:
                    owner = request.user.username
                except:
                    pass
                if not owner:
                    owner='default'
                request_status = RequestStatus(request=req,comment='Request approved by WebUI',owner=owner,
                                               status=req.cstatus)
                request_status.save_with_current_time()
            if req.request_type == 'MC':

                for slice, new_dataset in slice_new_input.items():
                    if new_dataset:
                        change_dataset_in_slice(req, int(slice), new_dataset)

            results = {'missing_tags': missing_tags,
                       'slices': [x for x in map(int,slices) if x not in (error_slices + no_action_slices)],
                       'wrong_slices':wrong_skipping_slices,
                       'double_trf':old_double_trf, 'error_slices':error_slices,
                       'no_action_slices' :no_action_slices,'success': True, 'new_status': req.cstatus,
                       'removed_input':removed_input, 'fail_slice_save':''}
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
        return_list += find_dataset_events(dataset_pattern)
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
def request_steps_approve(request, reqid, approve_level):
    if request.method == 'POST':
        return request_steps_approve_or_save(request, reqid, int(approve_level)-1)
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
            print e
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

def request_table_view(request, rid=None, show_hidden=False):
    # Prepare data for step manipulation page

    def get_approve_status(ste_task_list,slice=None):
        if slice:
            if slice.is_hide:
                return 'hidden'
        return_status = 'not_approved'
        exist_approved = False
        exist_not_approved = False
        for step_task in ste_task_list:
            if step_task['step']:
                if (step_task['step']['status'] == 'Approved')or(step_task['step']['status'] == 'Skipped'):
                    exist_approved = True
                else:
                    exist_not_approved = True
        if exist_approved and exist_not_approved:
            return_status = 'partially_approved'
        if exist_approved and not(exist_not_approved):
            return_status = 'approved'
        return return_status

    def approve_level(step_task_list):
        max_level = -1
        for index,step_task in enumerate(step_task_list):
            if step_task['step']:
                if (step_task['step']['status'] == 'Approved')or(step_task['step']['status'] == 'Skipped'):
                    max_level=index
        return max_level+1


    def form_step_obj(step,tasks,input_slice,foreign=False):
        skipped = 'skipped'
        tag = ''
        slice = ''
        if step:
            if foreign:
                skipped = 'foreign'
                slice=str(input_slice)
            elif (step['status'] =='Skipped')or(step['status']=='NotCheckedSkipped'):
                skipped = 'skipped'
            else:
                skipped = 'run'
            tag = step['ctag']
        task_short = ''
        if tasks:
            for task in tasks:
                task['short'] = task['status'][0:8]
        return {'step':step, 'tag':tag, 'skipped':skipped, 'tasks':tasks, 'slice':slice}

    def unwrap(pattern_dict):
        return_list = []
        if type(pattern_dict) == dict:
            for key in pattern_dict:
                if key != 'ctag':
                    return_list.append((key,pattern_dict[key]))
            return pattern_dict.get('ctag',''), return_list
        else:
            return pattern_dict,[('ctag',pattern_dict)]

    if request.method == 'GET':
        try:
            cur_request = TRequest.objects.get(reqid=rid)
            #steps_db =
            _logger.debug(form_request_log(rid,request,'Start prepare data fro request page'))
            long_description = cur_request.info_field('long_description')
            if cur_request.request_type != 'MC':
                STEPS_LIST = [str(x) for x in range(10)]
                pattern_list_name = [('Empty', [unwrap({'ctag':'','project_mode':'','nEventsPerJob':''}) for step in STEPS_LIST])]
            else:
                pattern_list_name = []
                STEPS_LIST = StepExecution.STEPS
                # Load patterns which are currently in use
                pattern_list = MCPattern.objects.filter(pattern_status='IN USE')
                pattern_list_name = [(x.pattern_name,
                                      [unwrap(json.loads(x.pattern_dict).get(step,{'ctag':'','project_mode':get_default_project_mode_dict()[step],'nEventsPerJob':get_default_nEventsPerJob_dict()[step]})) for step in StepExecution.STEPS]) for x in pattern_list]
                # Create an empty pattern for color only pattern
                pattern_list_name += [('Empty', [unwrap({'ctag':'','project_mode':get_default_project_mode_dict()[step],'nEventsPerJob':get_default_nEventsPerJob_dict()[step]}) for step in StepExecution.STEPS])]

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
            show_is_fast = False
            show_split = False
            if (cur_request.request_type in ['HLT','REPROCESSING']) or (cur_request.phys_group == 'VALI'):
                show_is_fast = True
            if (cur_request.request_type == 'MC') and (cur_request.phys_group!='VALI'):
                try:
                    if (not request.user.is_superuser) and (request.user.username not in MC_COORDINATORS):
                        autorized_change_request = False
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
                if (input_list_count>800) and (cur_request.request_type == 'MC'):

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
                        failed_task_list = ProductionTask.objects.filter(Q(status__in=['failed','broken','aborted']),Q(request=cur_request))
                        total_task_dict['red'] = len(failed_task_list)
                        total_task_dict['done'] = ProductionTask.objects.filter(Q(status='done'),Q(request=cur_request)).count()
                        total_task_dict['finished'] = ProductionTask.objects.filter(Q(status='finished'),Q(request=cur_request)).count()
                        total_task_dict['running'] = ProductionTask.objects.filter(Q(status='running'),Q(request=cur_request)).count()
                        total_task_dict['blue'] = total_task_dict['total'] - (total_task_dict['red'] +  total_task_dict['done'] + total_task_dict['finished'] + total_task_dict['running'])
                    #Find slices with broken slices
                    if (total_task_dict['red']>0)and(total_task_dict['red']<600):
                        map(lambda x: failed_slices.add(x.step.slice.id),failed_task_list)
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
                    for step_name in StepExecution.STEPS:
                        if step_name not in steps:
                            slice_steps_ordered.append(form_step_obj({},{},-1))
                        else:
                            step_dict = model_to_dict(steps[step_name])
                            step_dict.update({'ctag':steps[step_name].step_template.ctag})
                            step_dict.update({'slice':input_lists_pre_pattern})
                            slice_steps_ordered.append(form_step_obj(step_dict,{},-1))
                    approved = (total_steps_count >= input_list_count) and ((approve_level(slice_steps_ordered)*input_list_count)==(approved_steps_count))

                    slice_dict = model_to_dict(input_lists_pre_pattern)
                    slice_dict['dataset'] = ''
                    if approved:
                        input_lists.append((slice_dict, slice_steps_ordered, get_approve_status(slice_steps_ordered,input_lists_pre_pattern),
                                            False,'',approve_level(slice_steps_ordered),'no'))
                    else:
                        input_lists.append((slice_dict, slice_steps_ordered, 'not_approved',
                                            False,'',-1,'no'))
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
                        if slice.input_events >= NUMBER_EVENTS_TO_SPLIT:
                            show_split = True
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

                        slice_steps = {}
                        total_slice += 1
                        show_task = False
                        # creating a pattern
                        if input_list_count < 50:
                            if use_input_date_for_pattern:
                                if slice.input_data:
                                    current_slice_pattern = slice.input_data.split('.')
                                else:
                                    current_slice_pattern=''
                            else:
                                if slice.dataset:
                                    current_slice_pattern = slice.dataset.name.split('.')
                                else:
                                    current_slice_pattern=''

                            if current_slice_pattern:
                                for index,token in enumerate(current_slice_pattern):
                                    if index >= len(slice_pattern):
                                        slice_pattern.append(token)
                                    else:
                                        if token!=slice_pattern[index]:
                                            slice_pattern[index] = os.path.commonprefix([token,slice_pattern[index]])
                                            slice_pattern[index] += '*'
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
                            step.update({'ctag':ctag})
                            if cur_request.request_type == 'MC':

                                slice_steps.update({step_name:form_step_obj(step,step_task,slice.slice)})

                            else:

                                if step['id'] == step['step_parent_id']:
                                    slice_steps_list.append((step['id'],form_step_obj(step,step_task,slice.slice)))
                                else:
                                    temp_step_list.append((step,step_task))
                        if cur_request.request_type == 'MC':
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
                                            slice_steps_ordered[another_chain_index] = form_step_obj(another_chain_step_dict
                                                                                                     ,{}, another_chain_step.slice.slice,
                                                                                                  True)
                            #slice_steps_ordered = [slice_steps.get(x,form_step_obj({},{},slice.slice)) for x in StepExecution.STEPS]
                            approved = get_approve_status(slice_steps_ordered)

                            if (approved == 'approved')or(approved == 'partially_approved'):
                                    approved_count += 1
                            if (approved == 'approved'):
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
                                                    show_task,another_chain_step_dict['id'],approve_level(slice_steps_ordered),cloned))
                            else:
                                input_lists.append((slice_dict, slice_steps_ordered, get_approve_status(slice_steps_ordered,slice),
                                                    show_task,'',approve_level(slice_steps_ordered),cloned))

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
                            if (approved == 'approved')or(approved == 'partially_approved'):
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
                                                    another_chain_step['id'], approve_level(slice_steps),cloned))
                            else:
                                input_lists.append((slice_dict, slice_steps, get_approve_status(slice_steps,slice),  show_task, '',
                                                    approve_level(slice_steps),cloned))


            step_list = [{'name':x,'idname':x.replace(" ",'')} for x in STEPS_LIST]
            jira_problem_link = ''
            if cur_request.is_error:
                has_deft_problem = True
                if cur_request.jira_reference:
                    jira_problem_link = cur_request.jira_reference
            else:
                has_deft_problem = False
            _logger.debug(form_request_log(rid,request,'Finish prepare data fro request page'))
            return   render(request, 'prodtask/_reqdatatable.html', {
               'active_app' : 'prodtask',
               'parent_template' : 'prodtask/_index.html',
               'trequest': cur_request,
               'inputLists': input_lists,
               'step_list': step_list,
               'pattern_list': pattern_list_name,
               'pr_id': rid,
               'approvedCount': approved_count,
               'pattern': '.'.join(slice_pattern),
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
               'show_split':show_split
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

