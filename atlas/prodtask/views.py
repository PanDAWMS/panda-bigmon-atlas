import json
import logging
import os
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.views.decorators.csrf import csrf_protect
from django.core.exceptions import ObjectDoesNotExist
from django.core.urlresolvers import reverse
from ..getdatasets.models import ProductionDatasetsExec

import core.datatables as datatables

from .models import StepTemplate, StepExecution, InputRequestList, TRequest, MCPattern, Ttrfconfig, ProductionTask, \
    get_priority_object, ProductionDataset, RequestStatus, get_default_project_mode_dict, get_default_nEventsPerJob_dict
from .spdstodb import fill_template

from django.db.models import Count, Q

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
            trtf = Ttrfconfig.objects.all().filter(tag=tag.strip()[0], cid=int(tag.strip()[1:]))
            if not trtf:
                return_list.append(tag)
        except ObjectDoesNotExist,e:
            return_list.append(tag)
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


def step_is_equal(step_value, existed_step):
    if step_value['formats']:
        return (existed_step.step_template.output_formats == step_value['formats']) and \
               (existed_step.step_template.ctag == step_value['value'])
    else:
        return (existed_step.step_template.ctag == step_value['value'])


def approve_existed_step(step, new_status):
    if not (step.status == 'Approved') and not (step.status == 'Skipped'):
        if step.status != new_status:
            step.status = new_status
            step.save_with_current_time()
    pass






#TODO: FIX it. Make one commit
def create_steps(slice_steps, reqid, STEPS=StepExecution.STEPS, approve_level=99):
    """
    Creating/saving steps

     :param slice_steps: dict of slices this element {Slice number:[step tag,is_skipped]}
     :param reqid: request id
     :param is_approve: approve if true, save if false

    """

    try:
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
            replace_steps = False
            delete_chain_from = -1
            parent_step = None
            foreign_step = 0
            if int(steps_status[-1]['foreign_id']) !=0:
                foreign_step = int(steps_status[-1]['foreign_id'])
                parent_step = StepExecution.objects.get(id=foreign_step)
            steps_status.pop()
            if (foreign_step != 0) and (existed_foreign_step.id != foreign_step):
                delete_chain_from = 0
                replace_steps = True
            i = 0
            try:
                for index,step_value in enumerate(steps_status):
                    if step_value['value']:
                        if not replace_steps:
                            if i >= len(ordered_existed_steps):
                                replace_steps = True
                            else:
                                if step_is_equal(step_value,ordered_existed_steps[i]):
                                    approve_existed_step(ordered_existed_steps[i],step_status_definition(step_value['is_skipped'], index<=approve_level))
                                    i += 1
                                else:
                                    replace_steps = True
                                    delete_chain_from = i
                        if replace_steps:
                            #Create new step
                            _logger.debug("Create step: %s execution for request: %i slice: %i "%
                                          (steps_status[index],int(reqid),input_list.slice))
                            temp_priority = priority_obj.priority(STEPS[index], step_value['value'])
                            # store input_vents only for evgen step, otherwise
                            temp_input_events = -1
                            if (index == 0) or (steps_status[0]['value']==''):
                                temp_input_events = input_list.input_events
                            if step_value['formats']:
                                st = fill_template(STEPS[index],step_value['value'], temp_priority, step_value['formats'])
                            else:
                                st = fill_template(STEPS[index],step_value['value'], temp_priority)

                            st_exec = StepExecution(request=cur_request,slice=input_list,step_template=st,
                                                    priority=temp_priority, input_events=temp_input_events)
                            if delete_chain_from > 0 :
                                parent_step = ordered_existed_steps[delete_chain_from-1]
                            if not input_list.project_mode:
                                st_exec.set_task_config({'project_mode':get_default_project_mode_dict().get(STEPS[index],'')})
                                st_exec.set_task_config({'nEventsPerJob':get_default_nEventsPerJob_dict().get(STEPS[index],'-1')})
                                if index == 0:
                                    st_exec.set_task_config({'nEventsPerInputFile':get_default_nEventsPerJob_dict().get(STEPS[index],'-1')})
                            else:
                                st_exec.set_task_config({'project_mode':input_list.project_mode})
                            no_parent = True
                            if parent_step:
                                st_exec.step_parent = parent_step
                                no_parent = False

                            st_exec.status = step_status_definition(step_value['is_skipped'], index<=approve_level)
                            st_exec.save_with_current_time()
                            if no_parent:
                                st_exec.step_parent = st_exec
                                st_exec.save()
                            parent_step = st_exec
                            _logger.debug('Step: %i saved; tag: %s priority: %i'%(st_exec.id,
                                                                          step_value['value'],
                                                                          temp_priority))
                if delete_chain_from >=0:
                    for j in range(delete_chain_from,len(ordered_existed_steps)):
                        ordered_existed_steps[j].delete()
            except Exception,e:
                _logger.error("Problem step save/approval %s"%str(e))
                raise e

    except Exception, e:
        _logger.error("Problem step save/approval %s"%str(e))
        raise e


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
    for step in ordered_existed_steps:
        if step.status == 'NotCheckedSkipped':
            processed_tags.append(step.step_template.ctag)
            if (input_list.input_data):
                if step.step_template.step == 'Evgen':
                    input_type = 'EVNT'
                else:
                    input_type = 'AOD'
                dsid = input_list.input_data.split('.')[1]
                job_option_pattern = input_list.input_data.split('.')[2]
                #job_option_pattern="Pythia"
                dataset_events = find_skipped_dataset(dsid,job_option_pattern,processed_tags,input_type)
                print dataset_events
                return {slice:[x for x in dataset_events if x['events']>=input_list.input_events ]}
    return {}



def request_steps_approve_or_save(request, reqid, approve_level):
        results = {'success':False}
        try:
            data = request.body
            slice_steps = json.loads(data)
            _logger.debug("Steps modification for: %s" % slice_steps)
            slices = slice_steps.keys()
            for steps_status in slice_steps.values():
                for steps in steps_status[:-1]:
                    steps['value'] = steps['value'].strip()
            # Check input on missing tags, wrong skipping, find input
            missing_tags,wrong_skipping_tags = step_validation(slice_steps)
            results = {'data': missing_tags,'slices': slices, 'success': True}
            datasets_dict = {}
            if not missing_tags:
                _logger.debug("Start steps save/approval")
                req = TRequest.objects.get(reqid=reqid)

                if not (req.manager) or (req.manager == 'None'):
                    missing_tags.append('No manager name!')
                    results = {'data': missing_tags,'slices': slices, 'success': True}
                else:
                    if req.request_type == 'MC':
                        create_steps(slice_steps,reqid,StepExecution.STEPS, approve_level)
                        try:
                            for slice,steps_status in slice_steps.items():
                                if steps_status[0]['value'] and steps_status[0]['is_skipped'] and (slice not in wrong_skipping_tags):
                                    datasets_dict.update(form_skipped_slice(slice,reqid))
                        except:
                            pass
                    else:
                        create_steps(slice_steps,reqid,['']*len(StepExecution.STEPS), approve_level)
                    #TODO:Take owner from sso cookies
                    if req.cstatus.lower() == 'created':
                        req.cstatus = 'approved'
                        req.save()
                        request_status = RequestStatus(request=req,comment='Request approved by WebUI',owner='default',
                                                       status='approved')
                        request_status.save_with_current_time()
                results = {'data': missing_tags,'slices': slices,'dataset_skipped':datasets_dict, 'success': True}
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
    dataset_pattern = "mc"+"%"+str(DSID)+"%"+job_option+"%"+data_type+"%"+"%".join(tags)+"%"
    print dataset_pattern
    datasets = ProductionDatasetsExec.objects.extra(where=['name like %s'], params=[dataset_pattern]).exclude(status__iexact = u'deleted')
    return_list = []
    for dataset in datasets:
        return_list.append({'dataset_name':dataset.name,'events':dataset.events})
    print datasets
    return return_list

def step_validation(slice_steps):
    tags = []
    # Slices with skipped
    wrong_skipping_slices = set()
    for slice, steps_status in slice_steps.items():
        is_skipped = True
        for steps in steps_status[:-1]:
            if steps['value'] and (steps['value'] not in tags):
                tags.append(steps['value'])
            if steps['value']:
                if steps['is_skipped'] == True:
                    if not is_skipped:
                        wrong_skipping_slices.add(slice)
                else:
                    is_skipped = False
    missing_tags = find_missing_tags(tags)
    return missing_tags,list(wrong_skipping_slices)



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
            cur_request = TRequest.objects.get(reqid=reqid)
            cur_request.cstatus = 'test'
            cur_request.save()
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def tag_info(request, tag_name):
    if request.method == 'GET':
        results = {'success':False}
        try:
            trtf = Ttrfconfig.objects.all().filter(tag=tag_name[0], cid=int(tag_name[1:]))
            if trtf:
                results.update({'success':True,'name':tag_name,'output':trtf[0].formats,'transformation':trtf[0].trf,
                                'input':trtf[0].input,'step':trtf[0].step})
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')


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
            req = TRequest.objects.get(reqid=reqid)
            slices = InputRequestList.objects.filter(request=req).order_by("slice")
            for slice in slices:
                if slice.slice>=int(slice_from):
                    step_execs = StepExecution.objects.filter(slice=slice)
                    for step_exec in step_execs:
                        if(tag == step_exec.step_template.ctag)and(output_format == step_exec.step_template.output_formats):
                            task_config = json.loads(step_exec.task_config)
                            if 'project_mode' in task_config:
                                project_mode = task_config['project_mode']
                            if 'nEventsPerJob' in task_config:
                                nEventsPerJob = task_config['nEventsPerJob']
                            if 'nEventsPerInputFile' in task_config:
                                nEventsPerInputFile = task_config['nEventsPerInputFile']
                            input_events = step_exec.input_events
                            priority = step_exec.priority
            results.update({'success':True,'project_mode':project_mode,'input_events':str(input_events),
                            'priority':str(priority),'nEventsPerJob':str(nEventsPerJob),
                            'nEventsPerInputFile':str(nEventsPerInputFile)})
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def update_project_mode(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        updated_slices = []
        try:
            data = request.body
            checkecd_tag_format = json.loads(data)
            tag = checkecd_tag_format['tag_format'].split(':')[0]
            output_format, slice_from = checkecd_tag_format['tag_format'].split('-')
            output_format = output_format[len(tag)+1:]
            slice_from = 0
            new_project_mode = checkecd_tag_format['project_mode']
            new_input_events = int(checkecd_tag_format['input_events'])
            new_priority = int(checkecd_tag_format['priority'])
            new_nEventsPerInputFile = None
            if checkecd_tag_format['nEventsPerInputFile']:
                new_nEventsPerInputFile = int(checkecd_tag_format['nEventsPerInputFile'])
            new_nEventsPerJob = None
            if checkecd_tag_format['nEventsPerJob']:
                new_nEventsPerJob = int(checkecd_tag_format['nEventsPerJob'])
            req = TRequest.objects.get(reqid=reqid)
            slices = InputRequestList.objects.filter(request=req).order_by("slice")
            for slice in slices:
                if slice.slice>=int(slice_from):
                    step_execs = StepExecution.objects.filter(slice=slice)
                    for step_exec in step_execs:
                        if(tag == step_exec.step_template.ctag)and(output_format == step_exec.step_template.output_formats):
                            if step_exec.status != 'Approved':
                                task_config = json.loads(step_exec.task_config)
                                task_config['project_mode'] = new_project_mode
                                step_exec.task_config = ''
                                step_exec.set_task_config(task_config)
                                if new_nEventsPerInputFile:
                                    step_exec.set_task_config({'nEventsPerInputFile':new_nEventsPerInputFile})
                                if new_nEventsPerJob:
                                    step_exec.set_task_config({'nEventsPerJob':new_nEventsPerJob})
                                step_exec.input_events = new_input_events
                                step_exec.priority = new_priority
                                step_exec.save()
                                if slice.slice not in updated_slices:
                                   updated_slices.append(str(slice.slice))
            results.update({'success':True,'slices':updated_slices})
        except Exception,e:
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
                step_execs = StepExecution.objects.filter(slice=slice)
                for step_exec in step_execs:
                    tag_format = step_exec.step_template.ctag + ":" + step_exec.step_template.output_formats
                    task_config = json.loads(step_exec.task_config)
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

#TODO: Optimize by having only one query for steps and tasks
def input_list_approve(request, rid=None):

    # Prepare data for step manipulation page

    def get_approve_status(ste_task_list):
        return_status = 'not_approved'
        exist_approved = False
        exist_not_approved = False
        for step_task in ste_task_list:
            if step_task['step']:
                if (step_task['step'].status == 'Approved')or(step_task['step'].status == 'Skipped'):
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
                if (step_task['step'].status == 'Approved')or(step_task['step'].status == 'Skipped'):
                    max_level=index
        return max_level+1


    def form_step_obj(step,task,foreign=False):
        skipped = 'skipped'
        tag = ''
        slice = ''
        if step:
            if foreign:
                skipped = 'foreign'
                slice=str(step.slice.slice)
            elif (step.status=='Skipped')or(step.status=='NotCheckedSkipped'):
                skipped = 'skipped'
            else:
                skipped = 'run'
            tag = step.step_template.ctag
        task_short = ''
        if task:
            task_short = task.status[0:8]
        return {'step':step, 'tag':tag, 'skipped':skipped, 'task':task, 'task_short':task_short,'slice':slice}
    if request.method == 'GET':
        try:
            cur_request = TRequest.objects.get(reqid=rid)
            if cur_request.request_type != 'MC':
                STEPS_LIST = [str(x) for x in range(10)]
                pattern_list_name = [('Empty', ['' for step in STEPS_LIST])]
            else:
                STEPS_LIST = StepExecution.STEPS
                # Load patterns which are currently in use
                pattern_list = MCPattern.objects.filter(pattern_status='IN USE')
                pattern_list_name = [(x.pattern_name,
                                      [json.loads(x.pattern_dict).get(step,'') for step in StepExecution.STEPS]) for x in pattern_list]
                # Create an empty pattern for color only pattern
                pattern_list_name += [('Empty', ['' for step in StepExecution.STEPS])]

            show_reprocessing = (cur_request.request_type == 'REPROCESSING') or (cur_request.request_type == 'HLT')
            input_lists_pre = InputRequestList.objects.filter(request=cur_request).order_by('slice')
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
            edit_mode = False
            fully_approved = 0
            if not input_lists_pre:
                edit_mode = True
            else:
                # choose how to form input data pattern: from jobOption or from input dataset
                use_input_date_for_pattern = True
                if not input_lists_pre[0].input_data:
                    use_input_date_for_pattern = False
                if use_input_date_for_pattern:
                    slice_pattern = input_lists_pre[0].input_data.split('.')
                else:
                    slice_pattern = input_lists_pre[0].dataset.name.split('.')
                for slice in input_lists_pre:
                    step_execs = StepExecution.objects.filter(slice=slice)

                    slice_steps = {}
                    total_slice += 1
                    show_task = False
                    # creating a pattern
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
                        step_task = {}
                        try:
                            step_task = ProductionTask.objects.filter(step = step).order_by('-submit_time')[0]

                        except Exception,e:
                            step_task = {}

                        if step_task:
                            show_task = True

                        if cur_request.request_type == 'MC':

                            slice_steps.update({step.step_template.step:form_step_obj(step,step_task)})

                        else:

                            if step.id == step.step_parent.id:
                                slice_steps_list.append((step.id,form_step_obj(step,step_task)))
                            else:
                                temp_step_list.append((step,step_task))
                    if cur_request.request_type == 'MC':
                        slice_steps_ordered = [slice_steps.get(x,form_step_obj({},{})) for x in StepExecution.STEPS]
                        approved = get_approve_status(slice_steps_ordered)
                        if (approved == 'approved')or(approved == 'partially_approved'):
                                approved_count += 1
                        if (approved == 'approved'):
                            fully_approved +=1
                        input_lists.append((slice, slice_steps_ordered, get_approve_status(slice_steps_ordered),
                                            show_task,'',approve_level(slice_steps_ordered)))

                        if (not show_task)or(fully_approved<total_slice):
                            edit_mode = True
                    else:
                        i = 0
                        if not(slice_steps_list) and (len(temp_step_list) == 1):
                            if current_step[0].step_parent:
                                if current_step[0].step_parent.id != current_step[0].id:
                                    # step in other chain
                                    another_chain_step = StepExecution.objects.get(id=current_step[0].step_parent.id)
                                    slice_steps_list.append((another_chain_step.id, form_step_obj(another_chain_step,{},True)))
                            slice_steps_list.append((temp_step_list[j][0].id,form_step_obj(temp_step_list[j][0],temp_step_list[j][1])))
                            temp_step_list.pop(0)
                        if not slice_steps_list:
                            step_id_list = [x[0].id for x in temp_step_list]
                            # find a root of chain
                            for index,current_step in enumerate(temp_step_list):
                                if current_step[0].step_parent.id not in step_id_list:
                                    # step in other chain
                                    another_chain_step = StepExecution.objects.get(id=current_step[0].step_parent.id)
                                    slice_steps_list.append((another_chain_step.id, form_step_obj(another_chain_step,{},True)))
                                    slice_steps_list.append((current_step[0].id,form_step_obj(current_step[0],current_step[1])))
                                    temp_step_list.pop(index)

                        for i in range(len(temp_step_list)):
                            j = 0
                            while (temp_step_list[j][0].step_parent.id!=slice_steps_list[-1][0]):
                                j+=1
                                if j >= len(temp_step_list):
                                    raise ValueError('Not linked chain')
                                    #break
                            slice_steps_list.append((temp_step_list[j][0].id,form_step_obj(temp_step_list[j][0],temp_step_list[j][1])))

                        edit_mode = True
                        slice_steps = [x[1] for x in slice_steps_list] + [form_step_obj({},{})]*(len(STEPS_LIST)-len(slice_steps_list))
                        if another_chain_step:
                            input_lists.append((slice, slice_steps, get_approve_status(slice_steps),  show_task,
                                                another_chain_step.id, approve_level(slice_steps)))
                        else:
                            input_lists.append((slice, slice_steps, get_approve_status(slice_steps),  show_task, '',
                                                approve_level(slice_steps)))


            step_list = [{'name':x,'idname':x.replace(" ",'')} for x in STEPS_LIST]
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
               'show_reprocessing':show_reprocessing
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
        sClass='px100',
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
        sClass='px100',
        )
        
    timestamp = datatables.Column(
        label='Timestamp',
        sClass='px140',
        )


    class Meta:
        model = ProductionDataset
        id = 'dataset_table'
        var = 'datasetTable'
        bSort = True
        bPaginate = True
        bJQueryUI = True

        sScrollX = '100%'
      #  sScrollY = '25em'
        bScrollCollapse = True

        fnServerParams = "datasetServerParams"

        fnServerData =  "datasetServerData"

        aaSorting = [[1, "desc"]]
        aLengthMenu = [[100, 1000, -1], [100, 1000, "All"]]
        iDisplayLength = 100

        bServerSide = True

        def __init__(self):
            sAjaxSource = reverse('production_dataset_table')

    def apply_filters(self, request):
        qs = self.get_queryset()

        qs = qs.filter( status__in=['aborted','broken','failed','deleted',
                'toBeDeleted','toBeErased','waitErased','toBeCleaned','waitCleaned'] )

        parameters = [ ('datasetname','name'), ('status','status'), ]

        for param in parameters:
            value = request.GET.get(param[0], 0)
            if value and value != '':
                if param[0] == 'datasetname':
                    qs = qs.filter(Q( **{ param[1]+'__iregex' : value } ))
                else:
                    qs = qs.filter(Q( **{ param[1]+'__exact' : value } ))

        self.update_queryset(qs)


@datatables.datatable(ProductionDatasetTable, name='fct')
def production_dataset_table(request):
#    qs = request.fct.get_queryset()
    request.fct.apply_filters(request)
#    request.fct.update_queryset(qs)

    return TemplateResponse(request, 'prodtask/_dataset_table.html', {  'title': 'Aborted and Obsolete Production Dataset Status Table', 'active_app' : 'prodtask', 'table': request.fct,
                                                                'parent_template': 'prodtask/_index.html'})
