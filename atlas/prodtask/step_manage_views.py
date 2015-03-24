import json
import logging

from django.http import HttpResponse, HttpResponseRedirect

from django.views.decorators.csrf import csrf_protect
from time import sleep
from copy import deepcopy
from atlas.prodtask.models import RequestStatus
from ..prodtask.spdstodb import fill_template
from ..prodtask.request_views import clone_slices
from ..prodtask.helper import form_request_log
from ..prodtask.task_actions import do_action
from .views import form_existed_step_list, form_step_in_page, fill_dataset

from .models import StepExecution, InputRequestList, TRequest, Ttrfconfig, ProductionTask, ProductionDataset

_logger = logging.getLogger('prodtaskwebui')


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
def clone_slices_in_req(request, reqid, step_from, make_link_value):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = map(int,slices)
            _logger.debug(form_request_log(reqid,request,'Clone slices: %s' % str(ordered_slices)))
            ordered_slices.sort()
            if make_link_value == '1':
                make_link = True
            else:
                make_link = False
            step_from = int(step_from)
            clone_slices(reqid,reqid,ordered_slices,step_from,make_link,True)
            results = {'success':True}
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

def reject_steps_in_slice(current_slice):
    step_execs = StepExecution.objects.filter(slice=current_slice)
    ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
    for step in ordered_existed_steps:
        if ProductionTask.objects.filter(step=step).count() == 0:
            step.step_appr_time = None
            if step.status == 'Skipped':
                step.status = 'NotCheckedSkipped'
            elif step.status == 'Approved':
                step.status = 'NotChecked'
            step.save()

@csrf_protect
def reject_slices_in_req(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Reject slices: %s' % str(slices)))
            for slice_number in slices:
                current_slice = InputRequestList.objects.filter(request=reqid,slice=int(slice_number))
                reject_steps_in_slice(current_slice)
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def hide_slices_in_req(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            if '-1' in slices:
                del slices[slices.index('-1')]
            _logger.debug(form_request_log(reqid,request,'Hide slices: %s' % str(slices)))
            for slice_number in slices:
                current_slice = InputRequestList.objects.get(request=reqid,slice=int(slice_number))
                if not current_slice.is_hide:
                    current_slice.is_hide = True
                    reject_steps_in_slice(current_slice)
                else:
                    current_slice.is_hide = False
                current_slice.save()
            results = {'success':True}
        except Exception,e:
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
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')

def find_double_task(request_from,request_to,showObsolete=True,checkMode=True,obsoleteOnly=True):
    total = 0
    total_steps = 0
    total_tasks = 0
    nothing = 0
    alreadyObsolete = 0
    obsolets = []
    aborts = []
    status_list = ['obsolete','aborted','broken','failed','submitting','submitted','assigning','registered','ready','running','finished','done']
    for request_id in range(request_from,request_to):
        try:
            total1 = total
            total_steps1 = total_steps
            steps = StepExecution.objects.filter(request=request_id)
            tasks = list(ProductionTask.objects.filter(request=request_id))
            total_tasks +=  len(tasks)
            tasks_by_step = {}
            for task in tasks:
                tasks_by_step[task.step_id] = tasks_by_step.get(task.step_id,[])+[task]

            for current_step in steps:
                input_dict = {}
                if tasks_by_step.get(current_step.id,None):

                    for current_task in tasks_by_step[current_step.id]:
                        real_name = current_task.input_dataset[current_task.input_dataset.find(':')+1:]
                        input_dict[real_name]=input_dict.get(real_name,[])+[current_task]
                    # if len(input_dict.keys())>1:
                    #     long_step += 1
                    #     print 'check - ', current_step.id,input_dict[input_dict.keys()[0]][0].inputdataset
                    for input_dataset in input_dict.keys():
                        if len(input_dict[input_dataset])>1:
                            total_steps += 1
                            total += len(input_dict[input_dataset])
                            dataset_to_stay = 0
                            max_status_index = status_list.index(input_dict[input_dataset][0].status)
                            #print '-'
                            for index,ds in enumerate(input_dict[input_dataset][1:]):
                                if status_list.index(ds.status) > max_status_index:
                                    dataset_to_stay = index+1
                                    max_status_index = status_list.index(ds.status)
                            if input_dict[input_dataset][dataset_to_stay].status != 'done':
                                print 'To stay:', input_dict[input_dataset][dataset_to_stay].status,input_dict[input_dataset][dataset_to_stay].id,input_dataset
                            for index,ds in enumerate(input_dict[input_dataset]):

                                if ds.status == 'obsolete':
                                    if showObsolete:
                                        print ds.output_dataset
                                    alreadyObsolete += 1
                                if index != dataset_to_stay:
                                    if ds.status in ['obsolete','broken','failed','aborted']:
                                        #print 'Do nothing:',ds.status,ds.id
                                        nothing += 1
                                        pass
                                    elif ds.status in ['finished','done']:
                                        print 'Obsolete:',ds.status,ds.id
                                        obsolets.append(ds.id)
                                        print dataset_to_stay,'-',[(x.status,x.id) for x in input_dict[input_dataset]]
                                    else:
                                        print 'Abort:',ds.status,ds.id
                                        aborts.append(ds.id)

                            #print current_step.id,'-',input_dataset,'-',len(input_dict[input_dataset]),[x.status for x in input_dict[input_dataset]]
#            print request_id, '-',len(tasks), (total-total1),(total_steps-total_steps1)
            if (not checkMode):
                pass
                # for task_id in obsolets:
                #     res = do_action('mborodin',task_id,'obsolete')
                # if not obsoleteOnly:
                #     for task_id in aborts:
                #         res = do_action('mborodin',str(task_id),'abort')
                #         try:
                #             if res['status']['jedi_info']['status_code']!=0:
                #                 print res
                #         except:
                #             pass
                #         #print res
                #         #sleep(1)
        except Exception,e:
            print e
            pass
    print total_tasks,total_steps,total
    print 'obsoletes:',len(obsolets),'abort:',len(aborts),'Already obsolete:',alreadyObsolete




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
                            if 'token' in task_config:
                                destination_token = task_config['token']
                            input_events = step_exec.input_events
                            priority = step_exec.priority
            results.update({'success':True,'project_mode':project_mode,'input_events':str(input_events),
                            'priority':str(priority),'nEventsPerJob':str(nEventsPerJob),
                            'nEventsPerInputFile':str(nEventsPerInputFile),'destination':destination_token})
        except Exception,e:
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
                    step_execs = StepExecution.objects.filter(slice=slice)
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
        except Exception,e:
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
                                            'token':'','nGBPerJob':'','maxAttempt':''}
                        foreign_step_dict_index = 0
                    else:
                        foreign_step_dict = {'step':existed_foreign_step.step_template.ctag,'step_name':existed_foreign_step.step_template.step,'step_type':'foreign',
                                            'nEventsPerJob':'','nEventsPerInputFile':'','nFilesPerJob':'',
                                            'project_mode':'','input_format':'',
                                            'priority':'', 'output_formats':'','input_events':'',
                                            'token':'','nGBPerJob':'','maxAttempt':''}
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
                                        'maxAttempt':task_config.get('maxAttempt',''),
                                        'previousTasks':','.join(map(str,task_config.get('previous_task_list',[])))})

            dataset = ''
            if input_list.dataset:
                dataset = input_list.dataset.name
            jobOption = ''
            if input_list.input_data:
                jobOption = input_list.input_data
            results = {'success':True,'step_types':result_list, 'dataset': dataset, 'jobOption':jobOption,
                       'totalEvents':int(input_list.input_events),'comment':input_list.comment}
        except Exception,e:
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
        except Exception,e:
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
        step_execs = StepExecution.objects.filter(slice=current_slice)
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(step_execs)
        result_dict[slice] = []
        for index, step in enumerate(ordered_existed_steps):
            tasks_to_fix = []
            previous_tasks = step.get_task_config('previous_task_list')
            if previous_tasks:
                for task in previous_tasks:
                    remove_task_chain(result_dict,tasks_checked,task,tasks_parent)
            if tasks.has_key(step.id):
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
            current_slice = InputRequestList.objects.filter(request=production_request,slice=int(slice_number))
            step_execs = StepExecution.objects.filter(slice=current_slice)
            ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
            if len(ordered_existed_steps) == len(retry_action[slice_number]):
                new_slice = current_slice.values()[0]
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
                            step.set_task_config({'previous_task_list':map(int,retry_action[slice_number][real_step_index])})
                            if step.status == 'Skipped':
                                step.status = 'NotCheckedSkipped'
                            elif step.status == 'Approved':
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
            ordered_slices = map(int,slices)
            _logger.debug(form_request_log(reqid,request,'Retry slices: %s' % str(ordered_slices)))
            ordered_slices.sort()
            retry_action = prepare_slices_to_retry(reqid, ordered_slices)
            apply_retry_action(reqid, retry_action)
            results = {'success':True}
        except Exception,e:
            pass
    return HttpResponse(json.dumps(results), content_type='application/json')


def create_tier0_split_slice(slice_dict, steps_list):
    """
    Create slice in last tier0 request.
    :param slice_dict: Dict, possible keys ['dataset','comment','priority']
    :param steps_list: Dict, possinle keys ['ctag','output_formats','memory','priority'] + StepExecution.TASK_CONFIG_PARAMS
    :return: request number if succeed
    """
    last_request = (TRequest.objects.filter(request_type='TIER0').order_by('-reqid'))[0]
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
    new_slice.save()
    #create_steps({new_slice_number:step_list}, last_request.reqid, len(StepExecution.STEP)S*[''], 99)
    parent = None
    for step_dict in steps_list:
        new_step = StepExecution()
        new_step.request = last_request
        new_step.slice = new_slice
        new_step.input_events = -1
        if step_dict.get('ctag',''):
            ctag = step_dict.get('ctag','')
        else:
            raise ValueError('Ctag has to be defined for step')
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
        parent = new_step
    last_request.cstatus = 'approved'
    last_request.save()
    request_status = RequestStatus(request=last_request,comment='Request approved by Tier0',owner='tier0',
                                   status=last_request.cstatus)
    request_status.save_with_current_time()
    return last_request.reqid



def split_slice(reqid, slice_number, divider):
    """
    Create a new slices in reqid with number of events in each slice = total_events in slice / divider
    :param reqid: request id
    :param slice_number: slice number to split
    :param divider: divider count

    """

    def prepare_splitted_slice(slice_to_split, new_slice_number, ordered_existed_steps, index,  new_event_number):
            new_slice = slice_to_split.values()[0]
            new_slice['slice'] = new_slice_number
            del new_slice['id']
            new_slice['input_events'] = new_event_number
            comment = new_slice['comment']
            new_slice['comment'] = comment[:comment.find(')')+1] + '('+str(index)+')' + comment[comment.find(')')+1:]
            new_input_data = InputRequestList(**new_slice)
            new_input_data.save()
            parent = None
            for step_dict in ordered_existed_steps:
                current_step = deepcopy(step_dict)
                current_step.slice = new_input_data
                if parent:
                    current_step.step_parent = parent
                current_step.save()
                if current_step.input_events == slice_to_split[0].input_events:
                    current_step.input_events = new_input_data.input_events
                    current_step.save()
                if not parent:
                    current_step.step_parent = current_step
                    current_step.save()
                parent = current_step

    production_request = TRequest.objects.get(reqid=reqid)
    slice_to_split = InputRequestList.objects.filter(request = production_request, slice = slice_number)
    new_slice_number = (InputRequestList.objects.filter(request=production_request).order_by('-slice')[0]).slice + 1
    step_execs = StepExecution.objects.filter(slice=slice_to_split[0])
    ordered_existed_steps, existed_foreign_step = form_existed_step_list(step_execs)
    for step in ordered_existed_steps:
            step.id = None
            step.step_parent = step
    if (slice_to_split[0].input_events != -1) and (slice_to_split[0].input_events > divider) and \
            ((int(slice_to_split[0].input_events) / divider) < 200):
        for step_dict in ordered_existed_steps:
            if (step_dict.input_events != slice_to_split[0].input_events) and (step_dict.input_events != -1):
                raise ValueError("Can't split slice wrong event in step %s" % str(step_dict.input_events))
            if step_dict.status in StepExecution.STEPS_APPROVED_STATUS:
                raise ValueError("Can't split slice step %s is approved" % str(step_dict.status))
        for i in range(int(slice_to_split[0].input_events) / int(divider)):
            prepare_splitted_slice(slice_to_split,new_slice_number,ordered_existed_steps, i, divider)
            new_slice_number += 1
        if (slice_to_split[0].input_events % divider) != 0:
            prepare_splitted_slice(slice_to_split,new_slice_number,ordered_existed_steps,
                                   int(slice_to_split[0].input_events) / int(divider), slice_to_split[0].input_events % divider)
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
                except  Exception,e:
                    bad_slices.append(slice_number)
                    _logger.error("Problem with slice splitting : %s"%( e))
            if len(bad_slices) > 0:
                results = {'success':False,'badSlices':bad_slices,'goodSlices':good_slices}
            else:
                results = {'success':True,'badSlices':bad_slices,'goodSlices':good_slices}
        except Exception,e:
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
                    print dataset.name
                    i.dataset = dataset
                    i.save()
                    step.step_parent = step
                    step.save()
    print counter