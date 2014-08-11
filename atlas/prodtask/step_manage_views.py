import json
import logging

from django.http import HttpResponse, HttpResponseRedirect

from django.views.decorators.csrf import csrf_protect
from .views import form_existed_step_list

from .models import StepExecution, InputRequestList, TRequest, Ttrfconfig

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
def clone_slices_in_req(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict
            #form levels from input text lines
            #create chains for each input
            new_slice_number = InputRequestList.objects.filter(request=reqid).count()
            for slice_number in slices:
                current_slice = InputRequestList.objects.filter(request=reqid,slice=int(slice_number))
                new_slice = current_slice.values()[0]
                new_slice['slice'] = new_slice_number
                new_slice_number += 1
                del new_slice['id']
                new_input_data = InputRequestList(**new_slice)
                new_input_data.save()
                step_execs = StepExecution.objects.filter(slice=current_slice)
                ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
                for step in ordered_existed_steps:
                    step.id = None
                    step.slice = new_input_data
                    if step.status == 'Skipped':
                        step.status = 'NotCheckedSkipped'
                    elif step.status == 'Approved':
                        step.status = 'NotChecked'
                    step.save_with_current_time()
                    if parent_step:
                        step.step_parent = parent_step
                    else:
                        step.step_parent = step
                    step.save()
                    parent_step = step
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

@csrf_protect
def slice_steps(request, reqid, slice_number):
    if request.method == 'GET':
        results = {'success':False}
        try:
            req = TRequest.objects.get(reqid=reqid)
            input_list = InputRequestList.objects.get(request=req,slice=slice_number)
            existed_steps = StepExecution.objects.filter(request=req, slice=input_list)
            # Check steps which already exist in slice, and change them if needed
            ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            result_list = []
            if existed_foreign_step:
                result_list.append({'step':existed_foreign_step.step_template.ctag,'step_name':existed_foreign_step.step_template.step,'step_type':'foreign'})
            for step in ordered_existed_steps:
                is_skipped = 'not_skipped'
                if step.status == 'NotCheckedSkipped' or step.status == 'Skipped':
                    is_skipped = 'is_skipped'
                result_list.append({'step':step.step_template.ctag,'step_name':step.step_template.step,'step_type':is_skipped})
            results = {'success':True,'step_types':result_list}
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