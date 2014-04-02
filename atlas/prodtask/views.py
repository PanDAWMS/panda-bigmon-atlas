import json
import os
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.views.decorators.csrf import csrf_protect
from django.core.exceptions import ObjectDoesNotExist

import core.datatables as datatables

from .models import StepTemplate, StepExecution, InputRequestList, TRequest, MCPattern, Ttrfconfig, ProductionTask
from prodtask.spdstodb import fill_template


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
            return HttpResponseRedirect('/prodtask/step_execution_table/')
    return HttpResponseRedirect('/prodtask/step_execution_table/')


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

#TODO: FIX it. Make one commit
def create_steps(slice_steps,reqid,is_approve=True):
    try:
        cur_request = TRequest.objects.get(reqid=reqid)
        temp_priority = 1
        proceedded_steps = []
        temp_input_events = 50000
        for slice, steps_status in slice_steps.items():
            input_list = InputRequestList.objects.filter(request=cur_request, slice=int(slice))[0]
            existed_steps = StepExecution.objects.filter(request=cur_request, slice=input_list)
            for existed_step in existed_steps:
                step_index = StepExecution.STEPS.index(existed_step.step_template.step)
                if (existed_step.status == 'NotChecked') or (existed_step.status == 'NotCheckedSkipped'):
                    if existed_step.step_template.ctag == steps_status[step_index]['value']:
                            existed_step.status = step_status_definition(steps_status[step_index]['is_skipped'],is_approve)
                            existed_step.save()
                            proceedded_steps.append(step_index)
                    else:
                        if steps_status[step_index]['value'] == '':
                            existed_step.delete()
                        else:
                            temp_priority = existed_step.priority
                            temp_input_events = existed_step.input_events
                            st = fill_template(StepExecution.STEPS[step_index],steps_status[step_index]['value'],
                                               temp_priority)

                            st_exec = StepExecution(request=cur_request,slice=input_list,step_template=st,
                                                    priority=temp_priority, input_events=temp_input_events  )
                            st_exec.status = step_status_definition(steps_status[step_index]['is_skipped'],is_approve)
                            st_exec.save_with_current_time()
                            proceedded_steps.append(step_index)
                            existed_step.delete()
            for step_index,step_status in enumerate(steps_status):
                if step_index not in proceedded_steps:
                            if step_status['value'] != '':
                                st = fill_template(StepExecution.STEPS[step_index],steps_status[step_index]['value'],
                                                   temp_priority)
                                st_exec = StepExecution(request=cur_request,slice=input_list,step_template=st,
                                                        priority=temp_priority, input_events=temp_input_events  )
                                st_exec.status = step_status_definition(steps_status[step_index]['is_skipped'],is_approve)
                                st_exec.save_with_current_time()
                                proceedded_steps.append(step_index)

    except Exception, e:
        raise e


def request_steps_approve_or_save(request, reqid, is_approve):
        results = {'success':False}
        try:
            data = request.body
            slice_steps = json.loads(data)
            tags = []
            slices = slice_steps.keys()
            for steps_status in slice_steps.values():
                for steps in steps_status:
                    if steps['value'] and (steps['value'] not in tags):
                        tags.append(steps['value'])
            missing_tags = find_missing_tags(tags)
            print slices
            results = {'data': missing_tags,'slices': slices, 'success': True}
            if not missing_tags:
                create_steps(slice_steps,reqid,is_approve)
        except Exception, e:
            print e

        return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def request_steps_save(request, reqid):
    if request.method == 'POST':
        return request_steps_approve_or_save(request,reqid,False)
    return HttpResponseRedirect('/prodtask/inputlist_with_request/%s' % reqid)

@csrf_protect
def request_steps_approve(request, reqid=None):
    if request.method == 'GET':
        try:
            print reqid
            cur_request = TRequest.objects.get(reqid=reqid)
            steps_for_approve = StepExecution.objects.all().filter(request=cur_request)
            for st in steps_for_approve:
                st.status = 'Approved'
                st.save()
        except Exception, e:
            print e
            return HttpResponseRedirect('/prodtask/inputlist_with_request/%s' % reqid)
    if request.method == 'POST':
        return request_steps_approve_or_save(request,reqid,True)
    return HttpResponseRedirect('/prodtask/inputlist_with_request/%s' % reqid)



def home(request):
    tmpl = get_template('prodtask/_index.html')
    c = Context({'active_app' : 'prodtask', 'title'  : 'Monte Carlo Production Home'})
    return HttpResponse(tmpl.render(c))

def about(request):
    tmpl = get_template('prodtask/_about.html')
    c = Context({'active_app' : 'prodtask', 'title'  : 'Monte Carlo Production about', })
    return HttpResponse(tmpl.render(c))




def input_list_approve(request, rid=None):
    if request.method == 'GET':
        try:
                pattern_list = MCPattern.objects.filter(pattern_status='IN USE')
                pd = {}
                pattern_list_name = [(x.pattern_name,[json.loads(x.pattern_dict).get(step,'') for step in StepExecution.STEPS]) for x in pattern_list]
                pattern_list_name += [('Empty', ['' for step in StepExecution.STEPS])]
                # for step in StepExecution.STEPS:
                #     id_value = []
                #     for pattern in pattern_list:
                #             id_value += [{'idname':step.replace(" ",'')+pattern.pattern_name,
                #                           'value':json.loads(pattern.pattern_dict).get(step,'')}]
                #     pd.update({step:id_value})
                cur_request = TRequest.objects.get(reqid=rid)
                input_lists_pre = InputRequestList.objects.filter(request=cur_request)
                #input_lists = [x.update({'dataset_name':x.dataset.name}) for x in input_lists_pre]
                step_exec_dict = {}
                input_lists = []
                approved_count = 0
                total_slice = 0
                slice_pattern = []
                edit_mode = False
                if not input_lists_pre:
                    input_lists_pre = []
                    edit_mode = True
                else:
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
                        approved = 'Approved'
                        show_task = False

                        if use_input_date_for_pattern:
                            current_slice_pattern = slice.input_data.split('.')
                        else:
                            current_slice_pattern = slice.dataset.name.split('.')
                        for index,token in enumerate(current_slice_pattern):
                            if index >= len(slice_pattern):
                                slice_pattern.append(token)
                            else:
                                if token!=slice_pattern[index]:
                                    slice_pattern[index] = os.path.commonprefix([token,slice_pattern[index]])
                                    slice_pattern[index] += '*'

                        for step in step_execs:
                            skipped = (step.status=='Skipped')or(step.status=='NotCheckedSkipped')
                            try:
                                step_task = ProductionTask.objects.filter(step = step).order_by('-submit_time')[0]
                            except Exception,e:
                                step_task = {}
                            if step_task:
                                show_task = True
                                slice_steps.update({step.step_template.step:{'tag':step.step_template.ctag,'skipped':skipped,'task':step_task, 'task_short':step_task.status[0:8]}})
                            else:
                                slice_steps.update({step.step_template.step:{'tag':step.step_template.ctag,'skipped':skipped,'task':{}, 'task_short':''}})
                            if (step.status!='Approved')and(step.status!='Skipped'):
                                approved = 'Not approved'
                        if approved == 'Approved':
                            approved_count += 1
                        input_lists.append((slice,[slice_steps.get(x,{'tag':'','skipped':True,'task':{},'taskshort':''}) for x in StepExecution.STEPS],approved,show_task))
                        if not show_task:
                            edit_mode = True
                step_list = [{'name':x,'idname':x.replace(" ",'')} for x in  StepExecution.STEPS]
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
                   'edit_mode':edit_mode
                   })
        except Exception, e:
            print e
            #TODO: Error message
            return HttpResponseRedirect('/prodtask/request_table/')
    return HttpResponseRedirect('/prodtask/request_table/')


def step_template_details(request, rid=None):
    if rid:
        try:
            step_template = StepTemplate.objects.get(id=rid)
        except:
            return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/')

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
        fnRowCallback =  """
                        function( nRow, aData, iDisplayIndex, iDisplayIndexFull )
                        {
                            $('td:eq(0)', nRow).html('<a href="/prodtask/step_template/'+aData[0]+'/">'+aData[0]+'</a>&nbsp;&nbsp;'
                            );
                        }"""
        sScrollX = '100em'
        sScrollY = '20em'
        bScrollCollapse = True

        aaSorting = [[0, "desc"]]
        aLengthMenu = [[10, 50, 1000], [10, 50, 1000]]
        iDisplayLength = 10

        bServerSide = True
        sAjaxSource = '/prodtask/step_template_table/'

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
            return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/')

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
        fnRowCallback = """
                        function( nRow, aData, iDisplayIndex, iDisplayIndexFull )
                        {
                            $('td:eq(0)', nRow).html('<span style="float:right;"><a title="Approve this step" href="/prodtask/step_approve/'+aData[0]+'/'+aData[2]+'/'+aData[1]+'">approve</a>'+
                                '&nbsp;</span>&nbsp;');
                        }"""
        sScrollX = '100em'
        sScrollY = '20em'
        bScrollCollapse = True

        aaSorting = [[0, "desc"]]
        aLengthMenu = [[10, 50, 1000], [10, 50, 1000]]
        iDisplayLength = 10

        bServerSide = True
        sAjaxSource = '/prodtask/step_execution_table/'

@datatables.datatable(StepExecutionTable, name='fct')
def step_execution_table(request):
    qs = request.fct.get_queryset()
    request.fct.update_queryset(qs)
    return TemplateResponse(request, 'prodtask/_datatable.html', {  'title': 'StepExecutions Table', 'active_app' : 'prodtask', 'table': request.fct,
                                                                'parent_template': 'prodtask/_index.html'})
