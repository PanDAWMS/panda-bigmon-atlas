

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
import core.datatables as datatables

from .models import StepTemplate, StepExecution, InputRequestList, TRequest
from forms import StepExecutionForm


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




def home(request):
    tmpl = get_template('prodtask/_index.html')
    c = Context({'active_app' : 'prodtask', 'title'  : 'Monte Carlo Production Home'})
    return HttpResponse(tmpl.render(c))

def about(request):
    tmpl = get_template('prodtask/_about.html')
    c = Context({'active_app' : 'prodtask', 'title'  : 'Monte Carlo Production about', })
    return HttpResponse(tmpl.render(c))




def syncButton(request, rid=None):
    if request.method == 'GET':
        if rid:
            print rid
#            urFromSpds.fillAllFromSC2(int(rid)+2, '0AiEl32nvxJogdFhsbmRnNk80bm9qS29tTWhhSXdlVVE', APP_SETTINGS['task.auth']['user'], APP_SETTINGS['task.auth']['password'])
        #urFromSpds.fillAllFromSC('0AiEl32nvxJogdFhsbmRnNk80bm9qS29tTWhhSXdlVVE', APP_SETTINGS['task.auth']['user'], APP_SETTINGS['task.auth']['password'])
    return HttpResponseRedirect('/prodtask/request_table/')


def input_list_approve(request, rid=None):
    if request.method == 'GET':
        try:
                cur_request = TRequest.objects.get(reqid=rid)
                input_lists = InputRequestList.objects.filter(request=cur_request)
                if not input_lists:
                    input_lists = []
                #TODO: Change mock data on DB
                pattern_list = ['af2_mc11c','af2_mc12a']
                pd = {StepExecution.STEPS[0]:[{'idname':(StepExecution.STEPS[0]+'af2_mc11c'),'value':'e2683'},
                                              {'idname':(StepExecution.STEPS[0]+'af2_mc12a'),'value':'e2684'}],
                      StepExecution.STEPS[1]:[{'idname':(StepExecution.STEPS[1]+'af2_mc11c'),'value':'a220'},
                                              {'idname':(StepExecution.STEPS[1]+'af2_mc12a'),'value':'a221'}]}
                for i in range(2,len(StepExecution.STEPS)):
                    pd.update({StepExecution.STEPS[i]:[]})
                step_list = [{'name':x,'idname':x.replace(" ",''),'pattern':pd[x]} for x in  StepExecution.STEPS]
                return   render(request, 'prodtask/_reqdatatable.html', {
                   'active_app' : 'prodtask',
                   'parent_template' : 'prodtask/_index.html',
                   'trequest': cur_request,
                   'inputLists': input_lists,
                   'step_list': step_list,
                   'pattern_list': pattern_list
                   })
        except Exception, e:
            print e
            #TODO: Error message
            pass
    return HttpResponseRedirect('/prodtask/request_table/')

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
            req = StepExecution.objects.get(id=rid)
            form = StepExecutionForm(instance=req)
        except:
            return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/')

    return render(request, 'task/_form.html', {
       'active_app' : 'task',
       'pre_form_text' : 'StepExecution details with ID = %s' % rid,
       'form': form,
       'parent_template' : 'task/_index.html',
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
