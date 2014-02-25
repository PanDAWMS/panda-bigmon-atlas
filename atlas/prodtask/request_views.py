
from django.core.mail import send_mail
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
import core.datatables as datatables


from .forms import RequestForm, RequestUpdateForm, TRequestCreateCloneForm, TRequestCreateCloneConfirmation
from .models import TRequest, InputRequestList, StepExecution
from .settings    import APP_SETTINGS
from .spdstodb import fill_template, fill_steptemplate_from_gsprd, fill_steptemplate_from_file,  UrFromSpds

def request_details(request, rid=None):
    if rid:
        try:
            req = TRequest.objects.get(reqid=rid)
            form = RequestForm(instance=req)
        except:
            return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/')

    return render(request, 'prodtask/_form.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'TRequest details with ID = %s' % rid,
       'form': form,
       'parent_template' : 'prodtask/_index.html',
   })



def request_clone(request, rid=None):
    return request_clone_or_create(request, rid, 'Clonning of TRequest with ID = %s' % rid, 'prodtask:request_clone')

def request_update(request, rid=None):
    if request.method == 'POST':
        try:
            req = TRequest.objects.get(reqid=rid)
            form = RequestUpdateForm(request.POST, instance=req) # A form bound to the POST data
        except:
            return HttpResponseRedirect('/')
        if form.is_valid():
            # Process the data in form.cleaned_data
            req = TRequest(**form.cleaned_data)
            req.save()
            return HttpResponseRedirect('/prodtask/request/%s' % req.reqid) # Redirect after POST
    else:
        try:
            req = TRequest.objects.get(reqid=rid)
            form = RequestUpdateForm(instance=req)
        except:
            return HttpResponseRedirect('/')
    return render(request, 'prodtask/_form.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : 'Updating of TRequest with ID = %s' % rid,
       'form': form,
       'submit_url': 'prodtask:request_update',
       'url_args': rid,
       'parent_template' : 'prodtask/_index.html',
   })

def request_clone_or_create(request, rid, title, submit_url):

    if request.method == 'POST':
        form = TRequestCreateCloneForm(request.POST, request.FILES)
        if form.is_valid():
            # Process the data in form.cleaned_data
            ################### Extra fields in form. Get and remove for creating
            if form.cleaned_data.get('excellink') or form.cleaned_data.get('excelfile'):
                spreadsheet_dict = []
                #print request.FILES['excelfile']
                try:
                    if(form.cleaned_data.get('excellink')):
                        spreadsheet_dict += fill_steptemplate_from_gsprd(form.cleaned_data['excellink'])
                    if(form.cleaned_data.get('excelfile')):
                        input_excel = request.FILES['excelfile']
                        spreadsheet_dict +=  fill_steptemplate_from_file(input_excel)
                    del form.cleaned_data['excellink'], form.cleaned_data['excelfile']
                    form = TRequestCreateCloneConfirmation(form.cleaned_data)
                    inputlists = [x['input_dict'] for x in spreadsheet_dict]
                    request.session['spreadsheet_dict'] = spreadsheet_dict
                    return render(request, 'prodtask/_previewreq.html', {
                                               'active_app' : 'prodtask',
                                               'pre_form_text' : title,
                                               'form': form,
                                               'submit_url': submit_url,
                                               'url_args'  : rid,
                                               'parent_template' : 'prodtask/_index.html',
                                               'inputLists': inputlists
                                               })
                except Exception, e:
                            #print e
                            #TODO: Error message
                            pass

            elif 'spreadsheet_dict' in request.session:
                try:
                    #TODO: Waiting message
                    spreadsheet_dict = request.session['spreadsheet_dict']
                    del request.session['spreadsheet_dict']
                    longdesc = form.cleaned_data.get('long_description', '')
                    cc = form.cleaned_data.get('cc', '')
                    del  form.cleaned_data['long_description'], form.cleaned_data['cc'], form.cleaned_data['excellink'], form.cleaned_data['excelfile']
                    req = TRequest(**form.cleaned_data)
                    req.save()
                    send_mail('Request N %i was created' % req.reqid, longdesc, APP_SETTINGS['prodtask.email.from'] ,
                               APP_SETTINGS['prodtask.default.email.list']+cc.replace(';',',').split(','), fail_silently=True)
                    for current_slice in spreadsheet_dict:
                        input_data = current_slice["input_dict"]
                        input_data['request'] = req
                        irl = InputRequestList(**input_data)
                        irl.save()
                        for step in current_slice['step_exec_dict']:
                            st = fill_template(step['step_name'], step['tag'], step['step_exec']['priority'])
                            step['step_exec']['request'] = req
                            step['step_exec']['slice'] = irl
                            step['step_exec']['step_template'] = st
                            st_exec = StepExecution(**step['step_exec'])
                            st_exec.save_with_current_time()
                except Exception, e:
                            print e
                            #TODO: Error message
                            return HttpResponseRedirect('/prodtask/request_table/')
                return HttpResponseRedirect('/prodtask/inputlist_with_request/%s' % req.reqid)


            else:
                return render(request, 'prodtask/_form.html', {
                                                           'active_app' : 'prodtask',
                                                           'pre_form_text' : title,
                                                           'form': form,
                                                           'submit_url': submit_url,
                                                           'url_args'  : rid,
                                                           'parent_template' : 'prodtask/_index.html',
                                                           })
    else:

        if(rid):
            try:
                values = TRequest.objects.values().get(reqid=rid)
                #print values
                form = TRequestCreateCloneForm(values)
                del values['reqid']
            except:
                return HttpResponseRedirect('/')
        else:
            form = TRequestCreateCloneForm()

    return render(request, 'prodtask/_form.html', {
       'active_app' : 'prodtask',
       'pre_form_text' : title,
       'form': form,
       'submit_url': submit_url,
       'url_args'  : rid,
       'parent_template' : 'prodtask/_index.html',
       })

def request_create(request):
    return request_clone_or_create(request, None, 'Create TRequest', 'prodtask:request_create')



class RequestTable(datatables.DataTable):

    rid = datatables.Column(
        label='Request ID',
        model_field='reqid',
        )

    ref_link = datatables.Column(
        label='Link',
        )


    phys_group = datatables.Column(
        label='Group',
        )

    description = datatables.Column(
        label='Description',
        )

    campaign = datatables.Column(
        label='Campaign',
        )

    manager = datatables.Column(
        label='Manager',
        )

    status = datatables.Column(
        label='Approval status',
        )




    class Meta:
        model = TRequest
        bSort = True
        bPaginate = True
        bJQueryUI = True

        sScrollX = '100%'
        sScrollY = '25em'
        bScrollCollapse = True

        aaSorting = [[0, "desc"]]
        aLengthMenu = [[10, 50, 100, -1], [10, 50, 1000, "All"]]
        iDisplayLength = 10
        fnRowCallback = """
                        function( nRow, aData, iDisplayIndex, iDisplayIndexFull )
                        {
                            $('td:eq(0)', nRow).html('<a href="/prodtask/request/'+aData[0]+'/">'+aData[0]+'</a>&nbsp;&nbsp;'+
                                                     '<span style="float: right;" ><a href="/prodtask/request_update/'+aData[0]+'/">Update</a>&nbsp;'+
                                                     '<a href="/prodtask/request_clone/'+aData[0]+'/">Clone</a>&nbsp;'+
                                                     '<a href="/prodtask/inputlist_with_request/'+aData[0]+'/">List</a></span>'
                            );
                            $('td:eq(1)', nRow).html('<a href="'+aData[1]+'">'+aData[1]+'</a>');
                        }"""

        bServerSide = True
        sAjaxSource = '/prodtask/request_table/'





@datatables.datatable(RequestTable, name='fct')
def request_table(request):
    qs = request.fct.get_queryset()
    request.fct.update_queryset(qs)
    return TemplateResponse(request, 'prodtask/_datatable.html', {  'title': 'Production Requests Table', 'active_app' : 'prodtask', 'table': request.fct,
                                                                'parent_template': 'prodtask/_index.html'})

