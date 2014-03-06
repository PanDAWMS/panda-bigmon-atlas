
from django.core.mail import send_mail
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.utils import timezone
import core.datatables as datatables


from .forms import RequestForm, RequestUpdateForm, TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, TRequestCreateCloneConfirmation, TRequestDPDCreateCloneForm
from .models import TRequest, InputRequestList, StepExecution, ProductionDataset
from .settings    import APP_SETTINGS
from .spdstodb import fill_template, fill_steptemplate_from_gsprd, fill_steptemplate_from_file, UrFromSpds
from .dpdconfparser import ConfigParser
from core.xls_parser import open_tempfile_from_url

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
        if(rid):
            try:
                values = TRequest.objects.values().get(reqid=rid)
                if values['request_type'] == 'MC':
                    return request_clone_or_create(request, rid, 'Clonning of TRequest with ID = %s' % rid, 'prodtask:request_clone', TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, mcfile_form_prefill)
                elif values['request_type'] == 'GROUP':
                    return request_clone_or_create(request, rid, 'Clonning TRequest', 'prodtask:request_clone',
                                   TRequestDPDCreateCloneForm, TRequestCreateCloneConfirmation, dpd_form_prefill)
            except Exception,e  :
                return HttpResponseRedirect('/')
        else:
            return request_clone_or_create(request, rid, 'Clonning of TRequest with ID = %s' % rid, 'prodtask:request_clone', TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, mcfile_form_prefill)

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

def mcfile_form_prefill(form_data, request):
        spreadsheet_dict = []
        if(form_data.get('excellink')):
            spreadsheet_dict += fill_steptemplate_from_gsprd(form_data['excellink'])
        if(form_data.get('excelfile')):
            input_excel = request.FILES['excelfile']
            spreadsheet_dict += fill_steptemplate_from_file(input_excel)
        return spreadsheet_dict
    

def dpd_form_prefill(form_data, request):
        file_name = None
        spreadsheet_dict = []
        if(form_data.get('excellink')):
            file_name = open_tempfile_from_url(form_data['excellink'], 'txt')
        if(form_data.get('excelfile')):
            file_name = request.FILES['excelfile']
        conf_parser = ConfigParser()
        with open(file_name) as file_obj:
            output_dict = conf_parser.parse_config(file_obj)
        print output_dict
        if('group' in output_dict):
            form_data['phys_group'] = output_dict['group'][0].replace('GR_SM', 'StandartModel').replace('GR_','')
        if('comment' in output_dict):   
            form_data['description'] = output_dict['comment'][0]
        if('owner' in output_dict):   
            form_data['manager'] = output_dict['owner'][0]
        if('project' in output_dict):   
            form_data['campaign'] = output_dict['project'][0]
        
        
        for slice_index, ds in enumerate(output_dict['ds']):
            st_sexec_list = []
            sexec = {}
            irl = dict(slice=slice_index, brief=' ', comment=output_dict.get('comment', [''])[0], dataset=ds,
                                                                     input_data=output_dict.get('joboptions', [''])[0])
            if 'tag' in output_dict:

         
                sexec = dict(status='NotChecked', priority=int(output_dict.get('priority', [0])[0]),
                              input_events=int(output_dict.get('total_num_genev', [-1])[0]))
                st_sexec_list.append({'step_name' :'Reco', 'tag': output_dict['tag'][0], 'step_exec': sexec, 'memory':output_dict.get('ram', [None])[0] ,
                                      'formats': output_dict.get('formats', [None])[0]})
            spreadsheet_dict.append({'input_dict':irl, 'step_exec_dict':st_sexec_list})
        return spreadsheet_dict
    
#TODO: Change it to real dataset workflow 
def fill_dataset(ds):
        dataset = None
        try:
            dataset = ProductionDataset.objects.all().filter(name=ds)[0]
        except:
            pass
        finally:
            if dataset :
                return dataset
            else:
                dataset =  ProductionDataset.objects.create(name=ds,files=-1,timestamp=timezone.now())
                dataset.save()
                return dataset
                

def request_clone_or_create(request, rid, title, submit_url, TRequestCreateCloneForm, TRequestCreateCloneConfirmation, form_prefill):
    
    if request.method == 'POST':
        form = TRequestCreateCloneForm(request.POST, request.FILES)
        if form.is_valid():
            # Process the data in form.cleaned_data
            ################### Extra fields in form. Get and remove for creating
            if form.cleaned_data.get('excellink') or form.cleaned_data.get('excelfile'):
                file_dict = form_prefill(form.cleaned_data, request)
                
                del form.cleaned_data['excellink'], form.cleaned_data['excelfile']
                #print request.FILES['excelfile']
                try:
                    
                    form = TRequestCreateCloneConfirmation(form.cleaned_data)
                    inputlists = [x['input_dict'] for x in file_dict]
                    request.session['file_dict'] = file_dict
                    return render(request, 'prodtask/_previewreq.html', {
                                               'active_app' : 'mcprod',
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
                
            elif 'file_dict' in request.session:
                try:
                    #TODO: Waiting message
                    file_dict = request.session['file_dict']
                    del request.session['file_dict']
                    longdesc = form.cleaned_data.get('long_description', '')
                    cc = form.cleaned_data.get('cc', '')
                    del  form.cleaned_data['long_description'], form.cleaned_data['cc'], form.cleaned_data['excellink'], form.cleaned_data['excelfile']
                    req = TRequest(**form.cleaned_data)
                    print form.cleaned_data
                    req.save()
                    #send_mail('Request N %i was created' % req.reqid, longdesc, APP_SETTINGS['mcprod.email.from'] ,
                    #           APP_SETTINGS['mcprod.default.email.list']+cc.replace(';',',').split(','), fail_silently=True)
                    print file_dict
                    for current_slice in file_dict:
                        input_data = current_slice["input_dict"]
                        input_data['request'] = req
                        if input_data['dataset']:
                            input_data['dataset'] = fill_dataset(input_data['dataset'])
                        irl = InputRequestList(**input_data)
                        irl.save()
                        for step in current_slice['step_exec_dict']:
                            st = fill_template(step['step_name'], step['tag'], step['step_exec']['priority'], step.get('formats',None), step.get('memory',None))
                            step['step_exec']['request'] = req
                            step['step_exec']['slice'] = irl
                            step['step_exec']['step_template'] = st
                            print step['step_exec']
                            st_exec = StepExecution(**step['step_exec'])
                            st_exec.save_with_current_time()
                        
                except Exception, e:
                            print e
                            #TODO: Error message
                            return HttpResponseRedirect('/prodtask/request_table/')
                return HttpResponseRedirect('/prodtask/inputlist_with_request/%s' % req.reqid)
                        
               
            else:
                return render(request, 'prodtask/_form.html', {
                                                           'active_app' : 'mcprod',
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
       'active_app' : 'mcprod',
       'pre_form_text' : title,
       'form': form,
       'submit_url': submit_url,
       'url_args'  : rid,
       'parent_template' : 'prodtask/_index.html',
       })
    
def request_create(request):
    return request_clone_or_create(request, None, 'Create TRequest', 'prodtask:request_create',
                                   TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, mcfile_form_prefill)


def dpd_request_create(request):
    return request_clone_or_create(request, None, 'Create TRequest', 'prodtask:dpd_request_create',
                                   TRequestDPDCreateCloneForm, TRequestCreateCloneConfirmation, dpd_form_prefill)

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

