from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.core.urlresolvers import reverse
from django.http import HttpResponse, HttpResponseRedirect, HttpRequest
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.utils import timezone
import core.datatables as datatables
import json
import logging
from .forms import RequestForm, RequestUpdateForm, TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, \
    TRequestDPDCreateCloneForm, MCPatternForm, MCPatternUpdateForm, MCPriorityForm, MCPriorityUpdateForm, \
    TRequestReprocessingCreateCloneForm, TRequestHLTCreateCloneForm
from .models import TRequest, InputRequestList, StepExecution, ProductionDataset, MCPattern, StepTemplate, \
    get_priority_object, RequestStatus
from .models import MCPriority
from .settings import APP_SETTINGS
from .spdstodb import fill_template, fill_steptemplate_from_gsprd, fill_steptemplate_from_file, UrFromSpds
from .dpdconfparser import ConfigParser
from core.xls_parser import open_tempfile_from_url


_logger = logging.getLogger('prodtaskwebui')

def request_details(request, rid=None):
    if rid:
        try:
            req = TRequest.objects.get(reqid=rid)
            form = RequestForm(instance=req)
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    else:
        return HttpResponseRedirect(reverse('prodtask:request_table'))

    return render(request, 'prodtask/_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'TRequest details with ID = %s' % rid,
        'form': form,
        'parent_template': 'prodtask/_index.html',
    })


def request_clone(request, rid=None):
    if rid:
        try:
            values = TRequest.objects.values().get(reqid=rid)
            if values['request_type'] == 'MC':
                return request_clone_or_create(request, rid, 'Clonning of TRequest with ID = %s' % rid,
                                               'prodtask:request_clone', TRequestMCCreateCloneForm,
                                               TRequestCreateCloneConfirmation, mcfile_form_prefill)
            elif values['request_type'] == 'GROUP':
                return request_clone_or_create(request, rid, 'Clonning TRequest', 'prodtask:request_clone',
                                               TRequestDPDCreateCloneForm, TRequestCreateCloneConfirmation,
                                               dpd_form_prefill)
            elif values['request_type'] == 'REPROCESSING':
                 return request_clone_or_create(request, rid, 'Clonning TRequest', 'prodtask:request_clone',
                                                TRequestReprocessingCreateCloneForm, TRequestCreateCloneConfirmation,
                                                reprocessing_form_prefill)
        except Exception, e:
            _logger.error("Problem with request clonning #%i: %s"%(rid,e))
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    else:
        return request_clone_or_create(request, rid, 'Clonning of TRequest with ID = %s' % rid,
                                       'prodtask:request_clone', TRequestMCCreateCloneForm,
                                       TRequestCreateCloneConfirmation, mcfile_form_prefill)


def request_update(request, rid=None):
    if request.method == 'POST':
        try:
            req = TRequest.objects.get(reqid=rid)
            form = RequestUpdateForm(request.POST, instance=req)  # A form bound to the POST data
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
        if form.is_valid():
            # Process the data in form.cleaned_data
            _logger.debug("Update request #%i: %s"%(int(rid), form.cleaned_data))
            try:
                req = TRequest(**form.cleaned_data)
                req.save()
                return HttpResponseRedirect(reverse('prodtask:input_list_approve', args=(req.reqid,)))  # Redirect after POST
            except Exception,e :
                 _logger.error("Problem with request update #%i: %s"%(int(rid), e))
    else:
        try:
            req = TRequest.objects.get(reqid=rid)
            form = RequestUpdateForm(instance=req)
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    return render(request, 'prodtask/_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Updating of TRequest with ID = %s' % rid,
        'form': form,
        'submit_url': 'prodtask:request_update',
        'url_args': rid,
        'parent_template': 'prodtask/_index.html',
    })


def mcfile_form_prefill(form_data, request):
    spreadsheet_dict = []
    eroor_message = ''
    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))
            spreadsheet_dict += fill_steptemplate_from_gsprd(form_data['excellink'])
        elif form_data.get('excelfile'):
            input_excel = request.FILES['excelfile']
            _logger.debug('Try to read data from %s' % input_excel)
            spreadsheet_dict += fill_steptemplate_from_file(input_excel)
    except Exception, e:
        _logger.error('Problem with data gathering %s' % e)
        return {}, str(e)
    if (spreadsheet_dict == []) and (eroor_message == ''):
        eroor_message = 'No good data find in file. Please check that all reqired row are filled.'
    priorities = [x['input_dict']['priority'] for x in spreadsheet_dict]
    for priority in priorities:
        try:
            MCPriority.objects.get(priority_key=int(priority))
        except ObjectDoesNotExist, e:
            _logger.error("Priority %i doesn't exist in the system" % int(priority))
            return {}, "Priority %i doesn't exist in the system" % int(priority)
        except Exception, e:
            _logger.error("Problem with %i doesn't exist in the system" % int(priority))
            return {}, str(e)
    # Fill default values
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'Created'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'ATLAS'
    if not form_data.get('manager'):
        form_data['manager'] = 'None'
    if not form_data.get('request_type'):
        form_data['request_type'] = 'MC'
    _logger.debug('Gathered data: %s' % spreadsheet_dict)
    return spreadsheet_dict, eroor_message


def step_from_tag(tag_name):
    if tag_name[0] == 'r':
        return 'Reco'
    if tag_name[0] == 's':
        return 'Simul'
    if tag_name[0] == 't':
        return 'Rec TAG'
    if tag_name[0] == 'a':
        return 'Simul'
    return 'Reco'

def hlt_form_prefill(form_data, request):
    spreadsheet_dict = []
    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))
            file_name = open_tempfile_from_url(form_data['excellink'], 'txt')
            with open(file_name) as open_file:
                file_obj = open_file.read().split('\n')
        if form_data.get('excelfile'):
            file_obj = request.FILES['excelfile'].read().split('\n')
            _logger.debug('Try to read data from %s' % form_data.get('excelfile'))

        conf_parser = ConfigParser()
        output_dict = conf_parser.parse_config(file_obj)
    except Exception, e:
        _logger.error('Problem with data gathering %s' % e)
        eroor_message = str(e)
        return {},eroor_message
    # Fill default values
    form_data['request_type'] = 'HLT'
    if 'group' in output_dict:
        form_data['phys_group'] = output_dict['group'][0].replace('GR_SM', 'StandartModel').replace('GR_', '').replace('GP_','')
    if 'comment' in output_dict:
        form_data['description'] = output_dict['comment'][0]
    if 'owner' in output_dict:
        form_data['manager'] = output_dict['owner'][0].split("@")[0]
    if 'project' in output_dict:
        if not form_data['campaign']:
            form_data['campaign'] = output_dict['project'][0]
    if 'project' in output_dict:
        form_data['project'] = output_dict['project'][0]
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'Created'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'test'

    task_config = {}
    if 'events_per_job' in output_dict:
        nEventsPerJob = output_dict['events_per_job'][0]
        task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
    if 'project_mode' in output_dict:
        project_mode = output_dict['project_mode'][0]
        task_config.update({'project_mode':project_mode})
    if 'ds' in output_dict:
        for slice_index, ds in enumerate(output_dict['ds']):
            st_sexec_list = []
            irl = dict(slice=slice_index, brief=' ', comment=output_dict.get('comment', [''])[0], dataset=ds,
                       input_data=output_dict.get('joboptions', [''])[0],
                       project_mode=output_dict.get('project_mode', [''])[0],
                       priority=int(output_dict.get('priority', [0])[0]),
                       input_events=int(output_dict.get('total_num_genev', [-1])[0]))
            if 'tag' in output_dict:
                step_name = step_from_tag(output_dict['tag'][0])
                sexec = dict(status='NotChecked', priority=int(output_dict.get('priority', [0])[0]),
                             input_events=int(output_dict.get('total_num_genev', [-1])[0]))
                st_sexec_list.append({'step_name': step_name, 'tag': output_dict['tag'][0], 'step_exec': sexec,
                                      'memory': output_dict.get('ram', [None])[0],
                                      'formats': output_dict.get('formats', [None])[0],
                                      'task_config':task_config})
            spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
    eroor_message = ''
    if not spreadsheet_dict:
        eroor_message = 'No "ds" data founnd in file.'
    _logger.debug('Gathered data: %s' % spreadsheet_dict)
    return spreadsheet_dict, eroor_message


def dpd_form_prefill(form_data, request):
    spreadsheet_dict = []
    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))
            file_name = open_tempfile_from_url(form_data['excellink'], 'txt')
            with open(file_name) as open_file:
                file_obj = open_file.read().split('\n')
        if form_data.get('excelfile'):
            file_obj = request.FILES['excelfile'].read().split('\n')
            _logger.debug('Try to read data from %s' % form_data.get('excelfile'))

        conf_parser = ConfigParser()
        output_dict = conf_parser.parse_config(file_obj)
    except Exception, e:
        _logger.error('Problem with data gathering %s' % e)
        eroor_message = str(e)
        return {},eroor_message
    # Fill default values
    form_data['request_type'] = 'GROUP'
    if 'group' in output_dict:
        form_data['phys_group'] = output_dict['group'][0].replace('GR_SM', 'StandartModel').replace('GR_', '')
    if 'comment' in output_dict:
        form_data['description'] = output_dict['comment'][0]
    if 'owner' in output_dict:
        form_data['manager'] = output_dict['owner'][0].split("@")[0]
    if 'project' in output_dict:
        if not form_data['campaign']:
            form_data['campaign'] = output_dict['project'][0]
    if 'project' in output_dict:
        form_data['project'] = output_dict['project'][0]
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'Created'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'test'

    task_config = {}
    if 'events_per_job' in output_dict:
        nEventsPerJob = output_dict['events_per_job'][0]
        task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
    if 'project_mode' in output_dict:
        project_mode = output_dict['project_mode'][0]
        task_config.update({'project_mode':project_mode})
    if 'ds' in output_dict:
        for slice_index, ds in enumerate(output_dict['ds']):
            st_sexec_list = []
            irl = dict(slice=slice_index, brief=' ', comment=output_dict.get('comment', [''])[0], dataset=ds,
                       input_data=output_dict.get('joboptions', [''])[0],
                       project_mode=output_dict.get('project_mode', [''])[0],
                       priority=int(output_dict.get('priority', [0])[0]),
                       input_events=int(output_dict.get('total_num_genev', [-1])[0]))
            if 'tag' in output_dict:
                step_name = step_from_tag(output_dict['tag'][0])
                sexec = dict(status='NotChecked', priority=int(output_dict.get('priority', [0])[0]),
                             input_events=int(output_dict.get('total_num_genev', [-1])[0]))
                st_sexec_list.append({'step_name': step_name, 'tag': output_dict['tag'][0], 'step_exec': sexec,
                                      'memory': output_dict.get('ram', [None])[0],
                                      'formats': output_dict.get('formats', [None])[0],
                                      'task_config':task_config})
            spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
    eroor_message = ''
    if not spreadsheet_dict:
        eroor_message = 'No "ds" data founnd in file.'
    _logger.debug('Gathered data: %s' % spreadsheet_dict)
    return spreadsheet_dict, eroor_message


def reprocessing_form_prefill(form_data, request):
    spreadsheet_dict = []
    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))
            file_name = open_tempfile_from_url(form_data['excellink'], 'txt')
            with open(file_name) as open_file:
                file_obj = open_file.read().split('\n')
        if form_data.get('excelfile'):
            file_obj = request.FILES['excelfile'].read().split('\n')
            _logger.debug('Try to read data from %s' % form_data.get('excelfile'))
        conf_parser = ConfigParser()
        output_dict = conf_parser.parse_config(file_obj)
    except Exception, e:
        _logger.error('Problem with data gathering %s' % e)
        eroor_message = str(e)
        return {},eroor_message
    # Fill default values
    form_data['request_type'] = 'REPROCESSING'
    if 'group' in output_dict:
        form_data['phys_group'] = output_dict['group'][0].replace('GR_SM', 'StandartModel').replace('GR_', '')
    if 'comment' in output_dict:
        form_data['description'] = output_dict['comment'][0]
    if 'owner' in output_dict:
        form_data['manager'] = output_dict['owner'][0].split("@")[0]
    if 'project' in output_dict:
        if not form_data['campaign']:
            form_data['campaign'] = output_dict['project'][0]
    if 'project' in output_dict:
        form_data['project'] = output_dict['project'][0]
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'Created'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'test'
    task_config = {}
    if 'events_per_job' in output_dict:
        nEventsPerJob = output_dict['events_per_job'][0]
        task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
    if 'project_mode' in output_dict:
        project_mode = output_dict['project_mode'][0]
        task_config.update({'project_mode':project_mode})
    tag_tree = []
    # try:
    #     if form_data['tag_hierarchy']:
    #         tag_tree = string_to_tag_tree(form_data['tag_hierarchy'])
    # except Exception, e:
    #     _logger.error('Problem with data gathering %s' % e)
    #     eroor_message = str(e)
    #     return {},eroor_message
    if 'ds' in output_dict:
        for slice_index, ds in enumerate(output_dict['ds']):
            st_sexec_list = []
            irl = dict(slice=slice_index, brief=' ', comment=output_dict.get('comment', [''])[0], dataset=ds,
                       input_data=output_dict.get('joboptions', [''])[0],
                       project_mode=output_dict.get('project_mode', [''])[0],
                       priority=int(output_dict.get('priority', [0])[0]),
                       input_events=int(output_dict.get('total_num_genev', [-1])[0]))
            for tag_order, (tag_name, formats, tag_parent) in tag_tree:
                #TODO: Give a real name
                step_name = 'Reco'
                sexec = dict(status='NotChecked', priority=int(output_dict.get('priority', [0])[0]),
                             input_events=int(output_dict.get('total_num_genev', [-1])[0]))
                st_sexec_list.append({'step_name': step_name, 'tag': tag_name, 'step_exec': sexec,
                                      'memory': output_dict.get('ram', [None])[0],
                                      'formats': formats,
                                      'task_config':task_config,'step_order':tag_order,'step_parent':tag_parent})
            spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
    eroor_message = ''
    if not spreadsheet_dict:
        eroor_message = 'No "ds" data founnd in file.'
    _logger.debug('Gathered data: %s' % spreadsheet_dict)
    return spreadsheet_dict, eroor_message


def string_to_tag_tree(input_string):
        tag_tree_list = []
        input_string = input_string.replace('\n','')
        input_list = eval(input_string.encode('ascii'))
        if type(input_list) is not list:
            raise SyntaxError('Input should be python list: %s' % str(input_list))
        if type(input_list[0]) is not str:
            raise SyntaxError('Only one root is possible: %s' % str(input_list[0]))
        tag_tree_list, _, _ = recursive_string_tag_tree_parsing(input_list,
                                                                0, 0, tag_tree_list)
        return tag_tree_list


def recursive_string_tag_tree_parsing(rest_list, current_parent, current_position, tag_tree_dict, is_last = False):
    if type(rest_list) is str:
        if ':' in rest_list:
            tag_tree_dict.append((current_position, (rest_list.split(':')[0],rest_list.split(':')[1], current_parent)))
        else:
            tag_tree_dict.append((current_position, (rest_list,'', current_parent)))
        current_parent = current_position
        current_position += 1
    elif type(rest_list) is tuple:
        if not is_last:
            raise SyntaxError('Only one parent is possible: %s' % str(rest_list))
        for sub_token in rest_list[:-1]:
            tag_tree_dict, _, current_position = recursive_string_tag_tree_parsing(sub_token, current_parent,
                                                                                   current_position,
                                                                                   tag_tree_dict)
        tag_tree_dict, current_parent, current_position = recursive_string_tag_tree_parsing(rest_list[-1],
                                                                                            current_parent,
                                                                                            current_position,
                                                                                            tag_tree_dict, True)
    elif type(rest_list) is list:
        for token in rest_list[:-1]:
            tag_tree_dict, current_parent, current_position = recursive_string_tag_tree_parsing(token,
                                                                                                current_parent,
                                                                                                current_position,
                                                                                                tag_tree_dict)
        tag_tree_dict, current_parent, current_position = recursive_string_tag_tree_parsing(rest_list[-1],
                                                                                            current_parent,
                                                                                            current_position,
                                                                                            tag_tree_dict, True)
    else:
        raise SyntaxError('Wrong syntax: %s' % str(rest_list))
    return tag_tree_dict, current_parent, current_position





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


def request_email_body(long_description,ref_link,energy,campaign, link):
    return """
 %s

 The request thread is : %s

Technical details:
- Campaign %s %s
- Link to Request: %s

    """%(long_description,ref_link,energy,campaign, link)


def request_clone_or_create(request, rid, title, submit_url, TRequestCreateCloneForm, TRequestCreateCloneConfirmation,
                            form_prefill):
    """
    Fill form for creating request. Create request->slice->steps for POST
    View create two forms: first for request prefill, second for request creation

    :param request: request
    :param rid: production request id
    :param title: Title for page
    :param submit_url: Submit url
    :param TRequestCreateCloneForm: Form for request prefill
    :param TRequestCreateCloneConfirmation: Form for request submition
    :param form_prefill: function for prefilling data from inpu files and setting default fields

    """
    if request.method == 'POST':
        # Check prefill form
        form = TRequestCreateCloneForm(request.POST, request.FILES)
        if form.is_valid():

            # Process the data from request prefill form
            if form.cleaned_data.get('excellink') or form.cleaned_data.get('excelfile'):
                file_dict, error_message = form_prefill(form.cleaned_data, request)
                if error_message != '':
                    # recreate prefill form with error message
                    return render(request, 'prodtask/_requestform.html', {
                        'active_app': 'mcprod',
                        'pre_form_text': title,
                        'form': form,
                        'submit_url': submit_url,
                        'url_args': rid,
                        'error_message': error_message,
                        'parent_template': 'prodtask/_index.html',
                     })
                else:
                    del form.cleaned_data['excellink'], form.cleaned_data['excelfile']
                    # if 'tag_hierarchy' in form.cleaned_data:
                    #     del form.cleaned_data['tag_hierarchy']
                    try:
                        form = TRequestCreateCloneConfirmation(form.cleaned_data)
                        inputlists = [x['input_dict'] for x in file_dict]
                        # store data from prefill form to http request
                        request.session['file_dict'] = file_dict
                        # create request creation form
                        return render(request, 'prodtask/_previewreq.html', {
                            'active_app': 'mcprod',
                            'pre_form_text': title,
                            'form': form,
                            'submit_url': submit_url,
                            'url_args': rid,
                            'parent_template': 'prodtask/_index.html',
                            'inputLists': inputlists
                        })
                    except Exception, e:
                        _logger.error("Problem during request form creating: %s" % e)
                        return HttpResponseRedirect(reverse('prodtask:request_table'))
            # Process the data from create form form
            elif 'file_dict' in request.session:
                #TODO: Waiting message
                #TODO: One commission
                file_dict = request.session['file_dict']
                del request.session['file_dict']
                longdesc = form.cleaned_data.get('long_description', '')
                cc = form.cleaned_data.get('cc', '')
                del form.cleaned_data['long_description'], form.cleaned_data['cc'], form.cleaned_data['excellink'], \
                    form.cleaned_data['excelfile']
                if 'reqid' in form.cleaned_data:
                    del form.cleaned_data['reqid']
                # if 'tag_hierarchy' in form.cleaned_data:
                #         del form.cleaned_data['tag_hierarchy']
                form.cleaned_data['cstatus'] = 'Created'
                try:
                    _logger.debug("Creating request : %s" % form.cleaned_data)

                    req = TRequest(**form.cleaned_data)
                    req.save()
                    #TODO:Take owner from sso cookies
                    request_status = RequestStatus(request=req,comment='Request created by WebUI',owner='default',
                                                   status='Created')
                    request_status.save_with_current_time()
                    current_uri = request.build_absolute_uri(reverse('prodtask:input_list_approve',args=(req.reqid,)))
                    _logger.debug("e-mail with link %s" % current_uri)
                    send_mail('Request %i: %s %s %s' % (req.reqid,req.phys_group,req.campaign,req.description),
                              current_uri, APP_SETTINGS['prodtask.email.from'],
                              APP_SETTINGS['prodtask.default.email.list'] + cc.replace(';', ',').split(','),
                              fail_silently=True)
                    # Saving slices->steps
                    for current_slice in file_dict:
                        input_data = current_slice["input_dict"]
                        input_data['request'] = req
                        priority_obj = get_priority_object(input_data['priority'])
                        if input_data.get('dataset'):
                                input_data['dataset'] = fill_dataset(input_data['dataset'])
                        _logger.debug("Filling input data: %s" % input_data)
                        irl = InputRequestList(**input_data)
                        irl.save()
                        step_parent_dict = {}
                        for step in current_slice.get('step_exec_dict'):
                            st = fill_template(step['step_name'], step['tag'], input_data['priority'],
                                               step.get('formats', None), step.get('memory', None))
                            task_config= {}
                            upadte_after = False
                            if 'task_config' in step:
                                if 'nEventsPerJob' in step['task_config']:
                                    task_config.update({'nEventsPerJob':int(step['task_config']['nEventsPerJob'].get(step['step_name'],-1))})
                                    task_config.update({'nEventsPerInputFile':int(step['task_config']['nEventsPerJob'].get(step['step_name'],-1))})
                                if 'project_mode' in step['task_config']:
                                    task_config.update({'project_mode':step['task_config']['project_mode']})
                            step['step_exec']['request'] = req
                            step['step_exec']['slice'] = irl
                            step['step_exec']['step_template'] = st
                            step['step_exec']['priority'] = priority_obj.priority(st.step,st.ctag)
                            _logger.debug("Filling step execution data: %s" % step['step_exec'])
                            st_exec = StepExecution(**step['step_exec'])
                            if step_parent_dict:
                                if ('step_parent' in step) and ('step_order' in step):
                                    st_exec.step_parent = step_parent_dict[step['step_parent']]
                                else:
                                    st_exec.step_parent = step_parent_dict[0]
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

                except Exception, e:
                    _logger.error("Problem during request creat: %s" % str(e))
                    #TODO: Error messsage
                    return HttpResponseRedirect(reverse('prodtask:request_table'))
                return HttpResponseRedirect(reverse('prodtask:input_list_approve',args=(req.reqid,)))
            else:
                return render(request, 'prodtask/_form.html', {
                    'active_app': 'mcprod',
                    'pre_form_text': title,
                    'form': form,
                    'submit_url': submit_url,
                    'url_args': rid,
                    'parent_template': 'prodtask/_index.html',
                })
    # GET request
    else:

        # Create clone request form
        if (rid):
            try:
                _logger.debug("Clonning request #%s " % rid)
                values = TRequest.objects.values().get(reqid=rid)
                form = TRequestCreateCloneForm(values)
            except Exception, e:
                _logger.debug("Problem with clonning request #%s - %s " % (rid,e))
                return HttpResponseRedirect(reverse('prodtask:request_table'))
        # Create request form
        else:
            _logger.debug("Start request creation ")
            form = TRequestCreateCloneForm()
    # Create prefill form
    return render(request, 'prodtask/_form.html', {
        'active_app': 'mcprod',
        'pre_form_text': title,
        'form': form,
        'submit_url': submit_url,
        'url_args': rid,
        'parent_template': 'prodtask/_index.html',
    })


def request_create(request):
    return request_clone_or_create(request, None, 'Create MC Request', 'prodtask:request_create',
                                   TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, mcfile_form_prefill)


def dpd_request_create(request):
    return request_clone_or_create(request, None, 'Create DPD Request', 'prodtask:dpd_request_create',
                                   TRequestDPDCreateCloneForm, TRequestCreateCloneConfirmation, dpd_form_prefill)

def hlt_request_create(request):
    return request_clone_or_create(request, None, 'Create HLT Request', 'prodtask:hlt_request_create',
                                   TRequestHLTCreateCloneForm, TRequestCreateCloneConfirmation, hlt_form_prefill)

def reprocessing_request_create(request):
    return request_clone_or_create(request, None, 'Create Reprocessing Request', 'prodtask:reprocessing_request_create',
                                   TRequestReprocessingCreateCloneForm, TRequestCreateCloneConfirmation,
                                   reprocessing_form_prefill)

def mcpattern_create(request, pattern_id=None):
    if pattern_id:
        try:
            values = MCPattern.objects.values().get(id=pattern_id)
            pattern_dict = json.loads(values['pattern_dict'])
            pattern_step_list = [(step, pattern_dict.get(step, '')) for step in MCPattern.STEPS]
        except:
            return HttpResponseRedirect(reverse('prodtask:mcpattern_table'))
    else:
        values = {}
        pattern_step_list = [(step, '') for step in MCPattern.STEPS]
    if request.method == 'POST':
        form = MCPatternForm(request.POST, steps=[(step, '') for step in MCPattern.STEPS])
        if form.is_valid():
            mcp = MCPattern.objects.create(pattern_name=form.cleaned_data['pattern_name'],
                                           pattern_status=form.cleaned_data['pattern_status'],
                                           pattern_dict=json.dumps(form.steps_dict()))

            mcp.save()
            return HttpResponseRedirect(reverse('prodtask:mcpattern_table'))
    else:
         form = MCPatternForm(values, steps=pattern_step_list)
    return render(request, 'prodtask/_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Creating of mcpattern',
        'form': form,
        'submit_url': 'prodtask:mcpattern_create',
        'url_args': pattern_id,
        'parent_template': 'prodtask/_index.html',
    })


def step_list_from_json(json_pattern, STEPS=MCPattern.STEPS):
    pattern_dict = json.loads(json_pattern)
    return [(step, pattern_dict.get(step, '')) for step in STEPS]


def mcpattern_update(request, pattern_id):
    try:
        values = MCPattern.objects.values().get(id=pattern_id)
        pattern_step_list = step_list_from_json(values['pattern_dict'],MCPattern.STEPS)
    except:
        return HttpResponseRedirect(reverse('prodtask:mcpattern_table'))
    if request.method == 'POST':
        form = MCPatternUpdateForm(request.POST, steps=[(step, '') for step in MCPattern.STEPS])
        if form.is_valid():
            mcp = MCPattern.objects.get(id=pattern_id)
            mcp.pattern_status=form.cleaned_data['pattern_status']
            mcp.pattern_dict=json.dumps(form.steps_dict())
            mcp.save()
            return HttpResponseRedirect(reverse('prodtask:mcpattern_table'))
    else:
        form = MCPatternUpdateForm(values, steps=pattern_step_list)
    return render(request, 'prodtask/_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Updating of mcpattern: %s' % values['pattern_name'],
        'form': form,
        'submit_url': 'prodtask:mcpattern_update',
        'url_args': pattern_id,
        'parent_template': 'prodtask/_index.html',
    })




def mcpattern_table(request):
    mcpatterns = MCPattern.objects.all()
    patterns_obsolete = []
    patterns_in_use = []
    header_list = ['Pattern name'] + MCPattern.STEPS
    for mcpattern in mcpatterns:
        current_pattern = {}
        current_pattern.update({'id':mcpattern.id})
        current_pattern.update({'name':mcpattern.pattern_name})
        current_pattern.update({'pattern_steps':[x[1] for x in step_list_from_json(mcpattern.pattern_dict,MCPattern.STEPS)]})
        if mcpattern.pattern_status == 'IN USE':
            patterns_in_use.append(current_pattern)
        else:
            patterns_obsolete.append(current_pattern)
    return render(request, 'prodtask/_mcpattern.html', {
        'active_app': 'mcprod',
        'pre_form_text': "MC Pattern list",
        'submit_url': 'prodtask:mcpattern_table',
        'url_args': '',
        'parent_template': 'prodtask/_index.html',
        'patterns_obsolete': patterns_obsolete,
        'patterns_in_use': patterns_in_use,
        'header_list': header_list

    })


def mcpriority_table(request):
    mcpriorities = MCPriority.objects.order_by('priority_key')
    header_list = ['Priority'] + MCPriority.STEPS
    priorities = []
    for mc_priority in mcpriorities:
        current_priority = {}
        current_priority.update({'id':mc_priority.id})
        current_priority.update({'priority_key':mc_priority.priority_key})
        current_priority.update({'priority_steps':[x[1] for x in step_list_from_json(mc_priority.priority_dict,MCPriority.STEPS)]})
        priorities.append(current_priority)

    return render(request, 'prodtask/_mcpriority.html', {
        'active_app': 'mcprod',
        'pre_form_text': "MC priority list",
        'submit_url': 'prodtask:mcpriority_table',
        'url_args': '',
        'parent_template': 'prodtask/_index.html',
        'priorities': priorities,
        'header_list': header_list

    })

def mcpriority_create(request):

    values = {}
    pattern_step_list = [(step, '') for step in MCPriority.STEPS]
    if request.method == 'POST':
        form = MCPriorityForm(request.POST, steps=[(step, '') for step in MCPriority.STEPS])
        if form.is_valid():
            mcp = MCPriority.objects.create(priority_key=form.cleaned_data['priority_key'],
                                            priority_dict=json.dumps(form.steps_dict()))
            mcp.save()
            return HttpResponseRedirect(reverse('prodtask:mcpriority_table'))
    else:
         form = MCPriorityForm(values, steps=pattern_step_list)
    return render(request, 'prodtask/_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Creating of mc priority',
        'form': form,
        'submit_url': 'prodtask:mcpriority_create',
        'parent_template': 'prodtask/_index.html',
    })


def mcpriority_update(request, pattern_id):
    try:
        values = MCPriority.objects.values().get(id=pattern_id)
        priority_step_list = step_list_from_json(values['priority_dict'],MCPriority.STEPS)
    except:
        return HttpResponseRedirect(reverse('prodtask:mcpriority_table'))
    if request.method == 'POST':
        form = MCPriorityUpdateForm(request.POST, steps=[(step, '') for step in MCPriority.STEPS])
        if form.is_valid():
            mcp = MCPriority.objects.get(id=pattern_id)
            mcp.priority_dict=json.dumps(form.steps_dict())
            mcp.save()
            return HttpResponseRedirect(reverse('prodtask:mcpriority_table'))
    else:
        form = MCPriorityUpdateForm(values, steps=priority_step_list)
    return render(request, 'prodtask/_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Updating of mc priority: %s' % values['priority_key'],
        'form': form,
        'submit_url': 'prodtask:mcpriority_update',
        'url_args': pattern_id,
        'parent_template': 'prodtask/_index.html',
    })

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

    request_type = datatables.Column(
        label='Type',
    )


    cstatus = datatables.Column(
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
        iDisplayLength = 50


        bServerSide = True
        # fnRowCallback = """
        #                 function( nRow, aData, iDisplayIndex, iDisplayIndexFull )
        #                 {
        #                     $('td:eq(0)', nRow).html('<a href="/prodtask/request/'+aData[0]+'/">'+aData[0]+'</a>&nbsp;&nbsp;'+
        #                                              '<span style="float: right;" ><a href="/prodtask/request_update/'+aData[0]+'/">Update</a>&nbsp;'+
        #                                              '<a href="/prodtask/inputlist_with_request/'+aData[0]+'/">List</a></span>'
        #                     );
        #                     $('td:eq(1)', nRow).html('<a href="'+aData[1]+'">'+aData[1]+'</a>');
        #                 }"""

        fnServerData =  'requestServerData'

        def __init__(self):
            self.sAjaxSource = reverse('prodtask:request_table')


@datatables.datatable(RequestTable, name='fct')
def request_table(request):
    qs = request.fct.get_queryset()
    request.fct.update_queryset(qs)
    return TemplateResponse(request, 'prodtask/_request_table.html',
                            {'title': 'Production Requests Table', 'active_app': 'prodtask', 'table': request.fct,
                             'parent_template': 'prodtask/_index.html'})

