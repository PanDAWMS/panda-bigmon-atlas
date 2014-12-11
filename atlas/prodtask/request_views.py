import copy
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.core.urlresolvers import reverse
from django.http import HttpResponse, HttpResponseRedirect, HttpRequest
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse
from django.utils import timezone
from django.db.models import Count, Q
from django.views.decorators.csrf import csrf_protect
from django.db import transaction
from ..prodtask.views import form_existed_step_list, form_step_in_page, fill_dataset

from ..prodtask.ddm_api import find_dataset_events
import core.datatables as datatables
import json
import logging
from .forms import RequestForm, RequestUpdateForm, TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, \
    TRequestDPDCreateCloneForm, MCPatternForm, MCPatternUpdateForm, MCPriorityForm, MCPriorityUpdateForm, \
    TRequestReprocessingCreateCloneForm, TRequestHLTCreateCloneForm
from .models import TRequest, InputRequestList, StepExecution, ProductionDataset, MCPattern, StepTemplate, \
    get_priority_object, RequestStatus, get_default_nEventsPerJob_dict
from .models import MCPriority
from .settings import APP_SETTINGS
from .spdstodb import fill_template, fill_steptemplate_from_gsprd, fill_steptemplate_from_file, UrFromSpds
from .dpdconfparser import ConfigParser
from .xls_parser_new import open_tempfile_from_url


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


def clone_slices(reqid_source,  reqid_destination, slices, step_from, make_link):
        ordered_slices = map(int,slices)
        ordered_slices.sort()
        #form levels from input text lines
        #create chains for each input
        request_source = TRequest.objects.get(reqid=reqid_source)
        if reqid_source == reqid_destination:
            request_destination = request_source
        else:
            request_destination = TRequest.objects.get(reqid=reqid_destination)
        new_slice_number = InputRequestList.objects.filter(request=request_destination).count()
        old_new_step = {}
        for slice_number in ordered_slices:
            current_slice = InputRequestList.objects.filter(request=request_source,slice=int(slice_number))
            new_slice = current_slice.values()[0]
            new_slice['slice'] = new_slice_number
            new_slice_number += 1
            del new_slice['id']
            del new_slice['request_id']
            new_slice['request'] = request_destination
            new_input_data = InputRequestList(**new_slice)
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
                    if (index >= step_from) or (not make_link):
                        self_looped = step.id == step.step_parent.id
                        old_step_id = step.id
                        step.id = None
                        step.step_appr_time = None
                        step.step_def_time = None
                        step.step_exe_time = None
                        step.step_done_time = None
                        step.slice = new_input_data
                        step.request = request_destination
                        if (step.status == 'Skipped') or (index < step_from):
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

def request_clone_slices(reqid, owner, new_short_description, slices):
    _logger.debug("Clone request #%i"%(int(reqid)))
    request_destination = TRequest.objects.get(reqid=reqid)
    request_destination.reqid = None
    request_destination.cstatus = 'waiting'
    request_destination.description = new_short_description
    request_destination.save()
    request_status = RequestStatus(request=request_destination,comment='Request cloned from %i'%int(reqid),owner=owner,
                                                       status='waiting')
    _logger.debug("New request: #%i"%(int(reqid)))
    clone_slices(reqid,request_destination.reqid,slices,0,False)
    return request_destination.reqid




@csrf_protect
def request_clone2(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            ordered_slices = map(int,slices)
            ordered_slices.sort()
            new_short_description = input_dict['description']
            owner=''
            try:
                owner = request.user.username
            except:
                pass
            if not owner:
                owner = 'default'
            new_request_id = request_clone_slices(reqid,owner,new_short_description,ordered_slices)
            results = {'success':True,'new_request':int(new_request_id)}
        except Exception, e:
            _logger.error("Problem with request clonning #%i: %s"%(reqid,e))
        return HttpResponse(json.dumps(results), content_type='application/json')


def create_tarball_input(production_request_id):
    result = ''
    try:
        result_list = []
        production_request = TRequest.objects.get(reqid=production_request_id)
        steps = StepExecution.objects.filter(request=production_request)
        energy = production_request.energy_gev
        group = production_request.phys_group
        for step in steps:
            if (step.step_template.step=='Evgen') and (step.status not in StepExecution.STEPS_APPROVED_STATUS):
                dsid = ''
                short = ''
                if step.slice.input_data:
                    dsid = step.slice.input_data.split('.')[1]
                    short =  step.slice.input_data.split('.')[2]
                result_list.append(dict(group=group,dsid=int(dsid),stats=int(step.input_events),short=short,ecm=int(energy),
                                        jo=step.slice.input_data,etag=step.step_template.ctag))
        result = json.dumps(result_list)
    except Exception,e:
        # log error
        pass
    return result


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
        form_data['cstatus'] = 'waiting'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'AP'
    if not form_data.get('manager'):
        try:
            form_data['manager'] = request.user.username
        except:
            pass
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
    output_dict = {}
    error_message = ''
    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))
            file_name = open_tempfile_from_url(form_data['excellink'], 'txt')
            with open(file_name) as open_file:
                file_obj = open_file.read().split('\n')
        elif form_data.get('excelfile'):
            file_obj = request.FILES['excelfile'].read().split('\n')
            _logger.debug('Try to read data from %s' % form_data.get('excelfile'))
        elif form_data.get('hidden_json_slices'):
            spreadsheet_dict = parse_json_slice_dict(form_data.get('hidden_json_slices'))
        if not spreadsheet_dict:
            conf_parser = ConfigParser()
            output_dict = conf_parser.parse_config(file_obj,['formats'])

    except Exception, e:
        _logger.error('Problem with data gathering %s' % e)
        error_message = str(e)
        return {},error_message
    # Fill default values
    form_data['request_type'] = 'HLT'
    if 'group' in output_dict:
        form_data['phys_group'] = output_dict['group'][0].replace('GR_', '').replace('GP_','')
    if 'comment' in output_dict:
        form_data['description'] = output_dict['comment'][0]
    if 'owner' in output_dict:
        form_data['manager'] = output_dict['owner'][0].split("@")[0]
    else:
        try:
            form_data['manager'] = request.user.username
        except:
            pass
    if 'project' in output_dict:
        if not form_data['campaign']:
            form_data['campaign'] = output_dict['project'][0]
    if 'project' in output_dict:
        form_data['project'] = output_dict['project'][0]
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'waiting'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'GP'
    if not form_data.get('phys_group'):
        form_data['phys_group'] = 'THLT'

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


def parse_json_slice_dict(json_string):
    spreadsheet_dict = []
    input_dict = json.loads(json_string)
    slice_index = 0
    slices_dict = {}


    for slice_step in input_dict.keys():
        # prepare input
        current_step_dict = {}
        for key,item in input_dict[slice_step].items():
            current_step_dict[key] = str(item).strip()
        current_slice,current_step = slice_step.split('_')
        if int(current_slice) not in slices_dict.keys():
            slices_dict[int(current_slice)] = {'steps':{}}
        if current_step == '0':
            slices_dict[int(current_slice)].update(current_step_dict)
            slices_dict[int(current_slice)].update({'step_order':slice_step})
        else:
            slices_dict[int(current_slice)]['steps'][int(current_step)] = current_step_dict
            slices_dict[int(current_slice)]['steps'][int(current_step)].update({'step_order':slice_step})
    for slice_number in range(len(slices_dict.keys())):
            slice = slices_dict[slice_number]
            if  (slice['step_order'] != slice['parentstepshort']):
                datasets = ['foreign'] * len(slices_dict[int(slice['parentstepshort'].split('_')[0])]['datasets'].split(','))
                slice['datasets'] = ','.join(datasets)
            else:
                datasets = [x.strip() for x in slice['datasets'].split(',') if x]
            for prefix,dataset in enumerate(datasets):
                if dataset:
                    if  dataset == 'foreign':
                        irl = dict(slice=slice_index, brief=' ', comment='',
                                   input_data='',
                                   project_mode=slice['projectmode'],
                                   priority=int(slice['priority']),
                                   input_events=int(slice['totalevents']))
                    else:
                        irl = dict(slice=slice_index, brief=' ', comment='', dataset=dataset,
                                   input_data='',
                                   project_mode=slice['projectmode'],
                                   priority=int(slice['priority']),
                                   input_events=int(slice['totalevents']))
                    slice_index += 1
                    st_sexec_list = []
                    if slice['ctag']:
                        task_config = {}
                        nEventsPerJob = slice['eventsperjob']
                        task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
                        merge_options = ['nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob']
                        if slice['jediTag']:
                            task_config.update({'merging_tag':slice['jediTag']})
                            for merge_option in merge_options:
                                if slice[merge_option]:
                                    task_config.update({merge_option:slice[merge_option]})
                        if slice['cmtconfig']:
                            task_config.update({'project_mode':'cmtconfig='+slice['cmtconfig']+';'+slice['projectmode']})
                        else:
                            task_config.update({'project_mode':slice['projectmode']})
                        if slice['token']:
                             task_config.update({'token':'dst:'+slice['token'].replace('dst:','')})

                        if slice['inputFormat']:
                                    task_config.update({'input_format':slice['inputFormat']})
                        if slice['nFilesPerJob']:
                                    task_config.update({'nFilesPerJob':slice['nFilesPerJob']})
                        step_name = step_from_tag(slice['ctag'])
                        sexec = dict(status='NotChecked', priority=int(slice['priority']),
                                     input_events=int(slice['totalevents']))
                        st_sexec_list.append({'step_name': step_name, 'tag': slice['ctag'], 'step_exec': sexec,
                                              'memory': slice['ram'], 'step_order':str(prefix)+'_'+slice['step_order'],
                                              'step_parent': str(prefix)+'_'+slice['parentstepshort'],
                                              'formats': slice['formats'],
                                              'task_config':task_config})
                        for step_number in range(1,len(slice['steps'])+1):
                            step = slice['steps'][step_number]
                            if step['ctag']:
                                task_config = {}
                                nEventsPerJob = step['eventsperjob']
                                task_config.update({'nEventsPerJob':dict((x,nEventsPerJob) for x in StepExecution.STEPS)})
                                if step['cmtconfig']:
                                    task_config.update({'project_mode':'cmtconfig='+step['cmtconfig']+';'+step['projectmode']})
                                else:
                                    task_config.update({'project_mode':step['projectmode']})
                                if step['jediTag']:
                                    task_config.update({'merging_tag':step['jediTag']})
                                    for merge_option in merge_options:
                                        if step[merge_option]:
                                            task_config.update({merge_option:step[merge_option]})
                                if step['token']:
                                     task_config.update({'token':'dst:'+step['token'].replace('dst:','')})
                                if  step['inputFormat']:
                                    task_config.update({'input_format':step['inputFormat']})
                                if  step['nFilesPerJob']:
                                    task_config.update({'nFilesPerJob':step['nFilesPerJob']})
                                step_name = step_from_tag(step['ctag'])
                                sexec = dict(status='NotChecked', priority=int(step['priority']),
                                             input_events=int(step['totalevents']))
                                st_sexec_list.append({'step_name': step_name, 'tag': step['ctag'], 'step_exec': sexec,
                                                      'memory': step['ram'],'step_order':str(prefix)+'_'+step['step_order'],
                                                      'step_parent': str(prefix)+'_'+step['parentstepshort'],
                                                      'formats': step['formats'],
                                                      'task_config':task_config})
                            else:
                                break
                    spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})

    return spreadsheet_dict

def dpd_form_prefill(form_data, request):
    spreadsheet_dict = []
    output_dict = {}
    error_message = ''
    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))
            file_name = open_tempfile_from_url(form_data['excellink'], 'txt')
            with open(file_name) as open_file:
                file_obj = open_file.read().split('\n')
        elif form_data.get('excelfile'):
            file_obj = request.FILES['excelfile'].read().split('\n')
            _logger.debug('Try to read data from %s' % form_data.get('excelfile'))
        elif form_data.get('hidden_json_slices'):
            spreadsheet_dict = parse_json_slice_dict(form_data.get('hidden_json_slices'))
        if not spreadsheet_dict:
            conf_parser = ConfigParser()
            output_dict = conf_parser.parse_config(file_obj,['formats'])

    except Exception, e:
        _logger.error('Problem with data gathering %s' % e)
        error_message = str(e)
        return {},error_message
    # Fill default values
    form_data['request_type'] = 'GROUP'
    if 'group' in output_dict:
        form_data['phys_group'] = output_dict['group'][0].replace('GR_', '')
    if 'comment' in output_dict:
        form_data['description'] = output_dict['comment'][0]
    if 'owner' in output_dict:
        form_data['manager'] = output_dict['owner'][0].split("@")[0]
    else:
        try:
            form_data['manager'] = request.user.username
        except:
            pass
    if 'project' in output_dict:
        if not form_data['campaign']:
            form_data['campaign'] = output_dict['project'][0]
    if 'project' in output_dict:
        form_data['project'] = output_dict['project'][0]
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'waiting'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'GP'
    if not spreadsheet_dict:
        task_config = {}
        if 'events_per_job' in output_dict:
            nEventsPerJob = output_dict['events_per_job'][0]
            task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
        if 'project_mode' in output_dict:
            project_mode = output_dict['project_mode'][0]
            task_config.update({'project_mode':project_mode})
        if 'ds' in output_dict:
            formats = []
            for index,formats_count in enumerate(output_dict.get('formats_count_list', [None])):
                formats+=[output_dict['formats'][index]]*formats_count
            if len(formats)!=len(output_dict['ds']):
                error_message = 'ds and format lenght do not match'
                return {}, error_message
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
                                          'formats': formats[slice_index],
                                          'task_config':task_config})
                spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})

    if not spreadsheet_dict:
        error_message= 'No "ds" data founnd in file.'
    _logger.debug('Gathered data: %s' % spreadsheet_dict)
    return spreadsheet_dict, error_message


def reprocessing_form_prefill(form_data, request):
    spreadsheet_dict = []
    output_dict = {}
    error_message = ''

    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))
            file_name = open_tempfile_from_url(form_data['excellink'], 'txt')
            with open(file_name) as open_file:
                file_obj = open_file.read().split('\n')
        elif form_data.get('excelfile'):
            file_obj = request.FILES['excelfile'].read().split('\n')
            _logger.debug('Try to read data from %s' % form_data.get('excelfile'))
        elif form_data.get('hidden_json_slices'):
            spreadsheet_dict = parse_json_slice_dict(form_data.get('hidden_json_slices'))
        if not spreadsheet_dict:
            conf_parser = ConfigParser()
            output_dict = conf_parser.parse_config(file_obj,['formats'])


    except Exception, e:
        _logger.error('Problem with data gathering %s' % e)
        eroor_message = str(e)
        return {},eroor_message
    # Fill default values
    form_data['request_type'] = 'REPROCESSING'
    if 'group' in output_dict:
        form_data['phys_group'] = output_dict['group'][0].replace('GR_', '')
    if 'comment' in output_dict:
        form_data['description'] = output_dict['comment'][0]
    if 'owner' in output_dict:
        form_data['manager'] = output_dict['owner'][0].split("@")[0]
    else:
        try:
            form_data['manager'] = request.user.username
        except:
            pass
    if 'project' in output_dict:
        if not form_data['campaign']:
            form_data['campaign'] = output_dict['project'][0]
    if 'project' in output_dict:
        form_data['project'] = output_dict['project'][0]
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'waiting'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 8000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'AP'
    if not form_data.get('phys_group'):
        form_data['phys_group'] = 'REPR'
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


@csrf_protect
def find_datasets_by_pattern(request):
    if request.method == 'POST':
        data = request.body
        input_dict = json.loads(data)
        try:
            dataset_pattern = input_dict['datasetPattern']
            if dataset_pattern[-1] != '*' or dataset_pattern[-1] != '/':
                dataset_pattern+='*'
            return_list = find_dataset_events(dataset_pattern)
            results = {}
            results.update({'success':True,'data':return_list})
        except Exception, e:
            _logger.error('Problem with ddm %s' % e)
            results.update({'success':True,'data':[]})
        return HttpResponse(json.dumps(results), content_type='application/json')
    pass





def request_email_body(long_description,ref_link,energy,campaign, link, excel_link):
    if excel_link:
        return """
 %s

 The request thread is : %s

Technical details:
- Campaign %s %s
- Link to Request: %s
- Link to data source: %s
    """%(long_description,ref_link,energy,campaign, link, excel_link)
    else:
        return """
 %s

 The request thread is : %s

Technical details:
- Campaign %s %s
- Link to Request: %s
    """%(long_description,ref_link,energy,campaign, link)


def request_clone_or_create(request, rid, title, submit_url, TRequestCreateCloneForm, TRequestCreateCloneConfirmation,
                            form_prefill, default_step_values = {'nEventsPerJob':'1000','priority':'880'}):
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
            if (form.cleaned_data.get('excellink') or form.cleaned_data.get('excelfile')) or form.cleaned_data.get('hidden_json_slices'):
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
                        'default_step_values': default_step_values
                     })
                else:
                    if form.cleaned_data['excellink']:
                        request.session['excel_link'] = form.cleaned_data['excellink']
                    del form.cleaned_data['excellink'], form.cleaned_data['excelfile']
                    # if 'tag_hierarchy' in form.cleaned_data:
                    #     del form.cleaned_data['tag_hierarchy']
                    try:
                        form = TRequestCreateCloneConfirmation(form.cleaned_data)
                        inputlists = []
                        for slices in file_dict:
                            slice = slices['input_dict']
                            tags = []
                            for step in slices.get('step_exec_dict'):
                                tags.append(step.get('tag'))
                            inputlists.append(copy.deepcopy(slice))
                            inputlists[-1].update({'tags':','.join(tags)})
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
                file_dict = request.session['file_dict']
                excel_link = ''
                if 'excel_link' in request.session:
                    excel_link = request.session['excel_link']
                form2 = TRequestCreateCloneConfirmation(request.POST, request.FILES)
                if not form2.is_valid():
                    inputlists = [x['input_dict'] for x in file_dict]
                    # store data from prefill form to http request
                    return render(request, 'prodtask/_previewreq.html', {
                        'active_app': 'mcprod',
                        'pre_form_text': title,
                        'form': form2,
                        'submit_url': submit_url,
                        'url_args': rid,
                        'parent_template': 'prodtask/_index.html',
                        'inputLists': inputlists
                    })
                del request.session['file_dict']
                longdesc = form.cleaned_data.get('long_description', '')
                cc = form.cleaned_data.get('cc', '')

                del form.cleaned_data['long_description'], form.cleaned_data['cc'], form.cleaned_data['excellink'], \
                    form.cleaned_data['excelfile']
                form.cleaned_data['hidden_json_slices'] = 'a'
                if form.cleaned_data.get('hidden_json_slices'):
                    del form.cleaned_data['hidden_json_slices']
                if 'reqid' in form.cleaned_data:
                    del form.cleaned_data['reqid']
                # if 'tag_hierarchy' in form.cleaned_data:
                #         del form.cleaned_data['tag_hierarchy']
                form.cleaned_data['cstatus'] = 'waiting'
                try:
                    with transaction.atomic():
                        _logger.debug("Creating request : %s" % form.cleaned_data)

                        req = TRequest(**form.cleaned_data)
                        req.save()
                        owner=''
                        owner_mail = ''
                        try:
                            owner = request.user.username
                            owner_mail = request.user.email
                        except:
                            pass
                        if not owner:
                            owner = 'default'
                        if not owner_mail:
                            owner_mails = []
                        else:
                            owner_mails = [owner_mail]
                        request_status = RequestStatus(request=req,comment='Request created by WebUI',owner=owner,
                                                       status='waiting')
                        request_status.save_with_current_time()
                        current_uri = request.build_absolute_uri(reverse('prodtask:input_list_approve',args=(req.reqid,)))
                        _logger.debug("e-mail with link %s" % current_uri)
                        send_mail('Request %i: %s %s %s' % (req.reqid,req.phys_group,req.campaign,req.description),
                                  request_email_body(longdesc, req.ref_link, req.energy_gev, req.campaign,current_uri,excel_link),
                                  APP_SETTINGS['prodtask.email.from'],
                                  APP_SETTINGS['prodtask.default.email.list'] + owner_mails + cc.replace(';', ',').split(','),
                                  fail_silently=True)
                        # Saving slices->steps
                        step_parent_dict = {}
                        for current_slice in file_dict:
                            input_data = current_slice["input_dict"]
                            input_data['request'] = req
                            priority_obj = get_priority_object(input_data['priority'])
                            if input_data.get('dataset'):
                                    input_data['dataset'] = fill_dataset(input_data['dataset'])
                            _logger.debug("Filling input data: %s" % input_data)
                            irl = InputRequestList(**input_data)
                            irl.save()
                            for step in current_slice.get('step_exec_dict'):
                                st = fill_template(step['step_name'], step['tag'], input_data['priority'],
                                                   step.get('formats', None), step.get('memory', None))
                                task_config= {}
                                upadte_after = False
                                if 'task_config' in step:
                                    if 'nEventsPerJob' in step['task_config']:
                                        task_config.update({'nEventsPerJob':int(step['task_config']['nEventsPerJob'].get(step['step_name'],-1))})
                                        if step['step_name']=='Evgen':
                                            task_config.update({'nEventsPerInputFile':int(step['task_config']['nEventsPerJob'].get(step['step_name'],-1))})
                                    task_config_options = ['project_mode','input_format','token','nFilesPerMergeJob',
                                                           'nGBPerMergeJob','nMaxFilesPerMergeJob','merging_tag','nFilesPerJob']
                                    for task_config_option in task_config_options:
                                        if task_config_option in step['task_config']:
                                            task_config.update({task_config_option:step['task_config'][task_config_option]})
                                step['step_exec']['request'] = req
                                step['step_exec']['slice'] = irl
                                step['step_exec']['step_template'] = st
                                step['step_exec']['priority'] = priority_obj.priority(st.step,st.ctag)
                                _logger.debug("Filling step execution data: %s" % step['step_exec'])
                                st_exec = StepExecution(**step['step_exec'])
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

                except Exception, e:
                    _logger.error("Problem during request creat: %s" % str(e))
                    #TODO: Error messsage
                    return HttpResponseRedirect(reverse('prodtask:request_table'))
                return HttpResponseRedirect(reverse('prodtask:input_list_approve',args=(req.reqid,)))
            else:
                return render(request, 'prodtask/_requestform.html', {
                    'active_app': 'mcprod',
                    'pre_form_text': title,
                    'form': form,
                    'submit_url': submit_url,
                    'url_args': rid,
                    'parent_template': 'prodtask/_index.html',
                     'default_step_values': default_step_values
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
    return render(request, 'prodtask/_requestform.html', {
        'active_app': 'mcprod',
        'pre_form_text': title,
        'form': form,
        'submit_url': submit_url,
        'url_args': rid,
        'parent_template': 'prodtask/_index.html',
        'default_step_values': default_step_values
    })


def request_create(request):
    return request_clone_or_create(request, None, 'Create MC Request', 'prodtask:request_create',
                                   TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, mcfile_form_prefill,
                                   {'nEventsPerJob':'1000','priority':'880'})


def dpd_request_create(request):
    return request_clone_or_create(request, None, 'Create DPD Request', 'prodtask:dpd_request_create',
                                   TRequestDPDCreateCloneForm, TRequestCreateCloneConfirmation, dpd_form_prefill,
                                   {'nEventsPerJob':'5000','priority':'520'})

def hlt_request_create(request):
    return request_clone_or_create(request, None, 'Create HLT Request', 'prodtask:hlt_request_create',
                                   TRequestHLTCreateCloneForm, TRequestCreateCloneConfirmation, hlt_form_prefill,
                                   {'nEventsPerJob':'1000','priority':'880'})

def reprocessing_request_create(request):
    return request_clone_or_create(request, None, 'Create Reprocessing Request', 'prodtask:reprocessing_request_create',
                                   TRequestReprocessingCreateCloneForm, TRequestCreateCloneConfirmation,
                                   reprocessing_form_prefill,{'nEventsPerJob':'1000','priority':'880','projectmode':'maxAttempt=15;'})

def mcpattern_create(request, pattern_id=None):
    if pattern_id:
        try:
            values = MCPattern.objects.values().get(id=pattern_id)
            pattern_dict = json.loads(values['pattern_dict'])
            pattern_step_list = [(step, decompress_pattern(pattern_dict.get(step, ''))) for step in MCPattern.STEPS]
        except:
            return HttpResponseRedirect(reverse('prodtask:mcpattern_table'))
    else:
        values = {}
        pattern_step_list = [(step, ['']*2 + [get_default_nEventsPerJob_dict().get(step,'')]) for step in MCPattern.STEPS]
    if request.method == 'POST':
        form = MCPatternForm(request.POST, steps=[(step, ['']*3) for step in MCPattern.STEPS])
        if form.is_valid():
            mcp = MCPattern.objects.create(pattern_name=form.cleaned_data['pattern_name'],
                                           pattern_status=form.cleaned_data['pattern_status'],
                                           pattern_dict=json.dumps(compress_pattern(form.steps_dict())))

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

def decompress_pattern(pattern_dict):
    if pattern_dict:
        try:
            return [pattern_dict['ctag'],pattern_dict['project_mode'],pattern_dict['nEventsPerJob']]
        except:
            return [pattern_dict,'','']
    else:
        return ['','','']

def compress_pattern(form_dict):
    return_dict = {}
    for step in form_dict.keys():
        value_list = json.loads(form_dict[step])
        if value_list:
            if value_list[2] == 'None':
                value_list[2] = ''
            return_dict[step] = {'ctag':value_list[0],'project_mode':value_list[1],'nEventsPerJob':value_list[2]}
        else:
            return_dict[step] = {'ctag':'','project_mode':'','nEventsPerJob':''}
    return return_dict

def step_list_pattern_from_json(json_pattern, STEPS=MCPattern.STEPS):
    pattern_dict = json.loads(json_pattern)

    return [(step, decompress_pattern(pattern_dict.get(step, {}))) for step in STEPS]

def mcpattern_update(request, pattern_id):
    try:
        values = MCPattern.objects.values().get(id=pattern_id)
        pattern_step_list = step_list_pattern_from_json(values['pattern_dict'],MCPattern.STEPS)
    except:
        return HttpResponseRedirect(reverse('prodtask:mcpattern_table'))
    if request.method == 'POST':
        form = MCPatternUpdateForm(request.POST, steps=[(step, ['']*3) for step in MCPattern.STEPS])
        if form.is_valid():
            mcp = MCPattern.objects.get(id=pattern_id)
            mcp.pattern_status=form.cleaned_data['pattern_status']
            mcp.pattern_dict=json.dumps(compress_pattern(form.steps_dict()))
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
        current_pattern.update({'pattern_steps':[x[1][0] for x in step_list_pattern_from_json(mcpattern.pattern_dict,MCPattern.STEPS)]})
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
        sClass='numbers',
    )

    ref_link = datatables.Column(
        label='Link',
    )

    phys_group = datatables.Column(
        label='Group',
        sClass='centered',
    )

    description = datatables.Column(
        label='Description',
    )

    campaign = datatables.Column(
        label='Campaign',
        sClass='centered',
    )

    manager = datatables.Column(
        label='Manager',
        sClass='centered',
    )

    request_type = datatables.Column(
        label='Type',
        sClass='centered',
    )

    cstatus = datatables.Column(
        label='Approval status',
        sClass='centered rstat',
    )

    provenance = datatables.Column(
        bVisible='false',
    )


    class Meta:
        model = TRequest

        id = 'request_table'
        var = 'requestTable'

        bSort = True
        bPaginate = True
        bJQueryUI = True

        sDom = '<"top-toolbar"lf><"table-content"rt><"bot-toolbar"ip>'
        
        bAutoWidth = False
        bScrollCollapse = False
        
        aaSorting = [[0, "desc"]]
        aLengthMenu = [[10, 50, 100, -1], [10, 50, 1000, "All"]]
        iDisplayLength = 50

        bServerSide = True

        fnClientTransformData = "prepareData"

        fnServerParams = "requestServerParams"

        fnDrawCallback = "requestDrawCallback"


        def __init__(self):
            self.sAjaxSource = reverse('prodtask:request_table')

    def additional_data(self, request, qs):
        """
        Overload DataTables method for adding statuses info at the page
        :return: dictionary of data should be added to each server response of table data
        """
        status_stat = get_status_stat(qs)
        return { 'request_stat' : status_stat }

def get_status_stat(qs):
    """
    Compute ProductionRequests statuses by query set
    :return: list of statuses with count of requests in corresponding state
    """
    return [ { 'status':'total', 'count':qs.count() } ] +\
            [ { 'status':str(x['cstatus']), 'count':str(x['count']) }
              for x in qs.values('cstatus').annotate(count=Count('reqid')) ]


class Parameters(datatables.Parametrized):
    reqid = datatables.Parameter(label='Request ID')
    phys_group = datatables.Parameter(label='Physics group')
    campaign = datatables.Parameter(label='Campaign')
    manager = datatables.Parameter(label='Manager')

    type = datatables.Parameter(label='Type', model_field='request_type')
    status = datatables.Parameter(label='Status', model_field='cstatus')
    description = datatables.Parameter(label='Description')
    provenance = datatables.Parameter(label='Provenance')


@datatables.parametrized_datatable(RequestTable, Parameters, name='fct')
def request_table(request):
    """
    Request table
    :return: table page or data for it
    """
    qs = request.fct.get_queryset()
    request.fct.update_queryset(qs)
    return TemplateResponse(request, 'prodtask/_request_table.html',
                            {'title': 'Production Requests Table', 'active_app': 'prodtask', 'table': request.fct,
                             'parametrized': request.parametrized, 'parent_template': 'prodtask/_index.html'})

