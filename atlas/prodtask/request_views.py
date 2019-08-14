import copy
import json
import logging
import string
from math import sqrt

from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.core.urlresolvers import reverse
from django.db import transaction
from django.db.models import Count
from django.db.models import Q
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.template.response import TemplateResponse
from django.views.decorators.csrf import csrf_protect
from rucio.common.exception import DataIdentifierNotFound

from atlas.prodtask.ddm_api import number_of_files_in_dataset, DDM
from atlas.prodtask.views import make_slices_from_dict, request_clone_slices, fill_request_priority, fill_request_events
from ..prodtask.ddm_api import find_dataset_events
from ..prodtask.helper import form_request_log
from ..prodtask.views import form_existed_step_list, fill_dataset, egroup_permissions
from ..prodtask.views import set_request_status

#import core.datatables as datatables
import atlas.datatables as datatables
from .forms import RequestForm, RequestUpdateForm, TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, \
    TRequestDPDCreateCloneForm, MCPatternForm, MCPatternUpdateForm, MCPriorityForm, MCPriorityUpdateForm, \
    TRequestReprocessingCreateCloneForm, TRequestHLTCreateCloneForm, TRequestEventIndexCreateCloneForm, \
    form_input_list_for_preview
from .models import TRequest, InputRequestList, StepExecution, MCPattern, get_priority_object, RequestStatus, get_default_nEventsPerJob_dict, \
    ProductionTask, OpenEndedRequest, TrainProduction, ParentToChildRequest
from .models import MCPriority
from .settings import APP_SETTINGS
from .spdstodb import fill_template, fill_steptemplate_from_gsprd, fill_steptemplate_from_file
from .dpdconfparser import ConfigParser
from .xls_parser_new import open_tempfile_from_url


_logger = logging.getLogger('prodtaskwebui')

def get_object_form_step(step):
    result_object = {"datasetList":"","parentstepshort":"","inputFormat":"","ctag":"","formats":"","eventsperjob":"","totalevents":"",
               "ram":"","cmtconfig":"","projectmode":"","priority":"","nFilesPerJob":"","nGBPerJob":"","maxFailure":"",
               "maxAttempt":"","token":"","jediTag":"","nFilesPerMergeJob":"","nEventsPerMergeJob":"","nGBPerMergeJob":"","nMaxFilesPerMergeJob":"",
               "datasets":"","PDA":'',"PDAParams":''}
    result_object["inputFormat"] = step.get_task_config('input_format')
    result_object["ctag"] = step.step_template.ctag
    result_object["formats"] = step.step_template.output_formats
    result_object["eventsperjob"] = str(step.get_task_config('nEventsPerJob'))
    result_object["totalevents"] = str(step.input_events)
    result_object["ram"] = str(step.step_template.memory)
    result_object["priority"] = str(step.priority)
    project_without_cmt = []
    for token in step.get_task_config('project_mode').split(';'):
        if 'cmtconfig' in token:
            result_object["cmtconfig"] = token[len('cmtconfig')+1:]
        else:
            project_without_cmt.append(token)
    result_object["projectmode"] = ';'.join(project_without_cmt)
    for x in ["nFilesPerJob","nGBPerJob","maxFailure","maxAttempt","token","nEventsPerMergeJob","nFilesPerMergeJob","nGBPerMergeJob",
              "nMaxFilesPerMergeJob","spreadsheet_original",'split_events','PDA','PDAParams']:
        result_object[x] = str(step.get_task_config(x))
    result_object["jediTag"] = str(step.get_task_config('merging_tag'))
    for key in result_object:
        if (result_object[key] == 'None') or not (result_object[key]):
            result_object[key] = ''
    sorted(result_object)
    compare_str = reduce(lambda x, y: x+y, result_object.itervalues())
    return result_object,compare_str


def gather_form_dict(reqid, ordered_slices):
    step_history = {}
    slice_hashes = {}
    slice_number = 0
    result_dict = {}
    for current_slice_number in ordered_slices:
        slice = InputRequestList.objects.get(slice=current_slice_number,request=reqid)
        steps = StepExecution.objects.filter(request=reqid,slice=slice)
        ordered_existed_steps, parent_step = form_existed_step_list(steps)
        current_slice_objects = []
        slice_hash = ''
        for step in ordered_existed_steps:
            current_slice_object,step_hash = get_object_form_step(step)
            current_slice_objects.append(current_slice_object)
            slice_hash += step_hash
        is_doublicate = False
        for x in slice_hashes:
            if slice_hashes[x][0] == slice_hash:
                is_doublicate = True
                for index,step_id in enumerate(slice_hashes[x][1]):
                    step_history[ordered_existed_steps[index]]=step_id
                break
        if not is_doublicate:
            if not parent_step:
                current_slice_objects[0]["parentstepshort"] = str(slice_number)+'_'+'0'
            else:
                current_slice_objects[0]["parentstepshort"] = step_history[parent_step.id]
            result_dict[str(slice_number)+'_'+'0'] = current_slice_objects[0]
            for index,current_slice_object in enumerate(current_slice_objects[1:],1):
                current_slice_object["parentstepshort"] =str(slice_number)+'_'+str(index-1)
                result_dict[str(slice_number)+'_'+str(index)] = current_slice_object
            slice_steps = []
            for index,step in enumerate(ordered_existed_steps):
                step_history[step.id] = str(slice_number)+'_'+str(index)
                slice_steps.append(str(slice_number)+'_'+str(index))
            slice_hashes[slice_number] = (slice_hash,slice_steps)
            slice_number+=1
    return result_dict


@csrf_protect
def reprocessing_object_form(request, reqid):
    if request.method == 'POST':
       try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            ordered_slices = map(int,slices)
            _logger.debug(form_request_log(reqid,request,'Make new reprocessing request from slices: %s' % str(ordered_slices)))
            ordered_slices.sort()
            result_dict = gather_form_dict(reqid,ordered_slices)
            request.session['reprocessing_objects'] = result_dict
            return HttpResponse(json.dumps({'success':True}), content_type='application/json')
       except Exception,e:
            return HttpResponse(json.dumps({'success':False,'message':str(e)}),status=500, content_type='application/json')
    return HttpResponse(json.dumps({'success':False,'message':''}),status=500, content_type='application/json')


def previous_request_status(production_request_id):
    statuses = RequestStatus.objects.filter(request=production_request_id).order_by('-timestamp').values_list('status',flat=True)
    for status in statuses:
        if status not in ['approved','comment']:
            if status == 'waiting':
                return 'working'
            else:
                return status


@login_required(login_url='/prodtask/login/')
@csrf_protect
def short_hlt_form(request):
    if request.method == 'GET':
            return render(request, 'prodtask/_short_hlt_form.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Create hlt request',
                'parent_template': 'prodtask/_index.html',
            })
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            dataset = input_dict['dataset']
            PROJECT_MODE_COMMON = input_dict['commonProjectMode']+'site='+input_dict['sites']+';'

            priority = input_dict['priority']
            outputs = input_dict['outputs'].split('.')
            PROJECT_MODE_RECO = input_dict['recoProjectMode']
            #outputs.append('RAW')
            TAG_CONVERSION = {'recoTag':['Reco'],'reco2Tag':['Reco2'],'mergeTag':['HIST_HLTMON','HIST'],'aodTag':['AOD'],
                              'ntupTag':['NTUP_TRIGRATE','NTUP_TRIGCOST']}
            tags = {}
            for tag in TAG_CONVERSION:
                if input_dict.get(tag):
                    for tag_name in TAG_CONVERSION[tag]:
                        tags[tag_name] = input_dict[tag]
            number_of_files = number_of_files_in_dataset(dataset)
            if number_of_files < 2500:
                merge_files_number = 50
            else:
                merge_files_number = int(sqrt(number_of_files))+10
            if number_of_files == 0:
                raise ValueError('No files in dataset %s'%dataset)
            spreadsheet_dict = []
            slice_index = 0
            for x in input_dict['ram']:
                input_dict['ram'][x] = str(input_dict['ram'][x])
            if not input_dict['twoStep']:
                irl = dict(slice=slice_index, brief='Reco', comment='Reco', dataset=dataset,
                           input_data='',
                           project_mode=PROJECT_MODE_COMMON+PROJECT_MODE_RECO+'ramcount='+input_dict['ram']['recoRam'],
                           priority=priority,
                           input_events=-1)
                slice_index += 1
                sexec = dict(status='NotChecked', priority=priority,
                             input_events=-1)
                task_config =  {'maxAttempt':15,'maxFailure':5}
                st_sexec_list = []
                task_config.update({'project_mode':PROJECT_MODE_COMMON+PROJECT_MODE_RECO+'ramcount='+input_dict['ram']['recoRam'],
                                    'nEventsPerJob':dict((step,250) for step in StepExecution.STEPS)})
                st_sexec_list.append({'step_name': step_from_tag(tags['Reco']), 'tag': tags['Reco'], 'step_exec': sexec,
                                  'formats': '.'.join(outputs),
                                  'task_config':task_config,'step_order':'0_0','step_parent':'0_0'})
                spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
            else:
                irl = dict(slice=slice_index, brief='Reco', comment='Reco', dataset=dataset,
                           input_data='',
                           project_mode=PROJECT_MODE_COMMON+PROJECT_MODE_RECO+'ramcount='+input_dict['ram']['recoRam'],
                           priority=priority,
                           input_events=-1)
                slice_index += 1
                sexec = dict(status='NotChecked', priority=priority,
                             input_events=-1)
                task_config =  {'maxAttempt':15,'maxFailure':5}
                st_sexec_list = []
                task_config.update({'project_mode':PROJECT_MODE_COMMON+PROJECT_MODE_RECO+'ramcount='+input_dict['ram']['recoRam'],
                                    'nEventsPerJob':dict((step,250) for step in StepExecution.STEPS)})
                st_sexec_list.append({'step_name': step_from_tag(tags['Reco']), 'tag': tags['Reco'], 'step_exec': sexec,
                                  'formats': 'HIST_HLTMON.RAW',
                                  'task_config':task_config,'step_order':'0_0','step_parent':'0_0'})
                task_config =  {'maxAttempt':15,'maxFailure':5}
                task_config.update({'project_mode':PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['recoRam'],
                    'nFilesPerJob':1,'input_format':'RAW'})

                st_sexec_list.append({'step_name': step_from_tag(tags['Reco2']), 'tag': tags['Reco2'], 'step_exec': sexec,
                  'formats': 'ESD.HIST',
                  'task_config':task_config,'step_order':'0_1','step_parent':'0_0'})

                if 'AOD' in outputs:
                    task_config =  {'maxAttempt':15,'maxFailure':5}
                    task_config.update({'project_mode':PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['recoRam'],
                        'nFilesPerJob':1,'input_format':'ESD'})

                    st_sexec_list.append({'step_name': step_from_tag(tags['Reco2']), 'tag': tags['Reco2'], 'step_exec': sexec,
                      'formats': 'AOD',
                      'task_config':task_config,'step_order':'0_2','step_parent':'0_1'})
                spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
                irl = dict(slice=slice_index, brief='Reco', comment='Reco', dataset=dataset,
                           input_data='',
                           project_mode=PROJECT_MODE_COMMON+PROJECT_MODE_RECO+'ramcount='+input_dict['ram']['recoRam'],
                           priority=priority,
                           input_events=-1)
                slice_index += 1
                sexec = dict(status='NotChecked', priority=priority,
                             input_events=-1)
                st_sexec_list = []
                task_config =  {'maxAttempt':15,'maxFailure':5}
                task_config.update({'project_mode':PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['recoRam'],
                    'nFilesPerJob':1,'input_format':'RAW'})
                st_sexec_list.append({'step_name': step_from_tag(tags['Reco']), 'tag': tags['Reco'], 'step_exec': sexec,
                                  'formats': 'NTUP_TRIGCOST.NTUP_TRIGRATE.NTUP_TRIGEBWGHT',
                                  'task_config':task_config,'step_order':'1_0','step_parent':'0_0'})
                spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
            for hist_output in ['HIST_HLTMON','HIST','NTUP_TRIGRATE']:
                if (hist_output in outputs) and (hist_output in tags):

                    irl = dict(slice=slice_index, brief=hist_output, comment=hist_output, dataset=dataset,
                               input_data='',
                               project_mode=PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['mergeRam'],
                               priority=priority,
                               input_events=-1)

                    sexec = dict(status='NotChecked', priority=priority,
                                 input_events=-1)

                    st_sexec_list = []
                    step_parent = '0_0'
                    if hist_output=='HIST' and input_dict['twoStep']:
                        step_parent = '0_1'
                    if hist_output=='NTUP_TRIGRATE' and input_dict['twoStep']:
                        step_parent = '1_0'
                    task_config =  {'maxAttempt':20,'maxFailure':15}
                    task_config.update({'nEventsPerJob':dict((step,'') for step in StepExecution.STEPS)})
                    task_config.update(({'project_mode':PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['mergeRam'],
                                         'nFilesPerJob':merge_files_number,'input_format':hist_output}))
                    st_sexec_list.append({'step_name': step_from_tag(tags[hist_output]), 'tag': tags[hist_output], 'step_exec': sexec,
                                      'formats':hist_output,
                                      'task_config':task_config,'step_order':str(slice_index)+'_0','step_parent':step_parent})
                    task_config =  {'maxAttempt':20,'maxFailure':15}
                    task_config.update({'nEventsPerJob':dict((step,'') for step in StepExecution.STEPS)})
                    task_config.update(({'nFilesPerJob':80,
                                         'project_mode':PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['mergeRam'],
                                         'input_format':hist_output, 'token':'dst:CERN-PROD_TRIG-HLT'}))
                    st_sexec_list.append({'step_name': step_from_tag(tags[hist_output]), 'tag': tags[hist_output], 'step_exec': sexec,
                                        'formats':hist_output,
                                      'task_config':task_config,'step_order':str(slice_index)+'_1','step_parent':str(slice_index)+'_0'})
                    slice_index += 1
                    spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
            if ('NTUP_TRIGCOST' in outputs) and ('NTUP_TRIGCOST' in tags):
                irl = dict(slice=slice_index, brief='NTUP_TRIGCOST', comment='NTUP_TRIGCOST', dataset=dataset,
                           input_data='',
                           project_mode=PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['ntupRam'],
                           priority=priority,
                           input_events=-1)
                step_parent = '0_0'
                if input_dict['twoStep']:
                    step_parent = '1_0'
                sexec = dict(status='NotChecked', priority=priority,
                             input_events=-1)

                st_sexec_list = []
                task_config =  {'maxAttempt':20,'maxFailure':15}
                task_config.update({'nEventsPerJob':dict((step,'') for step in StepExecution.STEPS)})
                task_config.update(({'nFilesPerJob':50,'input_format':'NTUP_TRIGCOST',
                                     'project_mode':PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['ntupRam']}))
                st_sexec_list.append({'step_name': step_from_tag(tags['NTUP_TRIGCOST']), 'tag': tags['NTUP_TRIGCOST'], 'step_exec': sexec,

                                  'formats':'NTUP_TRIGCOST',
                                  'task_config':task_config,'step_order':str(slice_index)+'_0','step_parent':step_parent})
                slice_index += 1
                spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
            if ('AOD' in outputs) and ('AOD' in tags):
                irl = dict(slice=slice_index, brief='AOD', comment='AOD', dataset=dataset,
                           input_data='',
                           project_mode=PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['aodRam'],
                           priority=priority,
                           input_events=-1)

                sexec = dict(status='NotChecked', priority=970,
                             input_events=-1)
                step_parent = '0_0'
                if input_dict['twoStep']:
                    step_parent = '0_2'
                st_sexec_list = []
                task_config =  {'maxAttempt':20,'maxFailure':15}
                task_config.update({'nEventsPerJob':dict((step,'') for step in StepExecution.STEPS)})
                task_config.update(({'nFilesPerJob':10,'input_format':'AOD',
                                     'project_mode':PROJECT_MODE_COMMON+'ramcount='+input_dict['ram']['aodRam']}))
                st_sexec_list.append({'step_name': step_from_tag(tags['AOD']), 'tag': tags['AOD'], 'step_exec': sexec,

                                  'formats':'AOD',
                                  'task_config':task_config,'step_order':str(slice_index)+'_0','step_parent':step_parent})
                slice_index += 1
                spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
            request.session['file_dict'] = spreadsheet_dict
            request.session['hlt_dataset'] = dataset
            request.session['hlt_short_description'] = input_dict['short_description']
            request.session['hlt_ref_link'] = input_dict['ref_link']

        except Exception,e:
            return HttpResponse(json.dumps({'success':False,'message':str(e)}),status=500, content_type='application/json')
        return HttpResponse(json.dumps({'success':True}), content_type='application/json')


@csrf_protect
def hlt_form_prepare_request(request):
    if request.method == 'GET':
        try:
            spreadsheet_dict = request.session['file_dict']
            form_data = {}
            dataset = request.session['hlt_dataset']
            form_data['request_type'] = 'HLT'
            form_data['phys_group'] = 'THLT'
            form_data['manager'] = request.user.username
            try:
                form_data['energy_gev'] = int(dataset[dataset.find('_')+1:dataset.find('Te')])*1000
            except:
                pass
            form_data['campaign'] = dataset[:dataset.find('.')]
            form_data['project'] = dataset[:dataset.find('.')]
            form_data['provenance'] = 'AP'
            form_data['description'] = request.session['hlt_short_description']
            form_data['ref_link'] = request.session['hlt_ref_link']
            form = TRequestCreateCloneConfirmation(form_data)
            inputlists = form_input_list_for_preview(spreadsheet_dict)
            # store data from prefill form to http request
            return render(request, 'prodtask/_previewreq.html', {
                'active_app': 'mcprod',
                'pre_form_text': 'Create HLT Request',
                'form': form,
                'submit_url': 'prodtask:hlt_request_create',
                'url_args': None,
                'parent_template': 'prodtask/_index.html',
                'inputLists': inputlists,
                'bigSliceNumber': False
            })
        except Exception,e:
            return short_hlt_form(request)

@login_required(login_url='/prodtask/login/')
@csrf_protect
def short_valid_form(request):
    if request.method == 'GET':
            return render(request, 'prodtask/_short_validation_form.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Create validation reprocessing request',
                'parent_template': 'prodtask/_index.html',
            })
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            datasets = input_dict['dataset'].replace('\n',',').replace('\r',',').replace('\t',',').replace(' ',',').split(',')
            priority = input_dict['priority']
            spreadsheet_dict = []
            slice_index = 0
            last_dataset = None
            for dataset in datasets:
                if dataset:
                    format_ESD = input_dict['esd_format']
                    format_AOD = input_dict['aod_format']

                    irl = dict(slice=slice_index, brief='Reco', comment='Reco', dataset=dataset,
                           input_data='',
                           project_mode=input_dict['recoProjectMode'],
                           priority=priority,
                           input_events=-1)
                    sexec = dict(status='NotChecked', priority=priority,
                                 input_events=-1)
                    task_config =  {'maxAttempt':15,'maxFailure':5}
                    st_sexec_list = []
                    task_config.update({'project_mode':input_dict['recoProjectMode'],
                                        'nFilesPerJob':1})
                    st_sexec_list.append({'step_name': step_from_tag(input_dict['recoTag']), 'tag': input_dict['recoTag'], 'step_exec': sexec,
                                      'formats': format_AOD + '.'  + format_ESD,
                                      'task_config':task_config,'step_order':str(slice_index)+'_0','step_parent':str(slice_index)+'_0'})
                    sexec = dict(status='NotChecked', priority=priority,
                                 input_events=-1)
                    task_config =  {'maxAttempt':15,'maxFailure':5}
                    task_config.update({'project_mode':input_dict['AODMergeProjectMode'],
                                        'nFilesPerJob':10,'input_format':format_AOD})
                    st_sexec_list.append({'step_name': step_from_tag(input_dict['aodTag']), 'tag': input_dict['aodTag'], 'step_exec': sexec,
                                      'formats': format_AOD,
                                      'task_config':task_config,'step_order':str(slice_index)+'_1','step_parent':str(slice_index)+'_0'})
                    spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
                    slice_index += 1
                    if input_dict['doNTUP']:
                        irl = dict(slice=slice_index, brief='Reco', comment='Reco', dataset=dataset,
                               input_data='',
                               project_mode=input_dict['ntupProjectMode'],
                               priority=priority,
                               input_events=-1)

                        sexec = dict(status='NotChecked', priority=priority,
                                     input_events=-1)
                        task_config =  {'maxAttempt':15,'maxFailure':5}
                        st_sexec_list = []
                        task_config.update({'project_mode':input_dict['ntupProjectMode'],
                                            'nFilesPerJob':1,'input_format':format_AOD})
                        st_sexec_list.append({'step_name': step_from_tag(input_dict['ntupTag']), 'tag': input_dict['ntupTag'], 'step_exec': sexec,
                                          'formats': 'NTUP_PHYSVAL',
                                          'task_config':task_config,'step_order':str(slice_index)+'_0','step_parent':str(slice_index-1)+'_1'})
                        spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
                        slice_index += 1
                    last_dataset = dataset
            request.session['file_dict'] = spreadsheet_dict
            request.session['valid_dataset'] = last_dataset
            request.session['valid_short_description'] = input_dict['short_description']
            if 'ref_link' in input_dict:
                request.session['valid_ref_link'] = input_dict['ref_link']
            else:
                request.session['valid_ref_link'] = ''

        except Exception,e:
            return HttpResponse(json.dumps({'success':False,'message':str(e)}),status=500, content_type='application/json')
        return HttpResponse(json.dumps({'success':True}), content_type='application/json')


@csrf_protect
def valid_form_prepare_request(request):
    if request.method == 'GET':
        try:
            spreadsheet_dict = request.session['file_dict']
            form_data = {}
            dataset = request.session['valid_dataset']
            form_data['request_type'] = 'REPROCESSING'
            form_data['phys_group'] = 'VALI'
            form_data['manager'] = request.user.username
            try:
                form_data['energy_gev'] = int(dataset[dataset.find('_')+1:dataset.find('Te')])*1000
            except:
                pass
            form_data['campaign'] = dataset[:dataset.find('.')]
            form_data['project'] = dataset[:dataset.find('.')]
            form_data['provenance'] = 'AP'
            form_data['description'] = request.session['valid_short_description']
            if request.session['valid_ref_link']:
                form_data['ref_link'] = request.session['valid_ref_link']
            form = TRequestCreateCloneConfirmation(form_data)
            inputlists = form_input_list_for_preview(spreadsheet_dict)
            # store data from prefill form to http request
            return render(request, 'prodtask/_previewreq.html', {
                'active_app': 'mcprod',
                'pre_form_text': 'Create Reprocesing Request',
                'form': form,
                'submit_url': 'prodtask:hlt_request_create',
                'url_args': None,
                'parent_template': 'prodtask/_index.html',
                'inputLists': inputlists,
                'bigSliceNumber': False
            })
        except Exception,e:
            return short_valid_form(request)




def request_status_update(request_from,request_to):
        requests = TRequest.objects.filter(reqid__gte=request_from,reqid__lte=request_to)
        result_dict = {'executing':[],'processed':[],'done':[],'finished':[]}

        for req in requests:
            steps_with_tasks = []
            tasks = ProductionTask.objects.filter(request=req)

            is_done_exist = False
            is_finished_exist = False
            is_fail_exist = False
            is_run_exist = False
            for task in tasks:
                if task.status in ['running','submitting','registered','assigning']:
                    is_run_exist = True
                if task.status in ['failed','broken','aborted']:
                    is_fail_exist = True
                if task.status in ['finished']:
                    is_finished_exist = True
                if task.status in ['done']:
                    is_done_exist = True
                steps_with_tasks.append(task.step_id)
            # steps = StepExecution.objects.filter(request=req)
            # for step in steps:
            #      if step.id not in steps_with_tasks:
            #          if not step.slice.is_hide:
            #              is_run_exist = True
            #              break
            if is_run_exist and not(is_fail_exist or is_finished_exist or is_done_exist):
                result_dict['processed'].append(req)
            elif is_run_exist and (is_fail_exist or is_finished_exist or is_done_exist):
                result_dict['processed'].append(req)
            elif (not is_run_exist) and (is_fail_exist or is_finished_exist):
                result_dict['finished'].append(req)
            elif is_done_exist and not(is_run_exist or is_fail_exist or is_finished_exist):
                result_dict['finished'].append(req)
        for status,requests in result_dict.items():
            for req in requests:
                    #print req.reqid,status
                    if req.cstatus != status and req.cstatus == 'processed':
                        print req.reqid,status
                        req.cstatus = status
                        req.save()
                        request_status = RequestStatus(request=req,comment='automatic update',owner='default',
                                                         status=status)
                        request_status.save_with_current_time()


def clean_old_request(do_action=False):
    all_requests = TRequest.objects.filter(cstatus='test')
    total_request = 0
    for request in all_requests:
        task_count = ProductionTask.objects.filter(request=request).count()
        if (task_count == 0):
            print request.reqid
            if do_action:
                steps = StepExecution.objects.filter(request=request)
                for step in steps:
                    step.delete()
                request.delete()
            total_request +=1
    print total_request


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

@csrf_protect
def close_deft_ref(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            if json.loads(data)['close']:
                production_request = TRequest.objects.get(reqid=reqid)
                production_request.is_error = False
                production_request.save()
            results = {'success':True}
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')


def request_comments(request, reqid):
     if request.method == 'GET':
        results = {'success':False}
        try:
            _logger.debug(form_request_log(reqid,request,'Comments' ))
            comments = RequestStatus.objects.filter(request=reqid,status='comment').order_by('-timestamp')
            str_comments = []
            for comment in comments:
                str_comments.append(str(comment.timestamp)+'; '+comment.owner+'-'+comment.comment)
            results = {'success':True,'comments':str_comments}
        except Exception, e:
            _logger.error("Problem with comments")
        return HttpResponse(json.dumps(results), content_type='application/json')


@csrf_protect
def check_user_exists(request):
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            results = {'user_exists': User.objects.filter(username=input_dict['username']).exists()}
        except Exception, e:
            _logger.error("Problem with checking user : %s"%e)
            results = str(e)
        return HttpResponse(json.dumps(results), content_type='application/json')


@csrf_protect
def make_user_as_owner(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            production_request = TRequest.objects.get(reqid=reqid)
            current_manager = production_request.manager
            # if production_request.cstatus in ['waiting','registered','test']:
            data = request.body
            input_dict = json.loads(data)
            owner = input_dict['username']

            _logger.debug(form_request_log(reqid,request,'Change manager %s'%owner ))
            try:
                if owner and (owner != current_manager):
                    production_request.manager = owner
                    production_request.save()
                    current_manager = owner
            except:
                pass
            if production_request.cstatus == 'registered':
                change_request_status(request, reqid, 'working',
                                         'Request status was changed to %s by %s' %('working', request.user.username),
                                         'Request status is changed to %s by WebUI' % 'working')

            results = {'success':True,'ownerName':current_manager}
        except Exception, e:
            _logger.error("Problem with changing manager #%i: %s"%(reqid,e))
            results = str(e)
        return HttpResponse(json.dumps(results), content_type='application/json')


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
            new_ref = input_dict['ref']
            new_project = input_dict['project']
            owner=''
            try:
                owner = request.user.username
            except:
                pass
            if not owner:
                owner = 'default'
            _logger.debug(form_request_log(reqid,request,'Clone request' ))
            new_request_id = request_clone_slices(reqid, owner, new_short_description, new_ref, ordered_slices, new_project)
            results = {'success':True,'new_request':int(new_request_id)}
        except Exception, e:
            _logger.error("Problem with request clonning #%i: %s"%(reqid,e))
        return HttpResponse(json.dumps(results), content_type='application/json')


@csrf_protect
def status_history(request, reqid):
    if request.method == 'GET':
        results = {'success':False}
        try:
            _logger.debug(form_request_log(reqid,request,'Get status history'))
            request_status = RequestStatus.objects.filter(request=reqid).order_by('-timestamp')
            result_data = [{'status': x.status,'user': x.owner,'date': str(x.timestamp),'comment': x.comment} for x in request_status]
            results = {'success': True,'data': result_data}
        except Exception, e:
            _logger.error("Problem with getting status #%i: %s"%(reqid,e))
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


def request_update(request, reqid=None):
    if request.method == 'POST':
        try:
            req = TRequest.objects.get(reqid=reqid)
            form = RequestUpdateForm(request.POST, instance=req)  # A form bound to the POST data
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
        if form.is_valid():
            # Process the data in form.cleaned_data
            _logger.debug(form_request_log(reqid,request,'Update request: %s' % str(form.cleaned_data)))
            try:
                req = TRequest(**form.cleaned_data)
                req.save()
                return HttpResponseRedirect(reverse('prodtask:input_list_approve', args=(req.reqid,)))  # Redirect after POST
            except Exception,e :
                 _logger.error("Problem with request update #%i: %s"%(int(reqid), e))
    else:
        try:
            req = TRequest.objects.get(reqid=reqid)
            form = RequestUpdateForm(instance=req)
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    return render(request, 'prodtask/_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Updating of TRequest with ID = %s' % reqid,
        'form': form,
        'submit_url': 'prodtask:request_update',
        'url_args': reqid,
        'parent_template': 'prodtask/_index.html',
    })

MAX_EVENTS_IN_SLICE = 2000000


def check_need_split(spreadsheet_dict):
    total_number = 0
    for slice_input in spreadsheet_dict:
        if int(slice_input['input_dict']['input_events']) > MAX_EVENTS_IN_SLICE:
            total_number += 1
    return total_number


def change_order_slice(slice_input_dict,step_exec_dict,new_index):
    slice_input_dict['slice'] = new_index
    for step in step_exec_dict:
        step['step_order'] = str(new_index) + step['step_order'].split('_')[1]
        step['step_parent'] = str(new_index) + step['step_parent'].split('_')[1]
    return {'input_dict':slice_input_dict, 'step_exec_dict':step_exec_dict}


def do_big_slice_split(spreadsheet_dict, divider):
    modified_spreadsheet_dict= []
    new_index = 0
    for index,slice_input in enumerate(spreadsheet_dict):
        if (slice_input['input_dict']['input_events'] > divider) and ((int(slice_input['input_dict']['input_events']) / int(divider)) < 200):
            for i in range(int(slice_input['input_dict']['input_events']) / int(divider)):
                slice_input_dict = copy.deepcopy(slice_input['input_dict'])
                step_exec_dict = copy.deepcopy(slice_input['step_exec_dict'])
                slice_input_dict['input_events'] = int(divider)
                comment = slice_input_dict['comment']
                slice_input_dict['comment'] = comment[:comment.find(')')+1] + '('+str(i)+')' + comment[comment.find(')')+1:]
                modified_spreadsheet_dict.append(change_order_slice(slice_input_dict,step_exec_dict,new_index))
                new_index += 1
            if (slice_input['input_dict']['input_events'] % divider) != 0:
                slice_input_dict = copy.deepcopy(slice_input['input_dict'])
                step_exec_dict = copy.deepcopy(slice_input['step_exec_dict'])
                slice_input_dict['input_events'] = int(slice_input['input_dict']['input_events']) % int(divider)
                comment = slice_input_dict['comment']
                slice_input_dict['comment'] = comment[:comment.find(')')+1] + '('+str((int(slice_input['input_dict']['input_events']) / int(divider)) )+')' + comment[comment.find(')')+1:]
                modified_spreadsheet_dict.append(change_order_slice(slice_input_dict,step_exec_dict,new_index))
                new_index += 1
        else:
            if index != new_index:
                modified_spreadsheet_dict.append(change_order_slice(slice_input['input_dict'],slice_input['step_exec_dict'],new_index))
            else:
                modified_spreadsheet_dict.append(slice_input)
            new_index += 1
    return modified_spreadsheet_dict


def mcfile_form_prefill(form_data, request):
    spreadsheet_dict = []
    eroor_message = ''
    try:
        if form_data.get('excellink'):
            _logger.debug('Try to read data from %s' % form_data.get('excellink'))

            spreadsheet_dict += fill_steptemplate_from_gsprd(form_data['excellink'], form_data['version'])
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
    form_data['need_split'] = (check_need_split(spreadsheet_dict) > 0)
    # if form_data['need_split']:
    #     spreadsheet_dict = do_big_slice_split(spreadsheet_dict,2e6)
    if len(spreadsheet_dict)>220:
        if (not request.user.is_superuser) and \
                            ('MCCOORD' not in egroup_permissions(request.user.username)):
            eroor_message = "Too many samples selected for a single request - please factorise request into " \
                            "multiple smaller requests. For example, split up Full and Fast simulation and " \
                            "25 ns and 50 ns reconstruction and put them in separate requests. For special cases " \
                            "exceptions can be made, please contact the MC production coordinators if this is required."
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
        form_data['provenance'] = 'AP'
    if not form_data.get('phys_group'):
        form_data['phys_group'] = 'THLT'

    task_config = {'maxAttempt':30,'maxFailure':15}
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
    slice_numbers = slices_dict.keys()
    slice_numbers.sort()
    for slice_number in slice_numbers:
            slice = slices_dict[slice_number]
            if  (slice['step_order'] != slice['parentstepshort']):
                slice['datasets'] = slices_dict[int(slice['parentstepshort'].split('_')[0])]['datasets']
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
                        merge_options = ['nEventsPerMergeJob','nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob']
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
                        for parameter in ['nFilesPerJob','nGBPerJob','maxAttempt','maxFailure','PDA','PDAParams']:
                            if slice[parameter]:
                                    task_config.update({parameter:slice[parameter]})
                        if slice['jediTag']:
                                    task_config.update({'merging_tag':slice['jediTag']})
                                    for merge_option in merge_options:
                                        if slice[merge_option]:
                                            task_config.update({merge_option:slice[merge_option]})

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
                                if step['inputFormat']:
                                    task_config.update({'input_format':step['inputFormat']})
                                for parameter in ['nFilesPerJob','nGBPerJob','maxAttempt','maxFailure','PDA','PDAParams']:
                                    if step[parameter]:
                                        task_config.update({parameter:step[parameter]})
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
    allow_priority = False
    if 'owner' in output_dict:
        form_data['manager'] = output_dict['owner'][0].split("@")[0]
    else:
        try:
            form_data['manager'] = request.user.username
            if request.user.is_superuser:
                allow_priority = True
        except:
            pass
    if 'project' in output_dict:
        if not form_data['campaign']:
            form_data['campaign'] = output_dict['project'][0]
    if 'project' in output_dict:
        form_data['project'] = output_dict['project'][0]
    if 'energy' in output_dict:
        form_data['energy_gev'] = output_dict['energy'][0]
    if not form_data.get('cstatus'):
        form_data['cstatus'] = 'waiting'
    if not form_data.get('energy_gev'):
        form_data['energy_gev'] = 13000
    if not form_data.get('provenance'):
        form_data['provenance'] = 'GP'
    if not spreadsheet_dict:
        task_config = {'maxAttempt':30,'maxFailure':15}
        if 'events_per_job' in output_dict:
            nEventsPerJob = output_dict['events_per_job'][0]
            task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
        TASK_CONFIG_FIELDS =   ['project_mode','nGBPerJob','nFilesPerJob','nEventsPerMergeJob','nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob']
        for x in TASK_CONFIG_FIELDS:
            if x in output_dict:
                field_value = output_dict[x][0]
                task_config.update({x:field_value})

        if 'ds' in output_dict:
            formats = []
            for index,formats_count in enumerate(output_dict.get('formats_count_list', [None])):
                formats+=[output_dict['formats'][index]]*formats_count
            if len(formats)!=len(output_dict['ds']):
                error_message = 'ds and format lenght do not match'
                return {}, error_message
            for slice_index, ds in enumerate(output_dict['ds']):
                st_sexec_list = []
                current_priority = int(output_dict.get('priority', [0])[0])
                if (current_priority > 560) and (not allow_priority):
                    current_priority = 560
                irl = dict(slice=slice_index, brief=' ', comment=output_dict.get('comment', [''])[0], dataset=ds,
                           input_data=output_dict.get('joboptions', [''])[0],
                           project_mode=output_dict.get('project_mode', [''])[0],
                           priority=current_priority,
                           input_events=int(output_dict.get('total_num_genev', [-1])[0]))
                if 'tag' in output_dict:
                    step_name = step_from_tag(output_dict['tag'][0])
                    sexec = dict(status='NotChecked', priority=current_priority,
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
    task_config = {'maxAttempt':30,'maxFailure':15}
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


def eventindex_form_prefill(form_data, request):
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
    form_data['request_type'] = 'EVENTINDEX'
    if 'group' in output_dict:
        form_data['phys_group'] = output_dict['group'][0].replace('AP_', '')
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
        form_data['phys_group'] = 'VALI'
    task_config = {'maxAttempt':20,'maxFailure':5}
    if 'events_per_job' in output_dict:
        nEventsPerJob = output_dict['events_per_job'][0]
        task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
    if 'project_mode' in output_dict:
        project_mode = output_dict['project_mode'][0]
        task_config.update({'project_mode':project_mode})
    tag_tree = []

    if 'ds' in output_dict:
        for slice_index, ds in enumerate(output_dict['ds']):
            st_sexec_list = []
            irl = dict(slice=slice_index, brief=' ', comment=output_dict.get('comment', [''])[0], dataset=ds,
                       input_data=output_dict.get('joboptions', [''])[0],
                       project_mode=output_dict.get('project_mode', [''])[0],
                       priority=int(output_dict.get('priority', [0])[0]),
                       input_events=int(output_dict.get('total_num_genev', [-1])[0]))
            for tag_order, (tag_name, formats, tag_parent) in tag_tree:
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





def resend_email(request,reqid):
    if request.method == 'GET':
        if request.user.is_superuser:
            try:
                production_request = TRequest.objects.get(reqid=reqid)
                owner_mails = []
                current_uri = request.build_absolute_uri(reverse('prodtask:input_list_approve',args=(reqid,)))
                form_and_send_email(production_request,owner_mails,'',production_request.info_field('long_description'),
                                    current_uri,production_request.info_field('data_source'),True)
            except Exception,e:
                print e
        return HttpResponseRedirect('/')



def form_and_send_email(production_request, owner_mails, cc, long_description,current_uri,excel_link,need_approve,manager_name):
    long_description = filter(lambda x: x in string.printable, long_description)
    short_description = filter(lambda x: x in string.printable, production_request.description).replace('\n','').replace('\r','')
    subject = 'Request {group_name} {description} {energy} GeV'.format(group_name=production_request.phys_group,
                                                          description=short_description,
                                                          energy=str(production_request.energy_gev))
    mail_body = """
{long_description}

Best,

{manager_name}

Details:
- JIRA for the request : {ref_link}
- Campaign {energy} {campaign} {sub_campaign} {project}
- Link to Request: {link}
""".format(long_description=long_description,ref_link=production_request.ref_link,
               energy=production_request.energy_gev,campaign=production_request.campaign,
               sub_campaign=production_request.subcampaign, link = current_uri, manager_name=manager_name,
            project=production_request.project )
    if (production_request.phys_group != 'VALI') and (production_request.request_type == 'MC'):
        mail_body = "Dear PMG and PC,\n"+mail_body
        mail_from = "atlas.mc-production@cern.ch"
        if need_approve:
            owner_mails += ["atlas-csc-prodman@cern.ch"]
    else:
        mail_from = APP_SETTINGS['prodtask.email.from']
        pass
    if excel_link:
        mail_body += "- Data source: %s\n" % excel_link
    #print subject, mail_body, mail_from, APP_SETTINGS['prodtask.default.email.list'] + owner_mails + cc.replace(';', ',').split(','),manager_name
    send_mail(subject,
      mail_body,
      mail_from,
      APP_SETTINGS['prodtask.default.email.list'] + owner_mails + cc.replace(';', ',').split(','),
      fail_silently=True)




def request_clone_or_create(request, rid, title, submit_url, TRequestCreateCloneForm, TRequestCreateCloneConfirmation,
                            form_prefill, default_step_values = {'nEventsPerJob':'1000','priority':'880'}, version='2.0'):
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

    default_object = {}
    do_initialize = False
    if request.method == 'POST':
        # Check prefill form
        form = TRequestCreateCloneForm(request.POST, request.FILES)
        if form.is_valid():

            # Process the data from request prefill form
            if (form.cleaned_data.get('excellink') or form.cleaned_data.get('excelfile')) or form.cleaned_data.get('hidden_json_slices'):
                form.cleaned_data['version'] = version
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
                    if form.cleaned_data.has_key('excelfile'):
                        del form.cleaned_data['excelfile']
                    del form.cleaned_data['excellink']
                    # if 'tag_hierarchy' in form.cleaned_data:
                    #     del form.cleaned_data['tag_hierarchy']
                    try:
                        form = TRequestCreateCloneConfirmation(form.cleaned_data)
                        inputlists = form_input_list_for_preview(file_dict)
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
                            'inputLists': inputlists,
                            'bigSliceNumber': check_need_split(file_dict)
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
                    request.session['excel_link']=''
                form2 = TRequestCreateCloneConfirmation(request.POST, request.FILES)

                if not form2.is_valid():

                    inputlists = form_input_list_for_preview(file_dict)
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
                train = form.cleaned_data.get('train',None)
                close_train = None
                if 'close_train' in request.session:
                    close_train = request.session['close_train']
                    _logger.debug("Close train %s" % str(close_train))
                    del request.session['close_train']
                need_approve = form2.cleaned_data['need_approve']
                for x in ['excelfile','need_split','split_divider','train']:
                    if form.cleaned_data.has_key(x):
                        del form.cleaned_data[x]
                del form.cleaned_data['long_description'], form.cleaned_data['cc'], form.cleaned_data['excellink'], \
                     form.cleaned_data['need_approve']
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
                        manager_name = request.user.first_name + ' ' + request.user.last_name
                        req = TRequest(**form.cleaned_data)
                        info_fields = json.dumps({'long_description':longdesc,'cc':cc,'data_source':excel_link})
                        if len(info_fields)>2000:
                            req.info_fields = info_fields[:1999]
                            req.save()
                        req.info_fields = info_fields
                        req.save()

                        request_status = RequestStatus(request=req,comment='Request created by WebUI',owner=owner,
                                                       status='waiting')

                        request_status.save_with_current_time()
                        current_uri = request.build_absolute_uri(reverse('prodtask:input_list_approve',args=(req.reqid,)))
                        _logger.debug("e-mail with link %s" % current_uri)
                        try:
                            form_and_send_email(req,owner_mails,cc,longdesc,current_uri,excel_link,need_approve,manager_name)
                        except Exception,e:
                            _logger.error("Problem during mail sending: %s" % str(e))
                        # Saving slices->steps
                        try:
                            if close_train:
                                train_id = close_train
                                train = TrainProduction.objects.get(id=train_id)
                                train.status = 'Started'
                                train.request = req
                                train.save()
                            if train:
                                new_relation = ParentToChildRequest()
                                new_relation.train = train
                                new_relation.parent_request = req
                                new_relation.relation_type = 'BC'
                                new_relation.status = 'active'
                                new_relation.save()
                        except:
                            pass
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
                                        if step['task_config']['nEventsPerJob'].get(step['step_name'],''):
                                            task_config.update({'nEventsPerJob':int(step['task_config']['nEventsPerJob'].get(step['step_name']))})

                                            if step['step_name']=='Evgen':
                                                task_config.update({'nEventsPerInputFile':int(step['task_config']['nEventsPerJob'].get(step['step_name'],0))})
                                        else:
                                            task_config.update({'nEventsPerJob':step['task_config']['nEventsPerJob'].get(step['step_name'])})
                                    task_config_options = ['project_mode','input_format','token','nEventsPerMergeJob','nFilesPerMergeJob',
                                                           'nGBPerMergeJob','nMaxFilesPerMergeJob','merging_tag','nFilesPerJob',
                                                           'nGBPerJob','maxAttempt','maxFailure','spreadsheet_original','split_events','evntFilterEff',
                                                          'PDA',
                                                          'PDAParams']
                                    for task_config_option in task_config_options:
                                        if task_config_option in step['task_config']:
                                            task_config.update({task_config_option:step['task_config'][task_config_option]})
                                step['step_exec']['request'] = req
                                step['step_exec']['slice'] = irl
                                step['step_exec']['step_template'] = st
                                if not step['step_exec'].get('priority'):
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

                        try:
                            fill_request_priority(req.reqid,req.reqid)
                            fill_request_events(req.reqid,req.reqid)
                        except:
                            pass
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

            if ('reprocessing_objects') in request.session:
                default_object = json.dumps(request.session['reprocessing_objects'])
                del request.session['reprocessing_objects']
                do_initialize = True

    # Create prefill form
    return render(request, 'prodtask/_requestform.html', {
        'active_app': 'mcprod',
        'pre_form_text': title,
        'form': form,
        'submit_url': submit_url,
        'url_args': rid,
        'parent_template': 'prodtask/_index.html',
        'default_step_values': default_step_values,
        'default_object':default_object,
        'do_initialize':do_initialize

    })


@login_required(login_url='/prodtask/login/')
def request_create_new_spds(request):
    return request_clone_or_create(request, None, 'Create MC Request', 'prodtask:request_create_new_spds',
                                   TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, mcfile_form_prefill,
                                   {'nEventsPerJob':'1000','priority':'880','maxAttempt':'25','maxFailure':'15'},'3.0')

@login_required(login_url='/prodtask/login/')
def request_create(request):
    return request_clone_or_create(request, None, 'Create MC Request', 'prodtask:request_create',
                                   TRequestMCCreateCloneForm, TRequestCreateCloneConfirmation, mcfile_form_prefill,
                                   {'nEventsPerJob':'1000','priority':'880','maxAttempt':'25','maxFailure':'15'},'2.0')


@login_required(login_url='/prodtask/login/')
def dpd_request_create(request):
    return request_clone_or_create(request, None, 'Create DPD Request', 'prodtask:dpd_request_create',
                                   TRequestDPDCreateCloneForm, TRequestCreateCloneConfirmation, dpd_form_prefill,
                                   {'nEventsPerJob':'5000','priority':'520','maxAttempt':'25','maxFailure':'15'})


@login_required(login_url='/prodtask/login/')
def hlt_request_create(request):
    return request_clone_or_create(request, None, 'Create HLT Request', 'prodtask:hlt_request_create',
                                   TRequestHLTCreateCloneForm, TRequestCreateCloneConfirmation, hlt_form_prefill,
                                   {'nEventsPerJob':'1000','priority':'970','maxAttempt':'25','maxFailure':'15'})


@login_required(login_url='/prodtask/login/')
def reprocessing_request_create(request):
    return request_clone_or_create(request, None, 'Create Reprocessing Request', 'prodtask:reprocessing_request_create',
                                   TRequestReprocessingCreateCloneForm, TRequestCreateCloneConfirmation,
                                   reprocessing_form_prefill,{'ram':'3800', 'projectmode':'lumiblock=yes;',
                                                              'nEventsPerJob':'1000','maxAttempt':'25','priority':'880','maxFailure':'15'})

@login_required(login_url='/prodtask/login/')
def eventindex_request_create(request):
    return request_clone_or_create(request, None, 'Create EventIndex Request', 'prodtask:eventindex_request_create',
                                   TRequestEventIndexCreateCloneForm, TRequestCreateCloneConfirmation,
                                   eventindex_form_prefill,{'nFilesPerJob':'50','maxAttempt':'15','priority':'880','maxFailure':'5'})

@csrf_protect
def do_mc_management_approve(request, reqid):
    return change_request_status(request, reqid,'registered',
                             'Request was approved for processing by %s' %request.user.username,'Request is registered by WebUI')


@csrf_protect
def do_mc_management_cancel(request, reqid):
    return change_request_status(request, reqid,'cancelled',
                                 'Request was cancelled  by %s' %request.user.username, 'Request is cancelled by WebUI')

@csrf_protect
def do_mc_management_hold(request, reqid):
    return change_request_status(request, reqid,'hold',
                                 'Request was cancelled  by %s' %request.user.username, 'Request is put on hold by WebUI')

@csrf_protect
def change_production_request_status(request, reqid, new_status):
    if new_status in ['not-taken','working','monitoring','finished','reworking','remonitoring','cancelled',
                      'test','registered','approved','processed','waiting', 'hold']:
        return change_request_status(request, reqid, new_status,
                                     'Request status was changed to %s by %s' %(new_status, request.user.username),
                                     'Request status is changed to %s by WebUI' % new_status)
    else:
         return HttpResponse(json.dumps({}), content_type='application/json')


def change_request_status(request, reqid, status, message, comment):
    results = {}
    if request.method == 'POST':
        try:
            set_request_status(request.user.username, reqid, status, message, comment,request)
            results = {'newStatus': status,'message':message }
        except Exception,e:
            _logger.error("Problem during request status change: %s" % str(e))
        return HttpResponse(json.dumps(results), content_type='application/json')


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
        #bSortable=False,
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

    request_created = datatables.Column(
        label='Created',
        iDataSort=0,
        #bSortable=False,
        sClass='centered',
    )

    request_approved = datatables.Column(
        label='Approved',
        #bSortable=False,
        sClass='centered',
    )

    priority = datatables.Column(
        label='Priority',
        bSortable=False,
        sClass='centered',
        bSearchable=False,
    )
    request_events = datatables.Column(
        label='Events',
        bSortable=False,
        sClass='centered',
        bSearchable=False,
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
    ref_link = datatables.Parameter(label='Link')
    #phys_group = datatables.Parameter(label='Physics group')

    def _phys_group_Q(value):
        if value == 'NOVALI':
            return Q( phys_group__iexact='VALI').__invert__()
        return Q( phys_group__iexact=value )

    phys_group = datatables.Parameter(label='Physics group', get_Q=_phys_group_Q )
    campaign = datatables.Parameter(label='Campaign')
    manager = datatables.Parameter(label='Manager')
    subcampaign = datatables.Parameter(label='SubCampaign')
    def _project_Q(value):
        return Q(project_id=value)
    project = datatables.Parameter(label='Project', get_Q=_project_Q)

    type = datatables.Parameter(label='Type', model_field='request_type')
    #status = datatables.Parameter(label='Status', model_field='cstatus')

    def _status_Q(value):
        if value == 'notest':
            return Q( cstatus__iexact='test').__invert__()
        if value == 'active':
            return Q( cstatus__in=['approved', 'monitoring', 'working','reworking', 'remonitoring','registered', 'waiting','hold'])
        if value == 'ended':
             return Q( cstatus__in=['finished', 'cancelled', 'done','processed'])
        return Q( cstatus__iexact=value )

    status = datatables.Parameter(label='Status', model_field='cstatus', get_Q=_status_Q)
    description = datatables.Parameter(label='Description')
    provenance = datatables.Parameter(label='Provenance')

    def _openended_Q(value):
        if value == 'Open ended':

            req_ids=[]
            open_ended_requests = OpenEndedRequest.objects.filter(status='open')

            for x in open_ended_requests:
                req_ids.append(int(x.request.reqid))

            return Q(reqid__in = req_ids)
        return Q()

    open_ended = datatables.Parameter(label='Open ended', model_field='reqid' ,get_Q=_openended_Q)


@csrf_protect
def check_extend_request(request, reqid):
    if request.method == 'POST':
        try:
            data = request.body
            excel_link = json.loads(data)
            _logger.debug(form_request_log(reqid,request,'Extend request with: %s' % str(excel_link)))
            spreadsheet_dict = fill_steptemplate_from_gsprd(excel_link)
            slices_number = len(spreadsheet_dict)
            steps_number = 0
            for current_slice in spreadsheet_dict:
                steps_number += len(current_slice.get('step_exec_dict'))
            results = {'success':True, 'slices_number':slices_number,'steps_number':steps_number, 'message': ''}
        except Exception,e:
            results = {'success':False, 'message': str(e)}
        return HttpResponse(json.dumps(results), content_type='application/json')


@csrf_protect
def extend_request(request, reqid):
    if request.method == 'POST':
        results = {'success':False, 'message': 'Not started'}
        try:
            data = request.body
            excel_link = json.loads(data)
            production_request = TRequest.objects.get(reqid=reqid)
            _logger.debug(form_request_log(reqid,request,'Extend request with: %s' % str(excel_link)))
            spreadsheet_dict = fill_steptemplate_from_gsprd(excel_link)
            if make_slices_from_dict(production_request, spreadsheet_dict):
                results = {'success':True, 'message': ''}
        except Exception,e:
            results = {'success':False, 'message': str(e)}
        return HttpResponse(json.dumps(results), content_type='application/json')





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


def sizeof_fmt(num):
    for unit in ['','K','M']:
        if abs(num) < 1000:
            return "%3.1f%s%s" % (num, unit)
        num /= 1000
    return "%.1f%s%s" % (num, 'B')


def change_campaign(production_request_id, newcampaign,newsubcampaign, file_name):
    production_request = TRequest.objects.get(reqid = production_request_id)
    current_campaign = production_request.campaign
    current_subcampaign = production_request.subcampaign
    tasks = ProductionTask.objects.filter(request=production_request)
    datasets = []
    for task in tasks:
        if task.status not in (ProductionTask.RED_STATUS+['obsolete']):
            datasets.append(task.output_dataset)
    #datasets = ['mc16_13TeV.411044.PowhegPythia8EvtGen_ttbar_169p00_SingleLep.recon.AOD.e6696_e5984_a875_r10201_tid14138065_00']
    ddm = DDM()
    changed_datasets = []
    for dataset in datasets:
        try:
            current_dataset_campaign = ddm.dataset_metadata(dataset)['campaign']
            new_dataset_campaign = current_dataset_campaign.replace(current_subcampaign,newsubcampaign).replace(current_campaign,newcampaign)
            if new_dataset_campaign!=current_dataset_campaign:
                ddm.changeDatasetCampaign(dataset,new_dataset_campaign)
            print dataset, new_dataset_campaign
            changed_datasets.append(dataset)
        except DataIdentifierNotFound, e:
            pass
        except Exception, e:
            print e
    with open(file_name, 'a') as output_file:
        output_file.writelines((x+'\n' for x in changed_datasets))
    production_request.campaign = newcampaign
    production_request.subcampaign = newsubcampaign
    production_request.save()