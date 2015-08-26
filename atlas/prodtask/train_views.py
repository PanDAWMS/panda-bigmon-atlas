import copy
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.core.urlresolvers import reverse
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.template.response import TemplateResponse
from django.db.models import Count
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.decorators import login_required
from django.db import transaction
import json
import logging
from django.db.models import Q
from rest_framework import serializers,generics
from django.forms.models import model_to_dict
from ..prodtask.views import form_existed_step_list, form_step_in_page
from ..prodtask.request_views import make_slices_from_dict
from ..prodtask.forms import ProductionTrainForm, pattern_from_request, TRequestDPDCreateCloneForm, \
    TRequestCreateCloneConfirmation, form_input_list_for_preview
from ..prodtask.models import TrainProductionLoad,TrainProduction,TRequest, InputRequestList, StepExecution, \
    RequestStatus

_logger = logging.getLogger('prodtaskwebui')


def prepare_train_carriages(train_id):
    """
    distribute train loads by carriages
    :param train_id: id of the train
    :return: dict {skim: [{dataset:{[outputs]:AOD,loads_id:[load_id]}}],noskim...}
    """

    loads = list(TrainProductionLoad.objects.filter(train=train_id))
    train_carriages = {}
    for load in loads:

        datasets = load.datasets.split('\n')
        outputs = load.output_formats.split('.')
        for dataset in datasets:
            if dataset in train_carriages:
                train_carriages[dataset]['outputs'] += [x for x in outputs if x not in train_carriages[dataset]['outputs']]
                train_carriages[dataset]['loads_id'].append(load.id)
                train_carriages[dataset]['groups'].add(load.group)
            else:
                group = set()
                group.add(load.group)
                train_carriages[dataset] = {'outputs':outputs,'loads_id':[load.id], 'groups': group}
    return_value =[]
    for dataset in train_carriages.keys():
        train_carriage = train_carriages[dataset]
        train_carriage.update({'dataset':dataset})
        return_value.append(train_carriage)
    return train_carriages


def prepare_simple_train_carriages(train_id):

    loads = list(TrainProductionLoad.objects.filter(train=train_id))
    train_carriages = []
    for load in loads:
        datasets = load.datasets.split('\n')
        outputs_slices = [x[1] for x in json.loads(load.outputs)]
        for dataset in datasets:
            for outputs in outputs_slices:
                group = set()
                group.add(load.group)
                train_carriages.append({'dataset':dataset,'outputs':'.'.join(outputs),'loads_id':[load.id], 'groups': group})
    return train_carriages

def assembled_train(request,train_id):
    if request.method == 'GET':
        try:
            train_carriages = prepare_simple_train_carriages(train_id)
            results = []
            carriage_number = 0
            for train_carriage in train_carriages:
                results.append({'dataset':train_carriage['dataset'],'carriage_number':carriage_number,
                                'outputs':train_carriage['outputs'],
                                   'group':list(train_carriage['groups'])})
                carriage_number += 1
            return HttpResponse(json.dumps(results), content_type='application/json')
        except Exception,e:
            _logger.error("Problem with carriage forming  %s" % e)


def trains_list(request):
    if request.method == 'GET':
        trains = TrainProduction.objects.filter(status='loading').order_by('-id')
        return render(request, 'prodtask/_trains_list.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Trains',
                'trains': [model_to_dict(x) for x in trains],
                'submit_url': 'prodtask:trains_list',
                'parent_template': 'prodtask/_index.html',
            })


def trains_list_full(request):
    if request.method == 'GET':
        trains = TrainProduction.objects.all().order_by('-id')
        return render(request, 'prodtask/_trains_list.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Trains',
                'trains': [model_to_dict(x) for x in trains],
                'submit_url': 'prodtask:trains_list',
                'parent_template': 'prodtask/_index.html',
            })

class TrainCarriageSerializer(serializers.ModelSerializer):

    class Meta:
        model = TrainProductionLoad
        fields = ('id', 'group', 'train', 'datasets', 'outputs', 'manager')
        read_only_fields = ('id',)


class TrainLoads(generics.ListCreateAPIView):
    queryset = TrainProductionLoad.objects.all()
    serializer_class = TrainCarriageSerializer


class TrainLoad(generics.RetrieveUpdateDestroyAPIView):
    queryset = TrainProductionLoad.objects.all()
    serializer_class = TrainCarriageSerializer

@login_required(login_url='/prodtask/login/')
def train_create(request):
    if request.method == 'POST':
        try:
            form = ProductionTrainForm(request.POST)  # A form bound to the POST data
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
        if form.is_valid():
            # Process the data in form.cleaned_data

            _logger.debug('Create train: %')
            try:
                del form.cleaned_data['pattern_request_id']
                train = TrainProduction(**form.cleaned_data)
                train.status = 'loading'
                train.save()
                return HttpResponseRedirect(reverse('prodtask:train_edit', args=(train.id,)))  # Redirect after POST
            except Exception,e :
                 _logger.error("Problem with train creation  %s"% e)
    else:
        try:
            manager = request.user.username
            form = ProductionTrainForm({'manager':manager})
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    return render(request, 'prodtask/_train_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Create train',
        'form': form,
        'submit_url': 'prodtask:train_create',
        'parent_template': 'prodtask/_index.html',
    })



@csrf_protect
def check_slices_for_trains(request):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            step_number = int(input_dict['step_number'])
            production_request = input_dict['production_request']
            train_id = input_dict['train_id']
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = map(int,slices)
            ordered_slices.sort()
            req = TRequest.objects.get(reqid=int(production_request))
            not_approved = []
            parent_steps = []
            for slice_number in ordered_slices:
                input_list = InputRequestList.objects.get(request=req,slice=slice_number)
                existed_steps = StepExecution.objects.filter(request=req, slice=input_list)
                # Check steps which already exist in slice, and change them if needed
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
                if req.request_type == 'MC':
                    step_as_in_page = form_step_in_page(ordered_existed_steps,StepExecution.STEPS, None)
                    if 'Fullsim' not in input_list.comment:
                        step_number = 8
                    else:
                        step_number = 5
                else:
                    step_as_in_page = form_step_in_page(ordered_existed_steps,['']*len(StepExecution.STEPS),existed_foreign_step)
                if step_as_in_page[step_number]:
                    if step_as_in_page[step_number].status != 'Approved':
                        not_approved.append(slice_number)
                    else:
                        parent_steps.append(int(step_as_in_page[step_number].id))
                else:
                    not_approved.append(slice_number)
            if not not_approved:
                train_extension={'parent_request_slices':ordered_slices,'parent_steps':parent_steps,'train_id':int(train_id)}
                request.session['train_extension'] = train_extension

                results = {'success':True}
            else:
                results = {'success':False,
                           'message': 'All steps should be approved, problem with slices: %s'%str(not_approved)}
        except Exception,e:
            results = {'success':False,'message': str(e)}
    return HttpResponse(json.dumps(results), content_type='application/json')


@csrf_protect
def create_request_as_child(request):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            parent_request = TRequest.objects.get(reqid=input_dict['parent_request'])
            new_request = TRequest()
            new_request.campaign = parent_request.project
            new_request.project = parent_request.project
            new_request.description = input_dict['short_description']
            new_request.phys_group = input_dict['phys_group']
            new_request.provenance = 'GP'
            new_request.request_type = 'GROUP'
            new_request.energy_gev = parent_request.energy_gev
            new_request.manager = input_dict['manager']
            new_request.cstatus = 'waiting'
            new_request.save()
            request_status = RequestStatus(request=new_request,comment='Request created as child train WebUI',owner=input_dict['manager'],
                                                       status='waiting')

            request_status.save_with_current_time()
            parent_steps = input_dict['parent_steps']
            pattern_request = int(input_dict['pattern_request'])
            spreadsheet_dict = []
            outputs = input_dict['outputs']
            outputs_slices = json.loads(outputs)
            pattern_slices = set()
            for output_slices in outputs_slices:
                pattern_slices.add(output_slices[0])
            step_pattern = {}
            for pattern_slice_number in pattern_slices:
                pattern_slice = InputRequestList.objects.get(request=pattern_request,slice=int(pattern_slice_number))
                step_pattern[pattern_slice_number] = StepExecution.objects.filter(slice=pattern_slice)[0]
            slice_index = 0
            for parent_step_id in parent_steps:
                parent_step = StepExecution.objects.get(id=int(parent_step_id))
                for output_slice in outputs_slices:
                    current_step_pattern = step_pattern[output_slice[0]]
                    current_output_formats = []
                    for output in output_slice[1]:
                        if output in current_step_pattern.step_template.output_formats.split('.'):
                            current_output_formats.append(output)
                    if current_output_formats:
                        st_sexec_list = []
                        irl = dict(slice=slice_index, brief=current_step_pattern.slice.brief, comment=current_step_pattern.slice.comment, dataset=parent_step.slice.dataset_id,
                                   input_data=parent_step.slice.input_data,
                                   project_mode=current_step_pattern.slice.project_mode,
                                   priority=int(current_step_pattern.slice.priority),
                                   input_events=-1)
                        slice_index += 1
                        sexec = dict(status='NotChecked', priority=int(current_step_pattern.priority),
                                     input_events=-1)
                        task_config =  current_step_pattern.get_task_config()
                        nEventsPerJob = task_config.get('nEventsPerJob','')
                        task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
                        st_sexec_list.append({'step_name': current_step_pattern.step_template.step, 'tag': current_step_pattern.step_template.ctag
                                                 , 'step_exec': sexec,
                                          'memory': int(current_step_pattern.step_template.memory),
                                          'formats': '.'.join(current_output_formats),
                                          'task_config':task_config,'parent_step_id':parent_step_id})
                        spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
            make_slices_from_dict(new_request,spreadsheet_dict)
            results = {'success':True, 'request':new_request.reqid}

        except Exception,e:
            results = {'success':False, 'message':str(e)}
            return HttpResponse(json.dumps(results), status=500, content_type='application/json')
        return HttpResponse(json.dumps(results), content_type='application/json')

@login_required(login_url='/prodtask/login/')
def train_as_child(request, reqid):
    if 'train_extension' not in request.session:
        return HttpResponseRedirect(reverse('prodtask:input_list_approve_full', args=[reqid]))
    else:
        try:
            train_id = request.session['train_extension']['train_id']
            train = TrainProduction.objects.get(id=train_id)
            parent_request_slices = request.session['train_extension']['parent_request_slices']
            parent_steps = request.session['train_extension']['parent_steps']
            del request.session['train_extension']
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))

        return render(request, 'prodtask/_train_as_child.html', {
            'active_app': 'prodtask',
            'pre_form_text': "DPD train create",
            'url_args': reqid,
            'train': train,
            'parent_template': 'prodtask/_index.html',
            'parent_request': reqid,
            'parent_request_slices': parent_request_slices,
            'parent_steps':parent_steps,
            'pattern_request':train.pattern_request_id,
            'groups':[x[0] for x in TRequest.PHYS_GROUPS]


        })

@csrf_protect
def create_request_from_train(request,train_id):
    if request.method == 'GET':
        #if request.user.is_superuser or (request.user.username == 'nozturk') or (request.user.username == 'egramsta'):
        if True:
            train = TrainProduction.objects.get(id=train_id)
            loads = list(TrainProductionLoad.objects.filter(train=train_id))
            pattern_slices = set()
            for load in loads:
                outputs_slices = json.loads(load.outputs)
                for output_slices in outputs_slices:
                    pattern_slices.add(output_slices[0])
            step_pattern = {}
            for  pattern_slice_number in pattern_slices:
                pattern_slice = InputRequestList.objects.get(request=train.pattern_request,slice=int(pattern_slice_number))
                step_pattern[pattern_slice_number] = StepExecution.objects.filter(slice=pattern_slice)[0]
            slice_index = 0
            spreadsheet_dict = []
            for load in loads:
                datasets = load.datasets.split('\n')
                if 'bad_value' in datasets:
                    datasets = []
                outputs_slices = json.loads(load.outputs)
                for dataset in datasets:
                    for output_slice in outputs_slices:
                        current_parent_step = step_pattern[output_slice[0]]
                        current_output_formats = []
                        for output in output_slice[1]:
                            if output in current_parent_step.step_template.output_formats.split('.'):
                                current_output_formats.append(output)
                        if current_output_formats:
                            st_sexec_list = []
                            irl = dict(slice=slice_index, brief=current_parent_step.slice.brief, comment=current_parent_step.slice.comment, dataset=dataset,
                                       input_data='',
                                       project_mode=current_parent_step.slice.project_mode,
                                       priority=int(current_parent_step.slice.priority),
                                       input_events=-1)
                            slice_index += 1
                            sexec = dict(status='NotChecked', priority=int(current_parent_step.priority),
                                         input_events=-1)
                            task_config =  current_parent_step.get_task_config()
                            nEventsPerJob = task_config.get('nEventsPerJob','')
                            task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
                            st_sexec_list.append({'step_name': current_parent_step.step_template.step, 'tag': current_parent_step.step_template.ctag
                                                     , 'step_exec': sexec,
                                              'memory': int(current_parent_step.step_template.memory),
                                              'formats': '.'.join(current_output_formats),
                                              'task_config':task_config})
                            spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
                        
            form_data = {}
            form_data['request_type'] = 'GROUP'
            form_data['phys_group'] = 'PHYS'
            form_data['manager'] = 'atlas-dpd-production'
            form_data['energy_gev'] = train.pattern_request.energy_gev
            form_data['campaign'] = train.pattern_request.campaign
            form_data['project'] = train.pattern_request.project
            form_data['provenance'] = 'GP'
            form = TRequestCreateCloneConfirmation(form_data)
            inputlists = form_input_list_for_preview(spreadsheet_dict)
            # store data from prefill form to http request
            request.session['file_dict'] = spreadsheet_dict
            request.session['close_train'] = train_id
            return render(request, 'prodtask/_previewreq.html', {
                'active_app': 'mcprod',
                'pre_form_text': 'Create DPD Request',
                'form': form,
                'submit_url': 'prodtask:dpd_request_create',
                'url_args': None,
                'parent_template': 'prodtask/_index.html',
                'inputLists': inputlists,
                'bigSliceNumber': False
            })

@login_required(login_url='/prodtask/login/')
def train_edit(request, train_id):
    try:
        train = TrainProduction.objects.get(id=train_id)
        depart_date = train.departure_time.strftime('%Y-%m-%d')
        allow_assemble = True
    except:
        return HttpResponseRedirect(reverse('prodtask:request_table'))

    return render(request, 'prodtask/_train_creation.html', {
        'active_app': 'prodtask',
        'pre_form_text': "DPD train modification",
        'url_args': train_id,
        'train': train,
        'parent_template': 'prodtask/_index.html',
        'depart_date':depart_date,
        'groups':[x[0] for x in TRequest.PHYS_GROUPS],
        'allow_assemble':allow_assemble


    })


@login_required(login_url='/prodtask/login/')
def close_train(request, train_id):
    try:
        train = TrainProduction.objects.get(id=train_id)
        train.status = 'Closed'
        train.save()
        return train_edit(request, train_id)
    except:
        pass
    return HttpResponseRedirect(reverse('prodtask:request_table'))





def get_pattern_from_request(request,reqid):
    results = {}
    try:
       results =  pattern_from_request(reqid)
    except Exception,e:
            pass
    return HttpResponse(json.dumps(results), content_type='application/json')