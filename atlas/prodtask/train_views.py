import json
import logging
from dataclasses import asdict

from datetime import timedelta
from django.utils import timezone
from django.urls import reverse
from django.http import HttpResponse, HttpResponseRedirect, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.decorators import login_required
from rest_framework import serializers,generics
from django.forms.models import model_to_dict
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework import status
from atlas.prodtask.models import RequestStatus, HashTag, HashTagToRequest, SystemParametersHandler
from atlas.prodtask.views import create_steps_in_child_pattern, set_request_status, request_clone_slices, clone_slices
from atlas.prodtask.spdstodb import fill_template
from .hashtag import _set_request_hashtag
from .helper import form_json_request_dict
from ..prodtask.views import form_existed_step_list, form_step_in_page, create_request_for_pattern
from ..prodtask.forms import ProductionTrainForm, pattern_from_request, TRequestCreateCloneConfirmation, form_input_list_for_preview
from ..prodtask.models import TrainProductionLoad,TrainProduction,TRequest, InputRequestList, StepExecution, \
    ParentToChildRequest
from django.template import RequestContext
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated

from ..settings import OIDC_LOGIN_URL

_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')




def merge_pattern_train(train, requests):
    def form_output_lookup(outputs):
        result_dict = {}
        for slice in outputs:
            for output in slice[1]:
                result_dict[output] = slice[0]
        return result_dict
    outputs = train.output_by_slice
    outputs_lookup = form_output_lookup(outputs)
    dataset_slice = {}
    merge_requests = set()
    merged = 0
    for production_request in requests:
        slices = list(InputRequestList.objects.filter(request=production_request).order_by('id'))
        steps = list(StepExecution.objects.filter(request=production_request).order_by('slice'))
        if len(slices)!=len(steps):
            continue
        for index, step in enumerate(steps):
            dataset = slices[index].dataset
            current_dataset_outputs = dataset_slice.get(dataset,{})
            outputs = step.step_template.output_formats.split('.')
            if outputs[0] not in outputs_lookup:
                pass
            else:
                pattern_slice = outputs_lookup[outputs[0]]
                if pattern_slice not in current_dataset_outputs:
                    current_dataset_outputs[pattern_slice] = {'outputs':outputs,'count':1, 'requests':[production_request], 'slices':[slices[index].slice]}
                else:
                    new_outputs = [x for x in outputs if x not in current_dataset_outputs[pattern_slice]['outputs']]
                    if new_outputs:
                        merged += 1
                        current_dataset_outputs[pattern_slice]['count'] += 1
                        current_dataset_outputs[pattern_slice]['outputs'] = current_dataset_outputs[pattern_slice]['outputs'] + new_outputs
                        current_dataset_outputs[pattern_slice]['requests'].append(production_request)
                        current_dataset_outputs[pattern_slice]['slices'].append(slices[index].slice)
                        merge_requests.update(current_dataset_outputs[pattern_slice]['requests'])
                dataset_slice[dataset] = current_dataset_outputs
    return dataset_slice, merged, list(merge_requests)


def do_merge_requests(train_id, requests):
        train = TrainProduction.objects.get(id=train_id)
        dataset_slices, merged, merge_requests = merge_pattern_train(train,requests)
        dataset_to_merge = {}
        slices_to_skip = []
        for dataset in dataset_slices:
            for pattern_slice in dataset_slices[dataset]:
                if dataset_slices[dataset][pattern_slice]['count']>1:
                    dataset_to_merge.update({str(dataset_slices[dataset][pattern_slice]['requests'][0])+','+str(dataset_slices[dataset][pattern_slice]['slices'][0]):dataset_slices[dataset][pattern_slice]['outputs']})
                    for index, request_to_skip in enumerate(dataset_slices[dataset][pattern_slice]['requests'][1:],1):
                        slices_to_skip.append(str(request_to_skip)+','+str(dataset_slices[dataset][pattern_slice]['slices'][index]))
        for request in requests:
            if request not in merge_requests:
                print(request,'approved')
                set_request_status('cron',request,'working','Automatic merged approve', 'Request was approved after merging')
                set_request_status('cron',request,'approved','Automatic merged approve', 'Request was approved during merging')
        if len(merge_requests)>0:
            base_request = TRequest.objects.get(reqid=merge_requests[0])
            merged_request = request_clone_slices(base_request.reqid, base_request.manager, 'Merged request for pattern %s'%str(train.pattern_request_id), base_request.ref_link,  [], base_request.project, False)
            for request in merge_requests:
                slices = list(InputRequestList.objects.filter(request=request))
                for slice in slices:
                    slice_index = str(request)+','+str(slice.slice)
                    if slice_index not in slices_to_skip:
                        new_slice = clone_slices(request,merged_request,[slice.slice],0,False)
                        step = StepExecution.objects.filter(slice=InputRequestList.objects.get(request=merged_request,slice=new_slice[0]))[0]
                        if slice_index in list(dataset_to_merge.keys()):
                            step.step_template = fill_template(step.step_template.step,step.step_template.ctag,
                                                               step.step_template.priority,
                                                               '.'.join(dataset_to_merge[slice_index]))
                        step.status = 'Approved'
                        step.save()
                set_request_status('cron',request,'merged','Automatic merging', 'Request was merged to %s'%(str(merged_request)))
                relationship = ParentToChildRequest()
                relationship.status = 'active'
                relationship.parent_request = TRequest.objects.get(reqid=request)
                relationship.child_request = TRequest.objects.get(reqid=merged_request)
                relationship.train = None
                relationship.relation_type = 'MR'
                relationship.save()
                print(request,'merged', merged_request)
            set_request_status('cron',merged_request,'working','Automatic merged approve', 'Request was approved after merging')
            set_request_status('cron',merged_request,'approved','Automatic merged approve', 'Request was approved after merging')


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def merge_trains(request):
    data = request.data
    to_send = {}
    try:
        for train_id in data:
            train = TrainProduction.objects.get(id=train_id)
            dataset_slice, merged, temp = merge_pattern_train(train,data[train_id])
            to_send[train_id]  = dataset_slice
    except Exception as e:
        Response({"error": str(e)},status=status.HTTP_400_BAD_REQUEST)
    return Response({"load": to_send})

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def trains_to_merge(request):
    to_send = {}
    try:
        result = collect_trains(14)
        for entry in list(result.values()):
            to_send[int(entry['train'].id)] = {'train_name':entry['train'].description,'request':list(map(int,entry['requests'])),'request_count':len(entry['requests'])}
    except Exception as e:
        return Response({"error": str(e)},status=status.HTTP_400_BAD_REQUEST)
    return Response({"trains": to_send})



@login_required(login_url=OIDC_LOGIN_URL)
def train_luanch(request):
    if request.method == 'GET':
        return render(request, 'prodtask/_train_merge.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Trains',
                'submit_url': 'prodtask:train_luanch',
                'parent_template': 'prodtask/_index.html',
            })


def patterns_to_show():
    requests = []
    if HashTag.objects.filter(hashtag__iexact='PatternToShowMerge').exists():
        patterns = list(HashTagToRequest.objects.filter(hashtag=HashTag.objects.filter(hashtag__iexact='PatternToShowMerge')[0]).values_list('request_id', flat=True))
        for pattern in patterns:
            trains = list(TrainProduction.objects.filter(pattern_request_id=pattern))
            requests += [x.request for x in trains if x.request]
    return requests


def collect_trains(days):
    min_request = RequestStatus.objects.filter(status='waiting', timestamp__gt=timezone.now()-timedelta(days=days)).order_by('id')[0].request_id
    requests = list(TRequest.objects.filter(request_type='GROUP',reqid__gte=min_request, cstatus='registered'))
    requests += list(TRequest.objects.filter(reqid__in=patterns_to_show()))
    patterns = {}
    for production_request in requests:
        if TrainProduction.objects.filter(request = int(production_request.reqid)).exists():
            train = TrainProduction.objects.filter(request = int(production_request.reqid))[0]
            if int(train.pattern_request_id) in patterns:
                patterns[int(train.pattern_request_id)].update({'requests':patterns[int(train.pattern_request_id)].get('requests',[])+[train.request]})
            else:
                patterns[int(train.pattern_request_id)] = {'train':train,
                                                       'requests':[train.request]}

    return patterns
    # result = {}
    # merged = 0
    # total_merged = 0
    # for pattern in patterns.values():
    #     result[pattern['train'].id], merged = merge_pattern_train(pattern['train'],pattern['requests'])
    #     total_merged += merged
    # print  merged


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
    for dataset in list(train_carriages.keys()):
        train_carriage = train_carriages[dataset]
        train_carriage.update({'dataset':dataset})
        return_value.append(train_carriage)
    return train_carriages

def prepare_merged_train_carriages(train_id):
    loads = list(TrainProductionLoad.objects.filter(train=train_id))
    train = TrainProduction.objects.get(id=train_id)
    train_carriages = []
    pattern_slices = set()
    for load in loads:
        outputs_slices = json.loads(load.outputs)
        for output_slices in outputs_slices:
            pattern_slices.add(output_slices[0])
    step_pattern_formats = {}
    for pattern_slice_number in pattern_slices:
        pattern_slice = InputRequestList.objects.get(request=train.pattern_request,slice=int(pattern_slice_number))
        step_pattern_formats[pattern_slice_number] = StepExecution.objects.filter(slice=pattern_slice)[0].step_template.output_formats.split('.')
    merged_dict = {}
    for load in loads:
        datasets = load.datasets.split('\n')
        if 'bad_value' in datasets:
            datasets = []
        outputs_slices = json.loads(load.outputs)
        for dataset in datasets:
            for output_slice in outputs_slices:
                current_output_formats = []
                slice_number = output_slice[0]
                for output in output_slice[1]:
                    if output in step_pattern_formats[slice_number]:
                        current_output_formats.append(output)
                if (dataset,slice_number) not in merged_dict:
                    merged_dict.update({(dataset,slice_number):current_output_formats})
                else:
                    previous_formats = merged_dict[(dataset,slice_number)]
                    merged_dict.update({(dataset,slice_number):list(set(previous_formats+current_output_formats))})
    return merged_dict


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

@login_required(login_url=OIDC_LOGIN_URL)
def assembled_train(request,train_id):
    if request.method == 'GET':
        try:
            if TrainProduction.objects.get(id=train_id).pattern_request.cstatus == 'cancelled':
                raise ValueError('Pattern request was cancelled')
            train_carriages = prepare_simple_train_carriages(train_id)
            if not train_carriages:
                raise ValueError('No load to create request')
            results = []
            carriage_number = 0
            for train_carriage in train_carriages:
                results.append({'dataset':train_carriage['dataset'],'carriage_number':carriage_number,
                                'outputs':train_carriage['outputs'],
                                   'group':list(train_carriage['groups'])})
                carriage_number += 1
            return HttpResponse(json.dumps(results), content_type='application/json')
        except Exception as e:
            _logger.error("Problem with carriage forming  %s" % str(e))
            return HttpResponse(str(e), status=status.HTTP_400_BAD_REQUEST)


@login_required(login_url=OIDC_LOGIN_URL)
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


@login_required(login_url=OIDC_LOGIN_URL)
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





class TrainLoadByTrain(generics.ListCreateAPIView):
    queryset = TrainProductionLoad.objects.all()
    serializer_class = TrainCarriageSerializer
    authentication_classes = [TokenAuthentication, SessionAuthentication, BasicAuthentication]
    permission_classes = [IsAuthenticated]

    def list(self, request, train):
        # Note the use of `get_queryset()` instead of `self.queryset`
        queryset = TrainProductionLoad.objects.filter(train=train)
        serializer = TrainCarriageSerializer(queryset, many=True)
        return Response(serializer.data)


class TrainLoads(generics.ListCreateAPIView):
    queryset = TrainProductionLoad.objects.all()
    serializer_class = TrainCarriageSerializer

    authentication_classes = [TokenAuthentication, SessionAuthentication, BasicAuthentication]
    permission_classes = [IsAuthenticated]


class TrainLoad(generics.RetrieveUpdateDestroyAPIView):
    queryset = TrainProductionLoad.objects.all()
    serializer_class = TrainCarriageSerializer

    authentication_classes = [TokenAuthentication, SessionAuthentication, BasicAuthentication]
    permission_classes = [IsAuthenticated]

@login_required(login_url=OIDC_LOGIN_URL)
def train_create(request):
    if request.method == 'POST':
        try:
            form = ProductionTrainForm(request.POST)  # A form bound to the POST data
        except Exception as e:
            _logger.error("Problem with train creation  %s" % e)
            return HttpResponseRedirect(reverse('prodtask:request_table'))
        if form.is_valid():
            # Process the data in form.cleaned_data

            _logger.debug('Create train: %')
            try:
                del form.cleaned_data['pattern_request_id']
                train = TrainProduction(**form.cleaned_data)
                train.status = 'loading'
                outputs = train.outputs
                if len(outputs)>2000:
                    train.outputs = 'Error'
                train.save()
                if len(outputs)>2000:
                    train.outputs = outputs
                    train.save()
                return HttpResponseRedirect(reverse('prodtask:train_edit', args=(train.id,)))  # Redirect after POST
            except Exception as e :
                 _logger.error("Problem with train creation  %s"% e)
    else:
        try:
            manager = request.user.username
            form = ProductionTrainForm({'manager':manager})
        except Exception as e:
            _logger.error("Problem with train creation  %s" % e)
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    return render(request, 'prodtask/_train_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Create train',
        'form': form,
        'submit_url': 'prodtask:train_create',
        'parent_template': 'prodtask/_index.html',
    })


def create_pattern_train(pattern_request_id, pattern_type='MC'):

    pattern_train = TrainProduction()
    pattern_train.pattern_request = TRequest.objects.get(reqid=pattern_request_id)
    outputs = json.dumps(pattern_from_request(pattern_train.pattern_request))
    pattern_train.outputs = outputs
    if len(outputs) > 2000:
        pattern_train.outputs = 'Error'
    pattern_train.status = pattern_type
    pattern_train.departure_time = timezone.now()
    pattern_train.description = pattern_train.pattern_request.description
    pattern_train.manager = 'mborodin'
    pattern_train.save()
    if len(outputs) > 2000:
        pattern_train.outputs = outputs
        pattern_train.save()
    return pattern_train.id


@csrf_protect
def check_slices_for_trains(request):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            slices = input_dict['slices']
            step_number = int(input_dict['step_number'])
            is_mc = False
            if step_number == -1:
                is_mc = True
            production_request = input_dict['production_request']
            train_id = input_dict['train_id']
            if '-1' in slices:
                del slices[slices.index('-1')]
            ordered_slices = list(map(int,slices))
            ordered_slices.sort()
            req = TRequest.objects.get(reqid=int(production_request))
            not_approved = []
            parent_steps = []
            for slice_number in ordered_slices:
                input_list = InputRequestList.objects.get(request=req,slice=slice_number)
                existed_steps = StepExecution.objects.filter(request=req, slice=input_list)
                # Check steps which already exist in slice, and change them if needed
                ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
                if is_mc:
                    step_as_in_page = form_step_in_page(ordered_existed_steps,StepExecution.STEPS, None)
                    if 'fullsim' not in input_list.comment.lower():
                        step_number = 8
                        if not step_as_in_page[step_number]:
                            step_number = 6
                    else:
                        step_number = 6
                else:
                    if req.request_type == 'MC':
                        step_as_in_page = form_step_in_page(ordered_existed_steps, StepExecution.STEPS, None)
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
        except Exception as e:
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
            new_request = create_request_for_pattern(parent_request.reqid, input_dict['short_description'], input_dict['manager'])
            parent_steps = []
            for step_id in  input_dict['parent_steps']:
                parent_steps.append(StepExecution.objects.get(id=step_id))
            create_steps_in_child_pattern(new_request, parent_steps, int(input_dict['pattern_request']), json.loads(input_dict['outputs']))
            relationship = ParentToChildRequest()
            relationship.status = 'active'
            relationship.parent_request = parent_request
            relationship.child_request = new_request
            relationship.train = TrainProduction.objects.get(id=int(input_dict['train_id']))
            relationship.relation_type = 'MA'
            relationship.save()
            results = {'success':True, 'request':new_request.reqid}

        except Exception as e:
            results = {'success':False, 'message':str(e)}
            return HttpResponse(json.dumps(results), status=500, content_type='application/json')
        return HttpResponse(json.dumps(results), content_type='application/json')



def find_pattern_derivation_request(campaign: str, subcampaign: str) -> (int, [str]):
    all_patterns = SystemParametersHandler.get_daod_phys_production()
    for pattern in all_patterns:
        if pattern.campaign == campaign and ( pattern.subcampaign == SystemParametersHandler.DAOD_PHYS_Production.ALL_SUBCAMPAIGNS
                                              or pattern.subcampaign == subcampaign) and pattern.status == SystemParametersHandler.DAOD_PHYS_Production.STATUS.ACTIVE:
            return pattern.train_id, pattern.outputs, pattern.fullSimOnly
    raise Exception('Pattern derivation request not found for campaign %s and subcampaign %s'%(campaign, subcampaign))

def find_pattern_outputs(pattern_request_id: int, outputs: [str]):
    pattern_train = TrainProduction.objects.get(id=pattern_request_id)
    pattern_outputs = json.loads(pattern_train.outputs)
    chosen_slices = []
    for output_slice in pattern_outputs:
        if [x for x in outputs if x in output_slice[1]]:
            chosen_slices.append(output_slice)
    return chosen_slices, pattern_train.pattern_request_id

def find_steps_for_derivation(mc_request_id: int, full_sim_only = False) -> [StepExecution]:
    parent_steps = []
    ordered_slices = InputRequestList.objects.filter(request=mc_request_id).order_by('slice')
    for slice in ordered_slices:
        if not slice.is_hide:
            if full_sim_only and 'fullsim' not in slice.comment.lower():
                continue
            existed_steps = StepExecution.objects.filter(request=mc_request_id, slice=slice)
            ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            step_as_in_page = form_step_in_page(ordered_existed_steps, StepExecution.STEPS, None)
            AOD_input = False
            if existed_foreign_step:
                AOD_input = 'AOD' in existed_foreign_step.step_template.output_formats
            for step in step_as_in_page:
                if step:
                    if step.status == 'Approved' and ((step.get_task_config('input_format')=='AOD' or AOD_input) and
                        step.step_template.output_formats == 'AOD'):
                        parent_steps.append(step)
                    AOD_input = 'AOD' in step.step_template.output_formats
    return parent_steps


def filter_steps_for_derivation(parent_steps: [StepExecution], new_request: TRequest, full_sim_only: bool = False)-> [StepExecution]:
    new_request_slices = list(InputRequestList.objects.filter(request=new_request).order_by('slice'))
    existed_parent_steps = []
    for slice in new_request_slices:
        if not slice.is_hide:
            existed_steps = StepExecution.objects.filter(request=new_request, slice=slice)
            ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            if full_sim_only and 'fullsim' not in slice.comment.lower():
                    existed_foreign_step = None
            if existed_foreign_step:
                existed_parent_steps.append(existed_foreign_step)
    return [x for x in parent_steps if x not in existed_parent_steps]



def submit_derivation_steps(new_request: TRequest) -> bool:
    new_request_slices = list(InputRequestList.objects.filter(request=new_request).order_by('slice'))
    approve_request = False
    for slice in new_request_slices:
        if not slice.is_hide:
            existed_steps = StepExecution.objects.filter(request=new_request, slice=slice)
            ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
            if existed_foreign_step and not existed_foreign_step.broken_step:
                for step in ordered_existed_steps:
                    if step.status == StepExecution.STATUS.NOT_CHECKED:
                        step.status = StepExecution.STATUS.APPROVED
                        step.save()
                        approve_request = True
    if approve_request:
        set_request_status('cron', new_request.reqid, 'approved', 'Automatic child derivation approve',
                           'Request was automatically approved')
        return True
    return False

@csrf_protect
def submit_child_derivation(request, reqid):
    if request.method == 'POST':
        try:
            new_request_id = submit_child_derivation_request(reqid)
            results = {'success':True, 'request_id':new_request_id}
            _jsonLogger.info(f'Finish creating child derivaiton MC request {new_request_id}', extra=form_json_request_dict(reqid,request))
        except Exception as e:
            _jsonLogger.info(f'Error during MC request submission {e}', extra=form_json_request_dict(reqid,request))
            _logger.error(f'Error during MC request submission {e}')
            return HttpResponseBadRequest(e)
        return HttpResponse(json.dumps(results), content_type='application/json')

def submit_child_derivation_request(original_request_id: int) -> int:
    mc_request = TRequest.objects.get(reqid=original_request_id)
    pattern_derivation_request, outputs, full_sim_only = find_pattern_derivation_request(mc_request.campaign, mc_request.subcampaign)
    parent_steps = find_steps_for_derivation(mc_request.reqid, full_sim_only)
    pattern_outputs, pattern_derivation_request = find_pattern_outputs(pattern_derivation_request, outputs)
    if not ParentToChildRequest.objects.filter(parent_request=mc_request, relation_type='DP').exists():
        new_description = 'PHYS Derivation of %s'%mc_request.description
        new_request = create_request_for_pattern(mc_request.reqid, new_description, mc_request.manager)
        new_request.project = mc_request.project
        new_request.campaign = mc_request.campaign
        new_request.subcampaign = mc_request.subcampaign
        new_request.save()
        new_parent_child = ParentToChildRequest()
        new_parent_child.parent_request = TRequest.objects.get(reqid=original_request_id)
        new_parent_child.child_request = new_request
        new_parent_child.relation_type = 'DP'
        new_parent_child.status = 'active'
        new_parent_child.save()
        _set_request_hashtag(new_request.reqid, 'PHYSAutoProduction')
    else:
        new_request = ParentToChildRequest.objects.get(parent_request=mc_request, relation_type='DP').child_request
        parent_steps = filter_steps_for_derivation(parent_steps, new_request, full_sim_only)
    if parent_steps:
        create_steps_in_child_pattern(new_request, parent_steps,
                                      pattern_derivation_request,
                                      pattern_outputs)
    submit_derivation_steps(new_request)
    return new_request.reqid
@login_required(login_url=OIDC_LOGIN_URL)
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
            'pattern_description':train.pattern_request.description,
            'groups':[x[0] for x in TRequest.PHYS_GROUPS]


        })


def change_ram_request(production_request_id,new_ram_value):
    steps  = StepExecution.objects.filter(request=production_request_id)
    for step in steps:
        if step.get_task_config('project_mode'):
            new_project_modes = []
            new_project_modes.append('ramcount=%i'%new_ram_value)
            for token in step.get_task_config('project_mode').split(';'):
                if 'ramcount' not in token:
                   new_project_modes.append(token)
            step.set_task_config({'project_mode':';'.join(new_project_modes)})
        step.save()



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
                step_pattern[pattern_slice_number] = list(StepExecution.objects.filter(slice=pattern_slice).order_by('id'))
            slice_index = 0
            spreadsheet_dict = []
            merged_trains = prepare_merged_train_carriages(train_id)
            for slice_index, dataset_slice in enumerate(merged_trains):
                dataset = dataset_slice[0]
                current_parent_step = step_pattern[dataset_slice[1]][0]
                current_output_formats = merged_trains[dataset_slice]
                if current_output_formats:
                            st_sexec_list = []
                            irl = dict(slice=slice_index, brief=current_parent_step.slice.brief, comment=current_parent_step.slice.comment, dataset=dataset.strip(),
                                       input_data='',
                                       project_mode=current_parent_step.slice.project_mode,
                                       priority=int(current_parent_step.slice.priority),
                                       input_events=-1)
                            slice_index += 1
                            for step_index,current_parent_step in enumerate(step_pattern[dataset_slice[1]]):
                                sexec = dict(status='NotChecked', priority=int(current_parent_step.priority),
                                             input_events=-1)
                                task_config =  current_parent_step.get_task_config()
                                nEventsPerJob = task_config.get('nEventsPerJob','')
                                task_config.update({'nEventsPerJob':dict((step,nEventsPerJob) for step in StepExecution.STEPS)})
                                if step_index>0:
                                    step_parent = '%i_%i'%(slice_index,step_index-1)
                                else:
                                    step_parent = '%i_%i' % (slice_index, step_index )
                                st_sexec_list.append({'step_name': current_parent_step.step_template.step, 'tag': current_parent_step.step_template.ctag
                                                         , 'step_exec': sexec,
                                                  'memory': int(current_parent_step.step_template.memory),
                                                  'formats': '.'.join(current_output_formats),
                                                  'task_config':task_config,'step_order':'%i_%i' % (slice_index, step_index ),'step_parent':step_parent})
                            spreadsheet_dict.append({'input_dict': irl, 'step_exec_dict': st_sexec_list})
                        
            form_data = {}
            form_data['request_type'] = 'GROUP'
            form_data['phys_group'] = 'PHYS'
            form_data['manager'] = 'atlas-phys-dpd-production'
            form_data['energy_gev'] = train.pattern_request.energy_gev
            form_data['campaign'] = train.pattern_request.campaign
            form_data['project'] = train.pattern_request.project
            form_data['provenance'] = 'GP'
            form_data['cc'] = 'egramsta@cern.ch'
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

@login_required(login_url=OIDC_LOGIN_URL)
def train_edit(request, train_id):
    try:
        train = TrainProduction.objects.get(id=train_id)
        depart_date = train.departure_time.strftime('%Y-%m-%d')
        allow_assemble = True
        allow_close = False
        if (request.user.username in ['nozturk','egramsta']) or request.user.is_superuser:
            allow_close = True
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
        'allow_assemble':allow_assemble,
        'allow_close':allow_close


    })


@login_required(login_url=OIDC_LOGIN_URL)
def close_train(request, train_id):
    try:
        train = TrainProduction.objects.get(id=train_id)
        train.status = 'Closed'
        train.save()
        return train_edit(request, train_id)
    except:
        pass
    return HttpResponseRedirect(reverse('prodtask:request_table'))

@login_required(login_url=OIDC_LOGIN_URL)
def reopen_train(request, train_id):
    try:
        train = TrainProduction.objects.get(id=train_id)
        train.status = 'loading'
        train.save()
        return train_edit(request, train_id)
    except:
        pass
    return HttpResponseRedirect(reverse('prodtask:request_table'))


@login_required(login_url=OIDC_LOGIN_URL)
def pattern_train_list(request):
    if request.method == 'GET':
        PATTEN_TYPES = ['mc_pattern','data_pattern','mc_default_pattern']
        patterns = {}
        for pattern_type in PATTEN_TYPES:
            patterns_trains = TrainProduction.objects.filter(status=pattern_type).order_by('-id')
            patterns[pattern_type] = []
            for pattern_train in patterns_trains:
                patterns[pattern_type].append({'request_id':pattern_train.pattern_request.reqid,
                                               'request_name':pattern_train.pattern_request.description,
                                               'train_id':pattern_train.id})

        return render(request, 'prodtask/_pattern_list_creation.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Pattern for trains',
                 'patterns':[('MCPatterns','MC',patterns['mc_pattern']) ,
                             ('DataPatterns','Real data',patterns['data_pattern']),
                             ('MCDefaultPatterns','MC15b patterns for auto creation',patterns['mc_default_pattern'])] ,
                'parent_template': 'prodtask/_index.html',
            })


@csrf_protect
def add_pattern_to_list(request):
    results = {'success':False}
    if request.method == 'POST':
        try:
            data = request.body
            input_dict = json.loads(data)
            pattern_request_id = input_dict['request_id']
            pattern_type = {'MCPatterns':'mc_pattern','DataPatterns':'data_pattern','MCDefaultPatterns':'mc_default_pattern'}[input_dict['pattern_type']]
            if TrainProduction.objects.filter(status=pattern_type,pattern_request=int(pattern_request_id)).exists():
                results = {'success':False,'message': "This pattern already exist"}
            else:
                train_id = create_pattern_train(pattern_request_id,pattern_type)
                production_request = TRequest.objects.get(reqid=pattern_request_id)
                results = {'success':True,'requestName':production_request.description,'trainID':train_id}
        except Exception as e:
            results = {'success':False,'message': str(e)}
    return HttpResponse(json.dumps(results), content_type='application/json')


@csrf_protect
def remove_pattern_in_list(request):
    results = {'success':False}
    if request.method == 'POST':

        try:
            data = request.body
            input_dict = json.loads(data)
            train_id = input_dict['trainID']
            train = TrainProduction.objects.get(id=train_id)
            train.status = 'Cancelled'
            train.save()
            results = {'success':True}
        except Exception as e:
            results = {'success':False,'message': str(e)}
    return HttpResponse(json.dumps(results), content_type='application/json')

@csrf_protect
def get_pattern_from_request(request,reqid):
    results = {}
    try:
       results =  pattern_from_request(reqid)
    except Exception as e:
            pass
    return HttpResponse(json.dumps(results), content_type='application/json')


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_derivation_phys_pattern(request):
    current_patterns = SystemParametersHandler.get_daod_phys_production()
    mc_campaigns = SystemParametersHandler.get_mc_campaigns()
    result_campaigns = []
    for mc_campaign in mc_campaigns:
        mc_campaign.subcampaigns = [SystemParametersHandler.DAOD_PHYS_Production.ALL_SUBCAMPAIGNS] + mc_campaign.subcampaigns
        #mc_campaign.subcampaigns = [SystemParametersHandler.DAOD_PHYS_Production.ALL_SUBCAMPAIGNS]
        result_campaigns.append(mc_campaign)
    current_patterns_dict = [dict(request_id=TrainProduction.objects.get(id=x.train_id).pattern_request_id, **asdict(x)) for x in current_patterns]

    return Response({'current_patterns':current_patterns_dict,'mc_campaigns':[asdict(x) for x in result_campaigns],'steps':
                     [get_pattern_steps(x.train_id,x.outputs) for x in current_patterns]})

def get_pattern_steps(train_id, outputs):
    train = TrainProduction.objects.get(id=train_id)
    pattern_request = train.pattern_request
    steps = []
    for output in train.output_by_slice:
        output_intersections = [x for x in outputs if x in output[1]]
        if output_intersections:
            step = StepExecution.objects.filter(request=pattern_request,
                                                slice=InputRequestList.objects.get(request=pattern_request,
                                                                                   slice=output[0])).first()
            if step:
                steps.append({'ami_tag': step.step_template.ctag,
                               'project_mode': step.get_task_config('project_mode'),
                               'outputs': output_intersections})
    return steps

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_step_from_pattern(request):
    pattern_request_id = int( request.query_params.get('request_id'))
    if not TRequest.objects.filter(reqid=pattern_request_id):
        return Response([])
    pattern_request = TRequest.objects.get(reqid=pattern_request_id)
    outputs = request.query_params.get('outputs')
    if not outputs:
        return Response([])
    outputs = outputs.split('.')
    steps = []
    if TrainProduction.objects.filter(pattern_request=pattern_request).exists():
        train = TrainProduction.objects.get(pattern_request=pattern_request)
        for output in train.output_by_slice:
            output_intersections = [x for x in outputs if x in output[1]]
            if output_intersections:
                step = StepExecution.objects.filter(request=pattern_request, slice = InputRequestList.objects.get(request=pattern_request,slice=output[0])).first()
                if step:
                    steps.append([{'ami_tag':step.step_template.ctag,'project_mode':step.get_task_config('project_mode'),'outputs':output_intersections}])
    else:
        slices = InputRequestList.objects.filter(request=pattern_request)
        for slice in slices:
            if not slice.is_hide:
                step = StepExecution.objects.filter(request=pattern_request, slice = slice).first()
                output_intersections = [x for x in outputs if x in slice.output_formats.split('.')]
                if step and output_intersections:
                    steps.append([{'ami_tag':step.step_template.ctag,'project_mode':step.get_task_config('project_mode'),
                                   'outputs':output_intersections}])
    return Response(steps)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def save_derivation_phys_pattern(request):
    data = request.data
    new_patterns = []
    pattern_steps = []
    try:
        for pattern in data['patterns']:
            steps = []
            outputs = pattern['outputs'][0].split('.')
            if TrainProduction.objects.filter(pattern_request_id=pattern['request_id']).exists():
                train_id = TrainProduction.objects.filter(pattern_request_id=pattern['request_id']).last().id
                steps = get_pattern_steps(train_id, outputs)
            else:
                slices = InputRequestList.objects.filter(request=pattern['request_id'])
                for slice in slices:
                    if not slice.is_hide:
                        step = StepExecution.objects.filter(request=pattern['request_id'], slice=slice).first()

                        output_intersections = [x for x in outputs if x in step.step_template.output_formats.split('.')]

                        if step and output_intersections:
                            steps.append([{'ami_tag': step.step_template.ctag,
                                           'project_mode': step.get_task_config('project_mode'),
                                           'outputs': output_intersections}])
                if not steps:
                    raise Exception(f'No steps found for pattern {pattern["request_id"]}')
                train_id = create_pattern_train(pattern['request_id'])
            new_patterns.append(SystemParametersHandler.DAOD_PHYS_Production(pattern['campaign'],
                                                                             pattern['subcampaign'], outputs,
                                                                             train_id, pattern['status']))
            pattern_steps.append(steps)
        SystemParametersHandler.set_daod_phys_production(new_patterns)
        _jsonLogger.info("Save mc daod pattern",  extra={'user': request.user.username})
        _logger.info("Save mc daod pattern: " +json.dumps(data['patterns']))
        return Response({'steps':pattern_steps})
    except Exception as ex:
        _logger.error(f"Problem with mc daod phys pattern saving {ex}")
        return Response(f"Problem with mc daod phys pattern saving {ex}", status=400)

