import json
import logging
import pickle

from django.http import HttpResponse, HttpResponseRedirect

from django.views.decorators.csrf import csrf_protect
from time import sleep
from django.utils import timezone
from copy import deepcopy

from rest_framework.response import Response
from django.shortcuts import render

from atlas.prodtask.ddm_api import tid_from_container
from atlas.prodtask.models import RequestStatus
from ..prodtask.spdstodb import fill_template
from ..prodtask.request_views import clone_slices
from atlas.prodtask.views import set_request_status
from ..prodtask.helper import form_request_log
#from ..prodtask.task_actions import do_action
from .views import form_existed_step_list, form_step_in_page, fill_dataset, make_child_update
from django.db.models import Count, Q
from rest_framework.decorators import api_view

from .models import StepExecution, InputRequestList, TRequest, Ttrfconfig, ProductionTask, ProductionDataset, \
    ParentToChildRequest, TTask, MCPattern

_logger = logging.getLogger('prodtaskwebui')

def request_progress_main(request):
    if request.method == 'GET':
        return render(request, 'prodtask/_progress_stat.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Progress',
                'submit_url': 'prodtask:request_progress_main',
                'parent_template': 'prodtask/_index.html',
            })


def prepare_step_statistic(request_statistics):
        ordered_step_statistic = []
        for step_statistic in request_statistics['step_statistic']:
            percent_done = 0.0
            if request_statistics['step_statistic'][step_statistic]["input_events"] == 0:
                step_status = 'Unknown'
            else:
                percent_done = float(request_statistics['step_statistic'][step_statistic]['processed_events']) / float(request_statistics['step_statistic'][step_statistic]["input_events"])
                if (percent_done>0.90):
                   step_status = 'StepDone'
                elif (percent_done>0.10):
                   step_status = 'StepProgressing'
                else:
                   step_status =  'StepNotStarted'
            ordered_step_statistic.append({'statistic':request_statistics['step_statistic'][step_statistic],
                                           'step_name':step_statistic,'order':MCPattern.STEPS.index(step_statistic),
                                           'step_status':step_status,'percent':str(round(percent_done*100,2))+'%'})

        ordered_step_statistic.sort(key=lambda x:x['order'])
        return ordered_step_statistic


@api_view(['GET'])
def request_hashtag_monk(request):
    hashtag_monk = pickle.load(open('/data/hashtagmonk.pkl','rb'))
    result = []
    try:
        for entry in hashtag_monk:
            progress = request_progress(entry['requests_ids'])
            ordered_step_statistic = prepare_step_statistic(progress)
            result.append({'hashtags':entry['filter'],'step_statistic':ordered_step_statistic})
    except Exception,e:
         print str(e)
    return Response({"load": result})

@api_view(['GET'])
def request_progress_general(request, reqids):
    requests_to_process = map(int,reqids.split(','))
    request_statistics = request_progress(requests_to_process)
    result = {}
    ordered_step_statistic = []
    try:
        ordered_step_statistic = prepare_step_statistic(request_statistics)
        steps_name = [x['step_name'] for x in ordered_step_statistic]
        chains = []
        for chain in request_statistics['chains'].values():
            current_chain = [{}] * len(steps_name)
            chain_requests = set()
            for task_id in chain:
                i = steps_name.index(request_statistics['processed_tasks'][task_id]['step'])
                task = {'task_id':task_id}
                task.update(request_statistics['processed_tasks'][task_id])
                chain_requests.add(request_statistics['processed_tasks'][task_id]['request'])
                current_chain[i] = task
            chains.append({'chain':current_chain,'requests':chain_requests})
        result.update({'step_statistic':ordered_step_statistic,'chains':chains})
    except Exception,e:
        print str(e)
    return Response({"load": result})

def get_parent_tasks(task):
    if task.parent_id != task.id:
        return [int(task.parent_id)]
    if 'evgen'in task.name:
        return []
    task_input = task.input_dataset
    if 'tid' in task_input:
        return [int(task_input[task_input.rfind('tid')+3:task_input.rfind('_')])]
    else:
        return tid_from_container(task_input)


def request_progress(reqid_list):
    def get_all_tasks_from_request(parent_task_id):
        parent_task = ProductionTask.objects.get(id=parent_task_id)
        parent_request_tasks = list(ProductionTask.objects.filter(request=parent_task.request_id).values('id','total_events'))
        result_dict = {}
        for parent_request_task in parent_request_tasks:
            result_dict.update({int(parent_request_task['id']):int(parent_request_task['total_events'])})
        return result_dict

    def get_step_name(task_name):
        return '.'.join(task_name.split('.')[3:5])

    all_tasks = list(ProductionTask.objects.filter(request__in=reqid_list).order_by('id'))
    step_by_name = {}
    step_statistic = {}
    #key - task id, {"input_events":int, "processed_events":int, "chain_id":id}
    processed_tasks = {}
    other_requests_tasks = {}
    chains = {}

    for task in all_tasks:
        if task.status not in  ProductionTask.RED_STATUS:
            task_input_events = 0
            task_step = get_step_name(task.inputdataset)
            if task_step not in step_by_name:
                step_by_name.update({task_step:task.step.step_template.step})
            parent_tasks_id = get_parent_tasks(task)
            chain_id = 0
            #Count number of events for parent task
            #evgen
            if len(parent_tasks_id) == 0:
                task_input_events = task.step.input_events
                chain_id = int(task.id)
                chains.update({int(task.id):[int(task.id)]})
            if len(parent_tasks_id) == 1:
                parent_task_id = parent_tasks_id[0]
                if parent_task_id in processed_tasks:
                    task_input_events = processed_tasks[parent_task_id]["processed_events"]
                    chains[processed_tasks[parent_task_id]["chain_id"]] = chains[processed_tasks[parent_task_id]["chain_id"]] + [int(task.id)]
                    chain_id = processed_tasks[parent_task_id]["chain_id"]
                else:
                    if parent_task_id not in other_requests_tasks:
                        other_requests_tasks.update(get_all_tasks_from_request(parent_task_id))
                    task_input_events = other_requests_tasks[parent_task_id]
                    chains.update({int(task.id):[int(task.id)]})
                    chain_id = int(task.id)
            if len(parent_tasks_id) > 1:
                chains.update({int(task.id):[int(task.id)]})

                for parent_task_id in parent_tasks_id:
                    if parent_task_id in processed_tasks:
                        task_input_events += processed_tasks[parent_tasks_id]
                    else:
                        if parent_task_id not in other_requests_tasks:
                            other_requests_tasks.update(get_all_tasks_from_request(parent_task_id))
                        task_input_events += other_requests_tasks[parent_tasks_id]["processed_events"]
                chain_id = int(task.id)
            processed_tasks.update({int(task.id):{"input_events":task_input_events, "processed_events":task.total_events,
                                                  "chain_id":chain_id, 'status':task.status, 'step':step_by_name[task_step], 'request':task.request_id}})
            if step_by_name[task_step] in step_statistic:
                step_statistic[step_by_name[task_step]] = {'input_events':step_statistic[step_by_name[task_step]]['input_events']+task_input_events,
                                             'processed_events':step_statistic[step_by_name[task_step]]['processed_events']+task.total_events}
            else:
                step_statistic[step_by_name[task_step]] = {'input_events':task_input_events,
                                             'processed_events':task.total_events}
    return {'chains':chains,'processed_tasks':processed_tasks,'step_statistic':step_statistic}

