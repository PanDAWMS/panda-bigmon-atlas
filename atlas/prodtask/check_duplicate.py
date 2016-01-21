
import json
import logging

import datetime
import pickle

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render

from django.views.decorators.csrf import csrf_protect
from time import sleep
from django.utils import timezone
from copy import deepcopy
from atlas.prodtask.models import RequestStatus
from ..prodtask.spdstodb import fill_template
from ..prodtask.request_views import clone_slices
from atlas.prodtask.views import set_request_status
from ..prodtask.helper import form_request_log
#from ..prodtask.task_actions import do_action
from .views import form_existed_step_list, form_step_in_page, fill_dataset, make_child_update
from django.db.models import Count, Q

from .models import StepExecution, InputRequestList, TRequest, Ttrfconfig, ProductionTask, ProductionDataset, \
    ParentToChildRequest, TTask



def with_without_scope(dataset_name):
    if ':' in dataset_name:
        return [dataset_name,dataset_name.split(':')[1]]
    else:
        return [dataset_name.split('.')[0]+":"+dataset_name,dataset_name]

def find_similare_tasks(type,provenance='AP'):
    if type:
        all_tasks = list(ProductionTask.objects.filter(Q( status__in=['done','finished'] )&Q(name__contains=type)&
                                                     Q(provenance__exact=provenance)).values('name','id','total_events', 'request_id','timestamp'))
    else:
        all_tasks = list(ProductionTask.objects.filter(Q( status__in=['done','finished'] )&
                                             Q(provenance__exact=provenance)).values('name','id','total_events', 'request_id','timestamp'))

    sorted_tasks = sorted(all_tasks, key=lambda x: x['name'])
    #sorted_tasks = all_tasks
    first=sorted_tasks[0]
    same_name_list=[]
    current_list=[]
    for simul_task in sorted_tasks[1:]:
        if simul_task['name']==first['name']:
            if current_list:
                current_list+=[simul_task]
            else:
                current_list=[first,simul_task]
        else:
            if current_list:
               same_name_list+=[current_list]
               current_list=[]
        first=simul_task
    return same_name_list


def get_outputs_offset(task_param_dict):
    offset = 0
    outputs = []
    if 'skipFilesUsedBy' in task_param_dict:
        return 0,[]
    for value  in task_param_dict['jobParameters']:
        if type(value)==dict:
            if (value.get('value') == '--randomSeed=${SEQNUMBER}')or(value.get('value') =='randomSeed=${SEQNUMBER}')or\
                    (value.get('param_type') == 'input'):
                if value.get('offset',0)!=0:
                    offset =  value.get('offset')
            if (value.get('param_type') == 'output'):
               outputs.append(value.get('value').replace(' ','=').split('.')[0])
    return offset,outputs


def  find_duplicates_all_db(same_name_list):


    duplicates = []
    for index, same_tasks in enumerate(same_name_list):
        is_dataset = False
        input_datasets = []
        same_input = {}
        suspicious_tasks = set()
        container_suspicious = set()
        requests_set = set()
        if (index % 100)==0:
            print index,
        container_exist = False

        for same_task in same_tasks:
            requests_set.add(int(same_task['request_id']))
            ttask = TTask.objects.get(id=same_task['id'])
            container_names = []
            container_name = ''
            current_input_dataset = ttask.input_dataset
            if '/'  in  current_input_dataset:
                container_exist = True

            current_input_datasets = with_without_scope(current_input_dataset)
            if 'tid' in current_input_dataset:
                container_name = current_input_dataset.split('_tid')[0]+'/'
                container_names = with_without_scope(container_name)

            if current_input_dataset not in input_datasets:
                input_datasets += current_input_datasets
                same_input[current_input_dataset] = [{'task':same_task,'jedi_task':ttask}]
            else:
                if current_input_datasets[0] in same_input:
                    same_input[current_input_datasets[0]]+=[{'task':same_task,'jedi_task':ttask}]
                    suspicious_tasks.add(current_input_datasets[0])
                else:
                    same_input[current_input_datasets[1]]+=[{'task':same_task,'jedi_task':ttask}]
                    suspicious_tasks.add(current_input_datasets[1])
                is_dataset = True
            if container_name:
                if container_name not in input_datasets:
                    input_datasets += container_names
                    same_input[container_name] = [{'task':same_task,'jedi_task':ttask}]
                else:
                    if container_names[0] in same_input:
                        same_input[container_names[0]]+=[{'task':same_task,'jedi_task':ttask}]
                        suspicious_tasks.add(container_names[0])
                        container_suspicious.add(container_names[0])
                    else:
                        same_input[container_names[1]]+=[{'task':same_task,'jedi_task':ttask}]
                        suspicious_tasks.add(container_names[1])
                        container_suspicious.add(container_names[1])
        if not container_exist:
            for container in container_suspicious:
                same_input.pop(container)
                suspicious_tasks.remove(container)
        #print same_input
        if is_dataset  and (max(requests_set)>1000):
            for suspicious_task in suspicious_tasks:
                output_dict = {}
                for task in same_input[suspicious_task]:
                    offset, current_outputs = get_outputs_offset(task['jedi_task'].jedi_task_parameters)
                    for output in current_outputs:
                        output_dict[output] = output_dict.get(output,[]) + [{'offset':int(offset),'task':task['task']}]
                for tasks_offset in output_dict.values():
                    current_task_offset = sorted(tasks_offset, key=lambda x: x['offset'])
                    current_offset_task = current_task_offset[0]
                    new_series = False
                    for offset_task in current_task_offset[1:]:
                        if offset_task['offset'] == current_offset_task['offset']:
                            if not new_series:
                                duplicates.append([current_offset_task['task'],offset_task['task']])
                                new_series = True
                            else:
                                duplicates[-1] += [offset_task['task']]
                        else:
                            new_series = False
                        current_offset_task = offset_task
    return duplicates





def get_offset_from_jedi(id, task_parameters=None):
    if task_parameters:
        jedi_task = TTask.objects.get(id=id)
        task_parameters = jedi_task.jedi_task_parameters['jobParameters']
    offset = None
    for value  in task_parameters:
        if (value.get('value') == '--randomSeed=${SEQNUMBER}')or(value.get('value') =='randomSeed=${SEQNUMBER}'):
          offset =  value.get('offset')
    return offset

def find_simul_duplicates():
    simul_tasks = list(ProductionTask.objects.filter(Q( status__in=['done','finished'] )&
                                                     Q(provenance__exact='AP')&Q(name__startswith='mc')).values('name','id','total_events','inputdataset', 'request_id'))
    sorted_simul_tasks = sorted(simul_tasks, key=lambda x: x['name'])
    first=sorted_simul_tasks[0]
    result_list=[]
    current_list=[]
    for simul_task in sorted_simul_tasks[1:]:
        if simul_task['name']==first['name']:
            if current_list:
                current_list+=[simul_task]
            else:
                current_list=[first,simul_task]
        else:
            if current_list:
               result_list+=[current_list]
               current_list=[]
        first=simul_task
    bad_list = []
    task_id_list = []
    for simul_same_tasks in result_list:
        is_container = False
        is_dataset = False
        requests_set = set()
        for simul_same_task in simul_same_tasks:
            requests_set.add(int(simul_same_task['request_id']))
            if '/' in simul_same_task['inputdataset']:
                is_container = True
            else:
                is_dataset = True
            if (is_container and is_dataset) and (len(requests_set)>1) and (max(requests_set)>1000):
                bad_list.append(simul_same_tasks)
                break
    #print bad_list
    total_events = 0
    requests = set()
    for tasks in bad_list:
        print tasks[0]['name']+ ' - '+','.join([str(x['id']) for x in tasks])+' - '+','.join([str(x['request_id']) for x in tasks])
        total_events += sum([x['total_events'] for x in tasks])
        for task in tasks:
            requests.add(task['request_id'])
    print total_events
    print len(bad_list)
    print requests


def find_simul_split_dupl():
    tasks = list(ProductionTask.objects.filter(total_events__gt=1000000).filter(name__startswith='mc').filter(name__contains='simul'))
    for task in tasks:
        slice_event = task.step.input_events
        if slice_event != -1:
            if task.total_events > slice_event:
                print task.id


def find_all_mc_duplicates():
    def with_without_scope(dataset_name):
        if ':' in dataset_name:
            return [dataset_name,dataset_name.split(':')[1]]
        else:
            return [dataset_name.split('.')[0]+":"+dataset_name,dataset_name]

    simul_tasks = list(ProductionTask.objects.filter(Q( status__in=['done','finished'] )&
                                                     Q(provenance__exact='AP')&~Q(name__contains='evgen')&Q(name__startswith='mc')).values('name','id','total_events','inputdataset', 'request_id'))
    sorted_simul_tasks = sorted(simul_tasks, key=lambda x: x['name'])
    first=sorted_simul_tasks[0]
    same_name_list=[]
    current_list=[]
    for simul_task in sorted_simul_tasks[1:]:
        if simul_task['name']==first['name']:
            if current_list:
                current_list+=[simul_task]
            else:
                current_list=[first,simul_task]
        else:
            if current_list:
               same_name_list+=[current_list]
               current_list=[]
        first=simul_task

    bad_list = []
    task_id_list = []
    for same_tasks in same_name_list:
        is_container = False
        is_dataset = False
        input_datasets = []
        requests_set = set()
        for same_task in same_tasks:
            requests_set.add(int(same_task['request_id']))
            if '/' in same_task['inputdataset']:
                is_container = True
            else:
                ttask = TTask.objects.get(id=same_task['id'])
                current_input_dataset = ttask.input_dataset
                current_input_datasets = with_without_scope(current_input_dataset)
                if current_input_dataset not in input_datasets:
                    input_datasets += current_input_datasets
                else:
                    is_dataset = True
            if is_dataset and (len(requests_set)>1) and (max(requests_set)>1000):
                bad_list.append(same_tasks)
                break
    #print bad_list
    total_events = 0
    requests = set()
    for tasks in bad_list:
        print tasks[0]['name']+ ' - '+','.join([str(x['id']) for x in tasks])+' - '+','.join([str(x['request_id']) for x in tasks])
        total_events += sum([x['total_events'] for x in tasks])
        for task in tasks:
            requests.add(task['request_id'])
    print total_events
    print len(bad_list)
    print requests



def find_evgen_duplicates():
    evgen_tasks = list(ProductionTask.objects.filter(Q( status__in=['done','finished'] )&Q(name__startswith='mc')&Q(name__contains='evgen')).values('name','id','total_events', 'request_id'))
    sorted_evgen_tasks = sorted(evgen_tasks, key=lambda x: x['name'])
    first=sorted_evgen_tasks[0]
    result_list=[]
    current_list=[]
    for evgen_task in sorted_evgen_tasks[1:]:
        if evgen_task['name']==first['name']:
            if current_list:
                current_list+=[evgen_task]
            else:
                current_list=[first,evgen_task]
        else:
            if current_list:
               result_list+=[current_list]
               current_list=[]
        first=evgen_task
    bad_list = []
    task_id_list = []
    for evgen_same_tasks in result_list:
        offset = [get_offset_from_jedi(evgen_same_tasks[0]['id'])]

        evgen_same_tasks[0]['offset'] = offset[0]
        bad = False
        for evgen_same_task in evgen_same_tasks[1:]:
            current_offset = get_offset_from_jedi(evgen_same_task['id'])
            evgen_same_task['offset'] = current_offset
            if (current_offset!=None) and (current_offset in offset) and (not bad):
                bad_list.append(evgen_same_tasks)
                task_id_list.append(evgen_same_task['id'])
                bad = True
    #print bad_list
    total_events = 0
    requests = set()
    for_group = {}
    reuqest_by_name = {}
    for tasks in bad_list:
        #print tasks[0]['name']+ ' - '+','.join([str(x['id']) for x in tasks])+' - '+','.join([str(x['request_id']) for x in tasks])+' - '+','.join([str(x['offset']) for x in tasks])
        total_events += sum([x['total_events'] for x in tasks])
        task_id_list += [x['id'] for x in tasks if x['offset']==0]
        reuqest_by_name[tasks[0]['name']]=tasks[0]['request_id']
        if tasks[0]['name'].find('Sherpa')>-1:
            for_group[tasks[0]['name']]=[]
        for task in tasks:
            requests.add(task['request_id'])
            if task['offset'] == 0:
                if task['name'] in for_group:
                    for_group[task['name']]+=[task['id']]
    #print for_group

    #task_id_list.sort()
    #print len(bad_list)
    #print requests
    file_for_result_AP=open('/tmp/duplicationDescendAP.txt','w')
    file_for_result_GP=open('/tmp/duplicationDescendGP.txt','w')
    GP_requests = set()
    AP_requests = set()
    for task_group in for_group:
        duplicates, strange = find_task_by_input(for_group[task_group],task_group,reuqest_by_name[task_group])
        for duplicate in duplicates:
            if duplicates[duplicate][1] == 'AP':
                file_for_result_AP.write(str(duplicate)+','+str(duplicates[duplicate][2])+'\n')
                AP_requests.add(duplicates[duplicate][2])
            else:
                file_for_result_GP.write(str(duplicate)+','+str(duplicates[duplicate][2])+'\n')
                GP_requests.add(duplicates[duplicate][2])
        if strange:
            print strange
    print GP_requests
    print AP_requests
    file_for_result_AP.close()
    file_for_result_GP.close()

def find_task_by_input(task_ids, task_name, request_id):
    result_duplicate = []
    print task_ids
    for task_id in task_ids:
        task_pattern = '.'.join(task_name.split('.')[:-2]) + '%'+task_name.split('.')[-1] +'%'
        similare_tasks = list(ProductionTask.objects.extra(where=['taskname like %s'], params=[task_pattern]).filter(Q( status__in=['done','finished'] )).values('id','name','inputdataset','provenance','request_id').order_by('id'))
        task_chains = [int(task_id)]
        current_duplicates={int(task_id):(task_name,'AP',request_id)}
        for task in similare_tasks:
            task_input = task['inputdataset']
            #print task_input,'-',task['id']
            if 'py' not in task_input:
                if '/' in task_input:
                    task_chains.append(int(task['id']))
                else:
                    if 'tid' in task_input:
                        task_input_id = int(task_input[task_input.rfind('tid')+3:task_input.rfind('_')])
                        if (task_input_id in task_chains) :
                            task_chains.append(int(task['id']))
                            current_duplicates.update({int(task['id']):(task['name'],task['provenance'],task['request_id'])})
                    else:
                        print 'NOn tid:',task_input,task_name
        result_duplicate.append(current_duplicates)
    first_tasks = result_duplicate[0]
    second_tasks = result_duplicate[1]
    name_set = set()
    not_duplicate = []
    for task_id in first_tasks:
        name_set.add(first_tasks[task_id][0])
    for task_id in second_tasks.keys():
        if second_tasks[task_id][0] not in name_set:
            not_duplicate.append({task_id:second_tasks[task_id]})
    return second_tasks,not_duplicate


def find_downstreams_by_task(task_id):
    result_duplicate = []
    original_task = ProductionTask.objects.get(id=task_id)
    task_name = original_task.name
    task_pattern = '.'.join(task_name.split('.')[:-2]) + '%'+task_name.split('.')[-1] +'%'
    similare_tasks = list(ProductionTask.objects.extra(where=['taskname like %s'], params=[task_pattern]).
                          filter(Q( status__in=['done','finished','obsolete'] )).values('id','name','inputdataset','provenance','request_id','status').
                          order_by('id'))
    task_chains = [int(task_id)]
    current_duplicates={int(task_id):(task_name,original_task.provenance,original_task.request_id,'done')}
    for task in similare_tasks:
        task_input = task['inputdataset']
        #print task_input,'-',task['id']
        if 'py' not in task_input:
            if ('/' in task_input) and (int(task['id'])>int(task_id)) and (task_name!=task['name']):
                task_chains.append(int(task['id']))
                current_duplicates.update({int(task['id']):(task['name'],task['provenance'],task['request_id'],task['status'])})
                if (task['request_id'] != original_task.request_id) and (task['provenance']=='AP'):
                    print 'Simul problem' +'-'+ task_name + '-' + task['name']
                #print task_input,int(task['id'])
            else:
                if 'tid' in task_input:
                    task_input_id = int(task_input[task_input.rfind('tid')+3:task_input.rfind('_')])
                    #print task_input_id,task['id'], len(task_chains)

                    if (task_input_id in task_chains) :
                        task_chains.append(int(task['id']))
                        current_duplicates.update({int(task['id']):(task['name'],task['provenance'],task['request_id'],task['status'])})
                else:
                    print 'NOn tid:',task_input,task_name

    return current_duplicates

def find_identical_step(step):
    pass


def bulk_obsolete_from_file(file_name):
    with open(file_name,'r') as input_file:
        tasks = (int(line.split(',')[0]) for line in input_file if line)
        print timezone.now()
        for task_id in tasks:
            task = ProductionTask.objects.get(id=task_id)
            if task.status in ['finished','done']:
                task.status='obsolete'
                task.timestamp=timezone.now()
                task.save()
                print task.name, task.status

def bulk_find_downstream_from_file(file_name, output_file_name, provenance='AP', start_request=0):
    with open(file_name,'r') as input_file:
        tasks = (int(line.split(',')[0]) for line in input_file if line)
        output_file = open(output_file_name,'w')
        for task_id in tasks:
            downstream_tasks  = find_downstreams_by_task(task_id)
            for duplicate in sorted(downstream_tasks):
                if ((downstream_tasks[duplicate][1] == provenance) and (downstream_tasks[duplicate][3] != 'obsolete')) \
                        and (int(downstream_tasks[duplicate][2])>start_request):
                    output_file.write(str(task_id)+','+str(downstream_tasks[duplicate][0])+','
                                      +str(duplicate)+','+str(downstream_tasks[duplicate][2])+'\n')
        output_file.close()


def fix_wrong_parent(reqid):
    steps = StepExecution.objects.filter(request=reqid)
    for step in steps:
        if step.step_parent.step_template.ctag[0] == 't':
            new_step_parent = step.step_parent.step_parent
            step.step_parent = new_step_parent
            step.save()


def clean_fileUsedBy(dupl_list):
    result_list = []
    for duplicates in dupl_list:
        not_fileUsedBy = True
        for task in duplicates:
            task_db = ProductionTask.objects.get(id=task['id'])
            if 'skipFilesUsedBy' in task_db.step.get_task_config('project_mode'):
                not_fileUsedBy = False
                break
        if  not_fileUsedBy:
            result_list.append(duplicates)
    return result_list


def clean_reapeated(dupl_list):
    current_list = []
    result_list = []
    for duplicates in dupl_list:
        current_ids = []
        for task in duplicates:
            current_ids.append(int(task['id']))
        current_ids.sort()
        current_id_string = reduce(lambda x,y: str(x)+str(y),current_ids)
        if current_id_string not in current_list:
            current_list.append(current_id_string)
            result_list.append(duplicates)
    return result_list



def list_intersection(primary_list, searched_lists, field_name='id'):
    indexes = [0 for x in searched_lists]
    result_list = []
    for element in primary_list:
        for search_list_index, search_list in enumerate(searched_lists):
            if indexes[search_list_index] < len(search_list):
                print indexes[search_list_index],search_list[indexes[search_list_index]]
                while((search_list[indexes[search_list_index]][field_name]<element[field_name])and(indexes[search_list_index] < len(search_list))):
                    indexes[search_list_index]+=1
                if search_list[indexes[search_list_index]][field_name]==element[field_name]:
                    result_list.append(search_list[indexes[search_list_index]])
    return result_list

def check_duplication(days_delta):
    #tasks_to_check_id = list(ProductionTask.objects.filter(timestamp__gte=(datetime.datetime.now() -  datetime.timedelta(days=days_delta)),status__in=['done','finished']).order_by('id').values_list('id'))
    #all_ap_same = find_similare_tasks('')
    all_gp_same = find_similare_tasks('','GP')
    #find possible duplicates
    #ap_to_check = list_intersection(tasks_to_check_id,[all_ap_same])
    #gp_to_check = list_intersection(tasks_to_check_id,[all_gp_same])
    #print len(ap_to_check)
    #print len(gp_to_check)
    ap_duplicates = find_duplicates_all_db(all_gp_same)
    ap_duplicates_reduced = clean_reapeated(ap_duplicates)
    ap_duplicates_sorted = sorted(ap_duplicates_reduced, key=lambda x: max([y['id'] for y in x]))
    pickle.dump({'execute_time':datetime.datetime.now(),'duplicate_list':ap_duplicates_sorted},open('/tmp/gp_sorted.pkl','wb'))



def make_default_duplicate_page(request):
    #TODO: put to DB
    if request.method == 'GET':
        try:
            ap_date = pickle.load(open('/data/ap_sorted.pkl','rb'))
            gp_date = pickle.load(open('/data/gp_sorted.pkl','rb'))
            exec_time = ap_date['execute_time']
            ap_date['duplicate_list'].reverse()
            gp_date['duplicate_list'].reverse()
            ap_list = [{'ids':[y['id'] for y in x],'name':x[0]['name'],'date':max([y['timestamp'] for y in x])} for x in ap_date['duplicate_list']]
            gp_list = [{'ids':[y['id'] for y in x],'name':x[0]['name'],'date':max([y['timestamp'] for y in x])} for x in gp_date['duplicate_list']]
            return render(request, 'prodtask/_duplicate.html', {
                        'active_app': 'mcprod',
                        'parent_template': 'prodtask/_index.html',
                        'ap_list': ap_list,
                        'gp_list': gp_list,
                        'exec_time': exec_time


                     })
        except Exception,e:
            print e
            return HttpResponseRedirect('/')