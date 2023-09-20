from datetime import timedelta

from django.contrib.auth.decorators import login_required
from django.urls import reverse
from django.http import HttpResponseRedirect

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import WaitingStep, StepExecution, InputRequestList, ProductionTask, ProductionDataset
from django.shortcuts import render
from django.utils import timezone
import logging

from atlas.prodtask.views import set_request_status, make_child_update
from atlas.settings import OIDC_LOGIN_URL

_logger = logging.getLogger('prodtaskwebui')


def _parse_action_options(option_string):
    project_mode_dict = dict()
    for option in option_string.replace(' ', '').split(";"):
        if not option:
            continue
        if not '=' in option:
            raise Exception('The  option \"{0}\" has invalid format. '.format(option) +
                            'Expected format is \"optionName=optionValue\"')
        project_mode_dict.update({option.split("=")[0].lower(): option.split("=")[1]})
    return project_mode_dict

def process_request(production_request_id):
    waiting_step_todo = WaitingStep.objects.filter(status='active', request=production_request_id)
    process_actions(waiting_step_todo)

def find_waiting():
    waiting_step_todo = WaitingStep.objects.filter(status='active',execution_time__lte=timezone.now())
    process_actions(waiting_step_todo)

def process_actions(waiting_step_todo):
    for waiting_step in waiting_step_todo:
        waiting_step.status = 'executing'
        waiting_step.save()
    check2replicas = []
    checkPreStage = []
    for waiting_step in waiting_step_todo:
        if waiting_step.action == 1:
            postponed_step(waiting_step.id)
        if waiting_step.action == 2:
            check2replicas.append(waiting_step.id)
        if waiting_step.action == 3:
            check_evgen(waiting_step.id)
        if waiting_step.action == 4:
            checkPreStage.append(waiting_step.id)
    if check2replicas:
        ddm= DDM()
        max_attempts = WaitingStep.ACTIONS[2]['attempts']
        delay =  WaitingStep.ACTIONS[2]['delay']
        for waiting_step in check2replicas:
            try:
                check_two_replicas(waiting_step, ddm, max_attempts, delay)
            except Exception as e:
                print(str(e))
                waiting_step = WaitingStep.objects.get(id=waiting_step)
                waiting_step.status = 'active'
                waiting_step.save()
                _logger.error("Check replicas problem %s" % str(e))
    if checkPreStage:
        ddm= DDM()
        max_attempts = WaitingStep.ACTIONS[4]['attempts']
        delay =  WaitingStep.ACTIONS[4]['delay']
        for waiting_step in checkPreStage:
            try:
                do_pre_stage(waiting_step, ddm, max_attempts, delay)
            except Exception as e:
                _logger.error("Check replicas problem %s" % str(e))
                waiting_step = WaitingStep.objects.get(id=waiting_step)
                waiting_step.status = 'active'
                waiting_step.save()



def postponed_step(waiting_step_id):
    waiting_step = WaitingStep.objects.get(id=waiting_step_id)
    step = StepExecution.objects.get(id=waiting_step.step)
    if waiting_step.attempt>=2:
        step.status = 'Approved'
        waiting_step.done_time = timezone.now()
        waiting_step.status = 'done'
        waiting_step.message = 'Done at %s'%str(timezone.now())
        step.save()
    else:
        waiting_step.status = 'active'
        waiting_step.attempt += 1
        waiting_step.execution_time = waiting_step.execution_time + timedelta(hours=1)
    waiting_step.save()


def check_evgen(waiting_step_id):
    APPROVE_LEVEL = 0.5
    waiting_step = WaitingStep.objects.get(id=waiting_step_id)
    step = StepExecution.objects.get(id=waiting_step.step)
    slice = step.slice
    approved_steps = list(StepExecution.objects.filter(status='Approved',slice=slice))
    if len(approved_steps) == 0:
            # No approved steps -> remove waiting
            step.status = 'NotChecked'
            waiting_step.done_time = timezone.now()
            waiting_step.status = 'failed'
            waiting_step.message = 'Failed at %s' % str(timezone.now())
            step.save()
            waiting_step.save()

    else:
        # Check only first task of the first step

        tasks_to_check = ProductionTask.objects.filter(step=approved_steps[0])
        if tasks_to_check:
            task_to_check = tasks_to_check[0]
            if task_to_check.status in ProductionTask.RED_STATUS:
                # Parent failed -> remove waiting
                step.status = 'NotChecked'
                waiting_step.done_time = timezone.now()
                waiting_step.status = 'failed'
                waiting_step.message = 'Failed at %s' % str(timezone.now())
                step.save()
                waiting_step.save()
            else:
                total_files_tobeused = 0
                total_files_finished = 0
                if task_to_check.total_files_tobeused:
                    total_files_tobeused = task_to_check.total_files_tobeused
                if task_to_check.total_files_finished:
                    total_files_finished = task_to_check.total_files_finished
                if total_files_tobeused != 0:
                    if ((float(total_files_finished)/float(total_files_tobeused))>APPROVE_LEVEL):
                        step.status = 'NotChecked'
                        waiting_step.done_time = timezone.now()
                        waiting_step.status = 'done'
                        waiting_step.message = 'Done at %s' % str(timezone.now())
                        step.save()
                        waiting_step.save()
                        steps = StepExecution.objects.filter(slice=step.slice)
                        for current_step in steps:
                            if current_step.status == 'NotChecked':
                                current_step.status = 'Approved'
                        _logger.debug("Slice %s has been approved after evgen"%str(slice))
                        if step.status not in ['test','approved']:
                            set_request_status('cron', step.request_id, 'approved', 'Automatic waiting approve',
                                               'Request was automatically approved')
                            make_child_update(step.request_id, 'cron', step.slice.slice)
                    else:
                        if task_to_check.status in ['done','finished','obsolete']:
                            step.status = 'NotChecked'
                            waiting_step.done_time = timezone.now()
                            waiting_step.status = 'failed'
                            waiting_step.message = 'Failed at %s' % str(timezone.now())
                            step.save()
                            waiting_step.save()


def get_slice_input_datasets(container, ddm):
    if 'tid' not in container:
        datasets = ddm.dataset_in_container(container.strip())
        return datasets
    else:
        return [container.strip()]



def check_two_replicas(waiting_step_id, ddm, max_attempts, delay):
    waiting_step = WaitingStep.objects.get(id=waiting_step_id)
    step = StepExecution.objects.get(id=waiting_step.step)
    approve_step = False
    if (step.step_parent_id != step.id) and (step.step_parent.status not in ['Skipped','NotCheckedSkipped'] ) and (not ProductionTask.objects.filter(step=step.step_parent).exists()):
        waiting_step.status = 'active'
        waiting_step.execution_time = waiting_step.execution_time + timedelta(hours=delay)
        waiting_step.save()
        approve_step = True
    else:
        datasets = []
        if ProductionTask.objects.filter(step=step.step_parent).exists():
            outputs = step.get_task_config('input_format')
            if outputs:
                outputs = outputs.split('.')
            tasks = ProductionTask.objects.filter(step=step.step_parent)
            for task in tasks:
                output_datasets = ProductionDataset.objects.filter(task_id=task.id)
                for current_dataset in output_datasets:
                    if outputs:
                        if (current_dataset.name.split('.')[4] in outputs):
                            datasets.append(current_dataset.name)
                    else:
                        if current_dataset.name.split('.')[4] != 'log':
                            datasets.append(current_dataset.name)
        else:
            datasets = get_slice_input_datasets(step.slice.dataset, ddm)
        not_full_exists = False
        good_exists = False
        error_message = ''
        for dataset in datasets:
            try:
                replicas = ddm.number_of_full_replicas(dataset)
                if len(replicas) >= 2:
                    _logger.info("Two replicas  %s - %s" % (str(dataset),str(replicas)))
                    good_exists = True
                else:
                    not_full_exists = True
            except Exception as e:
                error_message = str(e)
                not_full_exists = True
            except:
                not_full_exists = True
        if good_exists and (not not_full_exists):
            waiting_step.status = 'done'
            waiting_step.message = 'All %s have >=2 replicas' %(str(datasets)[:1000])
            waiting_step.done_time = timezone.now()
            approve_step = True
        else:
            waiting_step.attempt += 1
            if waiting_step.attempt > max_attempts:
                waiting_step.status = 'failed'
            else:
                waiting_step.status = 'active'
                waiting_step.execution_time = timezone.now() + timedelta(hours=delay)
            if error_message:
                waiting_step.message = 'Error for of %s: %s ' % (str(datasets)[:1000],str(error_message))
            waiting_step.message = 'Some of %s have <2 replicas' % (str(datasets)[:1000])
        waiting_step.save()
    if approve_step:
        step.status = 'Approved'
        step.save()
        if step.request.cstatus not in ['test','approved']:
            set_request_status('cron', step.request.reqid, 'approved', 'Automatic pre action approve',
                               'Request was automatically approved')




def do_pre_stage(waiting_step_id, ddm, max_attempts, delay):
    waiting_step = WaitingStep.objects.get(id=waiting_step_id)
    step = StepExecution.objects.get(id=waiting_step.step)
    waiting_parameters = {'level':90}
    if step.get_task_config('PDAParams'):
        try:
            waiting_parameters_from_step = _parse_action_options(step.get_task_config('PDAParams'))
            if waiting_parameters.get('level'):
                waiting_parameters['level'] = int(waiting_parameters_from_step.get('level'))
                if waiting_parameters['level'] > 100:
                    waiting_parameters['level'] = 100
                if waiting_parameters['level'] < 0:
                    waiting_parameters['level'] = 0
        except Exception as e:
            _logger.error(" %s" % str(e))
    approve_step = False
    if (step.step_parent_id != step.id) and (step.step_parent.status not in ['Skipped','NotCheckedSkipped'] ):
        waiting_step.status = 'cancelled'
        waiting_step.save()
        approve_step = True
    else:
        datasets = get_slice_input_datasets(step.slice.dataset, ddm)
        error_message = ''
        dataset = datasets[0]
        try:
            replicas = ddm.full_replicas_per_type(dataset)
        except Exception as e:
            replicas = {'data':None,'tape':None}
            error_message = str(e)
        if len(replicas['data'])>0:
            waiting_step.status = 'done'
            waiting_step.message = '%s have >0 datadisk replicas' %(str(dataset))
            waiting_step.done_time = timezone.now()
            approve_step = True
        else:
            waiting_step.attempt += 1
            rse = 'type=DATADISK&datapolicynucleus=True'
            rule = ddm.dataset_active_rule_by_rse(dataset, rse)
            files = ddm.dataset_metadata(dataset)['length']
            level = waiting_parameters['level']
            if rule:
                if ((level == 100) and (int(rule['locks_ok_cnt'])==int(files)))or((float(rule['locks_ok_cnt']) / float(files)) >= (float(level) / 100.0)):
                    waiting_step.status = 'done'
                    waiting_step.message = '%s has > %s %%  files pre staged ' % (str(dataset), str(level))
                    waiting_step.done_time = timezone.now()
                    approve_step = True
                else:
                    link = '<a href="https://rucio-ui.cern.ch/did?name={name}">{name}</a>'.format(name=str(dataset))
                    rule_link = '<a href="https://rucio-ui.cern.ch/rule?rule_id={rule_id}">{rule_rse}</a>'.format(rule_id=str(rule['id']),rule_rse=rule['rse_expression'])
                    tape_replica = ''
                    if replicas['tape']:
                        tape_replica = replicas['tape'][0]['rse']
                    waiting_step.message = 'Rules exists for  %s from %s : %s %s/%s  (%s %% needed )' % (link,tape_replica,rule_link,
                                                                               str(rule['locks_ok_cnt']),str(files),str(level))
                    waiting_step.status = 'active'
                    temp =  {'datasets':[{'dataset':dataset,'disk':rse,'total_files':int(files), 'level':level,
                                          'staged_files':int(rule['locks_ok_cnt']),'tape':replicas['tape'][0]['rse']}]}
                    waiting_step.set_config(temp)
                    if int(files) - int(rule['locks_ok_cnt']) > 500:
                        delay = delay * 4
            else:
                if len(replicas['tape'])==0:
                    waiting_step.status = 'failed'
                    waiting_step.message = 'No replicas'
                else:
                    # make rule
                    waiting_step.status = 'active'
                    temp =  {'datasets':[{'dataset':dataset,'disk':rse,'total_files':int(files),'staged_files':0,
                                          'tape':replicas['tape'][0]['rse']}]}
                    if level != 0:
                        temp['level'] = level
                    waiting_step.set_config(temp)
                    link = '<a href="https://rucio-ui.cern.ch/did?name={name}">{name}</a>'.format(name=str(dataset))
                    waiting_step.message = '%s should be pre staged from %s by rule %s  (%s %% needed )'%(link,
                                                                                         replicas['tape'][0]['rse'],rse,str(level))
                    step.save()
                    #if waiting_step.get_config('do_rule') and (waiting_step.get_config('do_rule')=='Yes') :
                    if step.request.cstatus not in ['test']:
                        ddm.add_replication_rule(dataset, rse ,copies=1, lifetime=30*86400, weight='freespace',
                                                 activity='Staging', notify='P')
            if waiting_step.attempt > max_attempts:
                waiting_step.status = 'failed'
            else:
                waiting_step.execution_time = waiting_step.execution_time + timedelta(hours=delay)
            if error_message:
                waiting_step.message = 'Error for of %s: %s ' % (str(datasets),error_message)
        waiting_step.save()
    if approve_step:
        step.status = 'Approved'
        step.save()
        if step.request.cstatus not in ['test','approved']:
            set_request_status('cron', step.request.reqid, 'approved', 'Automatic pre action approve',
                               'Request was automatically approved')


def set_test_waiting(request,slice):
    step=StepExecution.objects.filter(slice=InputRequestList.objects.get(request=request,slice=slice))[0]
    step.status = 'Waiting'
    step.save()
    waiting_step = WaitingStep()
    waiting_step.step = step.id
    waiting_step.request = step.request
    waiting_step.create_time = timezone.now()
    waiting_step.execution_time = timezone.now()
    waiting_step.attempt = 0
    waiting_step.action = 1
    waiting_step.status = 'active'
    waiting_step.save()

@login_required(login_url=OIDC_LOGIN_URL)
def predefinition_action(request, wstep_id):

    try:
        waiting_step = WaitingStep.objects.get(id=wstep_id)
        action = WaitingStep.ACTIONS[waiting_step.action]

    except:
        return HttpResponseRedirect('/')



    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'PreDefiniton action with ID = %s' % wstep_id,
        'waiting_step': waiting_step,
        'action': action['description'],
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'prodtask/_waiting_step_action.html', request_parameters)

@login_required(login_url=OIDC_LOGIN_URL)
def finish_action(request, wstep_id):

    try:
        waiting_step = WaitingStep.objects.get(id=wstep_id)
        step = StepExecution.objects.get(id=waiting_step.step)
        waiting_step.status = 'done'
        waiting_step.message = 'Action was finished manually'
        waiting_step.done_time = timezone.now()
        waiting_step.save()
        step.status = 'Approved'
        step.save()
        if step.request.cstatus not in ['test','approved']:
            set_request_status('cron', step.request.reqid, 'approved', 'Automatic pre action approve',
                               'Request was automatically approved')
    except Exception as e:
        _logger.error("Finish action exception %s" % str(e))
        return HttpResponseRedirect('/')

    return HttpResponseRedirect(
        reverse('prodtask:input_list_approve_full', args=[step.request_id]) + '#inputList' + str(step.slice.slice))

@login_required(login_url=OIDC_LOGIN_URL)
def tape_load_page(request):
    current_load = tape_current_load()

    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Current tape load',
        'current_load': list(current_load.items()),
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'prodtask/_tape_load.html', request_parameters)

def tape_current_load():
    waiting_steps = WaitingStep.objects.filter(status__in=['active','executing'], action=4)
    tape_stat = {}
    for waiting_step in waiting_steps:
        try:
            metadata = waiting_step.get_config('datasets')[0]
            if metadata:
                tape = metadata['tape']
                if tape not in tape_stat:
                    tape_stat[tape] = {'in':{'MC':0,'REPROCESSING':0,'GROUP':0},'queued':{'MC':0,'REPROCESSING':0,'GROUP':0}}
                tape_stat[tape]['in']['REPROCESSING'] += metadata['total_files'] - metadata['staged_files']
        except:
            pass
    return tape_stat


def push_waiting_step(wstep_id):
    waiting_step = WaitingStep.objects.get(id=wstep_id)
    waiting_step.execution_time = timezone.now()
    waiting_step.save()

@login_required(login_url=OIDC_LOGIN_URL)
def cancel_action(request, wstep_id):

    try:
        waiting_step = WaitingStep.objects.get(id=wstep_id)
        step = StepExecution.objects.get(id=waiting_step.step)
        waiting_step.status = 'canceled'
        waiting_step.message = 'Action was canceled manually'
        waiting_step.done_time = timezone.now()
        waiting_step.save()
    except Exception as e:
        _logger.error("Cancel action exception %s" % str(e))
        return HttpResponseRedirect('/')

    return HttpResponseRedirect(
        reverse('prodtask:input_list_approve_full', args=[step.request_id]) + '#inputList' + str(step.slice.slice))