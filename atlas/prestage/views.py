import json

from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction
from datetime import timedelta

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import  StepExecution, InputRequestList, ProductionTask, ProductionDataset

from decimal import Decimal
from datetime import datetime
from rest_framework.decorators import api_view
from rest_framework.response import Response
import logging
# import os
from django.utils import timezone

from django.contrib.auth.decorators import login_required

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render

from atlas.prodtask.task_actions import _do_deft_action

_logger = logging.getLogger('prodtaskwebui')


def test_step_action(step_action_ids):
    pass

def check_staging_task(step_action_ids):
    ddm = DDM()
    #todo name and config
    config = ActionDefault.objects.get(name='stage_creation').get_config()
    delay = config['delay']
    max_waite_time = config['max_waite_time']
    rule = config['rule']
    for waiting_step in step_action_ids:
        try:
            check_tasks_for_prestage(waiting_step, ddm, rule, delay, max_waite_time)
        except Exception, e:
            _logger.error("Check replicas problem %s" % str(e))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()
    pass



def start_stagind_task(task):
    #Send resume command
    if(task.status in ['staging','waiting']):
        _do_deft_action('mborodin',int(task.id),'resume_task')
    pass


def perfom_dataset_stage(input_dataset, ddm, rule, lifetime, replicas=None):
    try:
        _logger.info('%s should be pre staged rule %s  '%(input_dataset,rule))
        rse = ddm.dataset_active_rule_by_rse(input_dataset, rule)
        if rse:
            return rse['id']
        if not replicas:
            ddm.add_replication_rule(input_dataset, rule, copies=1, lifetime=lifetime*86400, weight='freespace',
                                    activity='Staging', notify='P')
        else:
            ddm.add_replication_rule(input_dataset, rule, copies=1, lifetime=lifetime*86400, weight='freespace',
                                    activity='Staging', notify='P',source_replica_expression=replicas)

        return True
    except Exception, e:
        _logger.error("Can't create rule %s" % str(e))
        return False

def create_staging_action(input_dataset,task,ddm,rule,replicas=None,source=None,lifetime=None):

    #todo name and config
    config = ActionDefault.objects.get(name='active_staging').get_config()
    if not lifetime:
        lifetime = config['lifetime']
    if not DatasetStaging.objects.filter(dataset=input_dataset,status__in=DatasetStaging.ACTIVE_STATUS).exists():

        dataset_staging = DatasetStaging()
        dataset_staging.dataset = input_dataset
        dataset_staging.total_files = ddm.dataset_metadata(input_dataset)['length']
        dataset_staging.staged_files = 0
        dataset_staging.status = 'queued'
        if source:
            dataset_staging.source = source
        dataset_staging.save()

        if perfom_dataset_stage(input_dataset,ddm,rule,lifetime,replicas):
            dataset_staging.status = 'staging'
            dataset_staging.start_time = timezone.now()
            dataset_staging.save()
    else:
        dataset_staging = DatasetStaging.objects.filter(dataset=input_dataset,status__in=DatasetStaging.ACTIVE_STATUS).last()
    # Create new action
    if StepAction.objects.filter(step=task.step_id,action = 6,status__in=['active','executing']).exists():
        action_step = StepAction.objects.filter(step=task.step_id,action = 6,status__in=['active','executing']).last()
    else:
        action_step = StepAction()
        action_step.action = 6
        #todo add config
        action_step.create_time = timezone.now()
        action_step.set_config({'delay':config['delay']})
        step = task.step
        action_step.step = step.id
        level = None
        if step.get_task_config('PDAParams'):
            try:
                waiting_parameters_from_step = _parse_action_options(step.get_task_config('PDAParams'))
                if waiting_parameters_from_step.get('special'):
                    waiting_parameters_from_step = _parse_action_options(step.get_task_config('PDAParams'))
                    level = int(waiting_parameters_from_step.get('level'))
                    if level> 100:
                        level= 100
                    if level < 0:
                        level = 0
            except Exception, e:
                _logger.error(" %s" % str(e))
        if not level:
            level = config['level']
        action_step.set_config({'level':level})
        action_step.set_config({'lifetime':lifetime})
        action_step.set_config({'rule': rule})
        if replicas:
            action_step.set_config({'source_replica': replicas})
        if source:
            action_step.set_config({'tape': source})
        action_step.execution_time = timezone.now() + timedelta(hours=config['delay'])
        action_step.attempt = 0
        action_step.status = 'active'

        action_step.request = task.request
        action_step.save()
    action_dataset = ActionStaging()
    action_dataset.task = task.id
    action_dataset.dataset_stage = dataset_staging
    action_dataset.step_action = action_step
    #todo add share
    action_dataset.save()


def create_prestage(task,ddm,rule, special=False):
    input_dataset = task.primary_input
    #check that's only Tape replica
    replicas = ddm.full_replicas_per_type(input_dataset)
    if (len(replicas['data']) > 0):
        start_stagind_task(task)
        return True
    else:
        #No data replica - create a rule
        source_replicas = None
        input = None
        if special:
            rule, source_replicas, input =ddm.get_replica_pre_stage_rule(input_dataset)
        else:
            if replicas['tape']:
                input = [x['rse'] for x in replicas['tape']]
                if len(input) == 1:
                    input = input[0]
        create_staging_action(input_dataset,task,ddm,rule,source_replicas,input)


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


def check_tasks_for_prestage(action_step_id, ddm, rule, delay, max_waite_time):
    action_step = StepAction.objects.get(id=action_step_id)
    action_step.attempt += 1
    step = StepExecution.objects.get(id=action_step.step)
    special = False
    if step.get_task_config('PDAParams'):
        try:
            waiting_parameters_from_step = _parse_action_options(step.get_task_config('PDAParams'))
            if waiting_parameters_from_step.get('special'):
                special = True
        except Exception, e:
            _logger.error(" %s" % str(e))
    production_request = step.request
    tasks = ProductionTask.objects.filter(step=step)
    current_time = timezone.now()
    #todo add timeout
    if (not tasks) and (production_request.cstatus != 'approved'):
        action_step.status = 'failed'
        action_step.message = 'No tasks were defined'
        action_step.done_time = current_time
        action_step.save()
        return
    finish_action = True
    for task in tasks:
        if (task.status in ['staging','waiting']) and (not ActionStaging.objects.filter(task=task.id).exists()):
            try:
                create_prestage(task,ddm,rule,special)
            except Exception, e:
                _logger.error("Check replicas problem %s" % str(e))
                finish_action = False
    if finish_action and (production_request.cstatus != 'approved'):
        action_step.status = 'done'
        action_step.message = 'All task checked'
        action_step.done_time = current_time
    else:
        action_step.execution_time = action_step.execution_time + timedelta(hours=delay)
        action_step.status = 'active'
    action_step.save()


def do_staging(action_step_id, ddm):
    action_step = StepAction.objects.get(id=action_step_id)
    action_step.attempt += 1
    level = action_step.get_config('level')
    if not ActionStaging.objects.filter(step_action=action_step).exists():
        action_step.status = 'failed'
        action_step.message = 'No tasks were defined'
        action_step.done_time =  timezone.now()
        action_step.save()
        return
    action_finished = True
    for action_stage in ActionStaging.objects.filter(step_action=action_step):
        dataset_stage = action_stage.dataset_stage
        if dataset_stage.status == 'done':
            task = ProductionTask.objects.get(id=action_stage.task)
            start_stagind_task(task)
        if dataset_stage.status == 'staging':
            existed_rule = ddm.dataset_active_rule_by_rse(dataset_stage.dataset, action_step.get_config('rule'))
            if existed_rule:
                if not dataset_stage.rse:
                    dataset_stage.rse = existed_rule['id']
                dataset_stage.staged_files = int(existed_rule['locks_ok_cnt'])
                if ((level == 100) and (dataset_stage.staged_files == dataset_stage.total_files)) or (
                        (float(dataset_stage.staged_files) / float(dataset_stage.total_files)) >= (float(level) / 100.0)):
                    task = ProductionTask.objects.get(id=action_stage.task)
                    start_stagind_task(task)
                if (dataset_stage.staged_files != dataset_stage.total_files):
                    action_finished = False
                else:
                    dataset_stage.status = 'done'
                    dataset_stage.end_time = timezone.now()
                dataset_stage.save()
            else:
                action_finished = False
                if perfom_dataset_stage(dataset_stage.dataset, ddm, action_step.get_config('rule'),
                                        action_step.get_config('lifetime'),action_step.get_config('source_replica')):
                    dataset_stage.start_time = timezone.now()
                    dataset_stage.save()
        elif dataset_stage.status == 'queued':
            action_finished = False
            if perfom_dataset_stage(dataset_stage.dataset, ddm, action_step.get_config('rule'), action_step.get_config('lifetime'),action_step.get_config('source_replica')):
                dataset_stage.status = 'staging'
                dataset_stage.start_time = timezone.now()
                dataset_stage.save()
    if action_finished :
        action_step.status = 'done'
        action_step.message = 'All task started'
        action_step.done_time = timezone.now()
    else:
        action_step.execution_time = action_step.execution_time + timedelta(hours=action_step.get_config('delay'))
        action_step.status = 'active'
    action_step.save()



def activate_staging(step_action_ids):
    ddm = DDM()
    #todo name and config
    for waiting_step in step_action_ids:
        try:
            do_staging(waiting_step, ddm)
        except Exception, e:
            _logger.error("Check replicas problem %s" % str(e))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()
    pass

def follow_staging(step_action_ids):
    pass

def find_action_to_execute():
    action_step_todo = StepAction.objects.filter(status='active',execution_time__lte=timezone.now())
    process_actions(action_step_todo)

def process_actions(action_step_todo):
    for action_step in action_step_todo:
        action_step.status = 'executing'
        action_step.save()
    executing_actions = {}
    for action_step in action_step_todo:
        executing_actions[action_step.action] = executing_actions.get(action_step.action,[]) + [action_step.id]
    for action in executing_actions:
        if action == 1:
            test_step_action(executing_actions[action])
        if action == 6:
            activate_staging(executing_actions[action])
        elif action == 5:
            check_staging_task(executing_actions[action])
        elif action == 7:
            follow_staging(executing_actions[action])


def prestage_by_tape(request):
    try:
        result = []
        tape_stat = {}
        total = {'requested':0,'staged':0,'done':0}
        staging_requests = DatasetStaging.objects.filter(status__in=['staging','done'])
        for staging_request in staging_requests:
            if staging_request.total_files and staging_request.source:
                if staging_request.source not in tape_stat:
                    tape_stat[staging_request.source] = {'requested':0,'staged':0,'done':0}
                if staging_request.status == 'done':
                    tape_stat[staging_request.source]['done'] += staging_request.total_files
                    total['done'] += staging_request.total_files

                else:
                    tape_stat[staging_request.source]['requested'] += staging_request.total_files
                    total['requested'] += staging_request.total_files
                    if staging_request.staged_files:
                        tape_stat[staging_request.source]['staged'] += staging_request.staged_files
                        total['staged'] += staging_request.staged_files
        for tape in tape_stat:
            result.append({'name':tape,'requested':tape_stat[tape]['requested'],'staged':tape_stat[tape]['staged'],
                           'done':tape_stat[tape]['done'],
                           'percent':int(100*(float(tape_stat[tape]['staged']+tape_stat[tape]['done'])/float(tape_stat[tape]['done']+tape_stat[tape]['requested']) ))})
        result.append({'name':'total','requested':total['requested'],'staged':total['staged'],
                           'done':total['done'],
                           'percent':int(100*(float(total['staged']+total['done'])/float(total['done']+total['requested']))) })
        print result
    except:
        return HttpResponseRedirect('/')
    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Tape stats for active requests',
        'result_table': result,
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'prestage/prestage_by_tape.html', request_parameters)


def step_action_in_request(request, reqid):

    try:
        action_steps = StepAction.objects.filter(request=reqid)
        result = []
        for action_step in action_steps:
            current_action = action_step.__dict__
            if action_step.action == 6:
                if ActionStaging.objects.filter(step_action=action_step).exists():
                    staging = ActionStaging.objects.filter(step_action=action_step)[0]
                    dataset = staging.dataset_stage.dataset
                    current_action['task'] = staging.task
                    current_action['total_files'] = staging.dataset_stage.total_files
                    current_action['staged_files'] = staging.dataset_stage.staged_files
                    rse = staging.dataset_stage.rse
                    current_action['dataset'] = '<a href="https://rucio-ui.cern.ch/did?name={name}">Dataset</a>'.format(name=str(dataset))
                    current_action['rse'] = '<a href="https://rucio-ui.cern.ch/rule?rule_id={rule_id}">rule</a>'.format(
                        rule_id=rse, rule_rse=action_step.get_config('rule'))
                    if action_step.get_config('tape'):
                        current_action['tape'] = str(action_step.get_config('tape'))

            result.append(current_action)

    except:
        return HttpResponseRedirect('/')
    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'action for request ID = %s' % reqid,
        'result_table': result,
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'prestage/step_action_in_request.html', request_parameters)

def step_action(request, wstep_id):

    try:
        action_step = StepAction.objects.get(id=wstep_id)
        message = ''
        task = None
        if action_step.action == 6:
            if ActionStaging.objects.filter(step_action=action_step).exists():
                staging = ActionStaging.objects.filter(step_action=action_step)[0]
                dataset = staging.dataset_stage.dataset
                total_files = staging.dataset_stage.total_files
                staged_files = staging.dataset_stage.staged_files
                rse = staging.dataset_stage.rse
                task = staging.task
                link = '<a href="https://rucio-ui.cern.ch/did?name={name}">{name}</a>'.format(name=str(dataset))
                rule_link = '<a href="https://rucio-ui.cern.ch/rule?rule_id={rule_id}">{rule_rse}</a>'.format(
                    rule_id=rse, rule_rse=action_step.get_config('rule'))
                if action_step.get_config('tape'):
                    tape_replica = str(action_step.get_config('tape'))
                else:
                    tape_replica = 'tape'
                message = 'Rules exists for  %s from %s : %s %s/%s  (%s %% needed )' % (link, tape_replica, rule_link,
                                                                                                     str(staged_files),
                                                                                                     str(total_files),
                                                                                        str(action_step.get_config('level')))

    except:
        return HttpResponseRedirect('/')

    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'action with ID = %s' % wstep_id,
        'waiting_step': action_step,
        'message': message,
        'task' : task,
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'prestage/_step_action.html', request_parameters)