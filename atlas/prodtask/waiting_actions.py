from datetime import timedelta

from django.http import HttpResponseRedirect

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import WaitingStep, StepExecution, InputRequestList, ProductionTask, ProductionDataset
from django.shortcuts import render
from django.utils import timezone
import logging

from atlas.prodtask.views import set_request_status, make_child_update

_logger = logging.getLogger('prodtaskwebui')

def find_waiting():
    waiting_step_todo = WaitingStep.objects.filter(status='active',execution_time__lte=timezone.now())
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
            except Exception,e:
                print str(e)
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
            except Exception,e:
                _logger.error("Check replicas problem %s" % str(e))
                print str(e)
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
            except Exception,e:
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
                waiting_step.message = 'Error for of %s: %s ' % (str(datasets)[:1000],str(e))
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
        except Exception,e:
            replicas = {'data':None,'tape':None}
            error_message = str(e)
        if len(replicas['data'])>0:
            waiting_step.status = 'done'
            waiting_step.message = '%s have >0 datadisk replicas' %(str(dataset))
            waiting_step.done_time = timezone.now()
            approve_step = True
        else:
            waiting_step.attempt += 1
            rules = ddm.dataset_active_datadisk_rule(dataset)
            if len(rules) > 0:
                files = ddm.dataset_metadata(dataset)['length']
                if (float(rules[0]['locks_ok_cnt']) / float(files)) >=0.9:
                    waiting_step.status = 'done'
                    waiting_step.message = '%s has >0.9 files pre staged ' % (str(dataset))
                    waiting_step.done_time = timezone.now()
                    approve_step = True
                else:
                    waiting_step.message = 'Rules exists for  %s: %s %s/%s' % (str(dataset),rules[0]['rse_expression'], str(rules[0]['locks_ok_cnt']),str(files))
                    waiting_step.status = 'active'
            else:
                if len(replicas['tape'])==0:
                    waiting_step.status = 'failed'
                    waiting_step.message = 'No replicas'
                else:
                    # make rule
                    waiting_step.status = 'active'
                    disk = ddm.find_disk_for_tape(replicas['tape'][0]['rse'])
                    temp =  {'datasets':[{'dataset':dataset,'disk':disk['rse'],'tape':replicas['tape'][0]['rse']}]}
                    waiting_step.set_config(temp)
                    waiting_step.message = '%s should be pre staged from %s to %s'%(dataset,replicas['tape'][0]['rse'],disk['rse'])
                    step.save()
                    if waiting_step.get_config('do_rule') and (waiting_step.get_config('do_rule')=='Yes') :
                        ddm.add_replication_rule(dataset, disk['rse'], 'Staging')
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
