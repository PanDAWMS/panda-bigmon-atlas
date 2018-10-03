from datetime import timedelta

from django.http import HttpResponseRedirect

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import WaitingStep, StepExecution, InputRequestList, ProductionTask
from django.shortcuts import render
from django.utils import timezone
import logging

from atlas.prodtask.views import set_request_status

_logger = logging.getLogger('prodtaskwebui')

def find_waiting():
    waiting_step_todo = WaitingStep.objects.filter(status='active',execution_time__lte=timezone.now())
    for waiting_step in waiting_step_todo:
        waiting_step.status = 'executing'
        waiting_step.save()
    check2replicas = []
    for waiting_step in waiting_step_todo:
        if waiting_step.action == 1:
            postponed_step(waiting_step.id)
        if waiting_step.action == 2:
            check2replicas.append(waiting_step.id)
    if check2replicas:
        ddm= DDM()
        max_attempts = WaitingStep.ACTIONS[2]['attempts']
        delay =  WaitingStep.ACTIONS[2]['delay']
        for waiting_step in check2replicas:
            try:
                check_two_replicas(waiting_step, ddm, max_attempts, delay)
            except Exception,e:
                waiting_step.status = 'active'
                waiting_step.save()
                _logger.error("Check replicas problem %s" % str(e))


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



def get_slice_input_datasets(container, ddm):
    if 'tid' not in container:
        datasets = ddm.dataset_in_container(container.strip())
        return datasets
    else:
        return container.strip()



def check_two_replicas(waiting_step_id, ddm, max_attempts, delay):
    waiting_step = WaitingStep.objects.get(id=waiting_step_id)
    step = StepExecution.objects.get(id=waiting_step.step)
    approve_step = False
    if (step.step_parent_id != step.id) and (step.step_parent.status not in ['Skipped','NotCheckedSkipped'] ):
        waiting_step.status = 'cancelled'
        waiting_step.save()
        approve_step = True
    else:
        datasets = get_slice_input_datasets(step.slice.dataset_id, ddm)
        enough_replicas = True
        error_message = ''
        for dataset in datasets:
            try:
                if ddm.number_of_full_replicas(dataset) < 2:
                    enough_replicas = False
            except Exception,e:
                enough_replicas = False
                error_message = str(e)
        if enough_replicas:
            waiting_step.status = 'done'
            waiting_step.message = 'All %s have >=2 replicas' %(str(datasets))
            waiting_step.done_time = timezone.now()
            approve_step = True
        else:
            waiting_step.attempt += 1
            if waiting_step.attempt > max_attempts:
                waiting_step.status = 'failed'
            else:
                waiting_step.status = 'active'
                waiting_step.execution_time = waiting_step.execution_time + timedelta(hours=delay)
            if error_message:
                waiting_step.message = 'Error for of %s: %s ' % (str(datasets),str(e))
            waiting_step.message = 'Some of %s have <2 replicas' % (str(datasets))
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
