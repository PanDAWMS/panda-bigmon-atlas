import json

from django.db.models import Q

from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask
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

def check_staging_task(step_action_ids, check_archive=False):
    ddm = DDM()
    config = ActionDefault.objects.get(name='stage_creation').get_config()
    delay = config['delay']
    max_waite_time = config['max_waite_time']
    rule = config['rule']
    for waiting_step in step_action_ids:
        try:
            check_tasks_for_prestage(waiting_step, ddm, rule, delay, max_waite_time, check_archive)
        except Exception as e:
            _logger.error("Check replicas problem %s" % str(e))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()
    pass


class ResourceQueue(object):
    def __init__(self, resource_name):
        self.queue = []
        self.total_queued = 0
        self.resource_name = resource_name
        pass

    def priorities_queue(self):

        return

    def running_level(self):
        return 0

    def get_limits(self):
        self.minimum_level = 0
        self.maximum_level = 0
        self.continious_percentage = 0.0
        pass

    def do_submission(self):
        pass

    def find_submission(self):
        running_level = self.running_level()
        _logger.info("Find queued to submit {resource}: {maximum}/{minimum}/{percent} - running/queued "
                     "- {running}/{queued}".format(resource=self.resource_name,maximum=self.maximum_level,minimum=self.minimum_level,
                                                   percent=self.continious_percentage,running=running_level,queued=self.total_queued))
        if (float(running_level) / float(self.maximum_level)) < (self.continious_percentage / 100.0):
            self.priorities_queue()
            to_submit_list = []
            to_submit_value = 0
            required_submission = self.maximum_level - running_level
            if required_submission + self.minimum_level > self.total_queued:
                to_submit_list = self.queue
                to_submit_value = self.total_queued
            else:
                i = 0
                while (to_submit_value < required_submission):
                    if i > len(self.queue):
                        break
                    to_submit_list.append(self.queue[i])
                    to_submit_value += self.queue[i]['value']
                    i += 1
                if (i == (len(self.queue) - 1)) and (self.queue[-1]['value'] < self.minimum_level):
                    to_submit_list.append(self.queue[-1])
                    to_submit_value += self.queue[-1]['value']
            if ((running_level+to_submit_value) < self.minimum_level) and self.to_wait(to_submit_list):
                return []
            return to_submit_list

    def to_wait(self, submission_list):
        return False


class TestResource(ResourceQueue):

    def __init__(self, resource_name, test_queue):
        super().__init__(resource_name)
        self.__get_test_queue(test_queue)
        self.running_queue = [x for x in test_queue if x['status']=='running']
        self.get_limits()

    def __get_test_queue(self,test_queue):
        self.total_queued = 0
        for x in test_queue:
            if x['status'] == 'queued':
                self.total_queued += x['total']
                x['value'] =  x['total']
                self.queue.append(x)


    def running_level(self):
        running_level  = 0
        for x in self.running_queue :
            running_level += x['total'] - x['running']
        return running_level

    def priorities_queue(self):
        self.queue.sort(key=lambda x: x['value'])


    def get_limits(self):
        self.minimum_level = 5
        self.maximum_level = 100
        self.continious_percentage = 40

    def __submit(self, submission_list):
        ids = [x['id'] for x in submission_list]
        for x in self.queue:
            if x['id'] in ids:
                x['status'] = 'running'
                print(x)


    def do_submission(self):
        self.__submit(self.find_submission())


class TapeResource(ResourceQueue):

    def __init__(self, resource_name, ddm):
        super().__init__(resource_name)
        self.ddm = ddm
        self.__get_tape_queue()
        self.get_limits()


    def __get_tape_queue(self):
        queued_staging_request = DatasetStaging.objects.filter(source=self.resource_name,status='queued').values()
        self.total_queued = 0
        for x in queued_staging_request:
            x['value'] = x['total_files']
            self.total_queued += x['value']
            self.queue.append(x)

    def running_level(self):
        staing_requests =  DatasetStaging.objects.filter(source=self.resource_name,status__in=['staging']).values()
        running_level  = 0
        for x in staing_requests:
            running_level += x['total_files'] - x['staged_files']
        return running_level


    def get_limits(self):
        limits_config = ActionDefault.objects.get(type='Tape',name=self.resource_name)
        self.minimum_level = limits_config.get_config('minimum_level')
        self.maximum_level = limits_config.get_config('maximum_level')
        self.continious_percentage =limits_config.get_config('continious_percentage')

    def __submit(self, submission_list):
        #print(submission_list)
        if submission_list:
            total = sum([x['value'] for x in submission_list ])
            _logger.info("Submit rules for {resource}: {to_submit}, {total}".format(resource=self.resource_name,to_submit=len(submission_list),total=total))
            for x in submission_list:
                dataset_stagings = ActionStaging.objects.filter(dataset_stage=x['id'])
                rule = None
                lifetime = None
                source_replica = None
                for dataset_staging in dataset_stagings:
                    if dataset_staging.step_action.status == 'active':
                        rule = dataset_staging.step_action.get_config('rule')
                        lifetime = dataset_staging.step_action.get_config('lifetime')
                        source_replica =  dataset_staging.step_action.get_config('source_replica')
                if rule and lifetime:

                    #print(x['dataset'], rule)
                    try:
                        existed_rule = self.ddm.dataset_active_rule_by_rse(x['dataset'], rule)
                        if not existed_rule:
                            if not source_replica:
                                _logger.info("Submit rule for {resource}: {dataset} {rule}".format(resource=self.resource_name,
                                                                                                   dataset=x['dataset'],
                                                                                                   rule=rule))
                                self.ddm.add_replication_rule(x['dataset'], rule, copies=1, lifetime=lifetime*86400, weight='freespace',
                                                        activity='Staging', notify='P')
                            else:
                                _logger.info("Submit rule for {resource}: {dataset} {rule} {source}".format(resource=self.resource_name,
                                                                                                   dataset=x['dataset'],
                                                                                                   rule=rule, source=source_replica))
                                self.ddm.add_replication_rule(x['dataset'], rule, copies=1, lifetime=lifetime*86400, weight='freespace',
                                                        activity='Staging', notify='P', source_replica_expression=source_replica)
                        staging =  DatasetStaging.objects.get(id=x['id'])
                        staging.status = 'staging'
                        staging.save()
                    except Exception as e:
                        _logger.error("Problem during submission {resource}: {error}".format(resource=self.resource_name,
                                                                   error=str(e)))
                else:
                    _logger.error("Problem during submission for dataset {dataset_staging}".format(dataset_staging=x['id']))


    def to_wait(self, submission_list):
        oldest = min(submission_list,key=lambda x:x['start_time'])['start_time']
        return (timezone.now() - oldest) < timedelta(days=1)

    def do_submission(self):
        self.__submit(self.find_submission())


class TestTapeResource(TapeResource):
    def __init__(self, resource_name, limits):
        self.limits = limits
        super().__init__(resource_name)


    def get_limits(self):
        self.minimum_level = self.limits['minimum_level']
        self.maximum_level = self.limits['maximum_level']
        self.continious_percentage = self.limits['continious_percentage']


def start_stagind_task(task):
    #Send resume command
    if(task.status in ['staging','waiting','paused']):
        _logger.info('Resume task after pre stage %s ' % (str(task.id)))
        _do_deft_action('mborodin',int(task.id),'resume_task')
    pass

def send_use_archive_task(task):
    #Send resume command
    _do_deft_action('mborodin',int(task.id),'change_split_rule','UZ','1')


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
    except Exception as e:
        _logger.error("Can't create rule %s" % str(e))
        return False

def create_staging_action(input_dataset,task,ddm,rule,config,replicas=None,source=None,lifetime=None):

    if not lifetime:
        lifetime = config['lifetime']
    if not DatasetStaging.objects.filter(dataset=input_dataset,status__in=DatasetStaging.ACTIVE_STATUS).exists():
        if DatasetStaging.objects.filter(dataset=input_dataset,status='done').exists():
            dataset_staging = DatasetStaging.objects.filter(dataset=input_dataset,status='done').first()
            dataset_staging.update_time = None
            dataset_staging.rse = None
            dataset_staging.source = None
            dataset_staging.end_time = None
        else:
            dataset_staging = DatasetStaging()
            dataset_staging.dataset = input_dataset
        dataset_staging.total_files = ddm.dataset_metadata(input_dataset)['length']
        dataset_staging.staged_files = 0
        dataset_staging.status = 'queued'
        if source:
            dataset_staging.source = source
        dataset_staging.start_time = timezone.now()
        dataset_staging.save()

        # if perfom_dataset_stage(input_dataset,ddm,rule,lifetime,replicas):
        #     dataset_staging.status = 'staging'
        #     dataset_staging.start_time = timezone.now()
        #     dataset_staging.save()
    else:
        dataset_staging = DatasetStaging.objects.filter(dataset=input_dataset,
                                                        status__in=DatasetStaging.ACTIVE_STATUS).last()
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
                if waiting_parameters_from_step.get('level',None):
                    level = int(waiting_parameters_from_step.get('level'))
                    if level> 100:
                        level= 100
                    elif level < -1:
                        level = 0
            except Exception as e:
                _logger.error(" %s" % str(e))
        if not level:
            level = config['level']
        if level == -1:
            if dataset_staging.total_files:
                if dataset_staging.total_files < 1000:
                    level = 90
                else:
                    level = 95
            else:
                level = 95
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
    action_dataset.save()

def submit_all_tapes():
    ddm = DDM()
    for tape in ActionDefault.objects.filter(type='Tape'):
        if tape.get_config('active'):
            if DatasetStaging.objects.filter(source=tape.name,status='queued').exists():
                resource_tape = TapeResource(tape.name,ddm)
                resource_tape.do_submission()

def create_prestage(task,ddm,rule, input_dataset,config, special=False):
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
            special_datasets = list(InputRequestList.objects.filter(~Q(is_hide=True),request=29036).values_list('dataset',flat=True))
            if ddm.rucio_convention(input_dataset)[1] in special_datasets:
                rule, source_replicas, input = 'CERN-PROD_DATADISK', 'CERN-PROD_RAW',  'CERN-PROD_RAW'
            else:
                rule, source_replicas, input = ddm.get_replica_pre_stage_rule(input_dataset)
            if input == 'INFN-T1_DATATAPE':
                source_replicas = 'INFN-T1_DATATAPE'
        else:
            if replicas['tape']:
                input = [x['rse'] for x in replicas['tape']]
                if len(input) == 1:
                    input = input[0]
        create_staging_action(input_dataset,task,ddm,rule,config,source_replicas,input)


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


def create_follow_prestage_action(task):
    config =  ActionDefault.objects.get(name='active_archive_staging').get_config()
    action_step = StepAction()
    action_step.action = 9
    action_step.create_time = timezone.now()
    action_step.set_config({'delay': config['delay']})
    action_step.set_config({'task': task.id})
    step = task.step
    action_step.step = step.id
    action_step.execution_time = timezone.now() + timedelta(hours=config['delay'])
    action_step.attempt = 0
    action_step.status = 'active'
    action_step.request = task.request
    action_step.save()

def check_tasks_for_prestage(action_step_id, ddm, rule, delay, max_waite_time, check_archive=False):
    action_step = StepAction.objects.get(id=action_step_id)
    action_step.attempt += 1
    step = StepExecution.objects.get(id=action_step.step)
    special = False
    if step.get_task_config('PDAParams'):
        try:
            waiting_parameters_from_step = _parse_action_options(step.get_task_config('PDAParams'))
            if waiting_parameters_from_step.get('special'):
                special = True
        except Exception as e:
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
        step.remove_project_mode('toStaging')
        step.save()
        return
    finish_action = True
    fail_action = False
    for task in tasks:
        if (task.status in ['staging','waiting']) and (not ActionStaging.objects.filter(task=task.id).exists()):
            try:
                if check_archive:
                    input_dataset = find_archive_dataset(task.input_dataset,ddm)
                    if input_dataset:
                        send_use_archive_task(task)
                        create_prestage(task, ddm, rule, input_dataset, ActionDefault.objects.get(name='active_archive_staging').get_config(), special)
                        create_follow_prestage_action(task)
                    else:
                        fail_action = True
                else:
                    create_prestage(task,ddm,rule,task.input_dataset, ActionDefault.objects.get(name='active_staging').get_config(),special)
            except Exception as e:
                _logger.error("Check replicas problem %s" % str(e))
                finish_action = False
    if finish_action and (production_request.cstatus != 'approved'):
        action_step.status = 'done'
        action_step.message = 'All task checked'
        action_step.done_time = current_time
        step.remove_project_mode('toStaging')
        step.save()
    else:
        action_step.execution_time = current_time + timedelta(hours=delay)
        action_step.status = 'active'
    if fail_action:
        action_step.status = 'failed'
        action_step.message = 'No archive dataset is found'
        action_step.done_time = current_time
        step.remove_project_mode('toStaging')
        step.save()
    action_step.save()


def find_archive_dataset(dataset_name,ddm):
    for task in ProductionTask.objects.filter(primary_input=dataset_name[dataset_name.find(':')+1:],status='done'):
        if TTask.objects.get(id=task.id).jedi_task_parameters['processingType'] == 'archive':
            if ddm.dataset_exists(task.output_dataset):
                return task.output_dataset
    return None



def do_staging(action_step_id, ddm):
    action_step = StepAction.objects.get(id=action_step_id)
    action_step.attempt += 1
    level = action_step.get_config('level')
    current_time = timezone.now()
    if not ActionStaging.objects.filter(step_action=action_step).exists():
        action_step.status = 'failed'
        action_step.message = 'No tasks were defined'
        action_step.done_time = current_time
        action_step.save()
        return
    action_finished = True
    for action_stage in ActionStaging.objects.filter(step_action=action_step):
        dataset_stage = action_stage.dataset_stage
        task = ProductionTask.objects.get(id=action_stage.task)
        if dataset_stage.status == 'done':
            start_stagind_task(task)
        if dataset_stage.status == 'staging':
            no_update = dataset_stage.update_time and \
                    ((current_time-dataset_stage.update_time) < timedelta(hours=2*int(action_step.get_config('delay'))))
            if (not dataset_stage.rse) or (not no_update ):
                existed_rule = ddm.dataset_active_rule_by_rse(dataset_stage.dataset, action_step.get_config('rule'))
                if existed_rule:
                        dataset_stage.rse = existed_rule['id']
                        dataset_stage.staged_files = int(existed_rule['locks_ok_cnt'])
                        dataset_stage.update_time = current_time
                        if ((existed_rule['expires_at']-timezone.now().replace(tzinfo=None))<timedelta(days=5)) and \
                                (task.status not in ['done','finished','broken','aborted']):
                            try:
                                ddm.change_rule_lifetime(existed_rule['id'],15*86400)
                            except Exception as e:
                                _logger.error("Check replicas problem %s" % str(e))
                else:
                    action_finished = False
                    if perfom_dataset_stage(dataset_stage.dataset, ddm, action_step.get_config('rule'),
                                            action_step.get_config('lifetime'),
                                            action_step.get_config('source_replica')):
                        dataset_stage.start_time = current_time

            if ((level == 100) and (dataset_stage.staged_files == dataset_stage.total_files)) or \
                    (((dataset_stage.total_files-dataset_stage.staged_files)<= ActionDefault.FILES_TO_RELEASE) and
                     ((float(dataset_stage.staged_files) / float(dataset_stage.total_files)) >= (float(level) / 100.0))):
                start_stagind_task(task)
            if dataset_stage.staged_files != dataset_stage.total_files:
                action_finished = False
            else:
                dataset_stage.status = 'done'
                dataset_stage.update_time = current_time
                dataset_stage.end_time = current_time
            dataset_stage.save()

        elif dataset_stage.status == 'queued':
            action_finished = False
            # if perfom_dataset_stage(dataset_stage.dataset, ddm, action_step.get_config('rule'),
            #                         action_step.get_config('lifetime'), action_step.get_config('source_replica')):
            #     dataset_stage.status = 'staging'
            #     dataset_stage.start_time = current_time
            #     dataset_stage.save()
    if action_finished :
        action_step.status = 'done'
        action_step.message = 'All task started'
        action_step.done_time = current_time
    else:
        action_step.execution_time = current_time + timedelta(hours=action_step.get_config('delay'))
        action_step.status = 'active'
    action_step.save()








def activate_staging(step_action_ids):
    ddm = DDM()
    #todo name and config
    for waiting_step in step_action_ids:
        try:
            do_staging(waiting_step, ddm)
        except Exception as e:
            _logger.error("Check replicas problem %s" % str(e))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()
    pass

def follow_archive_staging(step_action_ids):

    for step_action in step_action_ids:
        try:
            current_time = timezone.now()
            action = StepAction.objects.get(id=step_action)
            task = ProductionTask.objects.get(id=action.get_config('task'))
            finish_action = False
            if task.status not in ['staging','waiting','registered'] + ProductionTask.NOT_RUNNING:
                send_use_archive_task(task)
                finish_action = True
            elif task.status in ProductionTask.NOT_RUNNING:
                finish_action = True
            if finish_action:
                action.status = 'done'
                action.message = 'All task started'
                action.done_time = current_time
            else:
                action.execution_time = current_time + timedelta(
                    hours=action.get_config('delay'))
                action.status = 'active'
            action.save()

        except Exception as e:
            _logger.error("Send use zip problem %s" % str(e))
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
        elif action == 6:
            activate_staging(executing_actions[action])
        elif action == 8:
            check_staging_task(executing_actions[action], True)
        elif action == 5:
            check_staging_task(executing_actions[action])
        elif action == 7:
            follow_staging(executing_actions[action])
        elif action == 9:
            follow_archive_staging(executing_actions[action])

def prestage_by_tape(request, reqid=None):
    try:
        result = []
        tape_stat = {}
        total = {'requested':0,'staged':0,'done':0}
        staging_requests = []
        if reqid:
            actions = StepAction.objects.filter(request=reqid,action=6 )
            for action in actions:
                for action_stage in ActionStaging.objects.filter(step_action=action):
                    staging_requests.append(action_stage.dataset_stage)
        else:
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
    except:
        return HttpResponseRedirect('/')
    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Tape stats for active requests',
        'result_table': result,
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'prestage/prestage_by_tape.html', request_parameters)

def prestage_by_tape_with_limits(request, reqid=None):
    try:

        total = {'files_queued':0,'files_staged':0, 'files_staging':0}
        result = []
        tapes = list(ActionDefault.objects.filter(type='Tape').order_by('id'))
        for tape in tapes:
            files_queued = 0
            files_staged = 0
            files_staging = 0

            datasets = DatasetStaging.objects.filter(source=tape.name,status__in=['staging', 'queued'])
            for dataset in datasets:
                if dataset.status == 'queued':
                    files_queued += dataset.total_files
                else:
                    files_staged += dataset.staged_files
                    files_staging += dataset.total_files - dataset.staged_files
            result.append({'name':tape.name,'minimum_level':tape.get_config('minimum_level'),
                           'maximum_level':tape.get_config('maximum_level'),
                           'continious_percentage':tape.get_config('continious_percentage'),
                           'files_queued':files_queued,'files_staged':files_staged,'files_staging':files_staging})
            total['files_queued'] += files_queued
            total['files_staged'] += files_staged
            total['files_staging'] += files_staging

        result.append({'name':'total','files_queued':total['files_queued'],'files_staged':total['files_staged'],
                           'files_staging':total['files_staging'] })
    except Exception as e:
        _logger.error("problem %s" % str(e))

        return HttpResponseRedirect('/')
    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Tape stats for active requests',
        'result_table': result,
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'prestage/prestage_by_tape_queued.html', request_parameters)


def step_action_in_request(request, reqid):

    try:
        action_steps = StepAction.objects.filter(request=reqid).order_by('id')
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


def replica_to_delete(reqid):
    action_steps = StepAction.objects.filter(request=reqid,action=6,status='done')
    ddm = DDM()
    result = {}
    for action_step in action_steps:
        for action_stage in ActionStaging.objects.filter(step_action=action_step):
            dataset_stage = action_stage.dataset_stage
            task = ProductionTask.objects.get(id=action_stage.task)
            if dataset_stage.status == 'done' and task.status == 'done':
                try:
                    ddm.get_rule(dataset_stage.rse)
                    if dataset_stage.source not in result:
                        result[dataset_stage.source] = {}
                        result[dataset_stage.source]['size'] = 0
                        result[dataset_stage.source]['rses'] = []
                    result[dataset_stage.source]['size'] += ddm.dataset_size(dataset_stage.dataset)
                    result[dataset_stage.source]['rses'].append(dataset_stage.rse)

                except:
                    pass
    return result


def replica_to_delete_dest(reqid):
    action_steps = StepAction.objects.filter(request=reqid,action=6,status='done')
    ddm = DDM()
    result = {}
    result_destination = {}
    for action_step in action_steps:
        for action_stage in ActionStaging.objects.filter(step_action=action_step):
            dataset_stage = action_stage.dataset_stage
            task = ProductionTask.objects.get(id=action_stage.task)
            if dataset_stage.status == 'done' and task.status == 'done':
                try:
                    ddm.get_rule(dataset_stage.rse)
                    if dataset_stage.source not in result:
                        result[dataset_stage.source] = {}
                        result[dataset_stage.source]['size'] = 0
                        result[dataset_stage.source]['rses'] = []
                    replica = ddm.dataset_replicas(dataset_stage.dataset)
                    result[dataset_stage.source]['size'] += ddm.dataset_size(dataset_stage.dataset)
                    result[dataset_stage.source]['rses'].append(dataset_stage.rse)
                    for x in replica:
                        if 'DATADISK' in x['rse']:
                            site = x['rse']
                            if site not in result_destination:
                                result_destination[site] = {}
                                result_destination[site]['size'] = 0
                                result_destination[site]['rses'] = []
                            result_destination[site]['size'] += ddm.dataset_size(dataset_stage.dataset)
                            result_destination[site]['rses'].append(dataset_stage.rse)
                            break

                except:
                    pass
    return result, result_destination


def todelete_action_in_request(request, reqid):

    try:
        result, result_dest = replica_to_delete_dest(reqid)
        #result = {'a':{'rses':[1,2],'size':1234566789}}
        to_display = [{'tape':x,'size':result[x]['size'],'total':len(result[x]['rses'])} for x in result.keys()]
        to_display.sort(key=lambda x:x['tape'])
        total = {'tape': 'total', 'size': sum([x['size'] for x in to_display]), 'total': sum([x['total'] for x in to_display])}
        to_display.append(total)
        to_display_dest = [{'tape':x,'size':result_dest[x]['size'],'total':len(result_dest[x]['rses'])} for x in result_dest.keys()]
        to_display_dest.sort(key=lambda x:x['tape'])
        total2 = {'tape': 'total', 'size': sum([x['size'] for x in to_display_dest]), 'total': sum([x['total'] for x in to_display_dest])}
        to_display_dest.append(total2)

    except:
        return HttpResponseRedirect('/')
    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Rules to delete for request ID = %s' % reqid,
        'result_table': to_display,
        'result_table_dest': to_display_dest,
        'parent_template' : 'prodtask/_index.html',
        }
    return render(request, 'prestage/todelete_by_request.html', request_parameters)


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
                if staging.dataset_stage.status in ['staging','done']:
                    if rse:
                        message = 'Rules exists for  %s from %s : %s %s/%s  (%s %% needed )' % (link, tape_replica, rule_link,
                                                                                                             str(staged_files),
                                                                                                             str(total_files),
                                                                                                str(action_step.get_config('level')))
                    else:
                        message = 'Rules submitted for  %s from %s : %s %s/%s  (%s %% needed )' % (link, tape_replica, action_step.get_config('rule'),
                                                                                                             str(staged_files),
                                                                                                             str(total_files),
                                                                                                str(action_step.get_config('level')))
                else:
                    message = 'Staging request is queued for {link_dataset} from {source}'.format(link_dataset=link,source=staging.dataset_stage.source)

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


@api_view(['POST'])
@login_required(login_url='/prodtask/login/')
def finish_action(request, action, action_id):

    try:
        action_step = StepAction.objects.get(id=action_id)
        if action_step.status in ['active','executing', 'done']:
            if action == 'cancel':
                action_step.status = 'canceled'
                action_step.message = 'Action was canceled manually'
                action_step.done_time = timezone.now()
                action_step.save()
            elif action == 'remove':
                if (action_step.action == 6) and (ActionStaging.objects.filter(step_action=action_step).exists()):
                    dataset_staging = ActionStaging.objects.filter(step_action=action_step).last().dataset_stage
                    if dataset_staging.rse:
                        ddm = DDM()
                        ddm.delete_replication_rule(ActionStaging.objects.filter(step_action=action_step).last().dataset_stage.rse)
                    dataset_staging.status = 'done'
                    dataset_staging.update_time = timezone.now()
                    dataset_staging.save()
                    action_step.status = 'canceled'
                    action_step.message = 'Action was canceled and rule deleted manually'
                    action_step.done_time = timezone.now()
                    action_step.save()
            elif action == 'push':
                action_step.execution_time = timezone.now()
                if action_step.status == 'executing':
                    action_step.status = 'active'
                action_step.save()
            elif action == 'finish':
                action_step.status = 'done'
                action_step.message = 'Action was finish manually'
                action_step.done_time = timezone.now()
                action_step.save()
                if action_step.action == 6:
                    if ActionStaging.objects.filter(step_action=action_step).exists()\
                            and (ActionStaging.objects.filter(step_action=action_step)[0].task):
                        task = ProductionTask.objects.get(id=ActionStaging.objects.filter(step_action=action_step)[0].task)
                        if task.status not in ProductionTask.NOT_RUNNING + ['to_abort']:
                            start_stagind_task(task)

            else:
                raise Exception('action is not supported')
    except Exception as e:
            content = str(e)
            return Response(content, status=500)

    return Response({'success': True})