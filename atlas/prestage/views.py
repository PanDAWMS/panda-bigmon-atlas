import json
import random

from django.db.models import Q

from atlas.cric.client import CRICClient
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, JediTasks, HashTag
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
            _logger.error("Check staging task problem %s %s" % (str(e), str(step_action_ids)))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()
    pass


class ResourceQueue(object):
    def __init__(self, resource_name):
        self.queue = []
        self.total_queued = 0
        self.resource_name = resource_name
        self.shares_penalty = {}
        self.queued_shares = set()
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

    def print_queue(self):
        pass

    def find_submission(self):
        running_level = self.running_level()
        _logger.info("Find queued to submit {resource}: {maximum}/{minimum}/{percent} - running/queued "
                     "- {running}/{queued}".format(resource=self.resource_name,maximum=self.maximum_level,minimum=self.minimum_level,
                                                   percent=self.continious_percentage,running=running_level,queued=self.total_queued))
        shares_to_search = []
        shares_penalties_prepared = {}
        if self.shares_penalty:
            for share in self.shares_penalty:
                for priority_share in self.shares_penalty[share]:
                    shares_penalties_prepared[share+str(priority_share[0])] = priority_share[1]
            _logger.info("Shares penalties {resource}: {penalties} ".format(resource=self.resource_name,
                                                       penalties=str(shares_penalties_prepared)))
            for share in self.queued_shares:
                if ((float(running_level) + float(shares_penalties_prepared.get(share,0)))/ float(self.maximum_level)) < (self.continious_percentage / 100.0):
                    shares_to_search.append(share)
        if shares_to_search and ('any' not in shares_to_search):
            shares_to_search.append('any')
        if shares_to_search or (not self.shares_penalty and (float(running_level) / float(self.maximum_level)) < (self.continious_percentage / 100.0)):
            self.priorities_queue()
            to_submit_list = []
            to_submit_value = 0
            skipped_list = []
            skipped_value = 0
            required_submission = self.maximum_level - running_level
            if required_submission + self.minimum_level > self.total_queued:
                to_submit_list = self.queue
                to_submit_value = self.total_queued
            else:
                for element in self.queue:
                    if (to_submit_value < required_submission) and (not(shares_to_search) or (element['share'] in shares_to_search)):
                        to_submit_list.append(element)
                        to_submit_value += element['value']
                    else:
                        skipped_list.append(element)
                        skipped_value += element['value']
                if (skipped_value < self.minimum_level):
                    to_submit_list+= skipped_list
                    to_submit_value += skipped_value
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

    def __init__(self, resource_name, ddm, test=False):
        super().__init__(resource_name)
        self.ddm = ddm
        self.__get_tape_queue()
        self.get_limits()
        self.is_test = test


    def __get_tape_queue(self):
        queued_staging_request = DatasetStaging.objects.filter(source=self.resource_name,status='queued').values()
        self.total_queued = 0
        for x in queued_staging_request:
            dataset_stagings = ActionStaging.objects.filter(dataset_stage=x['id'])
            priority = 0
            dataset_shares = []
            for dataset_staging in dataset_stagings:
                if dataset_staging.step_action.status == 'active':
                    task = ProductionTask.objects.get(id=dataset_staging.task)
                    priority = task.current_priority
                    if not priority:
                        priority = task.priority
                    if task.request.phys_group == 'VALI':
                        dataset_shares.append('VALI')
                    else:
                        dataset_shares.append(JediTasks.objects.get(id=task.id).gshare)
            if len(dataset_shares) >0:
                if len(dataset_shares)>1:
                    dataset_share = 'any'
                else:
                    dataset_share = dataset_shares[0]
                if dataset_share in self.shares_penalty:
                    for share_priority in self.shares_penalty[dataset_share]:
                        if priority <= share_priority[0]:
                            dataset_share = dataset_share+str(share_priority[0])
                            break
                x['value'] = x['total_files']
                x['priority'] = priority
                x['share'] = dataset_share
                self.queued_shares.add(dataset_share)
                self.total_queued += x['value']
                self.queue.append(x)

    def running_level(self):
        staing_requests =  DatasetStaging.objects.filter(source=self.resource_name,status__in=['staging']).values()
        running_level  = 0
        for x in staing_requests:
            running_level += x['total_files'] - x['staged_files']
        return running_level


    def get_limits(self):
        limits_config = ActionDefault.objects.get(name=self.resource_name)
        self.minimum_level = limits_config.get_config('minimum_level')
        self.maximum_level = limits_config.get_config('maximum_level')
        self.continious_percentage =limits_config.get_config('continious_percentage')


    def priorities_queue(self):
       # new_queue = self.queue.sort(key=lambda x:-x['priority'])
        self.queue.sort(key=lambda x:-x['priority'])

    def __submit(self, submission_list):
        #print(submission_list)
        total_submitted = 0
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
                        if not self.is_test:
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
                            total_submitted += x['value']
                        else:
                            print(x['dataset'], rule, source_replica)
                    except Exception as e:
                        _logger.error("Problem during submission {resource}: {error}".format(resource=self.resource_name,
                                                                   error=str(e)))
                else:
                    _logger.error("Problem during submission for dataset {dataset_staging}".format(dataset_staging=x['id']))
        return total_submitted

    def to_wait(self, submission_list):
        oldest = min(submission_list,key=lambda x:x['start_time'])['start_time']
        return (timezone.now() - oldest) < timedelta(days=1)

    def do_submission(self):
        return self.__submit(self.find_submission())


    def print_queue(self):
        self.priorities_queue()
        for x in self.queue:
            print(x)

class TapeResourceProcessed(TapeResource):
    def __init__(self, resource_name, ddm, running_level_processed, test=False):
        super().__init__(resource_name, ddm, test)
        self.running_level_processed = running_level_processed

    def running_level(self):
        return self.running_level_processed


class TapeResourceProcessedWithShare(TapeResource):
    def __init__(self, resource_name, ddm, shares_penalty, test=False):
        super().__init__(resource_name, ddm, test)
        self.shares_penalty = shares_penalty

class TestTapeResourceWithShare(TapeResourceProcessedWithShare):
    def __init__(self, resource_name, ddm, shares_penalty, test, limits):
        self.limits = limits
        super().__init__(resource_name, ddm, shares_penalty,test)


    def get_limits(self):
        limits_config = ActionDefault.objects.get(name=self.resource_name)
        self.minimum_level = limits_config.get_config('minimum_level')
        self.maximum_level = limits_config.get_config('maximum_level')
        self.continious_percentage =limits_config.get_config('continious_percentage')
        if self.limits.get('minimum_level'):
            self.minimum_level = self.limits['minimum_level']
        if self.limits.get('maximum_level'):
            self.maximum_level = self.limits['maximum_level']
        if self.limits.get('continious_percentage'):
            self.continious_percentage = self.limits['continious_percentage']

class TestTapeResource(TapeResource):
    def __init__(self, resource_name, ddm, limits):
        self.limits = limits
        super().__init__(resource_name, ddm)


    def get_limits(self):
        limits_config = ActionDefault.objects.get(name=self.resource_name)
        self.minimum_level = limits_config.get_config('minimum_level')
        self.maximum_level = limits_config.get_config('maximum_level')
        self.continious_percentage =limits_config.get_config('continious_percentage')
        if self.limits.get('minimum_level'):
            self.minimum_level = self.limits['minimum_level']
        if self.limits.get('maximum_level'):
            self.maximum_level = self.limits['maximum_level']
        if self.limits.get('continious_percentage'):
            self.continious_percentage = self.limits['continious_percentage']


def start_stagind_task(task):
    #Send resume command
    if(task.status in ['staging','waiting']):
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
    step = task.step
    waiting_parameters_from_step = None
    if step.get_task_config('PDAParams'):
        try:
            waiting_parameters_from_step = _parse_action_options(step.get_task_config('PDAParams'))
            if waiting_parameters_from_step.get('level', None):
                level = int(waiting_parameters_from_step.get('level'))
                if level > 100:
                    level = 100
                elif level < -1:
                    level = 0
        except Exception as e:
            _logger.error(" %s" % str(e))
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


    else:
        dataset_staging = DatasetStaging.objects.filter(dataset=input_dataset,
                                                        status__in=DatasetStaging.ACTIVE_STATUS).last()
    if dataset_staging.status == 'queued' and waiting_parameters_from_step and waiting_parameters_from_step.get('nowait', False):
        perfom_dataset_stage(input_dataset, ddm, rule, lifetime, replicas)
        dataset_staging.status = 'staging'
        dataset_staging.start_time = timezone.now()
        dataset_staging.save()
    # Create new action
    if StepAction.objects.filter(step=task.step_id,action = 6,status__in=['active','executing']).exists():
        action_step = StepAction.objects.filter(step=task.step_id,action = 6,status__in=['active','executing']).last()
    else:
        action_step = StepAction()
        action_step.action = 6
        #todo add config
        action_step.create_time = timezone.now()
        action_step.set_config({'delay':config['delay']})
        action_step.step = step.id
        level = None
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


def submit_queued_rule(action_step_id):
    action_step = StepAction.objects.get(id=action_step_id)
    ddm = DDM()
    for action_stage in ActionStaging.objects.filter(step_action=action_step):
        dataset_stage = action_stage.dataset_stage
        if dataset_stage.status == 'queued':
            perfom_dataset_stage(dataset_stage.dataset, ddm, action_step.get_config('rule'), action_step.get_config('lifetime'), action_step.get_config('source_replica'))
            dataset_stage.status = 'staging'
            dataset_stage.start_time = timezone.now()
            dataset_stage.save()


def submit_all_tapes():
    ddm = DDM()
    for tape in ActionDefault.objects.filter(type='PHYSICAL_TAPE'):
        if tape.get_config('active'):
            if DatasetStaging.objects.filter(source=tape.name,status='queued').exists():
                resource_tape = TapeResource(tape.name,ddm)
                resource_tape.do_submission()

def submit_all_tapes_processed():
    ddm = DDM()
    active_staged = find_active_staged()
    for tape in ActionDefault.objects.filter(type='PHYSICAL_TAPE'):
        if tape.get_config('active'):
            if DatasetStaging.objects.filter(source=tape.name,status='queued').exists():
                active_for_tape = [active_staged[x] for x in active_staged.keys() if
                                   active_staged[x]['tape'] == tape.name]
                total_submitted = sum([x['total']- x['value'] for x in active_for_tape])
                resource_tape = TapeResourceProcessed(tape.name,ddm,total_submitted)
                files_submitted = resource_tape.do_submission()
                if tape.get_config('is_slow') and (files_submitted == 0):
                    resource_tape = TapeResource(tape.name,ddm)
                    resource_tape.do_submission()
                elif tape.get_config('bunch_size') is not None:
                    if files_submitted > 0:
                        bunch_size = tape.get_config('bunch_size')
                        tape.set_config({'current_bunch': bunch_size})
                        tape.save()
                    elif tape.get_config('current_bunch') > 0:
                        resource_tape = TapeResource(tape.name,ddm)
                        files_submitted = resource_tape.do_submission()
                        if files_submitted > 0:
                            current_bunch = tape.get_config('current_bunch')
                            tape.set_config({'current_bunch': current_bunch - files_submitted})
                            tape.save()

def submit_all_tapes_processed_with_shares():
    ddm = DDM()
    active_staged = find_active_staged_with_share()
    for tape in ActionDefault.objects.filter(type='PHYSICAL_TAPE'):
        if tape.get_config('active'):
            if DatasetStaging.objects.filter(source=tape.name,status='queued').exists():
                active_for_tape = [active_staged[x] for x in active_staged.keys() if
                                   active_staged[x]['tape'] == tape.name]
                active_for_tape.sort(key=lambda x: x['priority'])
                shares_penalty_by_priority = {}
                for dataset in active_for_tape:
                    if dataset['share'] not in shares_penalty_by_priority:
                        shares_penalty_by_priority[dataset['share']] = {}
                    for share in shares_penalty_by_priority[dataset['share']].keys():
                        shares_penalty_by_priority[dataset['share']][share] += dataset['value']
                    if dataset['priority'] not in shares_penalty_by_priority[dataset['share']]:
                        shares_penalty_by_priority[dataset['share']][dataset['priority']] = dataset['value']
                shares_penalty = {}
                for share_name in shares_penalty_by_priority:
                    shares_penalty[share_name] = shares_penalty_by_priority[share_name].items()
                    shares_penalty[share_name] = [(x[0],x[1]* int(tape.get_config('maximum_level')) // 100000) for x in shares_penalty[share_name]]
                    shares_penalty[share_name].sort(key=lambda x:-x[0])

                resource_tape = TapeResourceProcessedWithShare(tape.name,ddm,shares_penalty)
                files_submitted = resource_tape.do_submission()
                if tape.get_config('is_slow') and (files_submitted == 0):
                    resource_tape = TapeResource(tape.name,ddm)
                    resource_tape.do_submission()
                elif tape.get_config('bunch_size') is not None:
                    if files_submitted > 0:
                        bunch_size = tape.get_config('bunch_size')
                        tape.set_config({'current_bunch': bunch_size})
                        tape.save()
                    elif tape.get_config('current_bunch') > 0:
                        resource_tape = TapeResource(tape.name,ddm)
                        files_submitted = resource_tape.do_submission()
                        if files_submitted > 0:
                            current_bunch = tape.get_config('current_bunch')
                            tape.set_config({'current_bunch': current_bunch - files_submitted})
                            tape.save()

def test_tape_processed(tape_name, test):
    ddm = DDM()
    active_staged = find_active_staged()
    for tape in ActionDefault.objects.filter(type='PHYSICAL_TAPE'):
        if tape.name == tape_name:
            if DatasetStaging.objects.filter(source=tape.name,status='queued').exists():
                active_for_tape = [active_staged[x] for x in active_staged.keys() if
                                   active_staged[x]['tape'] == tape.name]
                total_submitted = sum([x['total']- x['value'] for x in active_for_tape])
                resource_tape = TapeResourceProcessed(tape.name,ddm,total_submitted,test)
                files_submitted = resource_tape.do_submission()
                if files_submitted > 0 :
                    bunch_size = tape.get_config('bunch_size')
                    tape.set_config({'current_bunch':bunch_size})
                    tape.save()
                elif (tape.get_config('current_bunch') or 0)>0:
                    resource_tape = TapeResource(tape.name,ddm, test)
                    files_submitted = resource_tape.do_submission()
                    if files_submitted>0:
                        current_bunch = tape.get_config('current_bunch')
                        tape.set_config({'current_bunch':current_bunch-files_submitted})
                        tape.save()

def test_tape_shares(tape_name, test):
    ddm = DDM()
    active_staged = find_active_staged_with_share()
    for tape in ActionDefault.objects.filter(type='PHYSICAL_TAPE'):
        if tape.name == tape_name:
            if DatasetStaging.objects.filter(source=tape.name,status='queued').exists():
                active_for_tape = [active_staged[x] for x in active_staged.keys() if
                                   active_staged[x]['tape'] == tape.name]
                shares_penalty = {}
                for dataset in active_for_tape:
                    shares_penalty[dataset['share']] = shares_penalty.get(dataset['share'],0) + dataset['value']
                resource_tape = TestTapeResourceWithShare(tape.name,ddm,shares_penalty,test, {'minimum_level':1})
                files_submitted = resource_tape.do_submission()
                if files_submitted > 0 :
                    bunch_size = tape.get_config('bunch_size')
                    tape.set_config({'current_bunch':bunch_size})
                    tape.save()
                elif (tape.get_config('current_bunch') or 0)>0:
                    resource_tape = TapeResource(tape.name,ddm, test)
                    files_submitted = resource_tape.do_submission()
                    if files_submitted>0:
                        current_bunch = tape.get_config('current_bunch')
                        tape.set_config({'current_bunch':current_bunch-files_submitted})
                        tape.save()


def convert_input_to_physical_tape(input):
    if ActionDefault.objects.filter(name=input[:30],type='Tape').exists():
        ad = ActionDefault.objects.get(name=input[:30],type='Tape')
    else:
        cric_client = CRICClient()
        ddm_endpoints = cric_client.get_ddmendpoint()
        physical_tape = ddm_endpoints[input]['su']
        ad,_ = ActionDefault.objects.get_or_create(name=input[:30], type='Tape')
        ad.set_config({'su':physical_tape})
        ad.save()
    return ad.get_config('su')


def create_prestage(task,ddm,rule, input_dataset,config, special=None, destination=None):
    #check that's only Tape replica
    replicas = ddm.full_replicas_per_type(input_dataset)
    if (len(replicas['data']) > 0) and not destination:
        start_stagind_task(task)
        return True
    else:
        #No data replica - create a rule
        source_replicas = None
        input = None
        if special:
            if destination:
                rule, source_replicas, input = destination, special, special
            else:
                if special in ([x['rse'] for x in replicas['tape']]+['CERN-PROD_TEST-CTA', 'CERN-PROD_RAW']):
                    rule, source_replicas, input = ddm.get_replica_pre_stage_rule_by_rse(special)
                else:
                    rule, source_replicas, input = ddm.get_replica_pre_stage_rule(input_dataset)
        else:
            if replicas['tape']:
                input = [x['rse'] for x in replicas['tape']]
                if len(input) == 1:
                    input = input[0]
                else:
                    input_without_cern = [x for x in input if 'CERN'  not in x ]
                    if input_without_cern:
                        #Create rule because it could be recovered from CERN
                        if len(input_without_cern) == 1:
                            rule, source_replicas, input = ddm.get_replica_pre_stage_rule_by_rse(random.choice(input_without_cern))
                        else:
                            input = random.choice(input_without_cern)
                    else:
                        input = random.choice(input)
        input=convert_input_to_physical_tape(input)
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

def make_rule_from_google(dataset):
    ddm= DDM()
    ddm.add_replication_rule(dataset,'type=DATADISK&GCSEnable=True',copies=1, lifetime=30*86400, weight='freespace',
                                    activity='Staging', source_replica_expression='GOOGLE_EU')


def check_tasks_for_prestage(action_step_id, ddm, rule, delay, max_waite_time, check_archive=False):
    action_step = StepAction.objects.get(id=action_step_id)
    action_step.attempt += 1
    step = StepExecution.objects.get(id=action_step.step)
    special = False
    destination = None
    noidds = False
    level = None
    if step.get_task_config('PDAParams'):
        try:
            waiting_parameters_from_step = _parse_action_options(step.get_task_config('PDAParams'))
            if waiting_parameters_from_step.get('special'):
                special = waiting_parameters_from_step.get('special')
            if waiting_parameters_from_step.get('destination'):
                destination = waiting_parameters_from_step.get('destination')
            if waiting_parameters_from_step.get('noidds'):
                noidds = True
            if waiting_parameters_from_step.get('level'):
                level = waiting_parameters_from_step.get('level')
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
        step.remove_project_mode('inputPreStaging')
        step.save()
        return
    finish_action = True
    fail_action = False
    for task in tasks:
        if (task.status in ['staging','waiting']) and (not ActionStaging.objects.filter(task=task.id).exists()):
            try:
                if check_archive:
                    config = ActionDefault.objects.get(name='active_archive_staging').get_config()
                else:
                    config = ActionDefault.objects.get(name='active_staging').get_config()
                if not noidds and not check_archive:
                    config['level'] = 1
                if check_archive:
                    input_dataset = find_archive_dataset(task.input_dataset,ddm)
                    if input_dataset:
                        send_use_archive_task(task)
                        create_prestage(task, ddm, rule, input_dataset, config, special)
                        create_follow_prestage_action(task)
                    else:
                        fail_action = True
                else:
                    if level:
                        config['level'] = level
                    create_prestage(task,ddm,rule,task.input_dataset,config,special,destination)
            except Exception as e:
                _logger.error("Check task for prestage problem %s %s" % (str(e),str(action_step_id)))
                finish_action = False
    if finish_action and (production_request.cstatus != 'approved'):
        action_step.status = 'done'
        action_step.message = 'All task checked'
        action_step.done_time = current_time
        step.remove_project_mode('toStaging')
        step.remove_project_mode('inputPreStaging')
        step.save()
    else:
        action_step.execution_time = current_time + timedelta(hours=delay)
        action_step.status = 'active'
    if fail_action:
        action_step.status = 'failed'
        action_step.message = 'No archive dataset is found'
        action_step.done_time = current_time
        step.remove_project_mode('toStaging')
        step.remove_project_mode('inputPreStaging')
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
    delay = int(action_step.get_config('delay'))
    current_time = timezone.now()
    create_follow_action = False
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
            if True :
                existed_rule = ddm.dataset_active_rule_by_rse(dataset_stage.dataset, action_step.get_config('rule'))
                if existed_rule:
                        if dataset_stage.rse and dataset_stage.rse != existed_rule['id']:
                            _logger.error("do staging change rule from %s to %s for %s" % (str(dataset_stage.rse), str(existed_rule['id']),dataset_stage.dataset))
                        dataset_stage.rse = existed_rule['id']
                        if dataset_stage.staged_files != int(existed_rule['locks_ok_cnt']):
                            dataset_stage.staged_files = int(existed_rule['locks_ok_cnt'])
                            dataset_stage.update_time = current_time
                        else:
                            delay = 2*int(action_step.get_config('delay'))
                        if ((existed_rule['expires_at']-timezone.now().replace(tzinfo=None))<timedelta(days=5)) and \
                                (task.status not in ['done','finished','broken','aborted']):
                            try:
                                ddm.change_rule_lifetime(existed_rule['id'],15*86400)
                            except Exception as e:
                                _logger.error("Check do staging problem %s %s" % (str(e), str(action_step_id)))
                else:
                    action_finished = False
                    if perfom_dataset_stage(dataset_stage.dataset, ddm, action_step.get_config('rule'),
                                            action_step.get_config('lifetime'),
                                            action_step.get_config('source_replica')):
                        dataset_stage.start_time = current_time

            if ((level == 100) and (dataset_stage.staged_files == dataset_stage.total_files)) or \
                    (((dataset_stage.total_files-dataset_stage.staged_files)<= ActionDefault.FILES_TO_RELEASE) and
                     ((float(dataset_stage.staged_files) / float(dataset_stage.total_files)) >= (float(level) / 100.0))) or ((level == 1) and (dataset_stage.staged_files >0) ):
                start_stagind_task(task)
            if dataset_stage.staged_files != dataset_stage.total_files:
                action_finished = False
            else:
                dataset_stage.status = 'done'
                dataset_stage.update_time = current_time
                dataset_stage.end_time = current_time
                if task.status not in ProductionTask.NOT_RUNNING:
                    create_follow_action = True
            dataset_stage.save()

        elif dataset_stage.status == 'queued':
            action_finished = False

    if action_finished :
        action_step.status = 'done'
        action_step.message = 'All task started'
        action_step.done_time = current_time

    else:
        action_step.execution_time = current_time + timedelta(hours=delay)
        action_step.status = 'active'
    action_step.save()
    if create_follow_action:
        if not StepAction.objects.filter(action=7,step=action_step.step).exists():
            current_id = action_step.id
            new_follow_step = action_step
            new_follow_step.id = None
            new_follow_step.action = 7
            new_follow_step.status = 'active'
            new_follow_step.set_config({'stage_action': current_id})
            new_follow_step.save()
        else:
            new_follow_step = StepAction.objects.filter(action=7,step=action_step.step).last()
            if new_follow_step.status == 'done':
                new_follow_step.status = 'active'
                new_follow_step.save()




def delete_done_staging_rules(reqids):
    _logger.info("Find replica to delete for %s" % str(reqids))
    for reqid in reqids:
        to_delete_per_tape, res2 = replica_to_delete_dest(reqid)
        ddm = DDM()
        rses = []
        for x in to_delete_per_tape:
            rses += to_delete_per_tape[x]['rses']
        for rse in rses:
            _logger.info("Rule %s will be deleted" % str(rse))
            ddm.delete_replication_rule(rse)

def find_active_staged():
    active_stage_actions = list(StepAction.objects.filter(action=6,status='active'))
    followed_actions = StepAction.objects.filter(action=7,status='active')
    not_done_action_ids = [x.get_config('stage_action') for x in followed_actions]
    not_done_action = list(StepAction.objects.filter(id__in=not_done_action_ids))
    all_action =  active_stage_actions + not_done_action
    result = {}
    for action in all_action:
        for action_stage in ActionStaging.objects.filter(step_action=action):
            dataset_stage = action_stage.dataset_stage
            task = ProductionTask.objects.get(id=action_stage.task)
            if (dataset_stage.status not in ['queued']) and dataset_stage.source:
                if dataset_stage.dataset not in result:
                    result[dataset_stage.dataset] = {'value':dataset_stage.staged_files,'tape':dataset_stage.source, 'total':dataset_stage.total_files}
                if task.total_files_finished is None:
                    files_finished = 0
                else:
                    files_finished = task.total_files_used
                result[dataset_stage.dataset]['value'] = min([result[dataset_stage.dataset]['value'],dataset_stage.staged_files,files_finished])
    return result

def find_active_staged_with_share():
    active_stage_actions = list(StepAction.objects.filter(action=6,status='active'))
    followed_actions = StepAction.objects.filter(action=7,status='active')
    not_done_action_ids = [x.get_config('stage_action') for x in followed_actions]
    not_done_action = list(StepAction.objects.filter(id__in=not_done_action_ids))
    all_action =  active_stage_actions + not_done_action
    result = {}
    for action in all_action:
        for action_stage in ActionStaging.objects.filter(step_action=action):
            dataset_stage = action_stage.dataset_stage
            task = ProductionTask.objects.get(id=action_stage.task)
            if task.status in ProductionTask.NOT_RUNNING+['exhausted']:
                continue
            #share = task.request.request_type
            share = JediTasks.objects.get(id=task.id).gshare
            if task.request.phys_group == 'VALI':
                share = 'VALI'
            if (dataset_stage.status not in ['queued']) and dataset_stage.source:
                priority = task.priority
                if task.current_priority:
                    priority = task.current_priority
                if dataset_stage.dataset not in result:
                    result[dataset_stage.dataset] = {'value':dataset_stage.staged_files,'tape':dataset_stage.source,
                                                     'total':dataset_stage.total_files,'share': share, 'priority':priority}
                if share == 'VALI':
                    files_finished = dataset_stage.staged_files
                else:
                    if task.total_files_finished is None:
                        files_finished = 0
                    else:
                        files_finished = task.total_files_used
                result[dataset_stage.dataset]['value'] = min([result[dataset_stage.dataset]['value'],max([0,dataset_stage.staged_files-files_finished])])
                if(result[dataset_stage.dataset]['share']!=share):
                    result[dataset_stage.dataset]['share'] = 'any'
                if(result[dataset_stage.dataset]['priority']<priority):
                    result[dataset_stage.dataset]['priority'] = priority
    return result

def find_repeated_tasks_to_follow():
    to_repeat = []
    try:
        staged_tasks = []
        actions = StepAction.objects.filter(action=6, status='done', create_time__gte=timezone.now()-timedelta(days=30))
        for action in actions:
            if ActionStaging.objects.filter(step_action=action).exists():
                for action_stage in ActionStaging.objects.filter(step_action=action):
                        staged_tasks.append(action_stage.task)
        actions = StepAction.objects.filter(action=10, create_time__gte=timezone.now()-timedelta(days=30))
        for action in actions:
            if ActionStaging.objects.filter(step_action=action).exists():
                for action_stage in ActionStaging.objects.filter(step_action=action):
                        staged_tasks.append(action_stage.task)
        used_input = []
        for task_id in staged_tasks:
            task = ProductionTask.objects.get(id=task_id)
            dataset = task.primary_input
            if dataset not in used_input:
                used_input.append(dataset)
                repeated_tasks = ProductionTask.objects.filter(id__gt=task_id, submit_time__gte=timezone.now()-timedelta(days=7), primary_input=dataset)
                for rep_task in repeated_tasks:
                    if rep_task.status not in ProductionTask.NOT_RUNNING:
                        ttask = TTask.objects.get(id=rep_task.id)
                        if 'inputPreStaging' not  in ttask._jedi_task_parameters and not ActionStaging.objects.filter(task=rep_task.id).exists():
                            to_repeat.append(rep_task.id)
        ddm = DDM()
        for task in to_repeat:
            if create_replica_extension(task, ddm):
                _logger.info("Task {task} is now following".format(task=task))
    except Exception as e:
        _logger.error("Create follow repeated input tasks problem: %s" % str(e))
    return to_repeat

def activate_staging(step_action_ids):
    ddm = DDM()
    #todo name and config
    for waiting_step in step_action_ids:
        try:
            do_staging(waiting_step, ddm)
        except Exception as e:
            _logger.error("Check activate staging problem %s %s" % (str(e),waiting_step))
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


def follow_staged(waiting_step, ddm):
    current_time = timezone.now()
    action_step = StepAction.objects.get(id=waiting_step)
    done_staging_action = StepAction.objects.get(id=int(action_step.get_config('stage_action')))
    if not ActionStaging.objects.filter(step_action=done_staging_action).exists():
        action_step.status = 'failed'
        action_step.message = 'Something wrong'
        action_step.done_time = current_time
        action_step.save()
        return
    action_finished = True
    for action_stage in ActionStaging.objects.filter(step_action=done_staging_action):
        dataset_stage = action_stage.dataset_stage
        task = ProductionTask.objects.get(id=action_stage.task)
        if dataset_stage.status != 'done':
            continue
        if task.status not in ProductionTask.NOT_RUNNING:
            action_finished = False
            existed_rule = ddm.dataset_active_rule_by_rse(dataset_stage.dataset, action_step.get_config('rule'))
            if existed_rule:
                if (existed_rule['expires_at'] - timezone.now().replace(tzinfo=None)) < timedelta(days=5):
                    try:
                        ddm.change_rule_lifetime(existed_rule['id'], 15 * 86400)
                    except Exception as e:
                        _logger.error("Check follow staged problem %s %s" % (str(e), str(waiting_step)))

    if action_finished :
        action_step.status = 'done'
        action_step.message = 'All task started'
        action_step.done_time = current_time
    else:
        action_step.execution_time = current_time + timedelta(hours=action_step.get_config('delay')*4)
        action_step.status = 'active'
    action_step.save()


def create_replica_extension(task_id, ddm):
    task = ProductionTask.objects.get(id=task_id)
    if task.status not in ProductionTask.NOT_RUNNING:
        input_dataset = task.input_dataset
        replicas = ddm.full_replicas_per_type(input_dataset)
        if (len(replicas['data']) == 1) and (DatasetStaging.objects.filter(dataset=input_dataset).exists()):
            dataset_stage = DatasetStaging.objects.get(dataset=input_dataset)
            if ddm.dataset_active_rule_by_rule_id(dataset_stage.dataset, dataset_stage.rse):
                if StepAction.objects.filter(step=task.step.id,action=10).exists():
                    for step_action in StepAction.objects.filter(step=task.step.id,action=10):
                        for action_staging in ActionStaging.objects.filter(step_action=step_action):
                            if action_staging.task == task_id:
                                return False

                step_action = StepAction()
                step_action.step = task.step.id
                step_action.action = 10
                config = ActionDefault.objects.get(name='active_archive_staging').get_config()
                step_action.status = 'active'
                step_action.create_time = timezone.now()
                step_action.set_config({'delay': config['delay']})
                step_action.set_config({'task': task.id})
                step_action.set_config({'rule': dataset_stage.rse})
                step_action.execution_time = timezone.now() + timedelta(hours=config['delay'])
                step_action.attempt = 0
                step_action.request = task.request
                step_action.save()
                action_staging = ActionStaging()
                action_staging.step_action = step_action
                action_staging.dataset_stage = dataset_stage
                action_staging.task = task_id
                action_staging.save()
                follow_repeated_staged(step_action.id, ddm)
                return True
        return False



def follow_repeated_staged(waiting_step, ddm):
    current_time = timezone.now()
    action_step = StepAction.objects.get(id=waiting_step)
    if not ActionStaging.objects.filter(step_action=action_step).exists():
        action_step.status = 'failed'
        action_step.message = 'Something wrong'
        action_step.done_time = current_time
        action_step.save()
        return
    action_finished = True
    for action_stage in ActionStaging.objects.filter(step_action=action_step):
        dataset_stage = action_stage.dataset_stage
        task = ProductionTask.objects.get(id=action_stage.task)
        if dataset_stage.status != 'done':
            _logger.error("Follow staging action problem %s" % str(action_step.id))
            action_step.status = 'failed'
            action_step.message = 'Something wrong'
            action_step.done_time = current_time
            action_step.save()
        if task.status not in ProductionTask.NOT_RUNNING:
            action_finished = False
            existed_rule = ddm.dataset_active_rule_by_rule_id(dataset_stage.dataset, dataset_stage.rse)
            if existed_rule:
                if (existed_rule['expires_at'] - timezone.now().replace(tzinfo=None)) < timedelta(days=5):
                    try:
                        ddm.change_rule_lifetime(existed_rule['id'], 15 * 86400)
                    except Exception as e:
                        _logger.error("Check replicas extension problem %s %s" % (str(e), str(waiting_step)))
            else:
                action_finished = True

    if action_finished :
        action_step.status = 'done'
        action_step.message = 'All task started'
        action_step.done_time = current_time
    else:
        action_step.execution_time = current_time + timedelta(hours=action_step.get_config('delay')*4)
        action_step.status = 'active'
    action_step.save()

def follow_staging(step_action_ids):
    ddm = DDM()
    for waiting_step in step_action_ids:
        try:
            follow_staged(waiting_step, ddm)
        except Exception as e:
            _logger.error("Check replicas follow problem %s %s" % (str(e), str(waiting_step)))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()


def follow_repeated_staging(step_action_ids):
    ddm = DDM()
    for waiting_step in step_action_ids:
        try:
            follow_repeated_staged(waiting_step, ddm)
        except Exception as e:
            _logger.error("Check replicas follow repeated problem %s actionid %s" % (str(e), str(waiting_step)))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()

def process_all_action_by_type(action):
    action_step_todo = StepAction.objects.filter(status='active',action=action)
    process_actions(action_step_todo)


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
        elif action == 10:
            follow_repeated_staging(executing_actions[action])

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

        total = {'files_queued':0,'files_staged':0, 'files_staging':0, 'files_processed':0,'files_total_submitted':0}
        result = []
        tapes = list(ActionDefault.objects.filter(type__in=['Tape','PHYSICAL_TAPE']).order_by('id'))
        active_staged = find_active_staged()
        for tape in tapes:
            files_queued = 0
            files_staged = 0
            files_staging = 0
            active_for_tape = [active_staged[x] for x in active_staged.keys() if active_staged[x]['tape']==tape.name]
            processed = sum([x['value'] for x in active_for_tape])
            total_submitted = sum([x['total'] for x in active_for_tape])
            datasets = DatasetStaging.objects.filter(source=tape.name,status__in=['staging', 'queued'])
            for dataset in datasets:
                if dataset.status == 'queued':
                    files_queued += dataset.total_files
                else:
                    files_staged += dataset.staged_files
                    files_staging += dataset.total_files - dataset.staged_files
            result.append({'name':tape.name,'minimum_level':tape.get_config('minimum_level'),
                           'maximum_level':tape.get_config('maximum_level'),
                           'current_bunch':tape.get_config('current_bunch'),
                           'continious_percentage':tape.get_config('continious_percentage'),
                           'files_queued':files_queued,'files_staged':files_staged,'files_staging':files_staging,
                           'active':tape.get_config('active'),
                           'files_processed':processed,'files_total_submitted':total_submitted})
            total['files_queued'] += files_queued
            total['files_staged'] += files_staged
            total['files_staging'] += files_staging
            total['files_processed'] += processed
            total['files_total_submitted'] += total_submitted
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
    slices = list(InputRequestList.objects.filter(request=reqid).order_by('slice'))
    slices_done = {}
    for slice in slices:
        steps = list(StepExecution.objects.filter(request=reqid,slice=slice).values_list('id',flat=True))
        if ProductionTask.objects.filter(request=reqid,step__id__in=steps,status='done').count() == ProductionTask.objects.filter(request=reqid,step__id__in=steps).count():
            slices_done[slice.dataset]=True
        else:
            if slice.dataset in slices_done:
                slices_done.pop(slice.dataset)
    print(slices_done)
    rse_to_delete = []
    for action_step in action_steps:
        for action_stage in ActionStaging.objects.filter(step_action=action_step):
            dataset_stage = action_stage.dataset_stage
            task = ProductionTask.objects.get(id=action_stage.task)
            if dataset_stage.status == 'done' and task.status == 'done':
                try:
                    ddm.get_rule(dataset_stage.rse)
                    if task.step.slice.dataset in slices_done:
                        rse_to_delete.append(dataset_stage.rse)

                except:
                    pass
    return rse_to_delete


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
        tasks_messages = []
        task = None
        if action_step.action == 6:
            if ActionStaging.objects.filter(step_action=action_step).exists():
                for staging in ActionStaging.objects.filter(step_action=action_step):
                    dataset = staging.dataset_stage.dataset
                    total_files = staging.dataset_stage.total_files
                    staged_files = staging.dataset_stage.staged_files
                    rse = staging.dataset_stage.rse
                    task = staging.task
                    if ':' not in dataset:
                        dataset = '{0}:{1}'.format(dataset.split('.')[0],dataset)
                    link = '<a href="https://rucio-ui.cern.ch/did?name={name}">{name}</a>'.format(name=str(dataset))
                    rule_link = '<a href="https://rucio-ui.cern.ch/rule?rule_id={rule_id}">{rule_rse}</a>'.format(
                        rule_id=rse, rule_rse=action_step.get_config('rule'))

                    if action_step.get_config('tape'):
                        tape_replica = str(action_step.get_config('tape'))
                    else:
                        tape_replica = 'tape'
                    if staging.dataset_stage.source:
                        tape_replica = staging.dataset_stage.source
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
                    tasks_messages.append((task,message))

    except:
        return HttpResponseRedirect('/')

    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'action with ID = %s' % wstep_id,
        'waiting_step': action_step,
        'tasks_messages': tasks_messages,
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
            elif action == 'bypass':
                submit_queued_rule(action_step.id)
                action_step.execution_time = timezone.now()
                if action_step.status == 'executing':
                    action_step.status = 'active'
                action_step.save()

            else:
                raise Exception('action is not supported')
    except Exception as e:
            content = str(e)
            return Response(content, status=500)

    return Response({'success': True})


def sync_cric_deft():
    try:
        cric_client = CRICClient()
        storage_units = cric_client.get_storageunit()
        for su in storage_units.values():
            if su['type'] == 'TAPE' and su['stagingprofiles'] and su['stagingprofiles']['default']:
                ad,changed = ActionDefault.objects.get_or_create(name=su['name'],type='PHYSICAL_TAPE')
                if ad.get_config('minimum_level') != su['stagingprofiles']['default']['min_bulksize']:
                    ad.set_config({'minimum_level':su['stagingprofiles']['default']['min_bulksize']})
                    changed = True
                    _logger.info("Changed tape profile {resource}: minimum_level to {value}".format(resource=su['name'],
                                                                                            value=su['stagingprofiles']['default']['min_bulksize']))
                if ad.get_config('maximum_level') != (su['stagingprofiles']['default']['max_bulksize']or 100000):
                    ad.set_config({'maximum_level': su['stagingprofiles']['default']['max_bulksize'] or 100000})
                    _logger.info("Changed tape profile {resource}: maximum_level to {value}".format(resource=su['name'],
                                                                                            value=su['stagingprofiles']['default']['max_bulksize']))
                    changed = True
                if not ad.get_config('continious_percentage'):
                    ad.set_config({'continious_percentage': 50})
                    _logger.info("Changed tape profile {resource}: continious_percentage to {value}".format(resource=su['name'],
                                                                                            value=ad.get_config('continious_percentage')))
                    changed = True
                if changed:
                    ad.save()
    except Exception as e:
        _logger.error("Problem during cric syncing: %s" % str(e))


def recover_stale(task_id, replica=None):
    if ActionStaging.objects.filter(task=task_id).exists():
        action_stage = ActionStaging.objects.filter(task=task_id).last()
        dataset_stage = action_stage.dataset_stage
        if dataset_stage.status != 'staging':
            return False
        ddm = DDM()
        data_replica = ddm.biggest_datadisk(dataset_stage.dataset)
        replicas = ddm.full_replicas_per_type(dataset_stage.dataset)
        if replica is not None and replica not in [x['rse'] for x in replicas['tape']]:
            return False

        else:
            for new_replica in replicas['tape']:
                new_replica_storage = convert_input_to_physical_tape(new_replica['rse'])
                if (new_replica_storage != dataset_stage.source) and (new_replica['rse'] != dataset_stage.source ):
                    replica = new_replica['rse']
                    break
                if replica is None:
                    return  False
        _logger.info("Create recovery replica for dataset {dataset}: from {source} to  {destinattion}".format(dataset=dataset_stage.dataset,
                                                                                                source=replica,destinattion=data_replica['rse']))
        ddm.add_replication_rule(dataset_stage.dataset, data_replica['rse'],
                                 activity='Staging', source_replica_expression=replica)
        return (data_replica['rse'], replica)
    else:
        return False



def replica_to_be_submitted():
    action_steps = StepAction.objects.filter(action=5,status='verify')
    ddm = DDM()
    big_steps = {}
    used_steps = set()
    for action_step in action_steps:
        if (action_step.step not in used_steps) and (not action_step.get_config('checked')):
            tasks = ProductionTask.objects.filter(step=action_step.step)
            tapes_number = 0
            tapes_stat = []
            for task in tasks:
                replicas = ddm.full_replicas_per_type(task.primary_input)
                if len(replicas['data']) == 0:
                    input_without_cern = [x for x in replicas['tape'] if 'CERN'  not in x ]
                    if input_without_cern:
                        tape = random.choice(input_without_cern)
                    else:
                        tape = random.choice(replicas['tape'])
                    tapes_number = len(replicas['tape'])
                    tapes_stat.append({'number': tapes_number, 'task_status':task.status,
                                                               'tape': convert_input_to_physical_tape(
                                                                   tape['rse']),
                                                               'files': tape['length'],'bytes':tape['bytes']})
            action_step.set_config({'tape_replicas': tapes_stat})
            action_step.set_config({'checked':True})
            print(action_step.id, tapes_number, len(tapes_stat))
            if len(tapes_stat)<14:
                action_step.save()
                used_steps.add(action_step.step)
            else:
                action_step.set_config({'special': True})
                if action_step.step not in big_steps:
                    big_steps[action_step.step] = 0
                offset = big_steps[action_step.step]
                action_step.set_config({'tape_replicas': []})
                if offset < len(tapes_stat):
                    action_step.set_config({'tape_replicas': tapes_stat[offset:offset+10]})
                    big_steps[action_step.step] = offset+10
                else:
                    action_step.set_config({'repeated': True})
                    used_steps.add(action_step.step)
                print(action_step.id, tapes_number, len(tapes_stat),offset)
                action_step.save()
        else:
            action_step.set_config({'checked': True})
            action_step.set_config({'repeated': True})
            action_step.save()

def remove_repeated_steps():
    action_steps = StepAction.objects.filter(action=5,status='verify')
    used_steps = set()
    for action_step in action_steps:
        if (action_step.get_config('checked')) and (not action_step.get_config('special'))and (not action_step.get_config('repeated')):
            if action_step not in used_steps:
                used_steps.add(action_step.step)
            else:
                action_step.set_config({'repeated': True})
                action_step.save()

def get_stats_replica_to_submitted():
    action_steps = StepAction.objects.filter(action=5,status='verify')
    replicas = {}
    max_days = 0
    current_time = timezone.now()
    for action_step in action_steps:
        if (action_step.get_config('checked')) and (not action_step.get_config('repeated')):
            tape_replica = action_step.get_config('tape_replicas')
            for replica in tape_replica:
                if replica['tape'] not in replicas:
                    replicas[replica['tape']] = {'files':0,'by_day':{},'tasks_by_day':{},'tasks':0,'bytes':0,'tasks_done':0,'task_aborted':0}
                day = (timezone.now() - action_step.create_time).days
                if day > max_days:
                    max_days = day
                if day not in replicas[replica['tape']]['by_day']:
                    replicas[replica['tape']]['by_day'][day] = 0
                    replicas[replica['tape']]['tasks_by_day'][day] = 0
                replicas[replica['tape']]['by_day'][day] += replica['files']
                if replica['task_status'] not in ProductionTask.NOT_RUNNING:
                    replicas[replica['tape']]['tasks_by_day'][day] += 1
                replicas[replica['tape']]['files'] += replica['files']
                replicas[replica['tape']]['bytes'] += replica['bytes']
                replicas[replica['tape']]['tasks'] += 1
                if replica['task_status'] in ['done','finished']:
                    replicas[replica['tape']]['tasks_done'] += 1
                if replica['task_status'] in ProductionTask.RED_STATUS:
                    replicas[replica['tape']]['task_aborted'] += 1
    for x in replicas:
        stat_by_day = []
        stat_tasks_not_finished = []
        for day in range(max_days+1):
            if day in replicas[x]['by_day']:
                stat_by_day.append(replicas[x]['by_day'][day])
                stat_tasks_not_finished.append(replicas[x]['tasks_by_day'][day])
            else:
                stat_by_day.append(0)
                stat_tasks_not_finished.append(0)
        replicas[x]['stat_by_day'] = stat_by_day
        replicas[x]['stat_tasks_not_finished'] = stat_tasks_not_finished
    result = [{'tape':x,'files_to_stage':replicas[x]['files'],'size':replicas[x]['bytes'],'tasks':replicas[x]['tasks'],
               'done':replicas[x]['tasks_done'],'aborted':replicas[x]['task_aborted'],
              'stat_by_day':replicas[x]['stat_by_day'],'tasks_by_day':replicas[x]['stat_tasks_not_finished']} for x in replicas.keys()]
    return result


@api_view(['GET'])
def derivation_requests(request):
    try:
        result = get_stats_replica_to_submitted()
    except Exception as e:
        return Response({'error': str(e)}, status=400)
    return Response(result)

def change_replica_by_task(task_id, replica=None):
    if ActionStaging.objects.filter(task=task_id).exists():
        action_stage = ActionStaging.objects.filter(task=task_id).last()
        action_step = action_stage.step_action
        dataset_stage = action_stage.dataset_stage
        if dataset_stage.status != 'queued':
            return False
        ddm = DDM()
        replicas = ddm.full_replicas_per_type(dataset_stage.dataset)
        if replica is not None and replica not in [x['rse'] for x in replicas['tape']]:
            return False

        else:
            for new_replica in replicas['tape']:
              if new_replica['rse'] != dataset_stage.source:
                replica = new_replica['rse']
                break
            if replica is None:
                return  False
        rule, source_replicas, source = ddm.get_replica_pre_stage_rule_by_rse(replica)
        print(rule, source_replicas, source)
        action_step.set_config({'rule': rule})
        action_step.set_config({'tape': source})
        action_step.set_config({'source_replica': source_replicas})
        action_step.save()
        dataset_stage.source = source
        dataset_stage.save()
    else:
        return False


def check_replica_can_be_deleted(task_id, ddm):
    if ActionStaging.objects.filter(task=task_id).exists():
        task = ProductionTask.objects.get(id=task_id)
        if task.status not in ProductionTask.RED_STATUS + ['done','obsolete']:
            return False
        action_stage = ActionStaging.objects.filter(task=task_id).last()
        dataset_stage = action_stage.dataset_stage
        do_deletion = True
        for other_actions in ActionStaging.objects.filter(dataset_stage=dataset_stage):
            if ProductionTask.objects.get(id=other_actions.task).status not in ProductionTask.RED_STATUS + ['done','obsolete']:
                do_deletion = False
                break
        if do_deletion:
            _logger.info("Rule %s will be deleted" % str(dataset_stage.rse))
            ddm.delete_replication_rule(dataset_stage.rse)
        return True
    else:
        return True


def find_stage_task_replica_to_delete():
    HASHTAG_STAGE_CAROUSEL = 'stageCarousel'
    hashtag = HashTag.objects.get(hashtag=HASHTAG_STAGE_CAROUSEL)
    ddm = DDM()
    for task_id in hashtag.tasks_ids:
        try:
            if check_replica_can_be_deleted(task_id,ddm):
                task = ProductionTask.objects.get(id=task_id)
                task.remove_hashtag(HASHTAG_STAGE_CAROUSEL)
        except Exception as e:
            _logger.error("Staging replica deletion problem %s %s" % (str(e), str(task)))


