import json
import random
import time
from copy import deepcopy

from dataclasses import dataclass, field
from typing import List
from django.db.models import Q
from rest_framework import status
from rest_framework.authentication import SessionAuthentication, BasicAuthentication, TokenAuthentication
from rest_framework.permissions import IsAuthenticated

from atlas.cric.client import CRICClient
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, JediTasks, HashTag, \
    JediDatasets, JediDatasetContents, SystemParametersHandler, TRequest, TemplateVariable, days_ago
from datetime import timedelta

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import  StepExecution, InputRequestList, ProductionTask, ProductionDataset

from decimal import Decimal
from datetime import datetime
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
import logging
# import os
from django.utils import timezone

from django.contrib.auth.decorators import login_required

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render

from atlas.prodtask.task_actions import _do_deft_action
from atlas.settings import OIDC_LOGIN_URL
from atlas.task_action.task_management import TaskActionExecutor
from elasticsearch7_dsl import Search, connections, A
from elasticsearch7 import Elasticsearch
from atlas.settings.local import MONIT_ES

_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')
HASHTAG_STAGE_CAROUSEL = 'stageCarousel'

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
                if (((float(running_level) / float(self.maximum_level)) < (self.continious_percentage / 100.0)) and
                        (((float(shares_penalties_prepared.get(share,0)))/ float(self.maximum_level)) < (self.continious_percentage / 100.0))):
                    shares_to_search.append(share)
        if shares_to_search and ('any' not in shares_to_search):
            shares_to_search.append('any')
        if (shares_to_search or
                (not self.shares_penalty and (float(running_level) / float(self.maximum_level)) < (self.continious_percentage / 100.0))):
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


def weight_from_rule(rule: str) -> str|None:
    if 'freespace' in rule:
        return None
    else:
        return 'freespace'


class TapeResource(ResourceQueue):

    def __init__(self, resource_name, ddm, shares_penalty=None,  test=False):
        super().__init__(resource_name)
        self.ddm = ddm
        self.shares_penalty = shares_penalty
        self.__get_tape_queue()
        self.get_limits()
        self.priorities_queue()
        self.is_test = test


    def __get_tape_queue(self):
        queued_staging_request = DatasetStaging.objects.filter(source=self.resource_name,status='queued').values()
        self.total_queued = 0
        for x in queued_staging_request:
            dataset_stagings = ActionStaging.objects.filter(dataset_stage=x['id'])
            priority = 0
            min_task = None
            dataset_shares_set = set()
            for dataset_staging in dataset_stagings:
                if dataset_staging.step_action.status == 'active':
                    task = ProductionTask.objects.get(id=dataset_staging.task)
                    current_priority = task.current_priority
                    if not current_priority:
                        current_priority = task.priority
                    if current_priority > priority:
                        priority = current_priority
                    if not min_task or  task.id < min_task:
                        min_task = task.id
                    if task.request.phys_group == 'VALI':
                        if task.request.campaign == 'test':
                            dataset_shares_set.add('test')
                        else:
                            dataset_shares_set.add('VALI')
                    else:
                        if task.request.request_type in ['REPROCESSING']:
                            dataset_shares_set.add("{0}{1}".format(JediTasks.objects.get(id=task.id).gshare,task.ami_tag))
                        else:
                            dataset_shares_set.add(JediTasks.objects.get(id=task.id).gshare)
            dataset_shares = list(dataset_shares_set)
            if len(dataset_shares) >0:
                if len(dataset_shares)>1:
                    dataset_share = 'any'
                else:
                    dataset_share = dataset_shares[0]
                x['share'] = dataset_share
                if self.shares_penalty and (dataset_share in self.shares_penalty):
                    highest_priority = ''
                    for share_priority in self.shares_penalty[dataset_share]:
                        if priority >= share_priority[0]:
                            if priority > share_priority[0]:
                                x['share'] = dataset_share+ highest_priority
                            else:
                                x['share'] = dataset_share+str(share_priority[0])
                            break
                        highest_priority = str(share_priority[0])
                x['value'] = x['total_files']
                x['priority'] = priority
                x['order_by'] = [dataset_share, min_task]

                self.queued_shares.add(x['share'])
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
        new_queue = deepcopy(self.queue)
        new_queue.sort(key=lambda x:x['order_by'][1])
        share_order = ['any']
        test_tasks = False
        for x in new_queue:
            if x['order_by'][0] == 'test':
                test_tasks = True
            elif x['order_by'][0] not in share_order:
                share_order.append(x['order_by'][0])
        if test_tasks:
            share_order.append('test')
        new_queue.sort(key=lambda x:-x['priority'])
        self.queue = []
        for share in share_order:
            for x in new_queue:
                if x['order_by'][0] == share:
                    self.queue.append(x)

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
                weight = None
                for dataset_staging in dataset_stagings:
                    if dataset_staging.step_action.status == 'active':
                        rule = dataset_staging.step_action.get_config('rule')
                        rule = prepare_rule(rule, self.resource_name)
                        weight = weight_from_rule(rule)
                        lifetime = dataset_staging.step_action.get_config('lifetime')
                        source_replica =  dataset_staging.step_action.get_config('source_replica')
                if rule and lifetime:

                    #print(x['dataset'], rule)
                    try:
                        existed_rule = self.ddm.active_staging_rule(x['dataset'])
                        if not self.is_test:
                            if not existed_rule:
                                if not source_replica:
                                    _logger.info("Submit rule for {resource}: {dataset} {rule}".format(resource=self.resource_name,
                                                                                                       dataset=x['dataset'],
                                                                                                       rule=rule))
                                    _jsonLogger.info("Submit new rule for {resource}".format(resource=self.resource_name),extra={'dataset':x['dataset'],'resource':self.resource_name,
                                                                                                                             'rule':rule, 'files':x['value']})
                                    self.ddm.add_replication_rule(x['dataset'], rule, copies=1, lifetime=lifetime*86400, weight=weight,
                                                            activity='Staging', notify='P')
                                else:
                                    source_replica = fill_source_replica_template(self.ddm, x['dataset'], source_replica, self.resource_name)
                                    _logger.info("Submit rule for {resource}: {dataset} {rule} {source}".format(resource=self.resource_name,
                                                                                                       dataset=x['dataset'],
                                                                                                       rule=rule, source=source_replica))
                                    _jsonLogger.info("Submit new rule for {resource}".format(resource=self.resource_name),extra={'dataset':x['dataset'],'resource':self.resource_name,
                                                                                                                                 'rule':rule, 'files':x['value']})
                                    self.ddm.add_replication_rule(x['dataset'], rule, copies=1, lifetime=lifetime*86400, weight=weight,
                                                            activity='Staging', notify='P', source_replica_expression=source_replica)
                            staging =  DatasetStaging.objects.get(id=x['id'])
                            staging.status = 'staging'
                            staging.start_time = timezone.now()
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
        self.running_level_processed = running_level_processed
        super().__init__(resource_name, ddm, test)

    def running_level(self):
        return self.running_level_processed


class TapeResourceProcessedWithShare(TapeResource):
    def __init__(self, resource_name, ddm, shares_penalty, test=False):
        super().__init__(resource_name, ddm, shares_penalty, test)

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
        #_do_deft_action('mborodin',int(task.id),'resume_task')
        action_executor = TaskActionExecutor('mborodin', 'Resume task after pre stage')
        result, message = action_executor.resumeTask(int(task.id))
        if not result or 'Command rejected' in (message or ''):
            _logger.error(f"Resume command failed for {task.id} with {result} - {message}")



def send_use_archive_task(task):
    _do_deft_action('mborodin',int(task.id),'change_split_rule','UZ','1')

def translate_sub_dataset_name(dataset: str) -> str:
    if '_flt' in dataset:
        base_dataset = dataset.split('_sub')[0]
        if base_dataset.endswith('.'):
            return base_dataset[:-1]
        return base_dataset
    return dataset

def fill_source_replica_template(ddm, dataset, source_replica_template, physical_tape):
    if source_replica_template and ('{source_tape}' in source_replica_template):
        if '_sub' in dataset:
            dataset = translate_sub_dataset_name(dataset)
        dataset_tape_replicas = ddm.full_replicas_per_type(dataset)
        for tape in dataset_tape_replicas['tape']:
            if convert_input_to_physical_tape(tape['rse']) == physical_tape:
                return source_replica_template.replace('{source_tape}', tape['rse'])
        raise Exception('Source is not found')
    else:
        return source_replica_template

def render_destination_rule(rule: str, tape_name: str) -> str:
    if tape_name and '{destination_by_tape}' in rule:
        destination_config = ActionDefault.objects.get(name=tape_name).get_config().get('destination')
        if destination_config:
            return rule.replace('{destination_by_tape}', destination_config)
    return rule

def perfom_dataset_stage(input_dataset, ddm, rule, lifetime, replicas=None):
    try:
        _logger.info('%s should be pre staged rule %s  '%(input_dataset,rule))
        rse = ddm.active_staging_rule(input_dataset)
        if rse:
            return rse['id']
        weight = weight_from_rule(rule)
        if not replicas:
            ddm.add_replication_rule(input_dataset, rule, copies=1, lifetime=lifetime*86400, weight=weight,
                                    activity='Staging', notify='P')
        else:
            ddm.add_replication_rule(input_dataset, rule, copies=1, lifetime=lifetime*86400, weight=weight,
                                    activity='Staging', notify='P',source_replica_expression=replicas)

        return True
    except Exception as e:
        _logger.error("Can't create rule %s" % str(e))
        return False


def append_excluded_rse(rule: str) -> str:
    excluded_rses = SystemParametersHandler.get_excluded_staging_sites().sites
    if excluded_rses and excluded_rses != ['']:
        exclude_string = '\\'+'\\'.join(excluded_rses)
        return f'{rule}{exclude_string}'
    return rule

def prepare_rule(original_rule: str, tape_name: str = '') -> str:
    return render_destination_rule(append_excluded_rse(original_rule), tape_name)

def create_staging_action(input_dataset,task,ddm,rule,config,replicas=None,source=None,lifetime=None,data_replica_wihout_rule=False):
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
        if DatasetStaging.objects.filter(dataset=input_dataset,status__in=['done', 'canceled']).exists():
            dataset_staging = DatasetStaging.objects.filter(dataset=input_dataset,status__in=['done', 'canceled']).first()
            dataset_staging.update_time = None
            dataset_staging.rse = None
            dataset_staging.source = None
            dataset_staging.end_time = None
            dataset_staging.destination_rse = None
        else:
            dataset_staging = DatasetStaging()
            dataset_staging.dataset = input_dataset
        dataset_meta = ddm.dataset_metadata(input_dataset)
        dataset_staging.total_files = dataset_meta['length']
        dataset_staging.dataset_size = dataset_meta['bytes']
        dataset_staging.staged_files = 0
        dataset_staging.staged_size = 0
        dataset_staging.status = 'queued'
        if source:
            dataset_staging.source = source
        dataset_staging.start_time = timezone.now()
        dataset_staging.save()


    else:
        dataset_staging = DatasetStaging.objects.filter(dataset=input_dataset,
                                                        status__in=DatasetStaging.ACTIVE_STATUS).last()
    if dataset_staging.status == 'queued' and ( data_replica_wihout_rule or
                                                (waiting_parameters_from_step and waiting_parameters_from_step.get('nowait', False))):
        source_replica = fill_source_replica_template(ddm, input_dataset, replicas, dataset_staging.source)
        rule = prepare_rule(rule, dataset_staging.source)
        perfom_dataset_stage(input_dataset, ddm, rule, lifetime, source_replica)
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
            source_replica = fill_source_replica_template(ddm, dataset_stage.dataset, action_step.get_config('source_replica'), dataset_stage.source)
            rule = action_step.get_config('rule')
            rule = prepare_rule(rule, dataset_stage.source)
            perfom_dataset_stage(dataset_stage.dataset, ddm, rule, action_step.get_config('lifetime'), source_replica)
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
                    if tape.get_config('maximum_level') < 100000:
                        shares_penalty[share_name] = [(x[0],x[1]* int(tape.get_config('maximum_level')) // 100000) for x in shares_penalty[share_name]]
                    else:
                        shares_penalty[share_name] = [(x[0],x[1]) for x in shares_penalty[share_name]]
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


def filter_replicas_without_rules(ddm, original_dataset):
    replicas = ddm.full_replicas_per_type(original_dataset)
    rules = list(ddm.list_dataset_rules(original_dataset))
    rules_expression = []
    staging_rule = None
    for rule in rules:
        if rule['account'] == 'prodsys' and rule['activity'] == 'Staging':
            staging_rule = rule
        else:
            rules_expression.append(rule['rse_expression'])
    filtered_replicas = {'tape':[],'data':[]}
    data_replica_exists = len(replicas['data']) > 0
    for replica in replicas['tape']:
        if replica['rse'] in rules_expression:
            filtered_replicas['tape'].append(replica)
    if len(replicas['tape']) >= 1 and len(filtered_replicas['tape']) == 0 and len(rules) == 0:
        filtered_replicas['tape'] = replicas['tape']
    for replica in replicas['data']:
        if staging_rule is not None or replica['rse'] in rules_expression:
            filtered_replicas['data'].append(replica)
    all_data_replicas_without_rules = data_replica_exists and len(filtered_replicas['data']) == 0
    return filtered_replicas, staging_rule, all_data_replicas_without_rules



def create_prestage(task,ddm,rule, input_dataset,config, special=None, destination=None):
    #check that's only Tape replica
    original_data_replica = False
    original_dataset = input_dataset
    if '_sub' in input_dataset:
        original_dataset = translate_sub_dataset_name(input_dataset)
        original_data_replica = len(ddm.full_replicas_per_type(input_dataset)['data']) > 0
    replicas, staging_rule, all_data_replicas_without_rules = filter_replicas_without_rules(ddm, original_dataset)
    if ((len(replicas['data']) > 0) or original_data_replica) and not destination:
        start_stagind_task(task)
        if staging_rule:
            try:
                create_replica_extension(task.id, ddm)
            except Exception as e:
                _logger.error(f"Problem with replica extension {str(e)} task {task.id}" )
        return True
    else:
        if staging_rule is not None and staging_rule['expires_at'] and (
                (staging_rule['expires_at'] - timezone.now().replace(tzinfo=None)) < timedelta(days=30)):
            ddm.change_rule_lifetime(staging_rule['id'], 30 * 86400)
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
                    rule, source_replicas, input = ddm.get_replica_pre_stage_rule(original_dataset)
        else:
            if replicas['tape']:
                input = [x['rse'] for x in replicas['tape']]
                if len(input) == 1:
                    input = input[0]
                else:
                    input_without_cern = [x for x in input if 'CERN'  not in x ]
                    if input_without_cern:
                            rule, source_replicas, input = ddm.get_replica_pre_stage_rule_by_rse(random.choice(input_without_cern))
                    else:
                        input = random.choice(input)
        input=convert_input_to_physical_tape(input)
        create_staging_action(input_dataset,task,ddm,rule,config,source_replicas,input,all_data_replicas_without_rules)


def remove_stale_rules(days_after_last_update):
    dataset_in_staging =  DatasetStaging.objects.filter(status='staging')
    for dataset_stage_request in dataset_in_staging:
        dataset_stagings = ActionStaging.objects.filter(dataset_stage=dataset_stage_request)
        running_task = False
        tasks = []
        for stage_task in dataset_stagings:
            tasks.append(stage_task.task)
            if ProductionTask.objects.get(id=stage_task.task).status not in ProductionTask.NOT_RUNNING:
                running_task = True
                break
        if not running_task:
            tasks.sort()
            _logger.warning(f"Rule {dataset_stage_request.rse} has no running tasks {tasks}" )
            if (not dataset_stage_request.update_time) or (timezone.now() - dataset_stage_request.update_time) > timedelta(days=days_after_last_update):
                _logger.error(f"Rule {dataset_stage_request.rse} will be deleted" )
                ddm = DDM()
                if dataset_stage_request.rse:
                    try:
                        ddm.delete_replication_rule(dataset_stage_request.rse)
                    except:
                        _logger.error(f"Problem with rule {dataset_stage_request.rse} deletion" )
                dataset_stage_request.status = DatasetStaging.STATUS.CANCELED
                dataset_stage_request.save()


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
        if step.request.request_type in ['ANALYSIS'] and task.status == ProductionTask.STATUS.STAGING:
            jedi_task = TTask.objects.get(id=task.id)
            input_datasets = []
            config = ActionDefault.objects.get(name='active_staging').get_config()
            if not noidds:
                config['level'] = 1
            if level:
                config['level'] = level
            if TemplateVariable.KEY_NAMES.INPUT_DS in jedi_task.jedi_task_parameters:
                input_container = jedi_task.jedi_task_parameters[TemplateVariable.KEY_NAMES.INPUT_DS]
                input_datasets = ddm.dataset_in_container(input_container)
            for dataset in input_datasets:
                create_prestage(task, ddm, rule, dataset, config, special, destination)
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


def set_replica_to_delete(dataset_stage):
    for action_stage in ActionStaging.objects.filter(dataset_stage=dataset_stage):
        task = ProductionTask.objects.get(id=action_stage.task)
        if task.status not in ProductionTask.NOT_RUNNING:
            if (task.request.request_type in ['REPROCESSING']) or (task.request.request_type == 'MC' and task.request.phys_group != 'VALI'):
                if HASHTAG_STAGE_CAROUSEL not in [str(x) for x in task.hashtags]:
                    task.set_hashtag(HASHTAG_STAGE_CAROUSEL)


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
            if task.status not in ProductionTask.NOT_RUNNING:
                start_stagind_task(task)
                try:
                        ddm.change_rule_lifetime(dataset_stage.rse, 30 * 86400)
                        dataset_stage.update_time = current_time
                        dataset_stage.save()
                except Exception as e:
                    _logger.error("Check do staging problem %s %s" % (str(e), str(action_step_id)))
        if dataset_stage.status == 'staging':
            if True :
                existed_rule = ddm.active_staging_rule(dataset_stage.dataset)
                if existed_rule:
                        if dataset_stage.rse and dataset_stage.rse != existed_rule['id']:
                            _logger.error("do staging change rule from %s to %s for %s" % (str(dataset_stage.rse), str(existed_rule['id']),dataset_stage.dataset))
                        dataset_stage.rse = existed_rule['id']
                        if dataset_stage.staged_files != int(existed_rule['locks_ok_cnt']):
                            dataset_stage.staged_files = int(existed_rule['locks_ok_cnt'])
                            dataset_stage.staged_size = int(dataset_stage.dataset_size * dataset_stage.staged_files / dataset_stage.total_files)
                            dataset_stage.update_time = current_time
                        else:
                            delay = 2*int(action_step.get_config('delay'))
                        if ((existed_rule['expires_at']-timezone.now().replace(tzinfo=None)) < timedelta(days=5)) and \
                                (task.status not in ['done','finished','broken','aborted']) and \
                                ((existed_rule['expires_at']-timezone.now().replace(tzinfo=None)) > timedelta(hours=2)):
                            try:
                                ddm.change_rule_lifetime(existed_rule['id'],15*86400)
                            except Exception as e:
                                _logger.error("Check do staging problem %s %s" % (str(e), str(action_step_id)))
                else:
                    action_finished = False
                    source_replica = fill_source_replica_template(ddm, dataset_stage.dataset, action_step.get_config('source_replica'), dataset_stage.source)
                    rule = action_step.get_config('rule')
                    rule = prepare_rule(rule, dataset_stage.source)
                    if perfom_dataset_stage(dataset_stage.dataset, ddm, rule,
                                                action_step.get_config('lifetime'), source_replica):
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
                dataset_stage.staged_size = dataset_stage.dataset_size
                try:
                    set_replica_to_delete(dataset_stage)
                except Exception as e:
                    _logger.error("Set replica deletion problem %s %s" % (str(e), str(action_step_id)))
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
            if ((task.status in ProductionTask.NOT_RUNNING+['exhausted']) or
                    (task.start_time and (timezone.now() - task.start_time).days>30)) :
                continue
            if task.request.request_type in ['REPROCESSING']:
                share = "{0}{1}".format(JediTasks.objects.get(id=task.id).gshare,task.ami_tag)
            else:
                share = JediTasks.objects.get(id=task.id).gshare
            if task.request.phys_group == 'VALI':
                share = 'VALI'
            if (dataset_stage.status not in ['queued']) and dataset_stage.source:
                priority = task.priority
                if task.current_priority:
                    priority = task.current_priority
                if priority is None:
                    priority = 0
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

# def find_repeated_tasks_to_follow():
#     to_repeat = []
#     try:
#         staged_tasks = []
#         actions = StepAction.objects.filter(action=6, status='done', create_time__gte=timezone.now()-timedelta(days=30))
#         for action in actions:
#             if ActionStaging.objects.filter(step_action=action).exists():
#                 for action_stage in ActionStaging.objects.filter(step_action=action):
#                         staged_tasks.append(action_stage.task)
#         actions = StepAction.objects.filter(action=10, create_time__gte=timezone.now()-timedelta(days=30))
#         for action in actions:
#             if ActionStaging.objects.filter(step_action=action).exists():
#                 for action_stage in ActionStaging.objects.filter(step_action=action):
#                         staged_tasks.append(action_stage.task)
#         used_input = []
#         for task_id in staged_tasks:
#             task = ProductionTask.objects.get(id=task_id)
#             dataset = task.primary_input
#             if dataset not in used_input:
#                 used_input.append(dataset)
#                 repeated_tasks = ProductionTask.objects.filter(id__gt=task_id, submit_time__gte=timezone.now()-timedelta(days=7), primary_input=dataset)
#                 for rep_task in repeated_tasks:
#                     if rep_task.status not in ProductionTask.NOT_RUNNING:
#                         ttask = TTask.objects.get(id=rep_task.id)
#                         if 'inputPreStaging' not  in ttask._jedi_task_parameters and not ActionStaging.objects.filter(task=rep_task.id).exists():
#                             to_repeat.append(rep_task.id)
#         ddm = DDM()
#         for task in to_repeat:
#             if create_replica_extension(task, ddm):
#                 _logger.info("Task {task} is now following".format(task=task))
#     except Exception as e:
#         _logger.error("Create follow repeated input tasks problem: %s" % str(e))
#     return to_repeat

def find_repeated_tasks_to_follow():
    to_repeat = []
    try:
        used_input = []
        dataset_stagings = DatasetStaging.objects.filter(status='done', update_time__gte=timezone.now()-timedelta(days=30))
        for dataset_staging in dataset_stagings:
            dataset = dataset_staging.dataset[dataset_staging.dataset.find(':')+1:]
            if dataset not in used_input:
                used_input.append(dataset)
                repeated_tasks = ProductionTask.objects.filter(submit_time__gte=timezone.now()-timedelta(days=7), primary_input=dataset)
                for rep_task in repeated_tasks:
                    if rep_task.status not in ProductionTask.NOT_RUNNING:
                        if not ActionStaging.objects.filter(task=rep_task.id).exists():
                            to_repeat.append(rep_task.id)
        ddm = DDM()
        for task in to_repeat:
            _logger.info(f"Check task to follow {task}")
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
            existed_rule = ddm.active_staging_rule(dataset_stage.dataset)
            if existed_rule:
                if existed_rule['expires_at'] and (existed_rule['expires_at'] - timezone.now().replace(tzinfo=None)) < timedelta(days=10):
                    try:
                        ddm.change_rule_lifetime(existed_rule['id'], 30 * 86400)
                        dataset_stage.update_time = current_time
                        dataset_stage.save()
                    except Exception as e:
                        _logger.error("Check follow staged problem %s %s" % (str(e), str(waiting_step)))
            else:
                _logger.error("Check follow staged problem rule for %s was deleted step %s new rule will be created" % (dataset_stage.dataset, str(waiting_step)))
                source_replica = fill_source_replica_template(ddm, dataset_stage.dataset, action_step.get_config('source_replica'), dataset_stage.source)
                rule = action_step.get_config('rule')
                rule = prepare_rule(rule, dataset_stage.source)
                perfom_dataset_stage(dataset_stage.dataset, ddm, rule, action_step.get_config('lifetime'), source_replica)

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
        if DatasetStaging.objects.filter(dataset=input_dataset).exists():
            dataset_stage = DatasetStaging.objects.get(dataset=input_dataset)
            existed_rule = ddm.dataset_active_rule_by_rule_id(dataset_stage.dataset, dataset_stage.rse)
            if existed_rule:
                if StepAction.objects.filter(step=task.step.id,action=10).exists():
                    for step_action in StepAction.objects.filter(step=task.step.id,action=10):
                        for action_staging in ActionStaging.objects.filter(step_action=step_action):
                            if action_staging.task == task_id:
                                return False
                if (existed_rule['expires_at'] - timezone.now().replace(tzinfo=None)) < timedelta(days=30):
                    try:
                        ddm.change_rule_lifetime(existed_rule['id'], 30 * 86400)
                        dataset_stage.update_time = timezone.now()
                        dataset_stage.save()
                    except Exception as e:
                        _logger.error("Create replica extension problem %s task: %s" % (str(e), str(task_id)))
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
                if (existed_rule['expires_at'] - timezone.now().replace(tzinfo=None)) < timedelta(days=10):
                    try:
                        ddm.change_rule_lifetime(existed_rule['id'], 30 * 86400)
                        dataset_stage.update_time = current_time
                        dataset_stage.save()
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


def perfom_idds_disable(step_action_ids):
    current_time = timezone.now()
    config = ActionDefault.objects.get(name='disable_idds').get_config()
    for waiting_step in step_action_ids:
        try:
            action_step = StepAction.objects.get(id=waiting_step)
            task_id = action_step.get_config('task')
            task_status = TTask.objects.get(id=task_id).status
            if task_status in ProductionTask.NOT_RUNNING:
                if task_status == 'finished':
                    _logger.info("Retry command with iDDS disabled is sent for %s" % (str(task_id)))
                    _do_deft_action('mborodin',task_id,'retry', False, True)
                    action_step.status = 'done'
                    action_step.message = 'Command send'
                    action_step.done_time = current_time
                    action_step.save()
                else:
                    _logger.error("Retry command with iDDS disabled is not sent for %s because it's %s" % (str(task_id), task_status))
                    action_step.status = 'failed'
                    action_step.message = 'Command not send'
                    action_step.done_time = current_time
                    action_step.save()
            else:
                if action_step.attempt > config['lifetime']:
                    _logger.error("Retry command with iDDS disabled is not sent for %s because it takes too long" % (str(task_id)))
                    action_step.status = 'failed'
                    action_step.message = 'Command not send'
                    action_step.done_time = current_time
                    action_step.save()
                else:
                    action_step.status = 'active'
                    action_step.attempt += 1
                    action_step.execution_time = current_time + timedelta(minutes=config['delay'])
                    action_step.save()

        except Exception as e:
            _logger.error("IDDS disable send command problem: %s %s" % (str(e), str(waiting_step)))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()

def perfom_finish_reload(step_action_ids):
    current_time = timezone.now()
    config = ActionDefault.objects.get(name='reload_input_finished').get_config()
    action_executor = TaskActionExecutor('mborodin', 'Reload input task after finish')

    for waiting_step in step_action_ids:
        try:
            action_step = StepAction.objects.get(id=waiting_step)
            task_id = action_step.get_config('task')
            task_status = TTask.objects.get(id=task_id).status
            if task_status in ProductionTask.NOT_RUNNING:
                if task_status == ProductionTask.STATUS.FINISHED:
                    _logger.info("Reload input is sent for %s" % (str(task_id)))
                    action_executor.reloadInput(task_id)
                    action_step.status = 'done'
                    action_step.message = 'Command send'
                    action_step.done_time = current_time
                    action_step.save()
                else:
                    _logger.error("Reload input is sent  is not sent for %s because it's %s" % (str(task_id), task_status))
                    action_step.status = 'failed'
                    action_step.message = 'Command not send'
                    action_step.done_time = current_time
                    action_step.save()
            else:
                if action_step.attempt > config['lifetime']:
                    _logger.error("Reload input command is not sent for %s because it takes too long" % (str(task_id)))
                    action_step.status = 'failed'
                    action_step.message = 'Command not send'
                    action_step.done_time = current_time
                    action_step.save()
                else:
                    action_step.status = 'active'
                    action_step.attempt += 1
                    action_step.execution_time = current_time + timedelta(minutes=config['delay'])
                    action_step.save()

        except Exception as e:
            _logger.error("Reload input command send command problem: %s %s" % (str(e), str(waiting_step)))
            waiting_step = StepAction.objects.get(id=waiting_step)
            waiting_step.status = 'active'
            waiting_step.save()

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
        elif action == 12:
            perfom_idds_disable(executing_actions[action])
        elif action == 13:
            perfom_finish_reload(executing_actions[action])

@login_required(login_url=OIDC_LOGIN_URL)
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

@login_required(login_url=OIDC_LOGIN_URL)
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


@login_required(login_url=OIDC_LOGIN_URL)
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


@login_required(login_url=OIDC_LOGIN_URL)
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


@login_required(login_url=OIDC_LOGIN_URL)
def step_action(request, wstep_id):

    try:
        action_step = StepAction.objects.get(id=wstep_id)
        tasks_messages = []
        task = None
        ddm= DDM()
        if action_step.action == 6:
            if ActionStaging.objects.filter(step_action=action_step).exists():
                for staging in ActionStaging.objects.filter(step_action=action_step):
                    dataset = staging.dataset_stage.dataset
                    total_files = staging.dataset_stage.total_files
                    staged_files = staging.dataset_stage.staged_files
                    rse = staging.dataset_stage.rse
                    rule_rse = action_step.get_config('rule')
                    if rse:
                        try:
                            rucio_rule = ddm.get_rule(rse)
                        except:
                            rucio_rule = None
                        if rucio_rule:
                            rule_rse = rucio_rule['rse_expression']
                    task = staging.task
                    if ':' not in dataset:
                        dataset = '{0}:{1}'.format(dataset.split('.')[0],dataset)
                    link = '<a href="https://rucio-ui.cern.ch/did?name={name}">{name}</a>'.format(name=str(dataset))
                    rule_link = '<a href="https://rucio-ui.cern.ch/rule?rule_id={rule_id}">{rule_rse}</a>'.format(
                        rule_id=rse, rule_rse=rule_rse)

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
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def finish_action(request, action, action_id):

    try:
        action_step = StepAction.objects.get(id=action_id)
        if action_step.status in ['active','executing', 'done']:
            if action == 'cancel':
                action_step.status = StepAction.STATUS.CANCELED
                action_step.message = 'Action was canceled manually'
                action_step.done_time = timezone.now()
                action_step.save()
            elif action == 'remove':
                if (action_step.action == 6) and (ActionStaging.objects.filter(step_action=action_step).exists()):
                    dataset_staging = ActionStaging.objects.filter(step_action=action_step).last().dataset_stage
                    if dataset_staging.rse:
                        ddm = DDM()
                        ddm.delete_replication_rule(ActionStaging.objects.filter(step_action=action_step).last().dataset_stage.rse)
                    dataset_staging.status = DatasetStaging.STATUS.CANCELED
                    dataset_staging.update_time = timezone.now()
                    dataset_staging.save()
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
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def derivation_requests(request):
    try:
        result = get_stats_replica_to_submitted()
    except Exception as e:
        return Response({'error': str(e)}, status=400)
    return Response(result)



def change_replica_by_task(ddm, task_id, replica=None):
    if ActionStaging.objects.filter(task=task_id).exists():
        action_stage = ActionStaging.objects.filter(task=task_id).last()
        action_step = action_stage.step_action
        dataset_stage = action_stage.dataset_stage
        if dataset_stage.status != 'queued':
            return False

        if not replica or convert_input_to_physical_tape(replica) != dataset_stage.source:
            replicas = ddm.full_replicas_per_type(dataset_stage.dataset)
            if replica is not None:
                if replica not in [x['rse'] for x in replicas['tape']]:
                    return False
            else:
                for new_replica in replicas['tape']:
                  physical_replica = convert_input_to_physical_tape(new_replica['rse'])
                  if physical_replica != dataset_stage.source:
                    replica = new_replica['rse']
                    break
                if replica is None:
                    return  False
            physical_tape = convert_input_to_physical_tape(replica)
            dataset_stage.source = physical_tape
            dataset_stage.save()
        rule, source_replicas, source = ddm.get_replica_pre_stage_rule_by_rse(replica)
        print(rule, source_replicas, source)
        rule = append_excluded_rse(rule)
        action_step.set_config({'rule': rule})
        action_step.set_config({'tape': convert_input_to_physical_tape(source)})
        action_step.set_config({'source_replica': source_replicas})
        action_step.save()
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
            task = ProductionTask.objects.get(id=other_actions.task)
            if task.status not in (ProductionTask.RED_STATUS + ['done','obsolete']):
                if not ((task.status == 'finished') and (task.total_files_failed == 0)):
                    do_deletion = False
                    break
        if do_deletion:
            rule = None
            try:
                rule = ddm.get_rule(dataset_stage.rse)
            except:
                pass
            if rule:
                _logger.info("Rule %s will be deleted" % str(dataset_stage.rse))
                ddm.delete_replication_rule(dataset_stage.rse)
            return True
        return False
    else:
        return True


def find_stage_task_replica_to_delete():
    hashtag = HashTag.objects.get(hashtag=HASHTAG_STAGE_CAROUSEL)
    ddm = DDM()
    for task_id in hashtag.tasks_ids:
        try:
            if check_replica_can_be_deleted(task_id,ddm):
                task = ProductionTask.objects.get(id=task_id)
                task.remove_hashtag(HASHTAG_STAGE_CAROUSEL)
        except Exception as e:
            _logger.error("Staging replica deletion problem %s %s" % (str(e), str(task_id)))


def clean_stale_actions(days=2):
    actions = StepAction.objects.filter(status='executing', execution_time__lte=timezone.now()-timedelta(days=2))
    for action in actions:
        _logger.error("Action %s is stuck" % (str(action.id)))
        action.status = 'active'
        action.save()


def find_failed_files(task_id, dataset_name):
    if ':' not in dataset_name:
        dataset_name = dataset_name.split('.')[0]+':'+dataset_name
    dataset_id = JediDatasets.objects.get(id=task_id, datasetname=dataset_name).datasetid
    failed_files = list(JediDatasetContents.objects.filter(datasetid=dataset_id, jeditaskid=task_id,
                                                           procstatus__in=['failed','ready','running']).values_list('lfn',flat=True))
    return list(set(failed_files))


def keep_failed_files(task_id):
    task = ProductionTask.objects.get(id=task_id)
    input_dataset = task.inputdataset
    dataset_stage = DatasetStaging.objects.get(dataset=input_dataset)
    ddm = DDM()
    try:
        rse_dict = ddm.get_rule(dataset_stage.rse)
    except:
        rse_dict = None
    if rse_dict and task.status == 'finished' and task.total_files_failed != 0 and task.total_files_failed < 100:
        keep_files = True
        for other_actions in ActionStaging.objects.filter(dataset_stage=dataset_stage):
            other_task = ProductionTask.objects.get(id=other_actions.task)
            if (other_task != task) and ((timezone.now() - other_task.timestamp) < timedelta(days=30)):
                if task.status not in (ProductionTask.RED_STATUS + ['done','obsolete']):
                    if not ((task.status == 'finished') and (task.total_files_failed == 0)):
                        keep_files = False
                        break
        if keep_files:
            data_replicas =  ddm.full_replicas_per_type(input_dataset)['data']
            if data_replicas:
                failed_files_list = find_failed_files(task_id, input_dataset)
                name_base = input_dataset
                if '_tid' in input_dataset:
                    name_base = input_dataset.split('_tid')[0]
                new_name = name_base + '_sub' + str(task_id) + '_stg'
                scope = input_dataset.split('.')[0]
                files = [scope + ':' + file_name for file_name in failed_files_list]
                lifetime = int((rse_dict['expires_at'] - timezone.now().replace(tzinfo=None)).total_seconds())
                print(new_name, files, lifetime)
                ddm.register_dataset(new_name, files, meta={'task_id':task_id}, lifetime=lifetime)
                for replica in data_replicas:
                    print(new_name, replica['rse'])
                    ddm.add_replication_rule(new_name, replica['rse'], copies=1, lifetime=lifetime, weight=None,
                                                           activity='Staging')
                time.sleep(5)
                replicas = list(ddm.list_dataset_rules(new_name))
                old_rule = dataset_stage.rse
                if replicas:
                    dataset_stage.rse = replicas[0]['id']
                    dataset_stage.destination_rse = None
                    dataset_stage.save()
                ddm.delete_replication_rule(old_rule)


@dataclass
class StagingRequestWithTasks:
    dataset: DatasetStaging
    tasks: List[int] = field(default_factory=list)


def find_stale_stages(days=10):

    queued_replicas = DatasetStaging.objects.filter(dataset__startswith='data',status=DatasetStaging.STATUS.QUEUED,
                                                    start_time__lte=timezone.now()-timedelta(days=days))
    stage_requests = []
    for dataset_staging in queued_replicas:
        stage_request = StagingRequestWithTasks(dataset_staging)
        for action in ActionStaging.objects.filter(dataset_stage=dataset_staging):
            task = ProductionTask.objects.get(id=action.task)
            if task.status not in ProductionTask.NOT_RUNNING:
                stage_request.tasks.append(action.task)
        if stage_request.tasks:
            stage_requests.append(stage_request)
    stage_requests.sort(key=lambda x: x.dataset.start_time)
    ddm = DDM()
    task_to_resubmit_by_tape = {}
    for stage_request in stage_requests:
        replicas, stage_rule, data_replica_w_rule = filter_replicas_without_rules(ddm, stage_request.dataset.dataset)
        if len(replicas['tape']) > 1 and stage_rule is None:
            use_cern = False
            new_tape = ''
            if ('CERN' not in stage_request.dataset.source) and ('CERN' in ''.join([x['rse'] for x in replicas['tape']])):
                use_cern = True
            for tape_replica in replicas['tape']:
                if use_cern:
                    if 'CERN' in tape_replica['rse']:
                        new_tape = tape_replica['rse']
                elif convert_input_to_physical_tape(tape_replica['rse']) != stage_request.dataset.source:
                    new_tape = tape_replica['rse']
            task_to_resubmit_by_tape[new_tape] = task_to_resubmit_by_tape.get(new_tape,[]) + [stage_request]
    for tape_queue, stage_requests in task_to_resubmit_by_tape.items():
        physical_tape = convert_input_to_physical_tape(tape_queue)
        if not DatasetStaging.objects.filter(status=DatasetStaging.STATUS.QUEUED, source=physical_tape).exists():
            queue_config = ActionDefault.objects.get(type='PHYSICAL_TAPE', name=physical_tape)
            if (queue_config.get_config('allow_rebalancing') is not None) and queue_config.get_config('allow_rebalancing'):
                maximum_level = queue_config.get_config('maximum_level')
                tasks = []
                level = 0
                for stage_request in stage_requests:
                    tasks += stage_request.tasks
                    level += stage_request.dataset.total_files
                    if level > maximum_level:
                        break
                if tasks:
                    _logger.info("Change source replica to %s for tasks %s" % (str(tape_queue),str(tasks)))
                    for task in tasks:
                        change_replica_by_task(ddm, task, tape_queue)
                print(tape_queue, tasks)





def staging_rule_verification(dataset: str, stuck_days: int = 10) -> (bool,bool):
    """

    :param dataset:
    :param stuck_days:
    :return: Two booleans: 1) Is rule not updated for stuck_days and 2) if any of files stuck due to tape problem
    """

    if not DatasetStaging.objects.filter(dataset=dataset).exists():
        raise ValueError(f'Staging for {dataset} is not found')
    if not  DatasetStaging.objects.filter(dataset=dataset,status=DatasetStaging.STATUS.STAGING,
                                          update_time__lte=timezone.now()-timedelta(days=stuck_days)).exists():
        return False, False
    ddm = DDM()
    dataset_staging = DatasetStaging.objects.filter(dataset=dataset).last()
    rule_id = dataset_staging.rse
    # Check rucio claims it's Tape problem:
    rule_info = ddm.get_rule(rule_id)
    if rule_info.get('error') and ('[TAPE SOURCE]' in rule_info.get('error')):
        return True, True
    # Get list of files which are not yet staged
    stuck_files = [ file_lock['name'] for file_lock in ddm.list_locks(rule_id) if file_lock['state'] != 'OK']
    # Check in ES that files have failed attempts from tape. Limit to 1000 files, should be enough
    connection = Elasticsearch(hosts=MONIT_ES['hosts'],http_auth=(MONIT_ES['login'], MONIT_ES['password']),
                               verify_certs=MONIT_ES['verify_certs'], ca_certs=MONIT_ES['ca_cert'], timeout=10000)
    days_since_start = stuck_days
    if dataset_staging.start_time and ((timezone.now() - dataset_staging.start_time).days > days_since_start):
            days_since_start = (timezone.now() - dataset_staging.start_time).days
    tape_replicas = ddm.full_replicas_per_type(dataset_staging.dataset)['tape']
    # Find source Tape replica
    source = None
    for replica in tape_replicas:
        if convert_input_to_physical_tape(replica['rse']) == dataset_staging.source:
            source = replica['rse']
            break
    if not source:
        raise ValueError(f'{dataset_staging.dataset} tape replica is not found')
    s = Search(using=connection, index='monit_prod_ddm_enr_*').\
        query("terms", data__name=stuck_files[:1000]).\
        query("range", **{
                "metadata.timestamp": {
                    "gte": f"now-{days_since_start}d/d",
                    "lt": "now/d"
                }}).\
        query("match", data__event_type='transfer-failed').\
        query('match', data__src_endpoint=source)
    if s.count() > 0:
        return True, True
    return True, False


def staging_rule_file_errors(dataset: str):
    """
    Returns list of files which are not staged due to tape problem  (not due to other reasons)
    :param dataset:
    :return:
    """
    if not DatasetStaging.objects.filter(dataset=dataset).exists():
        raise ValueError(f'Staging for {dataset} is not found')
    if not  DatasetStaging.objects.filter(dataset=dataset,status=DatasetStaging.STATUS.STAGING).exists():
        raise ValueError(f'Staging for {dataset} is  done ')
    ddm = DDM()
    dataset_staging = DatasetStaging.objects.filter(dataset=dataset).last()
    rule_id = dataset_staging.rse
    # Check rucio claims it's Tape problem:
    rule_info = ddm.get_rule(rule_id)

    # Get list of files which are not yet staged
    stuck_files = [ file_lock['name'] for file_lock in ddm.list_locks(rule_id) if file_lock['state'] != 'OK']
    # Find source Tape replica
    source = None
    tape_replicas = ddm.full_replicas_per_type(dataset_staging.dataset)['tape']
    for replica in tape_replicas:
        if convert_input_to_physical_tape(replica['rse']) == dataset_staging.source:
            source = replica['rse']
            break
    if not source:
        raise ValueError(f'{dataset_staging.dataset} tape replica is not found')
    connection = Elasticsearch(hosts=MONIT_ES['hosts'],http_auth=(MONIT_ES['login'], MONIT_ES['password']),
                               verify_certs=MONIT_ES['verify_certs'],  ca_certs=MONIT_ES['ca_cert'], timeout=10000)
    days_since_start = (timezone.now() - dataset_staging.start_time).days
    a = A('terms', field='data.name')
    s = Search(using=connection, index='monit_prod_ddm_enr_*').\
        query("terms", data__name=stuck_files[:100]).\
        query("range", **{
        "metadata.timestamp": {
            "gte": f"now-{days_since_start}d/d",
            "lt": "now/d"
        }}).\
        query("match", data__event_type='transfer-failed').\
        query('match', data__src_endpoint=source)

    return s


# def staging_rule_percentage(dataset: str) -> (int,int):
#     """
#
#     :param dataset:
#     :param stuck_days:
#     :return: Two booleans: 1) Is rule not updated for stuck_days and 2) if any of files stuck due to tape problem
#     """
#
#     if not DatasetStaging.objects.filter(dataset=dataset).exists():
#         raise ValueError(f'Staging for {dataset} is not found')
#     if not  DatasetStaging.objects.filter(dataset=dataset,status=DatasetStaging.STATUS.DONE).exists():
#         raise ValueError(f'Staging for {dataset} is not done yet')
#     ddm = DDM()
#     dataset_staging = DatasetStaging.objects.filter(dataset=dataset).last()
#     rule_id = dataset_staging.rse
#     # Check rucio claims it's Tape problem:
#
#     # Get list of files which are not yet staged
#     stuck_files = [ file_lock['name'] for file_lock in ddm.list_locks(rule_id) if file_lock['state'] != 'OK']
#     # Check in ES that files have failed attempts from tape. Limit to 1000 files, should be enough
#     connection = Elasticsearch(hosts=MONIT_ES['hosts'],http_auth=(MONIT_ES['login'], MONIT_ES['password']),
#                                verify_certs=MONIT_ES['verify_certs'], timeout=10000)
#     days_since_start = stuck_days
#     tape_replicas = ddm.full_replicas_per_type(dataset_staging.dataset)['tape']
#     # Find source Tape replica
#     source = None
#     for replica in tape_replicas:
#         if convert_input_to_physical_tape(replica['rse']) == dataset_staging.source:
#             source = replica['rse']
#             break
#     if not source:
#         raise ValueError(f'{dataset_staging.dataset} tape replica is not found')
#     s = Search(using=connection, index='monit_prod_ddm_enr_*').\
#         query("terms", data__name=stuck_files[:1000]).\
#         query("range", **{
#                 "metadata.timestamp": {
#                     "gte": f"now-{days_since_start}d/d",
#                     "lt": "now/d"
#                 }}).\
#         query("match", data__event_type='transfer-failed').\
#         query('match', data__src_endpoint=source)
#     if s.count() > 0:
#         return True, True
#     return True, False

def staging_tasks_by_destination(destination_rse: str):
    staging_datasets = DatasetStaging.objects.filter(status__in=[DatasetStaging.STATUS.STAGING,DatasetStaging.STATUS.DONE],
                                                     update_time__gte=days_ago(30), destination_rse=destination_rse)
    destination_tasks = []
    for dataset in staging_datasets:
        tasks = [ProductionTask.objects.get(id=x) for x in ActionStaging.objects.filter(dataset_stage=dataset).values_list('task', flat=True)]
        destination_tasks += filter(lambda x: x.status not in ProductionTask.NOT_RUNNING, tasks)
    return destination_tasks

def fill_staging_destination():
    ddm = DDM()
    for dataset_stage in DatasetStaging.objects.filter(status=DatasetStaging.STATUS.STAGING):
        if not dataset_stage.destination_rse:
            try:
                first_file = ddm.list_locks(dataset_stage.rse).__next__()
                dataset_stage.destination_rse = first_file['rse']
                _logger.info(f"Fill destination for {dataset_stage.dataset} with {dataset_stage.destination_rse}")
                dataset_stage.save()
            except:
                pass


def recover_destination_excluded_from_nucleus(destination_rse: str):
    tasks = staging_tasks_by_destination(destination_rse)
    action_executor = TaskActionExecutor('mborodin', 'Release task since dest was excluded from nucleus')
    for task in tasks:
        action_executor.release_task(task.id)
    return tasks

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def data_carousel_config(request):
    try:
        excludeSites = SystemParametersHandler.get_excluded_staging_sites().sites
        carouselTapes = []
        for tape_info in ActionDefault.objects.filter(type='PHYSICAL_TAPE').order_by('name'):
            carouselTapes.append({'tapeName': tape_info.name, 'min_bulksize': tape_info.get_config('minimum_level'),
                                  'max_bulksize': tape_info.get_config('maximum_level'),
                                  'batchdelay': tape_info.get_config('continious_percentage'),
                                  'active': tape_info.get_config('active'),
                                  'baseRule': tape_info.get_config('destination')})
        return Response({'excludeSites': excludeSites, 'carouselTapes': carouselTapes}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def staging_analysis_step(task_id: int) -> StepExecution:
    DEFAULT_REQUEST = 301
    DEFAULT_SLICE = 0

    if not ProductionTask.objects.filter(id=task_id).exists():
        raise ValueError(f'Task {task_id} is not found')
    current_step = ProductionTask.objects.get(id=task_id).step
    if current_step.request.reqid == 32:
        task = ProductionTask.objects.get(id=task_id)
        current_step.id = None
        current_step.request = TRequest.objects.get(reqid=DEFAULT_REQUEST)
        current_step.slice = InputRequestList.objects.get(request=DEFAULT_REQUEST, slice=DEFAULT_SLICE)
        current_step.save()
        task.step = current_step
        task.save()
    return ProductionTask.objects.get(id=task_id).step


def create_analyis_staging(task_id: int) -> int:
    step = staging_analysis_step(task_id)
    sa = StepAction()
    sa.action = 14
    sa.request = step.request
    sa.step = step.id
    sa.attempt = 0
    sa.create_time = timezone.now()
    sa.execution_time = timezone.now() + timedelta(minutes=2)
    sa.status = 'active'
    sa.save()
    return sa.id