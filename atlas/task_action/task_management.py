import json
import re
from abc import ABC, abstractmethod

from django.contrib.auth.models import User
from rest_framework.request import Request

from atlas.JIRA.client import JIRAClient
from atlas.jedi.client import JEDIClient, JEDITaskActionInterface, JEDIClientTest
import logging
from django.utils import timezone

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.hashtag import add_or_get_request_hashtag
from atlas.prodtask.models import ProductionTask, TRequest, ActionStaging, StepAction, JediTasks
from atlas.prodtask.task_views import sync_deft_jedi_task
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework import status


logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')
from dataclasses import dataclass, asdict


class DEFTAction(ABC):

    @abstractmethod
    def create_disable_idds_action(self, task_id):
        pass

    @abstractmethod
    def create_finish_reload_action(self, task_id):
        pass

    # @abstractmethod
    # def increase_task_priority(self, task_id, delta):
    #     pass
    #
    # @abstractmethod
    # def decrease_task_priority(self, task_id, delta):
    #     pass

    @abstractmethod
    def obsolete_task(self, task_id):
        pass

    @abstractmethod
    def sync_jedi(self, task_id):
        pass

    @abstractmethod
    def set_hashtag(self, task_id, hashtag_name):
        pass

    @abstractmethod
    def remove_hashtag(self, task_id, hashtag_name):
        pass

    @abstractmethod
    def retry_new(self, task_id):
        pass

    @abstractmethod
    def clean_task_carriages(self, task_id, output_formats):
        pass

    @abstractmethod
    def kill_jobs_in_task(self, task_id, jobs_id_str, code, keepUnmerged):
        pass


@dataclass
class TaskActionExecutor(JEDITaskActionInterface, DEFTAction):

    username: str
    comment: str

    #ES_PATTERN = "https://es-atlas.cern.ch/kibana/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-7d,to:now))&_a=(columns:!(task,action,params,prod_request,user,return_code,return_message),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,key:funcName,negate:!f,params:(query:_log_production_task_action_message),type:phrase),query:(match_phrase:(funcName:_log_production_task_action_message))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,key:prod_request,negate:!f,params:(query:{0}),type:phrase),query:(match_phrase:(prod_request:{0})))),index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,interval:M,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))"
    ES_PATTERN = "https://os-atlas.cern.ch/dashboards/app/data-explorer/discover/#?_a=(discover:(columns:!(task,action,params,prod_request,user,return_code,return_message,_source),interval:M,sort:!(!('@timestamp',desc))),metadata:(indexPattern:bce7ecb0-7533-11eb-ba28-77fe4323ac05,view:discover))&_q=(filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,key:funcName,negate:!f,params:(query:_log_production_task_action_message),type:phrase),query:(match_phrase:(funcName:_log_production_task_action_message))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,key:prod_request,negate:!f,params:(query:{0}),type:phrase),query:(match_phrase:(prod_request:{0})))),query:(language:kuery,query:''))&_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-7d,to:now))"
    JIRA_MESSAGE_TEMPLATE = "Tasks actions for this request can be found [os-atlas|{link}]"

    def __init__(self, username, comment='', jedi_client=JEDIClient()):
        self.jedi_client = jedi_client
        self.username = username
        self.jira_client = None
        self.comment = comment

    @staticmethod
    def _log_production_task_action_message(username, comment, production_request_id, task_id, action, return_code, return_message, *args):
        _jsonLogger.info("Production task action",
                         extra={'task': str(task_id), 'prod_request': production_request_id,'user': username, 'action': action, 'params': json.dumps(args),
                                'return_code': str(return_code),'return_message': return_message ,'comment': comment})


    def _log_analysis_task_action_message(self, task_id, action, return_code, return_message, *args):
        _jsonLogger.info("Analysis task action",
                         extra={'task': str(task_id), 'user': self.username, 'comment': self.comment,
                                'action': action, 'params': json.dumps(args),
                                'return_code': str(return_code), 'return_message': return_message})


    def _log_action_message(self, task_id, action, return_code, return_message, *args):
        try:

            if ProductionTask.objects.filter(id=task_id).exists():
                task = ProductionTask.objects.get(id=task_id)
                if task.request_id > 300:
                    production_request = task.request
                    es_link = self.ES_PATTERN.format(production_request.reqid)
                    jira_message = self.JIRA_MESSAGE_TEMPLATE.format(link=es_link)
                    if production_request.jira_reference and not production_request.info_field('task_jira_es'):
                        if not self.jira_client:
                            self.jira_client = JIRAClient()
                            self.jira_client.authorize()
                        self.jira_client.add_issue_comment(production_request.jira_reference, jira_message)
                        production_request.set_info_field('task_jira_es', True)
                        production_request.save()
                    self._log_production_task_action_message(self.username, self.comment, production_request.reqid, task_id, action, return_code, return_message, *args)
                    return
            self._log_analysis_task_action_message(task_id, action, return_code, return_message, *args)
        except Exception as ex:
            logger.error(f"Action logging problem: {ex}")
            print(f"Action logging problem: {ex}")


    def _jedi_decorator(func):
        def inner(self, task_id, *args, **kwargs):
            try:
                result = func(self, task_id, *args, **kwargs)
                logger.info(f"JEDI action {task_id} {func.__name__} with parameters {args} from {self.username} result  {result}")
                return_code, return_message = result
                if type(return_code) is int and return_code == 0:
                    return_code = True
                self._log_action_message(task_id, func.__name__, bool(return_code), return_message, *args)
                return bool(return_code), return_message
            except Exception as ex:
                self._log_action_message(task_id, func.__name__, False, str(ex), *args)
                return False, str(ex)
        return inner

    def _action_logger(func):
        def inner(self, task_id, *args, **kwargs):
            try:
                return_code, return_message = func(self, task_id, *args, **kwargs)
                self._log_action_message(task_id, func.__name__, return_code, return_message, *args)
                return return_code, return_message
            except Exception as ex:
                self._log_action_message(task_id, func.__name__, False, str(ex), *args)
                return False, str(ex)
        return inner

    _jedi_decorator = staticmethod(_jedi_decorator)
    _action_logger = staticmethod(_action_logger)

    @_jedi_decorator
    def changeTaskPriority(self, jediTaskID, newPriority):
        return self.jedi_client.changeTaskPriority(jediTaskID, newPriority)

    @_jedi_decorator
    def killTask(self, jediTaskID):
        result = self.jedi_client.killTask(jediTaskID)
        if result[0] == 0:
            try:
                task = ProductionTask.objects.get(id=jediTaskID)
                task.status = ProductionTask.STATUS.TOABORT
                task.save()
            except:
                pass
        return result

    @_jedi_decorator
    def finishTask(self, jediTaskID, soft=False):
        return self.jedi_client.finishTask(jediTaskID, soft)

    @_jedi_decorator
    def changeTaskRamCount(self, jediTaskID, ramCount):
        return self.jedi_client.changeTaskRamCount(jediTaskID, ramCount)

    @_jedi_decorator
    def reassignTaskToSite(self, jediTaskID, site, mode=None):
        return self.jedi_client.reassignTaskToSite(jediTaskID, site, mode)

    @_jedi_decorator
    def reassignTaskToCloud(self, jediTaskID, cloud, mode=None):
        return  self.jedi_client.reassignTaskToCloud(jediTaskID, cloud, mode)

    @_jedi_decorator
    def reassignTaskToNucleus(self, jediTaskID, nucleus, mode=None):
        return self.jedi_client.reassignTaskToNucleus( jediTaskID, nucleus, mode)


    @_jedi_decorator
    def changeTaskWalltime(self, jediTaskID, wallTime):
        return  self.jedi_client.changeTaskWalltime(jediTaskID, wallTime)

    @_jedi_decorator
    def changeTaskCputime(self, jediTaskID, cpuTime):
        return self.jedi_client.changeTaskCputime(jediTaskID, cpuTime)

    @_jedi_decorator
    def changeTaskSplitRule(self, jediTaskID, ruleName, ruleValue):
        return self.jedi_client.changeTaskSplitRule(jediTaskID, ruleName, ruleValue)

    @_jedi_decorator
    def changeTaskAttribute(self, jediTaskID, attrName, attrValue):
        return self.jedi_client.changeTaskAttribute(jediTaskID, attrName, attrValue)

    @_jedi_decorator
    def retryTask(self, jediTaskID, verbose=False, noChildRetry=False, discardEvents=False, disable_staging_mode=False):
        result = self.jedi_client.retryTask(jediTaskID, verbose, noChildRetry, discardEvents, disable_staging_mode)
        if result[0] == 0:
            try:
                task = ProductionTask.objects.get(id=jediTaskID)
                if task.status in ProductionTask.NOT_RUNNING:
                    task.status = ProductionTask.STATUS.TORETRY
                    task.save()
            except:
                pass
        return result

    @_jedi_decorator
    def reloadInput(self, jediTaskID, verbose=False):
        return self.jedi_client.reloadInput(jediTaskID, verbose)

    @_jedi_decorator
    def pauseTask(self, jediTaskID, verbose=False):
        return self.jedi_client.pauseTask(jediTaskID, verbose)

    @_jedi_decorator
    def resumeTask(self, jediTaskID, verbose=False):
        return self.jedi_client.resumeTask(jediTaskID, verbose)

    @_jedi_decorator
    def release_task(self, jediTaskID, verbose=False):
        return self.jedi_client.release_task(jediTaskID, verbose)

    @_jedi_decorator
    def reassignShare(self, jediTaskID, share, reassign_running=False):
        return self.jedi_client.reassignShare([jediTaskID, ], share, reassign_running)

    @_jedi_decorator
    def triggerTaskBrokerage(self, jediTaskID):
        return self.jedi_client.triggerTaskBrokerage(jediTaskID)

    @_jedi_decorator
    def avalancheTask(self, jediTaskID, verbose=False):
        return self.jedi_client.avalancheTask(jediTaskID, verbose)

    @_jedi_decorator
    def increaseAttemptNr(self, jediTaskID, increase):
        return self.jedi_client.increaseAttemptNr(jediTaskID, increase)

    @_jedi_decorator
    def killUnfinishedJobs(self, jediTaskID, code=None, verbose=False, srvID=None, useMailAsID=False):
        return self.jedi_client.killUnfinishedJobs(jediTaskID, code, verbose, srvID, useMailAsID)

    @_action_logger
    def create_disable_idds_action(self, task_id):
        task = ProductionTask.objects.get(id=task_id)
        if ActionStaging.objects.filter(task=task.id).exists():
            if task.total_files_finished > 0:
                step = task.step
                actions = StepAction.objects.filter(step=step.id, action=12, status__in=StepAction.ACTIVE_STATUS)
                action_exists = False
                for action in actions:
                    if action.get_config('task') == task_id:
                        action_exists = True
                        break
                if not action_exists:
                    new_action = StepAction()
                    new_action.step = step.id
                    new_action.action = 12
                    new_action.set_config({'task': int(task_id)})
                    new_action.attempt = 0
                    new_action.status = StepAction.STATUS.ACTIVE
                    new_action.request = step.request
                    new_action.create_time = timezone.now()
                    new_action.execution_time = timezone.now()
                    new_action.save()
                    return self.finishTask(task_id)
            else:
                if task.status == 'staging':
                    try:
                        dataset_stage = ActionStaging.objects.filter(task=task.id)[0].dataset_stage
                        ddm = DDM()
                        rule = ddm.get_rule(dataset_stage.rse)
                        if rule['locks_ok_cnt'] == 0:
                            ddm.delete_replication_rule(dataset_stage.rse)
                        else:
                            return self.resumeTask(task_id)
                        return True, ''
                    except Exception as e:
                        return False, str(e)
                else:
                    return False, 'Task has no finished jobs'
        return False, 'Command rejected: No staging rule is found'

    @_action_logger
    def create_finish_reload_action(self, task_id):
        task = ProductionTask.objects.get(id=task_id)
        if task.total_files_finished > 0:
            step = task.step
            actions = StepAction.objects.filter(step=step.id, action=13, status__in=['active','executing'])
            action_exists = False
            for action in actions:
                if action.get_config('task') == task_id:
                    return False, 'Command rejected: Finish-reload input already exist'
            if not action_exists:
                new_action = StepAction()
                new_action.step = step.id
                new_action.action = 13
                new_action.set_config({'task':int(task_id)})
                new_action.attempt = 0
                new_action.status = 'active'
                new_action.request = step.request
                new_action.create_time = timezone.now()
                new_action.execution_time = timezone.now()
                new_action.save()
                return self.finishTask(task_id, True)
        return False, 'Command rejected: No jobs are finished yet'

    @_action_logger
    def obsolete_task(self, task_id):
        task = ProductionTask.objects.get(id=task_id)
        if task.status in ProductionTask.OBSOLETE_READY_STATUS:
            task.status = ProductionTask.STATUS.OBSOLETE
            task.timestamp = timezone.now()
            task.save()
            output_datasets = list(task.output_non_log_datasets())
            if filter(lambda x: 'EVNT' in x, output_datasets):
                # remove dataset from containers
                ddm = DDM()
                for dataset in output_datasets:
                    production_container = ddm.get_production_container_name(dataset)
                    if dataset in ddm.with_and_without_scope(ddm.dataset_in_container(production_container)):
                        ddm.delete_datasets_from_container(production_container, [dataset])
                    sample_container = ddm.get_sample_container_name(dataset)
                    if dataset in ddm.with_and_without_scope(ddm.dataset_in_container(sample_container)):
                        ddm.delete_datasets_from_container(sample_container, [dataset])
                    if dataset.startswith('mc16'):
                        sample_container = ddm.get_sample_container_name(dataset.replace('mc16', 'mc15'))
                        if dataset in ddm.with_and_without_scope(ddm.dataset_in_container(sample_container)):
                            ddm.delete_datasets_from_container(sample_container, [dataset])
        return True, ''


    def sync_jedi(self, task_id):
        sync_deft_jedi_task(task_id)
        return True, ''

    @_action_logger
    def set_hashtag(self, task_id, hashtag_name):
        task = ProductionTask.objects.get(id=task_id)
        hashtag = add_or_get_request_hashtag(hashtag_name)
        task.set_hashtag(hashtag)
        return True, ''

    @_action_logger
    def remove_hashtag(self, task_id, hashtag_name):
        task = ProductionTask.objects.get(id=task_id)
        hashtag = add_or_get_request_hashtag(hashtag_name)
        task.remove_hashtag(hashtag)
        return True, ''

    @_action_logger
    def retry_new(self, task_id):
        raise NotImplementedError("Not yet implemented")

    @_action_logger
    def clean_task_carriages(self, task_id, output_formats):
        raise NotImplementedError("Not yet implemented")

    @_jedi_decorator
    def kill_jobs_in_task(self, task_id, jobs_id_str, code=None, keepUnmerged=False):
        jobs_id = [int(x) for x in re.split(r"[^a-zA-Z0-9]", str(jobs_id_str)) if x]
        return self.jedi_client.killJobs(jobs_id, code=code, keepUnmerged=keepUnmerged)

@dataclass
class TaskActionAllowed():
    id: int
    action_allowed: bool = False
    user_allowed: bool = False

class TaskManagementAuthorisation():

    allowed_task_actions = {}
    __user_permitions = {}
    CHANGE_PARAMETERS_ACTIONS = ['change_priority', 'change_ram_count', 'change_wall_time', 'change_cpu_time', 'change_core_count', 'change_split_rule']
    REASSIGN_ACTIONS = ['reassign_to_site', 'reassign_to_cloud', 'reassign_to_nucleus', 'reassign_to_share']

    @dataclass
    class __AuthorisationTask():
        id: int
        name: str = ''
        owner: str = ''
        status: str = ''
        phys_group: str = ''
        request_phys_group: str = ''
        is_analy: bool = False
        is_group_production: bool = False

    def define_allowed_task_actions(self):
        #All task actions
        for status in  ProductionTask.ALL_JEDI_STATUS:
            self.allowed_task_actions[status] = ['set_hashtag', 'remove_hashtag', 'sync_jedi', 'abort']
        #Active task actions
        for status in ProductionTask.ALL_JEDI_STATUS:
            if status not in (ProductionTask.NOT_RUNNING + [ProductionTask.STATUS.ABORTING, ProductionTask.STATUS.TOABORT]):
                self.allowed_task_actions[status].extend(['finish',
                                                            'pause_task', 'resume_task', 'trigger_task',
                                                            'avalanche_task', 'reload_input', 'release_task',
                                                            'increase_attempt_number', 'abort_unfinished_jobs',
                                                          'disable_idds', 'kill_job', 'retry', 'finish_plus_reload'] +
                                                         self.CHANGE_PARAMETERS_ACTIONS +
                                                         self.REASSIGN_ACTIONS)
            if status == ProductionTask.STATUS.FINISHED:
                self.allowed_task_actions[status].extend(['retry', 'obsolete', 'retry_new', 'finish', 'reload_input', 'disable_idds', 'finish_plus_reload'] +
                                                         self.CHANGE_PARAMETERS_ACTIONS +
                                                         self.REASSIGN_ACTIONS)
            if status == ProductionTask.STATUS.DONE:
                self.allowed_task_actions[status].extend(['obsolete'])
            if status == ProductionTask.STATUS.OBSOLETE:
                self.allowed_task_actions[status].extend(['obsolete'])

        pass

    def check_task_allowed(self, status: str, action: str) -> bool:
        return action in self.allowed_task_actions[status]


    def get_task_info(self, task_id: int) -> __AuthorisationTask :
        task = self.__AuthorisationTask(id=task_id)
        if ProductionTask.objects.filter(id=task_id).exists():
            task_dict = ProductionTask.objects.values('username', 'name', 'status', 'request_id', 'phys_group',
                                                      'request__phys_group', 'request__request_type').get(id=task_id)
            task.owner = task_dict.get('username')
            task.name = task_dict.get('name')
            task.status = task_dict.get('status')
            task.phys_group = task_dict.get('phys_group')
            task.request_phys_group = task_dict.get('request__phys_group')
            task.is_group_production = (task_dict.get('request__request_type') == 'GROUP') and (task.phys_group not in ['VALI']) and (task.request_phys_group not in ['VALI'])
            task.is_analy = (task_dict.get('request_id') == 300) or task_dict.get('request__request_type') == 'ANALYSIS'
        else:
            task_dict = JediTasks.objects.values('username', 'taskname', 'status').get(id=task_id)
            task.owner = task_dict.get('username')
            task.name = task_dict.get('taskname')
            task.status = task_dict.get('status')
            task.is_analy = True
        if task.is_analy and task.name.startswith('group'):
                task.phys_group = task.name.split('.')[1]
                task.request_phys_group = task.phys_group
        return task

    def task_user_rights(self, username: str, user_fullname:str =None):
        allowed_groups = []
        group_permissions = []
        if User.objects.filter(username=username).exists():
            user = User.objects.get(username=username)
            user_groups = user.groups.all()
            for gp in user_groups:
                group_permissions += list(gp.permissions.all())
                if gp.name.startswith('IAM:atlas/') and gp.name.endswith('/production'):
                    allowed_groups.append(gp.name.split('/')[-2])
            for gp in group_permissions:
                if "has_" in gp.name and "_permissions" in gp.name:
                    allowed_groups.append(gp.codename)
        else:
            user = User(username=username)
            if user_fullname:
                user.last_name = user_fullname
        return user, allowed_groups


    def user_authorization(self, user: User, allowed_groups: [str], task: __AuthorisationTask, action: str, params, user_fullname) -> bool:
        if user.is_superuser:
            return True
        if not task.is_analy:
            if "DPD" in allowed_groups:
                return True
            if "MCCOORD" in allowed_groups:
                return True
            if (task.phys_group in allowed_groups) or (task.request_phys_group in allowed_groups) or (user.username == task.owner):
                if task.is_group_production:
                    if action in self.REASSIGN_ACTIONS:
                        return False
                    if action == 'change_priority' and int(params[0]) > 570:
                        return False
                return True
            return False
        else:
            if (f'{user.first_name} {user.last_name}' == task.owner) or (user_fullname and (user_fullname == task.owner)) or\
                    (task.name.split('.')[1] == user.username) or (task.phys_group in allowed_groups):
                return True

        return False


    def __task_action_check(self, task: __AuthorisationTask, user: User, allowed_groups: [str],  action: str, params, user_fullname) -> (bool, bool):
        user_is_allowed = False
        task_is_allowed = self.check_task_allowed(task.status, action)
        if task_is_allowed:
            user_is_allowed = self.user_authorization(user, allowed_groups, task, action, params, user_fullname)
        return task_is_allowed, user_is_allowed


    def task_action_authorisation(self, task_id: int, username: str, action: str, params=None, user_fullname: str=None) -> (bool, bool):
        user, allowed_groups = self.task_user_rights(username, user_fullname)
        task = self.get_task_info(task_id)
        return self.__task_action_check(task, user, allowed_groups, action, params, user_fullname)

    def tasks_action_authorisation(self, task_ids: [int], username: str, action: str, params=None, user_fullname: str=None) -> [TaskActionAllowed]:
        user, allowed_groups = self.task_user_rights(username)
        result = []
        for task_id in task_ids:
            task = self.get_task_info(task_id)
            action_allowed, user_allowed = self.__task_action_check(task, user, allowed_groups, action, params, user_fullname)
            result.append(TaskActionAllowed(task.id, action_allowed, user_allowed))
        return result

    def __init__(self):
        self.define_allowed_task_actions()
@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def tasks_action(request: Request):
    try:
        tasks_id = request.data['tasksID']
        action = request.data['action']
        params =  request.data['params']
        username = request.user.username
        user_fullname = None
        if 'user_fullname' in request.data:
             user_fullname = request.data['user_fullname']
        authentification_management = TaskManagementAuthorisation()
        tasks_allowed = authentification_management.tasks_action_authorisation(tasks_id, username, action, params, user_fullname)
        for task_verified in tasks_allowed:
            if not task_verified.user_allowed or not task_verified.action_allowed:
                logger.error(f"Action {action} for user {username} is not allowed for task {tasks_id}")
                return Response({'action_sent':False, 'action_verification': [asdict(x) for x in tasks_allowed], 'result': None})
        comment = request.data['comment']
        executor = TaskActionExecutor(username, comment)
        result = []
        if params:
            for task in tasks_id:
                return_code, return_info = do_jedi_action(executor, task, action, *params)
                result.append({'task_id':task, 'return_code': return_code, 'return_info': return_info})
        else:
            for task in tasks_id:
                return_code, return_info = do_jedi_action(executor, task, action, None)
                result.append({'task_id':task, 'return_code': return_code, 'return_info': return_info})
        return Response({'action_sent':True, 'result': result, 'action_verification':None})
    except Exception as ex:
        return Response(data=f"Task action execution problem: {ex}", status=status.HTTP_400_BAD_REQUEST)


def do_jedi_action(action_executor, task_id, action, *args):
    action_translation = {
            'abort': action_executor.killTask,
            'finish': action_executor.finishTask,
            'change_priority': action_executor.changeTaskPriority,
            'reassign_to_site': action_executor.reassignTaskToSite,
            'reassign_to_cloud': action_executor.reassignTaskToCloud,
            'reassign_to_nucleus': action_executor.reassignTaskToNucleus,
            'reassign_to_share': action_executor.reassignShare,
            'retry': action_executor.retryTask,
            'change_ram_count': action_executor.changeTaskRamCount,
            'change_wall_time': action_executor.changeTaskWalltime,
            'change_cpu_time': action_executor.changeTaskCputime,
            'increase_attempt_number': action_executor.increaseAttemptNr,
            'abort_unfinished_jobs': action_executor.killUnfinishedJobs,
            'delete_output': action_executor.clean_task_carriages,
            'kill_job': action_executor.kill_jobs_in_task,
            'obsolete': action_executor.obsolete_task,
            'change_core_count': action_executor.changeTaskAttribute,
            'change_split_rule': action_executor.changeTaskSplitRule,
            'pause_task': action_executor.pauseTask,
            'resume_task': action_executor.resumeTask,
            'trigger_task': action_executor.triggerTaskBrokerage,
            'avalanche_task': action_executor.avalancheTask,
            'reload_input': action_executor.reloadInput,
            'retry_new': action_executor.retry_new,
            'set_hashtag': action_executor.set_hashtag,
            'remove_hashtag': action_executor.remove_hashtag,
            'sync_jedi': action_executor.sync_jedi,
            'disable_idds': action_executor.create_disable_idds_action,
            'finish_plus_reload': action_executor.create_finish_reload_action,
            'release_task': action_executor.release_task
        }
    if args == (None,):
         return action_translation[action](task_id)
    return action_translation[action](task_id, *args)