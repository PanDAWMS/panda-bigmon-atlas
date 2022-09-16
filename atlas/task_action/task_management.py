import json
import re
from abc import ABC, abstractmethod

from atlas.JIRA.client import JIRAClient
from atlas.jedi.client import JEDIClient, JEDITaskActionInterface
import logging
from django.utils import timezone

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.hashtag import add_or_get_request_hashtag
from atlas.prodtask.models import ProductionTask, TRequest, ActionStaging, StepAction
from atlas.prodtask.task_views import sync_deft_jedi_task

logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')
from dataclasses import dataclass


class DEFTAction(ABC):

    @abstractmethod
    def create_disable_idds_action(self, task_id):
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

    ES_PATTERN = "https://es-atlas.cern.ch/kibana/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-7d,to:now))&_a=(columns:!(task,action,params,prod_request,user,return_code,return_message),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,key:funcName,negate:!f,params:(query:_log_production_task_action_message),type:phrase),query:(match_phrase:(funcName:_log_production_task_action_message))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,key:prod_request,negate:!f,params:(query:{0}),type:phrase),query:(match_phrase:(prod_request:{0})))),index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,interval:M,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))"
    JIRA_MESSAGE_TEMPLATE = "Tasks actions for this request can be found [es-atlas|{link}]"

    def __init__(self, username, comment=''):
        self.jedi_client = JEDIClient()
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
        return self.jedi_client.killTask(jediTaskID)

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
        return self.jedi_client.retryTask(jediTaskID, verbose, noChildRetry, discardEvents, disable_staging_mode)

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

        return False, 'No staging rule is found'

    @_action_logger
    def obsolete_task(self, task_id):
        task = ProductionTask.objects.get(id=task_id)
        if task.status in ProductionTask.OBSOLETE_READY_STATUS:
            task.status = ProductionTask.STATUS.OBSOLETE
            task.timestamp = timezone.now()
            task.save()
        return True, ''

    @_action_logger
    def sync_jedi(self, task_id):
        sync_deft_jedi_task(task_id)
        return True, ''

    @_action_logger
    def set_hashtag(self, task_id, hashtag_name):
        task = ProductionTask.objects.get(id=task_id)
        hashtag = add_or_get_request_hashtag(hashtag_name[0])
        task.set_hashtag(hashtag)
        return True, ''

    @_action_logger
    def remove_hashtag(self, task_id, hashtag_name):
        task = ProductionTask.objects.get(id=task_id)
        hashtag = add_or_get_request_hashtag(hashtag_name[0])
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
