import json
import logging
import gzip
import pickle
from copy import deepcopy
from abc import ABC, abstractmethod
from typing import Optional

import requests
from urllib.parse import urlencode
from ..settings import jediclient as jedi_settings

_logger = logging.getLogger('prodtaskwebui')



class JEDITaskActionInterface(ABC):

    @abstractmethod
    def changeTaskPriority(self, jediTaskID, newPriority):
        pass


    @abstractmethod
    def killTask(self, jediTaskID):
        pass

    @abstractmethod
    def finishTask(self, jediTaskID, soft):
        pass

    @abstractmethod
    def changeTaskRamCount(self, jediTaskID, ramCount):
        pass

    @abstractmethod
    def reassignTaskToSite(self, jediTaskID, site, mode):
        pass

    @abstractmethod
    def reassignTaskToCloud(self, jediTaskID, cloud, mode):
        pass

    @abstractmethod
    def reassignTaskToNucleus(self, jediTaskID, nucleus, mode):
        pass

    @abstractmethod
    def changeTaskWalltime(self, jediTaskID, wallTime):
        pass

    @abstractmethod
    def changeTaskCputime(self, jediTaskID, cpuTime):
        pass

    @abstractmethod
    def changeTaskSplitRule(self, jediTaskID, ruleName, ruleValue):
        pass

    @abstractmethod
    def changeTaskAttribute(self, jediTaskID, attrName, attrValue):
        pass

    @abstractmethod
    def retryTask(self, jedi_task_id, new_parameters, no_child_retry, discard_events,
                  disable_staging_mode, keep_gshare_priority, ignore_hard_exhausted):
        pass

    @abstractmethod
    def reloadInput(self, jediTaskID, ignore_hard_exhausted):
        pass

    @abstractmethod
    def pauseTask(self, jediTaskID):
        pass

    @abstractmethod
    def resumeTask(self, jediTaskID):
        pass

    @abstractmethod
    def reassignShare(self, jedi_task_ids, share, reassign_running):
        pass

    @abstractmethod
    def triggerTaskBrokerage(self, jediTaskID):
        pass

    @abstractmethod
    def release_task(self, jediTaskID):
        pass

    @abstractmethod
    def avalancheTask(self, jediTaskID):
        pass

    @abstractmethod
    def increaseAttemptNr(self, jediTaskID, increase):
        pass

    @abstractmethod
    def killUnfinishedJobs(self, jediTaskID, code, useMailAsID):
        pass

    @abstractmethod
    def enable_job_cloning(self, jedi_task_id: int, mode: Optional[str] = None, multiplicity: Optional[int] = None, num_sites: Optional[int] = None):
        pass


class JEDIJobsActionInterface(ABC):

    @abstractmethod
    def killJobs(self, ids, code, useMailAsID, keepUnmerged, jobSubStatus):
        pass

    @abstractmethod
    def reassignJobs(self, ids, forPending, firstSubmission):
        pass

class JEDIRuleActionInterface(ABC):

    @abstractmethod
    def change_staging_destination(self, dataset: str, request_id: int|None,):
        pass

    @abstractmethod
    def change_staging_source(self, dataset: str, request_id: int|None, cancel_fts: bool = False,  change_src_expr: bool = False, source_rse: Optional[str] = None):
        pass

    @abstractmethod
    def force_to_staging(self, dataset: str, request_id: int|None):
        pass



EC_Failed = 255

class JEDIClient(JEDITaskActionInterface, JEDIJobsActionInterface, JEDIRuleActionInterface):
    def __init__(self, base_url=jedi_settings.JEDI_BASE_URL, cert=jedi_settings.CERTIFICATE ):
        """Initializes new instance of JEDI class

        :param cert: path to certificate or to proxy
        :param base_url: JEDI REST API base url
        """

        self.cert = cert
        self._base_url = base_url
        #self._headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        self._headers = {'Content-Type': 'application/json'}

    def _form_url(self, command):
        return self._base_url + '/' + command


    def  _post_command(self, command, data, convert_boolean=True):
        url = self._form_url(command)
        if convert_boolean:
            data = self._convert_boolean_to_string(data)
        response = requests.get(url, cert=self.cert, data=gzip.compress(json.dumps(data).encode('utf-8')),
                                headers=self._headers, verify='/etc/ssl/certs/CERN-bundle.pem')
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        return self._jedi_output_distillation(response.content)

    def  _post_new_api_command(self, command, data):
        url = self._form_url(command).replace('server/panda/', '')
        headers = self._headers.copy()
        headers['Accept'] = 'application/json'
        response = requests.post(url, cert=self.cert, json=data,
                                headers=headers, verify='/etc/ssl/certs/CERN-bundle.pem')
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        return response.json()

    def enable_job_cloning(self, jedi_task_id: int, mode: Optional[str] = None, multiplicity: Optional[int] = None, num_sites: Optional[int] = None):
        data = {'jedi_task_id': int(jedi_task_id), 'mode': mode, 'multiplicity': multiplicity, 'num_sites': num_sites}
        return self._post_new_api_command('api/v1/task/enable_job_cloning', data)

    def change_staging_destination(self, dataset: str, request_id: int|None):
        data = {'request_id': request_id, 'dataset': dataset}
        return self._post_new_api_command('api/v1/data_carousel/change_staging_destination', data)

    def change_staging_source(self, dataset: str, request_id: int|None, cancel_fts: bool = False,  change_src_expr: bool = False, source_rse: Optional[str] = None):
        data = {'request_id': request_id, 'dataset': dataset, 'cancel_fts': cancel_fts, 'change_src_expr': change_src_expr, 'source_rse': source_rse}
        return self._post_new_api_command('api/v1/data_carousel/change_staging_source', data)

    def force_to_staging(self, dataset: str, request_id: int|None):
        data = {'request_id': request_id, 'dataset': dataset}
        return self._post_new_api_command('api/v1/data_carousel/force_to_staging', data)

    # change task priority
    def changeTaskPriority(self, jediTaskID, newPriority):
        """Change task priority
        """
        data = {'task_id': int(jediTaskID),
                'priority': int(newPriority)}
        return self._post_new_api_command('api/v1/task/change_priority', data)

    # kill task
    def killTask(self, jediTaskID):
        data = { 'task_id': int(jediTaskID)}
        return self._post_new_api_command('api/v1/task/kill', data)

    # finish task
    def finishTask(self, jediTaskID, soft=False):
        """Finish a task
        """
        data = { 'task_id': int(jediTaskID), 'soft': soft}
        return self._post_new_api_command('api/v1/task/finish', data)

    def changeTaskRamCount(self, jediTaskID, ramCount):
        """Change task priority
        """
        data = {'task_id': int(jediTaskID),
                'attribute_name': 'ramCount',
                'value': int(ramCount)}
        return self._post_new_api_command('api/v1/task/change_attribute', data)

    # reassign task to a site
    def reassignTaskToSite(self, jediTaskID, site, mode=None):
        """Reassign a task to a site. Existing jobs are killed and new jobs are generated at the site
        """
        data = {'task_id': int(jediTaskID), 'site': site}
        if mode is not None:
            data['mode'] = mode
        return self._post_new_api_command('api/v1/task/reassign', data)

    # reassign task to a cloud
    def reassignTaskToCloud(self, jediTaskID, cloud, mode=None):
        """Reassign a task to a cloud. Existing jobs are killed and new jobs are generated in the cloud

        """
        data = {'task_id': int(jediTaskID), 'cloud': cloud}
        if mode is not None:
            data['mode'] = mode
        return self._post_new_api_command('api/v1/task/reassign', data)

    # reassign task to a nucleus
    def reassignTaskToNucleus(self, jediTaskID, nucleus, mode=None):
        """Reassign a task to a nucleus. Existing jobs are killed and new jobs are generated in the cloud
        """
        data = {'task_id': int(jediTaskID), 'nucleus': nucleus}
        if mode is not None:
            data['mode'] = mode
        return self._post_new_api_command('api/v1/task/reassign', data)

    # reassign jobs
    def reassignJobs(self, ids, forPending=False, firstSubmission=None):
        """Triggers reassignment of jobs. This is not effective if jobs were preassigned to sites before being submitted.
           args:
               ids: the list of taskIDs
               forPending: set True if pending jobs are reassigned
               firstSubmission: set True if first jobs are submitted for a task, or False if not
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return code
                     True: request is processed
                     False: not processed
        """
        # serialize
        data = {'job_ids': ids}
        return self._post_new_api_command('api/v1/job/reassign', data)

    # change task walltime
    def changeTaskWalltime(self, jediTaskID, wallTime):
        """Change task priority
        """
        data = {'task_id': int(jediTaskID),
                'attribute_name': 'wallTime',
                'value': int(wallTime)}
        return self._post_new_api_command('api/v1/task/change_attribute', data)


    # change task cputime
    def changeTaskCputime(self, jediTaskID, cpuTime):
        """Change task cpuTime
        """
        data = {'task_id': int(jediTaskID),
                'attribute_name': 'cpuTime',
                'value': int(cpuTime)}
        return self._post_new_api_command('api/v1/task/change_attribute', data)


    # change split rule for task
    def changeTaskSplitRule(self, jediTaskID, ruleName, ruleValue):
        """Change split rule fo task
        """
        # instantiate curl
        data = {'task_id': int(jediTaskID),
                'attribute_name': ruleName,
                'value': ruleValue}
        return self._post_new_api_command('api/v1/task/change_split_rule', data)

    # change task attribute
    def changeTaskAttribute(self, jediTaskID, attrName, attrValue):
        """Change task attribute
        """
        data = {'task_id': int(jediTaskID),
                'attribute_name': attrName,
                'value': int(attrValue)}
        return self._post_new_api_command('api/v1/task/change_attribute', data)


    def retryTask(self, jedi_task_id, new_parameters: dict = None, no_child_retry=False, discard_events=False,
                  disable_staging_mode=False, keep_gshare_priority=False, ignore_hard_exhausted=False):
        """Retry task
        task_id(int): JEDI Task ID
        new_parameters(Dict, optional): a json dictionary with the new parameters for rerunning the task. The new parameters are merged with the existing ones.
                                        The parameters are the attributes in the JediTaskSpec object (https://github.com/PanDAWMS/panda-jedi/blob/master/pandajedi/jedicore/JediTaskSpec.py).
        no_child_retry(bool, optional): if True, the child tasks are not retried
        discard_events(bool, optional): if True, events will be discarded
        disable_staging_mode(bool, optional): if True, the task skips staging state and directly goes to subsequent state
        keep_gshare_priority(bool, optional): if True, the task keeps current gshare and priority
        ignore_hard_exhausted(bool, optional): if True, the task ignores the limits for hard exhausted state and can be retried even if it is very faulty
            Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
        """
        if not new_parameters:
            new_parameters = None
        data = {'task_id': int(jedi_task_id), 'new_parameters': new_parameters, 'no_child_retry': no_child_retry,
                'discard_events': discard_events, 'disable_staging_mode': disable_staging_mode,
                'keep_gshare_priority': keep_gshare_priority, 'ignore_hard_exhausted': ignore_hard_exhausted}
        return self._post_new_api_command('api/v1/task/retry', data)

    # reload input
    def reloadInput(self, jediTaskID, ignore_hard_exhausted=False):
        data = {'task_id': int(jediTaskID), 'ignore_hard_exhausted': ignore_hard_exhausted}
        return self._post_new_api_command('api/v1/task/reload_input', data)

    # pause task
    def pauseTask(self, jediTaskID):
        """Pause task
        """
        data = {'task_id': int(jediTaskID)}
        return self._post_new_api_command('api/v1/task/pause', data)


    def resumeTask(self, jediTaskID):
        """Resume task
        """
        data = {'task_id': int(jediTaskID)}
        return self._post_new_api_command('api/v1/task/resume', data)

    def release_task(self, jediTaskID):
        """release task from staging
        """
        data = {'task_id': int(jediTaskID)}
        return self._post_new_api_command('api/v1/task/release', data)


    def reassignShare(self, jedi_task_ids, share, reassign_running=False):
        """
           args:
               jedi_task_ids: task ids to act on
               share: share to be applied to jeditaskids
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return: a tuple of return code and message
                     1: logical error
                     0: success
                     None: database error
        """
        data = {'task_id_list': list(map(int, jedi_task_ids)),
                'share': share,
                'reassign_running_jobs': reassign_running}
        return self._post_new_api_command('api/v1/task/reassign_global_share', data)

    def triggerTaskBrokerage(self, jediTaskID):
        """Trigger task brokerge
        """
        data = {'task_id': int(jediTaskID),
                'hour_offset': -12}
        return self._post_new_api_command('api/v1/task/change_modification_time', data)


    def avalancheTask(self, jediTaskID):
        """force avalanche for task
        """
        data = {'task_id': int(jediTaskID)}
        return self._post_new_api_command('api/v1/task/avalanche', data)


    def increaseAttemptNr(self, jediTaskID, increase):
        data = {'task_id': int(jediTaskID),
                'increase': int(increase)}
        return self._post_new_api_command('api/v1/task/increase_attempts', data)

    def killUnfinishedJobs(self, jediTaskID, code=None, useMailAsID=False):
        """Kill unfinished jobs in a task.
            code
                2: expire
                3: aborted
                4: expire in waiting
                7: retry by server
                8: rebrokerage
                9: force kill
                10: fast rebrokerage in overloaded PQ
                50: kill by JEDI
                51: reassigned by JEDI
                52: force kill by JEDI
                55: killed since task is (almost) done
                60: workload was terminated by the pilot without actual work
                91: kill user jobs with prod role
                99: force kill user jobs with prod role

        """

        data = {'task_id': int(jediTaskID)}
        if code is not None:
            data['code'] = int(code)
        return self._post_new_api_command('api/v1/task/kill_unfinished_jobs', data)


    def killJobs(self, ids, code=None, useMailAsID=False, keepUnmerged=False, jobSubStatus=None):
        """Kill jobs. Normal users can kill only their own jobs.
        """

        data = {'job_ids': [int(job_id) for job_id in ids.split(',')], 'use_email_as_id': useMailAsID}
        if code is not None:
            data['code'] = int(code)
        kill_options = []
        if keepUnmerged:
            kill_options.append('keepUnmerged')
        if jobSubStatus is not None:
            kill_options.append(f'jobSubStatus={jobSubStatus}')
        if kill_options:
            data['kill_options'] = kill_options
        return self._post_new_api_command('api/v1/job/kill', data)

    def setDebugMode(self, job_id, debug_mode):
        data = {"job_id": int(job_id), "mode": debug_mode}
        return self._post_new_api_command('api/v1/job/set_debug_mode', data)

    @staticmethod
    def _jedi_output_distillation(jedi_respond_raw):
        jedi_respond = jedi_respond_raw
        if type(jedi_respond_raw) is bytes:
            try:
                jedi_respond = pickle.loads(jedi_respond_raw)
            except:
                try:
                    jedi_respond = json.loads(jedi_respond_raw)
                except:
                    jedi_respond = [True, jedi_respond_raw.decode('utf-8')]
        return_code = -1
        return_info = ''
        if type(jedi_respond) is int:
            return_code = jedi_respond
        elif (type(jedi_respond) is tuple) or (type(jedi_respond) is list):
            if len(jedi_respond) == 2 :
                return_code = jedi_respond[0]
                return_info = jedi_respond[1]
            else:
                return_code = jedi_respond[0]
        return return_code, return_info

    @staticmethod
    def _convert_boolean_to_string(data):
        converted_data = deepcopy(data)
        if type(converted_data) is dict:
            for key in converted_data:
                if type(converted_data[key]) is bool:
                    if converted_data[key]:
                        converted_data[key] = 'True'
                    else:
                        converted_data[key] = 'False'
        return converted_data


class JEDIClientTest(JEDIClient):

    def __init__(self, base_url=jedi_settings.JEDI_BASE_URL, cert=jedi_settings.CERTIFICATE):
        super().__init__(base_url, cert)

    def _post_command(self, command, data, convert_boolean=True):
        return 1, f"{command} {data}"