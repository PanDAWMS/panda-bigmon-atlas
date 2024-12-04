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
    def retryTask(self, jediTaskID, verbose, noChildRetry, discardEvents, disable_staging_mode):
        pass

    @abstractmethod
    def reloadInput(self, jediTaskID, verbose):
        pass

    @abstractmethod
    def pauseTask(self, jediTaskID, verbose):
        pass

    @abstractmethod
    def resumeTask(self, jediTaskID, verbose):
        pass

    @abstractmethod
    def reassignShare(self, jedi_task_ids, share, reassign_running):
        pass

    @abstractmethod
    def triggerTaskBrokerage(self, jediTaskID):
        pass

    @abstractmethod
    def release_task(self, jediTaskID, verbose):
        pass

    @abstractmethod
    def avalancheTask(self, jediTaskID, verbose):
        pass

    @abstractmethod
    def increaseAttemptNr(self, jediTaskID, increase):
        pass

    @abstractmethod
    def killUnfinishedJobs(self, jediTaskID, code, verbose, srvID, useMailAsID):
        pass

    @abstractmethod
    def enable_job_cloning(self, jedi_task_id: int, mode: Optional[str] = None, multiplicity: Optional[int] = None, num_sites: Optional[int] = None):
        pass


class JEDIJobsActionInterface(ABC):

    @abstractmethod
    def killJobs(self, ids, code, verbose, srvID, useMailAsID, keepUnmerged, jobSubStatus):
        pass

    @abstractmethod
    def reassignJobs(self, ids, forPending, firstSubmission):
        pass


EC_Failed = 255

class JEDIClient(JEDITaskActionInterface, JEDIJobsActionInterface):
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

    # change task priority
    def changeTaskPriority(self, jediTaskID, newPriority):
        """Change task priority
           args:
               jediTaskID: jediTaskID of the task to change the priority
               newPriority: new task priority
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return code
                     0: unknown task
                     1: succeeded
                     None: database error
        """

        data = {'jediTaskID': jediTaskID,
                'newPriority': newPriority}
        return self._post_command('changeTaskPriority', data)


    # kill task
    def killTask(self, jediTaskID):
        """Kill a task
           args:
               jediTaskID: jediTaskID of the task to be killed
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """

        data = {'properErrorCode': True, 'jediTaskID': jediTaskID}
        return self._post_command('killTask',data)




    # finish task
    def finishTask(self, jediTaskID, soft=False):
        """Finish a task
           args:
               jediTaskID: jediTaskID of the task to be finished
               soft: If True, new jobs are not generated and the task is
                     finihsed once all remaining jobs are done.
                     If False, all remaining jobs are killed and then the
                     task is finished
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """

        data = {'properErrorCode': True, 'jediTaskID': jediTaskID}
        if soft:
            data['soft'] = True
        return self._post_command('finishTask',data)

    def changeTaskRamCount(self, jediTaskID, ramCount):
        """Change task priority
           args:
               jediTaskID: jediTaskID of the task to change the priority
               ramCount: new ramCount for the task
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return code
                     0: unknown task
                     1: succeeded
                     None: database error
        """
        data = {'jediTaskID': jediTaskID,
                'attrName': 'ramCount',
                'attrValue': ramCount}
        return self._post_command('changeTaskAttributePanda',data)

    # reassign task to a site
    def reassignTaskToSite(self, jediTaskID, site, mode=None):
        """Reassign a task to a site. Existing jobs are killed and new jobs are generated at the site
           args:
               jediTaskID: jediTaskID of the task to be reassigned
               site: the site name where the task is reassigned
               mode: If soft, only defined/waiting/assigned/activated jobs are killed. If nokill, no jobs are killed. All jobs are killed by default.
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """
        maxSite = 60
        if site is not None and len(site) > maxSite:
            return EC_Failed, f'site parameter is too long > {maxSite}chars'
        data = {'jediTaskID': jediTaskID, 'site': site}
        if mode is not None:
            data['mode'] = mode
        return self._post_command('reassignTask', data)

    # reassign task to a cloud
    def reassignTaskToCloud(self, jediTaskID, cloud, mode=None):
        """Reassign a task to a cloud. Existing jobs are killed and new jobs are generated in the cloud
           args:
               jediTaskID: jediTaskID of the task to be reassigned
               cloud: the cloud name where the task is reassigned
               mode: If soft, only defined/waiting/assigned/activated jobs are killed. If nokill, no jobs are killed. All jobs are killed by default.
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """

        # execute
        data = {'jediTaskID': jediTaskID, 'cloud': cloud}
        if mode is not None:
            data['mode'] = mode
        return self._post_command('reassignTask',data)

    # reassign task to a nucleus
    def reassignTaskToNucleus(self, jediTaskID, nucleus, mode=None):
        """Reassign a task to a nucleus. Existing jobs are killed and new jobs are generated in the cloud
           args:
               jediTaskID: jediTaskID of the task to be reassigned
               nucleus: the nucleus name where the task is reassigned
               mode: If soft, only defined/waiting/assigned/activated jobs are killed. If nokill, no jobs are killed. All jobs are killed by default.
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """
        data = {'jediTaskID': jediTaskID, 'nucleus': nucleus}
        if mode is not None:
            data['mode'] = mode
        return self._post_command('reassignTask',data)

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
        strIDs = pickle.dumps(ids, protocol=0).decode('utf-8')
        data = {'ids': strIDs}
        if forPending:
            data['forPending'] = True
        if firstSubmission is not None:
            if firstSubmission:
                data['firstSubmission'] = True
            else:
                data['firstSubmission'] = False
        return self._post_command('reassignJobs',data)

    # change task walltime
    def changeTaskWalltime(self, jediTaskID, wallTime):
        """Change task priority
           args:
               jediTaskID: jediTaskID of the task to change the priority
               wallTime: new walltime for the task
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return code
                     0: unknown task
                     1: succeeded
                     None: database error
        """
        # instantiate curl
        data = {'jediTaskID': jediTaskID,
                'attrName': 'wallTime',
                'attrValue': wallTime}
        return self._post_command('changeTaskAttributePanda',data)

    # change task cputime
    def changeTaskCputime(self, jediTaskID, cpuTime):
        """Change task cpuTime
           args:
               jediTaskID: jediTaskID of the task to change the priority
               cpuTime: new cputime for the task
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return code
                     0: unknown task
                     1: succeeded
                     None: database error
        """
        # instantiate curl
        data = {'jediTaskID': jediTaskID,
                'attrName': 'cpuTime',
                'attrValue': cpuTime}
        return self._post_command('changeTaskAttributePanda',data)

    # change split rule for task
    def changeTaskSplitRule(self, jediTaskID, ruleName, ruleValue):
        """Change split rule fo task
           args:
               jediTaskID: jediTaskID of the task to change the rule
               ruleName: rule name
               ruleValue: new value for the rule
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return: a tupple of return code and message
                     0: unknown task
                     1: succeeded
                     2: disallowed to update the attribute
                     None: database error
        """
        # instantiate curl
        data = {'jediTaskID': jediTaskID,
                'attrName': ruleName,
                'attrValue': ruleValue}
        return self._post_command('changeTaskSplitRulePanda',data)

    # change task attribute
    def changeTaskAttribute(self, jediTaskID, attrName, attrValue):
        """Change task attribute
           args:
               jediTaskID: jediTaskID of the task to change the attribute
               attrName: attribute name
               attrValue: new value for the attribute
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return: a tupple of return code and message
                     0: unknown task
                     1: succeeded
                     2: disallowed to update the attribute
                     None: database error
        """
        data = {'jediTaskID': jediTaskID,
                'attrName': attrName,
                'attrValue': attrValue}
        return self._post_command('changeTaskAttributePanda',data)


    def retryTask(self, jediTaskID, verbose=False, noChildRetry=False, discardEvents=False, disable_staging_mode=False):
        """Retry task
           args:
               jediTaskID: jediTaskID of the task to retry
               noChildRetry: True not to retry child tasks
               discardEvents: discard events
               disable_staging_mode: disable staging mode
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """
        # instantiate curl
        data = {'jediTaskID': jediTaskID, 'properErrorCode': True}
        if noChildRetry:
            data['noChildRetry'] = True
        if discardEvents:
            data['discardEvents'] = True
        if disable_staging_mode:
            data['disable_staging_mode'] = True
        return self._post_command('retryTask',data)

    # reload input
    def reloadInput(self, jediTaskID, verbose=False):
        """Retry task
           args:
               jediTaskID: jediTaskID of the task to retry
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """
        data = {'jediTaskID': jediTaskID}
        return self._post_command('reloadInput',data)

    # pause task
    def pauseTask(self, jediTaskID, verbose=False):
        """Pause task
           args:
               jediTaskID: jediTaskID of the task to pause
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """
        data = {'jediTaskID': jediTaskID}
        return self._post_command('pauseTask',data)


    def resumeTask(self, jediTaskID, verbose=False):
        """Resume task
           args:
               jediTaskID: jediTaskID of the task to release
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """

        data = {'jediTaskID': jediTaskID}
        return self._post_command('resumeTask',data)

    def release_task(self, jediTaskID, verbose=False):
        """release task from staging

        args:
            jedi_task_id: jediTaskID of the task to avalanche
        returns:
            status code
                  0: communication succeeded to the panda server
                  255: communication failure
            tuple of return code and diagnostic message
                  0: request is registered
                  1: server error
                  2: task not found
                  3: permission denied
                  4: irrelevant task status
                100: non SSL connection
                101: irrelevant taskID
        """

        data = {'jedi_task_id': jediTaskID}
        return self._post_command('release_task',data)

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
        jedi_task_ids_pickle = pickle.dumps(jedi_task_ids, protocol=0).decode('utf-8')
        data = {'jedi_task_ids_pickle': jedi_task_ids_pickle,
                'share': share,
                'reassign_running': reassign_running}
        return self._post_command('reassignShare',data, False)

    def triggerTaskBrokerage(self, jediTaskID):
        """Trigger task brokerge
           args:
               jediTaskID: jediTaskID of the task to change the attribute
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return: a tupple of return code and message
                     0: unknown task
                     1: succeeded
                     None: database error
        """
        data = {'jediTaskID': jediTaskID,
                'diffValue': -12}
        return self._post_command('changeTaskModTimePanda',data)


    def avalancheTask(self, jediTaskID, verbose=False):
        """force avalanche for task
           args:
               jediTaskID: jediTaskID of the task to avalanche
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               tuple of return code and diagnostic message
                     0: request is registered
                     1: server error
                     2: task not found
                     3: permission denied
                     4: irrelevant task status
                   100: non SSL connection
                   101: irrelevant taskID
        """
        data = {'jediTaskID': jediTaskID}
        return self._post_command('avalancheTask',data)

    def increaseAttemptNr(self, jediTaskID, increase):
        """Change task priority
           args:
               jediTaskID: jediTaskID of the task to increase attempt numbers
               increase: increase for attempt numbers
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               return code
                     0: succeeded
                     1: unknown task
                     2: invalid task status
                     3: permission denied
                     4: wrong parameter
                     None: database error
        """
        data = {'jediTaskID': jediTaskID,
                'increasedNr': increase}
        return self._post_command('increaseAttemptNrPanda',data)

    def killUnfinishedJobs(self, jediTaskID, code=None, verbose=False, srvID=None, useMailAsID=False):
        """Kill unfinished jobs in a task. Normal users can kill only their own jobs.
        People with production VOMS role can kill any jobs.
        Running jobs are killed when next heartbeat comes from the pilot.
        Set code=9 if running jobs need to be killed immediately.
           args:
               jediTaskID: the taskID of the task
               code: specify why the jobs are killed
                     2: expire
                     3: aborted
                     4: expire in waiting
                     7: retry by server
                     8: rebrokerage
                     9: force kill
                     50: kill by JEDI
                     91: kill user jobs with prod role
               verbose: set True to see what's going on
               srvID: obsolete
               useMailAsID: obsolete
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               the list of clouds (or Nones if tasks are not yet assigned)
        """

        data = {'jediTaskID': jediTaskID, 'code': str(code)}
        return self._post_command('killUnfinishedJobs',data)

    def killJobs(self, ids, code=None, verbose=False, srvID=None, useMailAsID=False, keepUnmerged=False, jobSubStatus=None):
        """Kill jobs. Normal users can kill only their own jobs.
        People with production VOMS role can kill any jobs.
        Running jobs are killed when next heartbeat comes from the pilot.
        Set code=9 if running jobs need to be killed immediately.
           args:
               ids: the list of PandaIDs
               code: specify why the jobs are killed
                     2: expire
                     3: aborted
                     4: expire in waiting
                     7: retry by server
                     8: rebrokerage
                     9: force kill
                     10: fast rebrokerage on overloaded PQs
                     50: kill by JEDI
                     91: kill user jobs with prod role
               verbose: set True to see what's going on
               srvID: obsolete
               useMailAsID: obsolete
               keepUnmerged: set True not to cancel unmerged jobs when pmerge is killed.
               jobSubStatus: set job sub status if any
           returns:
               status code
                     0: communication succeeded to the panda server
                     255: communication failure
               the list of clouds (or Nones if tasks are not yet assigned)
        """
        # serialize
        strIDs = pickle.dumps(ids, protocol=0).decode('utf-8')

        data = {'ids': strIDs, 'code': str(code), 'useMailAsID': useMailAsID}
        killOpts = ''
        if keepUnmerged:
            killOpts += 'keepUnmerged,'
        if jobSubStatus is not None:
            killOpts += 'jobSubStatus={0},'.format(jobSubStatus)
        data['killOpts'] = killOpts[:-1]
        return self._post_command('killJobs',data)

    def setDebugMode(self, job_id, debug_mode):
        data = {"pandaID": job_id, "modeOn": debug_mode}
        return self._post_command('setDebugMode', data)

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