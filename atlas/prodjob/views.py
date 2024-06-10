import json
import requests
import logging
# import os
import re

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import render
from django.conf import settings
import atlas.deftcore.api.client as deft
from atlas.settings import OIDC_LOGIN_URL
from atlas.task_action.task_management import TaskManagementAuthorisation, TaskActionExecutor, do_jedi_action

_logger = logging.getLogger('prodtaskwebui')

_deft_client = deft.Client(auth_user=settings.DEFT_AUTH_USER, auth_key=settings.DEFT_AUTH_KEY,base_url=settings.BASE_DEFT_API_URL)

_deft_job_actions = {

    'kill_jobs': 'kill_job',
    'kill_job_by_task': 'kill_jobs',
    'set_debug_jobs': 'set_job_debug_mode',
    'reassign_jobs': 'reassign_jobs',

}
@login_required(login_url=OIDC_LOGIN_URL)
def request_jobs(request):
    params_for_bigpanda = ''
    request_path = request.META['QUERY_STRING']
    if request_path:
        params_for_bigpanda = "http://bigpanda.cern.ch/jobs/?" + request_path
    return render(request, 'prodjob/_job_table.html',{ 'params_for_bigpanda':  params_for_bigpanda  })


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
@login_required(login_url=OIDC_LOGIN_URL)
def jobs_action(request,action):
    """

    :type request: object
    """
    result = {}
    try:

        user = request.user.username

        is_superuser = request.user.is_superuser
        data = json.loads(request.body)
        jobs= data.get('jobs')
        args= data.get('parameters', [])

        fin_res=[]

        result = dict(owner=user, job=None, task=None, action=action, args=args,
                      status=None, accepted=False, registered=False,
                      exception=None, exception_source=None)


    except Exception as e:
        _logger.error("Problem during jobs action :%s" % str(e))
        result['exception'] = "Error"
        return HttpResponse(json.dumps(result))

    #do actions here
    try:
        if action == 'kill_jobs':

            for chunk in chunks(jobs,400):
                tasks = {}
                without_taskid = []
                for job in chunk:

                    if job.get('taskid'):
                        tasks[int(job['taskid'])] = tasks.get(int(job['taskid']),[])+[job['pandaid']]
                    else:
                        without_taskid.append(job['pandaid'])
                _logger.info("Send DEFT job kills for %s jobs for %s tasks" % (str(len(chunk)),str(len(tasks.keys()))))
                if without_taskid and not is_superuser:
                    result['exception'] = "Permission denied"
                    return HttpResponse(json.dumps(result))
                authentification_management = TaskManagementAuthorisation()
                executor = TaskActionExecutor(user, '')
                tasks_done = set()
                tasks_with_problems = set()
                for task, jobs in tasks.items():
                    user_allowed, action_allowed = authentification_management.task_action_authorisation(task, user, 'kill_job',
                                                                                            args)
                    if not user_allowed or not action_allowed:
                        tasks_with_problems.add(task)
                        continue
                    do_jedi_action(executor, task, 'kill_job', jobs, *args)
                    fin_res.append(result)
                    tasks_done.add(task)
                if without_taskid:
                    do_jedi_action(executor, without_taskid, 'kill_jobs_without_task', *args)
                    fin_res.append(result)
                if len(list(tasks_with_problems))>0:
                    result['exception'] = f"Action done for {len(list(tasks_done))} tasks, problem for {len(list(tasks_with_problems))} tasks"
                    return HttpResponse(json.dumps(result))
        elif action == 'set_debug_jobs':
            if len(jobs) > 10:
                result['exception'] = "Too many jobs to set debug mode"
                return HttpResponse(json.dumps(result))
            if not is_superuser:
                result['exception'] = "Permission denied"
                return HttpResponse(json.dumps(result))
            for job in jobs:
                if not job.get('taskid'):
                    result['exception'] = "Taskid is missing"
                    return HttpResponse(json.dumps(result))
            executor = TaskActionExecutor(user, '')
            for job in jobs:
                do_jedi_action(executor, job['taskid'], action, job['pandaid'],  *args)
                fin_res.append(result)
    except Exception as e:
        _logger.error("Problem during jobs action :%s" % str(e))
        result['exception'] = "Error"
        return HttpResponse(json.dumps(result))

    return HttpResponse(json.dumps(fin_res))


@login_required(login_url=OIDC_LOGIN_URL)
def get_jobs(request):

    result = ''
    try:
        url = json.loads(request.body)[0]
        _logger.info("Get jobs from bigpanda for: %s" % url)
        url=re.sub('&display_limit.*(\d+)','',url)
        url = url.replace('https','http')
        url = url.replace('jobsss', 'jobs')
        url = url.strip()
        if 'json' not in url:
            if url[-1]=='&':
                url += 'json'
            else:
                url += '&json'

        headers = {'content-type': 'application/json', 'accept': 'application/json'}
        resp = requests.get(url, headers=headers)
        data = resp.json()['jobs']
        result = json.dumps(data)
        del resp
    except Exception as e:
        _logger.error("Problem during reading job info from bigpanda:%s" % str(e))
    return HttpResponse(result)


def get_jobs_from_url(url):
    url=re.sub('&display_limit.*(\d+)','',url)
    url = url.replace('https','http')
    if 'json' not in url:
        if url[-1]=='&':
            url += '&'
        else:
            url += '&json'

    headers = {'content-type': 'application/json', 'accept': 'application/json'};
    resp = requests.get(url, headers=headers)
    data = resp.json()['jobs']
    return data

def get_job_from_id(id):
    url = "https://bigpanda.cern.ch/job?pandaid=%s"%(str(id))
    url = url.replace('https','http')
    if 'json' not in url:
        if url[-1]=='&':
            url += '&'
        else:
            url += '&json'

    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    resp = requests.get(url, headers=headers)
    data = resp.json()
    return data


def get_outputs_for_jobs(panda_ids):
    result = {}
    for id in panda_ids:
        result[id] = []
        job=get_job_from_id(id)
        for file in job['files']:
            result[id].append(file['lfn'])
    return result

def _do_deft_job_action(owner, task_id, job_id, action, *args):
    """
    Perform task action using DEFT API
    :param owner: username form which task action will be performed
    :param task_id: task ID
    :param action: action name
    :param args: additional arguments for the action (if needed)
    :return: dictionary with action execution details
    """

    result = dict(owner=owner, job=job_id, task=task_id, action=action, args=args,
                  status=None, accepted=False, registered=False,
                  exception=None, exception_source=None)

    try:

        func = getattr(_deft_client, _deft_job_actions[action])
    except AttributeError as e:
        result.update(exception=str(e))
        return result

    try:
        request_id = func(owner, task_id, job_id, *args)
    except Exception as e:
        result.update(exception=str(e),
                      exception_source=_deft_client.__class__.__name__)
        return result

    result['accepted'] = True

    try:
        status = _deft_client.get_status(request_id)
    except Exception as e:
        result.update(exception=str(e),
                      exception_source=_deft_client.__class__.__name__)
        return result

    result.update(registered=True, status=status)

    return result