import json
import requests
import logging
# import os
import re

from django.http import HttpResponse
from django.shortcuts import render
from django.conf import settings
import atlas.deftcore.api.client as deft


_logger = logging.getLogger('prodtaskwebui')

_deft_client = deft.Client(settings.DEFT_AUTH_USER, settings.DEFT_AUTH_KEY)

_deft_job_actions = {

    'kill_jobs': 'kill_job',
    'set_debug_jobs': 'set_job_debug_mode',
    'reassign_jobs': 'reassign_jobs',

}

def request_jobs(request):
    params_for_bigpanda = ''
    request_path = request.META['QUERY_STRING']
    if request_path:
        params_for_bigpanda = "http://bigpanda.cern.ch/jobs/?" + request_path
    return render(request, 'prodjob/_job_table.html',{ 'params_for_bigpanda':  params_for_bigpanda  })


def jobs_action(request,action):
    """

    :type request: object
    """

    user = request.user.username

    is_superuser = request.user.is_superuser
    data = json.loads(request.body)
    jobs= data.get('jobs')
    args= data.get('parameters', [])

    fin_res=[]

    result = dict(owner=user, job=None, task=None, action=action, args=args,
                  status=None, accepted=False, registered=False,
                  exception=None, exception_source=None)

    if not is_superuser:
        result['exception'] = "Permission denied"
        return HttpResponse(json.dumps(result))

    #do actions here
    for job in jobs:
        result.update(_do_deft_job_action(user, job['taskid'], job['pandaid'], action, *args))
        fin_res.append(result)

    return HttpResponse(json.dumps(fin_res))


def get_jobs(request):

    result = ''
    try:
        url = json.loads(request.body)[0]
        _logger.info("Get jobs from bigpanda for: %s" % url)
        url=re.sub('&display_limit.*(\d+)','',url)
        url = url.replace('https','http')
        if 'json' not in url:
            if url[-1]=='&':
                url += '&'
            else:
                url += '&json'

        headers = {'content-type': 'application/json', 'accept': 'application/json'}
        resp = requests.get(url, headers=headers)
        data = resp.json()['jobs']
        result = json.dumps(data)
        del resp
    except Exception, e:
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