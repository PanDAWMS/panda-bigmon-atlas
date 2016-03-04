import json
import requests
# import logging
# import os

from django.http import HttpResponse
from django.shortcuts import render

#import atlas.deftcore.api.client as deft

#from atlas.prodtask.task_actions import _do_deft_action

# _logger = logging.getLogger('prodtaskwebui')





def request_jobs(request):
    return render(request, 'prodjob/_job_table.html')


def jobs_action(request,action):
    """

    :type request: object
    """

    user = request.user.username

    is_superuser = request.user.is_superuser
    jobs= json.loads(request.body);

    #

#    result = dict(owner=owner, task=task_id, action=action, args=args,
#                  status=None, accepted=False, registered=False,
#                  exception=None, exception_source=None)

    result = dict(status=None,exception=None)

    #print action;
    #for job in jobs:
    #    print job['pandaid'];
    #print jobs
    if not is_superuser:
        result['exception'] = "Permission denied"
        return HttpResponse(json.dumps(result))
        #return HttpResponse('Permission denied')


    #if action == 'decrease_priority':
    #    result.update(decrease_task_priority(owner, task_id, *args))
    #if action in _deft_actions:
    #    result.update(_do_deft_action(owner, task_id, action, *args))

    #do actions here

    #for job in jobs:
        #kill_job(self, owner, task_id, job_id)
        #_do_deft_action(owner, task_id, action, *args)


    #return HttpResponse('OK')
    #result['status']="OK"

    #return json.dumps(result)
    result['exception'] = "Under development"
    return HttpResponse(json.dumps(result))


def get_jobs(request):
    #    curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861";
    #url = 'http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861';

    url = json.loads(request.body)[0];

    headers = {'content-type': 'application/json', 'accept': 'application/json'};
    resp = requests.get(url, headers=headers)
    #data = json.loads(resp.text);
    data = resp.json()['jobs'];
    #print data;
    #jdict = {data};
    #jlist = [];
    #for job in data:
    #    jlist.append([ '', job['pandaid'], job['attemptnr'], job['produsername'], job["reqid"],
    #                  job['taskid'], job['transformation'], job['jobstatus']])

    #return HttpResponse(json.dumps(jlist))
    return HttpResponse(json.dumps(data))


# def _do_deft_job_action(owner, job_id, task_id, action, *args):
#     """
#     Perform task action using DEFT API
#     :param owner: username form which task action will be performed
#     :param task_id: task ID
#     :param action: action name
#     :param args: additional arguments for the action (if needed)
#     :return: dictionary with action execution details
#     """
#
#     result = dict(owner=owner, task=task_id, action=action, args=args,
#                   status=None, accepted=False, registered=False,
#                   exception=None, exception_source=None)
#
#     if not action in _deft_actions:
#         result['exception'] = "Action '%s' is not supported" % action
#         return result
#
#     try:
#         func = getattr(_deft_client, _deft_actions[action])
#     except AttributeError as e:
#         result.update(exception=str(e))
#         return result
#
#     try:
#         request_id = func(owner, task_id, *args)
#     except Exception as e:
#         result.update(exception=str(e),
#                       exception_source=_deft_client.__class__.__name__)
#         return result
#
#     result['accepted'] = True
#
#     try:
#         status = _deft_client.get_status(request_id)
#     except Exception as e:
#         result.update(exception=str(e),
#                       exception_source=_deft_client.__class__.__name__)
#         return result
#
#     result.update(registered=True, status=status)
#
#     return result