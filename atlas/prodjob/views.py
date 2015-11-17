import json
import requests
# import logging
# import os

from django.http import HttpResponse
from django.shortcuts import render


# _logger = logging.getLogger('prodtaskwebui')





def request_jobs(request):
    return render(request, '_job_table.html')


def jobs_action(request):
    """

    :type request: object
    """
    user = request.user.username

    is_superuser = request.user.is_superuser
    #print request.body
    if not is_superuser:
        return HttpResponse('Permission denied')

    return HttpResponse('OK')


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