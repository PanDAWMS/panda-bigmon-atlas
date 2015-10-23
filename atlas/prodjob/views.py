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
    print request.body;
    return HttpResponse('OK')


def get_jobs(request):
    #    curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861";
    #url = 'http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861';

    url = json.loads(request.body)[0];

    headers = {'content-type': 'application/json', 'accept': 'application/json'};
    resp = requests.get(url, headers=headers)
    data = json.loads(resp.text);

    jlist = [];
    for job in data:
        jlist.append([job['pandaid'], job['jobstatus']])
    return HttpResponse(json.dumps(jlist))
