import json
import requests
# import logging
# import os

from django.http import HttpResponse
from django.shortcuts import render

#_logger = logging.getLogger('prodtaskwebui')





def request_jobs(request):
    jlist = [{"id":1},{"id":2}]
    if request.method == 'POST':
		return render(request, '_job_table.html', {'jlist':jlist})
    else:
        return render(request, '_job_table.html', {'jlist':jlist})

def jobs_action(request):
#    print(request.body);
#    curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861";
    url = 'http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861';
    headers = {'content-type': 'application/json','accept': 'application/json'};
    resp = requests.get(url, headers=headers)
    data = json.loads(resp.text);
    print data;
    jlist = [];
    for job in data:
        jlist.append([job['pandaid'],job['jobstatus']]);



    return HttpResponse(json.dumps(jlist))

def get_jobs(request):
    print("TEST")
    jlist = [{"id":3},{"id":4}]
    return HttpResponse(json.dumps(jlist))