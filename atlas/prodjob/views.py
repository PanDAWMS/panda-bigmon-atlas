import json
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
    print(request.body)
    jlist = [[111],[222]];
    x = json.dumps(jlist);
    return HttpResponse(x)

def get_jobs(request):
    print("TEST")
    jlist = [{"id":3},{"id":4}]
    return HttpResponse(json.dumps(jlist))