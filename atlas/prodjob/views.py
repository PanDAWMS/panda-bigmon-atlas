# import json
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
    return render(request, '_job_table.html', {'jlist':[{"id":1},{"id":4}]})