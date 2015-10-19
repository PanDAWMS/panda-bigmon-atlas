# import json
# import logging
# import os

#from django.http import HttpResponse
from django.shortcuts import render

#_logger = logging.getLogger('prodtaskwebui')

jlist = [{"id":1},{"id":2}]



def request_jobs(request):
    if request.method == 'POST':
		return render(request, '_job_table.html', {'jlist':jlist})
    else:
        return render(request, '_job_table.html', {'jlist':jlist})
