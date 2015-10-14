# import json
# import logging
# import os

#from django.http import HttpResponse
from django.shortcuts import render

#_logger = logging.getLogger('prodtaskwebui')





def request_jobs(request):
    if request.method == 'POST':
		return render(request, '_job_table.html')
    else:
        return render(request, '_job_table.html')
