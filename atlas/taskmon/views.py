import json
import requests
from django.core import serializers
from atlas.prodtask.models import ProductionTask
# import logging
# import os
import simplejson
from decimal import Decimal
from datetime import datetime

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


def get_tasks(request):
    qs = ProductionTask.objects.filter(request__reqid=4296).values()

    #data = serializers.serialize('json', qs)

    def decimal_default(obj):
        if isinstance(obj, Decimal):

            return float(obj)
        if isinstance(obj, datetime):

            return obj.isoformat()

        raise TypeError

    data = json.dumps(list(qs),default = decimal_default)
    print "Here"

    #
    #return HttpResponse(data)
    return HttpResponse(json.dumps(data))
    #    curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861";
    #url = 'http://bigpanda.cern.ch/jobs/?pandaid=2646731860,2646731861';

    #url = json.loads(request.body)[0];

    #headers = {'content-type': 'application/json', 'accept': 'application/json'};
    #resp = requests.get(url, headers=headers)

    #data = resp.json()['jobs'];

    #return HttpResponse(json.dumps(data))