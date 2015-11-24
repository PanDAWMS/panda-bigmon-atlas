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





def request_tasks(request):
    #get_tasks(request);
    return render(request, 'taskmon/_task_table.html')


def tasks_action(request):
    """

    :type request: object
    """
    user = request.user.username

    is_superuser = request.user.is_superuser
    #print request.body
    if not is_superuser:
        return HttpResponse('Permission denied')

    return HttpResponse('OK')


def get_task_array():
    task_array = [7090637,7090622,7090621,7090620,7090619]
    return task_array


def get_tasks(request):


    #low_reqid = json.loads(request.body)["low_reqid"]
    #high_reqid = json.loads(request.body)["high_reqid"]
    #reqid = json.loads(request.body)[0];
    #qs = ProductionTask.objects.filter(request__reqid=reqid).values()
    task_array = get_task_array()

    qs = ProductionTask.objects.filter(id__in=task_array).values()
    #qs = ProductionTask.objects.filter(request__reqid__range=(low_reqid,high_reqid)).values()
    #data = serializers.serialize('json', qs)

    def decimal_default(obj):
        if isinstance(obj, Decimal):

            return float(obj)
        if isinstance(obj, datetime):

            return obj.isoformat()

        raise TypeError

    data = json.dumps(list(qs),default = decimal_default)


    #
    #
    return HttpResponse(data)
