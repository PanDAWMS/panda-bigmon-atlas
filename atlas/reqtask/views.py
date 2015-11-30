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

    return render(request, 'reqtask/_task_table.html')


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


def get_task_array(request):

    task_array=request.session['selected_tasks']
    return task_array


def get_tasks(request):

    task_array = get_task_array(request)

    qs = ProductionTask.objects.filter(id__in=task_array).values()

    def decimal_default(obj):
        if isinstance(obj, Decimal):

            return float(obj)
        if isinstance(obj, datetime):

            return obj.isoformat()

        raise TypeError

    data = json.dumps(list(qs),default = decimal_default)

    return HttpResponse(data)
