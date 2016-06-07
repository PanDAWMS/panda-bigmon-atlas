import json
import requests
from django.core import serializers
from atlas.prodtask.models import GDPConfig

# import logging
# import os
from atlas.prodtask.task_views import get_clouds, get_sites, get_nucleus

from decimal import Decimal
from datetime import datetime

from django.http import HttpResponse
from django.shortcuts import render


# _logger = logging.getLogger('prodtaskwebui')


def gdpconfig(request, rid = None):


    return render(request, 'gdpconfig/_config_table.html')


def config_action(request,action):
    """

    :type request: object
    """
    result = dict(status=None,exception=None)

    user = request.user.username

    #print user,action

    is_superuser = request.user.is_superuser
    #print request.body
    if not is_superuser:
        result['status']='Failed'
        result['exception'] = 'Permission denied'
        return HttpResponse(json.dumps(result))
        #return HttpResponse('Permission denied')


    data_json = request.body

    if not data_json:
        result["exception"] = "Request data is empty"
        return HttpResponse(json.dumps(result))

    data = json.loads(data_json)

    sRow = data.get("srow")
    if not sRow:
        result["exception"] = "Row is empty"
        return HttpResponse(json.dumps(result))

    param = data.get("parameters")


    row_type = str_to_type(param).__name__

    #print row_type, sRow['type']

    if row_type != sRow['type']:
        result["exception"] = "Wrong type"
        return HttpResponse(json.dumps(result))
    #print sRow, params


    result['status'] = 'OK'
    #return HttpResponse('OK')
    return HttpResponse(json.dumps(result))

def str_to_type (s):
    """ Get possible cast type for a string

    Parameters
    ----------
    s : unicode string

    Returns
    -------
    float,int,str,bool : type
        Depending on what it can be cast to

    """
    try:
        f = float(s)
        if "." not in s:
            return int
        return float
    except ValueError:
        value = s.upper()
        if value == "TRUE" or value == "FALSE":
            return bool
#        return type(s)
        return str

def get_config(request):
    qs_val = GDPConfig.objects.all().values()
    data = json.dumps(list(qs_val))

    return HttpResponse(data)

