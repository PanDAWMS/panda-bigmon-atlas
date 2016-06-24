import json

from atlas.prodtask.models import GDPConfig, Cloudconfig

from decimal import Decimal
from datetime import datetime

import logging
# import os

from django.contrib.auth.decorators import login_required

from django.http import HttpResponse
from django.shortcuts import render


_logger = logging.getLogger('prodtaskwebui')


def gdpconfig(request, rid = None):

    return render(request, 'gdpconfig/_config_table.html')


def fairshare(request, rid = None):

    return render(request, 'gdpconfig/_fairshare_table.html')

@login_required(login_url='/gdpconfig/login/')
def config_action(request,action):
    """

    :type request: object
    """
    result = dict(status=None,exception=None)

    if not request.user.user_permissions.filter(name='Can edit GDPConfig').exists():
        result['status']='Failed'
        result['exception'] = 'Permission denied'
        return HttpResponse(json.dumps(result))



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

    if row_type != sRow['type']:
        if row_type == 'int' and sRow['type'] == 'float':
            pass
        elif sRow['type'] == 'str':
            pass
        else:
            result["exception"] = "Wrong type"
            return HttpResponse(json.dumps(result))


    qs = GDPConfig.objects.filter(app=sRow['app'],component=sRow['component'],key=sRow['key'],vo=sRow['vo'])

    _logger.info("GDPConfig: Update user:{user} old data:{old_data}".format(user=request.user.username,
                                                                 old_data=qs.values()))



    try:
        qs.update(value=param)
    except:
        result["exception"] = "Can't update DB"
        return HttpResponse(json.dumps(result))

    _logger.info("GDPConfig: Update user:{user} new data:{old_data}".format(user=request.user.username,
                                                                 old_data=qs.values()))

    return HttpResponse(json.dumps(result))

@login_required(login_url='/gdpconfig/login/')
def fairshare_action(request,action):
    """

    :type request: object
    """
    result = dict(status=None,exception=None)

    if not request.user.user_permissions.filter(name='Can edit GDPConfig').exists():
        result['status']='Failed'
        result['exception'] = 'Permission denied'
        return HttpResponse(json.dumps(result))



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


    qs = Cloudconfig.objects.filter(name=sRow['name'])

    _logger.info("GDPConfig - Fairshare: Update user:{user} old data:{old_data}".format(user=request.user.username,
                                                                 old_data=qs.values()))

    try:
        qs.update(fairshare=param)
    except:
        result["exception"] = "Can't update DB"
        return HttpResponse(json.dumps(result))

    _logger.info("GDPConfig - Fairshare: Update user:{user} new data:{old_data}".format(user=request.user.username,
                                                                 old_data=qs.values()))

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


def get_fairshare(request):
    qs_val = Cloudconfig.objects.all().values('name','fairshare')

    # def decimal_default(obj):
    #     if isinstance(obj, Decimal):
    #
    #         return float(obj)
    #     if isinstance(obj, datetime):
    #
    #         return obj.isoformat()
    #
    #     raise TypeError
    #
    # data= json.dumps(list(qs_val),default = decimal_default)

    data = json.dumps(list(qs_val))

    return HttpResponse(data)