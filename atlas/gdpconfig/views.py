import json

from rest_framework.authentication import SessionAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated

from atlas.prodtask.models import GDPConfig, GlobalShare

from decimal import Decimal
from datetime import datetime
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
import logging
# import os

from django.contrib.auth.decorators import login_required

from django.http import HttpResponse
from django.shortcuts import render
from functools import reduce

from atlas.settings import OIDC_LOGIN_URL

_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')

@login_required(login_url=OIDC_LOGIN_URL)
def gdpconfig(request, rid = None):

    return render(request, 'gdpconfig/_config_table.html')




@login_required(login_url=OIDC_LOGIN_URL)
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
                                                                 old_data=list(qs.values())))



    try:
        qs.update(value=param)
    except:
        result["exception"] = "Can't update DB"
        return HttpResponse(json.dumps(result))

    _logger.info("GDPConfig: Update user:{user} new data:{old_data}".format(user=request.user.username,
                                                                 old_data=list(qs.values())))
    _jsonLogger.info("GDPConfig: Update user:{user} new data:{old_data}".format(user=request.user.username,
                                                                 old_data=list(qs.values())))

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

@login_required(login_url=OIDC_LOGIN_URL)
def get_config(request):
    qs_val = list(GDPConfig.objects.all().values())
    data = json.dumps(list(qs_val))

    return HttpResponse(data)




@login_required(login_url=OIDC_LOGIN_URL)
def global_share(request):
    if request.method == 'GET':
        return render(request, 'gdpconfig/_global_share.html', {
                'active_app': 'gdpconfig',
                'pre_form_text': 'Global share',

                'parent_template': 'prodtask/_index.html',

            })

@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def global_share_tree(request):
    try:
        all_global_share = list(GlobalShare.objects.all().values())
        tree = []
        current_parents = ['']
        rest_shares = all_global_share
        percentages = {'root':100}

        while rest_shares:
            new_rest_shares = []
            layer = []
            for share in rest_shares:
                if share['parent'] in current_parents:
                    parent = 'root'
                    if share['parent']:
                        parent = share['parent']
                    layer.append({'name':share['name'],'parent':parent,'value':int(share['value'])})
                else:
                   new_rest_shares.append(share)
            #layer.sort(key=lambda x: (x['parent'],x['name']))
            tree.append(layer)
            current_parents = [x['name'] for x in layer]
            if (len(new_rest_shares)==len(rest_shares)):
                print(rest_shares)
                break
            rest_shares = new_rest_shares
        levels = len(tree)
        element_number = {'root':(reduce(lambda x,y:x*100,tree,1))}
        sorted_tree = []
        for level_number,layer in enumerate(tree):
            sorted_layer = []
            current_number = 1
            for element in layer:
                element['order_number'] = element_number[element['parent']] + reduce(lambda x,y:x*100,list(range(len(tree)-level_number-1)),current_number)
                current_number += 1
                element_number[element['name']] = element['order_number']
                sorted_layer.append(element)
            sorted_layer.sort(key=lambda x: (x['order_number'], x['name']))
            sorted_tree.append(sorted_layer)
        tree=sorted_tree
        table_to_show = []
        elements_left = len(all_global_share)
        current_counter = [0 for x in range(levels)]
        current_row = 0
        current_parent = ['root']
        while elements_left>0:
            current_objects_row = []
            if current_counter[current_row] == len(tree[current_row]):
                current_parent.pop()
                current_row -= 1
            else:
                if tree[current_row][current_counter[current_row]]['parent'] == current_parent[-1]:
                    for i in range(current_row):
                        current_objects_row.append({'id':str(len(table_to_show))+'_'+str(i),'show':False, 'name':' ',
                                                    'value':0})
                    current_object = tree[current_row][current_counter[current_row]]
                    current_object['show'] = True
                    current_object['original_value'] = current_object['value']
                    #current_object['percentage'] = float(percentages[current_object['parent']]*current_object['value']) / 100
                    #percentages[current_object['name']] = current_object['percentage']
                    current_object['id'] = current_object['name']
                    current_objects_row.append(current_object)
                    for i in range(current_row+1,levels):
                        current_objects_row.append({'id':str(len(table_to_show))+'_'+str(i),'show':False, 'name':' ',
                                                    'value':0})
                    elements_left -= 1
                    table_to_show.append(current_objects_row)
                    current_counter[current_row] += 1
                    if current_row<(levels-1):
                        current_row += 1
                        current_parent.append(current_object['name'])
                else:
                    current_parent.pop()
                    current_row -= 1
        # for row in table_to_show:
        #     for element in row:
        #         print element['name'],':',element['percentage'],element['value'],'-',
        #     print ''
        content = table_to_show
    except Exception as e:
        return HttpResponse(str(e), content_type='application/json',status=500)
    return Response(content)


@api_view(['POST'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def global_share_change(request):
    error_message = []
    try:
       data = request.data
       old_shares = {}
       for share in data:
           old_share = GlobalShare.objects.get(name=share)
           old_shares[old_share.name] = int(old_share.value)
       user = request.user
       if not user.user_permissions.filter(name='Can edit GDPConfig').exists():
           raise LookupError('User %s has no permission for global share change '% user.username)
       else:
            log_str = ''
            for share in data:
                log_str += share + ' from %i'%old_shares[share]+' to %i'%data[share]+'; '
                GlobalShare.objects.filter(name=share).update(value=data[share])
            _logger.info("GDPConfig - global share: Update user:{user} data:{log_str}".format(user=user.username,
                                                             log_str=log_str))
            _jsonLogger.info("Global share: Update user:{user} data:{log_str}".format(user=user.username,log_str=log_str))

    except Exception as e:
        error_message.append(str(e))
        _logger.error("GDPConfig - global share: %s"%error_message)
    if not error_message:
        content = {'result': 'OK'}
    else:
        content = {
                   'result': 'FAILED',
                   'exception': '; '.join(error_message)
                   }

    return Response(content)


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def get_json_param(request):
    try:
        field = request.query_params.get('key')
        if field:
            gdp_param = GDPConfig.objects.get(key=field)
            if gdp_param.type != 'json':
                raise Exception('The parameter is not json')
            else:
                return Response(gdp_param.value_json)
        raise Exception('No field specified')
    except Exception as e:
        return HttpResponse(str(e), content_type='application/json',status=500)

@api_view(['POST'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def save_json_param(request):
    try:
        user = request.user
        if not user.user_permissions.filter(name='Can edit GDPConfig').exists():
           return HttpResponse('User %s has no permission for global share change '% user.username, content_type='application/json',status=403)
        field = request.data['key']
        if field:
            gdp_param = GDPConfig.objects.get(key=field)
            new_json = request.data['value']
            if gdp_param.type != 'json':
                raise Exception('The parameter is not json')
            else:
                old_data = json.dumps(gdp_param.value_json)

                gdp_param.value_json = new_json
                gdp_param.save()
                _logger.info(f"GDPConfig: Update user:{user.username} json field {field} ")
                _jsonLogger.info(f"GDPConfig: Update user:{user} json field {field}",extra={'user':user.username,
                                                                                            'key':field,
                                                                                            'old_data':old_data,
                                                                                            'new_data':json.dumps(new_json)})
                return Response(new_json)

        raise Exception('No field specified')
    except Exception as e:
        _logger.error(f"GDPConfig: {str(e)}")
        return HttpResponse(str(e), content_type='application/json',status=500)