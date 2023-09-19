from rest_framework.authentication import SessionAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated

from atlas.ami.client import AMIClient
from django.contrib.auth.decorators import login_required
import logging


from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response

from atlas.settings import OIDC_LOGIN_URL

_logger = logging.getLogger('prodtaskwebui')


@login_required(login_url=OIDC_LOGIN_URL)
def amitag(request, amitag):
    error_message = ''
    tag = None
    sw_containers = []
    try:
        ami = AMIClient()
        tag = ami.get_ami_tag(amitag)
        if tag['tagType'] != 'sw':
            sw_containers = sw_by_amitag(ami,tag['baseRelease'])
        else:
            sw_containers = sw_by_amitag(ami,tag['swRelease'])
    except Exception as e:
        error_message = str(e)
    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Ami tag %s' % amitag,
        'ami_tag_dict': tag,
        'amitag' : amitag,
        'error': error_message,
        'parent_template' : 'prodtask/_index.html',
        'sw_containers':sw_containers,
        }

    return render(request, 'ami/ami_tag.html', request_parameters)



def sw_by_amitag(ami, amitag):
    sw_containers = []
    sw_tags = ami.ami_sw_tag_by_cache(amitag)
    for sw_tag in sw_tags:
        if sw_tag['STATE'] == 'USED':
            images = ami.ami_image_by_sw(sw_tag['TAGNAME'])
            for image in images:
                sw_containers.append({'container_name':image['IMAGENAME'],
                                      'cmtconfig':sw_tag['IMAGEARCH'] + '-' + sw_tag['IMAGEPLATFORM'] + '-' + sw_tag['IMAGECOMPILER'],
                                      'tagname':sw_tag['TAGNAME']})
    return sw_containers


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def sw_containers_by_amitag(request,amitag):
    try:
        ami = AMIClient()
        tag = ami.get_ami_tag(amitag)
        result = sw_by_amitag(ami,tag['baseRelease'])

    except Exception as e:
        _logger.error("Problem with pattern cloning %s" % str(e))
        return Response(str(e), status=400)
    return Response(result)