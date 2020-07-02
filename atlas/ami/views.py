
from atlas.ami.client import AMIClient
from django.contrib.auth.decorators import login_required

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render


@login_required()
def amitag(request, amitag):
    error_message = ''
    tag = None
    try:
        ami = AMIClient()
        tag = ami.get_ami_tag(amitag)
        sw_containers = []
        if tag['tagType'] != 'sw':
            sw_tags = ami.ami_sw_tag_by_cache(tag['baseRelease'])
        else:
            sw_tags = ami.ami_sw_tag_by_cache(tag['swRelease'])
        for sw_tag in sw_tags:
            if sw_tag['STATE'] == 'USED':
                images = ami.ami_image_by_sw(sw_tag['TAGNAME'])
                for image in images:
                    sw_containers.append((sw_tag['TAGNAME'],image['IMAGENAME'],image['IMAGETYPE']))

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