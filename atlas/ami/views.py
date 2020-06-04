
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
    except Exception as e:
        error_message = str(e)
    request_parameters = {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Ami tag %s' % amitag,
        'ami_tag_dict': tag,
        'amitag' : amitag,
        'error': error_message,
        'parent_template' : 'prodtask/_index.html',
        }

    return render(request, 'ami/ami_tag.html', request_parameters)