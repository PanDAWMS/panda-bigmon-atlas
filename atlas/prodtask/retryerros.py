import copy
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.urls import reverse
from django.forms import model_to_dict
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.template.response import TemplateResponse
from django.db.models import Count
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.decorators import login_required
from django.db import transaction
import json
import logging
from django.db.models import Q
from atlas.prodtask.models import RetryAction

from ..prodtask.helper import form_request_log

_logger = logging.getLogger('prodtaskwebui')

from .forms import RetryErrorsForm
from .models import RetryErrors, JediWorkQueue


@login_required(login_url='/prodtask/login/')
def retry_errors_list(request):
    if request.method == 'GET':
        retry_errors = list(RetryErrors.objects.all().order_by('id').values())
        for retry_error in retry_errors:
            try:
                if retry_error['work_queue']:
                    working_queue = JediWorkQueue.objects.get(id=retry_error['work_queue'])
                    retry_error['work_queue'] = str(working_queue)
                if retry_error['active'] == 'Y':
                    retry_error['is_active'] = 'Yes'
                else:
                    retry_error['is_active'] = 'No'
                retry_error['retry_action'] = str(RetryAction.objects.get(id=retry_error['retry_action_id']))
            except:
             pass
        return render(request, 'prodtask/_error_retry_table.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Retry errors',
                'title': 'Retry errors management',
                'retry_errors': retry_errors,
                'submit_url': 'prodtask:retry_errors_list',
                'parent_template': 'prodtask/_index.html',
            })

@login_required(login_url='/prodtask/login/')
def retry_errors_edit(request, retry_errors_id):
    if request.method == 'POST':
        if (request.user.is_superuser) or (request.user.username in ['fbarreir']):
            try:
                values = RetryErrors.objects.get(id=retry_errors_id)
                form = RetryErrorsForm(request.POST,instance=values)
                if form.is_valid():
                    # Process the data in form.cleaned_data
                    _logger.info("update user:{user} old data:{old_data}".format(user=request.user.username,
                                                                                             old_data=model_to_dict(values)))
                    retry_errors = RetryErrors(**form.cleaned_data)
                    retry_errors.save()
                    _logger.info("update user:{user} new data:{old_data}".format(user=request.user.username,
                                                                                             old_data=model_to_dict(retry_errors)))
                    return HttpResponseRedirect('/prodtask/retry_errors_list')
            except:
                return HttpResponseRedirect('/prodtask/retry_errors_list')
        else:
            return HttpResponseRedirect('/prodtask/retry_errors_list')
    else:
        try:
            if (request.user.is_superuser) or (request.user.username in ['fbarreir']):
                values = RetryErrors.objects.get(id=retry_errors_id)
                form = RetryErrorsForm(instance=values)
            else:
                return HttpResponseRedirect('/prodtask/retry_errors_list')
        except:
            return HttpResponseRedirect('/prodtask/retry_errors_list')



    return render(request, 'prodtask/_train_form.html', {
        'active_app' : 'prodtask',
        'pre_form_text' : 'Edit retry error entity # %i' % int(retry_errors_id),
        'form': form,
        'submit_url': 'prodtask:retry_errors_edit',
        'url_args'  : retry_errors_id,
        'parent_template' : 'prodtask/_index.html',
        })

@login_required(login_url='/prodtask/login/')
def retry_errors_clone(request, retry_errors_id):
    return retry_errors_clone_create(request, retry_errors_id,'prodtask:retry_errors_clone')

@login_required(login_url='/prodtask/login/')
def retry_errors_create(request):
    return retry_errors_clone_create(request, None,'prodtask:retry_errors_create')


def retry_errors_clone_create(request, retry_errors_id,submit_url):
    if retry_errors_id:
         message = 'Clone retry error entity: %i' % int(retry_errors_id)
    else:
        message = 'Create new retry error entity'
    if request.method == 'POST':
        form = RetryErrorsForm(request.POST)
        if form.is_valid():
            # Process the data in form.cleaned_data
            try:
                retry_errors = RetryErrors(**form.cleaned_data)
                retry_errors.id = None
                retry_errors.save()
                _logger.info("created user:{user} data:{old_data}".format(user=request.user.username,
                                                                             old_data=model_to_dict(retry_errors)))
            except Exception as e:
                _logger.error("Problem during error save: %s" % str(e))

            return HttpResponseRedirect('/prodtask/retry_errors_list')
    else:
        try:
            if (request.user.is_superuser) or (request.user.username in ['fbarreir']):
                if retry_errors_id:
                    values = RetryErrors.objects.get(id=retry_errors_id)
                    values.id=None
                    form = RetryErrorsForm(instance=values)
                else:
                    form = RetryErrorsForm()
            else:
                return HttpResponseRedirect('/prodtask/retry_errors_list')

        except:
            return HttpResponseRedirect('/prodtask/retry_errors_list')

    return render(request, 'prodtask/_train_form.html', {
        'active_app' : 'prodtask',
        'pre_form_text' : message,
        'form': form,
        'submit_url': submit_url,
        'url_args'  : retry_errors_id,
        'parent_template' : 'prodtask/_index.html',
        })

@login_required(login_url='/prodtask/login/')
def retry_errors_delete(request, retry_errors_id):
    if request.method == 'GET':
        if (request.user.is_superuser) or (request.user.username in ['fbarreir']):

            try:
                values = RetryErrors.objects.get(id=retry_errors_id)
                _logger.info("update user:{user} delete data:{old_data}".format(user=request.user.username,
                                                                             old_data=model_to_dict(values)))
                values.delete()

            except:
                pass

        return HttpResponseRedirect('/prodtask/retry_errors_list')