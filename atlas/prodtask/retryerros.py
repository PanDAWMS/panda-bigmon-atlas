import copy
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.core.urlresolvers import reverse
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

from ..prodtask.helper import form_request_log


from .forms import RetryErrorsForm
from .models import RetryErrors


def retry_errors_list(request):
    if request.method == 'GET':
        retry_errors = RetryErrors.objects.all()
        return render(request, 'prodtask/_error_retry_table.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Retry errors',
                'retry_errors': retry_errors,
                'submit_url': 'prodtask:retry_errors_list',
                'parent_template': 'prodtask/_index.html',
            })


def retry_errors_edit(request, retry_errors_id):
    if request.method == 'POST':
        try:
            values = RetryErrors.objects.get(id=retry_errors_id)
            form = RetryErrorsForm(request.POST,instance=values)
            if form.is_valid():
                # Process the data in form.cleaned_data
                retry_errors = RetryErrors(**form.cleaned_data)
                retry_errors.save()
                return HttpResponseRedirect('/prodtask/retry_errors_list')
        except:
            return HttpResponseRedirect('/prodtask/retry_errors_list')
    else:
        try:
            values = RetryErrors.objects.get(id=retry_errors_id)
            form = RetryErrorsForm(instance=values)
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


def retry_errors_clone(request, retry_errors_id):
    return retry_errors_clone_create(request, retry_errors_id,'prodtask:retry_errors_clone')


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
            retry_errors = RetryErrors(**form.cleaned_data)
            retry_errors.id = None
            retry_errors.save()
            return HttpResponseRedirect('/prodtask/retry_errors_list')
    else:
        try:
            if retry_errors_id:
                values = RetryErrors.objects.get(id=retry_errors_id)
                values.id=None
                form = RetryErrorsForm(instance=values)
            else:
                form = RetryErrorsForm()

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