
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, render_to_response
from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse

from ..settings import defaultDatetimeFormat

import core.datatables as datatables

from .forms import ProductionTaskForm, ProductionTaskCreateCloneForm, ProductionTaskUpdateForm
from .models import ProductionTask, TRequest

from .task_views import ProductionTaskTable

from .task_commands import killTask, changeTaskPriority

from django.db.models import Count, Q


from django.utils.timezone import utc
from datetime import datetime

import time
import json


def tasks_abort(request):
    empty_response = HttpResponse('')

    if request.method == 'POST':
        data_json = request.POST.get('data')
        if not data_json:
            return empty_response
        data = json.loads(data_json)
        tasks = data.get('tasks')
        if not tasks:
            return empty_response

        statuses = {}
        for task in tasks:
            res = killTask(task)
            status = 'error'
            if res and (res[0].startswith("(0,")):
                status = 'ok'
            statuses[task] = { 'status': status, 'reason': res[0]}

        response = { 'statuses': statuses }
        return HttpResponse(json.dumps(response))

    return empty_response


def tasks_change_priority(request):
    empty_response = HttpResponse('')

    if request.method == 'POST':
        data_json = request.POST.get('data')
        if not data_json:
            return empty_response
        data = json.loads(data_json)
        tasks = data.get('tasks')
        priority = data.get('priority')
        if (not tasks) or (priority is None):
            return empty_response

        statuses = {}
        for task in tasks:
            res = changeTaskPriority(task, priority)
            status = 'ok' if (res and (res[0].startswith("(0,"))) else 'failed'
            reason = str(res[0]) if len(res) > 1 else ''
            statuses[task] = { 'status': status, 'reason': res[0]}

        response = { 'statuses': statuses }
        return HttpResponse(json.dumps(response))

    return empty_response



@datatables.datatable(ProductionTaskTable, name='fct')
def task_manage(request):
    qs = request.fct.get_queryset()
    last_task_submit_time = ProductionTask.objects.order_by('-submit_time')[0].submit_time
    return TemplateResponse(request, 'prodtask/_task_manage.html', { 'title': 'Manage Production Tasks',
                                                                    'active_app' : 'prodtask',
                                                                    'table': request.fct,
                                                                    'parent_template': 'prodtask/_index.html',
                                                                    'last_task_submit_time' : last_task_submit_time,
                                                                    })

