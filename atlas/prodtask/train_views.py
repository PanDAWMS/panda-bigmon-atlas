import copy
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.core.urlresolvers import reverse
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
from rest_framework import serializers,generics
from django.forms.models import model_to_dict
from ..prodtask.forms import ProductionTrainForm
from ..prodtask.models import TrainProductionLoad,TrainProduction,TRequest

_logger = logging.getLogger('prodtaskwebui')


def prepare_train_carriages(train_id):
    """
    distribute train loads by carriages
    :param train_id: id of the train
    :return: dict {skim: [{dataset:{[outputs]:AOD,loads_id:[load_id]}}],noskim...}
    """

    loads = list(TrainProductionLoad.objects.filter(train=train_id))
    train_carriages = {'skim':{},'noskim':{}}
    for load in loads:
        skim = 'skim'
        if load.skim != 'skim':
            skim = 'noskim'
        datasets = load.datasets.split('\n')
        outputs = load.output_formats.split('.')
        for dataset in datasets:
            if dataset in train_carriages[skim]:
                train_carriages[skim][dataset]['outputs'] += [x for x in outputs if x not in train_carriages[skim][dataset]['outputs']]
                train_carriages[skim][dataset]['loads_id'].append(load.id)
                train_carriages[skim][dataset]['groups'].add(load.group)
            else:
                group = set()
                group.add(load.group)
                train_carriages[skim][dataset] = {'outputs':outputs,'loads_id':[load.id], 'groups': group}
    return train_carriages


def assembled_train(request,train_id):
    if request.method == 'GET':
        try:
            train_carriage = prepare_train_carriages(train_id)
            results = []
            carriage_number = 0
            for x in ['skim','noskim']:
                for dataset in sorted(train_carriage[x].keys()):
                    results.append({'dataset':dataset,'carriage_number':carriage_number,'skim':x,
                                       'group':list(train_carriage[x][dataset]['groups'])})
                    carriage_number += 1
            return HttpResponse(json.dumps(results), content_type='application/json')
        except Exception,e:
            _logger.error("Problem with carriage forming  %s" % e)


def trains_list(request):
    if request.method == 'GET':
        trains = TrainProduction.objects.all().order_by('departure_time')
        return render(request, 'prodtask/_trains_list.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Trains',
                'trains': [model_to_dict(x) for x in trains],
                'submit_url': 'prodtask:trains_list',
                'parent_template': 'prodtask/_index.html',
            })


class TrainCarriageSerializer(serializers.ModelSerializer):

    class Meta:
        model = TrainProductionLoad
        fields = ('id', 'group', 'train', 'datasets', 'output_formats', 'manager', 'skim')
        read_only_fields = ('id',)


class TrainLoads(generics.ListCreateAPIView):
    queryset = TrainProductionLoad.objects.all()
    serializer_class = TrainCarriageSerializer


class TrainLoad(generics.RetrieveUpdateDestroyAPIView):
    queryset = TrainProductionLoad.objects.all()
    serializer_class = TrainCarriageSerializer

@login_required(login_url='/prodtask/login/')
def train_create(request):
    if request.method == 'POST':
        try:
            form = ProductionTrainForm(request.POST)  # A form bound to the POST data
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
        if form.is_valid():
            # Process the data in form.cleaned_data

            _logger.debug('Create train: %')
            try:

                train = TrainProduction(**form.cleaned_data)
                train.status = 'loading'
                train.save()
                return HttpResponseRedirect(reverse('prodtask:train_edit', args=(train.id,)))  # Redirect after POST
            except Exception,e :
                 _logger.error("Problem with train creation  %s"% e)
    else:
        try:
            manager = request.user.username
            form = ProductionTrainForm({'manager':manager})
        except:
            return HttpResponseRedirect(reverse('prodtask:request_table'))
    return render(request, 'prodtask/_train_form.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Create train',
        'form': form,
        'submit_url': 'prodtask:train_create',
        'parent_template': 'prodtask/_index.html',
    })


@login_required(login_url='/prodtask/login/')
def train_edit(request, train_id):
    try:
        train = TrainProduction.objects.get(id=train_id)
        depart_date = train.departure_time.strftime('%Y-%m-%d')
    except:
        return HttpResponseRedirect(reverse('prodtask:request_table'))

    return render(request, 'prodtask/_train_creation.html', {
        'active_app': 'prodtask',
        'pre_form_text': "DPD train modification",
        'url_args': train_id,
        'train': train,
        'parent_template': 'prodtask/_index.html',
        'depart_date':depart_date,
        'groups':[x[0] for x in TRequest.PHYS_GROUPS]

    })