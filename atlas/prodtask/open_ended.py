import json
import logging

from django.http import HttpResponse, HttpResponseRedirect

from django.views.decorators.csrf import csrf_protect
from time import sleep
from copy import deepcopy
from atlas.prodtask.ddm_api import DDM
from ..prodtask.models import RequestStatus
from ..prodtask.spdstodb import fill_template
from ..prodtask.request_views import clone_slices, set_request_status
from ..prodtask.helper import form_request_log
from ..prodtask.task_actions import do_action
from .views import form_existed_step_list, form_step_in_page, fill_dataset

from .models import StepExecution, InputRequestList, TRequest, Ttrfconfig, ProductionTask, ProductionDataset

_logger = logging.getLogger('prodtaskwebui')


def extend_open_ended_request(reqid):
    """
    To extend request by adding dataset which are not yet processed. Container is taken from first slice,
    steps should be skipped on it.
    :param reqid: ID of request to extend
    :return:
    """

    slices = list(InputRequestList.objects.filter(request=reqid))
    container_name = slices[0].dataset.name
    datasets = []
    for slice in slices[1:]:
        datasets.append(slice.dataset.name)
    ddm = DDM()
    datasets_in_container = ddm.dataset_in_container(container_name)
    for dataset in datasets_in_container:
        if (dataset not in datasets) and (dataset[dataset.find(':')+1:] not in datasets):
            new_slice_number = clone_slices(reqid,reqid,[0],-1,False)
            new_slice = InputRequestList.objects.get(request=reqid,slice=new_slice_number)
            new_slice.dataset = fill_dataset(dataset)
            new_slice.save()
            steps = StepExecution.objects.filter(request=reqid,slice=new_slice)
            for step in steps:
                step.status = 'Approved'
                step.save()
    set_request_status('cron',reqid,'approved','Automatic openended approve', 'Request was automatically extended')




    pass
