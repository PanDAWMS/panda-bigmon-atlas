import logging
import pickle

from django.shortcuts import render
from rest_framework.response import Response

from atlas.prodtask.views import tasks_progress, prepare_step_statistic
#from ..prodtask.task_actions import do_action
from rest_framework.decorators import api_view

from .models import ProductionTask

_logger = logging.getLogger('prodtaskwebui')

def request_progress_main(request):
    if request.method == 'GET':
        return render(request, 'prodtask/_progress_stat.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Progress',
                'submit_url': 'prodtask:request_progress_main',
                'parent_template': 'prodtask/_index.html',
            })

def task_chain(request):
    if request.method == 'GET':
        return render(request, 'prodtask/_task_chain.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Progress',
                'parent_template': 'prodtask/_index.html',
            })


@api_view(['GET'])
def request_hashtag_monk(request, hashtags):
    hashtag_monk = pickle.load(open('/data/hashtagmonk.pkl','rb'))
    result = []
    try:
        for entry in hashtag_monk:
            progress = request_progress(entry['requests_ids'])
            ordered_step_statistic = prepare_step_statistic(progress)
            result.append({'hashtags':entry['filter'],'step_statistic':ordered_step_statistic})
    except Exception as e:
         print(str(e))
    return Response({"load": result})

@api_view(['GET'])
def request_progress_general(request, reqids):
    requests_to_process = list(map(int,reqids.split(',')))
    request_statistics = request_progress(requests_to_process)
    result = {}
    ordered_step_statistic = []
    try:
        ordered_step_statistic = prepare_step_statistic(request_statistics)
        steps_name = [x['step_name'] for x in ordered_step_statistic]
        chains = []
        for chain in list(request_statistics['chains'].values()):
            current_chain = [{}] * len(steps_name)
            chain_requests = set()
            for task_id in chain:
                i = steps_name.index(request_statistics['processed_tasks'][task_id]['step'])
                task = {'task_id':task_id}
                task.update(request_statistics['processed_tasks'][task_id])
                chain_requests.add(request_statistics['processed_tasks'][task_id]['request'])
                current_chain[i] = task
            chains.append({'chain':current_chain,'requests':chain_requests})
        result.update({'step_statistic':ordered_step_statistic,'chains':chains})
    except Exception as e:
        print(str(e))
    return Response({"load": result})


def request_progress(reqid_list):
    return tasks_progress(list(ProductionTask.objects.filter(request__in=reqid_list).order_by('id')))


