import json
import logging
from os import walk

from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_protect
from rest_framework.decorators import api_view
from rest_framework.response import Response

from atlas.prodtask.helper import form_request_log
from atlas.prodtask.models import HashTag, HashTagToRequest, ProductionTask, HashTagToTask
from atlas.prodtask.views import tasks_progress, prepare_step_statistic, form_hashtag_string
from .models import StepExecution, InputRequestList, TRequest

_logger = logging.getLogger('prodtaskwebui')


@api_view(['GET'])
def request_hashtags(request, hashtags):
    hashtags_to_process = hashtags.split(',')
    result={}
    tasks = []
    first_requests = True
    try:
        tasks_id = set()
        for hashtag in hashtags_to_process:
            tasks_id.update(tasks_by_hashtag(hashtag))
        tasks = ProductionTask.objects.filter(id__in=list(tasks_id))
        request_statistics = tasks_progress(tasks)
        result = {}
        ordered_step_statistic =   prepare_step_statistic(request_statistics)
        steps_name = [x['step_name'] for x in ordered_step_statistic]
        chains = []
        for chain in request_statistics['chains'].values():
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
    except Exception,e:
        print str(e)
    return Response({"load": result})


@api_view(['POST'])
def tasks_statistic_steps(request):
    result = {}
    try:
        tasks_ids = json.loads(request.body)
        tasks = ProductionTask.objects.filter(id__in=tasks_ids)
        request_statistics = tasks_progress(tasks)
        ordered_step_statistic = prepare_step_statistic(request_statistics)
        steps_name = [x['step_name'] for x in ordered_step_statistic]
        chains = []
        for chain in request_statistics['chains'].values():
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
    except Exception,e:
        print str(e)
    return Response({"load": result})


def tasks_by_hashtag(hashtag):
    if HashTag.objects.filter(hashtag__iexact=hashtag).exists():
        tasks_hashtags = list(HashTagToTask.objects.filter(hashtag=HashTag.objects.filter(hashtag__iexact=hashtag)[0]).values_list('task_id',flat=True))
        tasks_hashtags_int = map(int, tasks_hashtags)
        return tasks_hashtags_int
    return []

@csrf_protect
@api_view(['POST'])
def tasks_hashtag(request):
    result_tasks_list = []
    try:
        input_str = request.body
        input_str.replace('#','')
        input_str = input_str.replace('&','#&').replace('|','#|').replace('!','#!')
        tokens = input_str.split('#')
        hashtags_operations = {'and':[],'or':[],'not':[]}
        operators = {'&':'and','|':'or','!':'not'}
        for token in tokens:
            if token:
                hashtags_operations[operators[token[0:1]]].append(token[1:])
        result_tasks = set()
        for hashtag in hashtags_operations['or']:
            result_tasks.update(tasks_by_hashtag(hashtag))
        for hashtag in hashtags_operations['and']:
            current_tasks = tasks_by_hashtag(hashtag)
            if not result_tasks:
                result_tasks.update(current_tasks)
            else:
                temp_task_set = [x for x in current_tasks if x in result_tasks]
                result_tasks = set(temp_task_set)
        if result_tasks:
            for hashtag in hashtags_operations['not']:
                current_tasks = tasks_by_hashtag(hashtag)
                temp_task_set = [x for x in result_tasks if x not in current_tasks]
                result_tasks = set(temp_task_set)
        result_tasks_list = list(result_tasks)
        request.session['selected_tasks'] = result_tasks_list
    except Exception,e:
        print str(e)
    return Response(result_tasks_list)


@api_view(['GET'])
def hashtagslists(request):
    result = []
    try:

        hashtags = HashTag.objects.all()
        for hashtag in hashtags:
            hashtag_tasks_number = HashTagToTask.objects.filter(hashtag=hashtag).count()
            if hashtag_tasks_number >0:
                result.append({'hashtag':hashtag.hashtag,'tasks':hashtag_tasks_number})
    except Exception,e:
        pass
    result.sort(key=lambda x: x['tasks'])
    result.reverse()
    print result
    return Response(result)


def request_hashtags_main(request):
    if request.method == 'GET':
        return render(request, 'prodtask/_hashtags_list.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Hashtags to request',
                'submit_url': 'prodtask:request_progress_main',
                'parent_template': 'prodtask/_index.html',
            })


def add_or_get_request_hashtag(hashtag, type='UD'):
    existed_hashtags = list(HashTag.objects.filter(hashtag__iexact=hashtag))
    if existed_hashtags:
        existed_hashtag = existed_hashtags[0]
    else:
        existed_hashtag = HashTag()
        existed_hashtag.hashtag = hashtag
        existed_hashtag.type = type
        existed_hashtag.save()
    return existed_hashtag

@csrf_protect
def add_request_hashtag(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            hashtag = input_dict['hashtag']
            hashtag = hashtag.replace('#','')
            _logger.debug(form_request_log(reqid,request,'Add hashtag: %s' % str(hashtag)))
            existed_hashtag = add_or_get_request_hashtag(hashtag)
            if not HashTagToRequest.objects.filter(hashtag=existed_hashtag,request=reqid).exists():
                request_hashtag = HashTagToRequest()
                request_hashtag.hashtag = existed_hashtag
                request_hashtag.request = TRequest.objects.get(reqid=reqid)
                request_hashtag.save()
            hashtag_html,hashtag_href = form_hashtag_string(reqid)
            results = {'success':True,'data':{'html':hashtag_html,'href':hashtag_href}}
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')


def get_include_file(file_name):
    include_files  = []
    with open(file_name,'r') as input_file:
        for line in input_file:
            if line:
                if 'include(' in line:
                    include_files.append(line[line.find('(')+2:line.find(')')-1])
    return include_files


CVMFS_BASEPATH = '/cvmfs/atlas.cern.ch/repo/sw/Generators/'

def get_key_for_request(reqid):
    print reqid
    request = TRequest.objects.get(reqid=reqid)
    campaign = request.campaign.upper()
    base_path = CVMFS_BASEPATH+campaign+'JobOptions/latest/'
    common_keywords = get_common_keywords(base_path+'common/')
    slices = list(InputRequestList.objects.filter(request=request))

    for slice in slices:
        steps = list(StepExecution.objects.filter(slice=slice))
        tasks = []
        keywords = get_keyword_jo(slice.input_data, base_path+'share/',common_keywords)
        hashtags = []
        for keyword in keywords:
            hashtags.append(add_or_get_request_hashtag(keyword,'KW'))
        for step in steps:
            tasks += list(ProductionTask.objects.filter(step=step))
        for task in tasks:
            current_hashtags = HashTagToTask.objects.filter(task=task).values_list('hashtag_id',flat=True)
            for hashtag in hashtags:
                if hashtag.id not in current_hashtags:
                    new_hashtag = HashTagToTask()
                    new_hashtag.hashtag = hashtag
                    new_hashtag.task = task
                    new_hashtag.save()



def get_common_keywords(parent_dir_path):
    common_files = []
    for (dirpath, dirnames, filenames) in walk(parent_dir_path):
        common_files.extend([dirpath+'/'+x for x in filenames if x.endswith('py')])
    common_files_keywords = {}
    for common_file in common_files:
        common_files_keywords.update({common_file.split('/')[-1]:get_hashtag_from_file(common_file)[0]})
    return common_files_keywords

def get_keyword_jo(filename, base_jo_path, common_files_keywords):
    dir_name = 'DSID' +filename.split('.')[1][0:3]  +'xxx/'
    keywords = []
    #print base_jo_path+dir_name+filename
    try:
        keywords, includes = get_hashtag_from_file(base_jo_path+dir_name+filename)
        if not keywords:
            for include in includes:
                if include in common_files_keywords:
                    keywords += common_files_keywords[include]

    except Exception,e:
        print str(e)
    keywords.append(filename.split('.')[2].split('_')[0])

    return keywords


def get_keys_from_dir(dir_path, shared_includes):

    files_paths = []
    file_keys = {}
    for (dirpath, dirnames, filenames) in walk(dir_path):
        files_paths.extend(filenames)
        break
    for file_path in files_paths:
        file_keys.update({file_path:get_hashtag_from_file(file_path)})


def get_hashtag_from_file(file_name):
    keywords = []
    keywords_str = ''
    carry = False
    include_files  = []
    with open(file_name,'r') as input_file:
        for line in input_file:
            if line:
                if carry:
                    if ']' in line:
                        keywords_str +=  line[:line.find(']')]
                        break
                    else:
                        keywords_str +=  line
                if ('keywords' in line) and ('[' in line):
                    if ']' in line:
                        keywords_str = line[line.find('[')+1:line.find(']')]
                        break
                    else:
                        carry = True
                        keywords_str = line[line.find('[')+1:]
                if 'include(' in line:
                    include_files.append(line[line.find('(')+2:line.find(')')-1].split('/')[-1])


    tokens =  keywords_str.replace("'",'"').split('"')
    for token in tokens:
        if token:
            if token[0] not in [' ',',','\n','\t','\r']:
                keywords.append(token)
    return keywords, include_files
