import json
import logging
from os import walk

import time
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_protect
from rest_framework.decorators import api_view
from rest_framework.response import Response
import pickle

from atlas.prodtask.ddm_api import tid_from_container
from atlas.prodtask.helper import form_request_log
from atlas.prodtask.models import HashTag, HashTagToRequest, ProductionTask
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
        tasks = list(ProductionTask.objects.filter(id__in=list(tasks_id)))
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



def prefilll_tophashtag(hashtags, file_name):
    tasks_ids = set()
    print time.time()
    for hashtag in hashtags:
        tasks_ids.update(tasks_by_hashtag(hashtag))
    hashtag_dict = {}
    tasks = list(ProductionTask.objects.filter(id__in=tasks_ids))
    for task in tasks:
        hashtag_ids = [int(x.id) for x in task.hashtags]
        for hashtag_id in hashtag_ids:
            hashtag_dict[hashtag_id] = hashtag_dict.get(hashtag_id,0)+1
    print time.time()
    request_statistics = tasks_progress(tasks)
    print time.time()
    ordered_step_statistic = prepare_step_statistic(request_statistics)
    result = {'step_statistic':ordered_step_statistic, 'hashtags':hashtag_dict}
    pickle.dump(result,open(file_name,'wb'))


def hashtag_request_to_tasks():
    all_hashtags = HashTagToRequest.objects.all()
    for request_hashtag in all_hashtags:
        if ' ' not in request_hashtag.hashtag.hashtag:
            tasks = ProductionTask.objects.filter(request=request_hashtag.request)
            tasks_to_update = []
            for task in tasks:
                if not task.hashtag_exists(request_hashtag.hashtag):
                    tasks_to_update.append(task.id)
            if tasks_to_update:
                _logger.debug("Hashtag %s was added for tasks %s "%(request_hashtag.hashtag,tasks_to_update))
            map(lambda x: add_hashtag_to_task(request_hashtag.hashtag.hashtag,x),tasks_to_update)



@api_view(['POST'])
def tasks_statistic_steps(request):
    result = {}
    try:
        tasks_ids = json.loads(request.body)
        tasks = list(ProductionTask.objects.filter(id__in=tasks_ids))
        request_statistics = tasks_progress(tasks)
        ordered_step_statistic = prepare_step_statistic(request_statistics)
        steps_name = [x['step_name'] for x in ordered_step_statistic]
        chains = []

        for chain in request_statistics['chains'].values():
            current_chain = [{}] * len(steps_name)
            chain_requests = set()
            chain_name = ''
            for task_id in chain:
                i = steps_name.index(request_statistics['processed_tasks'][task_id]['step'])
                task = {'task_id':task_id}
                task.update(request_statistics['processed_tasks'][task_id])
                if not chain_name:
                    if '_' not in task['name'].split('.')[-1]:
                        chain_name = '.'.join(task['name'].split('.')[1:3])
                    else:
                        tags = task['name'].split('.')[-1]
                        chain_name = '.'.join(task['name'].split('.')[1:3])+'...'+tags[:tags.rfind('_')]
                chain_requests.add(request_statistics['processed_tasks'][task_id]['request'])
                current_chain[i] = task
            chains.append({'chain':current_chain,'requests':chain_requests, 'chain_name':chain_name})
        result.update({'step_statistic':ordered_step_statistic,'chains':chains})
    except Exception,e:
        print str(e)
    return Response({"load": result})


def tasks_by_hashtag(hashtag):
    if HashTag.objects.filter(hashtag__iexact=hashtag).exists():
        tasks_hashtags = HashTag.objects.filter(hashtag__iexact=hashtag)[0].tasks_ids
        return tasks_hashtags
    return []


@csrf_protect
@api_view(['POST'])
def tasks_requests(request):
    result_tasks_list = []
    try:
        input_str = request.body
        reqid_list = [int(x) for x in input_str.replace(' ',',').replace(';',',').split(',') if x]
        result_tasks_list = list(ProductionTask.objects.filter(request__in=reqid_list).order_by('id').values_list('id',flat=True))
        result_tasks_list_ids = map(int, result_tasks_list)
        request.session['selected_tasks'] = result_tasks_list_ids
    except Exception,e:
        print str(e)
    return Response(result_tasks_list)

@csrf_protect
@api_view(['POST'])
def tasks_hashtag(request):
    result_tasks_list = []
    try:
        input_str = request.body
        result_tasks_list = tasks_from_string(input_str)
        request.session['selected_tasks'] = result_tasks_list
    except Exception,e:
        print str(e)
    return Response(result_tasks_list)


def tasks_from_string(input_str):
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
    return list(result_tasks)

@api_view(['GET'])
def hashtagslists(request):
    result = []
    try:

        hashtags = HashTag.objects.all()
        for hashtag in hashtags:
            hashtag_tasks_number = hashtag.tasks_count
            if hashtag_tasks_number >0:
                result.append({'hashtag':hashtag.hashtag,'tasks':hashtag_tasks_number})
    except Exception,e:
        pass
    result.sort(key=lambda x: x['tasks'])
    result.reverse()
    print result
    return Response(result)


@api_view(['GET'])
def hashtags_campaign_lists(request):
    result = []
    try:

        campaign_summery = pickle.load(open('/data/hashtagscampaign.pkl','rb'))
        hashtags = campaign_summery['hashtags']
        for hashtag_id in hashtags:
            hashtag = HashTag.objects.get(id=hashtag_id).hashtag
            result.append({'hashtag':hashtag,'tasks': hashtags[hashtag_id]})

    except Exception,e:
        pass

    return Response(result)


@api_view(['GET'])
def campaign_steps(request):
    result = {}
    try:

        campaign_summery = pickle.load(open('/data/hashtagscampaign.pkl','rb'))
        result = campaign_summery['step_statistic']
    except Exception,e:
        print e
        pass

    return Response(result)

def request_hashtags_campaign(request):
    if request.method == 'GET':
        return render(request, 'prodtask/_hashtag_campaign.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Hashtags for mc16 campaign',
                'submit_url': 'prodtask:request_hashtags_campaign',
                'parent_template': 'prodtask/_index.html',
            })

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

@csrf_protect
def add_task_hashtag(request, taskid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            hashtag = input_dict['hashtag']
            hashtag = hashtag.replace('#','')
            existed_hashtag = add_or_get_request_hashtag(hashtag)
            add_hashtag_to_task(existed_hashtag,taskid)
            results = {'success':True,'data':existed_hashtag.hashtag}
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



def set_hashtags_keys_by_hahstag(hashtags):
    requests = set()
    for hashtag in hashtags:
        current_requests = list(HashTagToRequest.objects.filter(hashtag=HashTag.objects.get(hashtag=hashtag)).values_list('request_id',flat=True))
        requests.update(current_requests)
    #print requests
    map(get_key_for_request,list(requests))

def set_mc16_hashtags(hashtags):
    mc16_hashtag = HashTag.objects.get(hashtag='MC16a_CP')
    last_task_id = mc16_hashtag.last_task
    last_task = ProductionTask.objects.get(id=last_task_id)
    new_tasks = ProductionTask.objects.filter(request_id__gt=last_task.request_id,campaign='MC16',provenance='AP')
    unique_requests = set()
    for task in new_tasks:
        add_hashtag_to_task(mc16_hashtag.hashtag,task.id)
        unique_requests.add(int(task.request_id))
    map(get_key_for_request,list(unique_requests))
    #print unique_requests



def add_hashtag_to_task(hashtag_name, task_id):
    task = ProductionTask.objects.get(id=task_id)
    current_hashtags = task.hashtags
    hashtag = HashTag.objects.get(hashtag=hashtag_name)
    if hashtag not in current_hashtags:
        task.set_hashtag(hashtag_name)

def get_key_for_request(reqid):
    print reqid
    request = TRequest.objects.get(reqid=reqid)

    slices = list(InputRequestList.objects.filter(request=request))

    for slice in slices:
        steps = list(StepExecution.objects.filter(slice=slice))
        tasks = []
        campaign = slice.input_data.split('.')[0].upper()
        base_path = CVMFS_BASEPATH+campaign+'JobOptions/latest/'
        common_keywords = get_common_keywords(base_path+'common/')
        keywords = get_keyword_jo(slice.input_data, base_path+'share/',common_keywords)
        category = get_category(keywords, slice.input_data)
        hashtags = []
        for keyword in keywords:
            hashtags.append(add_or_get_request_hashtag(keyword,'KW'))
        if category:
            category_hashtag = add_or_get_request_hashtag(category,'KW')
            if category_hashtag not in hashtags:
                hashtags.append(category_hashtag)
        for step in steps:
            tasks += list(ProductionTask.objects.filter(step=step))
        for task in tasks:
            current_hashtags = task.hashtags
            for hashtag in hashtags:
                if hashtag not in current_hashtags:
                    task.set_hashtag(hashtag)

def fix_wrong_jo(request):
    slices = InputRequestList.objects.filter(request=request)
    for slice in slices:
        if '.py' not in slice.input_data:
            tid = tid_from_container(slice.dataset_id)[0]
            parent_slice = ProductionTask.objects.get(id=tid).step.slice
            if '.py' in parent_slice.input_data:
                slice.input_data = parent_slice.input_data
                slice.save()
                print slice.input_data, parent_slice.input_data


def get_category(jo_keys, job_option):
    PHYS_CATEGORIES_MAP = {"BPhysics":["charmonium","Jpsi","Bs","Bd","Bminus","Bplus",'CHARM','BOTTOM','BOTTOMONIUM','B0'],
                  "BTag":["bTagging", "btagging"],
                  "Diboson":["diboson","ZZ", "WW", "WZ", "WWbb", "WWll", "zz", "ww", "wz", "wwbb","wwll"],
                  "DrellYan":["drellyan"],
                  "Exotic":["exotic", "monojet", "blackhole", "technicolor", "RandallSundrum",
                       "Wprime", "Zprime", "magneticMonopole", "extraDimensions", "warpedED",
                       "randallsundrum", "wprime", "zprime", "magneticmonopole",
                       "extradimensions", "warpeded", "contactInteraction","contactinteraction",'SEESAW'],
                  "GammaJets":["photon", "diphoton"],
                  "Higgs":["WHiggs", "ZHiggs", "mH125", "Higgs", "VBF", "SMHiggs", "higgs", "mh125",
                       "zhiggs", "whiggs", "bsmhiggs", "chargedHiggs","BSMHiggs","smhiggs"],
                  "Minbias":["minBias", "minbias"],
                  "Multijet":["dijet", "multijet", "qcd"],
                  "Performance":["performance"],
                  "SingleParticle":["singleparticle"],
                  "SingleTop":["singleTop", "singletop"],
                  "SUSY":["SUSY", "pMSSM", "leptoSUSY", "RPV", "bino", "susy", "pmssm", "leptosusy", "rpv",'MSSM'],
                  "Triboson":["tripleGaugeCoupling", "triboson", "ZZW", "WWW", "triplegaugecoupling", "zzw", "www"],
                  "TTbar":["ttbar"],
                  "TTbarX":["ttw","ttz","ttv","ttvv","4top","ttW","ttZ","ttV","ttWW","ttVV"],
                  "Upgrade":["upgrad"],
                  "Wjets":["W", "w"],
                  "Zjets":["Z", "z"]}

    match = {}
    for phys_category in PHYS_CATEGORIES_MAP:
        current_map = [x.strip(' ').lower() for x in PHYS_CATEGORIES_MAP[phys_category]]
        match[phys_category] = len([x for x in jo_keys if x.lower() in current_map])
    closest_category = max(match,  key=match.get)
    if match[closest_category]!=0:
        return closest_category
    else:
        phys_short = job_option.split('_',1)[1].lower()
        if 'singletop' in phys_short: return "SingleTop"
        if 'ttbar'     in phys_short: return "TTbar"
        if 'jets'      in phys_short: return "Multijet"
        if 'h125'      in phys_short: return "Higgs"
        if 'ttbb'      in phys_short: return "TTbarX"
        if 'ttgamma'   in phys_short: return "TTbarX"
        if '_tt_'      in phys_short: return "TTbar"
        if 'upsilon'   in phys_short: return "BPhysics"
        if 'tanb'      in phys_short: return "SUSY"
        if '4topci'    in phys_short: return "Exotic"
        if 'xhh'       in phys_short: return "Higgs"
        if '3top'      in phys_short: return "TTbarX"
        if '_wt'       in phys_short: return "SingleTop"
        if '_wwbb'     in phys_short: return "SingleTop"
        if '_wenu_'    in phys_short: return "Wjets"
    return None


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
