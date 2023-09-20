from django.contrib.auth.decorators import login_required
from django.db.models import Count
import logging

from rest_framework.authentication import SessionAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated

from atlas.prodtask.models import ProductionTask, StepTemplate, MCPriority, HashTag
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from django.shortcuts import render


from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections
from atlas.settings.local import ESLogin

from atlas.settings import OIDC_LOGIN_URL

connections.create_connection(hosts=['http://aiatlas171.cern.ch:9200'], http_auth=(ESLogin['login'],ESLogin['password']), timeout=5000)
_logger = logging.getLogger('prodtaskwebui')
ES6 = Elasticsearch(hosts=['http://aiatlas171.cern.ch:9200'],http_auth=(ESLogin['login'],ESLogin['password']), timeout=5000)


DKB_ES6_SEARCH = {'search': Search, 'prod': {'using':ES6,  'index':"apinestedproduction_tasks", 'doc_type':'task'},
                  'analy': {'using':ES6,  'index':"apinestedanalysis_tasks", 'doc_type':'task'},
                  'task_duration_script':"doc['end_time'].value - doc['start_time'].value"}


from atlas.dkb.dkb2 import DKB_ES7_SEARCH

DEFAULT_SEARCH = DKB_ES7_SEARCH

SIZE_TO_DISPLAY = 2000

def test_connection():
    return Search(index="prodsys", doc_type='MC16')


@api_view(['POST'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def es_task_search_analy(request):
    search_string = request.data
    result, total = es_task_search_all(search_string, 'analy')
    return Response({'tasks':result,'total':total})


def es_task_search_all(search_string, task_type):
    search_values = []
    if task_type == 'all':
        search_values = [True, False]
    if task_type == 'analy':
        search_values = [True]
    if task_type == 'prod':
        search_values = [False]
    result = []
    total = 0
    for search_value in search_values:
        response = keyword_search_nested(key_string_from_input(search_string)['query_string'], search_value).execute()
        for hit in response:
            total += 1
            current_hit = hit.to_dict()
            if 'output_dataset' not in current_hit:
                current_hit['output_dataset'] = []
            result.append(current_hit)
    return result, total

@api_view(['POST'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def es_task_search(request):
    search_string = request.data
    result, total = es_task_search_all(search_string, 'prod')
    return Response({'tasks':result,'total':total})


def hits_to_tasks(hits):
    result = []
    for hit in hits:
        current_hit = hit.to_dict()
        result.append(current_hit)
    return result


@api_view(['POST'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def search_string_to_url(request):
    search_string = request.data
    return Response(key_string_from_input(search_string))

def key_string_from_input(search_string):
    prepared_strings = search_string['search_string'].replace('\n',',').replace('\r',',').replace('\t',',').replace(' ',',').split(',')
    query_string = ' AND '.join([x for x in prepared_strings if x])
    url = ','.join([x for x in prepared_strings if x])
    return {'url':url,'query_string':query_string}


@login_required(login_url=OIDC_LOGIN_URL)
def index(request):
    if request.method == 'GET':

        return render(request, 'dkb/_index_dkb.html', {
                'active_app': 'dkb',
                'pre_form_text': 'DKB',
                'title': 'DKB search',
                'parent_template': 'prodtask/_index.html',
            })


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def test_name(request):
    """Return name of the user"""
    return Response(request.user.username)


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def task_tree(request, task_id):
    result_tree = []
    task_info = []
    return Response(request.user.username)

def keyword_search_nested(keyword_string, is_analy=False):
    keyword_wildcard = []
    keyword_non_wildcard = []
    keywords = keyword_string.split(' AND ')
    for keyword in keywords:
        if ('?' in keyword) or ('*' in keyword):
            tokens = ['"'+x+'*"' for x in keyword.replace('"','').split('*') if x ]
            if keyword[-1] != '*':
                tokens[-1] = tokens[-1][:-2]+'"'
            keyword_wildcard+=tokens
        else:
            keyword_non_wildcard.append(keyword)
    query_string = []
    if keyword_wildcard:
        query_string.append({
            "query_string": {
                "query": ' AND '.join(keyword_wildcard),
                "analyze_wildcard": True,
                "fields":['taskname']
            }})
    if keyword_non_wildcard:
        query_string.append({
            "query_string": {
                "query": ' AND '.join(keyword_non_wildcard),
            }})
    if is_analy:
        es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['analy'])
    else:
        es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['prod'])
    query = es_search.update_from_dict({"query": {
        "bool": {
            "must":query_string,
            'should': {
                'nested': {
                    'path': 'output_dataset',
                    'score_mode': 'sum',
                    'query': {'match_all': {}},
                }
            }
        },

    }, 'size':SIZE_TO_DISPLAY
    })

    return query



def wrong_deriv():
    es_search = Search(index="test_prodsys_rucio_ami", doc_type='task')
    query = {
              "size": 1000,
              "_source": ["taskid", "requested_events", "input_events", "n_files_per_job", "n_events_per_job", "n_files_to_be_used", "primary_input_events", "primary_input_deleted", "task_timestamp", "processed_events", "primary_input"],
              "query": {
                "bool": {
                  "must": [
                      {"term": {"hashtag_list": "newfastcalosim"}},
                    {"match": {"step_name": "Deriv"}},
                    {"script": {"script": {
                          "inline": "doc['processed_events'].value > doc['input_events'].value",
                          "lang": "painless"
                    }}},
                    {"exists": {"field": "total_events"}}
                  ]
                }
              }
            }
    aggregs = es_search.update_from_dict(query)
    execute =  aggregs.execute()
    return execute





def get_format_by_request(search_dict):
    query = {
          "size": 0,
          "query": search_dict,
          "aggs": {
            "format": {
              "terms": {
                "field": "output_formats",
                "size": 1000
              }
            }
          }
        }

    es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['prod'])
    aggregs = es_search.update_from_dict(query)
    exexute = aggregs.execute()
    result = []
    if exexute.aggs:
        for x in exexute.aggs.format.buckets:
            result.append(x.key)
    return result




def running_events_stat_deriv_new(search_dict, status, formats):


    result = {}
    for format in formats:
        query = {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must": [
                        search_dict,
                        {"terms": {"status": status}},
                          {"nested": {
                              "path": "output_dataset",
                              "query": {"term": {"output_dataset.data_format.keyword": format}}
                          }}
                      ]
                    }
                  },

                      "aggs": {
                        "amitag": {
                          "terms": {"field": "ctag"},
                          "aggs": {
                            "input_events": {
                              "sum": {"field": "input_events"}
                            },
                            "processed_events": {
                              "sum": {"field": "processed_events"}
                            },
                              "input_bytes": {
                                  "sum": {"field": "input_bytes"}
                              }
                          }
                        }
                      }
                    }

        es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['prod'])
        execute=None
        try:
            aggregs = es_search.from_dict(query)
            execute = aggregs.execute()
        except Exception as e:
            _logger.error("Problem with es deriv : %s" % (e))
            aggregs = None

        if aggregs and execute.aggregations:
                    for x in execute.aggregations.amitag.buckets:
                        result[format+' '+x.key] = {'name': format+' '+x.key, 'processed_events': x.processed_events.value,
                                   'input_events': x.input_events.value,
                                   'total_tasks': x.doc_count, 'input_bytes': x.input_bytes.value }

    return result

def deriv_formats(search_dict):
    formats = get_format_by_request(search_dict)
    formats_dict = {}
    for format in formats:
        formats_dict.update( { format: {
                                "has_child": {
                                  "type": "output_dataset",
                                  "query": {"term": {"data_format": format}}
                                }
                              }})
    return formats_dict




def form_statistic_per_step(statistics,running_stat, finished_stat, mc_steps=True, steps_tasks={}):
    result = []
    total_percent_with_hs06 = []

    if mc_steps:
        steps = MCPriority.STEPS
        field = "total_events"
    else:
        steps = list(statistics.keys())
        field = "processed_events"
    for step in steps:
        if step in statistics:
            coeff = 1.0
            current_stat = statistics[step]
            if steps_tasks and (step in steps_tasks):
                    current_stat['total_tasks_db'] = sum([steps_tasks[step][x] for x in steps_tasks[step] if x not in ProductionTask.RED_STATUS])
                    coeff = current_stat['total_tasks'] / current_stat['total_tasks_db']
                    if coeff > 1:
                        coeff = 1
            else:
                current_stat['total_tasks_db'] = current_stat['total_tasks']
            percent_done = 0.0
            percent_runnning = 0.0
            percent_pending = 0.0
            percent_not_started = 100.0
            current_stat['finished_tasks'] = 0
            current_stat['finished_bytes'] = 0
            if current_stat["input_events"] == 0:
                step_status = 'Unknown'
            else:
                percent_done = coeff * float(current_stat[field]) / float(current_stat['input_events'])
                if percent_done>=00.9999 and (current_stat[field] < current_stat['input_events']):
                    percent_done=00.9999
                if (percent_done > 0.90):
                    step_status = 'StepDone'
                elif (percent_done > 0.10):
                    step_status = 'StepProgressing'
                else:
                    step_status = 'StepNotStarted'
                running_events = 0
                finished_events = 0
                if step in running_stat:
                    running_events = running_stat[step]['input_events'] - running_stat[step][field]
                if step in finished_stat:
                    finished_events = finished_stat[step]['input_events'] - finished_stat[step][field]
                    current_stat['finished_tasks'] = finished_stat[step]['total_tasks']
                    current_stat['finished_bytes'] = finished_stat[step]['input_bytes']
                percent_runnning = coeff * float(running_events) / float(current_stat['input_events'])
                percent_pending = coeff *  float(current_stat['input_events'] - current_stat[
                    field] - running_events - finished_events) / float(current_stat['input_events'])
                percent_not_started = (1 - coeff)
                if percent_pending < 0:
                    percent_pending = 0


            current_stat['step_status'] = step_status
            current_stat['percent_done'] = percent_done * 100
            current_stat['percent_runnning'] = percent_runnning * 100
            current_stat['percent_pending'] = percent_pending * 100
            current_stat['not_started'] = percent_not_started * 100
            result.append(current_stat)
            if current_stat['hs06']:
                total_percent_with_hs06.append((percent_done,current_stat['hs06']*current_stat['input_events']))
            elif current_stat['finished_tasks']>0:
                total_percent_with_hs06.append((percent_done,  current_stat['input_events']))
    result.sort(key=lambda x: -x['input_events'])
    total_campaign = None
    if len(total_percent_with_hs06) == len(statistics.keys()):
        total_campaign = sum([x[0]*x[1] for x in total_percent_with_hs06]) * 100 / sum([x[1] for x in total_percent_with_hs06])
    return result, total_campaign


@api_view(['POST'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def output_hashtag_stat(request):
    try:
        hashtags_raw = request.data['hashtag']
        task_ids = tasks_from_string(hashtags_raw)
        steps={}
        tasks = list(ProductionTask.objects.filter(id__in=task_ids).values('status','ami_tag','output_formats'))
        for task in tasks:
            step_name = task['output_formats']+' '+task['ami_tag']
            if step_name not in steps:
                steps[step_name] = {}
            if task['status'] not in steps[step_name]:
                steps[step_name][task['status']] = 0
            steps[step_name][task['status']] += 1
        status_dict = dict([(x['status'], x['count']) for x in
              ProductionTask.objects.filter(id__in=task_ids).values('status').annotate(count=Count('id'))])
        status_stat = [{'name':'total','count':sum(status_dict.values())}]
        for status in ProductionTask.STATUS_ORDER:
            if status in status_dict:
                status_stat.append({'name': status, 'count': status_dict[status]})
        hashtags_split = hashtags_raw.replace('&',',').replace('|',',').split(',')
        hashtags = [x.lower() for x in hashtags_split if x]
        #format_dict = deriv_formats({"terms": {"hashtag_list": hashtags}})
        formats = get_format_by_request({"terms": {"hashtag_list": hashtags}})
        statistics = statistic_by_request_deriv_new({"terms": {"hashtag_list": hashtags}}, formats)
        running_stat = running_events_stat_deriv_new({"terms": {"hashtag_list": hashtags}},['running'], formats)
        finished_stat = running_events_stat_deriv_new({"terms": {"hashtag_list": hashtags}},['finished','done'], formats)
        step_resut, total = form_statistic_per_step(statistics,running_stat, finished_stat, False, steps)
        result = {'steps':
                      step_resut,'status':status_stat, 'total_campaign': total}

    except Exception as e:

        return Response({'error':str(e)},status=400)
    return Response(result)



def count_output_stat(project, ami_tags, outputs=None):
    # no_empty = False
    if not outputs:
        # no_empty = True
        output_set = set()
        templates = StepTemplate.objects.filter(ctag__in=ami_tags)
        for template in templates:
            output_set.update(template.output_formats.split('.'))
        outputs = list(output_set)
    # input_datasets = {}
    result = []
    # ddm = DDM()
    for output in outputs:
        current_input_tasks = derivation_stat_nested(project, ami_tags, output)
        if current_input_tasks['total'] >0:
            result.append({'output': output, 'ratio': current_input_tasks['ratio'],'events_ratio': current_input_tasks['events_ratio'],
                           'tasks': current_input_tasks['total'],'tasks_ids':[]})
        # current_input_size = 0
        # current_sum = 0
        # current_input_events = 0
        # current_events = 0
        # good_tasks = []
        #
        # for input_dataset in current_input_tasks:
        #     if input_dataset['primary_input'] not in input_datasets:
        #         try:
        #             if input_dataset['input_bytes'] > 0:
        #                 input_datasets[input_dataset['primary_input']] ={'size':input_dataset['input_bytes'],'events':input_dataset['input_events']}
        #             else:
        #                 dataset_info = ddm.dataset_metadata(input_dataset['primary_input'])
        #                 input_datasets[input_dataset['primary_input']] = {'size':dataset_info['bytes'],'events':dataset_info['events']}
        #         except Exception,e:
        #             print str(e)
        #             input_datasets[input_dataset['primary_input']] = {'size':0,'events':0}
        #     if input_datasets[input_dataset['primary_input']]['size'] > 0:
        #         try:
        #             if input_dataset['events'] > -1:
        #                 events = input_dataset['events']
        #             else:
        #                 output_dataset_info = ddm.dataset_metadata(input_dataset['dataset_name'])
        #                 events = output_dataset_info['events']
        #             current_input_size += input_datasets[input_dataset['primary_input']]['size']
        #             current_input_events += input_datasets[input_dataset['primary_input']]['events']
        #             current_events += events
        #             current_sum += input_dataset['output_bytes']
        #             good_tasks.append(input_dataset['task_id'])
        #         except:
        #             pass
        #
        # if (current_input_size !=0)and(current_input_events!=0):
        #     result.append({'output':output,'ratio':float(current_sum)/float(current_input_size),
        #                    'events_ratio':float(current_events)/float(current_input_events),
        #                    'tasks':len(good_tasks),'tasks_ids':good_tasks})
        # else:
        #     if not no_empty:
        #         result.append({'output': output, 'ratio': 0,'events_ratio':0,
        #                        'tasks': len(good_tasks),'tasks_ids':good_tasks})
    result.sort(key=lambda x:x['output'])
    return result








@api_view(['POST'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def tasks_from_list(request):
    result_tasks_list = []
    try:
        input_str = request.data
        result_tasks_list = list(map(int, input_str['taskIDs']))
        request.session['selected_tasks'] =  result_tasks_list
    except Exception as e:
        return Response({'error':str(e)},status=400)
    return Response(result_tasks_list)

@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes((IsAuthenticated,))
def deriv_output_proportion(request,project,ami_tag):
    try:
        result = count_output_stat(project,[x for x in ami_tag.split(',') if x])
    except Exception as e:
            return Response({'error':str(e)},status=400)
    return Response(result)


def find_jo_by_dsid(dsid):
    tasks = es_by_keys_nested({'run_number':dsid,'step_name':'evgen'},10)
    if tasks:
        for es_task in tasks:
            task = ProductionTask.objects.get(id=es_task['taskid'])
            if task.status in ['done','finished']:
                return task.step.slice.input_data
    return str(dsid)


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


def tasks_by_hashtag(hashtag):
    if HashTag.objects.filter(hashtag__iexact=hashtag).exists():
        tasks_hashtags = HashTag.objects.filter(hashtag__iexact=hashtag)[0].tasks_ids
        return tasks_hashtags
    return []
#
def es_by_keys_nested(values, size=10000):
    search_dict = []
    for x in values:
        search_dict.append({'term':{x:values[x]}})

    es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['prod'])
    query = {
        "query": {
            "bool": {
                "must": search_dict,
                'should': {
                    'nested': {
                        'path': 'output_dataset',
                        'score_mode': 'sum',
                        'query': {'match_all': {}},
                    }
                }
            }
        }, 'size':size
    }
    search = es_search.update_from_dict(query)
    response = search.execute()
    result = []
    for hit in response:
        current_hit = hit.to_dict()
        if 'output_dataset' not in current_hit:
            current_hit['output_dataset'] = []
        result.append(current_hit)
    return result


def test_es_by_keys_nested(values, size=10000):

    search_dict = []
    for x in values:
        search_dict.append({'term':{x:values[x]}})
    test_param = DEFAULT_SEARCH['prod']
    test_param['index']='tasks_production3'
    es_search = DEFAULT_SEARCH['search'](**test_param)
    query = {
        "query": {
            "bool": {
                "must": search_dict,
                'should': {
                    'nested': {
                        'path': 'output_dataset',
                        'score_mode': 'sum',
                        'query': {'match_all': {}},
                    }
                }
            }
        }, 'size':size
    }
    search = es_search.update_from_dict(query)
    response = search.execute()
    result = []
    for hit in response:
        current_hit = hit.to_dict()
        if 'output_dataset' not in current_hit:
            current_hit['output_dataset'] = []
        result.append(current_hit)
    return result

def statistic_by_step_new(search_dict):
    es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['prod'])
    query = {
              "size": 0,
              "query": {
                "bool": {
                  "must": [
                      search_dict,
                    {"bool": {"must_not": [{"terms": {"status": ["aborted", "failed", "broken", "obsolete"]}}]}}
                  ]
                }
              },
              "aggs": {
                "steps": {
                  "terms": {"field": "step_name.keyword"},
                  "aggs": {
                    "input_events": {
                      "sum": {"field": "input_events"}
                    },

                  "not_deleted": {
                      "filter": {"term": {"primary_input_deleted": False}},
                      "aggs": {
                          "input_bytes": {
                              "sum": {"field": "input_bytes"}
                          }
                      }
                  },
                     "processed_events": {
                       "sum": {"field": "processed_events"}
                     },
                    "total_events": {
                          "sum": {"field": "total_events"}
                     },
                      "hs06":{
                          "sum": {"field": "toths06"}
                      },
                      "cpu_failed":{
                          "sum": {"field": "toths06_failed"}
                      },

                      "ended":{
                          "filter" : {"exists" : { "field" : "end_time" }},
                          "aggs":{

                              "duration":{
                              "avg":{
                                  "script":{
                                      "inline":"doc['end_time'].value - doc['start_time'].value"
                                  }
                          }}}
                      },
                      "output": {
                          "nested": {"path": "output_dataset"},
                          "aggs": {
                              "bytes": {
                                  "sum": {"field": "output_dataset.bytes"}
                              }
                          }
                      },
                    "status": {
                      "terms": {"field": "status"}
                    }
                  }
                }
              }
            }
    result = {}

    try:
        aggregs = es_search.update_from_dict(query)
        execute = aggregs.execute()
    except Exception as e:
        print("Problem with es deriv : %s" % (e))
        aggregs = None


    return execute


def derivation_stat_nested(project, ami, output):
    es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['prod'])
    query = {
          "size": 0,
          "query": {
            "bool": {
              "must": [
                {"terms": {"primary_input": ["aod","rpvll"]}},
                {"term": {"project": project.lower()}},
                {"terms": {"ctag": ami}},
                {"term": {"status": "done"}},
                {"range": {"input_bytes": {"gt": 0}}},
                {"nested": {
                    "path": "output_dataset",
                    "query": {"range": {"output_dataset.bytes": {"gt": 0}}}
                }}
              ]
            }
          },
          "aggs": {
            "output": {
              "nested": {"path": "output_dataset"},
              "aggs": {
                "not_deleted": {
                  "filter": {"term": {"output_dataset.deleted": False}},
                  "aggs": {
                    "format": {
                      "filter": {"term": {"output_dataset.data_format.keyword": output}},
                      "aggs": {
                        "sum_bytes": {
                           "sum": {"field": "output_dataset.bytes"}
                        },
                        "sum_events": {
                          "sum": {"field": "output_dataset.events"}
                        },
                        "task": {
                          "reverse_nested": {},
                          "aggs": {
                            "input_bytes": {
                              "sum": {"field": "input_bytes"}
                            },
                            "input_events": {
                              "sum": {"field": "requested_events"}
                            },
                            "ids": {
                              "terms" : {
                                "field" : "_uid",
                                "size": 100
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
    aggregs = es_search.update_from_dict(query)
    exexute =  aggregs.execute()
    total = exexute.aggregations.output.not_deleted.format.doc_count
    result_events = exexute.aggregations.output.not_deleted.format.sum_events.value
    result_bytes = exexute.aggregations.output.not_deleted.format.sum_bytes.value
    input_bytes = exexute.aggregations.output.not_deleted.format.task.input_bytes.value
    input_events = exexute.aggregations.output.not_deleted.format.task.input_events.value
    ratio = 0
    if input_bytes != 0:
        ratio = float(result_bytes)/float(input_bytes)
    events_ratio = 0
    if input_events != 0:
        events_ratio = float(result_events)/float(input_events)
    return {'total':total,'ratio':ratio,'events_ratio':events_ratio}

def deriv_formats_new(search_dict):
    formats = get_format_by_request(search_dict)
    formats_dict = {}
    for format in formats:
        formats_dict.update( { format: {
                                         {"term": {"output_dataset.data_format.keyword": format}}
                                }

                              })
    return formats_dict
#
def statistic_by_request_deriv_new(search_dict, formats):

    total_result = {}
    for format in formats:
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        search_dict,
                        {"nested": {
                            "path": "output_dataset",
                            "query": {"term": {"output_dataset.data_format.keyword": format}}
                        }},
                        {"bool": {"must_not": [{"terms": {"status": ["aborted", "failed", "broken", "obsolete"]}}]}},
                    ]
                }
            },

            "aggs": {
                "amitag": {
                    "terms": {"field": "ctag"},
                    "aggs": {
                        "input_events": {
                            "sum": {"field": "input_events"}
                        },
                        "not_deleted": {
                            "filter": {"term": {"primary_input_deleted": False}},
                            "aggs": {
                                "input_bytes": {
                                    "sum": {"field": "input_bytes"}
                                },
                                "input_events_datasets": {
                                    "sum": {"field": "primary_input_events"}
                                }
                            }
                        },
                        "processed_events": {
                            "sum": {"field": "processed_events"}
                        },
                        "cpu_total": {
                            "avg": {"field": "hs06"}
                        },
                        "cpu_total_hs06": {
                            "filter":
                                {"bool":{"must":[ {"exists": {"field": "toths06"}}]}},
                            "aggs": {
                                "cpu_total_hs06": {
                                    "sum": {"field": "toths06"}
                                }
                            }

                        },
                        "total_events": {
                            "sum": {"field": "total_events"}
                        },
                        # "cpu_failed": {
                        #   "sum": {"field": "toths06_failed"}
                        # },
                        "timestamp_defined": {
                            "filter": {
                                "bool": {
                                    "must": [
                                        {"exists": {"field": "start_time"}},
                                        {"exists": {"field": "end_time"}},
                                        # {"script": {"script": {"source":"doc['end_time'].isAfter(doc['start_time'])"}}}
                                    ]
                                }
                            },
                            "aggs": {
                                "walltime": {
                                    "avg": {"script": {
                                        "inline": DEFAULT_SEARCH['task_duration_script']}}
                                    #  "avg": {"field": "hs06"}
                                }
                            }
                        },
                        "output": {
                            "nested": {"path": "output_dataset"},
                            "aggs": {
                                "not_removed": {
                                    "filter": {
                                    "bool" :{
                                        "filter": [{"term": {"output_dataset.deleted": False}},
                                                    {"term": {"output_dataset.data_format.keyword": format}} ]
                                    }},


                                    "aggs": {
                                        "bytes": {
                                            "sum": {"field": "output_dataset.bytes"}
                                        },
                                        "events": {
                                            "sum": {"field": "output_dataset.events"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        es_search = DEFAULT_SEARCH['search'](**DEFAULT_SEARCH['prod'])

        result = {}

        try:
            aggregs = es_search.update_from_dict(query)
            execute = aggregs.execute()
        except Exception as e:
            print("Problem with es deriv : %s" % (e))
            aggregs = None
        if aggregs and execute.aggregations:
                    for x in execute.aggregations.amitag.buckets:
                        if x.timestamp_defined.walltime.value:
                            duration = float(x.timestamp_defined.walltime.value)/(3600.0*1000*24)
                        else:
                            duration = None
                        cpu_total = 0
                        if x.cpu_total.value:
                            cpu_total = x.cpu_total.value
                            if x.cpu_total_hs06.doc_count > 0:
                                cpu_total_adj = x.cpu_total_hs06.cpu_total_hs06.value * x.doc_count / (x.cpu_total_hs06.doc_count*x.input_events.value)
                                if cpu_total_adj * 100 < cpu_total:
                                    cpu_total = (cpu_total/1000)
                                    if cpu_total < 1:
                                        cpu_total = 1
                        input_events = x.input_events.value
                        # if x.not_deleted.input_events_datasets.value and input_events and (x.not_deleted.input_events_datasets.value>input_events):
                        #     input_events = x.not_deleted.input_events_datasets.value

                        result[format+' '+x.key] = {'name':format+' '+x.key,  'total_events':x.total_events.value,
                                   'input_events': input_events,
                                   'input_bytes': x.not_deleted.input_bytes.value, 'input_not_removed_tasks': x.not_deleted.doc_count,
                                   'output_bytes':x.output.not_removed.bytes.value,
                                   'output_not_removed_tasks':x.output.not_removed.doc_count, 'processed_events': x.processed_events.value,
                                   'total_tasks': x.doc_count, 'hs06':int(cpu_total), 'duration':duration, 'dataset_output_events':x.output.not_removed.events.value}
        total_result.update(result)

    return total_result
