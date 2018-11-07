import json
from django.http import HttpResponseForbidden
import logging

from atlas.art.models import PackageTest, TestsInTasks
from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import ProductionTask, StepTemplate, MCPriority
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authentication import TokenAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser
from django.shortcuts import render


from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections
from atlas.settings.local import ESLogin

connections.create_connection(hosts=['http://aiatlas171.cern.ch:9200'], http_auth=(ESLogin['login'],ESLogin['password']), timeout=500)
_logger = logging.getLogger('prodtaskwebui')

SIZE_TO_DISPLAY = 2000

def test_connection():
    return Search(index="prodsys", doc_type='MC16')


@api_view(['POST'])
def es_task_search_analy(request):
    search_string = json.loads(request.body)
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
        response = keyword_search2(key_string_from_input(search_string)['query_string'], search_value).execute()
        total += response.hits.total
        for hit in response:
            current_hit = hit.to_dict()
            current_hit['output_dataset'] = []
            for hit2 in hit.meta.inner_hits['output_dataset']:
                current_hit['output_dataset'].append(hit2.to_dict())
            result.append(current_hit)
    return result, total

@api_view(['POST'])
def es_task_search(request):
    search_string = json.loads(request.body)
    result, total = es_task_search_all(search_string, 'prod')
    return Response({'tasks':result,'total':total})


def hits_to_tasks(hits):
    result = []
    for hit in hits:
        current_hit = hit.to_dict()
#        current_hit['output_dataset'] = []
#        for hit2 in hit.meta.inner_hits['output_dataset']:
#            current_hit['output_dataset'].append(hit2.to_dict())
        result.append(current_hit)
    return result


@api_view(['POST'])
def search_string_to_url(request):
    search_string = json.loads(request.body)
    return Response(key_string_from_input(search_string))

def key_string_from_input(search_string):
    prepared_strings = search_string['search_string'].replace('\n',',').replace('\r',',').replace('\t',',').replace(' ',',').split(',')
    query_string = ' AND '.join(['"'+x+'"' for x in prepared_strings if x])
    url = ','.join([x for x in prepared_strings if x])
    return {'url':url,'query_string':query_string}

def index(request):
    if request.method == 'GET':

        return render(request, 'dkb/_index_dkb.html', {
                'active_app': 'dkb',
                'pre_form_text': 'DKB',
                'title': 'DKB search',
                'parent_template': 'prodtask/_index.html',
            })


def index2(request):
    if request.method == 'GET':

        return render(request, 'dkb/_index_dkb2.html', {
                'active_app': 'dkb',
                'pre_form_text': 'DKB',
                'title': 'DKB search',
                'parent_template': 'prodtask/_index.html',
            })

@api_view(['GET'])
def test_name(request):
    print request.user.username
    return Response(request.user.username)


@api_view(['GET'])
def task_tree(request, task_id):
    result_tree = []
    task_info = []
    return Response(request.user.username)


def built_task_tree():
    pass

def es_by_field(field, value):
    es_search = Search(index="test_prodsys_rucio_ami", doc_type='task')
    query = {
        "query": {
            "bool": {
                    "must": {
                                "term": { field : value}
                            }

                        # {"has_child": {
                        #     "type": "output_dataset",
                        #     "score_mode": "sum",
                        #     "query": {
                        #         "match_all": {}
                        #     },
                        #     "inner_hits": {}
                        # }}]

            }
        }, 'size':2000
    }
    search = es_search.update_from_dict(query)
    return search.execute()



def keyword_search2(keyword_string, is_analy=False):
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
        es_search = Search(index="analysis", doc_type='task')
    else:
        es_search = Search(index="test_prodsys_rucio_ami", doc_type='task')
    query = es_search.update_from_dict({"query": {
                                          "bool": {
                                            "must":query_string+ [
                                            { "has_child": {
                                                "type": "output_dataset",
                                                "score_mode": "sum",
                                                "query": {
                                                    "match_all": {}
                                                },
                                                "inner_hits": {"size":20}

                                            }}]
                                          },

                                        }, 'size':SIZE_TO_DISPLAY
    })

    return query


def new_derivation_stat(project, ami, output):
    es_search = Search(index="test_prodsys_rucio_ami", doc_type='task')
    query = {
      "size": 0,
      "query": {
        "bool": {
          "must": [
            {"term": {"primary_input": "aod"}},
            {"term": {"project": project.lower()}},
            {"terms": {"ctag": ami}},
            {"term": {"status": "done"}},
            {"range": {"input_bytes": {"gt": 0}}},
            {"has_child": {
                "type": "output_dataset",
                "query" : {
                  "bool": {
                    "must": [
                      {"term": {"data_format": output}},
                      {"range": {"bytes": {"gt": 0}}}
                    ]
                  }
                }
            }}
          ]
        }
      },
      "aggs": {
        "input_bytes": {
           "sum": {"field": "input_bytes"}
        },
        "input_events": {
           "sum": {"field": "requested_events"}
        },
        "output_datasets": {
          "children": {"type": "output_dataset"},
          "aggs": {
            "not_removed": {
              "filter": {"term": {"deleted": False}},
              "aggs": {
                "format": {
                  "filter": {"term": {"data_format":output}},
                  "aggs": {
                    "sum_bytes": {
                      "sum": {"field": "bytes"}
                    },
                    "sum_events": {
                      "sum": {"field": "events"}
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
    try:
        total = exexute.hits.total
        #print aggregs['aggregations'].__dict__['output_datasets']['not_removed']['format']['sum_events']['value'].__dict__
        result_events = exexute.aggregations.output_datasets.not_removed.format.sum_events.value
        result_bytes = exexute.aggregations.output_datasets.not_removed.format.sum_bytes.value
        input_bytes = exexute.aggregations.input_bytes.value
        input_events =  exexute.aggregations.input_events.value
        ratio = 0
        if input_bytes != 0:
            ratio = float(result_bytes)/float(input_bytes)
        events_ratio = 0
        if input_events != 0:
            events_ratio = float(result_events)/float(input_events)
        return {'total':total,'ratio':ratio,'events_ratio':events_ratio}
    except:
        return {'total': 0, 'ratio': 0, 'events_ratio': 0}



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
    exexute =  aggregs.execute()
    return exexute



def running_events_stat(hashtags, status):

    es_search = Search(index="test_prodsys_rucio_ami", doc_type='task')
    query = {
              "size": 0,
              "query": {
                "bool": {
                  "must": [
                    {"terms": {"hashtag_list": hashtags}},
                    {"terms": {"status": status}}
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
                    "total_events": {
                          "sum": {"field": "total_events"}
                     },
                    "status": {
                      "terms": {"field": "status"}
                    }
                  }
                }
              }
            }
    aggregs = es_search.update_from_dict(query)
    exexute = aggregs.execute()
    result = {}
    if exexute.hits.total>0:
        for x in exexute.aggs.steps.buckets:
            result[x.key] = {'name': x.key, 'total_events': x.total_events.value,
                       'input_events': x.input_events.value,
                       'total_tasks': x.doc_count}

    return result

def statistic_by_step(hashtag):
    es_search = Search(index="test_prodsys_rucio_ami", doc_type='task')
    query = {
              "size": 0,
              "query": {
                "bool": {
                  "must": [
                    {"terms": {"hashtag_list": hashtag}},
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
                    # "processed_events": {
                    #   "sum": {"field": "processed_events"}
                    # # },
                    # "requested_events": {
                    #       "sum": {"field": "requested_events"}
                    # },
                    "total_events": {
                          "sum": {"field": "total_events"}
                     },
                      "hs06":{
                          "sum": {
                              "script": {
                                  "inline": "doc['hs06'].value*doc['total_events'].value"
                              }
                          }
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
                      "children": {"type": "output_dataset"},
                      "aggs": {
                        "not_removed": {
                          "filter": {"term": {"deleted": False}},
                          "aggs": {
                            "bytes": {
                              "sum": {"field": "bytes"}
                            }
                          }
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
    aggregs = es_search.update_from_dict(query)
    exexute = aggregs.execute()
    result = {}
    if exexute.hits.total>0:
        for x in exexute.aggs.steps.buckets:
            result[x.key] = {'name': x.key,  'total_events': x.total_events.value,
                       'input_events': x.input_events.value,
                       'input_bytes': x.not_deleted.input_bytes.value, 'input_not_removed_tasks': x.not_deleted.doc_count,
                       'output_bytes':x.output.not_removed.bytes.value,
                       'output_not_removed_tasks':x.output.not_removed.doc_count,
                       'total_tasks': x.doc_count, 'hs06':x.hs06.value, 'duration':float(x.ended.duration.value)/(3600.0*1000*24)}

    return result


@api_view(['POST'])
def step_hashtag_stat(request):
    try:
        hashtags_raw = request.body
        hashtags_split = hashtags_raw.replace('&',',').replace('|',',').split(',')
        hashtags = [x.lower() for x in hashtags_split if x]
        statistics = statistic_by_step(hashtags)
        running_stat = running_events_stat(hashtags,['running'])
        finished_stat = running_events_stat(hashtags,['finished','done'])
        result = []
        for step in MCPriority.STEPS:
            if step in statistics:
                current_stat = statistics[step]
                percent_done = 0.0
                percent_runnning = 0.0
                percent_pending = 0.0
                if current_stat["input_events"] == 0:
                    step_status = 'Unknown'
                else:
                    percent_done = float(current_stat['total_events']) / float(current_stat['input_events'])
                    if (percent_done>0.90):
                       step_status = 'StepDone'
                    elif (percent_done>0.10):
                       step_status = 'StepProgressing'
                    else:
                       step_status =  'StepNotStarted'
                    running_events = 0
                    finished_events = 0
                    if step in running_stat:
                        running_events = running_stat[step]['input_events']-running_stat[step]['total_events']
                    if step in finished_stat:
                        finished_events = finished_stat[step]['input_events'] - finished_stat[step]['total_events']
                    percent_runnning = float(running_events) / float(current_stat['input_events'])
                    percent_pending = float(current_stat['input_events'] - current_stat['total_events'] -running_events - finished_events) / float(current_stat['input_events'])
                    if percent_pending < 0:
                        percent_pending = 0
                current_stat['step_status'] = step_status
                current_stat['percent_done'] = percent_done*100
                current_stat['percent_runnning'] = percent_runnning*100
                current_stat['percent_pending'] = percent_pending*100
                result.append(current_stat)
    except Exception,e:
        return Response({'error':str(e)},status=400)
    return Response(result)




def derivation_stat(project, ami, output):
    es_search = Search(index="test_prodsys_rucio_ami", doc_type='task')

    query2 = {
          "_source": ["primary_input","taskid","processed_events","input_bytes","requested_events"],
          "query": {
            "bool": {
              "must": [
                {"term": {"project": project.lower()}},
                {"terms": {"ctag": ami}},
                {"term": {"output_formats": output}},
                 {"terms": {"status": ["done"]}},
                {"has_child": {
                      "type": "output_dataset",
                      "score_mode": "sum",
                      "query": {
                          "term": {"data_format":output.upper()}
                      }, "inner_hits": {"size":20}
                  }}
               ]
             }
           }, 'size':2000
        }
    aggregs = es_search.update_from_dict(query2)
    exexute =  aggregs.execute()
    result_tasks = []
    for hit in exexute:
        if hasattr(hit, 'input_bytes'):
            input_bytes = hit.input_bytes
        else:
            input_bytes = 0
        for hit2 in hit.meta.inner_hits['output_dataset']:
            try:
                if not hit2.deleted:
                    if hasattr(hit2, 'events'):
                        events = hit2.events
                    else:
                        events = -1
                    if hit2.bytes > 0:
                        result_tasks.append({'primary_input':hit.primary_input,'output_bytes':hit2.bytes,
                                             'task_id':hit.taskid,'dataset_name':hit2.datasetname, 'input_bytes':input_bytes,
                                             'input_events':hit.requested_events,'events':events})
            except Exception,e:
                print str(e)
    return result_tasks

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
        current_input_tasks = new_derivation_stat(project, ami_tags, output)
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


def summary():
    es_search = Search(index="prodsys_rucio_ami", doc_type='task')

    query = {
        "query": {
               "bool": {
                 "must": [
                   { "term": { "status": "done" } }
                 ],
                 "should": [
                   { "term": { "hashtag_list": "MC16a_CP"} }
                 ]
               }
             },
             "aggs": {
               "category": {
                 "terms": {"field": "phys_category"},
                 "aggs": {
                   "step": {
                     "terms": {
                       "field": "step_name.keyword"
                     },
                     "aggs": {
                       "requested": {
                         "sum": {
                           "field": "requested_events"
                         }
                       },
                       "processed": {
                         "sum": {"field": "processed_events"}
                       }
                     }
                   }
                 }
               }
             }
            }


def keyword_search(keyword_string):
    es_search = Search(index="prodsys", doc_type='MC16')
    query = es_search.update_from_dict({'query':{'query_string':{"query": keyword_string,"analyze_wildcard":True}}})
    return query



@api_view(['POST'])
def tasks_from_list(request):
    result_tasks_list = []
    try:
        input_str = json.loads(request.body)
        result_tasks_list = map(int, input_str['taskIDs'])
        request.session['selected_tasks'] =  result_tasks_list
    except Exception,e:
        return Response({'error':str(e)},status=400)
    return Response(result_tasks_list)

@api_view(['GET'])
def deriv_output_proportion(request,project,ami_tag):
    try:
        result = count_output_stat(project,[x for x in ami_tag.split(',') if x])
    except Exception,e:
            print str(e)
            return Response({'error':str(e)},status=400)
    return Response(result)