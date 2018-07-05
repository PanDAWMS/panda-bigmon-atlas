import json
from django.http import HttpResponseForbidden
import logging

from atlas.art.models import PackageTest, TestsInTasks
from atlas.prodtask.models import ProductionTask
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
                                          }
                                        }, 'size':SIZE_TO_DISPLAY
    })

    return query


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
    aggregs =  es_search.update_from_dict(query)
    return aggregs.execute()

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