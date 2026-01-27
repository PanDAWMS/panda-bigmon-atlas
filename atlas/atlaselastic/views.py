import dataclasses
import json
from dataclasses import dataclass
from re import search
from typing import List

from atlas.settings.local import ATLAS_OS
from opensearchpy import OpenSearch, connections, Search

connections.create_connection(hosts=ATLAS_OS['hosts'],http_auth=(ATLAS_OS['login'], ATLAS_OS['password']), verify_certs=ATLAS_OS['verify_certs'],
                               ca_certs=ATLAS_OS['ca_cert'])
ATLAS_ES7 = OpenSearch(hosts=ATLAS_OS['hosts'],http_auth=(ATLAS_OS['login'], ATLAS_OS['password']), verify_certs=ATLAS_OS['verify_certs'], ca_certs=ATLAS_OS['ca_cert'], timeout=5000)


class LogsName():
    TASK_ACTIONS = "task_action.task_management"
    PRESTAGE_LOGS = "prestage.views"

@dataclass
class TaskActionLogMessage():
    task_id: int
    action: str
    return_message: str = ''
    return_code: str = ''
    username: str = ''
    params: str = ''
    timestamp: str = ''
    comment: str = ''
    dataset: str = ''

@dataclass
class TaskDatasetStats():
    task_id: int
    dataset_id: int
    dataset: str
    type: str
    task_hs06sec_finished: int = 0
    task_hs06sec_failed: int =0
    bytes: int = 0
    events: int = 0

def get_atlas_es_logs_base(logName: str) -> Search:
    return Search(index='atlas_prodsyslogs-*').extra(size=100).query("match", logName=logName)

def get_atlas_dataset_info_base() -> Search:
    return Search(index='atlas_datasets_info-*').extra(size=100)

def get_panda_logs_base(logName: str) -> Search:
    return Search(index='atlas_pandalogs-*').extra(size=100)

def get_lost_files_recovery_logs(task_id: int) -> List[str]:
    search = get_panda_logs_base('panda.log.recover_lost_files').query("match",jediTaskID=task_id).sort('-@timestamp')
    response = search.execute()
    result = []
    for hit in response:
        result.append(f"{hit['@timestamp']} - {hit.message}")
    return result



def get_rule_action_logs(dataset: str) -> any:
    search = get_atlas_es_logs_base(LogsName.TASK_ACTIONS).query("match_phrase",dataset=dataset)
    response = search.execute()
    result = []
    for hit in response:
        return_message = ''
        try:
            return_message = hit.return_message
        except:
            pass
        result.append(dataclasses.asdict(TaskActionLogMessage(task_id=0,
                                                              dataset=dataset,
                                                                action=hit.action,
                                                                return_message=return_message,
                                                              username=hit.user,
                                                              timestamp=hit['@timestamp'],
                                                              params=str(hit.params),
                                                              return_code=str(hit.return_code),
                                                              comment=hit.comment
                                                              )))
        result.sort(key=lambda x: x['timestamp'])
        result.reverse()
    return result

def get_tasks_action_logs(task_id: int) -> any:
    search = get_atlas_es_logs_base(LogsName.TASK_ACTIONS).query("match",task=str(task_id))
    response = search.execute()
    result = []
    for hit in response:
        return_message = ''
        try:
            return_message = hit.return_message
        except:
            pass
        result.append(dataclasses.asdict(TaskActionLogMessage(task_id=int(hit.task),
                                                                action=hit.action,
                                                                return_message=return_message,
                                                              username=hit.user,
                                                              timestamp=hit['@timestamp'],
                                                              params=str(hit.params),
                                                              return_code=str(hit.return_code),
                                                              comment=hit.comment
                                                              )))
        result.sort(key=lambda x: x['timestamp'])
        result.reverse()
    return result

def get_all_actions_logs(action: str, days: int) -> any:
    search = get_atlas_es_logs_base(LogsName.TASK_ACTIONS).query("match",action=action).query("range", **{
                "@timestamp": {
                    "gte": f"now-{days}d/d",
                    "lte": "now/d"
                }}).extra(size=2000)
    response = search.execute()
    result = []
    for hit in response:
        return_message = ''
        try:
            return_message = hit.return_message
        except:
            pass
        result.append(dataclasses.asdict(TaskActionLogMessage(task_id=int(hit.task),
                                                                action=hit.action,
                                                                return_message=return_message,
                                                              username=hit.user,
                                                              timestamp=hit['@timestamp'],
                                                              params=str(hit.params),
                                                              return_code=str(hit.return_code),
                                                              comment=hit.comment
                                                              )))
        result.sort(key=lambda x: x['timestamp'])
        result.reverse()
    return result

def get_task_stats(task_id: int) -> [TaskDatasetStats]:
    search = get_atlas_dataset_info_base().query("match",jeditaskid=task_id).sort('-@timestamp')
    response = search.execute()
    result = []
    try:
        for hit in response:
            if hit.type in ['input','pseudo_input' ,'output']:
                result.append(TaskDatasetStats(task_id=hit.jeditaskid,
                                                                  dataset_id=hit.datasetid,
                                                                  dataset=hit.datasetname,
                                                                  type=hit.type,
                                                                  task_hs06sec_finished=hit.task_hs06sec_finished or 0,
                                                                  task_hs06sec_failed=hit.task_hs06sec_failed or 0,
                                                                  bytes=hit.dataset_size or 0,
                                                                  events=hit.nevents))
        return result
    except Exception as ex:
        return []


def get_staged_number(dataset: str, days: int) -> int:
    search = get_atlas_es_logs_base(LogsName.PRESTAGE_LOGS).query("match_phrase",dataset=dataset).query("range", **{
                "@timestamp": {
                    "gte": f"now-{days}d/d",
                    "lte": "now/d"
                }})

    return search.count()

def get_unique_runs() -> [str]:
    query = {
  "size": 0,
  "query": {
    "bool": {
      "must": [
        { "term": { "datatype.keyword": "EVNT" } },
        {
          "range": {
            "timestamp": {
              "gte": "now-7d/d",
              "lt": "now/d"
            }
          }
        },
        {
          "terms": {
            "project.keyword": [
              "mc16_13TeV",
              "mc15_13TeV",
              "mc15_pPb8TeV",
              "mc21_13p6TeV"
            ]
          }
        }
      ],
      "must_not": [
        { "term": { "stream_name.keyword": "NA" } }
      ]
    }
  },
  "aggs": {
    "total_run_numbers": {
      "cardinality": {
        "field": "run_number.keyword"
      }
    }
  }
}
    search = Search(index='atlas_ddm-global-accounting*').update_from_dict(query)
    response = search.execute()
    return response


def get_paginated_run_numbers( page_size=10000):
    """
    Queries Elasticsearch to retrieve all unique run_number.keyword values in a paginated manner.
    :param es_host: Elasticsearch host (e.g., "http://localhost:9200")
    :param index_name: Index pattern to search (e.g., "atlas_ddm-global-accounting*")
    :param page_size: Number of results per page
    :return: List of all unique run_numbers that meet the criteria
    """

    all_run_numbers = []
    after_key = None  # Used for pagination

    while True:
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"datatype.keyword": "EVNT"}},
                        {
                            "range": {
                                "timestamp": {
                                    "gte": "now-7d/d",
                                    "lt": "now/d"
                                }
                            }
                        },

                    ],
                    "must_not": [
                        {"term": {"stream_name.keyword": "NA"}}
                    ]
                }
            },
            "aggs": {
                "run_numbers": {
                    "composite": {
                        "size": page_size,
                        "sources": [
                            {"run_number": {"terms": {"field": "run_number.keyword"}}}
                        ]
                    },
                    "aggs": {
                        "unique_stream_names": {
                            "cardinality": {
                                "field": "stream_name.keyword"
                            }
                        },
                        "filtered_runs": {
                            "bucket_selector": {
                                "buckets_path": {
                                    "streamCount": "unique_stream_names"
                                },
                                "script": "params.streamCount > 1"
                            }
                        }
                    }
                }
            }
        }

        # If this is not the first request, add pagination key
        if after_key:
            query["aggs"]["run_numbers"]["composite"]["after"] = after_key

        response = Search(index='atlas_ddm-global-accounting*').update_from_dict(query).execute()

        # Extract run_numbers
        buckets = response["aggregations"]["run_numbers"]["buckets"]
        for bucket in buckets:
            all_run_numbers.append(bucket["key"]["run_number"])

        # Check if there is more data to fetch
        if "after_key" in response["aggregations"]["run_numbers"]:
            after_key = response["aggregations"]["run_numbers"]["after_key"]
        else:
            break  # No more data to fetch

    return all_run_numbers

def get_different_streams() -> [str]:
    query = {
  "size": 0,
  "query": {
    "bool": {
      "must": [
        { "term": { "datatype.keyword": "EVNT" } },
        {
          "range": {
            "timestamp": {
              "gte": "now-7d/d",
              "lt": "now/d"
            }
          }
        },
        {
          "terms": {
            "project.keyword": [
              "mc16_13TeV",
              "mc15_13TeV",
              "mc15_pPb8TeV",
              "mc21_13p6TeV"
            ]
          }
        }
      ],
      "must_not": [
        { "term": { "stream_name.keyword": "NA" } }
      ]
    }
  },
  "aggs": {
    "run_numbers": {
      "terms": {
        "field": "run_number.keyword",
        "size": 62000
      },
      "aggs": {
        "unique_stream_names": {
          "cardinality": {
            "field": "stream_name.keyword"
          }
        },
        "filtered_runs": {
          "bucket_selector": {
            "buckets_path": {
              "streamCount": "unique_stream_names"
            },
            "script": "params.streamCount > 1"
          }
        }
      }
    }
  }
}
    search = Search(index='atlas_ddm-global-accounting*').update_from_dict(query)
    response = search.execute()
    return response
def get_datasets_by_run(run_number: str) -> any:
    query = {
        "size": 0,  # We don't need document details, just aggregations
        "query": {
            "bool": {
                "must": [
                    {"term": {"run_number.keyword": run_number}},
                    {"term": {"datatype.keyword": "EVNT"}},
                    {
                        "range": {
                            "timestamp": {
                                "gte": "now-7d/d",
                                "lt": "now/d"
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "unique_names": {
                "terms": {
                    "field": "name.keyword",
                    "size": 10000  # Adjust this if needed for larger datasets
                }
            }
        }
    }

    response =  Search(index='atlas_ddm-global-accounting*').update_from_dict(query).execute()

    # Extract unique names from the response
    names = [bucket["key"] for bucket in response["aggregations"]["unique_names"]["buckets"]]
    return names


def get_datasets_without_campaign() -> [str]:


    query = """
    {
  "version": true,
  "size": 5000,
  "sort": [
    {
      "timestamp": {
        "order": "desc",
        "unmapped_type": "boolean"
      }
    }
  ],
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "timestamp",
        "fixed_interval": "5m",
        "time_zone": "Etc/UTC",
        "min_doc_count": 1
      }
    }
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {
    "time_since_accessed": {
      "script": {
        "source": "if (doc.containsKey('accessed_at') && !doc['accessed_at'].empty) {return (doc['timestamp'].value.getMillis() - doc['accessed_at'].value.getMillis())/86400000} else {return -1}",
        "lang": "painless"
      }
    },
    "gigabytes": {
      "script": {
        "source": "return doc['bytes'].value / 1000000000.0;",
        "lang": "painless"
      }
    },
    "disk_gigabytes": {
      "script": {
        "source": "return doc['disk_bytes'].value / 1000000000.0;",
        "lang": "painless"
      }
    },
    "disk_cost": {
      "script": {
        "source": "if (doc.containsKey('accessed_at') && !doc['accessed_at'].empty) {return (doc['timestamp'].value.getMillis() - doc['accessed_at'].value.getMillis())/86400000 * doc['disk_bytes'].value / 1000000000000.0} else {return -1}",
        "lang": "painless"
      }
    },
    "total_repl_factor": {
      "script": {
        "source": "if (doc['primary_repl_factor'].size() != 0 && doc['secondary_repl_factor'].size() != 0) {return doc['primary_repl_factor'].value + doc['secondary_repl_factor'].value} else {return 0}",
        "lang": "painless"
      }
    },
    "size_repl_factor": {
      "script": {
        "source": "float r;if (doc['bytes'].value > 0) {r = (float)doc['disk_bytes'].value / (float)doc['bytes'].value} else {tr = (float) 0} return r",
        "lang": "painless"
      }
    },
    "accessed_last_month": {
      "script": {
        "source": "if (doc.containsKey('accessed_at') && !doc['accessed_at'].empty) {   if ((doc['timestamp'].value.getMillis() - doc['accessed_at'].value.getMillis()) / 86400000 < 30)   {return 1} else {return 0}} else {   return 0}",
        "lang": "painless"
      }
    },
    "accessed_last_year": {
      "script": {
        "source": "if (doc.containsKey('accessed_at') && !doc['accessed_at'].empty) {   if ((doc['timestamp'].value.getMillis() - doc['accessed_at'].value.getMillis()) / 86400000 < 365) n   {return 1} else {return 0}} else {   return 0}",
        "lang": "painless"
      }
    },
    "accessed_last_week": {
      "script": {
        "source": "if (doc.containsKey('accessed_at') && !doc['accessed_at'].empty) {   if ((doc['timestamp'].value.getMillis() - doc['accessed_at'].value.getMillis()) / 86400000 <= 7)  {return 1} else {return 0}} else {  return 0}",
        "lang": "painless"
      }
    }
  },
  "docvalue_fields": [
    {
      "field": "accessed_at",
      "format": "date_time"
    },
    {
      "field": "created_at",
      "format": "date_time"
    },
    {
      "field": "timestamp",
      "format": "date_time"
    }
  ],
  "_source": {
    "excludes": []
  },
  "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "bool": {
            "minimum_should_match": 1,
            "should": [
              {
                "match_phrase": {
                  "datatype": "HITS"
                }
              },
              {
                "match_phrase": {
                  "datatype": "AOD"
                }
              }
            ]
          }
        },
        {
          "range": {
            "created_at": {
              "gte": "2017-12-31T23:00:00.000+00:00",
              "lt": "2023-06-05T22:00:00.000+00:00"
            }
          }
        },
        {
          "range": {
            "timestamp": {
              "gte": "2023-10-08T00:00:00.000Z",
              "lte": "2023-10-08T03:00:00.000Z",
              "format": "strict_date_optional_time"
            }
          }
        }
      ],
      "should": [],
      "must_not": [
        {
          "exists": {
            "field": "campaign"
          }
        }
      ]
    }
  },
  "highlight": {
    "pre_tags": [
      "@kibana-highlighted-field@"
    ],
    "post_tags": [
      "@/kibana-highlighted-field@"
    ],
    "fields": {
      "*": {}
    },
    "fragment_size": 2147483647
  }
}"""
    query1 = { "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "bool": {
            "minimum_should_match": 1,
            "should": [
              {
                "match_phrase": {
                  "datatype": "HITS"
                }
              },
              {
                "match_phrase": {
                  "datatype": "AOD"
                }
              },
                {
                    "match_phrase": {
                        "datatype": "EVNT"
                    }
                },
                {
                    "match_phrase": {
                        "datatype": "NTUP_PILEUP"
                    }
                },
            ]
          }
        },
        {
          "range": {
            "created_at": {
              "gte": "2017-12-31T23:00:00.000+00:00",
              "lt": "2023-06-05T22:00:00.000+00:00"
            }
          }
        },
        {
          "range": {
            "timestamp": {
              "gte": "2023-10-08T00:00:00.000Z",
              "lte": "2023-10-08T03:00:00.000Z",
              "format": "strict_date_optional_time"
            }
          }
        }
      ],
      "should": [],
      "must_not": [
        {
          "exists": {
            "field": "campaign"
          }
        }
      ]
    }
  }}
    search = Search(index='atlas_ddm-global-accounting*').extra(size=10000).update_from_dict(query1)
    response = search.execute()
    return response


def opendistro_sql(query: str, fetch_size = None) -> any:
    if fetch_size:
        return ATLAS_ES7.transport.perform_request('POST', '/_opendistro/_sql?format=jdbc', body={'fetch_size': fetch_size, 'query': query })
    return ATLAS_ES7.transport.perform_request('POST', '/_opendistro/_sql?format=json', body={'query': query})
#    return ATLAS_ES7.transport.perform_request('POST', '/_plugins/_sql?format=json', body={'query': query})

def opendistro_ppl(query: str) -> any:
    return ATLAS_ES7.transport.perform_request('POST', '/_plugins/_ppl', body={'query': query})
def opendistro_sql_translate(query: str, fetch_size = None) -> any:
    return ATLAS_ES7.transport.perform_request('POST', '/_plugins/_sql/_explain', body={'query': query})

# def get_campaign_nevents_per_amitag(campaign: str, suffix) -> any:
#     stats = {}
#     output = {'evgen': 'EVNT', 'simul': 'HITS', 'pile': 'AOD'}
#     for step in ['evgen','simul', 'pile']:
#         stats[step] = []
#         step_campaign = campaign
#         if suffix.get(step):
#             step_campaign = f"{campaign}{suffix.get(step)}"
#         query = (f"select task_amitag, scope, sum(nevents) from atlas_datasets_info-* where type='output' and task_campaign='{step_campaign}' "
#                  f"and task_processingtype='{step}' and dataset_format='{output[step]}'  group by task_amitag, scope")
#         result = opendistro_sql(query)
#         if result.get('status') == 200:
#             for row in result.get('datarows'):
#                 if row[2] > 0 and 'val' not in row[1]:
#                     stats[step].append({'tag': row[0], 'scope': row[1], 'nevents': row[2]})
#         else:
#             raise Exception(f"Error in query {query}, {str(result)}")
#     return stats

def get_campaign_nevents_per_amitag(campaign: str, suffix) -> any:
    stats = {}
    output = {'evgen': 'EVNT', 'simul': 'HITS', 'pile': 'AOD'}
    for step in ['evgen','simul', 'pile']:
        stats[step] = []
        step_campaign = campaign
        if suffix.get(step):
            step_campaign = f"{campaign}{suffix.get(step)}"

        # ppl_query = (f"source=atlas_datasets_info-* | where type='output' task_campaign='{step_campaign}' task_processingtype='{step}' dataset_format='{output[step]}' | "
        #              "  stats sum(nevents) by scope, task_amitag ")
        ppl_query = (
            f"source=atlas_datasets_info-* "
            f"| where type='output' "
            f"and task_campaign='{step_campaign}' "
            f"and task_processingtype='{step}' "
            f"and dataset_format='{output[step]}' "
            f"| stats sum(nevents) by scope, task_amitag"
        )
        result = opendistro_ppl(ppl_query)
        if result.get('schema'):
            for row in result.get('datarows'):
                if row[0] > 0 and 'val' not in row[1]:
                    stats[step].append({'tag': row[2], 'scope': row[1], 'nevents': row[0]})
        else:
            raise Exception(f"Error in query {ppl_query}, {str(result)}")
    return stats

def get_campaign_nevents_per_dsid(dsid) -> any:
    stats = {}
    output = {'evgen': 'EVNT', 'simul': 'HITS', 'pile': 'AOD'}
    for step in ['evgen','simul', 'pile']:


        # ppl_query = (f"source=atlas_datasets_info-* | where type='output' datasetname='%.{dsid}.%' task_processingtype='{step}' dataset_format='{output[step]}' | "
        #              "  stats sum(nevents) by scope, task_amitag, task_campaign ")
        ppl_query = (
            f"source=atlas_datasets_info-* "
            f"| where type='output' "
            f"and task_processingtype='{step}' "
            f"and dataset_format='{output[step]}' "
            f"and datasetname like '%.{dsid}.%' "
            f"| stats sum(nevents) by scope, task_amitag, task_campaign"
        )

        result = opendistro_ppl(ppl_query)
        if result.get('schema'):
            for row in result.get('datarows'):
                if row[0] > 0 and 'val' not in row[1]:
                    stats[step].append({'tag': row[2], 'scope': row[1], 'nevents': row[0]})
        else:
            raise Exception(f"Error in query {ppl_query}, {str(result)}")
    return stats
def get_tasks_ids(**kwargs) -> any:
    task_ids = []
     # Transform kwargs to pair list
    filters = []
    for key, value in kwargs.items():
        if type(value) == list:
            tokens = [f"{key}='{x}'" for x in value]
            filters.append(f"({' or '.join(tokens)})")
        else:
            filters.append(f"({key}='{value}')")
    query = f"select distinct jeditaskid from atlas_datasets_info-* where type='output' and {' and '.join(filters)} LIMIT 40000"

    result = opendistro_sql_translate(query)
    q = result['root']['children'][0]['children'][0]['description']['request'].split(', ')[1].strip('sourceBuilder=')
    json_query = json.loads(q)
    #json_query['aggregations']['composite_buckets']['composite']['size'] = 40000
    #pprint(json_query['query'])
    #return result['root']['children'][0]['children'][0]['description']['request']
    #search = Search(index='atlas_datasets_info-*').extra(size=40000).update_from_dict(json_query)
    #response = search.execute()
    doc_count = 0
    resp = ATLAS_ES7.search(index='atlas_datasets_info-*', body={"size":2000, "fields":['jeditaskid'], "_source": False,"query":json_query["query"]}, scroll='1m')
    result = [x['fields']['jeditaskid'][0] for x in resp['hits']['hits']]
    # keep track of pass scroll _id
    old_scroll_id = resp['_scroll_id']
    # use a 'while' iterator to loop over document 'hits'
    while len(resp['hits']['hits']):

        # make a request using the Scroll API
        resp = ATLAS_ES7.scroll(
            scroll_id=old_scroll_id,
            scroll='1m'  # length of time to keep search context
        )

        # iterate over the document hits for each 'scroll'
        # for doc in resp['hits']['hits']:
        #     doc_count += 1
        result.extend([x['fields']['jeditaskid'][0] for x in resp['hits']['hits']])
        old_scroll_id = resp['_scroll_id']

    return result

def get_analysis_tasks_with_AOD_input() -> any:
    query = {
              "size": 0,
              "query": {
                "bool": {
                  "must": [
                    { "term": { "task_type": "anal" } },
                    { "term": { "type": "input" } },
                    { "term": { "dataset_format": "aod" } },
                    {
                      "range": {
                        "@timestamp": {
                          "gte": "now-90d/d",
                          "lt": "now/d"
                        }
                      }
                    }
                  ]
                }
              },
              "aggs": {
                "distinct_jeditaskid": {
                  "terms": {
                    "field": "jeditaskid",
                    "size": 10000
                  }
                }
              }
            }

    search = get_atlas_dataset_info_base().update_from_dict(query)
    response = search.execute()
    result = []
    # for hit in response:
    #     result.append(hit.jeditaskid)
    for hit in response.aggregations.distinct_jeditaskid.buckets:
        result.append(hit.key)
    return result
