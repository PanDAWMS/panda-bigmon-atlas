from elasticsearch7_dsl import Search, connections
from elasticsearch7 import Elasticsearch
from atlas.settings.local import DKB_ES7

connections.create_connection(hosts=DKB_ES7['hosts'],http_auth=(DKB_ES7['login'], DKB_ES7['password']), verify_certs=DKB_ES7['verify_certs'], ca_certs=DKB_ES7['ca_cert'])
ES7 = Elasticsearch(hosts=DKB_ES7['hosts'],http_auth=(DKB_ES7['login'], DKB_ES7['password']), verify_certs=DKB_ES7['verify_certs'], ca_certs=DKB_ES7['ca_cert'])

DKB_ES7_SEARCH = {'search': Search, 'prod': {'using':ES7,  'index':"tasks_production", 'doc_type':'task'},
                  'analy': Search(using=ES7, index="tasks_analysis", doc_type='task'),
                  'task_duration_script':"ChronoUnit.MILLIS.between(doc['start_time'].value, doc['end_time'].value)"}
