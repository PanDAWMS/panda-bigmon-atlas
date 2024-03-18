

from opensearchpy import OpenSearch, Search
from atlas.settings.local import DKB_OS2
OS2 = OpenSearch(hosts=DKB_OS2['hosts'], http_auth=(DKB_OS2['login'], DKB_OS2['password']), verify_certs=DKB_OS2['verify_certs'], ca_certs=DKB_OS2['ca_cert'])
DKB_OS2_SEARCH = {'search': Search, 'prod': {'using':OS2,  'index':"tasks_production", 'doc_type':'task'},
                  'analy': Search(using=OS2, index="tasks_analysis", doc_type='task'),
                  'task_duration_script':"ChronoUnit.MILLIS.between(doc['start_time'].value, doc['end_time'].value)"}

