

from opensearchpy import OpenSearch, Search
from django.conf import settings
OS2 = OpenSearch(hosts=settings.DKB_OS2['hosts'], http_auth=(settings.DKB_OS2['login'], settings.DKB_OS2['password']), verify_certs=settings.DKB_OS2['verify_certs'], ca_certs=settings.DKB_OS2['ca_cert'])
DKB_OS2_SEARCH = {'search': Search, 'prod': {'using':OS2,  'index':"tasks_production", 'doc_type':'task'},
                  'analy': Search(using=OS2, index="tasks_analysis", doc_type='task'),
                  'task_duration_script':"ChronoUnit.MILLIS.between(doc['start_time'].value, doc['end_time'].value)"}

