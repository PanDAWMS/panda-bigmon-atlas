from datetime import timedelta
from random import sample

from atlas.settings.local import MONIT_ES
from dataclasses import dataclass
from opensearchpy import OpenSearch, connections, Search

from atlas.prestage.views import convert_input_to_physical_tape
from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import ProductionTask, ActionStaging, TTask, TemplateVariable

MONIT_SEARCH = OpenSearch(hosts=MONIT_ES['hosts'],http_auth=(MONIT_ES['login'], MONIT_ES['password']), verify_certs=MONIT_ES['verify_certs'], ca_certs=MONIT_ES['ca_cert'], timeout=5000)

MONIT_DDM_INDEX = 'monit_prod_ddm_enr_*'


@dataclass
class StagedTaskInfo:
    spans: list
    done_attempts: [int]
    failed_attempts_after_success: int
    done_attempts_after_success: int
    files_staged: int
    task: int
    dataset: str
    source: str
    date_since: str
    date_until: str
    total_files: int
    total_attempts: int

def prepare_staging_task_info(task_id: int, dataset: str| None = None, source: str | None = None):
    SPANS_LIMIT = 1400

    task = ProductionTask.objects.get(id=task_id)
    ddm = DDM()
    src_tape = None

    if not dataset:
        action_stage = ActionStaging.objects.filter(task=task_id)
        if not action_stage:
            raise Exception(f"Task {task_id} has no staging actions")
        action = action_stage[0]
        staging = action.dataset_stage
        dataset = staging.dataset
        if not source:
            source = staging.source
    else:
        jedi_task = TTask.objects.get(id=task.id)
        input_datasets = []
        if TemplateVariable.KEY_NAMES.INPUT_DS in jedi_task.jedi_task_parameters:
            input_container = jedi_task.jedi_task_parameters[TemplateVariable.KEY_NAMES.INPUT_DS]
            if ',' in input_container:
                input_datasets = input_container.split(',')
            else:
                input_datasets = ddm.dataset_in_container(input_container)
        if dataset not in input_datasets:
            raise ValueError(f'{dataset} is not in input datasets of task {task_id}')
        if not source:
            raise ValueError('source is not provided')
    tape_replicas = ddm.full_replicas_per_type(dataset)['tape']
    for replica in tape_replicas:
        if convert_input_to_physical_tape(replica['rse']).startswith(source):
            src_tape = replica['rse']
            break
    if not src_tape:
        raise ValueError(f'{dataset} tape replica is not found')
    date_since = (task.submit_time - timedelta(hours=10)).strftime("%Y-%m-%d")
    if task.status not in ProductionTask.NOT_RUNNING:
        date_until = "now+1d/d"
    else:
        date_until = (task.submit_time + timedelta(days=7)).strftime("%Y-%m-%d")
    files = ddm.list_files(dataset)
    if files[0]['name'].startswith('data'):
        name_prefix = '.'.join(files[0]['name'].split('.')[0:5]) + '.'
    else:
        name_prefix = '.'.join(files[0]['name'].split('.')[0:2]) + '.'
    os_data = get_staged_task_info(name_prefix, src_tape, date_since, date_until)
    per_file_stats = {}
    total_attempts = len(os_data)
    for hit in os_data:
        if hit['_source']['data']['name'] not in per_file_stats:
            per_file_stats[hit['_source']['data']['name']] = []
        per_file_stats[hit['_source']['data']['name']].append(hit['_source']['data'])
    done_attempts = [0 for _ in range(1000)]
    failed_attempts_after_success = 0
    done_attempts_after_success = 0
    spans = []
    sample_files = list(per_file_stats.keys())
    if len(sample_files) > 300:
        sample_files  = sample(sample_files, 300)
    for file in per_file_stats:
        stats = per_file_stats[file]
        stats.sort(key=lambda x: x['submitted_at'])
        successful_transfer = False
        current_spans = []
        is_really_bad = False
        for attempt, stat in enumerate(stats):
            if successful_transfer and stat['event_type'] == 'transfer-done':
                current_spans.append({'x': file, 'y': [stat['created_at'], stat['transferred_at']], 'fillColor': '#775DD0'})
                done_attempts_after_success += 1
                is_really_bad = True
            if successful_transfer and stat['event_type'] == 'transfer-failed':
                current_spans.append({'x': file, 'y': [stat['created_at'], stat['transferred_at']], 'fillColor': '#6c3483'})
                is_really_bad = True
                failed_attempts_after_success += 1
            if not successful_transfer and stat['event_type'] == 'transfer-done':
                current_spans.append({'x': file, 'y': [stat['created_at'], stat['submitted_at']], 'fillColor': '#f7dc6f'})
                current_spans.append({'x': file, 'y': [stat['submitted_at'], stat['transferred_at']], 'fillColor': '#00E396'})
                successful_transfer = True
                done_attempts[attempt] += 1
            if not successful_transfer and stat['event_type'] == 'transfer-failed':
                current_spans.append({'x': file, 'y': [stat['created_at'], stat['transferred_at']], 'fillColor': '#FF4560'})
        if (len(spans) + len(current_spans) < SPANS_LIMIT) and (is_really_bad or file in sample_files):
            spans += current_spans
    if sum(done_attempts) > 0:
        done_attempts = done_attempts[:[i for i, e in enumerate(done_attempts) if e != 0][-1]+1]
    files_staged = sum(done_attempts)
    result = StagedTaskInfo(spans=spans, done_attempts=done_attempts, failed_attempts_after_success=failed_attempts_after_success,
                            done_attempts_after_success=done_attempts_after_success, files_staged=files_staged, task=task_id,
                            dataset=dataset, source=source, date_since=date_since, date_until=date_until, total_files=len(files),
                            total_attempts=total_attempts)
    return result



def get_staged_task_info(name_prefix: str, src_tape: str, date_since: str, date_until: str):



    body = {"size":"10000",
            "_source": ["data.created_at", "data.event_timestamp", "data.event_type", "data.name", "data.started_at", "data.submitted_at",
                        "data.transferred_at"],
  "query": {
    "bool": {
      "must": [
        {
          "prefix": {
            "data.name": name_prefix
          }
        },
        {
          "range": {
            "metadata.timestamp": {
              "gte": date_since,
              "lt": date_until
            }
          }
        },
        {
          "match": {
            "data.src_endpoint": src_tape
          }
        }
      ]
    }
  }
}

    first_bunch = MONIT_SEARCH.search(index=MONIT_DDM_INDEX, body=body, scroll='1m' )
    total = []
    scroll_id = first_bunch['_scroll_id']
    for hit in first_bunch['hits']['hits']:
        total.append(hit)
    while True:
        response = MONIT_SEARCH.scroll(scroll_id=scroll_id, scroll='1m')
        scroll_id = response['_scroll_id']
        if len(response['hits']['hits']) == 0:
            break
        for hit in response['hits']['hits']:
            total.append(hit)

    return total