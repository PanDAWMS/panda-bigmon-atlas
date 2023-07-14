import dataclasses
from dataclasses import dataclass

from elasticsearch7_dsl import Search, connections
from atlas.settings.local import ATLAS_ES

connections.create_connection(hosts=ATLAS_ES['hosts'],http_auth=(ATLAS_ES['login'], ATLAS_ES['password']), verify_certs=ATLAS_ES['verify_certs'])

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