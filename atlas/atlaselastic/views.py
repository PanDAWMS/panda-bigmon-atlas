import dataclasses
from dataclasses import dataclass

from elasticsearch7_dsl import Search, connections
from atlas.settings.local import ATLAS_ES

connections.create_connection(hosts=ATLAS_ES['hosts'],http_auth=(ATLAS_ES['login'], ATLAS_ES['password']), verify_certs=ATLAS_ES['verify_certs'])

class LogsName():
    TASK_ACTIONS = "task_action.task_management"

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


def get_atlas_es_logs_base(logName: str) -> Search:
    return Search(index='atlas_prodsyslogs-*').query("match", logName=logName)

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