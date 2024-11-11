from atlas.ami.client import AMIClient
from atlas.celerybackend.celery import app
from atlas.deftcore.taskdef import TaskDefinition


@app.task(ignore_result=True, time_limit=24*60*60*3)
def submit_request(request_id):
    task_definition = TaskDefinition()
    return task_definition.force_process_requests(request_id)

@app.task(ignore_result=True, time_limit=24*60*60*3)
def submit_requests_by_type(request_type):
    task_definition = TaskDefinition()
    return task_definition.process_requests(request_types=[request_type])

@app.task(ignore_result=True)
def sync_ami_projects():
    ami_client = AMIClient()
    ami_client.sync_ami_projects()
    return None

@app.task(ignore_result=True)
def sync_ami_types():
    ami_client = AMIClient()
    ami_client.sync_ami_types()
    return None

@app.task(ignore_result=True)
def sync_ami_phys_containers():
    ami_client = AMIClient()
    ami_client.sync_ami_phys_containers()
    return None

@app.task(ignore_result=True)
def sync_ami_tags():
    ami_client = AMIClient()
    ami_client.sync_ami_tags()
    return None
