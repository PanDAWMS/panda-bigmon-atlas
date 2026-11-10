import logging

from atlas.prodtask.models import ProductionTask, VerifiedTaskConfig, VerifiedTaskConfigScout
from atlas.prodtask.models import HashTag
from atlas.prodtask.helper import form_json_request_dict
from atlas.celerybackend.celery import app

_jsonLogger = logging.getLogger('prodtask_ELK')

def get_or_create_verified_task_config(task_id, status):
    task = ProductionTask.objects.get(id=task_id)
    ami_tag = task.ami_tag
    outputs = task.output_formats
    input_data = task.input_dataset
    input_type = f'{VerifiedTaskConfig.convert_scope_to_input_types(input_data)}.{VerifiedTaskConfig.get_latest_tag(input_data)}'
    existing_check = None
    for check in VerifiedTaskConfig.objects.filter(ami_tag=ami_tag, input_types = input_type, output_formats = outputs):
        if set(check.output_formats) == set(outputs):
            existing_check = check
            break
    if not existing_check:
        existing_check = VerifiedTaskConfig.objects.create(ami_tag=ami_tag, input_types=input_type, output_formats=outputs, status=status)
    return existing_check

def fill_verified_from_existing_tasks(task_ids: list[int]):
    for task_id in task_ids:
        task = ProductionTask.objects.get(id=task_id)
        if task.status in [ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED] and 'valid' not in task.name:
            existing_check = get_or_create_verified_task_config(task_id, VerifiedTaskConfig.Status.VERIFIED)
            existing_check.status = VerifiedTaskConfig.Status.VERIFIED
            existing_check.save()

def register_scout_task(task_id):
    existing_check = get_or_create_verified_task_config(task_id, VerifiedTaskConfig.Status.ON_GOING)
    existing_check.add_scout_task(task_id)
    _jsonLogger.info(f'Registered scout task',extra=form_json_request_dict(ProductionTask.objects.get(id=task_id).request_id,None,{'task': task_id}))

def release_scout_requests(task_id) -> set[int]:
    scout_check = VerifiedTaskConfigScout.objects.get(task_id=task_id).config
    scout_check.status = VerifiedTaskConfig.Status.VERIFIED
    scout_check.save()
    hashtag = HashTag.objects.get(hashtag=VerifiedTaskConfig.ACTIVE_SCOUT_HASHTAG)
    requests_to_release = set()
    for task in scout_check.scout_tasks:
        task.remove_hashtag(hashtag)
        requests_to_release.add(task.request_id)
    return requests_to_release




@app.task()
def check_scout_tasks():
    hashtag = HashTag.objects.get(hashtag=VerifiedTaskConfig.ACTIVE_SCOUT_HASHTAG)
    tasks = hashtag.tasks
    requests_to_release = set()
    for task in tasks:
        if task.status in ProductionTask.BAD_STATUS:
            task.remove_hashtag(hashtag)
            task.set_hashtag(VerifiedTaskConfig.FAILED_SCOUT_HASHTAG)
        if task.total_files_finished is not None and task.total_files_finished > 0:
            requests_to_release.update(release_scout_requests(task.id))
    for production_request_id in list(requests_to_release):
        _jsonLogger.info(f'Release scouting request',
                         extra=form_json_request_dict(production_request_id, None,
                                                ))
