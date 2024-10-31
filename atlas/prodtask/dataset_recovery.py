import gzip
import logging
import re
from dataclasses import dataclass
from typing import Optional
from atlas.dkb.views import es_by_keys_nested
from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import ProductionTask, StepExecution, InputRequestList, ParentToChildRequest, TRequest, \
    TTask, TemplateVariable, DatasetRecovery, DatasetRecoveryInfo
from atlas.prodtask.spdstodb import fill_template
from atlas.prodtask.views import set_request_status, clone_slices, request_clone_slices, form_existed_step_list
from atlas.task_action.task_management import TaskActionExecutor
_jsonLogger = logging.getLogger('prodtask_ELK')


def recreate_existing_outputs(task_id: int, outputs: [str], parent_task_id: Optional[int] = None):
    task = ProductionTask.objects.get(id=task_id)
    slice = task.step.slice
    step_execs = StepExecution.objects.filter(slice=slice,request=task.request)
    ordered_existed_steps, parent_step = form_existed_step_list(step_execs)
    if len(ordered_existed_steps)>1:
        raise Exception('More than one step for slice')
    production_request = TRequest.objects.get(reqid=task.request_id)
    slices = []
    new_description = 'Recreate tasks for %s' % production_request.description
    new_request_id = None
    for cloned_request in ParentToChildRequest.objects.filter(parent_request=production_request):
        if cloned_request.child_request and cloned_request.child_request.description == new_description:
            new_request_id = cloned_request.child_request.reqid
            break
    if not new_request_id:
        new_request_id = request_clone_slices(production_request.reqid, production_request.manager, new_description,
                                              production_request.ref_link, slices,
                                              production_request.project.project)
    new_slice_number = clone_slices(production_request.reqid, new_request_id, [slice.slice], -1, True, False)[0]
    new_slice = InputRequestList.objects.get(request=new_request_id, slice=new_slice_number)
    new_parent_step = None
    if parent_task_id is not None:
        parent_task = ProductionTask.objects.get(id=parent_task_id)
        new_slice.dataset = next(parent_task.output_non_log_datasets())
        if parent_task.status not in [ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED]:
            new_parent_step = parent_task.step
    else:
        new_slice.dataset = task.inputdataset
    new_slice.save()

    new_step = StepExecution.objects.get(request=new_request_id, slice=InputRequestList.objects.get(request=new_request_id, slice=new_slice_number))
    new_step.update_project_mode('taskRecreation', 'yes')
    if new_step.step_parent_id != new_step.id:
        new_step.step_parent = new_step
    if new_parent_step is not None:
        new_step.step_parent = new_parent_step
    if outputs:
        new_step_template = fill_template(new_step.step_template.step, new_step.step_template.ctag,
                                          new_step.step_template.priority, '.'.join(outputs), new_step.step_template.memory)
        new_step.update_project_mode('checkOutputDeleted', 'yes')
        new_step.step_template = new_step_template
    new_step.status = 'Approved'
    new_step.save()
    set_request_status('cron', new_request_id, 'approved', 'Task recreation',
                       'Request was automatically approved')
    return new_request_id, new_slice_number

@dataclass
class TaskDatasetRecover:
    task_id: int | None
    input_dataset: str
    size: int
    status: str
    replicas: [str]



def check_unavailable_datasets(dataset: str, ddm: DDM) -> TaskDatasetRecover| None:
    if ':' in dataset:
        dataset = dataset.split(':')[-1]
    input_unavailable = ddm.check_only_unavailable_rse(dataset)
    if input_unavailable:
        size = ddm.dataset_metadata(dataset)['bytes']
        status = 'unavailable'
        if DatasetRecovery.objects.filter(original_dataset=dataset).exists():
            dataset_recovery = DatasetRecovery.objects.get(original_dataset=dataset)
            if dataset_recovery.status == DatasetRecovery.STATUS.PENDING:
                status = 'pending'
            else:
                status = 'submitted'
        return TaskDatasetRecover(None, dataset, size, status, input_unavailable)
    else:
        return None

def check_empty_datasets(dataset: str, ddm: DDM) -> TaskDatasetRecover| None:
    if ':' in dataset:
        dataset = dataset.split(':')[-1]
    if not ddm.dataset_exists(dataset):
        return TaskDatasetRecover(None, dataset, 0, 'unavailable', [])
    else:
        return None

def get_unavaliaible_dataset_info(dataset: str):
    ddm = DDM()
    return [check_unavailable_datasets(dataset, ddm)]

def get_unavalaible_daod_input_datasets(task_ids: [int]) -> [TaskDatasetRecover]:
    ddm = DDM()
    result = []
    for task_id in task_ids:
        jedi_task = TTask.objects.get(id=task_id)
        if jedi_task.name.startswith('user') or jedi_task.name.startswith('group'):
            input_datasets = []
            if TemplateVariable.KEY_NAMES.INPUT_DS in jedi_task.jedi_task_parameters:
                input_container = jedi_task.jedi_task_parameters[TemplateVariable.KEY_NAMES.INPUT_DS]
                if ',' in input_container:
                    input_datasets = input_container.split(',')
                else:
                    input_datasets = ddm.dataset_in_container(input_container)
            for input_dataset in input_datasets:
                verify_dataset = check_unavailable_datasets(input_dataset, ddm)
                if verify_dataset:
                    verify_dataset.task_id = task_id
                    result.append(verify_dataset)
    return result


def find_deleted_input(dataset: str, child_task_id: int):
    task_id = int(dataset[dataset.rfind('tid')+3:dataset.rfind('_')])
    return find_recreated_task(task_id, child_task_id)

def extract_chain_input_from_datasets(dataset_name: str, chain: [int] = None) -> int:
    result = re.match(r'^.+_tid(?P<tid>\d+)_00$', dataset_name)
    if result:
        parent_task = ProductionTask.objects.get(id=int(result.groupdict()['tid']))
        if chain is None:
            chain = []
        chain.append(parent_task.id)
        if parent_task.inputdataset:
            return extract_chain_input_from_datasets(parent_task.inputdataset, chain)
        else:
            return chain
    else:
        return chain

def find_recreated_task(original_task_id: int, task_id_gt: int):
    task = ProductionTask.objects.get(id=original_task_id)
    similar_tasks = list(ProductionTask.objects.filter(name=task.name, id__gt = task_id_gt))
    if similar_tasks:
        original_task_dkb = es_by_keys_nested({'taskid': original_task_id})
        if original_task_dkb and 'chain_data' in original_task_dkb[0]:
            chain_tasks = original_task_dkb[0]['chain_data']
        else:
            chain_tasks = extract_chain_input_from_datasets(task.inputdataset)
        for task in similar_tasks:
            if task.status not in ProductionTask.BAD_STATUS:
                task_dkb = es_by_keys_nested({'taskid': task.id})
                if task_dkb and 'chain_data' in task_dkb[0]:
                    chain_recovery = task_dkb[0]['chain_data']
                else:
                    chain_recovery = extract_chain_input_from_datasets(task.inputdataset)
                if set(chain_tasks) - set(chain_recovery) !=  set(chain_tasks):
                    return task.id

    return None

def recreate_stuck_replica_task(task_id: int):
    task = ProductionTask.objects.get(id=task_id)
    outputs = list(task.output_non_log_datasets())
    ddm = DDM()
    output_formats_to_recreate = []
    deleted_datasets = []
    recreated_task = None
    if not ddm.dataset_exists(task.inputdataset):
        recreated_task = find_deleted_input(task.inputdataset, task_id)
        if recreated_task is None:
            raise Exception('Original task input dataset does not exist')

    for output in outputs:
        if not ddm.dataset_exists(output):
            output_formats_to_recreate.append(output.split('.')[-1])
            deleted_datasets.append(output)
        elif ddm.check_only_unavailable_rse(output):
            output_formats_to_recreate.append(output.split('.')[-1])
            deleted_datasets.append(output)
    action_executor = TaskActionExecutor('mborodin', 'Obsolete task to be recreated')
    if len(output_formats_to_recreate) == len(outputs):
        #obsolete task
        recovery_request, recovery_slice = recreate_existing_outputs(task_id, [], recreated_task)
        if recreated_task is None:
            action_executor.obsolete_task(task_id)
        return recovery_request, recovery_slice, deleted_datasets
    elif len(output_formats_to_recreate) > 0:
        #recreate only missing outputs
        recovery_request, recovery_slice = recreate_existing_outputs(task_id, output_formats_to_recreate, recreated_task)
        for output_format in output_formats_to_recreate:
            action_executor.obsolete_task_output(task_id, output_format)
        return recovery_request, recovery_slice, deleted_datasets
    return None


def register_recreation_request(datasets_info: [TaskDatasetRecover], username: str, comment: str) -> [int]:
    ddm = DDM()
    requests_registered = []
    for dataset_info in datasets_info:
        dataset = dataset_info.input_dataset
        if ':' in dataset:
            dataset = dataset.split(':')[-1]
        _jsonLogger.info('Register recreation request',
                         extra={'dataset': dataset, 'username': username, 'comment': comment})
        if DatasetRecovery.objects.filter(original_dataset=dataset).exists():
            dataset_recovery = DatasetRecovery.objects.get(original_dataset=dataset)
            if dataset_recovery.status != DatasetRecovery.STATUS.DONE:
                if username not in dataset_recovery.requestor:
                    dataset_recovery.requestor += ',' + username
                dataset_recovery.replicas = ','.join(dataset_info.replicas)
                dataset_recovery.save()
                dataset_recovery_info = DatasetRecoveryInfo.objects.get(dataset_recovery=dataset_recovery)
                info_obj = dataset_recovery_info.info_obj
                if dataset_info.task_id not in dataset_recovery_info.info_obj.linked_tasks:
                    info_obj.linked_tasks.append(dataset_info.task_id)
                if comment:
                    info_obj.comment = "\n".join([dataset_recovery_info.info_obj.comment, f'{username}: {comment}'])
                dataset_recovery_info.info_obj = info_obj
                dataset_recovery_info.save()
            requests_registered.append(dataset_recovery.id)
        else:
            dataset_recovery = DatasetRecovery(original_dataset=dataset, status=DatasetRecovery.STATUS.PENDING, size=dataset_info.size)
            dataset_recovery.requestor = username
            dataset_recovery.type = DatasetRecovery.TYPE.RECOVERY
            original_task_id = int(dataset[dataset.rfind('tid')+3:dataset.rfind('_')])
            dataset_recovery.original_task = ProductionTask.objects.get(id=original_task_id)
            dataset_recovery.sites = ','.join(dataset_info.replicas)
            dataset_recovery.save()
            dataset_recovery = DatasetRecovery.objects.get(original_dataset=dataset)
            dataset_recovery_info = DatasetRecoveryInfo(dataset_recovery=dataset_recovery)
            parent_containers = list(ddm.list_parent_containers(dataset))
            if comment:
                comment = f'{username}: {comment}'
            dataset_recovery_info.info_obj = DatasetRecoveryInfo.Info(comment=comment, containers=parent_containers, linked_tasks=[int(dataset_recovery.original_task.id)])
            dataset_recovery_info.save()
            requests_registered.append(dataset_recovery.id)
    return requests_registered


def create_dataset_recovery_for_different_formats(dataset_recovery_id: int, datasets: [str], recovery_request: int, recovery_slice: int):
    dataset_recovery = DatasetRecovery.objects.get(id=dataset_recovery_id)
    ddm= DDM()
    for dataset in datasets:
        if ':' in dataset:
            dataset = dataset.split(':')[-1]
        if dataset != dataset_recovery.original_dataset and DatasetRecovery.objects.filter(original_dataset=dataset).exists():
            continue
        if dataset == dataset_recovery.original_dataset:
            continue
        dataset_recovery_new = DatasetRecovery(original_dataset=dataset, status=DatasetRecovery.STATUS.ACCOMPANY, size=0)
        dataset_recovery_new.requestor = 'mborodin'
        dataset_recovery_new.type = DatasetRecovery.TYPE.RECOVERY
        dataset_recovery_new.original_task = dataset_recovery.original_task
        dataset_recovery_new.sites = ''
        dataset_recovery_new.save()
        dataset_recovery_new = DatasetRecovery.objects.get(original_dataset=dataset)
        dataset_recovery_info = DatasetRecoveryInfo(dataset_recovery=dataset_recovery_new)
        parent_containers = list(ddm.list_parent_containers(dataset))
        comment = 'Recreated as accompanying dataset for %s' % dataset_recovery.original_dataset
        dataset_recovery_info.info_obj = DatasetRecoveryInfo.Info(comment=comment, containers=parent_containers,
                                                                  linked_tasks=[], recovery_request=recovery_request, recovery_slice=recovery_slice)
        dataset_recovery_info.save()



def submit_dataset_recovery_requests(dataset_recovery_ids: [int]):
    submitted_requests = []
    for dataset_recovery in DatasetRecovery.objects.filter(id__in=dataset_recovery_ids):
        if dataset_recovery.status == DatasetRecovery.STATUS.PENDING:
            dataset_recovery_info = DatasetRecoveryInfo.objects.get(dataset_recovery=dataset_recovery)
            try:
                _jsonLogger.info('Submit recreation request',
                                 extra={'dataset': dataset_recovery.original_dataset})
                result = recreate_stuck_replica_task(dataset_recovery.original_task.id)
                if result:
                    if len(result[2]) > 1:
                        create_dataset_recovery_for_different_formats(dataset_recovery.id, result[2], result[0], result[1])
                    info_obj = dataset_recovery_info.info_obj
                    info_obj.recovery_request = result[0]
                    info_obj.recovery_slice = result[1]
                    dataset_recovery_info.info_obj = info_obj
                    dataset_recovery_info.save()
                else:
                    dataset_recovery_info.error = 'No outputs to recreate'
                    dataset_recovery_info.save()
                    continue
            except Exception as e:
                _jsonLogger.error('Error during recreation request',
                                  extra={'dataset': dataset_recovery.original_dataset, 'error': str(e)})
                dataset_recovery_info.error = str(e)
                dataset_recovery_info.save()
                continue
            dataset_recovery.status = DatasetRecovery.STATUS.SUBMITTED
            dataset_recovery.save()
            submitted_requests.append(dataset_recovery.id)
        else:
            raise Exception('Dataset recovery request is not pending')
    return submitted_requests


def finish_dataset_recovery(dataset_recovery_id: int):
    dataset_recovery = DatasetRecovery.objects.get(id=dataset_recovery_id)
    _jsonLogger.info('Finish dataset recovery',
                     extra={'dataset': dataset_recovery.original_dataset})
    dataset_recovery_info = DatasetRecoveryInfo.objects.get(dataset_recovery=dataset_recovery)
    ddm = DDM()
    task = dataset_recovery.recovery_task
    outputs = list(task.output_non_log_datasets())
    original_format = dataset_recovery.original_dataset.split('.')[-2]
    recreated_dataset = [output for output in outputs if output.split('.')[-2] == original_format][0]
    if task.step != task.step.step_parent:
        if dataset_recovery.original_task.status not in ProductionTask.BAD_STATUS:
            action_executor = TaskActionExecutor('mborodin', f'Obsolete task as it\'s recreated {task.id}')
            action_executor.obsolete_task(dataset_recovery.original_task.id)
    for container in dataset_recovery_info.info_obj.containers:
        try:
            if dataset_recovery.original_dataset in ddm.with_and_without_scope(list(ddm.dataset_in_container(container))):
                ddm.delete_datasets_from_container(container, [dataset_recovery.original_dataset])
            ddm.register_datasets_in_container(container, [recreated_dataset])
        except Exception as e:
            pass
        pass
    for acc_dataset_recovery in DatasetRecovery.objects.filter(original_task=dataset_recovery.original_task, status=DatasetRecovery.STATUS.ACCOMPANY):
        acc_dataset_recovery_info = DatasetRecoveryInfo.objects.get(dataset_recovery=acc_dataset_recovery)
        acc_original_format = acc_dataset_recovery.original_dataset.split('.')[-2]
        acc_recreated_dataset = [output for output in outputs if output.split('.')[-2] == acc_original_format][0]
        for container in acc_dataset_recovery_info.info_obj.containers:
            try:
                if acc_dataset_recovery.original_dataset in ddm.with_and_without_scope(list(ddm.dataset_in_container(container))):
                    ddm.delete_datasets_from_container(container, [acc_dataset_recovery.original_dataset])
                ddm.register_datasets_in_container(container, [acc_recreated_dataset])
            except Exception as e:
                pass
            pass
        acc_dataset_recovery.status = DatasetRecovery.STATUS.DONE
        acc_dataset_recovery.recovery_task = task
        acc_dataset_recovery.save()
    for task_id in dataset_recovery_info.info_obj.linked_tasks:
        # Send reload command to the task
        pass

    dataset_recovery.status = DatasetRecovery.STATUS.DONE
    dataset_recovery.save()



def check_submitted_recovery_requests():
    for dataset_recovery in DatasetRecovery.objects.filter(status=DatasetRecovery.STATUS.SUBMITTED):
        dataset_recovery_info = DatasetRecoveryInfo.objects.get(dataset_recovery=dataset_recovery)
        if dataset_recovery_info.info_obj.recovery_request:
            try:
                step = StepExecution.objects.get(request=dataset_recovery_info.info_obj.recovery_request,
                                                 slice=InputRequestList.objects.get(request=dataset_recovery_info.info_obj.recovery_request, slice=dataset_recovery_info.info_obj.recovery_slice))
                if ProductionTask.objects.filter(step=step, request=dataset_recovery_info.info_obj.recovery_request).exists():
                    task = ProductionTask.objects.get(step=step, request=dataset_recovery_info.info_obj.recovery_request)
                    dataset_recovery.recovery_task = task
                    dataset_recovery.status = DatasetRecovery.STATUS.RUNNING
                    dataset_recovery.save()
                    _jsonLogger.info('Recovery task found',
                                     extra={'dataset': dataset_recovery.original_dataset, 'task_id': task.id})
                    if task.status in [ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED]:
                        finish_dataset_recovery(dataset_recovery.id)
            except Exception as e:
                _jsonLogger.error('Error during recovery task search',
                                  extra={'dataset': dataset_recovery.original_dataset, 'error': str(e)})
                dataset_recovery_info.error = str(e)
                dataset_recovery_info.save()
                continue


def find_fixed_recovery(dataset_recovery_id):
    dataset_recovery = DatasetRecovery.objects.get(id=dataset_recovery_id)
    current_task = dataset_recovery.recovery_task
    for task in ProductionTask.objects.filter(request=current_task.request):
        if task.inputdataset == current_task.inputdataset and task.output_formats == current_task.output_formats:
            dataset_recovery.recovery_task = task
            dataset_recovery.status = DatasetRecovery.STATUS.RUNNING
            dataset_recovery.save()
            if task.status in [ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED]:
                finish_dataset_recovery(dataset_recovery.id)
            return

def check_running_recovery_requests():
    for dataset_recovery in DatasetRecovery.objects.filter(status=DatasetRecovery.STATUS.RUNNING):
        if dataset_recovery.recovery_task.status in [ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED]:
            finish_dataset_recovery(dataset_recovery.id)
        else:
            if dataset_recovery.recovery_task.status in ProductionTask.BAD_STATUS:
                find_fixed_recovery(dataset_recovery.id)


def deleted_aods(lifetime_filepath: str) -> [str]:
    """
    Find all DAODs which are not in the list of DAODs to save
    :param daod_lifetime_filepath: path to file with DAODs to save
    :param output_daods_file: path to file with all DAODs
    :return: list of DAODs to delete
    """
    all_AOD_datasets_to_delete = []
    with  gzip.open(lifetime_filepath, 'rt') as f:
        for line in f:
            dataset = line.strip().split(' ')[0]
            #if ('.DAOD_RPVLL.' in dataset) or ('.DAOD_IDTIDE.' in dataset):
            if ('.AOD.' in dataset):
                all_AOD_datasets_to_delete.append(dataset)
    return all_AOD_datasets_to_delete

def check_dataset_recreated(dataset: str) -> Optional[int]:
    ddm = DDM()
    if ddm.dataset_exists(dataset):
        return None
    result = re.match(r'^.+_tid(?P<tid>\d+)_00$', dataset)
    if result:
        try:
            task = ProductionTask.objects.get(id=int(result.groupdict()['tid']))
            dkb_task = es_by_keys_nested({'taskid': task.id})[0]
            if 'chain_data' in dkb_task:
                chain_tasks = dkb_task['chain_data']
            else:
                chain_tasks = extract_chain_input_from_datasets(task.inputdataset)
            similar_tasks = es_by_keys_nested({'taskname.keyword': task.name})
            for similar_task in similar_tasks:
                if similar_task['taskid'] > task.id:
                    similar_dkb_task = es_by_keys_nested({'taskid': similar_task['taskid']})[0]
                    if 'chain_data' in similar_dkb_task:
                        chain_recovery = similar_dkb_task['chain_data']
                        if set(chain_tasks) - set(chain_recovery) != set(chain_tasks):
                            return similar_task['taskid']
        except:
            pass
    return None

@dataclass
class DatasetRecoveryProcedure():
    pass


def find_deleted_datasets(container: str) -> [int]:
    container = container.split('_tid')[0].split(':')[-1]
    ami_tags = container.split('.')[-1].split('_')
    possible_tasks = ProductionTask.objects.filter(name__startswith='.'.join(container.split('.')[:3]), ami_tag=ami_tags[0]).order_by('-id')
    tasks_to_check = []
    for task in possible_tasks:
        if task.status in ProductionTask.BAD_STATUS and task.status != ProductionTask.STATUS.OBSOLETE:
            continue
        if all([ami_tag in task.name for ami_tag in ami_tags]):
            tasks_to_check.append(task.id)

    return tasks_to_check

def find_steps_to_recover(dataset: str) -> DatasetRecoveryProcedure:
    pass