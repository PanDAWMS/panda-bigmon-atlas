from dataclasses import dataclass

from django.db.models import Q

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import InputRequestList, StepExecution, ProductionTask, HashTag
from atlas.prodtask.spdstodb import fill_template
from atlas.prodtask.views import clone_slices, form_existed_step_list, set_request_status
from atlas.task_action.task_management import TaskActionExecutor


@dataclass
class SliceToFix:
    slice_id: int
    steps_number: int
    replace_first_step: bool

@dataclass
class ReprocessingTaskFix:
    original_task_id: int
    slices_to_clone: [SliceToFix]
    tasks_to_abort: [int]
    container: str
    error: str

def patched_containers(request_id: int) -> [int]:
    patched_tasks = []
    steps = list(StepExecution.objects.filter(request=request_id))
    for step in steps:
        if step.get_project_mode('patchRepro') and step.get_project_mode('patchRepro').isdigit() and not step.slice.is_hide:
            patched_tasks.append(int(step.get_project_mode('patchRepro')))
    return patched_tasks

def find_reprocessing_to_fix(request_id: int, patched_tasks: [int]) -> [ReprocessingTaskFix]:
    slices = list(InputRequestList.objects.filter(request=request_id).filter(~Q(is_hide=True),).order_by("slice"))
    tasks_to_fix: [ReprocessingTaskFix] = []
    checked_slices = []
    for index, slice in enumerate(slices):
        original_ordered_existed_steps, parent_step = (
            form_existed_step_list(list(StepExecution.objects.filter(slice=slice, request=request_id))))
        if (slice.slice not in checked_slices and
                not parent_step and len(original_ordered_existed_steps) > 0 and
                ProductionTask.objects.filter(step=original_ordered_existed_steps[0],status=ProductionTask.STATUS.FINISHED, request=request_id).exists()):
            original_task = ProductionTask.objects.get(step=original_ordered_existed_steps[0], request=request_id)
            if original_task.id in patched_tasks:
                continue
            outputs = original_task.output_formats.split('.')
            checked_output = []
            reprocessing_task_to_fix = ReprocessingTaskFix(original_task.id, [], [],slice.dataset, '')
            slices_for_dataset = []
            for search_slice in slices[index+1:]:
                if search_slice.dataset == slice.dataset:
                    slices_for_dataset.append(search_slice)
            slices_for_dataset.reverse()
            checked_task_by_output = []
            tasks_to_abort = []
            for slice_for_dataset in slices_for_dataset:
                checked_slices.append(slice_for_dataset.slice)
                ordered_existed_steps, parent_step = form_existed_step_list(list(
                    StepExecution.objects.filter(request=request_id, slice=slice_for_dataset)))
                if not parent_step:
                    continue
                clone_step_number = 0
                original_is_parent = False
                for step in ordered_existed_steps:
                    if ProductionTask.objects.filter(step=step, request=request_id).exists():
                        task = ProductionTask.objects.get(step=step, request=request_id)
                        if f'{task.parent_id}{task.output_formats}' in checked_task_by_output:
                            break
                        checked_task_by_output.append(f'{task.parent_id}{task.output_formats}')
                        if task.parent_id == original_task.id:
                            original_is_parent = True
                            checked_output.append(step.get_task_config('input_format'))
                        clone_step_number += 1
                        if task.status not in ProductionTask.BAD_STATUS:
                            tasks_to_abort.append(task.id)
                if clone_step_number > 0:
                    reprocessing_task_to_fix.slices_to_clone.append(SliceToFix(slice_for_dataset.slice, clone_step_number, original_is_parent))
            clone_step_number = 1
            for step in original_ordered_existed_steps[1:]:
                if ProductionTask.objects.filter(step=step, request=request_id).exists():
                    task = ProductionTask.objects.get(step=step)
                    if f'{task.parent_id}{task.output_formats}' in checked_task_by_output:
                            break
                    clone_step_number += 1
                    if task.parent_id == original_task.id:
                        checked_output.append(step.get_task_config('input_format'))
                    if task.status not in ProductionTask.BAD_STATUS:
                        tasks_to_abort.append(task.id)
            reprocessing_task_to_fix.slices_to_clone.append(
                SliceToFix(slice.slice, clone_step_number, True))
            reprocessing_task_to_fix.tasks_to_abort = tasks_to_abort
            if[x for x in outputs if x not in checked_output]:
                reprocessing_task_to_fix.error = 'Not all outputs from main reprocessing tasks have output'
            tasks_to_fix.append(reprocessing_task_to_fix)
    return tasks_to_fix


def change_step_repro_fix(request_id: int, base_new_slice: InputRequestList, original_task_id: int, ami_tag: str) -> StepExecution:
    original_ordered_existed_steps, parent_step = (
        form_existed_step_list(list(StepExecution.objects.filter(slice=base_new_slice, request=request_id))))
    new_step = original_ordered_existed_steps[0]
    task = ProductionTask.objects.get(id=original_task_id)
    new_step.update_project_mode('patchRepro',original_task_id)
    new_step.step_template = fill_template(task.step.step_template.step, ami_tag, task.step.step_template.priority,
                                             task.step.step_template.output_formats, task.step.step_template.memory)
    new_step.status = StepExecution.STATUS.APPROVED
    new_step.save()
    return new_step


def change_slice_to_container(request_id: int, slice_number: int, original_task_id: int, replace_first_step: bool):
    slice = InputRequestList.objects.get(request=request_id, slice=slice_number)
    original_ordered_existed_steps, _ = form_existed_step_list(list(StepExecution.objects.filter(slice=slice, request=request_id)))
    step = original_ordered_existed_steps[0]
    step.update_project_mode('patchRepro', 'wait')
    if replace_first_step:
        input_format = step.get_task_config('input_format')
        step.step_parent = step
        step.update_project_mode('useContainerName', 'yes')
        step.update_project_mode('mergeCont', 'yes')
        task = ProductionTask.objects.get(id=original_task_id)
        output_dataset = [x for x in task.output_non_log_datasets() if x.split('.')[-2] == input_format][0]
        slice.dataset = output_dataset.split('_tid')[0]
        slice.save()
    step.status = StepExecution.STATUS.SKIPPED
    step.save()
    for step in original_ordered_existed_steps[1:]:
        step.status = StepExecution.STATUS.SKIPPED
        step.update_project_mode('patchRepro','wait')
        step.save()








def approve_merge_patch_slices(request_id: int, container_identifier: str):
    slices = list(InputRequestList.objects.filter(request=request_id).filter(~Q(is_hide=True),).order_by("slice"))
    for slice in slices:
        if container_identifier in slice.dataset:
            ordered_existed_steps, parent_step = form_existed_step_list(list(StepExecution.objects.filter(slice=slice, request=request_id)))
            for step in ordered_existed_steps:
                if  step.status == StepExecution.STATUS.SKIPPED and step.get_project_mode('patchRepro') and step.get_project_mode('patchRepro') == 'wait':
                    step.remove_project_mode('patchRepro')
                    step.status = StepExecution.STATUS.APPROVED
                    step.save()

def find_done_patched_tasks():
    REPRO_PATCH_HASHTAG = 'ReproPatch'
    ht = HashTag.objects.get(hashtag=REPRO_PATCH_HASHTAG)
    tasks = ht.tasks.filter(status__in=[ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED] )
    for task in tasks:
        unleash_repro_patch_merge(task.id)
        task.remove_hashtag(ht)
    failed_tasks = ht.tasks.filter(status__in=ProductionTask.BAD_STATUS)
    for task in failed_tasks:
        task.remove_hashtag(ht)

def unleash_repro_patch_merge(task_id: int):
    task = ProductionTask.objects.get(id=task_id)
    request = task.request
    if task.status != ProductionTask.STATUS.DONE:
        return False
    original_task = ProductionTask.objects.get(id=int(task.step.get_project_mode('patchRepro')))
    original_datasets = original_task.output_non_log_datasets()
    container_by_output = {}
    for dataset in original_datasets:
        container_by_output[dataset.split('.')[-2]] = dataset.split('_tid')[0]
    produced_tasks = task.output_non_log_datasets()
    ddm = DDM()
    for dataset in produced_tasks:
        output_name = dataset.split('.')[-2]
        ddm.register_datasets_in_container(container_by_output[output_name], [dataset])
    approve_merge_patch_slices(request.reqid, '.'.join(task.name.split('.')[1:2]))
    set_request_status('cron', request.reqid, 'approved', 'Reprocessing patch',
                       'Request was automatically approved')
    return True

def clone_fix_reprocessing_task(reprocessing_task: ReprocessingTaskFix, ami_tag: str):
    original_task = ProductionTask.objects.get(id=reprocessing_task.original_task_id)
    request_id = original_task.request_id
    slices_to_clone = {x.slice_id:x for x in reprocessing_task.slices_to_clone}
    slices_to_clone_keys = list(slices_to_clone.keys())
    slices_to_clone_keys.sort()
    new_base_slice = None
    parent_steps = {}
    if slices_to_clone[slices_to_clone_keys[0]].steps_number > 1:
        new_base_slice = clone_slices(request_id, request_id, slices_to_clone_keys[0], -1, False)[0]
        original_ordered_existed_steps, parent_step = (
            form_existed_step_list(list(
                StepExecution.objects.filter(slice=InputRequestList.objects.get(request=request_id, slice=slices_to_clone_keys[0]),
                                             request=request_id))))
        new_steps = form_existed_step_list(list(
                StepExecution.objects.filter(slice=InputRequestList.objects.get(request=request_id, slice=new_base_slice),
                                             request=request_id)))
        for index, step in enumerate(original_ordered_existed_steps[1:]):
            parent_steps[step.id] = new_steps[index]

    new_slices = clone_slices(request_id, request_id, slices_to_clone_keys, -1, False, predefined_parrent=parent_steps)
    base_new_slice = InputRequestList.objects.get(request=request_id, slice=new_slices[0])
    change_step_repro_fix(request_id, base_new_slice, reprocessing_task.original_task_id, ami_tag)
    if new_base_slice:
        change_slice_to_container(request_id, new_base_slice, reprocessing_task.original_task_id, True)
    for slise_index, original_slice in enumerate(slices_to_clone_keys[1:]):
        change_slice_to_container(request_id, new_slices[slise_index+1], reprocessing_task.original_task_id, slices_to_clone[original_slice].replace_first_step)
    ddm = DDM()
    for output_dataset in original_task.output_non_log_datasets():
        ddm.keepDataset(output_dataset)
    action_executor = TaskActionExecutor('mborodin', 'Abort tasks to be patched')
    for task_to_abort in  reprocessing_task.tasks_to_abort:
            action_executor.obsolete_or_abort_synced_task(task_to_abort)
    return new_slices