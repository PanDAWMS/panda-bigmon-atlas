from dataclasses import dataclass

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import ProductionTask, StepExecution, InputRequestList, ParentToChildRequest, TRequest, \
    TTask, TemplateVariable, DatasetRecovery
from atlas.prodtask.spdstodb import fill_template
from atlas.prodtask.views import set_request_status, clone_slices, request_clone_slices, form_existed_step_list


def recreate_existing_outputs(task_id: int, outputs: [str]):
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
    new_slice.dataset = task.inputdataset
    new_slice.save()

    new_step = StepExecution.objects.get(request=new_request_id, slice=InputRequestList.objects.get(request=new_request_id, slice=new_slice_number))
    if new_step.step_parent_id != new_step.id:
        new_step.step_parent = new_step
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
                input_unavailable = ddm.check_only_unavailable_rse(input_dataset)
                if input_unavailable:
                    size = ddm.dataset_metadata(input_dataset)['bytes']
                    status = 'unavailable'
                    if DatasetRecovery.objects.filter(original_dataset=input_dataset).exists():
                        dataset_recovery = DatasetRecovery.objects.get(original_dataset=input_dataset)
                        if dataset_recovery.status == DatasetRecovery.STATUS.PENDING:
                            status = 'pending'
                        else:
                            status = 'submitted'
                    result.append(TaskDatasetRecover(task_id, input_dataset, size, status, input_unavailable))
    return result


def recreate_stuck_replica_task(task_id: int):
    task = ProductionTask.objects.get(id=task_id)
    outputs = list(task.output_non_log_datasets())
    ddm = DDM()
    output_formats_to_recreate = []
    for output in outputs:
        if ddm.check_only_unavailable_rse(output):
            output_formats_to_recreate.append(output.split('.')[-1])
    if len(output_formats_to_recreate) == len(outputs):
        #obsolete task
        return recreate_existing_outputs(task_id, [])
    elif len(output_formats_to_recreate) > 0:
        #recreate only missing outputs
        return recreate_existing_outputs(task_id, output_formats_to_recreate)

    return None