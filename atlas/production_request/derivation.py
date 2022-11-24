from collections import Counter
from dataclasses import dataclass, field
from functools import reduce
from typing import List, Mapping

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats, TRequest, \
    ProductionDataset, GroupProductionDeletionExtension, InputRequestList, StepExecution, StepTemplate, SliceError, \
    JediTasks, JediDatasetContents

@dataclass
class DerivationDatasetInfo():
    dataset: str
    outputs: str
    task_id: int
    request_id: int
    task_status: str

@dataclass
class DerivationContainer():
    container: str
    datasets: [DerivationDatasetInfo]
    is_wrong_name: bool = False
    is_failed: bool = False
    is_running: bool = False
    has_failing: bool = False
    requests_id: [int] = field(default_factory=list)
    output_formats: [str] = field(default_factory=list)
    projects: [str] = field(default_factory=list)




    def reduce_datasets(self) -> None:
        def reduce_list(current_list):
            if len(current_list) == 1:
                return current_list
            else:
                all_formats = reduce(lambda x, y: x + y, [x.outputs.split('.') for x in current_list])
                if len(all_formats) == len(set(all_formats)):
                    return current_list
                else:
                    format_count = Counter(all_formats)
                    for output_format, repeated in format_count.items():
                        if repeated>1:
                            max_id_index = None
                            to_remove = []
                            for index, dataset in enumerate(current_list):
                                if output_format in dataset.outputs.split('.'):
                                    if max_id_index is None:
                                        max_id_index = index
                                    else:
                                        if  dataset.task_id > current_list[max_id_index].task_id:
                                            to_remove.append(max_id_index)
                                            max_id_index = index
                                        else:
                                            to_remove.append(index)
                            new_list = []
                            for index, dataset in enumerate(current_list):
                                if index not in to_remove:
                                    new_list.append(dataset)
                            current_list = new_list
                    return current_list


        sorted_datasets_info = sorted(self.datasets, key=lambda x: x.dataset)
        new_dataset_list = []
        current_dataset_name = ''
        current_list = []
        for dataset in sorted_datasets_info:
            if not current_dataset_name or dataset.dataset == current_dataset_name:
                current_dataset_name = dataset.dataset
                current_list.append(dataset)
            else:
                new_dataset_list+=reduce_list(current_list)
                current_dataset_name = dataset.dataset
                current_list = [dataset]
        new_dataset_list+=reduce_list(current_list)
        self.datasets = new_dataset_list


    def get_stats(self):
        self.is_failed = True
        self.is_running = False
        self.has_failing = False
        requests = set()
        outputs = set()
        projects = set()
        for dataset in self.datasets:
            status = dataset.task_status
            self.is_running |= status not in ProductionTask.NOT_RUNNING
            self.has_failing |= status in ProductionTask.RED_STATUS
            self.is_failed &= status in ProductionTask.RED_STATUS
            requests.add(dataset.request_id)
            projects.add(dataset.dataset.split('.')[0])
            outputs.update(dataset.outputs.split('.'))
        self.requests_id = list(requests)
        self.output_formats = list(outputs)
        self.projects = list(projects)

def ami_tags_reduction_w_data(postfix, data=False):
    if 'tid' in postfix:
        postfix = postfix[:postfix.find('_tid')]
    if data:
        return postfix
    new_postfix = []
    first_letter = ''
    for token in postfix.split('_'):
        if token[0] != first_letter and not (token[0] == 's' and first_letter == 'a'):
            new_postfix.append(token)
        first_letter = token[0]
    return '_'.join(new_postfix)

def get_container_name(dataset_name):
    container_name = '.'.join(dataset_name.split('.')[:-1] + [ami_tags_reduction_w_data(dataset_name.split('.')[-1], dataset_name.startswith('data') or ('TRUTH' in dataset_name) )])
    if dataset_name.startswith('data') or ('TRUTH' in dataset_name):
        return container_name
    postfix = container_name.split('.')[-1]
    if postfix.split('_')[-1].startswith('r'):
        return container_name.replace('merge','recon')
    if postfix.split('_')[-1].startswith('e'):
        return container_name.replace('merge','evgen')
    if postfix.split('_')[-1].startswith('s') or postfix.split('_')[-1].startswith('a'):
        return container_name.replace('merge','simul')
    return container_name

def find_all_inputs_by_tag(ami_tag: str) -> [DerivationContainer]:
    tasks = ProductionTask.objects.filter(ami_tag=ami_tag)
    forming_containers : dict[str,DerivationContainer]= {}
    datasets = {}
    containers_content = {}
    ddm = DDM()
    result = []
    format_reduction = lambda x: '.'.join(sorted(list(set(x.split('.')))))
    for task in tasks:
        input_container = task.step.slice.dataset[task.step.slice.dataset.find(':')+1:]
        input_dataset = task.inputdataset[task.inputdataset.find(':')+1:]
        if task.parent_id != task.id or not input_container or input_container.endswith('py'):
            input_container = get_container_name(input_dataset)
        if 'tid' not in input_dataset:
            input_dataset = task.input_dataset[task.input_dataset.find(':')+1:]
        dataset_added = False
        cleaned_formats = format_reduction(task.output_formats)
        if input_container not in forming_containers:
            forming_containers[input_container] = DerivationContainer(container=input_container,
                                                              datasets=[DerivationDatasetInfo(dataset=input_dataset,
                                                              outputs=cleaned_formats,
                                                              task_id=task.id,
                                                              request_id=task.request_id,
                                                                task_status=task.status)])
            dataset_added = True
        else:
            derivation_container = forming_containers[input_container]
            if input_dataset+cleaned_formats not in [x.dataset+x.outputs for x in derivation_container.datasets]:
                derivation_container.datasets.append(DerivationDatasetInfo(input_dataset,cleaned_formats,
                                                                           task.id,task.request_id,task.status))
                dataset_added = True
            else:
                index = [x.dataset+x.outputs for x in derivation_container.datasets].index(input_dataset+cleaned_formats)
                if task.id > derivation_container.datasets[index].task_id:
                    derivation_container.datasets[index].task_id = task.id
                    derivation_container.datasets[index].request_id = task.request_id
                    derivation_container.datasets[index].task_status = task.status
                    dataset_added = True

        if dataset_added:
            datasets[input_dataset+cleaned_formats] = {'task_id':task.id, 'container': input_container, 'formats': task.output_formats}
    for derivation_container in forming_containers.values():
        short_container = get_container_name(derivation_container.datasets[0].dataset)
        derivation_container.reduce_datasets()
        derivation_container.is_wrong_name = False
        if derivation_container.container.split('.')[1].isnumeric() and (short_container != derivation_container.container)\
                and (short_container.replace('mc16_13TeV','mc15_13TeV') != derivation_container.container):
            new_datasets = [x[x.find(':') + 1:] for x in ddm.dataset_in_container(short_container)]
            if not [x.dataset for x in derivation_container.datasets if x.dataset not in new_datasets]:
                derivation_container.is_wrong_name = True
                if short_container not in forming_containers:
                    derivation_container.container = short_container
                else:
                    short_deriv_container = forming_containers[short_container]
                    short_deriv_container.datasets += derivation_container.datasets
                    short_deriv_container.reduce_datasets()
                    derivation_container.container = ''
    total_requests = set()
    total_outputs = set()
    total_projects = set()
    for derivation_container in forming_containers.values():
        if  derivation_container.container != '':
            derivation_container.get_stats()
            total_requests.update(derivation_container.requests_id)
            total_outputs.update(derivation_container.output_formats)
            total_projects.update(derivation_container.projects)
            result.append(derivation_container)
    return result, total_requests, total_outputs, total_projects






