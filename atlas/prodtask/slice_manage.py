import logging

from atlas.prodtask.ddm_api import find_dataset_events
from atlas.prodtask.models import TRequest, InputRequestList, StepExecution

_logger = logging.getLogger('prodtaskwebui')

def form_skipped_slice(slice, reqid):
    cur_request = TRequest.objects.get(reqid=reqid)
    input_list = InputRequestList.objects.filter(request=cur_request, slice=int(slice))[0]
    existed_steps = StepExecution.objects.filter(request=cur_request, slice=input_list)
    # Check steps which already exist in slice
    try:
        ordered_existed_steps, existed_foreign_step = form_existed_step_list(existed_steps)
    except ValueError,e:
        ordered_existed_steps, existed_foreign_step = [],None
    if ordered_existed_steps[0].status == 'Skipped' and input_list.dataset:
        return {}
    processed_tags = []
    last_step_name = ''
    for step in ordered_existed_steps:
        if step.status == 'NotCheckedSkipped' or step.status == 'Skipped':
            processed_tags.append(step.step_template.ctag)
            last_step_name = step.step_template.step

        else:
            input_step_format = step.get_task_config('input_format')
            break
    if input_list.input_data and processed_tags:
        try:
            input_type = ''
            default_input_type_prefix = {
                'Evgen': {'format':'EVNT','prefix':''},
                'Simul': {'format':'HITS','prefix':'simul.'},
                'Merge': {'format':'HITS','prefix':'merge.'},
                'Reco': {'format':'AOD','prefix':'recon.'},
                'Rec Merge': {'format':'AOD','prefix':'merge.'}
            }
            if last_step_name in default_input_type_prefix:
                if input_step_format:
                    input_type = default_input_type_prefix[last_step_name]['prefix'] + input_step_format
                else:
                    input_type = default_input_type_prefix[last_step_name]['prefix'] + default_input_type_prefix[last_step_name]['format']
            dsid = input_list.input_data.split('.')[1]
            job_option_pattern = input_list.input_data.split('.')[2]
            dataset_events = find_skipped_dataset(dsid,job_option_pattern,processed_tags,input_type)
            #print dataset_events
            #return {slice:[x for x in dataset_events if x['events']>=input_list.input_events ]}
            return {slice:dataset_events}
        except Exception,e:
            logging.error("Can't find skipped dataset: %s" %str(e))
            return {}
    return {}


def form_existed_step_list(step_list):
    result_list = []
    temporary_list = []
    another_chain_step = None
    for step in step_list:
        if step.step_parent == step:
            if result_list:
                raise ValueError('Not linked chain')
            else:
                result_list.append(step)
        else:
           temporary_list.append(step)
    if not result_list:
        for index,current_step in enumerate(temporary_list):
            if current_step.step_parent not in temporary_list:
                # step in other chain
                another_chain_step = current_step.step_parent
                result_list.append(current_step)
                temporary_list.pop(index)
    for i in range(len(temporary_list)):
        j = 0
        while (temporary_list[j].step_parent!=result_list[-1]):
            j+=1
            if j >= len(temporary_list):
                raise ValueError('Not linked chain')
        result_list.append(temporary_list[j])
    return (result_list,another_chain_step)


def find_skipped_dataset(DSID,job_option,tags,data_type):
    """
    Find a datasets and their events number for first not skipped step in chain
    :param DSID: dsid of the chain
    :param job_option: job option name of the chain input
    :param tags: list of tags which were already proceeded
    :param data_type: expected data type
    :return: list of dict {'dataset_name':'...','events':...}
    """
    return_list = []
    for base_value in ['valid','mc']:
        dataset_pattern = base_value+"%"+str(DSID)+"%"+job_option+"%"+data_type+"%"+"%".join(tags)+"%"
        _logger.debug("Search dataset by pattern %s"%dataset_pattern)
        return_list += find_dataset_events(dataset_pattern)
    return return_list