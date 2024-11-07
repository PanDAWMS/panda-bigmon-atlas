
import json
import logging
import re
from django.core.exceptions import ObjectDoesNotExist
from atlas.prodtask.models import StepExecution, HashTag, HashTagToRequest, TaskTemplate, ProductionDataset, ProductionTask, TTask
from atlas.deftcore.protocol import Protocol, TaskStatus, TaskDefConstants
from django.utils import timezone
from atlas.JIRA.client import JIRAClient
from atlas.prodtask.ddm_api import DDM

logger = logging.getLogger('deftcore')

MONITORING_REQUEST_LINK_FORMAT = 'https://prodtask-dev.cern.ch/prodtask/inputlist_with_request/%d/'
# noinspection PyUnresolvedReferences
class TaskRegistration(object):
    @staticmethod
    def register_task_id():
        return TTask().get_id()

    def register_task_output(self, output_params, task_proto_id, task_id, parent_task_id, usergroup, campaign):
        for key in list(output_params.keys()):
            for output_dataset_name in output_params[key]:
                output_dataset_name = output_dataset_name.replace(
                    TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_proto_id,
                    TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_id)
                if ProductionDataset.objects.filter(name=output_dataset_name).exists():
                    continue
                else:
                    dataset = ProductionDataset(name=output_dataset_name,
                                                task_id=task_id,
                                                parent_task_id=parent_task_id,
                                                phys_group=usergroup,
                                                timestamp=timezone.now(),
                                                campaign=campaign)

                    dataset.save()

                    logger.debug('Dataset {0} is registered'.format(output_dataset_name))

    @staticmethod
    def _register_task_reference(step):
        return step.request.jira_reference

    @staticmethod
    def register_request_reference(request):
        try:
            if not request.jira_reference:
                link_to_request = MONITORING_REQUEST_LINK_FORMAT % request.reqid

                ticket_summary = 'Request {0}'.format(request.reqid)
                ticket_description = \
                    'Request Id: {0}\nDescription: {1}\nReference link: {2}\n'.format(
                        request.reqid, request.description, request.jira_reference) + \
                    'Manager: {0}\nLink to the request: {1}'.format(
                        request.manager, link_to_request)

                client = JIRAClient()
                client.authorize()

                issue_key = client.create_issue(ticket_summary, ticket_description)

                request.jira_reference = issue_key
                request.save()
            return request.jira_reference
        except Exception as ex:
            logger.exception('register_request_reference, exception occurred: {0}'.format(str(ex)))
            return None

    def register_task_template(self, task, step, parent_task_id, template_type=None, template_build=None):
        protocol = Protocol()
        if TaskTemplate.objects.filter(step=step,request=step.request, template_type=template_type,build=template_build).exists():
            task_template = TaskTemplate.objects.get(step=step,request=step.request, template_type=template_type,build=template_build)
            task_template.name = task['taskName']
            task_template.task_error = None
            task_template.task_template = protocol.serialize_task(task)
        else:
            task_template = TaskTemplate(step=step,
                                         request=step.request,
                                         parent_id=parent_task_id,
                                         name=task['taskName'],
                                         template_type=template_type,
                                         task_template=protocol.serialize_task(task),
                                         build=template_build)
        task_template.save()
        return   task_template






    def register_task(self, task, step, task_id, parent_task_id, chain_id, project, input_data_name, number_of_events,
                      campaign, subcampaign, bunchspacing, ttcr_timestamp, truncate_output_formats=None,
                      task_common_offset=None):
        protocol = Protocol()

        issue_key = self._register_task_reference(step)
        task['ticketID'] = issue_key

        # FIXME
        is_extension = False
        try:
            for param in task['jobParameters']:
                if 'offset' in list(param.keys()):
                    if param['offset'] != 0:
                        is_extension = True
                        break
        except Exception as ex:
            logger.exception('register_task, checking offset failed: {0}'.format(str(ex)))

        jedi_task = TTask(id=task_id,
                          parent_tid=parent_task_id,
                          status=protocol.TASK_STATUS[TaskStatus.WAITING],
                          total_done_jobs=0,
                          submit_time=timezone.now(),
                          vo=task['vo'],
                          prodSourceLabel=task['prodSourceLabel'],
                          name=task['taskName'],
                          username=task['userName'],
                          chain_id=chain_id,
                          _jedi_task_parameters=protocol.serialize_task(task))

        total_req_events = 0
        if number_of_events > 0:
            total_req_events = number_of_events

        output_formats = step.step_template.output_formats
        if truncate_output_formats:
            output_format_list = output_formats.split('.')
            output_formats_truncated_list = list()
            for output_format in output_format_list:
                max_length = ProductionTask._meta.get_field('output_formats').max_length
                if len('.'.join(output_formats_truncated_list + [output_format, ])) > max_length:
                    break
                output_formats_truncated_list.append(output_format)
            output_formats = '.'.join(output_formats_truncated_list)

        prod_task = ProductionTask(id=task_id,
                                   step_id=step.id,
                                   request_id=step.request.reqid,
                                   parent_id=parent_task_id,
                                   name=task['taskName'],
                                   project=project,
                                   phys_group=task['workingGroup'].split('_')[1],
                                   provenance=task['workingGroup'].split('_')[0],
                                   status='waiting',
                                   total_events=0,
                                   total_req_jobs=0,
                                   total_done_jobs=0,
                                   submit_time=timezone.now(),
                                   bug_report=0,
                                   priority=task['taskPriority'],
                                   inputdataset=input_data_name,
                                   timestamp=timezone.now(),
                                   vo=task['vo'],
                                   prodSourceLabel=task['prodSourceLabel'],
                                   username=task['userName'],
                                   chain_id=chain_id,
                                   dynamic_jobdef=protocol.is_dynamic_jobdef_enabled(task),
                                   campaign=campaign,
                                   subcampaign=subcampaign,
                                   bunchspacing=bunchspacing,
                                   total_req_events=total_req_events,
                                   pileup=protocol.is_pileup_task(task),
                                   simulation_type=protocol.get_simulation_type(step),
                                   is_extension=is_extension,
                                   ttcr_timestamp=ttcr_timestamp,
                                   primary_input=self.get_primary_input(task),
                                   ami_tag=step.step_template.ctag,
                                   output_formats=output_formats)

        if issue_key:
            prod_task.reference = issue_key

        prod_task.save()
        jedi_task.save()

        if task_common_offset:
            task_common_offset_hashtag = TaskDefConstants.DEFAULT_TASK_COMMON_OFFSET_HASHTAG_FORMAT.format(
                task_common_offset
            )
            try:
                hashtag = HashTag.objects.get(hashtag=task_common_offset_hashtag)
            except ObjectDoesNotExist:
                hashtag = HashTag(hashtag=task_common_offset_hashtag, type='UD')
                hashtag.save()
            prod_task.set_hashtag(hashtag.hashtag)
        try:
            request_hashtags = HashTagToRequest.objects.filter(request=step.request)
            for hashtag in request_hashtags:
                prod_task.set_hashtag(hashtag.hashtag)
        except Exception as e:
            logger.warning('Problem with hashtag registration {0}'.format(str(e)))
        logger.debug('Task {0} is registered'.format(task_id))

    def _register_task_input(self, prod_task, events_per_file):
        pass

    def get_step_output(self, step_id, exclude_failed=True, task_id=None):
        try:
            step = StepExecution.objects.get(id=step_id)
        except ObjectDoesNotExist:
            logger.debug('get_step_output, step {0} is not found'.format(step_id))
            return list()
        if task_id:
            tasks = ProductionTask.objects.filter(id=task_id)
        else:
            tasks = ProductionTask.objects.filter(step=step).order_by('-id')
        if exclude_failed:
            tasks = tasks.exclude(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort'])
        if len(tasks) == 0:
            return list()
        dataset_name_list = [dataset.name for dataset in ProductionDataset.objects.filter(task_id=tasks[0].id)]
        return dataset_name_list

    def get_step_tasks(self, step_id, exclude_failed=True):
        try:
            step = StepExecution.objects.get(id=step_id)
        except ObjectDoesNotExist:
            return list()
        tasks = ProductionTask.objects.filter(step=step).order_by('id')
        if exclude_failed:
            tasks = tasks.exclude(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort'])
        if len(tasks) == 0:
            return list()
        return [task.id for task in tasks]

    def get_task_parameter(self, task_id, param_name):
        try:
            task = TTask.objects.get(id=task_id)
        except ObjectDoesNotExist:
            logger.debug('get_task_parameter, task {0} is not found'.format(task_id))
            return None
        return json.loads(task._jedi_task_parameters)[param_name]

    def get_parent_task_id(self, step, task_id):
        if not step.step_parent_id or step.step_parent_id == step.id:
            return task_id
        parent_tasks = \
            ProductionTask.objects.filter(step=step.step_parent_id).exclude(
                status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']).order_by('-id')
        if len(parent_tasks) == 0:
            return task_id
        return parent_tasks[0].id

    def get_dataset_task_id(self, dataset_name):
        try:
            dataset = ProductionDataset.objects.get(name=dataset_name)
            return dataset.task_id
        except ObjectDoesNotExist:
            return None

    def get_primary_input(self, task):
        job_parameters = task['jobParameters']
        primary_input_param = None
        primary_input_dsn = None
        for job_param in job_parameters:
            if 'param_type' not in list(job_param.keys()) or job_param['param_type'].lower() != 'input'.lower():
                continue
            if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                if not result:
                    continue
                in_type = result.groupdict()['intype']
                if in_type.lower() == 'logs'.lower() or \
                        re.match(r'^.*(PtMinbias|Cavern).*$', in_type, re.IGNORECASE) or \
                        in_type.lower() == 'ZeroBiasBS':
                    continue
                primary_input_param = job_param
                break
        if primary_input_param:
            primary_input_dsn = str(primary_input_param['dataset']).split('/')[0].split(':')[-1]
        return primary_input_dsn

    @staticmethod
    def check_task_output(task_id, types):
        output_datasets = \
            [dataset.name for dataset in ProductionDataset.objects.filter(task_id=task_id)]

        rucio_client = DDM()

        output_type_status_dict = dict()

        for output_dataset in output_datasets:
            result = re.match(TaskDefConstants.DEFAULT_DATA_NAME_PATTERN, output_dataset)
            if not result:
                continue
            output_type = result.groupdict().get('data_type', None)
            if output_type not in types:
                continue
            if output_type:
                output_dataset_exists = rucio_client.is_dsn_exists_with_rule_or_replica(output_dataset)
                output_type_status_dict.update({output_type: output_dataset_exists})

        return output_type_status_dict
