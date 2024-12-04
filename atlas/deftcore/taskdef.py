
import glob
import logging
import os
import re
import json
import subprocess
import csv
import io
import ast
import datetime
import copy
import random
import sys
import traceback

import math
from copy import deepcopy

from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q
from django.template import Context, Template
from django.utils import timezone
from distutils.version import LooseVersion
from atlas.prodtask.models import (TRequest, RequestStatus, InputRequestList, StepExecution, ProductionDataset, HashTag,
                                   HashTagToRequest, OpenEndedRequest, StepAction, GlobalShare, SliceError,
                                   TaskTemplate,
                                   TConfig, JediDatasets, ProductionTask, TTask, JediDatasetContents, DistributedLock)
from atlas.deftcore.protocol import (Protocol, StepStatus, TaskParamName, TaskDefConstants, TaskStatus )
from atlas.deftcore.protocol import RequestStatus as RequestStatusEnum
from .taskreg import TaskRegistration
from atlas.ami.client import AMIClient
from atlas.prodtask.ddm_api import DDM
from atlas.cric.client import CRICClient
from atlas.JIRA.client import JIRAClient
from .projectmode import ProjectMode
import  xml.etree.ElementTree as ET

logger = logging.getLogger('deftcore')
REQUEST_GRACE_PERIOD = 1

def get_exception_string():
    ex_info = sys.exc_info()
    ex_string = traceback.format_exception_only(*ex_info[:2])[-1]
    ex_string = ex_string[:-1].replace("[u'", "").replace("']", "")
    return ex_string

class NotEnoughEvents(Exception):
    def __init__(self, previous_tasks):
        message = 'Not enough events, previous tasks: {0} '.format(str(previous_tasks))
        super(NotEnoughEvents, self).__init__(message)



class TaskDuplicateDetected(Exception):
    def __init__(self, previous_task_id, reason_code, **kwargs):
        prefix = ', '.join(['{0} = {1}'.format(name, value) for name, value in list(kwargs.items())])
        message = '[Check duplicates] The task is rejected, previous_task = {0}, reason_code = {1}' \
            .format(previous_task_id, reason_code)
        if prefix:
            message = '[{0}] {1}'.format(prefix, message)
        super(TaskDuplicateDetected, self).__init__(message)


class NoMoreInputFiles(Exception):
    pass


class ParentTaskInvalid(Exception):
    def __init__(self, parent_task_id, task_status):
        message = 'Parent task {0} is {1}'.format(parent_task_id, task_status)
        super(ParentTaskInvalid, self).__init__(message)


class InputLostFiles(Exception):
    def __init__(self, dsn):
        message = 'Input {0} has lost files'.format(dsn)
        super(InputLostFiles, self).__init__(message)


class NumberOfFilesUnavailable(Exception):
    def __init__(self, dataset, ex_message=None):
        message = \
            '[Check duplicates] The task is rejected. Number of files is unavailable (dataset = {0})'.format(dataset)
        if ex_message:
            message = '{0}. {1}'.format(message, ex_message)
        super(NumberOfFilesUnavailable, self).__init__(message)


class UniformDataException(Exception):
    def __init__(self, dataset_name, events_per_file, number_events, number_files, config_events_per_file,
                 parent_events_per_job, parent_task_id):
        message = \
            'The task is rejected because of inconsistency. ' + \
            'nEventsPerInputFile={0} does not match to nEventsPerJob={1} of the parent (taskId={2}). '.format(
                config_events_per_file, parent_events_per_job, parent_task_id) + \
            'DDM ({0}): nEventsPerInputFile={1}, events={2}, files={3}'.format(
                dataset_name, events_per_file, number_events, number_files)
        super(UniformDataException, self).__init__(message)


class MaxJobsPerTaskLimitExceededException(Exception):
    def __init__(self, number_of_jobs):
        message = 'The task is rejected. The limit of number of jobs per task ({0}) is exceeded. '.format(
            TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK) + \
                  'Expected number of jobs for this task is {0}'.format(int(number_of_jobs))
        super(MaxJobsPerTaskLimitExceededException, self).__init__(message)


class MaxEventsPerTaskLimitExceededException(Exception):
    def __init__(self, number_of_events):
        message = 'The task is rejected. The limit of events per task ({0}) is exceeded. '.format(
            TaskDefConstants.DEFAULT_MAX_EVENTS_EVGEN_TASK)  + \
                  'Requested number of events for this task is {0}'.format(int(number_of_events))
        super(MaxEventsPerTaskLimitExceededException, self).__init__(message)


class TaskConfigurationException(Exception):
    def __init__(self, message):
        super(TaskConfigurationException, self).__init__(message)


class GRLInputException(Exception):
    def __init__(self, message):
        super(GRLInputException, self).__init__(message)


class TaskSmallEventsException(Exception):
    def __init__(self, number_of_events):
        message = 'The task is rejected. ' + \
                  'Too few events will be produced to guarantee that the pileup distribution is correct ({0}). '.format(
                      number_of_events) + 'Use isSmallEvents=yes in project_mode to force it'
        super(TaskSmallEventsException, self).__init__(message)


class UnknownSiteException(Exception):
    def __init__(self, site_name):
        message = 'The site "{0}" is unknown to AGIS'.format(site_name)
        super(UnknownSiteException, self).__init__(message)



class ContainerIsNotFoundException(Exception):
    def __init__(self, site_name):
        message = 'The site "{0}" has no cvmfs but container is not found for this ami tag'.format(site_name)
        super(ContainerIsNotFoundException, self).__init__(message)


class TaskDefineOnlyException(Exception):
    def __init__(self, url):
        message = 'The task parameters are defined: {0}'.format(url)
        super(TaskDefineOnlyException, self).__init__(message)


class OutputNameMaxLengthException(Exception):
    def __init__(self, output_name):
        message = 'The task is rejected. The output name "{0}" has length {1} but max allowed length is {2}'.format(
            output_name, len(output_name), TaskDefConstants.DEFAULT_OUTPUT_NAME_MAX_LENGTH)
        super(OutputNameMaxLengthException, self).__init__(message)


class NoRequestedCampaignInput(Exception):
    pass


class InvalidMergeException(Exception):
    def __init__(self, dsn, tag_name):
        message = 'The task is rejected. Merging with ratio 1:1 is skipped (dsn = {0}, tag = {1})'.format(dsn, tag_name)
        super(InvalidMergeException, self).__init__(message)


class MergeInverseException(Exception):
    def __init__(self, neventsprtinputfile, neventsperjob):
        message = 'The task is rejected. Merging tasks must have more events per job ' \
                  ' than events per input file' \
                  ' (nEventsPerJob = {0}, nEventsPerInputFile = {1})'.format(neventsperjob, neventsprtinputfile)
        super(MergeInverseException, self).__init__(message)

class UnmergedInputProcessedException(Exception):
    def __init__(self, task_id):
        message = 'The task is rejected. Unmerged input is already processed (task_id = {0})'.format(task_id)
        super(UnmergedInputProcessedException, self).__init__(message)


class MergedInputProcessedException(Exception):
    def __init__(self, task_id):
        message = 'The task is rejected. Merged input is already processed (task_id = {0})'.format(task_id)
        super(MergedInputProcessedException, self).__init__(message)


class InputEventsForChildStepException(Exception):
    def __init__(self):
        message = "To avoid possible events duplication child step can't have total events set. Please use all events or" \
                  " change input for the slice (Split->Split by events)"
        super(InputEventsForChildStepException, self).__init__(message)

class WrongCacheVersionUsedException(Exception):
    def __init__(self, version, data_version):
        message = 'The task is rejected. The major part of the current cache version ({0}) for derivation '. \
                      format(version) + \
                  'is not equal to the version with which the corresponding input AODs were produced ({0})'. \
                      format(data_version)
        super(WrongCacheVersionUsedException, self).__init__(message)


class EmptyDataset(Exception):
    pass


class BlacklistedInputException(Exception):
    def __init__(self, rses):
        message = 'The task is rejected. Input is available only on blacklisted storage ({0})'.format(
            ', '.join(rses)
        )
        super(BlacklistedInputException, self).__init__(message)

def divisorGenerator(n):
    large_divisors = []
    for i in range(1, int(math.sqrt(n) + 1)):
        if n % i == 0:
            yield i
            if i*i != n:
                large_divisors.append(n // i)
    for divisor in reversed(large_divisors):
        yield divisor

def minHigherDivisor(value, n):
    if value >= n:
        return n
    for x in divisorGenerator(n):
        if x > value:
            return x
    return n

# noinspection PyBroadException, PyUnresolvedReferences
def is_optimal_first_event(step: StepExecution) -> bool:
    """
    Check if the first event is optimal for the given step.
    :param step: StepExecution object.
    :return: True if the first event is optimal, False otherwise.
    """
    if ProjectMode(step).optimalFirstEvent or (step.request.campaign.replace('MC', '').isdigit() and
                                               int(step.request.campaign.replace('MC', '')) >= 21):
        return True
    return False


class TaskDefinition(object):
    def __init__(self, evgen_csv_encoding='utf-8'):
        self.evgen_csv_encoding = evgen_csv_encoding
        self.protocol = Protocol()
        self.task_reg = TaskRegistration()
        self.ami_client = AMIClient()
        self.rucio_client = DDM()
        self.agis_client = CRICClient()
        self.template_type = None
        self.template_build = None
        self._checked_ami_tags = []
        self._verified_evgen_releases = set()

    @staticmethod
    def _get_usergroup(step):
        return '{0}_{1}'.format(step.request.provenance, step.request.phys_group)

    @staticmethod
    def _get_project(step):
        project = step.request.project.project
        if not project:
            project = TaskDefConstants.DEFAULT_DEBUG_PROJECT_NAME
        return project

    @staticmethod
    def _get_energy(step, ctag):
        energy_ctag = None
        for name in list(ctag.keys()):
            if re.match(r'^(--)?ecmEnergy$', name, re.IGNORECASE):
                energy_ctag = int(ctag[name])
        energy_req = int(step.request.energy_gev)
        if energy_ctag and energy_ctag != energy_req:
            raise Exception("Energy mismatch")
        else:
            return energy_req

    @staticmethod
    def get_step_input_data_name(step):
        if step.step_template.step.lower() == 'evgen'.lower():
            return step.slice.input_data
        else:
            if step.slice.dataset:
                return step.slice.dataset
            elif step.slice.input_data:
                return step.slice.input_data
            else:
                return None

    @staticmethod
    def is_new_jo_format(name):
        return ('/' in name) and (name.split('/')[0].isdigit())

    @staticmethod
    def parse_data_name(name):
        # New format
        if TaskDefinition.is_new_jo_format(name):
            data_name_dict = dict()
            data_name_dict['number'] = name.split('/')[0]
            data_name_dict['file_name'] = name.split('/')[1]
            data_name_dict['prod_step'] = 'py'
            data_name_dict['project'] = name.split('/')[1].split('.')[0]
            data_name_dict['brief'] = name.split('/')[1].split('.')[1]
            data_name_dict['version'] = ''
            data_name_dict.update({'name': name})
            return data_name_dict
        else:
            result = re.match(TaskDefConstants.DEFAULT_DATA_NAME_PATTERN, name)
            if not result:
                raise Exception('Invalid data name')

            data_name_dict = result.groupdict()
            data_name_dict.update({'name': name})
            return data_name_dict

    @staticmethod
    def _get_svn_output(svn_command):
        svn_args = ['svn']
        svn_args.extend(svn_command)
        process = subprocess.Popen(svn_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        return process.communicate()[0]

    @staticmethod
    def _get_evgen_input_dict(content):
        input_db = csv.DictReader(io.StringIO(content))
        input_dict = dict()

        for i, row in enumerate(input_db):
            try:
                if row[input_db.fieldnames[0]].strip().startswith("#"):
                    continue

                try:
                    dsid = int(row[input_db.fieldnames[0]].strip())
                except Exception:
                    continue
                energy = int(row[input_db.fieldnames[1]].strip())

                if dsid not in list(input_dict.keys()):
                    input_dict[dsid] = dict()
                if energy not in list(input_dict[dsid].keys()):
                    input_dict[dsid][energy] = dict()

                if row[input_db.fieldnames[2]]:
                    input_event_file = row[input_db.fieldnames[2]].strip().strip('/')
                    if len(input_event_file) > 1:
                        input_dict[dsid][energy]['inputeventfile'] = input_event_file

                if row[input_db.fieldnames[3]]:
                    input_conf_file = row[input_db.fieldnames[3]].strip().strip('/')
                    if len(input_conf_file) > 1:
                        input_dict[dsid][energy]['inputconfigfile'] = input_conf_file
            except Exception:
                continue

        return input_dict

    @staticmethod
    def _parse_jo_file(content):
        lines = list()
        for line in content.splitlines():
            if line.startswith('#'):
                continue
            lines.append(line)
        return '\n'.join(lines)

    @staticmethod
    def _read_param_from_jo(jo, names, ignore_case=True):
        value = None
        if ignore_case:
            names = [name.lower() for name in names]
            jo = jo.lower()
        if any(p in jo for p in names):
            for line in jo.splitlines():
                if any(p in line for p in names):
                    try:
                        if '#' in line:
                            line = line[:line.find('#')]
                        value = int(line.replace(' ', '').split('=')[-1])
                        logger.info('Using ({0}) from JO file: {1}'.format('|'.join(names), value))
                        break
                    except Exception as ex:
                        logger.warning('JO line parse error: {0}'.format(ex))
        return value

    @staticmethod
    def _read_events_per_job_from_jo(jo):
        return TaskDefinition._read_param_from_jo(jo, ['evgenConfig.minevents', 'evgenConfig.nEventsPerJob'])


    @staticmethod
    def unset_slice_error(request, slice):
        try:
            if SliceError.objects.filter(request=request, slice=slice, is_active=True).exists():
                slice_error = SliceError.objects.filter(request=request, slice=slice)[0]
                slice_error.is_active = False
                slice_error.save()
        except Exception as ex:
            logger.warning('Slice error saving failed: {0}'.format(ex))

    @staticmethod
    def set_slice_error(request, slice, exception_type, message):
        try:
            slice_error = SliceError(request=request, slice=InputRequestList.objects.get(id=slice))
            if SliceError.objects.filter(request=request, slice=slice).exists():
                slice_error = SliceError.objects.filter(request=request, slice=slice)[0]
            slice_error.exception_type = exception_type
            slice_error.message = message
            slice_error.exception_time = timezone.now()
            slice_error.is_active = True
            slice_error.save()
        except Exception as ex:
            logger.warning('Slice error saving failed: {0}'.format(ex))

    @staticmethod
    def _read_files_per_job_from_jo(jo):
        return TaskDefinition._read_param_from_jo(jo, ['evgenConfig.inputFilesPerJob'])

    def _get_evgen_input_files_new(self, input_data_dict, energy, evgen_input_container=None):
        if len(str(input_data_dict['number'])) <= 6:
            path_template = Template("/{{number|slice:\"0:3\"}}xxx/{{number}}/{{file_name}}")
        else:
            path_template = Template("{{number|slice:\"0:1\"}}/{{number|slice:\"0:4\"}}xxx/{{number}}/{{file_name}}")
        job_options_file_path = path_template.render(
            Context({'number': str(input_data_dict['number']), 'file_name': input_data_dict['file_name']},
                    autoescape=False))
        path = TaskDefConstants.DEFAULT_NEW_EVGEN_JO_PATH + job_options_file_path
        with open(path, 'r') as fp:
            job_options_file_content = fp.read()
        params = dict()
        if evgen_input_container:
            result = self.rucio_client.get_datasets_and_containers(evgen_input_container,
                                                                   datasets_contained_only=True)
            if 'EVNT' in evgen_input_container and len(result['containers']) == 0:
                raise Exception('EVNT input should be container')
            if 'EVNT' in evgen_input_container:
                params.update({'inputEVNTFile': result['containers']})
                params.update({'isEvntToEvnt': True})

            else:
                params.update({'inputGeneratorFile': result['containers']})

        events_per_job = self._read_events_per_job_from_jo(job_options_file_content)
        if events_per_job is not None:
            params.update({'nEventsPerJob': events_per_job})

        files_per_job = self._read_files_per_job_from_jo(job_options_file_content)
        if files_per_job is not None:
            params.update({'nFilesPerJob': files_per_job})

        params.update({'ecmEnergy': energy})
        return params

    def _get_evgen_input_files(self, input_data_dict, energy, svn=False, use_containers=True, use_evgen_otf=False):
        path_template = Template("share/DSID{{number|slice:\"0:3\"}}xxx/{{file_name}}")
        job_options_file_path = path_template.render(
            Context({'number': str(input_data_dict['number']), 'file_name': input_data_dict['name']}, autoescape=False))

        evgen_input_path = 'share/evgeninputfiles.csv'

        if svn:
            root_path = Template(TaskDefConstants.DEFAULT_EVGEN_JO_SVN_PATH_TEMPLATE).render(
                Context({'campaign': str(input_data_dict['project']).upper()}, autoescape=False))
            latest_tag = ''

            svn_path = "%s%s%s" % (root_path, latest_tag, evgen_input_path)

            if input_data_dict['project'].lower() == 'mc14'.lower():
                svn_path = svn_path.replace('MC14JobOptions', 'MC12JobOptions')
                path_template = Template("share/MC15Val/{{file_name}}")
                job_options_file_path = path_template.render(
                    Context({'file_name': input_data_dict['name']}, autoescape=False))
            elif input_data_dict['project'].lower() == 'mc10'.lower():
                return {}
            svn_command = ['cat', svn_path]
            evgen_input_content = self._get_svn_output(svn_command)

            svn_path = "%s%s%s" % (root_path, latest_tag, job_options_file_path)
            svn_command = ['cat', svn_path]
            job_options_file_content = self._get_svn_output(svn_command)
        else:
            root_path = Template(TaskDefConstants.DEFAULT_EVGEN_JO_PATH_TEMPLATE).render(
                Context({'campaign': str(input_data_dict['project']).upper()}, autoescape=False))

            path = "%s%s" % (root_path, evgen_input_path)

            if input_data_dict['project'].lower() == 'mc14'.lower():
                path = path.replace('MC14JobOptions', 'MC12JobOptions')
                path_template = Template("share/MC15Val/{{file_name}}")
                job_options_file_path = path_template.render(
                    Context({'file_name': input_data_dict['name']}, autoescape=False))
            elif input_data_dict['project'].lower() == 'mc10'.lower():
                return {}
            try:
                with open(path, 'rb') as fp:
                    evgen_input_content = fp.read().decode(self.evgen_csv_encoding)
            except IOError:
                logger.warning("Evgen input content file %s is not found" % path)
                evgen_input_content = ''

            path = "%s%s" % (root_path, job_options_file_path)
            with open(path, 'r') as fp:
                job_options_file_content = fp.read()

        evgen_input_dict = self._get_evgen_input_dict(evgen_input_content)
        if not evgen_input_dict:
            raise Exception('evgeninputfiles.csv file is corrupted')

        params = dict()

        dsid = int(input_data_dict['number'])
        energy = int(energy)

        content = self._parse_jo_file(job_options_file_content)

        if not use_evgen_otf:
            if content.find('evgenConfig.inputconfcheck') >= 0:
                # inputGenConfFile
                try:
                    dsid_row = evgen_input_dict[dsid]
                except KeyError:
                    raise Exception("Invalid request parameter: DSID = %d" % dsid)
                try:
                    energy_row = dsid_row[energy]
                except KeyError:
                    raise Exception("Invalid request parameter: Energy = %d GeV" % energy)
                try:
                    evgen_input_container = "%s/" % energy_row['inputconfigfile']
                except KeyError:
                    raise Exception("Suitable inputconfigfile candidate not found in evgeninputfiles.csv")
                if use_containers:
                    params.update({'inputGenConfFile': [evgen_input_container]})
                else:
                    result = self.rucio_client.get_datasets_and_containers(evgen_input_container,
                                                                           datasets_contained_only=True)
                    params.update({'inputGenConfFile': result['datasets']})
            elif content.find('evgenConfig.inputfilecheck') >= 0:
                # inputGeneratorFile
                try:
                    dsid_row = evgen_input_dict[dsid]
                except KeyError:
                    raise Exception("Invalid request parameter: DSID = %d" % dsid)
                try:
                    energy_row = dsid_row[energy]
                except KeyError:
                    raise Exception("Invalid request parameter: Energy = %d GeV" % energy)
                try:
                    evgen_input_container = "%s/" % energy_row['inputeventfile']
                except KeyError:
                    raise Exception("Suitable inputeventfile candidate not found in evgeninputfiles.csv")
                if use_containers:
                    params.update({'inputGeneratorFile': [evgen_input_container]})
                else:
                    result = self.rucio_client.get_datasets_and_containers(evgen_input_container,
                                                                           datasets_contained_only=True)
                    params.update({'inputGeneratorFile': result['datasets']})
            else:
                dsid_row = evgen_input_dict.get(dsid)
                if dsid_row:
                    entry = dsid_row.get(energy)
                    if entry:
                        if list(entry.keys())[0] == 'inputeventfile':
                            evgen_input_container = "%s/" % entry['inputeventfile']
                            params.update({'inputGeneratorFile': [evgen_input_container]})
                        elif list(entry.keys())[0] == 'inputconfigfile':
                            evgen_input_container = "%s/" % entry['inputconfigfile']
                            params.update({'inputGenConfFile': [evgen_input_container]})

        events_per_job = self._read_events_per_job_from_jo(job_options_file_content)
        if events_per_job is not None:
            params.update({'nEventsPerJob': events_per_job})

        files_per_job = self._read_files_per_job_from_jo(job_options_file_content)
        if files_per_job is not None:
            params.update({'nFilesPerJob': files_per_job})

        return params

    def _add_input_dataset_name(self, name, params):
        input_dataset_dict = self.parse_data_name(name)
        param_name = "input%sFile" % input_dataset_dict['data_type']
        if param_name not in list(params.keys()):
            params[param_name] = list()
        params[param_name].append(name)

    def _add_output_dataset_name(self, name, params):
        output_dataset_dict = self.parse_data_name(name)
        param_name = "output%sFile" % output_dataset_dict['data_type']
        if param_name not in list(params.keys()):
            params[param_name] = list()
        params[param_name].append(name)

    def _get_parent_task_id_from_input(self, input_data_name):
        input_data_dict = self.parse_data_name(input_data_name)
        version = input_data_dict['version']
        result = re.match(r'^\w*_tid(?P<tid>\d*)_00$', version)
        if result:
            return int(result.groupdict()['tid'])
        else:
            return 0

    def get_input_params(self, step, first_step, restart, energy, use_containers=True, use_evgen_otf=False,
                         task_id=None):
        # returns input_params = {'inputAODFile': [...], 'inputEVNTFile': [...], ...}
        input_params = dict()

        if step.step_parent_id == step.id or (step.id == first_step.id and not restart):
            # first step - external input

            # get input from request
            input_data_name = self.get_step_input_data_name(step)

            if not input_data_name:
                return input_params
            is_new_format = False
            if self.is_new_jo_format(input_data_name):
                is_new_format = True
            input_data_dict = self.parse_data_name(input_data_name)

            if input_data_dict['prod_step'].lower() == 'py'.lower():
                # event generation - get input from latest JobOptions or SVN
                # inputGeneratorFile, inputGenConfFile
                if is_new_format:
                    if step.slice.dataset:
                        input_params.update(
                            self._get_evgen_input_files_new(input_data_dict, energy, step.slice.dataset))
                    else:
                        input_params.update(
                            self._get_evgen_input_files_new(input_data_dict, energy))
                    input_params.update({'jobConfig': input_data_dict['number']})
                else:
                    input_params.update(
                        self._get_evgen_input_files(input_data_dict, energy, use_evgen_otf=use_evgen_otf))
                    job_config = "%sJobOptions/%s" % (input_data_dict['project'], input_data_name)
                    input_params.update({'jobConfig': job_config})
                project_mode = ProjectMode(step)
                if project_mode.nEventsPerJob:
                    events_per_job = project_mode.nEventsPerJob
                    input_params.update({'nEventsPerJob': events_per_job})
                    logger.info('Using nEventsPerJob from project_mode: nEventsPerJob={0}'.format(events_per_job))
            else:
                result = self.rucio_client.get_datasets_and_containers(input_data_name, datasets_contained_only=True)
                if use_containers and result['containers']:
                    input_data = result['containers']
                elif use_containers:
                    if not self.rucio_client.is_dsn_container(input_data_name):
                        input_data = result['datasets']
                    else:
                        datasets = self.rucio_client.list_datasets_in_container(input_data_name)
                        if not datasets:
                            raise Exception('The container {0} is empty'.format(input_data_name))
                        logger.debug('Using container {0}'.format(input_data_name))
                        input_data = [input_data_name, ]
                else:
                    input_data = result['datasets']
                for input_dataset_name in input_data:
                    self._add_input_dataset_name(input_dataset_name, input_params)
        else:
            # not first step - internal input, from previous step
            task_config = ProjectMode.get_task_config(step)
            input_formats = list()
            if 'input_format' in list(task_config.keys()):
                for format_name in task_config['input_format'].split('.'):
                    input_formats.append(format_name)
            for input_dataset_name in self.task_reg.get_step_output(step.step_parent_id, task_id=task_id):
                data_type = self.parse_data_name(input_dataset_name)['data_type']

                if data_type.lower() == 'log'.lower():
                    continue

                if input_formats:
                    if data_type in input_formats:
                        self._add_input_dataset_name(input_dataset_name, input_params)
                else:
                    self._add_input_dataset_name(input_dataset_name, input_params)

        return input_params


    def _find_grl_xml_file(self, project, file_name):
        if not file_name.endswith('.xml'):
            file_name = file_name + '.xml'
        grl_file = glob.glob(TaskDefConstants.DEFAULT_GRL_XML_PATH.format(project=project) + '*/' +  file_name)
        if not grl_file:
            raise GRLInputException(f'GRL {file_name} file is not found')
        return grl_file[-1]


    def _get_GRL_from_xml(self, file_path):
        grl_xml_root = ET.parse(file_path).getroot()
        grl_range = {}
        for run in grl_xml_root.findall('NamedLumiRange/LumiBlockCollection'):
            run_number = int(run.find('Run').text)
            grl_range[run_number] = []
            for lumiblock_range in run.findall('LBRange'):
                start = int(lumiblock_range.get('Start'))
                end = int(lumiblock_range.get('End'))
                grl_range[run_number].append((start, end))
        logger.info("GRL finds for the file %s " % file_path)
        return grl_range

    def _filter_input_dataset_by_GRL(self, dataset, grl_range):
        run_number =  int(dataset.split('.')[1])
        if run_number not in grl_range:
            raise GRLInputException(f'{run_number} is not found in the Good Run List')
        current_run_ranges = grl_range[run_number]
        files_in_dataset = self.rucio_client.list_file_long(dataset)
        filtered_files = []
        for input_file in files_in_dataset:
            if 'lumiblocknr' not in input_file:
                raise GRLInputException('lumiblocknr is not found in the input dataset')
            for lumiblock_range in current_run_ranges:
                if (input_file['lumiblocknr'] >= lumiblock_range[0]) and (input_file['lumiblocknr'] <= lumiblock_range[1]):
                    filtered_files.append(input_file)
                    break
        logger.info("GRL files filtered for dataset %s with %d files from %d" % (dataset,  len(filtered_files), len(files_in_dataset)))
        return filtered_files, len(filtered_files) == len(files_in_dataset)

    def _filter_input_dataset_by_previous_task(self, dataset: str, previous_task_id: int):
        files_in_dataset = self.rucio_client.list_file_long(dataset)
        if JediDatasets.objects.filter(id=int(previous_task_id), datasetname=dataset).exists():
            jedi_dataset = JediDatasets.objects.get(id=int(previous_task_id), datasetname=dataset)
        else:
            if ':' in dataset:
                dataset = dataset.split(':')[-1]
            else:
                dataset = dataset.split('.')[0] + ':' + dataset
            jedi_dataset = JediDatasets.objects.get(id=previous_task_id, datasetname=dataset)
        files_in_filter_dataset_names = [x.lfn for x in JediDatasetContents.objects.filter(jeditaskid=previous_task_id, datasetid=jedi_dataset.datasetid, status='finished')]
        filtered_files = []
        for input_file in files_in_dataset:
            if input_file['name'] in files_in_filter_dataset_names:
                filtered_files.append(input_file)
        logger.info(f"List files filtered for dataset {dataset} from {previous_task_id} with {len(filtered_files)} files from {len(files_in_dataset)}" )
        if not filtered_files:
            raise GRLInputException(f'No files are found in the dataset {dataset} from {previous_task_id}')
        return filtered_files, len(filtered_files) == len(files_in_dataset)

    def _filter_input_dataset_by_FLD(self, dataset, filter_dataset):
        files_in_dataset = self.rucio_client.list_file_long(dataset)
        files_in_filter_dataset_names = [x['name'] for x in self.rucio_client.list_file_long(filter_dataset)]
        filtered_files = []
        for input_file in files_in_dataset:
            if input_file['name'] in files_in_filter_dataset_names:
                filtered_files.append(input_file)
        logger.info(f"List files filtered for dataset {dataset} from {filter_dataset} with {len(filtered_files)} files from {len(files_in_dataset)}" )
        if not filtered_files:
            raise GRLInputException(f'No files are found in the dataset {dataset} from {filter_dataset}')
        return filtered_files, len(filtered_files) == len(files_in_dataset)



    def _register_input_GRL_dataset(self, grl_dataset_name, filtered_files, task_id):
        logger.info("GRL input dataset %s with %d files is registered for a task %d" % (
        grl_dataset_name, len(filtered_files), task_id))
        files_to_store = [f"{x['scope']}:{x['name']}" for x in filtered_files]
        files_list = list(self.split_list(files_to_store, len(files_to_store) // 100 + 1))
        try:
            self.rucio_client.register_dataset(grl_dataset_name, files_list[0])
            for files in files_list[1:]:
                if files:
                    self.rucio_client.register_files_in_dataset(grl_dataset_name, files)
        except Exception as ex:
            raise GRLInputException(str(ex))


    def _find_grl_dataset_input_name(self, input_dataset_name, filtered_files):
        if input_dataset_name.endswith('RAW'):
            input_dataset_name = input_dataset_name + '.'
        previous_datasets = self.rucio_client.list_datasets_by_pattern('{base}_sub*_flt'.format(base=input_dataset_name))
        versions = [0]
        for dataset in previous_datasets:
            versions.append(int(dataset[dataset.find('_sub') + len('_sub'):dataset.find('_flt')]))
            if len(filtered_files) == self.rucio_client.get_number_files(dataset):
                file_to_check = self.rucio_client.list_files_name_in_dataset(dataset)
                if not [x for x in filtered_files if x['name'] not in file_to_check]:
                    return dataset, False
        version = max(versions) + 1
        return '{base}_sub{version:04d}_flt'.format(base=input_dataset_name,version=version), True

    def _find_optimal_evnt_offset(self, task_name):
        previous_task_list = ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']),
                                                  name=task_name)
        max_previous_offset = 0
        for task in previous_task_list:
            jedi_task = TTask.objects.get(id=task.id)
            task_params = json.loads(jedi_task._jedi_task_parameters)
            old_offset = 0
            old_offset_is_not_found = True
            is_optimal = False
            for param in task_params['jobParameters']:
                if 'dataset' in list(param.keys()) and param['dataset'] == 'seq_number':
                    old_offset = param['offset']
                    old_offset_is_not_found = False
                    break
            for param in task_params['jobParameters']:
                if 'firstEvent=${SEQNUMBER' in param.get('value',''):
                    is_optimal = True
            if ('nFiles' not in task_params) or ('nFilesPerJob' not in task_params) or old_offset_is_not_found:
                raise Exception("Something wrong with optimal first event settings")
            if is_optimal:
                max_previous_offset = max(max_previous_offset,old_offset + math.ceil(int(task_params['nFiles'])/int(task_params['nFilesPerJob'])))
            else:
                max_previous_offset = max(max_previous_offset,old_offset + int(task_params['nFiles']))
        return max_previous_offset



    def _find_tag_fold(self, version_list, folding_prod_step):
        if ProductionTask.objects.filter(ami_tag=version_list[-1], status__in=['done','finished']).exists():
            task = ProductionTask.objects.filter(ami_tag=version_list[-1], status__in=['done','finished']).latest('id')
            parent_dataset = task.inputdataset
            while folding_prod_step not in parent_dataset:
                if 'tid' in parent_dataset:
                    task_id = self._get_parent_task_id_from_input(parent_dataset)
                    if task_id !=0:
                        parent_dataset =  ProductionTask.objects.get(id=task_id).inputdataset
                    else:
                        break
                else:
                    break
            if folding_prod_step in parent_dataset:
                if '_tid' in parent_dataset:
                    parent_dataset = parent_dataset.split('_tid')[0]
                tag =  parent_dataset.split('_')[-1]
                if tag in version_list:
                    return tag
                else:
                    raise Exception("Tag folding is not possible, different tag is used in the past, please create a new AMItag, old tag: {0}".format(tag))
            else:
                raise Exception("Something wrong with tag folding, {0} is not found in parent tasks".format(folding_prod_step))
        else:
            return ''

    def _fold_single_tag_style(self, version_list):
        new_version = []
        first_letter = ''
        for token in version_list:
            if token[0] != first_letter and not (token[0] == 's' and first_letter == 'a'):
                new_version.append(token)
            first_letter = token[0]
        return new_version
    def _check_tag_folding(self, version_list, trf_name):
        if trf_name.lower() == 'ReSim_tf.py'.lower():
            # Fold tags for ReSim case. remove old sim and sim merge tag
            logger.info('Try to find previous tasks for ReSim tag folding {0}'.format(str(version_list),trf_name))

            simul_tag = self._find_tag_fold(version_list, 'simul')
            if not simul_tag:
                simul_tag = [tag for tag in version_list if not tag.startswith('e')][0]
                logger.info('New ReSim tag folding pair {0} - {1}'.format(str(simul_tag),version_list[-1]))
            new_version_list = []
            for version in version_list:
                if version != simul_tag:
                    new_version_list.append(version)
                else:
                    break
            if version_list[-1] not in new_version_list:
                new_version_list.append(version_list[-1])
            return new_version_list
        elif trf_name.lower() in ['NTUPMerge_tf.py'.lower(), 'POOLtoEI_tf.py'.lower()] and sum([len(x) for x in version_list]) >= 45:
            folded_versions = self._fold_single_tag_style(version_list[:-1])
            return folded_versions + version_list[-1:]
        return version_list



    def _construct_taskname(self, input_data_name, project, prod_step, ctag_name, trf_name):
        input_data_dict = self.parse_data_name(input_data_name)
        version_list = list()
        old_version = input_data_dict['version']
        if old_version:
            result = re.match(r'^.*(?P<tid>_tid\d+_\d{2})$', old_version)
            if result:
                old_version = old_version.replace(result.groupdict()['tid'], '')
            version_list.extend(old_version.split('_'))
        version_list = [version for version in version_list if ('part' not in version_list) and ('all' not in version_list)]
        version_list.append(ctag_name)
        version_list = self._check_tag_folding(version_list,trf_name)
        version = '_'.join(version_list)
        input_data_dict.update({'project': project, 'prod_step': prod_step, 'version': version})
        name_template = Template(TaskDefConstants.DEFAULT_TASK_NAME_TEMPLATE)
        return name_template.render(Context(input_data_dict, autoescape=False))


    def _construct_output(self, input_data_dict, project, prod_step, ctag_name, data_type, task_id, trf_name):
        version_list = list()
        old_version = input_data_dict['version']
        if old_version:
            result = re.match(r'^.*(?P<tid>_tid\d+_\d{2})$', old_version)
            if result:
                old_version = old_version.replace(result.groupdict()['tid'], '')
            version_list.extend(old_version.split('_'))
        version_list = [version for version in version_list if ('part' not in version_list) and ('all' not in version_list)]
        version_list.append(ctag_name)
        version_list = self._check_tag_folding(version_list,trf_name)
        version = '_'.join(version_list)
        input_data_dict.update({'project': project,
                                'prod_step': prod_step,
                                'version': version,
                                'data_type': data_type,
                                'task_id': task_id})
        output_template = Template(TaskDefConstants.DEFAULT_TASK_OUTPUT_NAME_TEMPLATE)
        return output_template.render(Context(input_data_dict, autoescape=False))

    def _get_output_params(self, input_data_name, output_types, project, prod_step, ctag_name, task_id, trf_name):
        # returns output_params = {'outputAODFile': [...], 'outputEVNTFile': [...], ...}
        output_params = dict()
        for output_type in output_types:
            output_dataset_name = self._construct_output(self.parse_data_name(input_data_name),
                                                         project,
                                                         prod_step,
                                                         ctag_name,
                                                         output_type,
                                                         task_id, trf_name)
            self._add_output_dataset_name(output_dataset_name, output_params)
        return output_params

    @staticmethod
    def _normalize_parameter_value(name, value, sub_steps):
        if not value:
            return value

        enclosed_value = False

        value = value.replace('%0B', ' ').replace('%2B', '+').replace('%9B', '; ').replace('%3B', ';')
        value = value.replace('"', '%8B').replace('%2C', ',')

        if re.match('^(--)?asetup$', name, re.IGNORECASE) or re.match('^(--)?triggerConfig$', name, re.IGNORECASE):
            return value.replace('%8B', '"').replace('%8C', '"')

        value = value.replace('%8B', '\\"').replace('%8C', '\\"')

        if re.match('^(--)?reductionConf$', name, re.IGNORECASE):
            enclosed_value = True
        elif re.match('^(--)?formats', name, re.IGNORECASE):
            enclosed_value = True
        elif re.match('^(--)?validationFlags$', name, re.IGNORECASE):
            enclosed_value = True
        elif re.match('^(--)?athenaopts$', name, re.IGNORECASE):
            value = value.encode().decode('unicode_escape')
        elif re.match('^(--)?extraParameter$', name, re.IGNORECASE):
            enclosed_value = True

        while value.find('\\\\"') >= 0 and not re.match('^(--)?athenaopts$', name, re.IGNORECASE):
            value = value.replace('\\\\"', '\\"')

        if value.replace('\\', '')[0] == '"' and value.replace('\\', '')[-1] == '"':

            enclosed_value = True

            if len(value) >= 2:
                if value[0:2] == '\\"':
                    # remove \\ from start if enclosed string
                    value = value[1:]
                if value[-2:] == '\\"':
                    # remove \\ from end if enclosed string
                    value = '%s"' % value[:-2]

        # escape all Linux spec chars
        if value.find(' ') >= 0 or value.find('(') >= 0 or value.find('=') >= 0 or value.find('*') >= 0 \
                or value.find(';') >= 0 or value.find('{') >= 0 or value.find('}') >= 0 \
                or re.match('^(--)?ignorePatterns', name, re.IGNORECASE):
            if not enclosed_value:
                value = '"%s"' % value

        # support for transformation sub_steps
        if sub_steps is not None:
            sub_step_exists = False
            for sub_step in sub_steps:
                if "%s:" % sub_step in value:
                    sub_step_exists = True
                    break
            if sub_step_exists:
                sub_values = list()
                sep = ' '
                for sub_value in value.split(sep):
                    if len(sub_value) >= 2:
                        if sub_value[0:2] == '\\"':
                            sub_value = sub_value[1:]
                        if sub_value[-2:] == '\\"':
                            sub_value = '%s"' % sub_value[:-2]
                    sub_values.append(sub_value)
                value = ' '.join(sub_values)

        return value

    def _get_parameter_value(self, name, source_dict, sub_steps=None):
        name = name.lower()
        for key in list(source_dict.keys()):
            param_name_prefix = '--'
            key_name = key
            if key_name.startswith(param_name_prefix):
                key_name = key_name[len(param_name_prefix):]
            if re.match("^(%s)?%s$" % (param_name_prefix, key_name), name, re.IGNORECASE) \
                    and str(source_dict[key]).lower() != 'none'.lower():
                if not (isinstance(source_dict[key], str) or isinstance(source_dict[key], str)):
                    return source_dict[key]
                if self.ami_client.is_new_ami_tag(source_dict):
                    return source_dict[key]
                value = re.sub(' +', ' ', source_dict[key])
                if name.find('config') > 0 and value.find('+') > 0:
                    value = ','.join(value.split('+'))
                if name.find('config') > 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('include') > 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('release') == 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('d3pdval') == 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('trigfilterlist') == 0 and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name == '--hepevttrigger' and value.find(' ') > 0:
                    value = ','.join(value.split(' '))
                if name.find('exec') > 0:
                    value = value.replace('%3B', ';').replace(' ;', ';').replace('; ', ';').replace('%2C', ',').replace(
                        '%2B', '+')
                    # support for AMI escaping
                    value = value.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
                    if value.find('from') == 0:
                        value_parts = value.split(' ')
                        value = ' '.join(value_parts[:4])
                        if len(value_parts) > 4:
                            value += ",%s" % ','.join(value_parts[4:])
                    elif value.find(' ') > 0:
                        value = ','.join(value.split(' '))
                return self._normalize_parameter_value(name, value, sub_steps)
        return ''

    def _get_latest_db_release(self):
        data_dict = self.rucio_client.get_datasets_and_containers(TaskDefConstants.DEFAULT_DB_RELEASE_DATASET_NAME_BASE)
        dataset_list = sorted(data_dict['datasets'], reverse=True)
        return dataset_list[0]

    @staticmethod
    def _get_parameter_name(name, source_dict):
        param_name = None
        name = name.lower()
        for key in list(source_dict.keys()):
            if re.match("^(--)?%s.*$" % key, name, re.IGNORECASE):
                param_name = key
                break
        if param_name:
            return param_name
        else:
            raise Exception("%s parameter is not found" % name)

    @staticmethod
    def _get_input_output_param_name(params, input_type, extended_pattern=False):
        if extended_pattern:
            pattern = r'^(--)?(input|output).*%s.*File$' % input_type
        else:
            pattern = r'^(--)?(input|output)%s.*File$' % input_type
        for key in list(params.keys()):
            if re.match(pattern, key, re.IGNORECASE):
                return key
        return None

    def _get_output_params_order(self, task_proto_dict):
        order_dict = dict()
        count = 0
        for param in task_proto_dict['job_params']:
            if 'param_type' in list(param.keys()):
                if param['param_type'] == 'output':
                    order_dict.update({self.parse_data_name(param['dataset'])['data_type']: count})
                    count += 1
        return order_dict

    @staticmethod
    def _get_primary_input(job_parameters):
        for job_param in job_parameters:
            if 'param_type' not in list(job_param.keys()) or job_param['param_type'].lower() != 'input'.lower():
                continue
            if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                if not result:
                    continue
                in_type = result.groupdict()['intype']
                if in_type.lower() == 'logs'.lower() or re.match(r'^(Low|High)PtMinbias.*$', in_type, re.IGNORECASE):
                    continue
                return job_param
        return None




    @staticmethod
    def _get_job_parameter(name, job_parameters):
        for job_param in job_parameters:
            if re.match(r"^(--)?%s" % name, job_param['value'], re.IGNORECASE):
                return job_param
        return None

    def _check_number_of_events(self, step, project_mode):
        if step.request.request_type.lower() == 'MC'.lower():
            number_of_events = int(step.input_events)
            project = self._get_project(step)
            bunchspacing = project_mode.bunchspacing
            campaign = ':'.join([_f for _f in (step.request.campaign, step.request.subcampaign, bunchspacing,) if _f])
            if number_of_events > 0:
                small_events_numbers = dict()
                small_events_numbers.update({
                    r'mc15_13TeV': 10000,
                    r'mc16_13TeV': 2000,
                    r'mc15:mc15(a|b|c)': 10000,
                    r'mc16:mc16(a|b|c|\*)': 2000
                })
                small_events_threshold = 0
                for pattern in list(small_events_numbers.keys()):
                    if re.match(pattern, project, re.IGNORECASE) or re.match(pattern, campaign, re.IGNORECASE):
                        small_events_threshold = small_events_numbers[pattern]
                        break
                if number_of_events < small_events_threshold:
                    force_small_events = project_mode.isSmallEvents or False
                    if not force_small_events:
                        raise TaskSmallEventsException(number_of_events)

    def _enum_previous_tasks(self, task_id, data_type, list_task_id):
        task = TTask.objects.get(id=task_id)
        task_params = json.loads(task._jedi_task_parameters)['jobParameters']
        primary_input = self._get_primary_input(task_params)
        if primary_input:
            dsn = primary_input['dataset']
            if re.match(TaskDefConstants.DEFAULT_DATA_NAME_PATTERN, dsn):
                parent_id = self._get_parent_task_id_from_input(dsn)
                dsn_dict = self.parse_data_name(dsn)
                if parent_id and dsn_dict['data_type'] == data_type:
                    list_task_id.append(parent_id)
                    self._enum_previous_tasks(parent_id, data_type, list_task_id)

    def _enum_all_previous_tasks(self, task_id, list_task_id):
        task = TTask.objects.get(id=task_id)
        task_params = json.loads(task._jedi_task_parameters)['jobParameters']
        primary_input = self._get_primary_input(task_params)
        if primary_input:
            dsn = primary_input['dataset']
            if re.match(TaskDefConstants.DEFAULT_DATA_NAME_PATTERN, dsn):
                parent_id = self._get_parent_task_id_from_input(dsn)
                if parent_id:
                    list_task_id.append(parent_id)
                    self._enum_all_previous_tasks(parent_id, list_task_id)

    def _enum_next_tasks(self, task_id, data_type, list_task_id):
        next_task_list = ProductionTask.objects.filter(primary_input__endswith='_tid{0}_00'.format(task_id),
                                                       output_formats__contains=data_type,
                                                       primary_input__contains=data_type)
        for next_task in next_task_list:
            list_task_id.append(int(next_task.id))
            self._enum_next_tasks(int(next_task.id), data_type, list_task_id)

    def _extract_chain_input_from_datasets(self, dataset_name):
        result = re.match(r'^.+_tid(?P<tid>\d+)_00$', dataset_name)
        if result:
            parent_task = ProductionTask.objects.get(id=int(result.groupdict()['tid']))
            if parent_task.status in ['done']:
                return parent_task.total_events
            if ('evgen' in parent_task.name) and (parent_task.step.input_events > 0):
                return parent_task.step.input_events
            if parent_task.number_of_files and parent_task.events_per_job:
                return parent_task.number_of_files * parent_task.events_per_file
            if '_tid' in parent_task.primary_input:
                return  self._extract_chain_input_from_datasets(parent_task.primary_input)
        return -1


    def _get_total_number_of_jobs(self, task, nevents):
        nevents_per_job = task.get('nEventsPerJob', 0)

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return
        if nevents > 0:
            number_of_jobs = nevents / nevents_per_job
        else:
            total_nevents = 0
            try:
                total_nevents = self.rucio_client.get_number_events(dsn)
                if not total_nevents:
                    raise EmptyDataset()
            except:
                chain_input_events = self._extract_chain_input_from_datasets(dsn)
                if chain_input_events > 0:
                    total_nevents = chain_input_events
            number_of_jobs = total_nevents / nevents_per_job
        return  number_of_jobs


    def _extract_chain_input_events(self, step):
        if step.step_parent_id == step.id:
            return step.input_events

        parent_step = StepExecution.objects.get(id=step.step_parent_id)

        if parent_step.status.lower() != self.protocol.STEP_STATUS[StepStatus.APPROVED].lower():
            return step.input_events

        if parent_step.input_events != -1:
            return parent_step.input_events

        return self._extract_chain_input_events(parent_step)

    def _get_result_number_of_jobs(self, task, nevents, step):
        number_of_jobs = 0
        nevents_per_job = task.get('nEventsPerJob', 0)
        nfiles_per_job = task.get('nFilesPerJob', 0)

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        requested_nevents = 0

        if nevents > 0:
            number_of_jobs = nevents / nevents_per_job
            requested_nevents = nevents
        else:
            total_nevents = 0
            try:
                total_nevents = self.rucio_client.get_number_events(dsn)
                if not total_nevents:
                    raise EmptyDataset()
            except:
                chain_input_events = self._extract_chain_input_events(step)
                if chain_input_events > 0:
                    total_nevents = chain_input_events
            requested_nevents = total_nevents
            if nevents_per_job != 0:
                number_of_jobs = total_nevents / nevents_per_job
            else:
                number_of_jobs = total_nevents / nfiles_per_job
        return  number_of_jobs, requested_nevents


    def _check_task_number_of_jobs(self, task, nevents, step):
        number_of_jobs, requested_nevents = self._get_result_number_of_jobs(task, nevents, step)

        if number_of_jobs >= TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK / 5 or \
                requested_nevents < TaskDefConstants.NO_ES_MIN_NUMBER_OF_EVENTS:
            task['esConvertible'] = False

        if requested_nevents >= TaskDefConstants.NO_ES_MIN_NUMBER_OF_EVENTS:
            task['skipShortInput'] = True

    def _check_task_recreated(self, task, step):
        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return False

        dsn = primary_input['dataset']
        if not dsn:
            return False
        if dsn.startswith('group') or dsn.startswith('user'):
            return False
        task_id = self._get_parent_task_id_from_input(dsn)
        if task_id == 0:
            return False
        parent_task = ProductionTask.objects.get(id=task_id)
        hashtag = HashTag.objects.get(hashtag=TaskDefConstants.MC_DELETED_REPROCESSING_REQUEST_HASHTAG)
        if not HashTagToRequest.objects.filter(request=step.request,hashtag=hashtag).exists() and not parent_task.hashtag_exists(TaskDefConstants.MC_DELETED_REPROCESSING_REQUEST_HASHTAG):
            return False
        all_previous_tasks = list()
        self._enum_all_previous_tasks(parent_task.id,all_previous_tasks)
        all_previous_tasks.append(parent_task.id)
        all_previous_tasks_set = set(all_previous_tasks)
        output_formats = set(step.step_template.output_formats.split('.'))
        similar_tasks = ProductionTask.objects.filter(name=task['taskName'])
        for similar_task in similar_tasks:
            if similar_task.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                continue
            similar_task_output_formats = set(similar_task.output_formats.split('.'))
            if len(output_formats.intersection(similar_task_output_formats)) > 0:
                similar_task_previous_tasks = list()
                self._enum_all_previous_tasks(similar_task.id,similar_task_previous_tasks)
                similar_task_previous_task_set = set(similar_task_previous_tasks)
                if len(all_previous_tasks_set.intersection(similar_task_previous_task_set))>0:
                    previous_output_status_dict = \
                        self.task_reg.check_task_output(similar_task.id, output_formats)
                    for requested_output_type in output_formats:
                        if requested_output_type not in list(previous_output_status_dict.keys()):
                            continue
                        if previous_output_status_dict[requested_output_type]:
                            logger.info('Duplication found during deep check')

                            raise TaskDuplicateDetected(similar_task.id, 1,
                                                        request=step.request.reqid,
                                                        slice=step.slice.slice,
                                                        processed_formats='.'.join(similar_task_output_formats),
                                                        requested_formats='.'.join(output_formats),
                                                        tag=step.step_template.ctag)
        return True



    def _check_task_merged_input(self, task, step, prod_step):
        # skip EI tasks
        if step.request.request_type.lower() == 'EVENTINDEX'.lower():
            return

        # skip evgen/merging/super-merging
        if prod_step.lower() == 'merge'.lower() or prod_step.lower() == 'evgen'.lower():
            return

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        dsn_dict = self.parse_data_name(dsn)

        if dsn_dict['prod_step'].lower() == 'merge'.lower():
            return

        task_id = self._get_parent_task_id_from_input(dsn)
        if task_id == 0:
            return

        next_tasks = list()
        self._enum_next_tasks(task_id, dsn_dict['data_type'], next_tasks)
        if not next_tasks:
            return

        for next_task_id in next_tasks:
            task_list = ProductionTask.objects.filter(project=step.request.project,
                                                      ami_tag=step.step_template.ctag,
                                                      primary_input__endswith='_tid{0}_00'.format(next_task_id))
            for prod_task in task_list:

                if prod_task.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                    continue
                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = prod_task.output_formats
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue

                raise MergedInputProcessedException(prod_task.id)

    def _task_full_chain(self, step, parent_task_id, project_mode):
        for cloud, cloud_dict in TaskDefConstants.FULL_CHAIN.items():
            if cloud_dict['project_mode'] in project_mode.project_mode_dict:
                return cloud
        if parent_task_id:
            task = ProductionTask.objects.get(id=parent_task_id)
            for cloud, cloud_dict in TaskDefConstants.FULL_CHAIN.items():
                if cloud_dict['hashtag'] in  [x.hashtag for x in task.hashtags]:
                    return cloud
        return None


    def _set_task_full_chain(self, task_config, project_mode, task_full_chain):
        cloud_dict = TaskDefConstants.FULL_CHAIN[task_full_chain]
        if 'site' in cloud_dict:
            project_mode.site = cloud_dict['site']
        if 'nucleus' in cloud_dict:
            project_mode.nucleus = cloud_dict['nucleus']
        if 'token' in cloud_dict:
            task_config['token'] = cloud_dict['token']
        project_mode.useDestForLogs = True
        project_mode.disableReassign = True
        return cloud_dict['hashtag']

    def set_jedi_full_chain(self, task_config, parent_task_id, project_mode):
        if not project_mode.fullChain and parent_task_id:
            parent_task  = ProductionTask.objects.get(id=parent_task_id)
            ttask = TTask.objects.get(id=parent_task_id)
            if parent_task.hashtag_exists(TaskDefConstants.JEDI_FULL_CHAIN) or ttask._get_task_params().get('fullChain')=='capable':
                task_config['full_chain'] = 'capable'


    def _check_task_unmerged_input(self, task, step, prod_step):
        # skip EI tasks
        if step.request.request_type.lower() == 'EVENTINDEX'.lower():
            return

        # skip evgen/merging/super-merging
        if prod_step.lower() == 'merge'.lower() or prod_step.lower() == 'evgen'.lower():
            return

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        dsn_dict = self.parse_data_name(dsn)

        if dsn_dict['prod_step'].lower() != 'merge'.lower():
            return

        merge_task_id = self._get_parent_task_id_from_input(dsn)
        if merge_task_id == 0:
            return

        previous_tasks = list()
        self._enum_previous_tasks(merge_task_id, dsn_dict['data_type'], previous_tasks)
        if not previous_tasks:
            return

        for previous_task_id in previous_tasks:
            task_list = ProductionTask.objects.filter(project=step.request.project,
                                                      ami_tag=step.step_template.ctag,
                                                      primary_input__endswith='_tid{0}_00'.format(previous_task_id))
            for prod_task in task_list:
                if prod_task.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                    continue

                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = prod_task.output_formats
                processed_output_types = [e for e in requested_output_types if e in previous_output_types.split('.')]
                if not processed_output_types:
                    continue

                raise UnmergedInputProcessedException(prod_task.id)

    def _check_task_cache_version_consistency(self, task, step, trf_release):
        if step.request.request_type.lower() != 'GROUP'.lower():
            return

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        dsn_dict = self.parse_data_name(dsn)

        if dsn_dict['data_type'].lower() != 'AOD'.lower():
            return

        parent_task_id = self._get_parent_task_id_from_input(dsn)
        if parent_task_id == 0:
            return

        previous_tasks = list()
        previous_tasks.append(parent_task_id)
        self._enum_previous_tasks(parent_task_id, dsn_dict['data_type'], previous_tasks)
        if not previous_tasks:
            return

        for previous_task_id in previous_tasks:
            previous_task = ProductionTask.objects.get(id=previous_task_id)
            previous_task_ctag = self._get_ami_tag_cached(previous_task.ami_tag)
            previous_task_prod_step = self._get_prod_step(previous_task.ami_tag, previous_task_ctag)
            if previous_task_prod_step.lower() == 'merge'.lower():
                continue
            previous_task_trf_release = previous_task_ctag['SWReleaseCache'].split('_')[1]
            if int(trf_release.split('.')[0]) != int(previous_task_trf_release.split('.')[0]):
                raise WrongCacheVersionUsedException(trf_release, previous_task_trf_release)

    def _check_task_blacklisted_input(self, task, project_mode):
        enabled = project_mode.preventBlacklistedInput or False

        if not enabled:
            return

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            return

        dsn = primary_input['dataset']
        if not dsn:
            return

        dataset_rses = self.rucio_client.get_dataset_rses(dsn)
        blacklisted_rses = self.agis_client.get_blacklisted_rses()

        if len(dataset_rses) == 0 or len(blacklisted_rses) == 0:
            return

        for rse in dataset_rses:
            if rse not in blacklisted_rses:
                return

        raise BlacklistedInputException(dataset_rses)


    @staticmethod
    def translate_sub_dataset(dataset):
        if '_flt' in dataset:
            base_dataset =  dataset.split('_sub')[0]
            if base_dataset.endswith('.'):
                return base_dataset[:-1]
            return base_dataset
        return dataset

    def _check_task_input(self, task, task_id, number_of_events, task_config, parent_task_id, input_data_name, step,
                          primary_input_offset=0, prod_step=None, reuse_input=None, evgen_params=None,
                          task_common_offset=None):
        project_mode = ProjectMode(step)
        if (prod_step.lower() == 'evgen'.lower()) and number_of_events > TaskDefConstants.DEFAULT_MAX_EVENTS_EVGEN_TASK:
            raise MaxEventsPerTaskLimitExceededException(number_of_events)

        primary_input = self._get_primary_input(task['jobParameters'])
        if not primary_input:
            logger.info("Task Id = %d, No primary input. Checking of input is skipped" % task_id)
            return
        check_optimal_events_violation = False
        if prod_step.lower() == 'evgen'.lower():
            check_optimal_events_violation = 'SEQNUMBER' not in self._get_job_parameter('firstEvent', task['jobParameters'])['value']
        if prod_step.lower() == 'merge'.lower():
            dsn = primary_input['dataset']
            tag_name = step.step_template.ctag
            version = self.parse_data_name(dsn)['version']
            merge_nevents_per_job = task.get('nEventsPerJob', 0)
            merge_nevents_per_input_file = task.get('nEventsPerInputFile', 0)
            merge_nfiles_per_job = task.get('nFilesPerJob', 0)
            merge_ngb_per_job = task.get('nGBPerJob', 0)
            if str(version.split('_tid')[0]).endswith(tag_name):
                is_merge_1_to_1 = True
                if merge_nevents_per_job > 0 and merge_nevents_per_input_file > 0:
                    if merge_nevents_per_job != merge_nevents_per_input_file:
                        is_merge_1_to_1 = False
                elif merge_nfiles_per_job > 1:
                    is_merge_1_to_1 = False
                elif merge_ngb_per_job > 0:
                    is_merge_1_to_1 = False
                if is_merge_1_to_1:
                    raise InvalidMergeException(dsn, tag_name)

            if (merge_nevents_per_job > 0) and (merge_nevents_per_input_file > 0) and \
                    (merge_nevents_per_job < merge_nevents_per_input_file) and \
                    not((merge_nfiles_per_job > 1) or (merge_ngb_per_job > 0)):
                raise MergeInverseException(merge_nevents_per_input_file, merge_nevents_per_job)

        lost_files_exception = None

        try:
            primary_input_dataset = primary_input['dataset'].split(':')[-1]
            prod_dataset = ProductionDataset.objects.filter(
                name=primary_input_dataset).first()
            if not prod_dataset:
                primary_input_dataset = f"{primary_input_dataset.split('.')[0]}:{primary_input_dataset}"
                prod_dataset = ProductionDataset.objects.filter(
                    name=primary_input_dataset).first()
            if prod_dataset:
                prod_task = ProductionTask.objects.filter(id=prod_dataset.task_id).first()
                if prod_task:
                    if prod_task.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                        raise ParentTaskInvalid(prod_task.id, prod_task.status)
                if prod_dataset.ddm_status and prod_dataset.ddm_status == TaskDefConstants.DDM_LOST_STATUS:
                    raise InputLostFiles(prod_dataset.name)
        except InputLostFiles as ex:
            lost_files_exception = ex

        try:
            if lost_files_exception:
                if step.request.jira_reference:
                    jira_client = JIRAClient()
                    jira_client.authorize()
                    jira_client.log_exception(step.request.jira_reference, lost_files_exception)
        except Exception as ex:
            logger.exception('Exception occurred: {0}'.format(ex))

        if 'nEventsPerInputFile' not in list(task_config.keys()):
            nevents_per_files = self.get_events_per_file(primary_input['dataset'])
            if not nevents_per_files:
                logger.info(
                    "Step = {0}, nEventsPerInputFile is unavailable (dataset = {1})".format(step.id, input_data_name))
            task_config['nEventsPerInputFile'] = nevents_per_files
            log_msg = "_check_task_input, step = %d, input_data_name = %s, found nEventsPerInputFile = %d" % \
                      (step.id, input_data_name, task_config['nEventsPerInputFile'])
            logger.info(log_msg)

        primary_input_total_files = 0
        primary_input_events_from_rucio = 0
        try:
            primary_input_total_files = self.rucio_client.get_number_files(primary_input['dataset'])
            primary_input_events_from_rucio = self.rucio_client.get_number_events(primary_input['dataset'])
        except Exception:
            logger.info('_check_task_input, get_number_files or get_number_events for {0} failed (parent_task_id = {2}): {1}'.format(
                primary_input['dataset'], get_exception_string(), parent_task_id))
            task_output_name_suffix = '_tid{0}_00'.format(TaskDefConstants.DEFAULT_TASK_ID_FORMAT % parent_task_id)
            if not str(primary_input['dataset']).endswith(task_output_name_suffix):
                raise NumberOfFilesUnavailable(primary_input['dataset'], get_exception_string())

        logger.info("primary_input_total_files={0} ({1})".format(primary_input_total_files, primary_input['dataset']))

        if primary_input_total_files > 0:
            self.verify_data_uniform(step, primary_input['dataset'])

        number_of_jobs = 0
        if number_of_events > 0 and 'nEventsPerJob' in list(task_config.keys()):
            number_of_jobs = number_of_events / int(task_config['nEventsPerJob'])
        else:
            if 'nEventsPerInputFile' in list(task_config.keys()) and 'nEventsPerJob' in list(task_config.keys()) \
                    and primary_input_total_files > 0:
                number_of_jobs = \
                    math.ceil(primary_input_total_files * int(task_config['nEventsPerInputFile']) / int(task_config['nEventsPerJob']))
            if 'nEventsPerJob' in list(task_config.keys()) and project_mode.useRealNumEvents and \
                    primary_input_events_from_rucio>0:
                number_of_jobs = \
                    math.ceil(primary_input_events_from_rucio / int(task_config['nEventsPerJob']))



        if number_of_jobs > TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK:
            raise MaxJobsPerTaskLimitExceededException(number_of_jobs)

        if task_common_offset:
            task_common_offset_hashtag = TaskDefConstants.DEFAULT_TASK_COMMON_OFFSET_HASHTAG_FORMAT.format(
                task_common_offset
            )
            try:
                hashtag = HashTag.objects.get(hashtag=task_common_offset_hashtag)
            except ObjectDoesNotExist:
                hashtag = HashTag(hashtag=task_common_offset_hashtag, type='UD')
                hashtag.save()
            dsn_no_scope = primary_input['dataset'].split(':')[-1]
            for task_same_hashtag in hashtag.tasks:
                if task_same_hashtag.status in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                    continue
                task_params = json.loads(TTask.objects.get(id=task_same_hashtag.id)._jedi_task_parameters)
                task_input = self._get_primary_input(task_params['jobParameters'])
                task_dsn_no_scope = task_input['dataset'].split(':')[-1]
                if task_dsn_no_scope == dsn_no_scope:
                    current_offset = int(task_input['offset']) + int(task_params['nFiles'])
                    primary_input_offset = current_offset
                    break

        number_of_input_files_used = 0
        previous_tasks = list()
        ps1_task_list = []
        # search existing task with same input_data_name and tag in ProdSys1 and ProdSys2
        # ps1_task_list = TTaskRequest.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete']),
        #                                             project=step.request.project,
        #                                             inputdataset=input_data_name,
        #                                             ctag=step.step_template.ctag,
        #                                             formats=step.step_template.output_formats)
        for ps1_task in ps1_task_list:
            previous_tasks.append(int(ps1_task.reqid))
            total_r_jobs = ps1_task.total_req_jobs or 0
            number_of_input_files_used += \
                int(((ps1_task.total_events / ps1_task.events_per_file) / ps1_task.total_req_jobs or 0) * total_r_jobs)

        task_list = ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']),
                                                  project=step.request.project,
                                                  # inputdataset=input_data_name,
                                                  step__step_template__ctag=step.step_template.ctag).filter(
            Q(inputdataset=input_data_name) |
            # Q(inputdataset__endswith=input_data_name.split(':')[-1]) |
            Q(inputdataset__contains=input_data_name.split('/')[0].split(':')[-1]) |
            Q(step__slice__dataset=input_data_name) |
            # Q(step__slice__input_dataset__endswith=input_data_name.split(':')[-1]) |
            Q(step__slice__dataset__contains=input_data_name.split('/')[0].split(':')[-1]) |
            Q(step__slice__input_data=input_data_name) |
            # Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])
            Q(step__slice__input_data__contains=input_data_name.split('/')[0].split(':')[-1])
        )

        for prod_task_existing in task_list:

            # comparing output formats
            requested_output_types = step.step_template.output_formats.split('.')
            previous_output_types = prod_task_existing.step.step_template.output_formats.split('.')
            processed_output_types = [e for e in requested_output_types if e in previous_output_types]
            if not processed_output_types:
                continue

            # FIXME: support for _Cont% (ContF, ..., ContJfinal)
            container_name = "%s_Cont" % input_data_name.split('/')[0].split(':')[-1]
            if container_name in prod_task_existing.step.slice.dataset:
                continue

            task_id = int(prod_task_existing.id)
            previous_tasks.append(task_id)
            jedi_task_existing = TTask.objects.get(id=prod_task_existing.id)
            task_existing = json.loads(jedi_task_existing._jedi_task_parameters)

            if 'use_real_nevents' in list(task_existing.keys()):
                raise Exception('Extensions are not allowed if useRealNumEvents is specified')
            if check_optimal_events_violation:
                if 'SEQNUMBER' in self._get_job_parameter('firstEvent', task_existing['jobParameters'])['value']:
                    raise Exception(f'None optimal first event extensions are not allowed, previous task: {task_id}')
            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
            previous_dsn_no_scope = previous_dsn.split(':')[-1]
            if '_sub' in previous_dsn_no_scope:
                previous_dsn_no_scope = self.translate_sub_dataset(previous_dsn_no_scope)
            current_dsn = primary_input['dataset']
            current_dsn_no_scope = current_dsn.split(':')[-1]

            is_current_dsn_tid_type = bool(re.match(r'^.+_tid(?P<tid>\d+)_00$', current_dsn_no_scope, re.IGNORECASE))
            is_previous_dsn_tid_type = bool(re.match(r'^.+_tid(?P<tid>\d+)_00$', previous_dsn_no_scope, re.IGNORECASE))

            if is_current_dsn_tid_type != is_previous_dsn_tid_type:
                if not is_current_dsn_tid_type:
                    raise Exception('Mixed input for tasks with the same configuration is not allowed. ' +
                                    'Current input is {0}, the previous task ({1}) used {2} as input'.format(
                                        current_dsn_no_scope, task_id, previous_dsn_no_scope
                                    ))

            if current_dsn_no_scope != previous_dsn_no_scope:
                continue
            if project_mode.checkOutputDeleted and prod_task_existing.status in ['done', 'finished']:
                previous_output_status_dict = \
                    self.task_reg.check_task_output(task_id, requested_output_types)
                dataset_still_exists = False
                for requested_output_type in requested_output_types:
                    if previous_output_status_dict[requested_output_type]:
                        dataset_still_exists = True
                if not dataset_still_exists:
                    logger.info('Output {0} of task {1} is deleted'.format(
                        str(requested_output_types), task_id))
                    continue
            if prod_task_existing.status == 'done':
                if 'nFiles' in task_existing:
                    number_of_input_files_used += int(task_existing['nFiles'])
                else:
                    if 'nEventsPerJob' in task_existing and 'nEventsPerInputFile' in task_existing:
                        try:
                            jedi_dataset_info = JediDatasets.objects.get(id=jedi_task_existing.id,
                                                                        datasetname__contains=primary_input['dataset'])
                            number_files_finished = int(jedi_dataset_info.total_files_finished)
                            number_of_input_files = \
                                math.ceil(float(task_existing['nEventsPerJob']) / float(
                                    task_existing['nEventsPerInputFile']) * number_files_finished)
                            number_of_input_files_used += int(number_of_input_files)
                        except ObjectDoesNotExist:
                            current_dsn = primary_input['dataset']
                            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
                            if current_dsn == previous_dsn:
                                raise Exception('Task duplication candidate is found: task_id={0}. '.format(task_id) +
                                                '(The part of) input was already processed')
                    else:
                        raise TaskDuplicateDetected(task_id, 1,
                                                    request=step.request.reqid,
                                                    slice=step.slice.slice,
                                                    processed_formats='.'.join(processed_output_types),
                                                    requested_formats='.'.join(requested_output_types),
                                                    tag=step.step_template.ctag)
            elif prod_task_existing.status == 'finished':
                if 'nFiles' in task_existing:
                    number_of_input_files_used += int(task_existing['nFiles'])
                else:
                    if 'nEventsPerJob' in task_existing and 'nEventsPerInputFile' in task_existing:
                        try:
                            jedi_dataset_info = JediDatasets.objects.get(id=jedi_task_existing.id,
                                                                        datasetname__contains=primary_input['dataset'])
                            nfiles = int(jedi_dataset_info.nfiles)
                            number_of_input_files = \
                                math.ceil(float(task_existing['nEventsPerJob']) / float(
                                    task_existing['nEventsPerInputFile']) * nfiles)
                            number_of_input_files_used += int(number_of_input_files)
                        except ObjectDoesNotExist:
                            current_dsn = primary_input['dataset']
                            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
                            if current_dsn == previous_dsn:
                                raise Exception('Task duplication candidate is found: task_id={0}. '.format(task_id) +
                                                '(The part of) input was already processed')
                    else:
                        raise TaskDuplicateDetected(task_id, 3,
                                                    request=step.request.reqid,
                                                    slice=step.slice.slice,
                                                    processed_formats='.'.join(processed_output_types),
                                                    requested_formats='.'.join(requested_output_types),
                                                    tag=step.step_template.ctag)
            else:
                if 'nFiles' in task_existing:
                    number_of_input_files_used += int(task_existing['nFiles'])
                else:
                    raise TaskDuplicateDetected(task_id, 2,
                                                request=step.request.reqid,
                                                slice=step.slice.slice,
                                                processed_formats='.'.join(processed_output_types),
                                                requested_formats='.'.join(requested_output_types),
                                                tag=step.step_template.ctag)
            log_msg = '[NotERROR][Check duplicates] request={0}, chain={1} ({2}),'.format(
                step.request.reqid, step.slice.slice, step.id)
            log_msg += ' previous_task={0} ({1}), n_files_used={2}'.format(
                task_id, prod_task_existing.status, number_of_input_files_used)
            logger.debug(log_msg)

        if number_of_events > 0:
            number_input_files_requested = math.ceil(number_of_events / int(task_config['nEventsPerInputFile']))
            if 'nFiles' in task and task['nFiles'] > 0 and task['nFiles'] > number_input_files_requested:
                number_input_files_requested = task['nFiles']
        else:
            number_input_files_requested = primary_input_total_files - number_of_input_files_used

        if reuse_input:
            primary_input['offset'] = 0
            for param in task['jobParameters']:
                if 'dataset' in list(param.keys()) and param['dataset'] == 'seq_number':
                    param['offset'] = number_of_input_files_used
            return
        if ((number_input_files_requested + number_of_input_files_used) > primary_input_total_files) and primary_input_total_files == 0:
            raise Exception("Input container doesn't exist or empty")
        if ((number_input_files_requested + number_of_input_files_used) > primary_input_total_files and
            ((number_input_files_requested + number_of_input_files_used-primary_input_total_files)/primary_input_total_files)<0.01):
            number_input_files_requested = primary_input_total_files - number_of_input_files_used

        if (number_input_files_requested + number_of_input_files_used) > primary_input_total_files \
                or number_input_files_requested < 0:
            if number_input_files_requested < 0:
                logger.error('[ERROR] number_input_files_requested={0}, request={1}, chain={2} ({3})'.format(
                    number_input_files_requested, step.request.reqid, step.slice.slice, step.id))
            raise NoMoreInputFiles("No more input files. requested/used/total = %d/%d/%d, previous_tasks = %s" %
                                   (number_input_files_requested, number_of_input_files_used, primary_input_total_files,
                                    str(previous_tasks)))
        else:
            primary_input['offset'] = number_of_input_files_used
            random_seed_param = self._get_job_parameter('randomSeed', task['jobParameters'])
            if random_seed_param:
                random_seed_param['offset'] = number_of_input_files_used
            if prod_step.lower() == 'evgen'.lower():
                if  project_mode.optimalFirstEvent or (project_mode.optimalFirstEvent is None and task_config.get('optimalFirstEvent')):
                    max_offset = self._find_optimal_evnt_offset(task['taskName'])
                    random_seed_param['offset'] = max(max_offset,math.ceil(number_of_input_files_used / task['nFilesPerJob']))
                else:
                    events_per_file = int(task_config['nEventsPerInputFile'])
                    first_event_param = self._get_job_parameter('firstEvent', task['jobParameters'])
                    if first_event_param:
                        first_event_param['offset'] = number_of_input_files_used * events_per_file

        if evgen_params and prod_step.lower() == 'evgen'.lower():
            random_seed_param = self._get_job_parameter('randomSeed', task['jobParameters'])
            if random_seed_param:
                random_seed_param['offset'] = evgen_params['offset']
            first_event_param = self._get_job_parameter('firstEvent', task['jobParameters'])
            if first_event_param and not (project_mode.optimalFirstEvent or task_config.get('optimalFirstEvent')):
                first_event_param['offset'] = evgen_params['event_offset']

        if primary_input_offset:
            primary_input['offset'] = primary_input_offset

    @staticmethod
    def _get_merge_tag_name(step):
        task_config = ProjectMode.get_task_config(step)
        merging_tag_name = ''
        if 'merging_tag' in list(task_config.keys()):
            merging_tag_name = task_config['merging_tag']
        if not merging_tag_name:
            merging_tag_name = ProjectMode(step).merging
        return merging_tag_name


    def  _set_pre_stage(self, step, task_proto_dict, project_mode):
        # set staging if input is only on Tape
        if (not project_mode.noprestage and not project_mode.patchRepro and not project_mode.repeatDoneTaskInput
                and step.request.request_type in ['REPROCESSING', 'GROUP', 'MC','HLT']):
            primary_input = self._get_primary_input(task_proto_dict['job_params'])['dataset']
            if '_sub' in primary_input:
                return
            if self.rucio_client.dataset_exists(primary_input) and self.rucio_client.only_tape_replica(primary_input):
                sa = StepAction()
                task_config = ProjectMode.get_task_config(step)
                if task_config.get('PDA', '')  == 'preStageWithTaskArchive':
                    sa.action = StepAction.STAGING_ARCHIVE_ACTION
                else:
                    sa.action = StepAction.STAGING_ACTION
                sa.request = step.request
                sa.step = step.id
                sa.attempt = 0
                sa.create_time = timezone.now()
                sa.execution_time = timezone.now() + datetime.timedelta(minutes=2)
                sa.status = 'active'
                if not StepAction.objects.filter(step=int(step.id), action=sa.action,
                                                 status__in=['active', 'executing', 'verify']).exists():
                    sa.save()
                else:
                    old_step_action = StepAction.objects.get(step=int(step.id), action=sa.action,
                                                             status__in=['active', 'executing', 'verify'])
                    if old_step_action.status == 'verify':
                        old_step_action.status = 'active'
                        old_step_action.save()
                if project_mode.toStaging is None:
                    task_proto_dict.update({'to_staging': True})

                if project_mode.inputPreStaging is None:
                    task_proto_dict.update({'input_pre_staging': True})
                logger.info('Prestage is set for dataset {0}'.format(
                    primary_input))




    def _check_site_container(self, task_proto_dict):
        if ('site' in task_proto_dict) and (',' not in task_proto_dict['site']) and \
                ('BOINC' not in task_proto_dict['site']) and ('container_name' not in task_proto_dict):
            containers = self.agis_client.list_site_sw_containers(task_proto_dict['site'])
            architecture = task_proto_dict['architecture'].split('#')[0]
            if containers:
                sw_containers = []
                sw_tags = self.ami_client.ami_sw_tag_by_cache('_'.join([task_proto_dict['cache'],task_proto_dict['release_base']]))
                for sw_tag in sw_tags:
                    if sw_tag['STATE'] == 'USED':
                        images = self.ami_client.ami_image_by_sw(sw_tag['TAGNAME'])
                        for image in images:
                            sw_containers.append({'name': image['IMAGENAME'],
                                                  'cmtconfig': sw_tag['IMAGEARCH'] + '-' + sw_tag[
                                                     'IMAGEPLATFORM'] + '-' + sw_tag['IMAGECOMPILER']})
                for sw_container in sw_containers:
                    if (sw_container['cmtconfig'] == architecture) and (sw_container['name'] in containers):
                        task_proto_dict.update(
                            {'container_name': sw_container['name']})
                        return
                raise ContainerIsNotFoundException(task_proto_dict['site'])

    @staticmethod
    def split_list(input_list, n):
        k, m = divmod(len(input_list), n)
        return (input_list[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

    def _register_mc_overlay_dataset(self, mc_pileup_overlay, number_of_jobs, task_id, task, split_by_dataset=False):

        used_files = set()
        nevents_per_job = task.get('nEventsPerJob')
        for dataset in mc_pileup_overlay['datasets']:
            previous_task_id = self.rucio_client.dataset_metadata(dataset).get('task_id')
            if ProductionTask.objects.filter(id=previous_task_id).exists() and \
                    ProductionTask.objects.get(id=previous_task_id).status not in ['failed', 'broken', 'aborted', 'obsolete', 'toabort']:
                used_files.update(self.rucio_client.list_files_with_scope_in_dataset(dataset))
        events_per_pileup_file = self.rucio_client.get_number_events(mc_pileup_overlay['files'][0])
        pileup_files_per_job = nevents_per_job // events_per_pileup_file
        if pileup_files_per_job == 0:
            if (mc_pileup_overlay.get('event_ratio',1) > 1)  or (task.get('nEventsPerInputFile', 0) == events_per_pileup_file):
                pileup_files_per_job = 1
            else:
                raise Exception(f'Mismatch events in pileup and events per job: events per job {nevents_per_job} - pileup {events_per_pileup_file}')
        files_required = math.ceil(number_of_jobs) * pileup_files_per_job
        if files_required > len(mc_pileup_overlay['files']):
            raise Exception(f'Not enough overlay files: requested {files_required} available {len(mc_pileup_overlay["files"])}')
        files_to_store = []
        if split_by_dataset:
            files_per_datset = mc_pileup_overlay['files_per_dataset']
        #     list of keys in random order
            keys = list(files_per_datset.keys())
            random.shuffle(keys)
            files_to_store = []
            for key in keys:
                dataset_files = files_per_datset[key]
                not_used_files = set(dataset_files) - used_files
                if len(not_used_files) > files_required:
                    files_to_store = self.rucio_client.choose_random_files(list(not_used_files),files_required,random_seed=None,previously_used=[])
                    break
            if not files_to_store:
                split_by_dataset = False
        if not split_by_dataset:
            if (len(used_files) + files_required) > len(mc_pileup_overlay['files']):
                files_to_store = self.rucio_client.choose_random_files(mc_pileup_overlay['files'],files_required,random_seed=None,previously_used=[])
            else:
                files_to_store = self.rucio_client.choose_random_files(mc_pileup_overlay['files'],files_required,random_seed=None,previously_used=list(used_files))
        logger.info("MC overlay dataset %s with %d files is registered for a task %d" % (mc_pileup_overlay['input_dataset_name'], len(files_to_store),task_id))
        files_list = list(self.split_list(files_to_store,len(files_to_store)//100+1))
        self.rucio_client.register_dataset(mc_pileup_overlay['input_dataset_name'],files_list[0],meta={'task_id':task_id})
        for files in files_list[1:]:
            if files:
                self.rucio_client.register_files_in_dataset(mc_pileup_overlay['input_dataset_name'],files)

    def _find_overlay_input_dataset(self, param_value, dsid, split_by_dataset=False):
        def ami_tags_reduction(postfix):
            new_postfix = []
            first_letter = ''
            for token in postfix.split('_'):
                if token[0] != first_letter:
                    new_postfix.append(token)
                first_letter = token[0]
            return '_'.join(new_postfix)

        if param_value[-1] == '/':
            param_value = param_value[:-1]
        files_per_dataset = {}
        if not split_by_dataset:
            files_list = self.rucio_client.list_files_with_scope_in_dataset(param_value, True)
            files_per_dataset = {param_value: files_list}
        else:
            datasets = self.rucio_client.list_datasets_in_container(param_value)
            files_list = []
            for dataset in datasets:
                current_dataset_files = self.rucio_client.list_files_with_scope_in_dataset(dataset, True)
                files_per_dataset[dataset] = current_dataset_files
                files_list.extend(current_dataset_files)
        name_base = param_value
        if '_tid' in name_base:
            name_base = name_base.split('_tid')[0]
        if len(name_base.split('.')[-1]) > (50 - len('_sub1234_run123456')):
            name_base = '.'.join(name_base.split('.')[:-1]+[ami_tags_reduction(name_base.split('.')[-1])])
        previous_datasets = self.rucio_client.list_datasets_by_pattern('{base}_sub*_rnd{dsid}'.format(base=name_base,dsid=dsid))
        version = 1
        versions = []
        for dataset in previous_datasets:
            versions.append(int(dataset[dataset.find('_sub')+len('_sub'):dataset.find('_rnd')]))
        if versions:
            version = max(versions) + 1
        input_dataset_name = '{base}_sub{version:04d}_rnd{dsid}'.format(base=name_base,dsid=dsid,version=version)
        from rucio.common.exception import RucioException
        if version == 1:
            for i in range(10):
                try:
                    self.rucio_client.register_dataset(input_dataset_name)
                    break
                except RucioException as ex:
                    version += 1
                    input_dataset_name = '{base}_sub{version:04d}_rnd{dsid}'.format(base=name_base,dsid=dsid,version=version)

            version += 1
            input_dataset_name = '{base}_sub{version:04d}_rnd{dsid}'.format(base=name_base, dsid=dsid, version=version)

        return {'files':files_list, 'datasets':previous_datasets,'version': version,'input_dataset_name':input_dataset_name, 'files_per_dataset':files_per_dataset}

    def _define_merge_params(self, step, task_proto_dict, train_production=False):
        task_config = ProjectMode.get_task_config(step)

        merging_tag_name = ''

        if 'merging_tag' in list(task_config.keys()):
            merging_tag_name = task_config['merging_tag']
        if 'nFilesPerMergeJob' in list(task_config.keys()):
            merging_number_of_files_per_job = int(task_config['nFilesPerMergeJob'])
            task_proto_dict.update({'merging_number_of_files_per_job': merging_number_of_files_per_job})
        if 'nGBPerMergeJob' in list(task_config.keys()):
            merging_number_of_gb_pef_job = int(task_config['nGBPerMergeJob'])
            task_proto_dict.update({'merging_number_of_gb_pef_job': merging_number_of_gb_pef_job})
        if 'nMaxFilesPerMergeJob' in list(task_config.keys()):
            merging_number_of_max_files_per_job = int(task_config['nMaxFilesPerMergeJob'])
            task_proto_dict.update({'merging_number_of_max_files_per_job': merging_number_of_max_files_per_job})

        if not merging_tag_name:
            merging_tag_name = ProjectMode(step).merging

        if not merging_tag_name:
            logger.debug("[merging] Merging tag name is not specified")
            return

        ctag = self._get_ami_tag_cached(merging_tag_name)

        if ',' in ctag['transformation']:
            raise Exception("[merging] JEDI does not support tags with multiple transformations")

        trf_name = ctag['transformation']
        trf_cache = ctag['SWReleaseCache'].split('_')[0]
        trf_release = ctag['SWReleaseCache'].split('_')[1]
        #trf_params = self.ami_client.get_trf_params(trf_cache, trf_release, trf_name, force_ami=True)
        trf_params, _ = self._get_ami_transform_param_cached(trf_cache, trf_release, trf_name, force_ami=True)

        # proto_fix
        if trf_name.lower() == 'HLTHistMerge_tf.py'.lower():
            if '--inputHISTFile' not in trf_params:
                trf_params.append('--inputHISTFile')
            if '--outputHIST_MRGFile' not in trf_params:
                trf_params.remove('--outputHISTFile')
                trf_params.append('--outputHIST_MRGFile')
        elif trf_name.lower() == 'DAODMerge_tf.py'.lower():
            input_params = ["--input%sFile" % output_format for output_format in
                            step.step_template.output_formats.split('.')]
            for input_param in input_params:
                if input_param not in trf_params:
                    trf_params.append(input_param)
                    result = re.match(r'^(--)?input(?P<intype>.*)File', input_param, re.IGNORECASE)
                    if result:
                        in_type = result.groupdict()['intype']
                        output_param = "--output%s_MRGFile" % in_type
                        if output_param not in trf_params:
                            trf_params.append(output_param)
            if '--inputDAOD_EGAM1File' not in trf_params:
                trf_params.append('--inputDAOD_EGAM1File')
            if '--inputDAOD_EGAM3File' not in trf_params:
                trf_params.append('--inputDAOD_EGAM3File')
            if '--outputDAOD_EGAM1_MRGFile' not in trf_params:
                trf_params.append('--outputDAOD_EGAM1_MRGFile')
            if '--outputDAOD_EGAM3_MRGFile' not in trf_params:
                trf_params.append('--outputDAOD_EGAM3_MRGFile')

        trf_options = {}
        for key in list(Protocol.TRF_OPTIONS.keys()):
            if re.match(key, trf_name, re.IGNORECASE):
                trf_options.update(Protocol.TRF_OPTIONS[key])

        input_count = 0
        output_count = 0
        merging_job_parameters = list()
        for name in trf_params:
            if re.match(r'^(--)?amiTag$', name, re.IGNORECASE):
                param_dict = {'name': name, 'value': merging_tag_name}
                param_dict.update(trf_options)
                merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
            elif re.match(r'^(--)?input.*File$', name, re.IGNORECASE):
                result = re.match(r'^(--)?input(?P<intype>.*)File$', name, re.IGNORECASE)
                if not result:
                    continue
                merging_input_type = result.groupdict()['intype']
                if merging_input_type.lower() == 'Logs'.lower():
                    logs_param_dict = {'name': name, 'value': "${TRN_LOG0}"}
                    logs_param_dict.update(trf_options)
                    merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, logs_param_dict))
                    continue
                order_dict = self._get_output_params_order(task_proto_dict)
                for output_type in order_dict:
                    output_internal_type = output_type.split('_')[0]
                    if merging_input_type in output_internal_type or output_type == merging_input_type:
                        if step.request.request_type.lower() in ['GROUP'.lower()]:
                            param_dict = {'name': name, 'value': "${TRN_OUTPUT%d/L}" % order_dict[output_type]}
                        else:
                            param_dict = {'name': name, 'value': "${TRN_OUTPUT%d}" % order_dict[output_type]}
                        param_dict.update(trf_options)
                        merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                        input_count += 1
                        break
            elif re.match(r'^(--)?output.*File$', name, re.IGNORECASE):
                result = re.match(r'^(--)?output(?P<intype>.*)File$', name, re.IGNORECASE)
                if not result:
                    continue
                merging_output_type = result.groupdict()['intype']
                merging_output_internal_type = ''
                if re.match(r'^(--)?output.*_MRGFile$', name, re.IGNORECASE):
                    result = re.match(r'^(--)?output(?P<type>\w+)_MRGFile$', name, re.IGNORECASE)
                    if result:
                        merging_output_internal_type = result.groupdict()['type']
                order_dict = self._get_output_params_order(task_proto_dict)
                for output_type in order_dict:
                    output_internal_type = output_type.split('_')[0]
                    if (merging_output_type in output_internal_type) or (output_type == merging_output_type) or \
                            (merging_output_internal_type and merging_output_internal_type in output_internal_type) or \
                            (output_type == merging_output_internal_type):
                        param_dict = {'name': name, 'value': "${OUTPUT%d}" % order_dict[output_type]}
                        param_dict.update(trf_options)
                        merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                        output_count += 1
                        break
            else:
                param_value = self._get_parameter_value(name, ctag)
                if not param_value or str(param_value).lower() == 'none':
                    continue
                if str(param_value).lower() == 'none,none':
                    continue
                param_dict = {'name': name, 'value': param_value}
                param_dict.update(trf_options)

                if re.match('^(--)?autoConfiguration', name, re.IGNORECASE):
                    if ' ' in param_value:
                        param_dict.update({'separator': ' '})

                merging_job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))

        if input_count == 0:
            raise Exception('[merging] no inputs')

        if output_count == 0:
            raise Exception('[merging] no outputs')

        merging_job_parameters_str = ' '.join([param['value'] for param in merging_job_parameters])

        if train_production:
            task_proto_dict['um_name_at_end'] = True
        task_proto_dict['merge_output'] = True
        task_proto_dict['merge_spec'] = dict()
        task_proto_dict['merge_spec']['transPath'] = trf_name
        task_proto_dict['merge_spec']['jobParameters'] = merging_job_parameters_str

    @staticmethod
    def _get_prod_step(ctag_name, ctag):
        prod_step = str(ctag['productionStep']).replace(' ', '')
        if ctag_name[0] in ('a',):
            if 'Reco'.lower() in ctag['transformation'].lower():
                prod_step = 'recon'
            elif 'Sim'.lower() in ctag['transformation'].lower():
                prod_step = 'simul'
        return prod_step

    def _get_ami_tag_cached(self, tag_name):
        try:
            ctag = self._tag_cache.get(tag_name)
        except AttributeError:
            self._tag_cache = dict()
            ctag = None
        if not ctag:
            ctag = self.ami_client.get_ami_tag(tag_name)
            self._tag_cache.update({tag_name: ctag})
        return ctag

    def _get_ami_transform_param_cached(self, trf_cache, trf_release, trf_transform, sub_step_list=False, force_dump_args=False,
                                        force_ami=False):
        sw_name = trf_cache + trf_release + trf_transform + str(sub_step_list) + str(force_dump_args) + str(force_ami)
        try:
            sw_transform, new_sub_step_list = deepcopy(self._sw_cache.get(sw_name, (None, None)))
        except AttributeError:
            self._sw_cache = dict()
            sw_transform = None
            new_sub_step_list = None
        if sw_transform is None:
            sw_transform, new_sub_step_list = self.ami_client.get_trf_params(trf_cache, trf_release, trf_transform, sub_step_list, force_dump_args,
                                                          force_ami)
            self._sw_cache.update({sw_name: (deepcopy(sw_transform), deepcopy(new_sub_step_list))})
        return sw_transform, new_sub_step_list

    @staticmethod
    def _check_task_events_consistency(task_config):
        n_events_input_file = int(task_config['nEventsPerInputFile'])
        n_events_job = int(task_config['nEventsPerJob'])
        if (n_events_input_file % n_events_job == 0) or (n_events_job % n_events_input_file == 0):
            pass
        else:
            raise Exception(
                "The task is rejected because of inconsistency. " +
                "nEventsPerInputFile=%d, nEventsPerJob=%d" % (n_events_input_file, n_events_job)
            )

    def create_task_chain(self, step_id, max_number_of_steps=None, restart=None, input_dataset=None,
                          first_step_number_of_events=0, primary_input_offset=0, first_parent_task_id=0,
                          container_name=None, evgen_params=None):
        logger.info("Processing step %d" % step_id)

        try:
            first_step = StepExecution.objects.get(id=step_id)
            if first_step_number_of_events:
                first_step.input_events = int(first_step_number_of_events)
        except ObjectDoesNotExist:
            raise Exception("Step %d is not found" % step_id)

        chain = list()
        chain.append(first_step)

        step = first_step
        chain_id = 0
        if first_parent_task_id:
            parent_task_id = first_parent_task_id
        else:
            parent_task_id = self.task_reg.get_parent_task_id(step, 0)

        while step is not None:
            try:
                step = StepExecution.objects.get(~Q(id=step.id), step_parent_id=step.id, slice=first_step.slice)
                if step.status.lower() == self.protocol.STEP_STATUS[StepStatus.APPROVED].lower():
                    chain.append(step)
            except ObjectDoesNotExist:
                step = None

        if max_number_of_steps:
            chain = chain[:max_number_of_steps]

        for step in chain:
            priority = step.priority
            number_of_events = int(step.input_events)
            username = step.request.manager
            usergroup = self._get_usergroup(step)
            ctag_name = step.step_template.ctag
            output_types = step.step_template.output_formats.split('.')
            project = self._get_project(step)
            ctag = self._get_ami_tag_cached(ctag_name)
            energy_gev = self._get_energy(step, ctag)
            prod_step = self._get_prod_step(ctag_name, ctag)
            task_config = ProjectMode.get_task_config(step)
            memory = TaskDefConstants.DEFAULT_MEMORY
            base_memory = TaskDefConstants.DEFAULT_MEMORY_BASE
            follow_hashtags = []
            trf_name = ctag['transformation']
            if ctag.get('status', '') == 'invalid':
                raise Exception("The tag {0} is invalid".format(ctag_name))
            trf_options = {}
            for key in list(Protocol.TRF_OPTIONS.keys()):
                if re.match(key, trf_name, re.IGNORECASE):
                    trf_options.update(Protocol.TRF_OPTIONS[key])

            if ctag_name[0] in ('f', 'm', 'v', 'k') and 'phconfig' in ctag:
                tzero_tag = self.ami_client.get_ami_tag_tzero(ctag_name)
                if type(tzero_tag) == str:
                    tzero_tag = json.loads(tzero_tag)
                tzero_outputs = tzero_tag['transformation']['args']['outputs']
                if type(tzero_outputs) == str:
                    tzero_outputs = ast.literal_eval(tzero_outputs)
                try:
                    self.ami_client.apply_phconfig_ami_tag(ctag)
                except SyntaxError:
                    raise Exception('phconfig content of {0} tag has invalid syntax'.format(ctag_name))
                except Exception as ex:
                    raise Exception('apply_phconfig_ami_tag failed: {0}'.format(str(ex)))
                trf_options.update({'separator': ' '})

            if ',' in trf_name:
                raise Exception("JEDI does not support tags with multiple transformations")

            trf_cache = ctag['SWReleaseCache'].split('_')[0]
            trf_release = ctag['SWReleaseCache'].split('_')[1]
            trf_release_base = '.'.join(trf_release.split('.')[:3])

            use_nightly_release = False
            version_base = None
            version_major = None
            version_timestamp = None
            if 'T' in trf_release:
                # release contains timestamp - nightly
                # 21.1.2-21.0-2017-04-03T2135
                if '--' not in trf_release:
                    version_base = trf_release[:trf_release.index('-')]
                    version_timestamp = '-'.join(trf_release.split('-')[-3:])
                    version_major = trf_release[trf_release.index('-') + 1:].replace(version_timestamp,'')[:-1]
                else:
                    trf_release = trf_release.replace('--','$$')
                    release_part = trf_release[trf_release.index('-') + 1:]
                    version_major = release_part[:release_part.index('-')].replace('$$','--')
                    version_timestamp = release_part[release_part.index('-') + 1:].replace('$$','--')
                    release_part = release_part.replace('--','$$')
                    trf_release = trf_release.replace('$$','--')
                use_nightly_release = True

            project_mode = ProjectMode(step, '{0}-{1}'.format(trf_cache, trf_release),
                                       use_nightly_release=use_nightly_release)

            change_output_type_dict = dict()
            try:
                if project_mode.changeType:
                    for pair in project_mode.changeType.split(','):
                        change_output_type_dict[pair.split(':')[0]] = pair.split(':')[-1]
            except Exception as ex:
                raise Exception('changeType has invalid format: {0}'.format(str(ex)))

            index_consistent_param_list = list()
            try:
                if project_mode.indexConsistent:
                    for name in project_mode.indexConsistent.split(','):
                        index_consistent_param_list.append(name)
            except Exception as ex:
                raise Exception('indexConsistent has invalid format: {0}'.format(str(ex)))

            skip_prod_step_check = project_mode.skipProdStepCheck or False

            if 'merge'.lower() in step.step_template.step.lower() and 'merge' not in prod_step.lower():
                if step.request.request_type in ['MC'] and not skip_prod_step_check:
                    raise Exception('productionStep in the tag ({0}) differs from the step name in the request ({1})'
                                    .format(prod_step, step.step_template.step))

            ami_types = None
            try:
                ami_types = self.ami_client.get_types()
            except Exception as ex:
                logger.exception('Getting AMI types failed: {0}'.format(str(ex)))

            if ami_types:
                ami_types.append('EI')
                ami_types.append('BS_TRIGSKIM')
                for output_type in output_types:
                    if output_type not in ami_types:
                        raise Exception("The output format \"%s\" is not registered in AMI" % output_type)
            else:
                logger.warning('AMI type list is empty')

            trf_dict = dict()
            trf_dict.update({trf_name: [trf_cache, trf_release, trf_release_base]})

            force_ami = self.ami_client.is_new_ami_tag(ctag)

            trf_params = list()
            trf_sub_steps = list()
            for key in list(trf_dict.keys()):
                # trf_params.extend(self.ami_client.get_trf_params(trf_dict[key][0], trf_dict[key][1], key,
                #                                                  sub_step_list=trf_sub_steps, force_ami=force_ami))
                trf_from_cache, sub_steps_from_cache = self._get_ami_transform_param_cached(trf_dict[key][0], trf_dict[key][1], key,
                                                                sub_step_list=True, force_ami=force_ami)
                trf_params.extend(trf_from_cache)
                if sub_steps_from_cache is not None:
                    trf_sub_steps.extend(sub_steps_from_cache)
                #trf_params.append('--multithreaded')

            if not trf_params:
                raise Exception("AMI: list of transformation parameters is empty")
            if not project_mode.skipTRFParamCheck:
                if ctag_name not in self._checked_ami_tags:
                    if self.ami_client.check_trf_params_in_ami_tag(ctag_name, trf_params):
                        self._checked_ami_tags.append(ctag_name)
            skip_evgen_check = project_mode.skipEvgenCheck or False
            use_real_nevents = project_mode.useRealNumEvents

            use_containers = False
            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'evgen'.lower() or prod_step.lower() == 'simul'.lower():
                    use_containers = True
                if prod_step.lower() == 'simul'.lower() and not skip_evgen_check:
                    if step.step_parent_id != step.id:

                        try:
                            evgen_step = StepExecution.objects.get(id=step.step_parent_id)
                            if evgen_step.step_template.step.lower() != 'Evgen'.lower():
                                raise Exception('The parent is not EVNT. The checking is skipped')
                            if evgen_step.step_template.step.lower() == 'Evgen Merge'.lower():
                                raise Exception('The parent is EVNT merging. The checking is skipped')
                            if evgen_step.status != self.protocol.STEP_STATUS[StepStatus.APPROVED]:
                                raise Exception('The parent step is skipped. The checking is skipped')
                            # use only JO input
                            evgen_step.slice.dataset = evgen_step.slice.input_data
                            evgen_input_params = self.get_input_params(evgen_step, evgen_step, False, energy_gev,
                                                                       use_evgen_otf=True)
                            if 'nEventsPerJob' in list(evgen_input_params.keys()):
                                evgen_events_per_job = int(evgen_input_params['nEventsPerJob'])
                                evgen_step_task_config = ProjectMode.get_task_config(evgen_step)
                                evgen_step_task_config.update({'nEventsPerJob': evgen_events_per_job})
                                ProjectMode.set_task_config(evgen_step, evgen_step_task_config,
                                                            keys_to_save=('nEventsPerJob',))
                                task_config.update({'nEventsPerInputFile': evgen_events_per_job})
                                ProjectMode.set_task_config(step, task_config, keys_to_save=('nEventsPerInputFile',))
                        except Exception as ex:
                            logger.warning("Checking the parent evgen step failed: %s" % str(ex))

            if 'nEventsPerInputFile' in list(task_config.keys()):
                n_events_input_file = int(task_config['nEventsPerInputFile'])
                parent_step = StepExecution.objects.get(id=step.step_parent_id)
                is_parent_merge = 'Merge'.lower() in parent_step.step_template.step.lower()
                is_parent_approved = \
                    parent_step.status.lower() == self.protocol.STEP_STATUS[StepStatus.APPROVED].lower()
                if parent_step.id != step.id and not is_parent_merge and is_parent_approved:
                    parent_task_config = ProjectMode.get_task_config(parent_step)
                    if 'nEventsPerJob' in list(parent_task_config.keys()):
                        n_events_job_parent = int(parent_task_config['nEventsPerJob'])
                        if n_events_input_file != n_events_job_parent:
                            if project_mode.nEventsPerInputFile:
                                pass
                            else:
                                raise Exception(
                                    'The task is rejected because of inconsistency. ' +
                                    'nEventsPerInputFile={0} does not match to nEventsPerJob={1} of the parent'.format(
                                        n_events_input_file, n_events_job_parent))

            overlay_production = False
            train_production = False

            for key in list(ctag.keys()):
                if re.match(r'^(--)?reductionConf$', key, re.IGNORECASE):
                    if str(ctag[key]).lower() != 'none':
                        train_production = True
                        break
                if re.match('^(--)?formats', key, re.IGNORECASE):
                    if str(ctag[key]).lower() != 'none':
                        train_production = True
                        break

            use_evgen_otf = project_mode.isOTF or False
            use_no_output = project_mode.noOutput or False
            leave_log = True
            if project_mode.leaveLog is not None:
                leave_log = project_mode.leaveLog

            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'simul'.lower():
                    leave_log = True

            if project_mode.mergeCont:
                use_containers = True

            bunchspacing = project_mode.bunchspacing
            max_events_forced = project_mode.maxEvents

            if project_mode.fixedMaxEvents:
                input_data_name = step.slice.input_data
                input_data_dict = self.parse_data_name(input_data_name)
                if self.is_new_jo_format(input_data_name):
                    params = self._get_evgen_input_files_new(input_data_dict, energy_gev)
                else:
                    params = self._get_evgen_input_files(input_data_dict, energy_gev, use_evgen_otf=use_evgen_otf)
                if 'nEventsPerJob' in params:
                    max_events_forced = params['nEventsPerJob']
                else:
                    raise Exception(
                        'JO file {0} does not contain nEventsPerJob definition. '.format(input_data_name) +
                        'fixedMaxEvents option cannot be used. The task is rejected')

            skip_events_forced = project_mode.skipEvents

            if project_mode.nEventsPerRange:
                task_config.update({'nEventsPerRange': project_mode.nEventsPerRange})

            allow_no_output_patterns = list()
            try:
                if project_mode.allowNoOutput:
                    for pattern in project_mode.allowNoOutput.split(','):
                        allow_no_output_patterns.append(pattern)
            except Exception as ex:
                logger.exception("allowNoOutput has invalid format: %s" % str(ex))

            hidden_output_patterns = list()
            try:
                if project_mode.hiddenOutput:
                    for pattern in project_mode.hiddenOutput.split(','):
                        hidden_output_patterns.append(pattern)
            except Exception as ex:
                logger.exception("hiddenOutput has invalid format: %s" % str(ex))

            output_ratio = project_mode.outputRatio or 0

            ignore_trf_params = list()
            try:
                if project_mode.ignoreTrfParams:
                    for param in project_mode.ignoreTrfParams.split(','):
                        ignore_trf_params.append(param)
            except Exception as ex:
                logger.exception("ignoreTrfParams has invalid format: %s" % str(ex))

            empty_trf_params = list()
            try:
                if project_mode.emptyTrfParams:
                    for param in project_mode.emptyTrfParams.split(','):
                        empty_trf_params.append(param)
            except Exception as ex:
                logger.exception("emptyTrfParams has invalid format: %s" % str(ex))

            use_dataset_name = project_mode.useDatasetName
            use_container_name = project_mode.useContainerName
            use_direct_io = project_mode.useDirectIo
            task_common_offset = project_mode.commonOffset

            env_params_dict = dict()
            try:
                if project_mode.env:
                    for pair in project_mode.env.split(','):
                        env_params_dict[pair.split(':')[0]] = pair.split(':')[-1]
            except Exception as ex:
                logger.exception("env parameter has invalid format: %s" % str(ex))

            secondary_input_offset = project_mode.secondaryInputOffset
            ei_output_filename = project_mode.outputEIFile

            if input_dataset:
                step.slice.dataset = input_dataset
            input_params = self.get_input_params(step, first_step, restart, energy_gev, use_containers, use_evgen_otf,
                                                 task_id=parent_task_id)
            if not input_params:
                input_params = self.get_input_params(step, first_step, True, energy_gev, use_containers, use_evgen_otf,
                                                     task_id=parent_task_id)

            if 'input_params' in list(task_config.keys()):
                input_params.update(task_config['input_params'])

            if 'nFilesPerJob' in list(input_params.keys()) and 'nFilesPerJob' not in list(task_config.keys()):
                task_config.update({'nFilesPerJob': int(input_params['nFilesPerJob'])})

            if evgen_params and prod_step.lower() == 'evgen'.lower():
                input_params.update(evgen_params)
                number_of_events = evgen_params['nevents']
                task_config['nFiles'] = evgen_params['nfiles']

            try:
                if step.request.request_type.lower() == 'MC'.lower() and 'nEventsPerInputFile' in list(
                        task_config.keys()):
                    real_input_events = 0
                    for key in list(input_params.keys()):
                        if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                            for input_name in input_params[key]:
                                result = re.match(r'^.+_tid(?P<tid>\d+)_00$', input_name)
                                if result:
                                    if parent_task_id == int(result.groupdict()['tid']):
                                        continue
                                real_input_events += int(
                                    task_config['nEventsPerInputFile']) * self.rucio_client.get_number_files(input_name)
                    if real_input_events < number_of_events:
                        real_input_difference = float(number_of_events - real_input_events) / float(
                            number_of_events) * 100
                        if real_input_difference <= TaskDefConstants.DEFAULT_ALLOWED_INPUT_EVENTS_DIFFERENCE:
                            number_of_events = real_input_events
                            step.input_events = number_of_events
                            step.slice.input_events = number_of_events
                            step.slice.save()
                            step.save()
            except Exception:
                logger.warning('Checking real number of input events failed: {0}'.format(get_exception_string()))

            use_evnt_filter = None
            if self.protocol.is_evnt_filter_step(project_mode, task_config) and prod_step.lower() == 'evgen'.lower():
                input_types = list()
                for key in list(input_params.keys()):
                    result = re.match(r'^(--)?input(?P<intype>.*)File', key, re.IGNORECASE)
                    if not result:
                        continue
                    in_type = result.groupdict()['intype']
                    input_types.append(in_type)
                if len(input_types) == 1 and 'EVNT' in input_types:
                    efficiency = 0
                    safety_factor = 0.1

                    efficiency = float(task_config.get('evntFilterEff', efficiency))
                    efficiency = float(project_mode.evntFilterEff or efficiency)

                    safety_factor = float(task_config.get('evntSafetyFactor', safety_factor))
                    safety_factor = float(project_mode.evntSafetyFactor or safety_factor)

                    input_data_name = step.slice.input_data
                    input_data_dict = self.parse_data_name(input_data_name)
                    if self.is_new_jo_format(input_data_name):
                        max_events_forced = \
                            self._get_evgen_input_files_new(input_data_dict, energy_gev)['nEventsPerJob']
                        job_config = input_data_dict['number']
                    else:
                        max_events_forced = \
                            self._get_evgen_input_files(
                                input_data_dict, energy_gev, use_evgen_otf=use_evgen_otf)['nEventsPerJob']
                        job_config = "%sJobOptions/%s" % (input_data_dict['project'], input_data_name)
                    input_params.update({'jobConfig': job_config})
                    input_params.update({'nEventsPerJob': max_events_forced})
                    if 'inputEVNTFile' in list(input_params.keys()):
                        input_params['inputEVNT_PreFile'] = input_params['inputEVNTFile']
                    ignore_trf_params.append('inputEVNTFile')

                    min_events = max_events_forced
                    project_mode.nEventsPerInputFile = min_events

                    if 'nEventsPerInputFile' not in list(task_config.keys()):
                        input_name = input_params[list(input_params.keys())[0]][0]
                        input_file_min_events = self.get_events_per_file(input_name)
                    else:
                        input_file_min_events = int(task_config['nEventsPerInputFile'])

                    number_files_per_job = int(
                        min_events / (efficiency * (1 - safety_factor)) / input_file_min_events) + 1
                    number_files = number_of_events * number_files_per_job / min_events
                    task_config['nFilesPerJob'] = number_files_per_job
                    task_config['nFiles'] = number_files

                    use_evnt_filter = True
            if input_params.get('isEvntToEvnt') and prod_step.lower() == 'evgen'.lower():
                input_types = list()
                for key in list(input_params.keys()):
                    result = re.match(r'^(--)?input(?P<intype>.*)File', key, re.IGNORECASE)
                    if not result:
                        continue
                    in_type = result.groupdict()['intype']
                    input_types.append(in_type)
                if len(input_types) == 1 and 'EVNT' in input_types:
                    if 'inputEVNTFile' in list(input_params.keys()):
                        input_params['inputEVNT_PreFile'] = input_params['inputEVNTFile']
                    ignore_trf_params.append('inputEVNTFile')
                    ignore_trf_params.append('skipEvents')
                    max_events_forced = input_params['nEventsPerJob']
                    use_evnt_filter = True
                    if project_mode.optimalFirstEvent is None:
                        task_config['optimalFirstEvent'] = True

                    task_config['nFiles'] = int(number_of_events * input_params['nFilesPerJob'] / max_events_forced)
                    if project_mode.optimalFirstEvent or task_config.get('optimalFirstEvent'):
                        if int(input_params['nFilesPerJob']) == 1:
                            nEventsOptimal = max_events_forced
                        else:
                            nEventsOptimal = minHigherDivisor(int(max_events_forced) // (int(input_params['nFilesPerJob'])-1) ,int(max_events_forced))
                        project_mode.nEventsPerInputFile = nEventsOptimal
                        input_params['nEventsPerInputFile'] = nEventsOptimal
                        task_config['nEventsPerInputFile'] = nEventsOptimal
                    else:
                        project_mode.nEventsPerInputFile = max_events_forced
                        input_params['nEventsPerInputFile'] = max_events_forced
                        task_config['nEventsPerInputFile'] = max_events_forced

            use_lhe_filter = None
            is_evnt = False
            if prod_step.lower() == 'evgen'.lower():
                input_types = list()
                is_evnt = True
                for key in list(input_params.keys()):
                    result = re.match(r'^(--)?input(?P<intype>.*)File', key, re.IGNORECASE)
                    if not result:
                        continue
                    for input_name in input_params[key]:
                        try:
                            input_name_dict = self.parse_data_name(input_name)
                            if input_name_dict['prod_step'] == 'evgen':
                                input_types.append(input_name_dict['data_type'])
                        except Exception as ex:
                            if 'TXT' in input_name and key!='inputGenConfFile':
                                input_types.append('TXT')
                            logger.error('parse_data_name failed: {0} (input_name={1})'.format(ex, input_name))
                if len(input_types) == 1 and 'TXT' in input_types:
                    if is_optimal_first_event(step):
                        task_config.update({'optimalFirstEvent': True})
                    min_events = int(input_params.get('nEventsPerJob', 0)) or int(task_config.get('nEventsPerJob', 0))
                    if min_events:
                        if not project_mode.nEventsPerInputFile and not (project_mode.optimalFirstEvent or task_config.get('optimalFirstEvent')):
                            project_mode.nEventsPerInputFile = min_events

                        number_files_per_job = int(task_config.get('nFilesPerJob', 1))
                        number_files = math.ceil(number_of_events * number_files_per_job / min_events)
                        task_config['nFiles'] = number_files
                        if number_files_per_job > 1:
                            max_events_forced = min_events
                        use_lhe_filter = True

            # proto_fix
            use_input_with_dataset = False
            if trf_name.lower() == 'Trig_reco_tf.py'.lower() or trf_name.lower() == 'TrigMT_reco_tf.py'.lower():
                trf_options.update({'separator': ' '})
                for name in trf_params:
                    if re.match(r'^(--)?inputBS_RDOFile$', name, re.IGNORECASE) and 'RAW'.lower() in ','.join(
                            [e.lower() for e in list(input_params.keys())]):
                        input_param_name = self._get_input_output_param_name(input_params, 'RAW', extended_pattern=True)
                        if input_param_name:
                            use_bs_rdo = self._get_parameter_value('prodSysBSRDO', ctag)
                            logger.info("use_bs_rdo = %s" % str(use_bs_rdo))
                            if 'RAW' in output_types or use_bs_rdo:
                                input_params['inputBS_RDOFile'] = input_params[input_param_name]
                                trf_params.remove('--inputBSFile')
                            else:
                                if 'TRIGCOST' not in input_param_name:
                                    input_params['inputBSFile'] = input_params[input_param_name]
                                    if '--inputBS_RDOFile' in trf_params:
                                        trf_params.remove('--inputBS_RDOFile')
                        break
                if 'athenaopts' in list(ctag.keys()) and ctag_name == 'r6395':
                    ctag['athenaopts'] = \
                        ' -c "import os;os.unsetenv(\'FRONTIER_SERVER\');rerunLVL1=True" -J ' + \
                        'TrigConf::HLTJobOptionsSvc --use-database --db-type Coral --db-server ' + \
                        'TRIGGERDBREPR --db-smkey 598 --db-hltpskey 401 --db-extra "{\'lvl1key\': 82}" '
            elif trf_name.lower() == 'POOLtoEI_tf.py'.lower():
                use_no_output = True
                for key in list(input_params.keys()):
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        input_params['inputPOOLFile'] = input_params[key]
                        del input_params[key]
            elif trf_name.lower() == 'HITSMerge_tf.py'.lower():
                param_name = 'inputLogsFile'
                if param_name not in ignore_trf_params:
                    ignore_trf_params.append(param_name)
                if step.request.campaign.lower() == 'MC23'.lower():
                    if not project_mode.skipShortOutput:
                        project_mode.skipShortOutput = True
                        project_mode.respectSplitRule = True
            elif trf_name.lower() == 'BSOverlayFilter_tf.py'.lower():
                overlay_production = True
                for key in list(input_params.keys()):
                    for name in input_params[key][:]:
                        if re.match(r'^.+_tid(?P<tid>\d+)_00$', name, re.IGNORECASE):
                            input_params[key].remove(name)
                for key in list(input_params.keys()):
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        input_params['inputBSCONFIGFile'] = input_params[key]
                        del input_params[key]
            elif trf_name.lower() == 'ReSim_tf.py'.lower():
                trf_options.update({'separator': ' '})
            elif trf_name.lower() == 'HISTPostProcess_tf.py'.lower():
                trf_options.update({'separator': ' '})
                use_input_with_dataset = True

            input_data_name = None
            skip_check_input = False

            if step.step_parent_id == step.id:
                input_data_name = self.get_step_input_data_name(step)
            else:
                for key in list(input_params.keys()):
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        if len(input_params[key]):
                            input_data_name = input_params[key][0]
                            break

            if not input_data_name:
                raise Exception("Input data list is empty")

            if use_dataset_name and use_containers:
                datasets = self.rucio_client.list_datasets_in_container(input_data_name)
                if not datasets:
                    raise Exception(
                        'The container {0} is empty. Impossible to construct a task name'.format(input_data_name)
                    )
                input_data_name = datasets[0]

            if use_container_name and container_name and (step == first_step):
                input_data_name = container_name

            input_data_dict = self.parse_data_name(input_data_name)

            if use_evnt_filter or is_evnt:
                input_data_name = step.slice.input_data
                input_data_dict = self.parse_data_name(input_data_name)

            if input_data_dict['project'].lower().startswith('mc') and project.lower().startswith('data'):
                raise Exception("The project 'data' is invalid for MC inputs")

            taskname = self._construct_taskname(input_data_name, project, prod_step, ctag_name, trf_name)
            if not self.template_type:
                task_proto_id = self.task_reg.register_task_id()
            else:
                task_proto_id = TaskDefConstants.TEMPLATE_TASK_ID # replace

            if 'EVNT' in output_types and prod_step.lower() == 'evgen'.lower():
                if input_data_name.split('.py')[0].lower().endswith('lhe'):
                    raise Exception("The JO is LHE. The output EVNT is not allowed")
                use_evnt_txt = False
                if 'Ph' in input_data_name or 'Powheg' in input_data_name:
                    if (trf_cache == 'AtlasProduction' and LooseVersion(trf_release) >= LooseVersion('19.2.4.11')) or \
                            (trf_cache == 'MCProd' and LooseVersion(trf_release) >= LooseVersion('19.2.4.9.3')) or \
                            (trf_cache == 'AthGeneration'):
                        if 'inputGenConfFile' in list(input_params.keys()):
                            use_evnt_txt = True
                        elif 'inputGenConfFile' not in list(input_params.keys()) and \
                                'inputGeneratorFile' not in list(input_params.keys()):
                            use_evnt_txt = True
                    if use_evnt_txt:
                        if 'TXT' not in output_types:
                            pass
                    else:
                        if 'TXT' in output_types:
                            output_types.remove('TXT')
                if 'aMcAtNlo' in input_data_name:
                    if int(ctag_name[1:]) >= 6000 and \
                            project in ('mc15_13TeV', 'mc16_13TeV', 'mc15_valid', 'mc16_valid', 'mc15_5TeV'):
                        if 'inputGenConfFile' in list(input_params.keys()):
                            use_evnt_txt = True
                        elif 'inputGenConfFile' not in list(input_params.keys()) and \
                                'inputGeneratorFile' not in list(input_params.keys()):
                            use_evnt_txt = True
                    if use_evnt_txt:
                        if 'TXT' not in output_types:
                            pass
                    else:
                        if 'TXT' in output_types:
                            output_types.remove('TXT')
                if  LooseVersion(trf_release) >= LooseVersion('22.0'):
                    if not project_mode.skipHEPMCCheck and not self._check_evgen_hepmc(trf_cache, trf_release, step.request.campaign):
                        logger.warning(f"HEPMC check for {trf_cache} {trf_release} {step.request.campaign} failed")
                # if  LooseVersion(trf_release) >= LooseVersion('22.0'):
                #     if self.is_madgraph(input_data_name) and not project_mode.coreCount:
                #         project_mode.coreCount  = 8


            skip_scout_jobs = None
            try:
                try:
                    oe = OpenEndedRequest.objects.get(request__reqid=step.request.reqid)
                    if oe.status.lower() == 'open':
                        request_task_list = ProductionTask.objects.filter(request=step.request)
                        for prod_task in request_task_list:
                            task_output_types = prod_task.step.step_template.output_formats.split('.')
                            if set(task_output_types) == set(output_types):
                                jedi_task = TTask.objects.get(id=prod_task.id)
                                if jedi_task.status.lower() in (self.protocol.TASK_STATUS[TaskStatus.RUNNING],
                                                                self.protocol.TASK_STATUS[TaskStatus.FINISHED],
                                                                self.protocol.TASK_STATUS[TaskStatus.DONE]):
                                    if jedi_task.total_done_jobs > 0:
                                        break
                except ObjectDoesNotExist:
                    pass
            except Exception as ex:
                logger.exception("Checking OE failed: %s" % str(ex))

            if 'nEventsPerJob' in list(input_params.keys()):
                task_config.update({'nEventsPerJob': int(input_params['nEventsPerJob'])})
                ProjectMode.set_task_config(step, task_config, keys_to_save=('nEventsPerJob',))

            random_seed_offset = 0
            first_event_offset = 0
            skip_check_input_ne = False
            evgen_input_formats = list()

            if prod_step.lower() == 'evgen'.lower():
                evgen_number_input_files = 0
                for key in list(input_params.keys()):
                    if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                        for input_name in input_params[key]:
                            evgen_number_input_files += self.rucio_client.get_number_files(input_name)
                            try:
                                input_name_dict = self.parse_data_name(input_name)
                                if input_name_dict['prod_step'] == 'evgen':
                                    evgen_input_formats.append(input_name_dict['data_type'])
                            except Exception:
                                if 'TXT' in input_name and key!='inputGenConfFile':
                                    evgen_input_formats.append('TXT')
                if number_of_events > 0 and task_config.get('nEventsPerJob', None) and not evgen_params:
                    events_per_job = int(task_config['nEventsPerJob'])
                    if not (number_of_events % events_per_job == 0):
                        raise Exception('The task is rejected because of inconsistency. ' +
                                        'nEvents={0}, nEventsPerJob={1}, step={2}'.format(
                                            number_of_events, events_per_job, prod_step))
                if evgen_number_input_files == 1:
                    events_per_job = int(task_config['nEventsPerJob'])
                    task_config.update({'split_slice': True})
                    ProjectMode.set_task_config(step, task_config, keys_to_save=('split_slice',))
                    random_seed_offset = int(self._get_number_events_processed(step)[0] / events_per_job)
                    first_event_offset = random_seed_offset * events_per_job
                    skip_check_input = True
                    if number_of_events > 0:
                        task_config['nEventsPerInputFile'] = number_of_events
                        skip_check_input_ne = True
                elif evgen_number_input_files > 1:
                    if len(evgen_input_formats) == 1 and 'TXT' in evgen_input_formats:
                        if 'nEventsPerInputFile' in list(task_config.keys()) and 'nEventsPerJob' in list(
                                task_config.keys()):
                            if not project_mode.nEventsPerInputFile:
                                if (project_mode.optimalFirstEvent or task_config.get('optimalFirstEvent')) and task_config.get('nFilesPerJob') and int(task_config['nFilesPerJob'])>1:
                                    task_config['nEventsPerInputFile'] = minHigherDivisor(int(task_config['nEventsPerJob']) // (int(task_config['nFilesPerJob'])-1) ,int(task_config['nEventsPerJob']))
                                else:
                                    task_config['nEventsPerInputFile'] = int(task_config['nEventsPerJob'])
                            else:
                                task_config['nEventsPerInputFile'] = project_mode.nEventsPerInputFile
                    if 'nEventsPerInputFile' in list(task_config.keys()) and task_config['nEventsPerInputFile'] > 0 \
                            and number_of_events > 0:
                        evgen_number_input_files_requested = \
                            math.ceil(number_of_events / task_config['nEventsPerInputFile'])
                        if evgen_number_input_files_requested < evgen_number_input_files and not use_evnt_filter and \
                                not use_lhe_filter:
                            task_config['nFiles'] = int(evgen_number_input_files_requested)
                elif evgen_number_input_files == 0:
                    skip_check_input_ne = True
                    events_per_job = int(task_config['nEventsPerJob'])
                    task_config.update({'split_slice': True})
                    ProjectMode.set_task_config(step, task_config, keys_to_save=('split_slice',))
                    random_seed_offset = int(self._get_number_events_processed(step)[0] / events_per_job)
                    first_event_offset = random_seed_offset * events_per_job

                if 'nEventsPerJob' in list(task_config.keys()) and number_of_events > 0:
                    evgen_number_jobs = number_of_events / int(task_config['nEventsPerJob'])
                    if evgen_number_jobs <= 10:
                        skip_scout_jobs = True

            reduction_conf_base_output_types = list()

            if train_production:
                reduction_conf = list()
                for output_type in output_types[:]:
                    if output_type.lower().startswith('DAOD_'.lower()) or \
                            output_type.lower().startswith('D2AOD_'.lower()):
                        reduction_conf.append(output_type.split('_')[-1])
                        reduction_conf_base_output_types.append(output_type.split('_')[0])
                        output_param_name = "--output{0}File".format(output_type)
                        if output_param_name not in trf_params:
                            trf_params.append(output_param_name)
                for key in list(ctag.keys()):
                    if re.match('^(--)?reductionConf', key, re.IGNORECASE):
                        ctag[key] = ' '.join(reduction_conf)
                        break
                    if re.match('^(--)?formats', key, re.IGNORECASE):
                        ctag[key] = ' '.join(reduction_conf)
                        break

            # proto_fix
            if trf_name.lower() == 'HLTHistMerge_tf.py'.lower():
                if '--inputHISTFile' not in trf_params:
                    trf_params.append('--inputHISTFile')
                if '--outputHIST_MRGFile' not in trf_params:
                    trf_params.remove('--outputHISTFile')
                    trf_params.append('--outputHIST_MRGFile')
            elif trf_name.lower() == 'SkimNTUP_trf.py'.lower():
                for input_key in list(input_params.keys()):
                    trf_params.append(input_key)
                for output_type in output_types:
                    trf_params.append("output%sFile" % output_type)
            elif trf_name.lower() == 'csc_MergeHIST_trf.py'.lower():
                if '--inputHISTFile' not in trf_params:
                    trf_params.append('--inputHISTFile')
                if '--outputHISTFile' not in trf_params:
                    trf_params.append('--outputHISTFile')
            if 'D2AOD' in reduction_conf_base_output_types:
                trf_params.remove('--inputAODFile')
                trf_params.remove('--preExec')
            if project_mode.rivet and 'YODA' not in output_types:
                output_types.append('YODA')
            if 'log' not in output_types:
                output_types.append('log')
            output_params = self._get_output_params(input_data_name,
                                                    output_types,
                                                    project,
                                                    prod_step,
                                                    ctag_name,
                                                    task_proto_id,
                                                    trf_name)

            # proto_fix
            if trf_name.lower() == 'TrainReco_tf.py'.lower():
                trf_params.extend(["--%s" % key for key in list(input_params.keys())])
                trf_params.extend(
                    ["--%s" % key for key in list(output_params.keys()) if key.lower() != 'outputlogFile'.lower()])

            # proto_fix
            if trf_name.lower() == 'DigiMReco_trf.py'.lower():
                if 'preExec' not in trf_params:
                    trf_params.append('preExec')
                if 'postExec' not in trf_params:
                    trf_params.append('postExec')
                if 'preInclude' not in trf_params:
                    trf_params.append('preInclude')

            if change_output_type_dict:
                for key in list(change_output_type_dict.keys()):
                    output_param_name = "output{0}File".format(key)
                    if output_param_name in list(output_params.keys()):
                        output_params["output{0}File".format(change_output_type_dict[key])] = \
                            output_params.pop(output_param_name)

            no_input = True

            if train_production:
                trf_options.update({'separator': ' '})

            if parent_task_id > 0 and not use_real_nevents:
                try:
                    number_of_events_per_input_file = self.task_reg.get_task_parameter(parent_task_id, 'nEventsPerJob')
                    if 'nEventsPerInputFile' not in list(task_config.keys()):
                        task_config.update({'nEventsPerInputFile': number_of_events_per_input_file})
                except Exception:
                    pass

            if 'nEventsPerInputFile' in list(task_config.keys()) and 'nEventsPerJob' in list(task_config.keys()) and \
                    (not skip_check_input_ne) and not project_mode.nEventsPerInputFile and not (project_mode.optimalFirstEvent or task_config.get('optimalFirstEvent')):
                self._check_task_events_consistency(task_config)


            if project_mode.primaryInputOffset is not None:
                primary_input_offset = project_mode.primaryInputOffset
            if project_mode.randomSeedOffset is not None:
                random_seed_offset = project_mode.randomSeedOffset
                if step.request.phys_group in ['VALI']:
                    skip_check_input = True

            random_seed_proto_key = TaskParamName.RANDOM_SEED
            if step.request.request_type.lower() == 'MC'.lower():
                random_seed_proto_key = TaskParamName.RANDOM_SEED_MC

            is_pile_task = False
            is_not_transient_output = False

            if step.step_template.step.lower() in ['Rec Merge'.lower(), 'Atlf Merge'.lower()]:
                is_not_transient_output = True

            job_parameters = list()

            output_trf_params = list()

            mc_pileup_overlay = {'is_overlay':False,'datasets':[],'version':1,'input_dataset_name':None, 'files': []}
            for output_type in output_types:
                if (trf_name.lower() == 'Trig_reco_tf.py'.lower() or trf_name.lower() == 'TrigMT_reco_tf.py'.lower()) \
                        and output_type == 'RAW':
                    output_type = 'BS'
                if trf_name.lower().startswith('TrigFTK'.lower()) and output_type == 'RAW':
                    output_type = 'BS'
                if output_type in list(change_output_type_dict.keys()):
                    output_type = change_output_type_dict[output_type]
                param_names = \
                    [e for e in trf_params if re.match(r"^(--)?output%s.*File$" % output_type, e, re.IGNORECASE)]
                if not param_names:
                    continue
                for param_name in param_names:
                    output_trf_params.append(param_name)

            for name in trf_params:
                if re.match('^(--)?extraParameter$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if param_value and str(param_value).lower() != 'none':
                        if '=' in param_value:
                            param_dict = {'name': param_value.split('=')[0], 'value': param_value.split('=')[1]}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                            )
                        else:
                            for extra_param in param_value.split(' '):
                                if extra_param not in trf_params:
                                    trf_params.append(extra_param)
                    break

            name = self._get_input_output_param_name(input_params, 'RAW')
            if not name:
                name = self._get_input_output_param_name(input_params, 'DRAW')
            if name and 'TRIGCOST' not in name:
                input_bs_type = 'inputBSFile'
                if input_bs_type not in list(input_params.keys()):
                    input_params[input_bs_type] = list()
                for input_param_value in input_params[name]:
                    if input_param_value not in input_params[input_bs_type]:
                        input_params[input_bs_type].append(input_param_value)
                del input_params[name]

            if empty_trf_params:
                for param in empty_trf_params:
                    if param:
                        for trf_param in trf_params[:]:
                            if re.match(r"^(--)?%s$" % param, trf_param, re.IGNORECASE):
                                param_dict = {'name': trf_param, 'value': '""'}
                                param_dict.update(trf_options)
                                job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                                logger.info(
                                    'TRF parameter {0} is used with an empty value'.format(trf_param)
                                )
                                trf_params.remove(trf_param)

            if ignore_trf_params:
                for param in ignore_trf_params:
                    if param:
                        for trf_param in trf_params[:]:
                            if re.match(r"^(--)?%s$" % param, trf_param, re.IGNORECASE):
                                logger.info(
                                    'TRF parameter {0} is removed from the list. It is ignored'.format(trf_param)
                                )
                                trf_params.remove(trf_param)
            full_chain_hashtag = None
            if project_mode.site is None:

                task_full_chain = self._task_full_chain(step, parent_task_id, project_mode)
                if task_full_chain:
                    full_chain_hashtag = self._set_task_full_chain(task_config, project_mode, task_full_chain)
                else:
                    self.set_jedi_full_chain(task_config, parent_task_id, project_mode)
            for name in trf_params:
                if re.match(r'^(--)?runNumber$', name, re.IGNORECASE):
                    run_number = input_data_dict['number']
                    try:
                        param_dict = {'name': name, 'value': int(run_number)}
                    except Exception as ex:
                        logger.exception("Exception occurred during obtaining runNumber: %s" % str(ex))
                        continue
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                elif re.match(r'^(--)?amiTag$', name, re.IGNORECASE):
                    param_dict = {'name': name, 'value': ctag_name}
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                elif re.match(r'^(--)?geometryversion$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value('geometry', ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(TaskParamName.CONSTANT, param_dict))
                elif re.match(r'^(--)?DBRelease$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        if (project_mode.containerName is None) and ('container_name' not in list(task_config.keys())):
                            project_mode.ipConnectivity = 'http'
                        continue
                    if not re.match(r'^\d+(\.\d+)*$', param_value):
                        if param_value.lower() == 'latest'.lower():
                            param_dict = {'name': name, 'dataset': self._get_latest_db_release()}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.DB_RELEASE, param_dict)
                            )
                        else:
                            param_dict = {'name': name, 'value': param_value}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                            )
                    else:
                        if trf_name.lower().find('_tf.') >= 0:
                            # --DBRelease=x.x.x
                            param_dict = {'name': name, 'value': param_value}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                            )
                        else:
                            db_rel_version = ''.join(["%2.2i" % int(i) for i in param_value.split('.')])
                            db_rel_name = \
                                "%s%s" % (TaskDefConstants.DEFAULT_DB_RELEASE_DATASET_NAME_BASE, db_rel_version)
                            param_dict = {'name': name,
                                          'dataset': db_rel_name}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.DB_RELEASE, param_dict)
                            )
                elif re.match(r'^(--)?jobConfig', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, input_params)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?rivetAnas', name, re.IGNORECASE):
                    if project_mode.rivet:
                        param_value = project_mode.rivet
                    else:
                        param_value = self._get_parameter_value(name, input_params)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?ignoreBlacklist', name, re.IGNORECASE):
                    if project_mode.ignoreBlacklist:
                        param_value = project_mode.ignoreBlacklist
                    else:
                        param_value = self._get_parameter_value(name, input_params)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?ignoreTestLHE', name, re.IGNORECASE):
                    if project_mode.ignoreTestLHE:
                        param_value = project_mode.ignoreTestLHE
                    else:
                        param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?ecmEnergy', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, input_params)
                    if not param_value:
                        param_value = self._get_energy(step, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?skipEvents$', name, re.IGNORECASE):
                    if (skip_events_forced is not None) and (skip_events_forced >= 0):
                        param_dict = {'name': name, 'value': skip_events_forced}
                        param_dict.update(trf_options)
                        job_parameters.append(
                            self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                        )
                        continue
                    else:
                        param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        if ('nEventsPerJob' in list(task_config.keys()) or 'nEventsPerRange' in list(
                                task_config.keys()) or
                            project_mode.tgtNumEventsPerJob) and \
                                ('nEventsPerInputFile' in list(task_config.keys()) or use_real_nevents):
                            param_dict = {'name': name}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.SKIP_EVENTS, param_dict)
                            )
                        else:
                            logger.warning('skipEvents parameter is omitted (step={0})'.format(step.id))
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match(r'^(--)?randomSeed$', name, re.IGNORECASE):
                    param_dict = {'name': name, 'offset': random_seed_offset}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(random_seed_proto_key, param_dict)
                    )
                elif re.match(r'^(--)?digiSeedOffset1$', name, re.IGNORECASE) or \
                        re.match(r'^(--)?digiSeedOffset2$', name, re.IGNORECASE):
                    input_real_data = False
                    # or check .RAW in the end: data12_8TeV.00208811.physics_JetTauEtmiss.merge.RAW?
                    for key in list(input_params.keys()):
                        if re.match(r'^(--)?inputBSFile$', key, re.IGNORECASE) or \
                                re.match(r'^(--)?inputRAWFile$', key, re.IGNORECASE):
                            input_real_data = True
                    if input_real_data:
                        continue
                    param_dict = {'name': name, 'offset': random_seed_offset}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(random_seed_proto_key, param_dict)
                    )
                elif re.match(r'^(--)?maxEvents$', name, re.IGNORECASE):
                    if max_events_forced:
                        param_value = max_events_forced
                    else:
                        param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        if ('nEventsPerJob' in list(task_config.keys()) or 'nEventsPerRange' in list(
                                task_config.keys()) or
                            project_mode.tgtNumEventsPerJob) and \
                                ('nEventsPerInputFile' in list(task_config.keys()) or use_real_nevents):
                            param_dict = {'name': name}
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.MAX_EVENTS, param_dict)
                            )
                        else:
                            logger.warning('maxEvents parameter is omitted (step={0})'.format(step.id))
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match('^(--)?firstEvent$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        if project_mode.optimalFirstEvent or task_config.get('optimalFirstEvent'):
                            param_dict = {'name': name, 'nEventsPerJob': int(task_config['nEventsPerJob']) }
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.SPECIAL_FIRST_EVENT, param_dict)
                            )
                        elif ('nEventsPerJob' in list(task_config.keys()) or 'nEventsPerRange' in list(
                                task_config.keys()) or
                            project_mode.tgtNumEventsPerJob) and \
                                ('nEventsPerInputFile' in list(task_config.keys()) or use_real_nevents):
                            param_dict = {'name': name, 'offset': 0}
                            if prod_step.lower() == 'evgen'.lower():
                                param_dict.update({'offset': first_event_offset})
                            param_dict.update(trf_options)
                            job_parameters.append(
                                self.protocol.render_param(TaskParamName.FIRST_EVENT, param_dict)
                            )
                        else:
                            logger.warning('firstEvents parameter is omitted (step={0})'.format(step.id))
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
                elif re.match('^(--)?extraParameter$', name, re.IGNORECASE):
                    continue
                elif re.match('^(--)?input(Filter|VertexPos)File$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if overlay_production:
                        proto_key = TaskParamName.OVERLAY_FILTER_FILE
                        param_dict = {'name': name, 'task_id': task_proto_id}
                    else:
                        proto_key = TaskParamName.CONSTANT
                        param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)
                    job_parameters.append(self.protocol.render_param(proto_key, param_dict))
                elif re.match('^(--)?hitarFile$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    param_dict = {'name': name, 'dataset': param_value}
                    param_dict.update(trf_options)
                    param = self.protocol.render_param(TaskParamName.HITAR_FILE, param_dict)
                    nf = self.rucio_client.get_number_files(param_value)
                    if nf > 1:
                        param['attributes'] = ''
                        param['ratio'] = 1
                    if secondary_input_offset:
                        param['offset'] = secondary_input_offset
                    job_parameters.append(param)
                elif re.match('^(--)?inputZeroBiasBSFile$', name, re.IGNORECASE) \
                        or re.match('^(--)inputRDO_BKGFile?$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if project_mode.randomMCOverlay and project_mode.randomMCOverlay != 'no':
                        mc_pileup_overlay['is_overlay'] = True
                        if project_mode.randomMCOverlay == 'single':
                            mc_pileup_overlay.update(
                                self._find_overlay_input_dataset(param_value, input_data_dict['number'], True))
                        else:
                            mc_pileup_overlay.update(self._find_overlay_input_dataset(param_value,input_data_dict['number']))
                        param_dict = {'name': name, 'dataset': mc_pileup_overlay['input_dataset_name']}
                        param_dict.update(trf_options)
                        if project_mode.eventRatio:
                            event_ratio = project_mode.eventRatio \
                                if '.' in str(project_mode.eventRatio) else int(project_mode.eventRatio)
                            param_dict.update({'event_ratio': event_ratio})
                            if type(event_ratio) is int and event_ratio > 1:
                                mc_pileup_overlay['event_ratio'] = event_ratio
                        second_input_param = \
                            self.protocol.render_param(TaskParamName.SECONDARY_INPUT_ZERO_BIAS_BS_RND, param_dict)
                        second_input_param['ratio'] = 1
                        job_parameters.append(second_input_param)
                    else:
                        if param_value[-1] != '/' and ('_tid' not in param_value):
                            if self.rucio_client.is_dsn_container(param_value):
                                param_value = '%s/' % param_value
                        param_dict = {'name': name, 'dataset': param_value}
                        param_dict.update(trf_options)
                        eventRatio = None
                        if project_mode.eventRatio:
                            event_ratio = project_mode.eventRatio \
                                if '.' in str(project_mode.eventRatio) else int(project_mode.eventRatio)
                            param_dict.update({'event_ratio': event_ratio})

                        n_pileup = None
                        if not project_mode.useRandomBkg:
                            second_input_param = \
                                self.protocol.render_param(TaskParamName.SECONDARY_INPUT_ZERO_BIAS_BS, param_dict)
                        else:
                            second_input_param = \
                                self.protocol.render_param(TaskParamName.SECONDARY_INPUT_ZERO_BIAS_BS_RND, param_dict)
                        if project_mode.npileup:
                            if 'inputRDO_BKGFile' in name:
                                raise Exception('Npileup is not supported for inputRDO_BKGFile')
                            n_pileup = project_mode.npileup \
                                if '.' in str(project_mode.npileup) else int(project_mode.npileup)
                            second_input_param['ratio']= n_pileup
                        if not n_pileup and not eventRatio:
                            second_input_param['eventRatio'] = 1
                            second_input_param['ratio'] = 1

                        if secondary_input_offset:
                            second_input_param['offset'] = secondary_input_offset
                        job_parameters.append(second_input_param)
                    is_pile_task = True
                elif re.match(r'^.*(PtMinbias|Cavern).*File$', name, re.IGNORECASE):
                    param_name = name
                    if not self.ami_client.is_new_ami_tag(ctag):
                        if re.match(r'^(--)?input(Low|High)PtMinbias.*File$', name, re.IGNORECASE):
                            name = name.replace('--', '').replace('input', '')
                        if re.match(r'^(--)?inputCavern.*File$', name, re.IGNORECASE):
                            name = name.replace('--', '').replace('input', '')
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if ',' not in param_value:
                        param_input_list = [param_value]
                    else:
                        param_input_list = param_value.split(',')
                    for param_index, param_value in enumerate(param_input_list):
                        if param_value[-1] != '/' and ('_tid' not in param_value):
                            param_value = '%s/' % param_value
                        param_value = param_value.strip()
                        postfix = ''
                        if param_index == 0:
                            postfix_index = ''
                        else:
                            postfix_index = f'_{param_index}'
                        if 'Low'.lower() in name.lower():
                            postfix = f'_LOW{postfix_index}'
                        elif 'High'.lower() in name.lower():
                            postfix = f'_HIGH{postfix_index}'

                        param_dict = {'name': param_name, 'dataset': param_value, 'postfix': postfix}
                        param_dict.update(trf_options)
                        if project_mode.eventRatio:
                            current_event_ratio = project_mode.eventRatio
                            if ',' in project_mode.eventRatio:
                                current_event_ratio = project_mode.eventRatio.split(',')[param_index]
                            if '.' in current_event_ratio:
                                event_ratio = str(current_event_ratio)
                            else:
                                event_ratio = int(current_event_ratio)
                            param_dict.update({'event_ratio': event_ratio})
                        if postfix.startswith('_LOW'):
                            if project_mode.eventRatioLow:
                                current_event_ratio = project_mode.eventRatioLow
                                if ',' in project_mode.eventRatioLow:
                                    current_event_ratio = project_mode.eventRatioLow.split(',')[param_index]
                                if '.' in current_event_ratio:
                                    event_ratio = str(current_event_ratio)
                                else:
                                    event_ratio = int(current_event_ratio)
                                param_dict.update({'event_ratio': event_ratio})
                        elif postfix.startswith('_HIGH'):
                            if project_mode.eventRatioHigh:
                                current_event_ratio = project_mode.eventRatioHigh
                                if ',' in project_mode.eventRatioLow:
                                    current_event_ratio = project_mode.eventRatioHigh.split(',')[param_index]
                                if '.' in current_event_ratio:
                                    event_ratio = str(current_event_ratio)
                                else:
                                    event_ratio = int(current_event_ratio)
                                param_dict.update({'event_ratio': event_ratio})
                        if 'Cavern'.lower() in name.lower():
                            second_input_param = self.protocol.render_param(TaskParamName.SECONDARY_INPUT_CAVERN,
                                                                            param_dict)
                        else:
                            second_input_param = self.protocol.render_param(TaskParamName.SECONDARY_INPUT_MINBIAS,
                                                                            param_dict)
                        n_pileup = TaskDefConstants.DEFAULT_MINIBIAS_NPILEUP
                        if project_mode.npileup:
                            n_pileup = project_mode.npileup \
                                if '.' in str(project_mode.npileup) else int(project_mode.npileup)
                        if postfix == '_LOW':
                            if project_mode.npileuplow:
                                n_pileup = project_mode.npileuplow \
                                    if '.' in str(project_mode.npileuplow) else int(project_mode.npileuplow)
                        elif postfix == '_HIGH':
                            if project_mode.npileuphigh:
                                n_pileup = project_mode.npileuphigh \
                                    if '.' in str(project_mode.npileuphigh) else int(project_mode.npileuphigh)
                        second_input_param['ratio'] = n_pileup
                        if secondary_input_offset:
                            second_input_param['offset'] = secondary_input_offset
                        job_parameters.append(second_input_param)

                    is_pile_task = True
                elif re.match(r'^(--)?input.*File$', name, re.IGNORECASE):
                    param_name = re.sub("(?<=input)evgen(?=file)", "EVNT".lower(), name.lower())
                    # BS (byte stream) - for all *RAW* (DRAW, RAW, DRAW_ZEE, etc.) [2]
                    if re.match(r'^(--)?inputBSFile$', name, re.IGNORECASE) and 'RAW'.lower() in ','.join(
                            [e.lower() for e in list(input_params.keys())]):
                        if 'TRIGCOST'.lower() not in ','.join([e.lower() for e in list(input_params.keys())]):
                            param_name = self._get_input_output_param_name(input_params, 'RAW')
                            if not param_name:
                                param_name = self._get_input_output_param_name(input_params, 'DRAW')
                                if not param_name:
                                    continue
                        else:
                            continue
                    if re.match(r'^(--)?inputESDFile$', name, re.IGNORECASE) and 'ESD'.lower() in ','.join(
                            [e.lower() for e in list(input_params.keys())]):
                        param_name = self._get_input_output_param_name(input_params, 'ESD')
                        if not param_name:
                            param_name = self._get_input_output_param_name(input_params, 'DESD')
                            if not param_name:
                                continue
                    if re.match(r'^(--)?inputLogsFile$', name, re.IGNORECASE) and 'log'.lower() in ','.join(
                            [e.lower() for e in list(input_params.keys())]):
                        param_name = self._get_input_output_param_name(input_params, 'log')
                        if not param_name:
                            continue
                    if re.match(r'^(--)?inputHISTFile', name, re.IGNORECASE) and 'HIST'.lower() in ','.join(
                            [e.lower() for e in list(input_params.keys())]):

                        param_name = self._get_input_output_param_name(input_params, 'HIST')
                        if not param_name:
                            continue
                    if re.match(r'^(--)?input(AOD|POOL)File$', name, re.IGNORECASE) and 'AOD'.lower() in ','.join(
                            [e.lower() for e in list(input_params.keys())]):
                        param_name = self._get_input_output_param_name(input_params, 'AOD')
                        if not param_name:
                            param_name = self._get_input_output_param_name(input_params, 'DAOD')
                            if not param_name:
                                continue
                    if re.match(r'^(--)?inputDataFile$', name, re.IGNORECASE):
                        param_name = self._get_input_output_param_name(input_params, '')
                    param_value = list(self._get_parameter_value(param_name, input_params))
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if not len(param_value):
                        continue
                    if len(param_value) > 1:
                        param_value = "{{%s_dataset}}" % self._get_parameter_name(param_name, input_params)
                    else:
                        param_value = param_value[0]

                    postfix = ''

                    try:
                        result = re.match(r'^(--)?input(?P<intype>.*)File$', name, re.IGNORECASE)
                        if result:
                            postfix = "_%s" % result.groupdict()['intype']
                            postfix = postfix.upper()
                    except Exception:
                        pass

                    param_dict = {'name': name, 'dataset': param_value, 'postfix': postfix}
                    param_dict.update(trf_options)
                    if use_direct_io:
                        input_param = self.protocol.render_param(TaskParamName.INPUT_DIRECT_IO, param_dict)
                    elif use_input_with_dataset:
                        input_param = self.protocol.render_param(TaskParamName.INPUT_WITH_DATASET, param_dict)
                    else:
                        input_param = self.protocol.render_param(TaskParamName.INPUT, param_dict)
                    job_parameters.append(input_param)
                    no_input = False
                elif re.match(r'^(--)?output(DAOD|D2AOD)File$', name, re.IGNORECASE) and train_production:
                    result = re.match(r'^(--)?output(?P<type>\w+)File$', name, re.IGNORECASE)
                    if not result:
                        continue
                    reduction_conf_base_output_type = result.groupdict()['type']
                    if reduction_conf_base_output_type not in reduction_conf_base_output_types:
                        continue
                    param_dict = {'name': name,
                                  'task_id': task_proto_id}
                    param_dict.update(trf_options)
                    merge_tag_name = self._get_merge_tag_name(step)
                    if merge_tag_name:
                        param_proto_key = TaskParamName.TRAIN_DAOD_FILE_JEDI_MERGE
                    else:
                        param_proto_key = TaskParamName.TRAIN_DAOD_FILE
                    job_parameters.append(self.protocol.render_param(param_proto_key, param_dict))
                elif re.match(r'^(--)?output.*File$', name, re.IGNORECASE):
                    if use_no_output:
                        continue
                    if output_trf_params and name not in output_trf_params:
                        continue
                    param_name = name
                    if re.match(r'^(--)?output.*_MRGFile$', name, re.IGNORECASE):
                        result = re.match(r'^(--)?output(?P<type>\w+)_MRGFile$', name, re.IGNORECASE)
                        if result:
                            internal_type = result.groupdict()['type']
                            if internal_type.lower() == 'BS'.lower():
                                internal_type = 'RAW'
                            param_name = self._get_input_output_param_name(output_params, internal_type)
                            if not param_name:
                                param_name = self._get_input_output_param_name(output_params, "D%s" % internal_type)
                                if not param_name:
                                    continue
                    elif re.match(r'^(--)?outputBS.*File$', name, re.IGNORECASE) and 'RAW'.lower() in ','.join(
                            [e.lower() for e in list(output_params.keys())]):
                        param_name = self._get_input_output_param_name(output_params, 'RAW')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputAODFile$', name, re.IGNORECASE) and 'AOD'.lower() in ','.join(
                            [e.lower() for e in list(output_params.keys())]):
                        if train_production:
                            continue
                        param_name = self._get_input_output_param_name(output_params, 'AOD')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputESDFile$', name, re.IGNORECASE) and 'ESD'.lower() in ','.join(
                            [e.lower() for e in list(output_params.keys())]):
                        param_name = self._get_input_output_param_name(output_params, 'ESD')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputDAODFile$', name, re.IGNORECASE) and 'DAOD'.lower() in ','.join(
                            [e.lower() for e in list(output_params.keys())]):
                        param_name = self._get_input_output_param_name(output_params, 'DAOD')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputHITS_RNMFile$', name, re.IGNORECASE) and 'HITS'.lower() in ','.join(
                                [e.lower() for e in list(output_params.keys())]):
                            continue
                    elif re.match(r'^(--)?outputHITS.*File$', name, re.IGNORECASE) and 'HITS'.lower() in ','.join(
                            [e.lower() for e in list(output_params.keys())]):
                        param_name = self._get_input_output_param_name(output_params, 'HITS')
                        if not param_name:
                            continue
                    elif re.match(r'^(--)?outputArchFile$', name, re.IGNORECASE):
                        param_name = self._get_input_output_param_name(output_params, output_types[0])
                    param_value = list(self._get_parameter_value(param_name, output_params))
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if not len(param_value):
                        continue
                    param_value = param_value[0]
                    output_dataset_dict = self.parse_data_name(param_value)
                    output_data_type = output_dataset_dict['data_type']
                    param_dict = {'name': name,
                                  'dataset': param_value,
                                  'data_type': output_data_type,
                                  'task_id': task_proto_id}
                    param_dict.update(trf_options)
                    proto_key = TaskParamName.OUTPUT
                    if project_mode.orderedOutput:
                        proto_key = TaskParamName.ORDERED_OUTPUT
                    if train_production and output_data_type.split('_')[0] == 'DAOD':
                        proto_key = TaskParamName.TRAIN_OUTPUT
                    elif re.match(r'^(--)?outputTXT_EVENTIDFile$', name, re.IGNORECASE):
                        proto_key = TaskParamName.TXT_EVENTID_OUTPUT
                    elif re.match(r'^(--)?outputTXT.*File$', name, re.IGNORECASE):
                        proto_key = TaskParamName.TXT_OUTPUT
                    elif re.match(r'^(--)?outputTAR_CONFIGFile$', name, re.IGNORECASE):
                        proto_key = TaskParamName.TAR_CONFIG_OUTPUT
                    elif re.match(r'^(--)?outputArchFile$', name, re.IGNORECASE):
                        proto_key = TaskParamName.ZIP_OUTPUT
                        arch_param_dict = {'idx': 0}
                        arch_proto_key = TaskParamName.ZIP_MAP
                        arch_param = self.protocol.render_param(arch_proto_key, arch_param_dict)
                        job_parameters.append(arch_param)
                    elif re.match(r'^(--)?outputYODAFile$', name, re.IGNORECASE):
                        proto_key = TaskParamName.YODA_OUTPUT
                    elif re.match(r'^(--)?outputDRAW.*File$', name, re.IGNORECASE):
                        proto_key = TaskParamName.RAW_OUTPUT
                    output_param = self.protocol.render_param(proto_key, param_dict)
                    if project_mode.spacetoken is not None:
                        output_param['token'] = project_mode.spacetoken
                    if 'token' in list(task_config.keys()):
                        output_param['token'] = task_config['token']
                    if train_production and output_data_type.split('_')[0] == 'DAOD':
                        output_param['hidden'] = True
                    if train_production and output_data_type.split('_')[0] == 'D2AOD':
                        output_param['hidden'] = True
                    if is_not_transient_output:
                        output_param['transient'] = not is_not_transient_output
                    if allow_no_output_patterns:
                        for pattern in allow_no_output_patterns:
                            if re.match(r'^(--)?output%sFile$' % pattern, name, re.IGNORECASE):
                                output_param['allowNoOutput'] = True
                                logger.info("Output parameter {0} has attribute allowNoOutput=True".format(name))
                                break
                    if hidden_output_patterns:
                        for pattern in hidden_output_patterns:
                            if re.match(r'^(--)?output%sFile$' % pattern, name, re.IGNORECASE):
                                output_param['hidden'] = True
                                logger.info("Output parameter {0} has attribute hidden=True".format(name))
                                break
                    if output_ratio > 0:
                        output_param['ratio'] = output_ratio
                    job_parameters.append(output_param)
                elif re.match('^(--)?jobNumber$', name, re.IGNORECASE):
                    if trf_name.lower() == 'AtlasG4_tf.py'.lower():
                        continue
                    elif (trf_name.lower() == 'Sim_tf.py'.lower()) and (not project_mode.enableJobNumber):
                        continue
                    param_dict = {'name': name, 'offset': random_seed_offset}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(random_seed_proto_key, param_dict)
                    )
                elif re.match('^(--)?filterFile$', name, re.IGNORECASE):
                    param_value = self._get_parameter_value(name, ctag)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    dataset = param_value
                    run_number_str = input_data_dict['number']
                    filenames = self.rucio_client.list_files_name_in_dataset(dataset)
                    filter_filename = None
                    for filename in filenames:
                        if run_number_str in filename:
                            filter_filename = filename
                            break
                    if not filter_filename:
                        logger.info("Step = %d, filter file is not found" % step.id)
                        continue
                    param_dict = {'name': name, 'dataset': dataset, 'filename': filter_filename, 'ratio': 1,
                                  'files': [{'lfn': filter_filename}]}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.FILTER_FILE, param_dict)
                    )
                else:
                    param_value = self._get_parameter_value(name, ctag, sub_steps=trf_sub_steps)
                    if not param_value or str(param_value).lower() == 'none':
                        continue
                    if str(param_value).lower() == 'none,none':
                        continue
                    param_dict = {'name': name, 'value': param_value}
                    param_dict.update(trf_options)

                    if trf_sub_steps is not None:
                        for trf_sub_step in trf_sub_steps:
                            if "%s:" % trf_sub_step in param_value:
                                param_dict.update({'separator': ' '})
                                break

                    if re.match('^(--)?validationFlags', name, re.IGNORECASE):
                        param_dict.update({'separator': ' '})
                    elif re.match('^(--)?skipFileValidation', name, re.IGNORECASE):
                        if param_value.lower() == 'True'.lower():
                            param_dict.update({'separator': ''})
                            param_dict.update({'value': ''})
                        else:
                            continue
                    elif re.match('^(--)?athenaMPMergeTargetSize', name, re.IGNORECASE):
                        param_dict.update({'separator': ' '})
                    elif re.match('^(--)?(steering|triggerConfig|CA)', name, re.IGNORECASE):
                        if ' ' in param_value:
                            param_dict.update({'separator': ' '})

                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )

            if not job_parameters:
                raise Exception("List of task parameters is empty")
            no_output = True
            input_types_defined = list()
            output_types_defined = list()
            output_types_defined.append('log')
            # filter doubled input parameters
            current_job_parameters = list()
            for job_param in job_parameters:
                if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                    result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                    if not result:
                        current_job_parameters.append(job_param)
                        continue
                    in_type = result.groupdict()['intype']
                    parameter_used = False
                    for index,current_job_param in enumerate(current_job_parameters):
                        if current_job_param.get('param_type','')=='input' and 'dataset' in current_job_param and\
                                current_job_param['dataset'] == job_param.get('dataset',''):
                            parameter_used = True
                            if in_type.lower() in [x.lower() for x in current_job_param['dataset'].split('.')]:
                                current_job_parameters[index] = job_param
                            break
                    if not parameter_used:
                        current_job_parameters.append(job_param)
                else:
                    current_job_parameters.append(job_param)
            job_parameters = current_job_parameters
            for job_param in job_parameters:
                if index_consistent_param_list:
                    name = job_param.get('value', '').split('=')[0].replace('--', '')
                    if name in index_consistent_param_list or '--{0}'.format(name) in index_consistent_param_list:
                        job_param['indexConsistent'] = True
                if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                    result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                    if not result:
                        continue
                    in_type = result.groupdict()['intype']
                    if in_type.lower() == 'logs'.lower() or \
                            re.match(r'^.*(PtMinbias|Cavern|ZeroBiasBS|HITAR|Filter|RDO_BKG).*$', in_type,
                                     re.IGNORECASE):
                        continue
                    input_types_defined.append(in_type)
                    # moving primary input parameter
                    job_parameters.remove(job_param)
                    job_parameters.insert(0, job_param)
                if 'param_type' not in list(job_param.keys()):
                    continue
                if job_param['param_type'].lower() == 'output'.lower():
                    no_output = False
                    if 'dataset' in list(job_param.keys()):
                        output_dataset_dict = self.parse_data_name(job_param['dataset'])
                        output_types_defined.append(output_dataset_dict['data_type'])

            if no_output and not use_no_output:
                raise Exception("Output data are missing")

            output_types_not_defined = list(set(output_types).difference(set(output_types_defined)))
            if output_types_not_defined and not use_no_output:
                message = 'These requested outputs are not defined properly: {0}.' \
                    .format('.'.join(output_types_not_defined))
                param_names = \
                    [e.replace('--', '') for e in trf_params if re.match(r"^(--)?output.*File$", e, re.IGNORECASE)]
                message = '{0}\n[tag = {1} ({2},{3}_{4})] {2} supports only these output parameters: {5}' \
                    .format(message, ctag_name, trf_name, trf_cache, trf_release, ','.join(param_names))
                raise Exception(message)

            log_param_dict = {'dataset': output_params['outputlogFile'][0], 'task_id': task_proto_id}
            log_param_dict.update(trf_options)
            if project_mode.orderedOutput:
                log_param = self.protocol.render_param(TaskParamName.ORDERED_LOG, log_param_dict)
            else:
                log_param = self.protocol.render_param(TaskParamName.LOG, log_param_dict)
            if is_not_transient_output:
                log_param['transient'] = not is_not_transient_output
            if leave_log:
                self.protocol.set_leave_log_param(log_param)
            if 'token' in list(task_config.keys()):
                if (step.request.request_type.lower() in ['GROUP'.lower()]) or project_mode.useDestForLogs:
                    log_param['token'] = task_config['token']
            if 'Data' in input_types_defined:
                for job_param in job_parameters:
                    if re.match(r'^(--)?inputDataFile', job_param['value'], re.IGNORECASE):
                        if len(input_types_defined) > 1:
                            job_parameters.remove(job_param)
                            break

            if trf_name.lower() == 'DigiMReco_trf.py'.lower():
                if 'outputESDFile' not in list(output_params.keys()):
                    param_dict = {'name': 'outputESDFile', 'value': 'ESD.TMP._0000000_tmp.pool.root'}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )
            elif trf_name.lower() == 'Trig_reco_tf.py'.lower() or trf_name.lower() == 'TrigMT_reco_tf.py'.lower():
                for job_param in job_parameters[:]:
                    if re.match('^(--)?jobNumber$', job_param['value'], re.IGNORECASE):
                        job_parameters.remove(job_param)
                        break
            elif trf_name.lower() == 'csc_MergeHIST_trf.py'.lower():
                for job_param in job_parameters[:]:
                    job_param['value'] = job_param['value'].split('=')[-1]
            elif trf_name.lower() == 'POOLtoEI_tf.py'.lower():
                if use_no_output:
                    param_dict = {'name': '--outputEIFile', 'value': 'temp.ei.spb'}
                    if ei_output_filename:
                        param_dict = {'name': '--outputEIFile', 'value': ei_output_filename}
                    param_dict.update(trf_options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )

            if env_params_dict:
                for key in list(env_params_dict.keys()):
                    param_dict = {'name': '--env {0}'.format(key), 'value': env_params_dict[key]}
                    options = {'separator': '='}
                    param_dict.update(options)
                    job_parameters.append(
                        self.protocol.render_param(TaskParamName.CONSTANT, param_dict)
                    )

            if project_mode.reprocessing or (step.request.phys_group.lower() == 'REPR'.lower()):
                task_type = 'reprocessing'
            else:
                task_type = prod_step
                if is_pile_task:
                    task_type = 'pile'
            if prod_step.lower() == 'archive'.lower():
                task_type = prod_step

            campaign = ':'.join([_f for _f in (step.request.campaign, step.request.subcampaign, bunchspacing,) if _f])

            task_request_type = None
            if step.request.request_type.lower() == 'TIER0'.lower():
                task_request_type = 'T0spillover'

            task_trans_home_separator = '-'
            task_release = trf_release
            task_release_base = trf_release_base
            trans_uses_prefix = ''

            if use_nightly_release:
                task_trans_home_separator = '/'
                task_release = version_timestamp
                task_release_base = version_major

            task_proto_dict = {
                'trans_home_separator': task_trans_home_separator,
                'trans_uses_prefix': trans_uses_prefix,
                'job_params': job_parameters,
                'log': log_param,
                'architecture': project_mode.cmtconfig,
                'type': task_type,
                'taskname': taskname,
                'priority': priority,
                'cache': trf_cache,
                'release': task_release,
                'transform': trf_name,
                'release_base': task_release_base,
                'username': username,
                'usergroup': usergroup,
                'no_wait_parent': True,
                'max_attempt': TaskDefConstants.DEFAULT_MAX_ATTEMPT,
                'skip_scout_jobs': skip_scout_jobs,
                'campaign': campaign,
                'req_id': int(step.request.reqid),
                'prod_source': TaskDefConstants.DEFAULT_PROD_SOURCE,
                'use_real_nevents': use_real_nevents,
                'cpu_time_unit': 'HS06sPerEvent',
                'write_input_to_file': use_direct_io,
                'cloud': TaskDefConstants.DEFAULT_CLOUD,
                'reuse_sec_on_demand': True if is_pile_task else None,
                'request_type': task_request_type,
                'scout_success_rate': TaskDefConstants.DEFAULT_SCOUT_SUCCESS_RATE
            }

            core_count = 1
            if project_mode.coreCount is not None:
                core_count = project_mode.coreCount
                task_proto_dict.update({'number_of_cpu_cores': core_count})

            if step.request.request_type.lower() == 'MC'.lower() and step.request.phys_group.lower() == 'HION'.lower():
                if prod_step.lower() == 'recon'.lower() and project_mode.coreCount is None:
                    raise Exception('Core count is not defined for HION recon task. Please set coreCount in project mode')
            # https://twiki.cern.ch/twiki/bin/view/AtlasComputing/ProdSys#Default_cpuTime_cpu_TimeUnit_tab
            # https://twiki.cern.ch/twiki/bin/view/AtlasComputing/ProdSys#Default_base_RamCount_ramCount_r
            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'simul'.lower():
                    cpu_time = 3000
                    if core_count > 1:
                        if [x for x in job_parameters if 'multithreaded' in x.get('value','')]:
                            memory = 150
                            base_memory = 2200
                            cpu_time = 1500
                        else:
                            memory = 500
                            base_memory = 1000
                    task_proto_dict.update({'cpu_time': cpu_time})
                    task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
                elif prod_step.lower() == 'recon'.lower() or is_pile_task:
                    if core_count > 1:
                        memory = 1750
                        base_memory = 2000
            elif step.request.request_type.lower() == 'HLT'.lower():
                if prod_step.lower() == 'recon'.lower():
                    task_proto_dict.update({'cpu_time': 300})
                    task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
                    memory = 4000
                elif prod_step.lower() == 'merge'.lower():
                    if 'HIST'.lower() in '.'.join([e.lower() for e in output_types_defined]):
                        task_proto_dict.update({'cpu_time': 0})
                    else:
                        task_proto_dict.update({'cpu_time': 1})
                    task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
            elif step.request.request_type.lower() == 'GROUP'.lower():
                task_proto_dict.update({'cpu_time': 0})
                task_proto_dict.update({'cpu_time_unit': 'HS06sPerEvent'})
                task_proto_dict.update({'base_wall_time': 60})
                task_proto_dict.update({'cpu_time': 200})
                if project.lower().startswith('data'):
                    task_proto_dict.update({'goal': str(100.0)})
                    task_proto_dict.update({'use_exhausted': True})
                if core_count > 1:
                    memory = 1750
                    base_memory = 2000

            if trf_name in ['AODMerge_tf.py', 'DAODMerge_tf.py', 'Archive_tf.py', 'ESDMerge_tf.py', 'RDOMerge_tf.py',
                            'ReSim_tf.py']:
                task_proto_dict.update({'out_disk_count': 1000})
                task_proto_dict.update({'out_disk_unit': 'kB'})

            task_proto_dict.update({'ram_count': int(memory)})
            task_proto_dict.update({'base_ram_count': int(base_memory)})
            task_proto_dict.update({'ram_unit': 'MBPerCore'})

            if step.request.request_type.lower() == 'GROUP'.lower():
                task_proto_dict.update({'respect_split_rule': True})

            if project_mode.ramCount is not None:
                task_proto_dict.update({'ram_count': project_mode.ramCount})
            if project_mode.baseRamCount is not None:
                task_proto_dict.update({'base_ram_count': project_mode.baseRamCount})
            if project_mode.baseWalltime is not None:
                task_proto_dict.update({'base_wall_time': project_mode.baseWalltime})
            if project_mode.maxCoreCount is not None:
                task_proto_dict.update({'max_core_count': project_mode.maxCoreCount})
            if project_mode.nChunksToWait is not None:
                task_proto_dict.update({'number_of_chunks_to_wait': project_mode.nChunksToWait})
            if project_mode.site is not None:
                site_value = project_mode.site
                specified_sites = list()
                if ',' in site_value:
                    specified_sites.extend(site_value.split(','))
                else:
                    specified_sites.append(site_value)
                available_sites = self.agis_client.get_sites()
                for site_name in specified_sites:
                    if site_name not in available_sites:
                        raise UnknownSiteException(site_name)
                task_proto_dict.update({'site': site_value})
            if project_mode.excludedSites is not None:
                site_value = project_mode.excludedSites
                specified_sites = list()
                if ',' in site_value:
                    specified_sites.extend(site_value.split(','))
                else:
                    specified_sites.append(site_value)
                available_sites = self.agis_client.get_sites()
                for site_name in specified_sites:
                    if site_name not in available_sites:
                        raise UnknownSiteException(site_name)
                task_proto_dict.update({'excludedSites': site_value})

            if project_mode.disableReassign is not None:
                task_proto_dict.update({'disable_reassign': project_mode.disableReassign or None})

            if project_mode.skipScout is not None:
                task_proto_dict.update({'skip_scout_jobs': project_mode.skipScout or None})

            if project_mode.t1Weight is not None:
                task_proto_dict.update({'t1_weight': project_mode.t1Weight or None})

            if project_mode.lumiblock is not None:
                task_proto_dict.update({'respect_lb': project_mode.lumiblock or None})

            if project.lower() == 'mc14_ruciotest'.lower():
                task_proto_dict.update({'ddm_back_end': 'rucio'})
                task_proto_dict.update({'prod_source': 'rucio_test'})

            if no_input and number_of_events > 0:
                task_proto_dict.update({'number_of_events': number_of_events})
            elif not no_input and number_of_events > 0:
                if prod_step.lower() != 'evgen'.lower() and 'nEventsPerInputFile' in list(task_config.keys()):
                    number_input_files_requested = \
                        math.ceil(number_of_events / int(task_config['nEventsPerInputFile']))
                    if number_input_files_requested == 0:
                        raise Exception(
                            "Number of requested input files is null (Input events=%d, nEventsPerInputFile=%d)" %
                            (int(number_of_events), int(task_config['nEventsPerInputFile']))
                        )
                    task_proto_dict.update({'number_of_files': int(number_input_files_requested)})
                elif prod_step.lower() != 'evgen'.lower() and 'nEventsPerInputFile' not in list(task_config.keys()):
                    task_proto_dict.update({'number_of_events': number_of_events})

            if no_input:
                task_proto_dict.update({'no_primary_input': no_input})
                if 'number_of_events' not in list(task_proto_dict.keys()):
                    raise Exception("Number of events to be processed is mandatory when task has no input")

            if no_input and prod_step.lower() not in ['evgen', 'simul']:
                raise Exception('This type of task ({0}) cannot be submitted without input'.format(prod_step))

            if 'nFiles' in list(task_config.keys()):
                number_of_files = int(task_config['nFiles'])
                task_proto_dict.update({'number_of_files': number_of_files})

            if 'nEvents' in list(task_config.keys()):
                task_proto_dict.update({'number_of_events': int(task_config['nEvents'])})

            if 'nEventsPerInputFile' in list(task_config.keys()) and not no_input:
                number_of_events_per_input_file = int(task_config['nEventsPerInputFile'])
                task_proto_dict.update({'number_of_events_per_input_file': number_of_events_per_input_file})

            if 'nEventsPerJob' in list(task_config.keys()):
                number_of_events_per_job = int(task_config['nEventsPerJob'])
                task_proto_dict.update({'number_of_events_per_job': number_of_events_per_job})

            if 'nFilesPerJob' in list(task_config.keys()):
                number_of_files_per_job = int(task_config['nFilesPerJob'])
                if number_of_files_per_job == 0:
                    task_proto_dict.update({'number_of_files_per_job': None})
                else:
                    task_proto_dict.update({'number_of_files_per_job': number_of_files_per_job})
                if number_of_files_per_job > TaskDefConstants.DEFAULT_MAX_FILES_PER_JOB:
                    task_proto_dict.update({'number_of_max_files_per_job': number_of_files_per_job})

            if 'nEventsPerRange' in list(task_config.keys()):
                number_of_events_per_range = int(task_config['nEventsPerRange'])
                task_proto_dict.update({'number_of_events_per_range': number_of_events_per_range})

            if ('nEventsPerInputFile' in list(task_config.keys()) and 'nEventsPerJob' in list(task_config.keys())) and not('nFilesPerJob' in list(task_config.keys())):
                number_of_max_files_per_job = int(task_config['nEventsPerJob']) / int(task_config['nEventsPerInputFile'])
                if number_of_max_files_per_job > TaskDefConstants.DEFAULT_MAX_FILES_PER_JOB:
                    task_proto_dict.update({'number_of_max_files_per_job': math.ceil(number_of_max_files_per_job)})

            if 'nGBPerJob' in list(task_config.keys()):
                number_of_gb_per_job = int(task_config['nGBPerJob'])
                task_proto_dict.update({'number_of_gb_per_job': number_of_gb_per_job})

            if 'maxAttempt' in list(task_config.keys()):
                max_attempt = int(task_config['maxAttempt'])
                task_proto_dict.update({'max_attempt': max_attempt})

            if 'outputPostProcessing' in list(task_config.keys()):
                task_proto_dict.update({'output_post_processing': task_config['outputPostProcessing']})

            if 'multiStepExec' in list(task_config.keys()):
                task_proto_dict.update({'multi_step_exec': task_config['multiStepExec']})

            if 'container_name' in list(task_config.keys()):
                task_proto_dict.update({'container_name': task_config['container_name']})

            if 'onlyTagsForFC' in list(task_config.keys()):
                if task_config['onlyTagsForFC']:
                    task_proto_dict.update({'only_tags_for_fc': task_config['onlyTagsForFC']})

            if 'full_chain' in list(task_config.keys()):
                if task_config['full_chain']:
                    task_proto_dict.update({'full_chain': task_config['full_chain']})

            if 'maxFailure' in list(task_config.keys()):
                max_failure = int(task_config['maxFailure'])
                task_proto_dict.update({'max_failure': max_failure})

            if 'nEventsPerMergeJob' in list(task_config.keys()):
                number_of_events_per_merge_job = int(task_config['nEventsPerMergeJob'])
                task_proto_dict.update({'number_of_events_per_merge_job': number_of_events_per_merge_job})

            if step.request.phys_group.lower() in [e.lower() for e in ['THLT', 'REPR']]:
                task_proto_dict.update({'no_throttle': True})

            if step.request.phys_group.lower() in [e.lower() for e in ['REPR']]:
                task_proto_dict.update({'use_exhausted': True})

            if step.request.request_type.lower() == 'EVENTINDEX'.lower():
                task_proto_dict.update(({'ip_connectivity': "'full'"}))

            if mc_pileup_overlay['is_overlay']:
                task_proto_dict.update({'task_broker_on_master': True})

            if project_mode.ipConnectivity is not None:
                task_proto_dict.update({'ip_connectivity': "'%s'" % project_mode.ipConnectivity})

            if project_mode.tgtNumEventsPerJob:
                task_proto_dict.update({'tgt_num_events_per_job': project_mode.tgtNumEventsPerJob})

            if project_mode.cpuTime is not None:
                task_proto_dict.update({'cpu_time': project_mode.cpuTime})

            if project_mode.cpuTimeUnit is not None:
                task_proto_dict.update({'cpu_time_unit': project_mode.cpuTimeUnit})

            if project_mode.iointensity is not None:
                task_proto_dict.update({'io_intensity': project_mode.iointensity})
                task_proto_dict.update({'io_intensity_unit': 'kBPerS'})

            if project_mode.gshare is not None:
                all_gshares = GlobalShare.objects.all().values_list('name',flat=True)
                for gshare in all_gshares:
                    if gshare.replace(" ","") == project_mode.gshare:
                        task_proto_dict.update({'global_share': gshare})
                        break

            if project_mode.workDiskCount is not None:
                task_proto_dict.update({'work_disk_count': project_mode.workDiskCount})

            if project_mode.workDiskUnit is not None:
                task_proto_dict.update({'work_disk_unit': project_mode.workDiskUnit})

            if project_mode.goal is not None:
                task_proto_dict.update({'goal': project_mode.goal})
                if str(project_mode.goal) == '100':
                    task_proto_dict.update({'use_exhausted': True})

            if project_mode.skipFilesUsedBy:
                task_proto_dict.update({'skip_files_used_by': project_mode.skipFilesUsedBy})
                skip_check_input = True

            if project_mode.taskRecreation:
                skip_check_input = True
            if project_mode.patchRepro:
                if project_mode.patchRepro == 'wait':
                    raise Exception('Task is waiting patch to be produced')
                task_proto_dict.update({'skip_files_used_by': project_mode.patchRepro})
                skip_check_input = True
                follow_hashtags.append(TaskDefConstants.REPRO_PATCH_HASHTAG)
            if project_mode.noThrottle is not None:
                task_proto_dict.update({'no_throttle': project_mode.noThrottle or None})

            if project_mode.ramUnit is not None:
                task_proto_dict.update({'ram_unit': project_mode.ramUnit})

            if project_mode.baseRamCount is not None:
                task_proto_dict.update({'base_ram_count': project_mode.baseRamCount})

            if project_mode.nucleus is not None:
                task_proto_dict.update({'nucleus': project_mode.nucleus})

            if project_mode.workQueueName is not None:
                task_proto_dict.update({'work_queue_name': project_mode.workQueueName})

            if project_mode.allowInputWAN is not None:
                task_proto_dict.update({'allow_input_wan': project_mode.allowInputWAN})

            if project_mode.allowInputLAN is not None:
                task_proto_dict.update({'allow_input_lan': "'{0}'".format(project_mode.allowInputLAN)})

            if project_mode.nMaxFilesPerJob:
                task_proto_dict.update({'number_of_max_files_per_job': project_mode.nMaxFilesPerJob})

            if project_mode.orderedOutput:
                task_proto_dict.update({'add_nth_field_to_lfn': 3})
                task_proto_dict.update({'use_file_as_source_lfn': True})

            ttcr_timestamp = None

            try:
                if not self.template_type:
                    ttcr = TConfig.get_ttcr(project, prod_step, usergroup)
                    if ttcr > 0:
                        ttcr_timestamp = timezone.now() + datetime.timedelta(seconds=ttcr)
                        task_proto_dict.update({'ttcr_timestamp': str(ttcr_timestamp)})
            except Exception as ex:
                logger.exception('Getting TTC failed: {0}'.format(str(ex)))

            if project_mode.useJobCloning is not None:
                task_proto_dict.update({'use_job_cloning': project_mode.useJobCloning})

            if project_mode.nSitesPerJob:
                task_proto_dict.update({'number_of_sites_per_job': project_mode.nSitesPerJob})

            if project_mode.superBoost:
                task_proto_dict.update({'use_job_cloning':'runonce'})
                task_proto_dict.update({'number_of_sites_per_job': 2})

            if project_mode.altStageOut is not None:
                task_proto_dict.update({'alt_stage_out': project_mode.altStageOut})

            if project_mode.cpuEfficiency is not None:
                task_proto_dict.update({'cpu_efficiency': project_mode.cpuEfficiency})

            if project_mode.minGranularity is not None:
                task_proto_dict.update({'min_granularity': project_mode.minGranularity})
                if project_mode.maxEventsPerJob is None:
                    task_proto_dict.update({'max_events_per_job': TaskDefConstants.DEFAULT_MAX_EVENTS_PER_GRANULE_JOB})

            if project_mode.maxEventsPerJob is not None:
                task_proto_dict.update({'max_events_per_job': project_mode.maxEventsPerJob})

            if project_mode.respectSplitRule is not None:
                task_proto_dict.update({'respect_split_rule': project_mode.respectSplitRule or None})

            if step.request.request_type.lower() == 'MC'.lower():
                if prod_step.lower() == 'simul'.lower() and len(trf_release.split('.')) > 2 and \
                        ('.'.join(trf_release.split('.')[0:3]) in ['21.0.15','21.0.31']) and trf_name in ['Sim_tf.py']:
                    if project_mode.esConvertible is None:
                        project_mode.esConvertible = True

            # if not project_mode.ipConnectivity and int(trf_release.split('.')[0])<=20:
            #     task_proto_dict.update({'ip_connectivity': "''" })

            if project_mode.esFraction is not None:
                if project_mode.esFraction > 0:
                    task_proto_dict.update({'es_fraction': project_mode.esFraction})
                    task_proto_dict.update({'es_convertible': True})
                    project_mode.esMerging = True

            if project_mode.esConvertible is not None:
                if project_mode.esConvertible:
                    task_proto_dict.update({'es_convertible': True})
                    project_mode.esMerging = True
                    task_proto_dict['not_discard_events'] = True
                else:
                    task_proto_dict.update({'es_convertible': None})

            if project_mode.onSiteMerging is not None:
                es_merging_tag_name = ctag_name
                es_merging_trf_name = 'HITSMerge_tf.py'
                task_proto_dict['es_merge_spec'] = {}
                task_proto_dict['es_merge_spec']['transPath'] = es_merging_trf_name
                task_proto_dict['es_merge_spec']['jobParameters'] = \
                    '--AMITag {0} --DBRelease=current '.format(
                        es_merging_tag_name) + \
                    '--outputHitsFile=${OUTPUT0} --inputHitsFile=@inputFor_${OUTPUT0}'

            if project_mode.intermediateTask is not None:
                task_proto_dict.update({'intermediate_task': project_mode.intermediateTask})

            if project_mode.esMerging is not None:
                if project_mode.esMerging and not project_mode.onSiteMerging:
                    es_merging_tag_name = ctag_name
                    es_merging_trf_name = 'HITSMerge_tf.py'
                    task_proto_dict['es_merge_spec'] = {}
                    task_proto_dict['es_merge_spec']['transPath'] = es_merging_trf_name
                    name_postfix = ''
                    if trf_release in ['20.3.7.5', '20.7.8.7']:
                        name_postfix = '_000'
                    task_proto_dict['es_merge_spec']['jobParameters'] = \
                        '--AMITag {0} --DBRelease=current --autoConfiguration=everything '.format(
                            es_merging_tag_name) + \
                        '--outputHitsFile=${OUTPUT0} --inputHitsFile=@inputFor_${OUTPUT0}' + name_postfix

            if project_mode.esConsumers is not None:
                task_proto_dict['number_of_es_consumers'] = project_mode.esConsumers

            if project_mode.esMaxAttempt is not None:
                task_proto_dict['max_attempt_es'] = project_mode.esMaxAttempt

            if project_mode.esMaxAttemptJob is not None:
                task_proto_dict['max_attempt_es_job'] = project_mode.esMaxAttemptJob

            if project_mode.nJumboJobs is not None:
                task_proto_dict['number_of_jumbo_jobs'] = project_mode.nJumboJobs

            if project_mode.nEventsPerOutputFile is not None:
                task_proto_dict['number_of_events_per_output_file'] = project_mode.nEventsPerOutputFile

            if project_mode.nEventsPerWorker:
                task_proto_dict['number_of_events_per_worker'] = project_mode.nEventsPerWorker

            if project_mode.processingType is not None:
                task_proto_dict.update({'type': project_mode.processingType})

            if project_mode.prodSourceLabel is not None:
                task_proto_dict.update({'prod_source': project_mode.prodSourceLabel})

            if project_mode.skipShortInput is not None:
                task_proto_dict.update({'skip_short_input': project_mode.skipShortInput or None})

            if project_mode.skipShortOutput is not None:
                task_proto_dict.update({'skip_short_output': project_mode.skipShortOutput or None})

            if project_mode.registerEsFiles is not None:
                task_proto_dict.update({'register_es_files': project_mode.registerEsFiles or None})

            if project_mode.transUsesPrefix:
                task_proto_dict.update({'trans_uses_prefix': project_mode.transUsesPrefix})

            if step.request.request_type.lower() == 'MC'.lower() and use_real_nevents and 'nEventsPerJob' in list(task_config.keys()):
                task_proto_dict.update({'no_wait_parent': False})

            if project_mode.noWaitParent is not None:
                task_proto_dict.update({'no_wait_parent': project_mode.noWaitParent or None})

            if project_mode.usePrefetcher is not None:
                task_proto_dict.update({'use_prefetcher': project_mode.usePrefetcher or None})

            if project_mode.disableAutoFinish is not None:
                task_proto_dict.update({'disable_auto_finish': project_mode.disableAutoFinish or None})

            if project_mode.isMergeTask:
                task_proto_dict.update({'use_exhausted': True})
                task_proto_dict.update({'goal': str(100.0)})
                task_proto_dict.update({'fail_when_goal_unreached': False})
                task_proto_dict.update({'disable_auto_finish': True})

            if project_mode.outDiskCount is not None:
                task_proto_dict['out_disk_count'] = project_mode.outDiskCount
                task_proto_dict['out_disk_unit'] = 'kB'

            if project_mode.inFilePosEvtNum is not None:
                task_proto_dict.update({'in_file_pos_evt_num': project_mode.inFilePosEvtNum or None})

            if project_mode.tgtMaxOutputForNG is not None:
                task_proto_dict.update({'tgt_max_output_for_ng': project_mode.tgtMaxOutputForNG})

            if project_mode.maxWalltime is not None:
                task_proto_dict.update({'max_walltime': project_mode.maxWalltime})

            if project_mode.notDiscardEvents is not None:
                task_proto_dict.update({'not_discard_events': project_mode.notDiscardEvents or None})

            if project_mode.scoutSuccessRate is not None:
                task_proto_dict.update({'scout_success_rate': project_mode.scoutSuccessRate})

            reuse_input = None
            if project_mode.reuseInput is not None:
                if project_mode.reuseInput > 0:
                    reuse_input = project_mode.reuseInput

            if project_mode.orderByLB is not None:
                task_proto_dict.update({'order_by_lb': project_mode.orderByLB or None})

            truncate_output_formats = project_mode.truncateOutputFormats

            if project_mode.useZipToPin is not None:
                task_proto_dict.update({'use_zip_to_pin': project_mode.useZipToPin})

            if project_mode.noLoopingCheck is not None:
                task_proto_dict.update({'no_looping_check': project_mode.noLoopingCheck or None})

            if project_mode.taskBrokerOnMaster is not None:
                task_proto_dict.update({'task_broker_on_master': project_mode.taskBrokerOnMaster or None})

            if project_mode.onSiteMerging is not None:
                task_proto_dict.update({'on_site_merging': project_mode.onSiteMerging or None})

            if project_mode.releasePerLB is not None:
                task_proto_dict.update({'release_per_LB': project_mode.releasePerLB or None})

            if project_mode.toStaging is not None:
                task_proto_dict.update({'to_staging': project_mode.toStaging or None})

            if project_mode.inputPreStaging is not None:
                task_proto_dict.update({'input_pre_staging': project_mode.inputPreStaging or None})
                
            if project_mode.allowEmptyInput is not None:
                task_proto_dict.update({'allow_empty_input': project_mode.allowEmptyInput or None})

            if project_mode.fullChain is not None:
                task_proto_dict.update({'full_chain': project_mode.fullChain or None})

            if project_mode.orderInputBy is not None:
                task_proto_dict.update({'order_input_by': project_mode.orderInputBy or None})

            if project_mode.nocvmfs is not None:
                task_proto_dict.update({'multi_step_exec': {'containerOptions': {'execArgs': '--nocvmfs'}}})

            if project_mode.containerName is not None:
                task_proto_dict.update({'container_name': project_mode.containerName or None})

            if step.request.request_type.lower() == 'MC'.lower():
                if 'nEventsPerJob' in list(task_config.keys()) and number_of_events > 0:
                    number_of_jobs = int(number_of_events) / int(task_config['nEventsPerJob'])
                    if number_of_jobs <= 10:
                        task_proto_dict.update({'use_exhausted': True})
                        task_proto_dict.update({'goal': str(100.0)})
                        task_proto_dict.update({'fail_when_goal_unreached': False})
                        task_proto_dict.update({'disable_auto_finish': True})
                    else:
                        if number_of_events <= 1000:
                            task_proto_dict.update({'use_exhausted': True})
                            task_proto_dict.update({'goal': str(100.0)})
                            task_proto_dict.update({'fail_when_goal_unreached': True})

            if not evgen_params:
                self._check_number_of_events(step, project_mode)

            if number_of_events > 0 and 'nEventsPerJob' in list(task_config.keys()):
                number_of_jobs = number_of_events / int(task_config['nEventsPerJob'])
                if number_of_jobs > TaskDefConstants.DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK:
                    raise MaxJobsPerTaskLimitExceededException(number_of_jobs)

            if project_mode.failWhenGoalUnreached is not None:
                task_proto_dict.update({'fail_when_goal_unreached': project_mode.failWhenGoalUnreached or None})

            io_intensity = None

            if prod_step.lower() == 'merge'.lower():
                if trf_name.lower() == 'ESDMerge_tf.py'.lower():
                    io_intensity = 3000
                elif trf_name.lower() == 'HISTMerge_tf.py'.lower():
                    io_intensity = 2000
                elif trf_name.lower() == 'EVNTMerge_tf.py'.lower():
                    io_intensity = 4000
                elif trf_name.lower() == 'HITSMerge_tf.py'.lower():
                    io_intensity = 3000
                elif trf_name.lower() == 'AODMerge_tf.py'.lower():
                    io_intensity = 2000
                elif trf_name.lower() == 'NTUPMerge_tf.py'.lower():
                    io_intensity = 2000
                elif trf_name.lower() == 'DAODMerge_tf.py'.lower():
                    io_intensity = 2000
                elif trf_name.lower() == 'RDOMerge_tf.py'.lower():
                    io_intensity = 4000
            elif prod_step.lower() == 'deriv'.lower():
                if step.request.provenance.lower() == 'GP'.lower():
                    if (trf_name.lower() == 'Reco_tf.py'.lower()) or (trf_name.lower() == 'Derivation_tf.py'.lower()):
                        io_intensity = 500
                if re.match('^AP_(?!SOFT|REPR|UPG|THLT|VALI).*$', usergroup, re.IGNORECASE):
                    if trf_name.lower() == 'Reco_tf.py'.lower():
                        io_intensity = 5000
                    if trf_name.lower() == 'PRWConfig_tf.py'.lower():
                        io_intensity = 5000
            if trf_name.lower() == 'Archive_tf.py'.lower():
                io_intensity = 5000

            if io_intensity:
                task_proto_dict.update({'io_intensity': int(io_intensity)})
                task_proto_dict.update({'io_intensity_unit': 'kBPerS'})
            #Set GShare
            if usergroup in ['AP_VALI','GP_VALI']:
                if not project_mode.gshare:
                    task_proto_dict.update({'global_share': 'Validation'})
            # test Event Service
            if project_mode.testES:
                if project_mode.nEventsPerWorker:
                    task_proto_dict['number_of_events_per_worker'] = project_mode.nEventsPerWorker
                else:
                    task_proto_dict['number_of_events_per_worker'] = 1
                if project_mode.nEsConsumers:
                    task_proto_dict['number_of_es_consumers'] = project_mode.nEsConsumers
                else:
                    task_proto_dict['number_of_es_consumers'] = 1
                if project_mode.esProcessingType is not None:
                    task_proto_dict['type'] = project_mode.esProcessingType
                if project_mode.maxAttemptES is not None:
                    task_proto_dict['max_attempt_es'] = project_mode.maxAttemptES
                task_proto_dict['es_merge_spec'] = {}
                task_proto_dict['es_merge_spec']['transPath'] = 'HITSMerge_tf.py'
                name_postfix = ''
                if trf_release in ['20.3.7.5', '20.7.8.7']:
                    name_postfix = "_000"
                task_proto_dict['es_merge_spec']['jobParameters'] = \
                    "--AMITag s2049 --DBRelease=current --autoConfiguration=everything " \
                    "--outputHitsFile=${OUTPUT0} --inputHitsFile=@inputFor_${OUTPUT0}" + name_postfix

            if not use_real_nevents and \
                    'number_of_events_per_input_file' not in list(task_proto_dict.keys()) and \
                    'number_of_gb_per_job' not in list(task_proto_dict.keys()) and \
                    'tgt_max_output_for_ng' not in list(task_proto_dict.keys()):
                if 'number_of_files_per_job' not in list(task_proto_dict.keys()) and not project_mode.onSiteMerging:
                    task_proto_dict.update({'number_of_files_per_job': 1})

            if 'number_of_gb_per_job' in list(task_proto_dict.keys()) or 'tgt_max_output_for_ng' in list(
                    task_proto_dict.keys()):
                if 'respect_split_rule' not in list(task_proto_dict.keys()):
                    task_proto_dict.update({'respect_split_rule': True})

            if use_real_nevents:
                if 'number_of_max_files_per_job' not in list(task_proto_dict.keys()):
                    task_proto_dict.update({'number_of_max_files_per_job': 200})

            if 'number_of_gb_per_job' in list(task_proto_dict.keys()):
                if not project_mode.nMaxFilesPerJob:
                    task_proto_dict.update({'number_of_max_files_per_job': 1000})

            if use_real_nevents and 'number_of_events_per_input_file' in list(task_proto_dict.keys()):
                raise TaskConfigurationException(
                    "The task is rejected due to incompatible parameters: useRealNumEvents, 'Events per Input file'"
                )
            if is_pile_task and (not use_real_nevents) and (not ('number_of_events_per_input_file' in list(task_proto_dict.keys()))):
                raise TaskConfigurationException(
                    "The task is rejected - pile tasks required  Events per Input file or useRealNumEvents to be set"
                )

            if (prod_step.lower() == 'simul'.lower()and (not use_real_nevents) and (not ('number_of_events_per_input_file' in list(task_proto_dict.keys()))) and
                    not project_mode.noInputSimul):
                raise TaskConfigurationException(
                    "The task is rejected - simul tasks required  Events per Input file or useRealNumEvents to be set"
                )
            self._define_merge_params(step, task_proto_dict, train_production)

            if not project_mode.skipCMTConfigCheck:
                self._check_site_container(task_proto_dict)

            try:
                if not self.template_type:
                    self._set_pre_stage(step, task_proto_dict, project_mode)
            except Exception as ex:
                logger.warning('Prestage has problem {0}'.format(
                    str(ex)))
            task_proto = self.protocol.render_task(task_proto_dict)

            task_elements = list()

            input_file_dict = dict()
            for key in list(input_params.keys()):
                if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                    input_file_dict.update({key: input_params[key]})

            if len(list(input_file_dict.keys())):
                input_list_length = len(input_file_dict[list(input_file_dict.keys())[0]])
                all_lists = [input_file_dict[key] for key in list(input_file_dict.keys())]
                if any(len(input_list) != input_list_length for input_list in all_lists):
                    raise Exception("Input lists are different lengths")
                context_dict_list = list()
                for i in range(input_list_length):
                    context_dict = dict()
                    for key in list(input_file_dict.keys()):
                        context_dict.update({"%s_dataset" % key: input_file_dict[key][i]})
                    if len(list(context_dict.keys())):
                        context_dict_list.append(context_dict)
                for context_dict in context_dict_list:
                    template_string = self.protocol.serialize_task(task_proto)
                    task_template = Template(template_string)
                    task_string = task_template.render(Context(context_dict))
                    if not self.template_type:
                        task_id = self.task_reg.register_task_id()
                    else:
                        task_id = task_proto_id
                    task_string = task_string.replace(TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_proto_id,
                                                      TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_id)

                    task = self.protocol.deserialize_task(task_string)
                    task_elements.append({task_id: task})
            else:
                task_string = self.protocol.serialize_task(task_proto)
                if not self.template_type:
                    task_id = self.task_reg.register_task_id()
                else:
                    task_id = task_proto_id
                task_string = task_string.replace(TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_proto_id,
                                                  TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_id)
                task = self.protocol.deserialize_task(task_string)
                task_elements.append({task_id: task})

            if not len(task_elements):
                raise Exception("Input container doesn't exist or empty")

            for task_element in task_elements:
                task_id = list(task_element.keys())[0]
                task = list(task_element.values())[0]

                for key in list(output_params.keys()):
                    for output_dataset_name in output_params[key]:
                        output_dataset_name = output_dataset_name.replace(
                            TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_proto_id,
                            TaskDefConstants.DEFAULT_TASK_ID_FORMAT % task_id)
                        if len(output_dataset_name) > TaskDefConstants.DEFAULT_OUTPUT_NAME_MAX_LENGTH:
                            raise OutputNameMaxLengthException(output_dataset_name)

                if step.request.request_type.lower() == 'MC'.lower():
                    if prod_step.lower() == 'simul'.lower() and int(trf_release.split('.')[0]) >= 21 and not project_mode.onSiteMerging and not project_mode.noInputSimul:
                        self._check_task_number_of_jobs(task, number_of_events, step)

                self._check_task_unmerged_input(task, step, prod_step)
                self._check_task_merged_input(task, step, prod_step)
                # self._check_task_cache_version_consistency(task, step, trf_release)
                self._check_task_blacklisted_input(task, project_mode)
                self._check_campaign_subcampaign(step)
                set_mc_reprocessing_hashtag = False
                if not skip_check_input:
                    self._check_task_input(task, task_id, number_of_events, task_config, parent_task_id,
                                           input_data_name, step, primary_input_offset, prod_step,
                                           reuse_input=reuse_input, evgen_params=evgen_params,
                                           task_common_offset=task_common_offset)
                    set_mc_reprocessing_hashtag = self._check_task_recreated(task, step)
                if mc_pileup_overlay['is_overlay'] and not self.template_type:
                    split_by_datasets = project_mode.randomMCOverlay == 'single'
                    self._register_mc_overlay_dataset(mc_pileup_overlay, self._get_total_number_of_jobs(task, number_of_events), task_id, task, split_by_datasets)
                if project_mode.GRL or project_mode.FLD or project_mode.repeatDoneTaskInput:
                    primary_input = self._get_primary_input(task['jobParameters'])
                    primary_input_dataset = primary_input['dataset']
                    filtered_files = []
                    whole_dataset = False
                    if project_mode.GRL:
                        grl_file = self._find_grl_xml_file(input_data_name.split(':')[0].split('.')[0], project_mode.GRL)
                        grl_range = self._get_GRL_from_xml(grl_file)

                        filtered_files, whole_dataset = self._filter_input_dataset_by_GRL(primary_input_dataset, grl_range)
                    elif project_mode.FLD:
                        filtered_files, whole_dataset = self._filter_input_dataset_by_FLD(primary_input_dataset, project_mode.FLD)
                    elif project_mode.repeatDoneTaskInput:
                        filtered_files, whole_dataset = self._filter_input_dataset_by_previous_task(primary_input_dataset, project_mode.repeatDoneTaskInput)
                    if not whole_dataset:
                        new_input_name, new_dataset = self._find_grl_dataset_input_name(primary_input_dataset, filtered_files)
                        if new_dataset:
                            self._register_input_GRL_dataset(new_input_name, filtered_files, task_id)
                        for entity in task['jobParameters']:
                            if  entity['dataset'] == primary_input_dataset:
                                entity['dataset'] = new_input_name
                                break
                if step == first_step:
                    chain_id = task_id
                    primary_input_offset = 0
                    if first_parent_task_id:
                        parent_task_id = first_parent_task_id
                    else:
                        parent_task_id = self.task_reg.get_parent_task_id(step, task_id)
                if not self.template_type:
                    self.task_reg.register_task(task, step, task_id, parent_task_id, chain_id, project, input_data_name,
                                                number_of_events, step.request.campaign, step.request.subcampaign,
                                                bunchspacing, ttcr_timestamp,
                                                truncate_output_formats=truncate_output_formats,
                                                task_common_offset=task_common_offset)

                    self.task_reg.register_task_output(output_params,
                                                       task_proto_id,
                                                       task_id,
                                                       parent_task_id,
                                                       usergroup,
                                                       step.request.subcampaign)
                else:
                    task_template = self.task_reg.register_task_template(task, step, parent_task_id,
                                                                         template_type=self.template_type, template_build=self.template_build)
                    self.template_results[step.id] = task_template
                if set_mc_reprocessing_hashtag:
                    try:
                        created_task = ProductionTask.objects.get(id=task_id)
                        created_task.set_hashtag(TaskDefConstants.MC_DELETED_REPROCESSING_REQUEST_HASHTAG)
                    except Exception as e:
                        logger.warning('Problem with hashtag registration {0}'.format(str(e)))
                if full_chain_hashtag:
                    try:
                        created_task = ProductionTask.objects.get(id=task_id)
                        created_task.set_hashtag(full_chain_hashtag)
                    except Exception as e:
                        logger.error('Problem with hashtag registration {0}'.format(str(e)))
                for hashtag in follow_hashtags:
                    try:
                        created_task = ProductionTask.objects.get(id=task_id)
                        created_task.set_hashtag(hashtag)
                    except Exception as e:
                        logger.error('Problem with hashtag registration {0}'.format(str(e)))
                parent_task_id = task_id

    def _get_number_events_processed(self, step, requested_datasets=None):
        number_events_processed = 0
        tasks = []
        input_data_name = self.get_step_input_data_name(step)
        project_mode = ProjectMode(step)
        ps1_task_list = []
        # ps1_task_list = TTaskRequest.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete']),
        #                                             project=step.request.project,
        #                                             inputdataset=input_data_name,
        #                                             ctag=step.step_template.ctag,
        #                                             formats=step.step_template.output_formats)
        for ps1_task in ps1_task_list:
            number_events_processed += int(ps1_task.total_events or 0)

        split_slice = ProjectMode.get_task_config(step).get('split_slice')

        if split_slice:
            ps2_task_list = list(
                ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                                              (Q(step__slice__dataset=input_data_name) |
                                               Q(step__slice__dataset__endswith=input_data_name.split(':')[-1]) |
                                               Q(step__slice__input_data=input_data_name) |
                                               Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                                              project=step.request.project,
                                              step__step_template__ctag=step.step_template.ctag))
            # check child
            child_tasks = []
            for dataset in requested_datasets or []:
                child_tasks += list(ProductionTask.objects.filter(
                    ~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                    (Q(inputdataset=dataset) |
                     Q(inputdataset__endswith=dataset.split(':')[-1])),
                    project=step.request.project,
                    step__step_template__ctag=step.step_template.ctag))
            ps2_task_list += [x for x in child_tasks if x not in ps2_task_list]
        else:
            ps2_task_list = \
                list(ProductionTask.objects.filter(
                    ~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                    (Q(step__slice__dataset=input_data_name) |
                     Q(step__slice__dataset__endswith=input_data_name.split(':')[-1]) |
                     Q(step__slice__input_data=input_data_name) |
                     Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                    project=step.request.project,
                    step__step_template__ctag=step.step_template.ctag,
                    step__step_template__output_formats=step.step_template.output_formats))

            # check child
            child_tasks = []
            for dataset in requested_datasets or []:
                child_tasks += list(ProductionTask.objects.filter(
                    ~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                    (Q(inputdataset=dataset) |
                     Q(inputdataset__endswith=dataset.split(':')[-1])),
                    project=step.request.project,
                    step__step_template__ctag=step.step_template.ctag,
                    step__step_template__output_formats=step.step_template.output_formats))
            ps2_task_list += [x for x in child_tasks if x not in ps2_task_list]

        max_by_offset = 0
        for ps2_task in ps2_task_list:

            if split_slice:
                # comparing output formats
                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = ps2_task.step.step_template.output_formats.split('.')
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue

            jedi_task_existing = TTask.objects.get(id=ps2_task.id)

            previous_dsn = None
            if requested_datasets:
                task_existing = json.loads(jedi_task_existing._jedi_task_parameters)
                previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
                requested_datasets_no_scope = [e.split(':')[-1] for e in requested_datasets]
                previous_dsn_no_scope = previous_dsn.split(':')[-1]
                if previous_dsn_no_scope not in requested_datasets_no_scope:
                    continue

            if project_mode.checkOutputDeleted and ps2_task.status in ['done', 'finished']:
                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_status_dict = \
                    self.task_reg.check_task_output(ps2_task.id, requested_output_types)
                previous_output_exists = False
                for requested_output_type in requested_output_types:
                    if requested_output_type not in list(previous_output_status_dict.keys()):
                        continue
                    if previous_output_status_dict[requested_output_type]:
                        previous_output_exists = True
                        break
                    else:
                        logger.info('Output {0} of task {1} is deleted'.format(requested_output_type, ps2_task.id))
                if not previous_output_exists:
                    continue
            tasks.append(ps2_task.id)
            number_events = int(ps2_task.total_req_events or 0)
            if not number_events:
                if ps2_task.parent_id != ps2_task.id and previous_dsn:
                    number_events = self.rucio_client.get_number_events(previous_dsn)
                number_events = max(ps2_task.total_events, number_events)
            number_events_processed += number_events
            offset = jedi_task_existing.get_job_parameter('firstEvent', 'offset')
            if offset and offset > 0:
                max_by_offset = max(max_by_offset, number_events + offset)

        return max(number_events_processed, max_by_offset), tasks

    def _get_processed_datasets(self, step, requested_datasets=None, check_output_deleted=False):
        processed_datasets = []
        input_data_name = self.get_step_input_data_name(step)
        split_slice = ProjectMode.get_task_config(step).get('split_slice')

        if split_slice:
            ps2_task_list = \
                ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                                              (Q(step__slice__dataset=input_data_name) |
                                               Q(step__slice__dataset__endswith=input_data_name.split(':')[-1]) |
                                               Q(step__slice__input_data=input_data_name) |
                                               Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                                              project=step.request.project,
                                              step__step_template__ctag=step.step_template.ctag)
        else:
            ps2_task_list = \
                ProductionTask.objects.filter(~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                                              (Q(step__slice__dataset=input_data_name) |
                                               Q(step__slice__dataset__endswith=input_data_name.split(':')[-1]) |
                                               Q(step__slice__input_data=input_data_name) |
                                               Q(step__slice__input_data__endswith=input_data_name.split(':')[-1]) |
                                               Q(inputdataset=input_data_name) |
                                               Q(inputdataset__endswith=input_data_name.split(':')[-1])),
                                              project=step.request.project,
                                              step__step_template__ctag=step.step_template.ctag,
                                              step__step_template__output_formats=step.step_template.output_formats)

        for ps2_task in ps2_task_list:

            if split_slice:
                # comparing output formats
                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = ps2_task.step.step_template.output_formats.split('.')
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue
            if check_output_deleted and ps2_task.status in ['done', 'finished']:
                requested_output_types = step.step_template.output_formats.split('.')
                previous_output_types = ps2_task.step.step_template.output_formats.split('.')
                processed_output_types = [e for e in requested_output_types if e in previous_output_types]
                if not processed_output_types:
                    continue
                previous_output_status_dict = self.task_reg.check_task_output(ps2_task.id, processed_output_types)
                previous_output_exists = False
                for requested_output_type in processed_output_types:
                    if requested_output_type not in list(previous_output_status_dict.keys()):
                        continue
                    if previous_output_status_dict[requested_output_type]:
                        previous_output_exists = True
                        break
                    else:
                        logger.info('Output {0} of task {1} is deleted'.format(requested_output_type, ps2_task.id))
                if not previous_output_exists:
                    continue
            jedi_task_existing = TTask.objects.get(id=ps2_task.id)
            task_existing = json.loads(jedi_task_existing._jedi_task_parameters)
            previous_dsn = self._get_primary_input(task_existing['jobParameters'])['dataset']
            requested_datasets_no_scope = [e.split(':')[-1] for e in requested_datasets]
            previous_dsn_no_scope = previous_dsn.split(':')[-1]
            if requested_datasets:
                if previous_dsn_no_scope not in requested_datasets_no_scope:
                    continue
            processed_datasets.append(previous_dsn_no_scope)
        return processed_datasets

    def get_events_per_file(self, input_name):
        nevents_per_file = 0
        try:
            try:
                nevents_per_file = self.rucio_client.get_nevents_per_file(input_name)
            except Exception:
                nevents_per_file = self.ami_client.get_nevents_per_file(input_name)
        except Exception:
            logger.info("get_nevents_per_file, exception occurred: %s" % get_exception_string())
        return nevents_per_file

    def get_events_per_input_file(self, step, input_name, use_real_events=False):
        task_config = ProjectMode.get_task_config(step)
        if 'nEventsPerInputFile' not in list(task_config.keys()) or use_real_events:
            events_per_file = int(self.get_events_per_file(input_name))
        else:
            events_per_file = int(task_config['nEventsPerInputFile'])
        return events_per_file

    def get_events_in_datasets(self, datasets, step, use_real_events=False):
        number_events = 0
        for dataset_name in datasets:
            events_per_file = self.get_events_per_input_file(step, dataset_name, use_real_events=use_real_events)
            number_events_in_dataset = events_per_file * self.rucio_client.get_number_files(dataset_name)
            if use_real_events:
                number_events_in_rucio_dataset = self.rucio_client.get_number_events(dataset_name)
                if number_events_in_rucio_dataset > 0:
                    number_events_in_dataset = min(number_events_in_dataset, number_events_in_rucio_dataset)
            number_events += number_events_in_dataset
        return number_events

    def get_dataset_subcampaign(self, name):
        task_id = self._get_parent_task_id_from_input(name)
        if task_id == 0:
            return None

        tasks = ProductionTask.objects.filter(id=task_id)
        if not tasks:
            subcampaign = self.rucio_client.get_campaign(name)
            for e in list(TaskDefConstants.DEFAULT_SC_HASHTAGS.keys()):
                for pattern in TaskDefConstants.DEFAULT_SC_HASHTAGS[e]:
                    result = re.match(r'{0}'.format(pattern), subcampaign)
                    if result:
                        return e

        task = tasks[0]
        sc_hashtags = \
            [e + TaskDefConstants.DEFAULT_SC_HASHTAG_SUFFIX for e in list(TaskDefConstants.DEFAULT_SC_HASHTAGS.keys())]
        for e in sc_hashtags:
            try:
                hashtag = HashTag.objects.get(hashtag=e)
            except ObjectDoesNotExist:
                hashtag = HashTag(hashtag=e, type='UD')
                hashtag.save()
            if task.hashtag_exists(hashtag):
                return e.split(TaskDefConstants.DEFAULT_SC_HASHTAG_SUFFIX)[0]

        subcampaign = self.rucio_client.get_campaign(name)
        for e in list(TaskDefConstants.DEFAULT_SC_HASHTAGS.keys()):
            for pattern in TaskDefConstants.DEFAULT_SC_HASHTAGS[e]:
                result = re.match(r'{0}'.format(pattern), subcampaign)
                if result:
                    hashtag = HashTag.objects.get(hashtag=e + TaskDefConstants.DEFAULT_SC_HASHTAG_SUFFIX)
                    task.set_hashtag(hashtag)
                    return e

        return None

    def verify_container_consistency(self, input_name):
        if not self.rucio_client.is_dsn_container(input_name):
            return True

        dataset_list = list()
        result = self.rucio_client.get_datasets_and_containers(input_name, datasets_contained_only=True)
        dataset_list.extend(result['datasets'])

        previous_events_per_file = 0

        for dataset_name in dataset_list:
            number_files = self.rucio_client.get_number_files(dataset_name)
            number_events = self.rucio_client.get_number_events(dataset_name)
            events_per_file = math.ceil(float(number_events) / float(number_files))
            if previous_events_per_file == 0:
                previous_events_per_file = events_per_file
            else:
                if events_per_file != previous_events_per_file:
                    return False
        return True

    def verify_data_uniform(self, step, input_name):
        data_type = None
        try:
            data_type = self.parse_data_name(input_name)['data_type']
        except Exception:
            pass
        if data_type in ['TXT']:
            return

        task_config = ProjectMode.get_task_config(step)
        project_mode = ProjectMode(step)
        config_events_per_file = int(task_config.get('nEventsPerInputFile', 0))
        if not config_events_per_file:
            return
        dataset_list = list()

        if self.rucio_client.is_dsn_container(input_name):
            result = self.rucio_client.get_datasets_and_containers(input_name, datasets_contained_only=True)
            dataset_list.extend(result['datasets'])
        else:
            dataset_list.append(input_name)

        for dataset_name in dataset_list:
            events_per_file = 0
            number_files = self.rucio_client.get_number_files(dataset_name)
            number_events = self.rucio_client.get_number_events(dataset_name)
            if number_events > 0:
                events_per_file = math.ceil(float(number_events) / float(number_files))

            if not events_per_file:
                continue

            parent_events_per_job = 0
            parent_task_id = 0
            try:
                result = re.match(r'^.+_tid(?P<tid>\d+)_00$', dataset_name)
                if result:
                    parent_task = ProductionTask.objects.get(id=int(result.groupdict()['tid']))
                    parent_task_id = int(parent_task.id)
                    parent_events_per_job = int(ProjectMode.get_task_config(parent_task.step).get('nEventsPerJob', 0))
            except Exception as ex:
                logger.exception('Getting parent nEventsPerJob failed: {0}'.format(str(ex)))

            if parent_events_per_job:
                if config_events_per_file != parent_events_per_job:
                    if not project_mode.nEventsPerInputFile:
                        raise UniformDataException(dataset_name, events_per_file, number_events, number_files,
                                                   config_events_per_file, parent_events_per_job, parent_task_id)

    def _get_splitting_dict(self, step):
        # splitting chains
        splitting_dict = dict()
        if step.request.request_type.lower() in ['MC'.lower(), 'GROUP'.lower()]:
            ctag = self._get_ami_tag_cached(step.step_template.ctag)
            prod_step = self._get_prod_step(step.step_template.ctag, ctag)
            project_mode = ProjectMode(step)
            is_none_campaign = False
            prod_steps = list()
            campaigns = dict()

            input_data_name = self.get_step_input_data_name(step)
            if not self.is_new_jo_format(input_data_name):
                result = self.rucio_client.get_datasets_and_containers(input_data_name, datasets_contained_only=True)
            else:
                result = {'datasets': []}
            for name in result['datasets']:
                try:
                    name_dict = self.parse_data_name(name)
                    name_prod_step = name_dict['prod_step']
                    if name_prod_step not in prod_steps:
                        prod_steps.append(name_prod_step)
                    campaign = self.get_dataset_subcampaign(name)
                    if campaign:
                        if campaign not in list(campaigns.keys()):
                            campaigns[campaign] = list()
                        campaigns[campaign].append(name)
                    else:
                        if ('val' not in name) and (name.startswith('mc')) and (name[:4] > 'mc15'):
                            is_none_campaign = True
                except Exception as ex:
                    raise Exception(
                        'Processing of sub-campaign/campaign or production step failed: {0}'.format(str(ex)))
            if len(prod_steps) > 1:
                task_config = ProjectMode.get_task_config(step)
                task_config_changed = False
                if not project_mode.forceSplitInput:
                    task_config['project_mode'] = 'forceSplitInput=yes;{0}'.format(task_config.get('project_mode', ''))
                    task_config_changed = True
                if not project_mode.useContainerName:
                    task_config['project_mode'] = 'useContainerName=yes;{0}'.format(task_config.get('project_mode', ''))
                    task_config_changed = True
                if task_config_changed:
                    ProjectMode.set_task_config(step, task_config, keys_to_save=('project_mode',))
                    project_mode = ProjectMode(step)
            if len(list(campaigns.keys())) >= 1:
                if not project_mode.forceSplitInput:
                    task_config = ProjectMode.get_task_config(step)
                    task_config['project_mode'] = 'forceSplitInput=yes;{0}'.format(task_config.get('project_mode', ''))
                    ProjectMode.set_task_config(step, task_config, keys_to_save=('project_mode',))
                    project_mode = ProjectMode(step)
                if project_mode.runOnlyCampaign:
                    if is_none_campaign:
                        raise Exception('some of dataset has no sub-campaign/campaign, please contact MC coordinators')
                    requested_campaigns = list()
                    for value in project_mode.runOnlyCampaign.split(','):
                        for e in list(TaskDefConstants.DEFAULT_SC_HASHTAGS.keys()):
                            for pattern in TaskDefConstants.DEFAULT_SC_HASHTAGS[e]:
                                if re.match(r'{0}'.format(pattern), value) and (e not in requested_campaigns):
                                    requested_campaigns.append(e)
                    requested_datasets = list()
                    for requested_campaign in requested_campaigns:
                        if requested_campaign in list(campaigns.keys()):
                            requested_datasets.extend(campaigns[requested_campaign])
                    if len(requested_datasets) > 0:
                        result['datasets'] = requested_datasets
                    else:
                        raise NoRequestedCampaignInput()
                else:
                    requested_campaign = str(step.request.subcampaign)
                    requested_campaign = requested_campaign.replace('MC20','MC16')
                    if (requested_campaign.lower().startswith('MC16'.lower()) or  requested_campaign.lower().startswith('MC23'.lower())) and \
                            step.request.request_type.lower() == 'MC'.lower():
                        if is_none_campaign:
                            raise Exception(
                                'some of dataset has no sub-campaign/campaign, please contact MC coordinators')
                        requested_datasets = list()
                        for campaign in list(campaigns.keys()):
                            if campaign in TaskDefConstants.CAMPAIGNS_INTERCHANGEABLE[requested_campaign]:
                                requested_datasets.extend(campaigns[campaign])

                        if len(requested_datasets) > 0:
                            result['datasets'] = requested_datasets
                        else:
                            raise NoRequestedCampaignInput()

            force_merge_container = project_mode.mergeCont
            use_default_splitting_rule = True
            if project_mode.forceSplitInput:
                use_default_splitting_rule = False

            reuse_input = None
            if project_mode.reuseInput:
                if project_mode.reuseInput > 0:
                    reuse_input = project_mode.reuseInput

            if use_default_splitting_rule and \
                    (prod_step.lower() == 'evgen'.lower() or prod_step.lower() == 'simul'.lower()
                     or force_merge_container):
                return splitting_dict
            if project_mode.patchRepro:
                if project_mode.patchRepro == 'wait':
                    raise Exception('Task is waiting patched repro')
                project_mode.skipFilesUsedBy = int(project_mode.patchRepro)
            if project_mode.skipFilesUsedBy:
                job_params = self.task_reg.get_task_parameter(project_mode.skipFilesUsedBy, 'jobParameters')
                primary_input = self._get_primary_input(job_params)
                if primary_input:
                    splitting_dict[step.id] = list()
                    splitting_dict[step.id].append({'dataset': primary_input['dataset'],
                                                    'offset': 0,
                                                    'number_events': int(step.input_events),
                                                    'container': None})
                    return splitting_dict

            task_config = ProjectMode.get_task_config(step)
            if 'previous_task_list' in list(task_config.keys()):
                previous_task_list = ProductionTask.objects.filter(id__in=task_config['previous_task_list'])
                for previous_task in previous_task_list:
                    job_params = self.task_reg.get_task_parameter(previous_task.id, 'jobParameters')
                    primary_input = self._get_primary_input(job_params)
                    if primary_input:
                        if step.id not in list(splitting_dict.keys()):
                            splitting_dict[step.id] = list()
                        splitting_dict[step.id].append({'dataset': primary_input['dataset'],
                                                        'offset': 0,
                                                        'number_events': int(step.input_events),
                                                        'container': None})
                if splitting_dict:
                    return splitting_dict

            if reuse_input and len(result['datasets']) == 1:
                for i in range(reuse_input):
                    if step.id not in list(splitting_dict.keys()):
                        splitting_dict[step.id] = list()
                    splitting_dict[step.id].append({'dataset': result['datasets'][0], 'offset': 0,
                                                    'number_events': int(step.input_events), 'container': None})
                return splitting_dict

            if not self.rucio_client.is_dsn_container(input_data_name):
                return splitting_dict

            use_real_events = True
            if project_mode.useRealEventsCont is not None:
                use_real_events = project_mode.useRealEventsCont

            logger.info("Step = %d, container = %s, list of datasets = %s" %
                        (step.id, input_data_name, result['datasets']))

            number_events_in_container = \
                self.get_events_in_datasets(result['datasets'], step, use_real_events=use_real_events)
            if not number_events_in_container:
                raise Exception(
                    'Container {0} has no events or there is no information in AMI/Rucio'.format(input_data_name))

            logger.info("Step = %d, container = %s, number_events_in_container = %d" %
                        (step.id, input_data_name, number_events_in_container))
            if not number_events_in_container:
                logger.info("Step = %d, container %s is empty or nEventsPerInputFile is missing, skipping the step" %
                            (step.id, input_data_name))
                return splitting_dict
            number_events_processed, previous_existed_tasks = self._get_number_events_processed(step, result['datasets'])
            logger.info("Step = %d, number_events_processed = %d" % (step.id, number_events_processed))
            if project_mode.randomSeedOffset is not None and step.request.phys_group in ['VALI']:
                number_events_processed, previous_existed_tasks = 0, []
            if step.input_events > 0:
                number_events_requested = int(step.input_events)
            else:
                number_events_requested = number_events_in_container - number_events_processed

            if number_events_requested <= 0:
                raise NotEnoughEvents(previous_existed_tasks)

            if (number_events_requested + number_events_processed) > number_events_in_container:
                number_events_available = number_events_in_container - number_events_processed
                events_remains = \
                    float(number_events_requested - number_events_available) / float(number_events_requested) * 100
                if events_remains <= 10:
                    number_events_requested = number_events_available
                else:
                    raise NotEnoughEvents(previous_existed_tasks)
            if (step.input_events <= 0) and (step.request.request_type.lower() in ['GROUP'.lower()]):
                processed_datasets = []
                if number_events_processed > 0:
                    processed_datasets = self._get_processed_datasets(step, result['datasets'], project_mode.checkOutputDeleted)
                for dataset_name in result['datasets']:
                    if dataset_name.split(':')[-1] not in processed_datasets:
                        events_per_file = self.get_events_per_input_file(step, dataset_name,
                                                                         use_real_events=use_real_events)
                        if not events_per_file:
                            logger.info(
                                "Step = %d, nEventsPerInputFile for dataset %s is missing, skipping this dataset" %
                                (step.id, dataset_name))
                            return splitting_dict
                        number_events = events_per_file * self.rucio_client.get_number_files(
                            dataset_name)
                        if number_events:
                            if step.id not in list(splitting_dict.keys()):
                                splitting_dict[step.id] = list()
                            splitting_dict[step.id].append({'dataset': dataset_name, 'offset': 0,
                                                            'number_events': number_events,
                                                            'container': input_data_name})
                return splitting_dict
            start_offset = 0
            for dataset_name in result['datasets']:
                offset = 0
                number_events = 0
                events_per_file = self.get_events_per_input_file(step, dataset_name, use_real_events=use_real_events)
                if not events_per_file:
                    logger.info("Step = %d, nEventsPerInputFile for dataset %s is missing, skipping this dataset" %
                                (step.id, dataset_name))
                    return splitting_dict
                number_events_in_dataset = events_per_file * self.rucio_client.get_number_files(dataset_name)
                number_events_in_rucio_dataset = self.rucio_client.get_number_events(dataset_name)
                if number_events_in_rucio_dataset > 0:
                    number_events_in_dataset = min(number_events_in_dataset, number_events_in_rucio_dataset)
                try:
                    if (start_offset + number_events_in_dataset) < number_events_processed:
                        # skip dataset, all events are processed
                        continue
                    offset = number_events_processed - start_offset
                    if number_events_requested > number_events_in_dataset - offset:
                        number_events = number_events_in_dataset - offset
                    else:
                        number_events = number_events_requested
                        # break, all events are requested
                        break
                finally:
                    start_offset += number_events_in_dataset
                    number_events_requested -= number_events
                    number_events_processed += number_events
                    if number_events:
                        if step.id not in list(splitting_dict.keys()):
                            splitting_dict[step.id] = list()
                        splitting_dict[step.id].append({'dataset': dataset_name,
                                                        'offset': int(offset / events_per_file),
                                                        'number_events': number_events,
                                                        'container': input_data_name})
        return splitting_dict

    def _get_evgen_input_list(self, step, optimalFirstEvent = False):
        evgen_input_list = list()
        input_data_name = self.get_step_input_data_name(step)
        task_config = ProjectMode.get_task_config(step)
        project_mode = ProjectMode(step)
        ctag_name = step.step_template.ctag
        ctag = self._get_ami_tag_cached(ctag_name)
        energy_gev = self._get_energy(step, ctag)
        input_params = self.get_input_params(step, step, False, energy_gev, False)
        container_name_key = None
        container_name = None
        for key in list(input_params.keys()):
            if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                container_name_key = key
                container_name = input_params[key][0]
                break

        if not container_name:
            raise Exception('No input container found')

        if 'nFilesPerJob' in list(input_params.keys()) and 'nFilesPerJob' not in list(task_config.keys()):
            task_config.update({'nFilesPerJob': int(input_params['nFilesPerJob'])})

        if 'previous_task_list' in list(task_config.keys()):
            previous_task_list = ProductionTask.objects.filter(
                id__in=task_config['previous_task_list'])
            for previous_task in previous_task_list:
                jedi_task = TTask.objects.get(id=previous_task.id)
                task_params = json.loads(jedi_task._jedi_task_parameters)
                job_params = task_params['jobParameters']
                random_seed = self._get_job_parameter('randomSeed', job_params)
                dsn = self._get_primary_input(job_params)['dataset'].split(':')[-1]
                offset = 0
                if random_seed:
                    offset = int(random_seed['offset'])
                else:
                    raise Exception('There is no randomSeed parameter in the previous task')
                nfiles = int(task_params.get('nFiles'))
                nfiles_per_job = int(task_params.get('nFilesPerJob'))
                nevents_per_job = int(task_params.get('nEventsPerJob'))
                if not nfiles or not nfiles_per_job or not nevents_per_job:
                    raise Exception(
                        'Necessary task parameters are missing in the previous task')
                input_params_split = copy.deepcopy(input_params)
                input_params_split['nevents'] = math.ceil(float(nfiles * nevents_per_job) / float(nfiles_per_job))
                input_params_split['nfiles'] = nfiles
                input_params_split['offset'] = offset
                input_params_split['event_offset'] = int(offset * nevents_per_job / nfiles_per_job)
                input_params_split[container_name_key] = list([dsn])
                evgen_input_list.append(input_params_split)
            return evgen_input_list

        datasets = self.rucio_client.list_datasets_in_container(container_name)

        nfiles_used = 0
        task = None
        task_list = \
            ProductionTask.objects.filter(
                ~Q(status__in=['failed', 'broken', 'aborted', 'obsolete', 'toabort']) &
                (Q(step__slice__input_data=input_data_name) |
                 Q(step__slice__input_data__endswith=input_data_name.split(':')[-1])),
                project=step.request.project,
                step__step_template__ctag=step.step_template.ctag).order_by(
                '-id')
        for previous_task in task_list:
            requested_output_types = step.step_template.output_formats.split('.')
            previous_output_types = previous_task.step.step_template.output_formats.split('.')
            processed_output_types = [e for e in requested_output_types if e in previous_output_types]
            if not processed_output_types:
                continue
            task = previous_task
            break

        task_dsn_no_scope = None

        if task:
            jedi_task = TTask.objects.get(id=task.id)
            task_params = json.loads(jedi_task._jedi_task_parameters)
            task_random_seed = \
                self._get_job_parameter('randomSeed', task_params['jobParameters'])
            task_dsn_no_scope = \
                self._get_primary_input(task_params['jobParameters'])['dataset'].split(':')[-1]
            offset = 0
            if task_random_seed:
                offset = int(task_random_seed['offset'])
            nfiles_used = offset
            if optimalFirstEvent:
                nfiles_used = offset * task_config.get('nFilesPerJob', 1)
            if 'nFiles' in task_params:
                nfiles_in_tid_ds = 0
                if 'tid' in task_dsn_no_scope:
                    nfiles_in_tid_ds = self.rucio_client.get_number_files_from_metadata(task_dsn_no_scope)
                nfiles_used += max([int(task_params['nFiles']), nfiles_in_tid_ds])

        if project_mode.splitEvgenOffsetByLast:
            total_files = 0
            for dsn in datasets:
                tid_dsn_no_scope = dsn.split(':')[-1]
                nfiles_in_tid_ds = self.rucio_client.get_number_files_from_metadata(dsn)
                total_files += nfiles_in_tid_ds
                if tid_dsn_no_scope == task_dsn_no_scope:
                    nfiles_used = max([total_files, nfiles_used])
                    break

        nevents_per_job = int(input_params.get('nEventsPerJob'))
        if not nevents_per_job:
            raise Exception(
                'JO file {0} does not contain nEventsPerJob definition. '.format(
                    input_data_name) +
                'The task is rejected')
        nfiles_per_job = task_config.get('nFilesPerJob', 1)

        nfiles_requested = math.ceil(int(step.input_events) * nfiles_per_job / nevents_per_job)
        nfiles = 0
        files_used_count = nfiles_used
        files_requested_count = nfiles_requested

        for dsn in datasets:
            dsn_no_scope = dsn.split(':')[-1]
            nfiles_in_ds = self.rucio_client.get_number_files_from_metadata(dsn)
            files_used_count -= nfiles_in_ds
            if files_used_count >= 0:
                continue
            if nfiles_in_ds + files_used_count > 0:
                continue
            if dsn_no_scope == task_dsn_no_scope:
                continue
            input_params_split = copy.deepcopy(input_params)
            files_requested_count -= nfiles_in_ds
            if files_requested_count > 0:
                input_params_split['nevents'] = math.ceil(nfiles_in_ds * nevents_per_job // nfiles_per_job)
                input_params_split['nfiles'] = nfiles_in_ds
                input_params_split['offset'] = nfiles_used + nfiles
                input_params_split['event_offset'] = \
                    math.ceil(input_params_split['offset'] * nevents_per_job )
                if optimalFirstEvent:
                    input_params_split.pop('event_offset')
                    input_params_split['offset'] = math.ceil((nfiles_used + nfiles)/nfiles_per_job)
                input_params_split[container_name_key] = list([dsn])
                evgen_input_list.append(input_params_split)
                nfiles += nfiles_in_ds

            else:
                input_params_split['nevents'] = \
                    math.ceil((nfiles_requested - nfiles) * nevents_per_job // nfiles_per_job)
                input_params_split['nfiles'] = (nfiles_requested - nfiles)
                input_params_split['offset'] = nfiles_used + nfiles
                input_params_split['event_offset'] = \
                    math.ceil(input_params_split['offset'] * nevents_per_job )
                if optimalFirstEvent:
                    input_params_split.pop('event_offset')
                    input_params_split['offset'] = math.ceil((nfiles_used + nfiles)/nfiles_per_job)
                input_params_split[container_name_key] = list([dsn])
                evgen_input_list.append(input_params_split)
                nfiles += (nfiles_requested - nfiles)

                break
        if nfiles < nfiles_requested:
            raise Exception(
                'No more input files in {0}. Only {1} files are available'.format(container_name, nfiles)
            )
        if not evgen_input_list:
            raise Exception(
                'No unprocessed datasets in the container {0}'.format(container_name)
            )
        return evgen_input_list

    def _build_linked_step_list(self, req, input_slice):
        # Approved
        step_list = list(StepExecution.objects.filter(request=req,
                                                      status=self.protocol.STEP_STATUS[StepStatus.APPROVED],
                                                      slice=input_slice))
        result_list = []
        temporary_list = []
        another_chain_step = None
        for step in step_list:
            if step.step_parent_id == step.id:
                if result_list:
                    raise ValueError('Not linked chain')
                else:
                    result_list.append(step)
            else:
                temporary_list.append(step)
        if not result_list:
            for index, current_step in enumerate(temporary_list):
                step_parent = StepExecution.objects.get(id=current_step.step_parent_id)
                if step_parent not in temporary_list:
                    # step in other chain
                    another_chain_step = step_parent
                    result_list.append(current_step)
                    temporary_list.pop(index)
        for i in range(len(temporary_list)):
            j = 0
            while temporary_list[j].step_parent_id != result_list[-1].id:
                j += 1
                if j >= len(temporary_list):
                    raise ValueError('Not linked chain')
            result_list.append(temporary_list[j])
        return result_list, another_chain_step

    @staticmethod
    def _get_request_status(request, summary_log, locked_time, current_status, keep_approved = False):
        if current_status in ['test']:
            return 'test'
        if keep_approved:
            return 'approved'
        statuses = RequestStatus.objects.filter(request=request).order_by('-timestamp')
        if statuses[0].timestamp > locked_time and statuses[0].status == 'approved' and current_status == 'approved':
            return 'approved'
        number_of_repeated_attempts = 0
        for status in statuses:
            if status.owner == 'deft' and status.comment == TaskDefConstants.REPEAT_ATTEMPT_MESSAGE:
                number_of_repeated_attempts += 1
            else:
                break

        if number_of_repeated_attempts < TaskDefConstants.TRANSIENT_ERROR_ATTEMPTS:
            for error in TaskDefConstants.ERRORS_TO_REPEAT:
                if error in summary_log:
                    new_status = RequestStatus()
                    new_status.request = request
                    new_status.status = 'approved'
                    new_status.comment = TaskDefConstants.REPEAT_ATTEMPT_MESSAGE
                    new_status.owner = 'deft'
                    new_status.timestamp = timezone.now()
                    new_status.save()
                    return 'approved'
        for status in statuses:
            if status.status not in ['approved', 'comment']:
                if status.status in ['waiting', 'registered']:
                    return 'working'
                else:
                    return status.status

    def set_error(self, log_msg, request, step, exception_name, exception_type, exception_string, jira_client ):
        self.set_slice_error(request, step.slice.id, exception_type, exception_string)
        if not self.template_type:
            jira_client.log_exception(request.jira_reference, exception_name, log_msg=log_msg)
        else:
            logger.exception(log_msg)
            if step.id in self.template_results:
                task_template = self.template_results[step.id]
            else:
                if TaskTemplate.objects.filter(step=step, request=step.request, template_type=self.template_type,
                                               build=self.template_build).exists():
                    task_template = TaskTemplate.objects.get(step=step, request=step.request,
                                                             template_type=self.template_type, build=self.template_build)
                    task_template.name = 'NotDefined'
                    task_template.task_error = None
                    task_template.task_template = '{}'
                else:
                    task_template = TaskTemplate(step=step,
                                                 request=step.request,
                                                 parent_id=0,
                                                 name='NotDefined',
                                                 template_type=self.template_type,
                                                 task_template='{}',
                                                 build=self.template_build)
            task_template.task_error = log_msg
            task_template.save()
            self.template_results[step.id] = task_template

    def _define_tasks_for_request(self, request_id, jira_client, restart=False, template_type=None):
        request = TRequest.objects.get(reqid=request_id)
        if request.locked:
            logger.info("Request %d is locked by other thread" % request.reqid)
            return
        if DistributedLock.acquire_lock(f'process_request_{request_id}', 5):
            request.locked = True
            request.save()
            logger.info("Request %d is locked" % request.reqid)
        else:
            logger.info("Request %d is locked by other thread" % request.reqid)
            return
        self.lock_request_time = timezone.now()
        logger.info("Processing production requests")
        logger.info(f"Requests to process: {request_id}")
        self.template_type = template_type
        if self.template_type:
            self.template_results = {}
        keep_approved = False
        requests = [request]
        for request in requests:
            try:
                logger.info("Processing request %d" % request.reqid)
                exception = False
                summary_log = ''
                processed_slices = []
                first_steps = list()
                for input_slice in InputRequestList.objects.filter(request=request).order_by('slice'):
                    steps_in_slice = StepExecution.objects.filter(request=request,
                                                                  status=self.protocol.STEP_STATUS[StepStatus.APPROVED],
                                                                  slice=input_slice).order_by('id')
                    try:
                        steps_in_slice, _ = self._build_linked_step_list(request, input_slice)
                    except Exception as ex:
                        logger.exception("_build_linked_step_list failed: %s" % str(ex))

                    if steps_in_slice:
                        for step in steps_in_slice:
                            if not self.task_reg.get_step_output(step.id, exclude_failed=False) or restart:
                                first_steps.append(step)
                                break
                logger.info("Request = %d, chains: %s" % (request.reqid, str([int(st.id) for st in first_steps])))
                TaskRegistration.register_request_reference(request)
                for step in first_steps:
                    step_parent = StepExecution.objects.get(id=step.step_parent_id)
                    if step_parent.status == self.protocol.STEP_STATUS[StepStatus.WAITING]:
                        continue
                    try:
                        phys_cont_list = list()
                        evgen_input_list = list()
                        ami_hashtag_input_list = list()
                        input_data_name = self.get_step_input_data_name(step)
                        processed_slices.append(step.slice_id)
                        if (self.template_type):
                            self.unset_slice_error(step.request, step.slice)
                        if input_data_name.startswith('ami#'):
                            ami_hashtag_input = input_data_name.split('ami#')[-1]
                            if ami_hashtag_input:
                                logger.info('AMI # "{0}" is used as input'.format(ami_hashtag_input))
                                ami_hashtag_input_list = self.ami_client.list_containers_for_hashtag(
                                    ami_hashtag_input.split(':')[0], ami_hashtag_input.split(':')[-1]
                                )
                                if not ami_hashtag_input_list:
                                    raise Exception('AMI # "{0}" input list is empty'.format(ami_hashtag_input))
                            else:
                                raise Exception('AMI # input "{0}" has wrong format'.format(input_data_name))

                        if input_data_name and not ami_hashtag_input_list:
                            try:
                                input_data_dict = self.parse_data_name(input_data_name)
                            except Exception as e:
                                if step.id != step.step_parent_id:
                                    input_data_dict = None
                                else:
                                    raise e
                            if input_data_dict:
                                force_split_evgen = ProjectMode(step).splitEvgen

                                if str(input_data_dict['number']).lower().startswith('period'.lower()) \
                                        or input_data_dict['prod_step'].lower() == 'PhysCont'.lower():
                                    input_params = self.get_input_params(step, step, None, 0, False)
                                    if not input_params:
                                        raise Exception("No datasets in the period container %s" % input_data_name)
                                    for key in list(input_params.keys()):
                                        if re.match(r'^(--)?input.*File$', key, re.IGNORECASE):
                                            phys_cont_list.extend(input_params[key])
                                elif input_data_dict['prod_step'].lower() == 'py'.lower() and force_split_evgen:
                                    evgen_input_list.extend(self._get_evgen_input_list(step, ProjectMode(step).optimalFirstEvent))
                        if phys_cont_list:
                            for input_dataset in phys_cont_list:
                                try:
                                    self.create_task_chain(step.id, input_dataset=input_dataset)
                                except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                        UnmergedInputProcessedException) as ex:
                                    log_msg = \
                                        'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                                            request.reqid, step.slice.slice, step.id, self.get_step_input_data_name(step),
                                            get_exception_string())
                                    self.set_error(log_msg, request, step, ex, type(ex).__name__, get_exception_string(),jira_client )
                                    exception = True
                                    summary_log += log_msg
                                    continue
                                except Exception as ex:
                                    raise ex
                        elif evgen_input_list:
                            for input_params in evgen_input_list:
                                try:
                                    self.create_task_chain(step.id,
                                                           first_step_number_of_events=input_params['nevents'],
                                                           evgen_params=input_params)
                                except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                        UnmergedInputProcessedException) as ex:
                                    log_msg = \
                                        'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                                            request.reqid, step.slice.slice, step.id, self.get_step_input_data_name(step),
                                            get_exception_string())
                                    self.set_error(log_msg, request, step, ex, type(ex).__name__, get_exception_string(), jira_client )
                                    exception = True
                                    summary_log += log_msg
                                    continue
                                except Exception as ex:
                                    raise ex
                        elif ami_hashtag_input_list:
                            for input_dataset in ami_hashtag_input_list:
                                try:
                                    if int(step.input_events) != -1:
                                        raise Exception(
                                            'Input/total events should be -1 (ALL) if AMI # is used as input')
                                    self.create_task_chain(step.id, input_dataset=input_dataset,
                                                           first_step_number_of_events=-1)
                                except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                        UnmergedInputProcessedException, MergeInverseException) as ex:
                                    log_msg = \
                                        'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                                            request.reqid, step.slice.slice, step.id, self.get_step_input_data_name(step),
                                            get_exception_string())
                                    self.set_error(log_msg, request, step, ex, type(ex).__name__, get_exception_string(), jira_client )
                                    exception = True
                                    summary_log += log_msg
                                    continue
                                except Exception as ex:
                                    raise ex
                        else:
                            use_parent_output = None
                            if step.id != step.step_parent_id:
                                parent_step = StepExecution.objects.get(id=step.step_parent_id)
                                if parent_step.status.lower() == self.protocol.STEP_STATUS[StepStatus.APPROVED].lower():
                                    use_parent_output = True
                                    if parent_step.request.cstatus.lower() == \
                                            self.protocol.REQUEST_STATUS[RequestStatusEnum.APPROVED].lower()\
                                            and parent_step.request != step.request:
                                        keep_approved = True
                                elif parent_step.status.lower() == \
                                        self.protocol.STEP_STATUS[StepStatus.NOTCHECKED].lower():
                                    raise Exception("Parent step is '{0}'".format(parent_step.status))
                            splitting_dict = dict()
                            try:
                                if not use_parent_output:
                                    splitting_dict = self._get_splitting_dict(step)
                                elif type(step.input_events) is int and step.input_events > -1:
                                    parent_step = StepExecution.objects.get(id=step.step_parent_id)
                                    if step.input_events < parent_step.slice.input_events:
                                        raise InputEventsForChildStepException()
                            except NotEnoughEvents:
                                raise Exception(get_exception_string())
                            except UniformDataException as ex:
                                raise ex
                            except InputEventsForChildStepException as ex:
                                raise ex
                            except NoRequestedCampaignInput:
                                raise Exception('No input for specified campaign')
                            except Exception:
                                raise
                            if step.id not in list(splitting_dict.keys()):
                                if use_parent_output:
                                    parent_step = StepExecution.objects.get(id=step.step_parent_id)
                                    for task_id in self.task_reg.get_step_tasks(parent_step.id):
                                        try:
                                            self.create_task_chain(step.id, restart=use_parent_output,
                                                                   first_step_number_of_events=-1,
                                                                   first_parent_task_id=task_id)
                                        except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                                UnmergedInputProcessedException, MergeInverseException) as ex:
                                            log_msg = 'Request = {0}, Chain = {1} ({2}),'.format(
                                                request.reqid, step.slice.slice, step.id)
                                            log_msg += ' input = {0}, exception occurred: {1}'.format(
                                                self.get_step_input_data_name(step), get_exception_string())
                                            self.set_error(log_msg, request, step, ex, type(ex).__name__,
                                                           get_exception_string(), jira_client)
                                            exception = True
                                            summary_log += log_msg
                                            continue
                                        except Exception as ex:
                                            raise ex
                                else:
                                    self.create_task_chain(step.id, restart=use_parent_output)
                            else:
                                for step_input in splitting_dict[step.id]:
                                    try:
                                        self.create_task_chain(step.id, restart=use_parent_output,
                                                               input_dataset=step_input['dataset'],
                                                               first_step_number_of_events=step_input['number_events'],
                                                               primary_input_offset=step_input['offset'],
                                                               container_name=step_input['container'])
                                    except (TaskDuplicateDetected, NoMoreInputFiles, ParentTaskInvalid,
                                            UnmergedInputProcessedException, UniformDataException, MergeInverseException) as ex:
                                        log_msg = 'Request = {0}, Chain = {1} ({2}),'.format(
                                            request.reqid, step.slice.slice, step.id)
                                        log_msg += ' input = {0}, exception occurred: {1}'.format(
                                            self.get_step_input_data_name(step), get_exception_string())
                                        self.set_error(log_msg, request, step, ex, type(ex).__name__,
                                                       get_exception_string(), jira_client)

                                        exception = True
                                        summary_log += log_msg
                                        continue
                                    except Exception as ex:
                                        raise ex
                    except KeyboardInterrupt:
                        pass
                    except Exception as ex:
                        log_msg = 'Request = {0}, Chain = {1} ({2}), input = {3}, exception occurred: {4}'.format(
                            request.reqid, step.slice.slice, step.id, self.get_step_input_data_name(step),
                            get_exception_string())
                        if self.template_type:
                            self.set_error(log_msg, request, step, ex, type(ex).__name__, get_exception_string(), jira_client)
                        else:
                            self.set_slice_error(request,step.slice.id,'default',get_exception_string())
                            logger.exception(log_msg)
                            if request.jira_reference:
                                try:
                                    jira_client.add_issue_comment(request.jira_reference, log_msg)
                                    exception = True
                                    summary_log += log_msg
                                except Exception:
                                    pass
                            continue

                request.cstatus = self._get_request_status(request, summary_log, self.lock_request_time, request.cstatus, keep_approved)
                if not exception:
                    slices_with_error = list(SliceError.objects.filter(request=request, is_active=True))
                    for slice_error in slices_with_error:
                        if not slice_error.slice.is_hide and slice_error.slice_id not in processed_slices:
                            exception = True
                            break
                request.is_error = exception
                request.save()
                logger.info("Request = %d, status = %s" % (request.reqid, request.cstatus))
            finally:
                # unlock request
                request.locked = False
                request.save()
                logger.info("Request %s is unlocked" % request.reqid)

    def force_process_requests(self, request_id, restart=False):
        jira_client = JIRAClient()
        try:
            jira_client.authorize()
        except Exception as ex:
            logger.exception('JIRAClient::authorize failed: {0}'.format(str(ex)))
        self._define_tasks_for_request(request_id, jira_client, restart)

    def test_process_requests(self, request_id, template_type=None):
        jira_client = JIRAClient()
        try:
            jira_client.authorize()
        except Exception as ex:
            logger.exception('JIRAClient::authorize failed: {0}'.format(str(ex)))
        self._define_tasks_for_request(request_id, jira_client, False, template_type)

    def process_requests(self, restart=False, no_wait=False, request_types=None):
        jira_client = JIRAClient()
        try:
            jira_client.authorize()
        except Exception as ex:
            logger.exception('JIRAClient::authorize failed: {0}'.format(str(ex)))
        request_status = self.protocol.REQUEST_STATUS[RequestStatusEnum.APPROVED]

        requests = TRequest.objects.filter(~Q(locked=True),
                                           reqid__gt=800,
                                           cstatus=request_status).order_by('reqid')
        logger.info("Search for %s requests" % request_types)

        if request_types and requests:
            requests = requests.filter(request_type__in=request_types)
        if len(requests) == 0:
            return
        ready_request_list = list()
        for request in requests:
            is_fast = request.is_fast or False
            last_access_timestamp = \
                RequestStatus.objects.filter(request=request, status=request_status).order_by('-id')[0].timestamp
            now = timezone.now()
            time_offset = (now - last_access_timestamp).seconds
            if (time_offset // 3600) < REQUEST_GRACE_PERIOD:
                if (not no_wait) and (not is_fast):
                    logger.info("Request %d is skipped, approved at %s" % (request.reqid, last_access_timestamp))
                    continue
            if request.phys_group != 'VALI' and TRequest.objects.filter(locked=True, phys_group=request.phys_group, request_type=request.request_type).count()>1:
                logger.info("Request %d is skipped, another request is in progress" % request.reqid)
                continue
            ready_request_list.append(request)
        request = ready_request_list[0]
        self._define_tasks_for_request(request.reqid, jira_client, restart)

    def _check_evgen_hepmc(self, trf_cache, trf_release, campaign):
        if (trf_cache+trf_release+campaign) in self._verified_evgen_releases:
            return True
        trf_release_base = '.'.join(trf_release.split('.')[0:2])
        release_path = f'/cvmfs/atlas.cern.ch/repo/sw/software/{trf_release_base}/{trf_cache}/{trf_release}/InstallArea/'
        if not os.path.exists(release_path):
            return False
        dir_name = os.listdir(release_path)[0]
        file_path = os.path.join(release_path, dir_name, 'env_setup.sh')
        hepmc_version_pattern = 'HEPMCVER='
        if campaign.lower() in ['mc16','mc20']:
            expected_versions = ['2' , '3']
        elif  campaign.lower() in ['mc21','mc23']:
            expected_versions = ['3']
        else:
            return False
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                for line in f:
                    if re.search(hepmc_version_pattern, line):
                        version = line.split('HEPMCVER=')[1][0]
                        if version in expected_versions:
                            self._verified_evgen_releases.add(trf_cache+trf_release+campaign)
                            return True
                        else:
                            raise Exception(f'HEPMC version {version} is not expected for {campaign}. Use skipHEPMCCheck to skip this check')

        return False

    def _check_campaign_subcampaign(self, step: StepExecution):
        if '/' in step.request.campaign or '/' in step.request.subcampaign:
            raise Exception('Campaign and subcampaign should not contain "/"')
        return True

    def is_madgraph(self, input_data_name: str) -> bool:
        if 'amcpy' in input_data_name.lower() or 'mgpy' in input_data_name.lower():
            return True
        return False

