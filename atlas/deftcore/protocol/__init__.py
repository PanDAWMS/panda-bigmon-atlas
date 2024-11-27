import logging
import os
import ast
import json
import re
from django.template import Context, Template
from django.template.defaultfilters import stringfilter
from django import template
from enum import Enum, auto
logger = logging.getLogger('deftcore')

register = template.Library()


@register.filter(is_safe=True)
@stringfilter
def json_str(value):
    return value.replace('\\', '\\\\').replace('"', '\\"')


class Constant(object):
    def __init__(self, format_or_value, format_arg_names=None):
        self.format_or_value = format_or_value
        self.format_arg_names = format_arg_names or tuple()

    def is_dynamic(self):
        return self.format_arg_names is not tuple()


class Constants(type):
    def __new__(mcs, name, bases, namespace):
        attributes = dict()

        for attr_name in list(namespace.keys()):
            attr = namespace[attr_name]
            if isinstance(attr, Constant):
                if isinstance(attr.format_or_value, str) or isinstance(attr.format_or_value, str):
                    args = list()
                    for arg_name in attr.format_arg_names:
                        arg_attr = namespace[arg_name]
                        if arg_attr.is_dynamic():
                            raise Exception('{0} class definition is incorrect'.format(name))
                        args.append(arg_attr.format_or_value)
                    attr_value = attr.format_or_value % tuple(args)
                else:
                    attr_value = attr.format_or_value
                # FIXME: implement support for protocol fixes
                attributes[attr_name] = attr_value

        for attr_name in list(attributes.keys()):
            namespace[attr_name] = attributes[attr_name]

        cls = super(Constants, mcs).__new__(mcs, name, bases, namespace)

        return cls

    def __setattr__(cls, name, value):
        raise TypeError('Constant {0} cannot be updated'.format(name))


class TaskParamName(Enum):
    CONSTANT = auto()
    SKIP_EVENTS = auto()
    MAX_EVENTS = auto()
    RANDOM_SEED = auto()
    RANDOM_SEED_MC = auto()
    FIRST_EVENT = auto()
    SPECIAL_FIRST_EVENT = auto()
    DB_RELEASE = auto()
    INPUT = auto()
    INPUT_DIRECT_IO = auto()
    INPUT_WITH_DATASET = auto()
    OUTPUT = auto()
    ORDERED_OUTPUT = auto()
    RAW_OUTPUT = auto()
    TXT_OUTPUT = auto()
    SECONDARY_INPUT_MINBIAS = auto()
    SECONDARY_INPUT_CAVERN = auto()
    SECONDARY_INPUT_ZERO_BIAS_BS = auto()
    SECONDARY_INPUT_ZERO_BIAS_BS_RND = auto()
    LOG = auto()
    ORDERED_LOG = auto()
    JOB_NUMBER = auto()
    FILTER_FILE = auto()
    TRAIN_DAOD_FILE = auto()
    TRAIN_DAOD_FILE_JEDI_MERGE = auto()
    TRAIN_OUTPUT = auto()
    TXT_EVENTID_OUTPUT = auto()
    TAR_CONFIG_OUTPUT = auto()
    ZIP_OUTPUT = auto()
    YODA_OUTPUT = auto()
    ZIP_MAP = auto()
    OVERLAY_FILTER_FILE = auto()
    HITAR_FILE = auto()


class TaskStatus(Enum):
    TESTING = auto()
    WAITING = auto()
    FAILED = auto()
    BROKEN = auto()
    OBSOLETE = auto()
    ABORTED = auto()
    TOABORT = auto()
    RUNNING = auto()
    FINISHED = auto()
    DONE = auto()
    TORETRY = auto()


class StepStatus(Enum):
    APPROVED = auto(),
    NOTCHECKED = auto(),
    WAITING = auto()


class RequestStatus(Enum):
    APPROVED = auto(),
    PROCESSED = auto()
    WORKING = auto()


# noinspection PyBroadException, PyUnresolvedReferences
class Protocol(object):
    VERSION = '2.0'

    TASK_PARAM_TEMPLATES = {
        TaskParamName.CONSTANT: """{
            "type": "constant",
            "value": "{{name}}{{separator}}{{value|json_str}}"
        }""",
        TaskParamName.SKIP_EVENTS: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${SKIPEVENTS}"
        }""",
        TaskParamName.MAX_EVENTS: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${MAXEVENTS}"
        }""",
        TaskParamName.RANDOM_SEED: """{
            "offset": {{offset}},
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${RNDMSEED}"
        }""",
        TaskParamName.RANDOM_SEED_MC: """{
            "offset": {{offset}},
            "param_type": "pseudo_input",
            "type": "template",
            "dataset": "seq_number",
            "value": "{{name}}{{separator}}${SEQNUMBER}"
        }""",
        TaskParamName.FIRST_EVENT: """{
            "offset": {{offset}},
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${FIRSTEVENT}"
        }""",
        TaskParamName.SPECIAL_FIRST_EVENT: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${SEQNUMBER/M[({{nEventsPerJob}}*(#-1))+1]}"
        }""",
        TaskParamName.DB_RELEASE: """{
            "dataset": "{{dataset}}",
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=${DBR}"
        }""",
        TaskParamName.INPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=${IN{{postfix}}/L}"
        }""",
        TaskParamName.INPUT_WITH_DATASET: """{
        "dataset": "{{dataset}}",
        "offset": 0,
        "param_type": "input",
        "type": "template",
        "value": "{{name}}={{dataset}}#${IN{{postfix}}/L}"
        }""",
        TaskParamName.INPUT_DIRECT_IO: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=@${IN{{postfix}}/F}"
        }""",
        TaskParamName.OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.pool.root"
        }""",
        TaskParamName.ORDERED_OUTPUT: """{
        "dataset": "{{dataset}}",
        "offset": 0,
        "param_type": "output",
        "token": "ATLASDATADISK",
        "type": "template",
        "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}${MIDDLENAME}._${SN}.pool.root"
        }""",
        TaskParamName.RAW_OUTPUT: """{
        "dataset": "{{dataset}}",
        "offset": 0,
        "param_type": "output",
        "token": "ATLASDATADISK",
        "type": "template",
        "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}"
        }""",
        TaskParamName.TXT_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.tar.gz"
        }""",
        # FIXME: OverlayTest
        TaskParamName.TXT_EVENTID_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}=events.{{task_id|stringformat:\".08d\"}}._${SN}.txt"
        }""",
        TaskParamName.TAR_CONFIG_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.tar.gz"
        }""",
        TaskParamName.ZIP_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.zip"
        }""",
        TaskParamName.YODA_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.yoda.gz"
        }""",
        TaskParamName.ZIP_MAP: """{
            "type": "constant",
            "value": "<ZIP_MAP>${OUTPUT{{idx}}}:${IN_DATA/L}</ZIP_MAP>"
        }""",
        # FIXME: OverlayTest
        TaskParamName.OVERLAY_FILTER_FILE: """{
            "type": "constant",
            "value": "{{name}}{{separator}}events.{{task_id|stringformat:\".08d\"}}._${SN}.txt"
        }""",
        TaskParamName.SECONDARY_INPUT_MINBIAS: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "ratio": 0,
            "eventRatio": {{event_ratio|default:'"None"'}},
            "type": "template",
            "value": "{{name}}=${IN_MINBIAS{{postfix}}/L}"
        }""",
        TaskParamName.SECONDARY_INPUT_CAVERN: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "ratio": 0,
            "eventRatio": {{event_ratio|default:'"None"'}},
            "type": "template",
            "value": "{{name}}=${IN_CAVERN/L}"
        }""",
        TaskParamName.SECONDARY_INPUT_ZERO_BIAS_BS: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "ratio": 0,
            "eventRatio": {{event_ratio|default:'"None"'}},
            "type": "template",
            "value": "{{name}}=${IN_ZERO_BIAS_BS/L}"
        }""",
        TaskParamName.SECONDARY_INPUT_ZERO_BIAS_BS_RND: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "input",
            "ratio": 0,
            "random": "True",
            "eventRatio": {{event_ratio|default:'"None"'}},
            "type": "template",
            "value": "{{name}}=${IN_ZERO_BIAS_BS/L}"
        }""",
        TaskParamName.LOG: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "log",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "log.{{task_id|stringformat:\".08d\"}}._${SN}.job.log.tgz"
        }""",
        TaskParamName.ORDERED_LOG: """{
        "dataset": "{{dataset}}",
        "offset": 0,
        "param_type": "log",
        "token": "ATLASDATADISK",
        "type": "template",
        "value": "log.{{task_id|stringformat:\".08d\"}}${MIDDLENAME}._${SN}.job.log.tgz"
    }""",
        TaskParamName.JOB_NUMBER: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}${SN}"
        }""",
        TaskParamName.FILTER_FILE: """{
            "dataset": "{{dataset}}",
            "attributes": "repeat,nosplit",
            "param_type": "input",
            "ratio": {{ratio}},
            "type": "template",
            "files": {{files}},
            "value": "{{name}}=${IN_FILTER_FILE/L}"
        }""",
        TaskParamName.HITAR_FILE: """{
            "dataset": "{{dataset}}",
            "attributes": "repeat,nosplit",
            "param_type": "input",
            "type": "template",
            "value": "{{name}}=${IN_HITAR/L}"
        }""",
        TaskParamName.TRAIN_DAOD_FILE: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}{{task_id|stringformat:\".08d\"}}._${SN/P}.pool.root.1"
        }""",
        TaskParamName.TRAIN_DAOD_FILE_JEDI_MERGE: """{
            "param_type": "number",
            "type": "template",
            "value": "{{name}}{{separator}}{{task_id|stringformat:\".08d\"}}._${SN/P}.pool.root.1.panda.um"
        }""",
        TaskParamName.TRAIN_OUTPUT: """{
            "dataset": "{{dataset}}",
            "offset": 0,
            "param_type": "output",
            "token": "ATLASDATADISK",
            "type": "template",
            "value": "{{name}}={{data_type}}.{{task_id|stringformat:\".08d\"}}._${SN}.pool.root.1"
        }"""
    }

    TASK_STATUS = {
        TaskStatus.TESTING: 'testing',
        TaskStatus.WAITING: 'waiting',
        TaskStatus.FAILED: 'failed',
        TaskStatus.BROKEN: 'broken',
        TaskStatus.OBSOLETE: 'obsolete',
        TaskStatus.ABORTED: 'aborted',
        TaskStatus.TOABORT: 'toabort',
        TaskStatus.RUNNING: 'running',
        TaskStatus.FINISHED: 'finished',
        TaskStatus.DONE: 'done',
        TaskStatus.TORETRY: 'toretry'
    }

    STEP_STATUS = {
        StepStatus.APPROVED: 'Approved',
        StepStatus.NOTCHECKED: 'NotChecked',
        StepStatus.WAITING: 'Waiting'
    }

    REQUEST_STATUS = {
        RequestStatus.APPROVED: 'approved',
        RequestStatus.PROCESSED: 'processed',
        RequestStatus.WORKING: 'working'
    }

    TRF_OPTIONS = {
        r'^.*_tf.py$': {'separator': '='}
    }

    def render_param(self, proto_key, param_dict):
        for key in list(param_dict.keys()):
            if not isinstance(param_dict[key], (str, int)):
                try:
                    param_dict[key] = json.dumps(param_dict[key])
                except Exception:
                    param_dict[key] = param_dict[key]
        default_param_dict = {'separator': '='}
        default_param_dict.update(param_dict)
        t = Template(self.TASK_PARAM_TEMPLATES[proto_key])
        param = json.loads(t.render(Context(default_param_dict, autoescape=False)))
        for key in list(param.keys())[:]:
            if param[key] == 'None':
                param.pop(key, None)
        return param

    @staticmethod
    def render_task(task_dict):
        path = '{0}{1}task.json'.format(os.path.dirname(__file__), os.path.sep)
        with open(path, 'r') as fp:
            task_template = Template(fp.read())
        proto_task = json.loads(task_template.render(Context(task_dict, autoescape=False)))
        task = {}
        for key in list(proto_task.keys()):
            if proto_task[key] == "" or proto_task[key] == "''" or proto_task[key].lower() == 'None'.lower() or \
                    proto_task[key].lower() == '\'None\''.lower():
                continue
            task[key] = ast.literal_eval(proto_task[key])
        return task

    @staticmethod
    def is_dynamic_jobdef_enabled(task):
        keys = [k.lower() for k in list(task.keys())]
        if 'nEventsPerJob'.lower() in keys or 'nFilesPerJob'.lower() in keys:
            return False
        else:
            return True

    @staticmethod
    def is_pileup_task(task):
        job_params = task['jobParameters']
        for job_param in job_params:
            if re.match(r'^.*(PtMinbias|Cavern).*File.*$', str(job_param['value']), re.IGNORECASE):
                return True
        return False

    @staticmethod
    def get_simulation_type(step):
        if step.request.request_type.lower() == 'MC'.lower():
            if step.step_template.step.lower() == 'evgen'.lower():
                return 'notMC'
            if str(step.step_template.ctag).lower().startswith('a'):
                return 'fast'
            else:
                return 'full'
        return 'notMC'

    @staticmethod
    def get_primary_input(task):
        job_params = task['jobParameters']
        for job_param in job_params:
            if 'param_type' not in list(job_param.keys()) or job_param['param_type'].lower() != 'input'.lower():
                continue
            if re.match(r'^(--)?input.*File', job_param['value'], re.IGNORECASE):
                result = re.match(r'^(--)?input(?P<intype>.*)File', job_param['value'], re.IGNORECASE)
                if not result:
                    continue
                in_type = result.groupdict()['intype']
                if in_type.lower() == 'logs'.lower() or re.match(r'^.*(PtMinbias|Cavern).*$', in_type, re.IGNORECASE):
                    continue
                return job_param
        return None

    @staticmethod
    def set_leave_log_param(log_param):
        log_param['token'] = TaskDefConstants.LEAVE_LOG_TOKEN
        log_param['destination'] = TaskDefConstants.LEAVE_LOG_DESTINATION
        log_param['transient'] = TaskDefConstants.LEAVE_LOG_TRANSIENT_FLAG

    @staticmethod
    def is_leave_log_param(log_param):
        token = None
        destination = None
        if 'token' in list(log_param.keys()):
            token = log_param['token']
        if 'destination' in list(log_param.keys()):
            destination = log_param['destination']
        if token == TaskDefConstants.LEAVE_LOG_TOKEN and destination == TaskDefConstants.LEAVE_LOG_DESTINATION:
            return True
        else:
            return False

    @staticmethod
    def is_evnt_filter_step(project_mode, task_config):
        return project_mode.evntFilterEff or 'evntFilterEff' in list(task_config.keys())

    @staticmethod
    def serialize_task(task):
        return json.dumps(task, sort_keys=True)

    @staticmethod
    def deserialize_task(task_string):
        return json.loads(task_string)


class TaskDefConstants(object, metaclass=Constants):
    DEFAULT_PROD_SOURCE = Constant('managed')
    DEFAULT_DEBUG_PROJECT_NAME = Constant('mc12_valid')
    DEFAULT_PROJECT_MODE = Constant({'cmtconfig': 'i686-slc5-gcc43-opt', 'spacetoken': 'ATLASDATADISK'})

    # Primary Real Datasets: project.runNumber.streamName.prodStep.dataType.Version
    # Physics Containers: project.period.superdatasetName.dataType.Version
    # Monte Carlo Datasets: project.datasetNumber.physicsShort.prodStep.dataType.Version
    DEFAULT_DATA_NAME_PATTERN = Constant(r'^(.+:)?(?P<project>\w+)\.' +
                                         r'(?P<number>(\d+|\w+))\.' +
                                         r'(?P<brief>\w+)\.' +
                                         r'(?P<prod_step>\w+)\.*' +
                                         r'(?P<data_type>\w*)\.*' +
                                         r'(?P<version>\w*)' +
                                         r'(?P<container>/|$)')

    DEFAULT_EVGEN_JO_SVN_PATH_TEMPLATE = Constant(
        'svn+ssh://svn.cern.ch/reps/atlasoff/Generators/{{campaign}}JobOptions/trunk/')
    DEFAULT_EVGEN_JO_PATH_TEMPLATE = Constant('/cvmfs/atlas.cern.ch/repo/sw/Generators/{{campaign}}JobOptions/latest/')
    DEFAULT_NEW_EVGEN_JO_PATH = '/cvmfs/atlas.cern.ch/repo/sw/Generators/MCJobOptions/'
    DEFAULT_GRL_XML_PATH = '/cvmfs/atlas.cern.ch/repo/sw/database/GroupData/GoodRunsLists/{project}/'
    DEAFULT_SW_RELEASE_PATH = '/cvmfs/atlas.cern.ch/repo/sw/software/{release_base}/{project}/{release}/InstallArea/'
    DEFAULT_TASK_ID_FORMAT_BASE = Constant('.08d')
    DEFAULT_TASK_ID_FORMAT = Constant('%%%s', ('DEFAULT_TASK_ID_FORMAT_BASE',))
    DEFAULT_TASK_NAME_TEMPLATE = Constant('{{project}}.{{number}}.{{brief}}.{{prod_step}}.{{version}}')
    DEFAULT_TASK_OUTPUT_NAME_TEMPLATE = Constant('{{project}}.' +
                                                 '{{number}}.' +
                                                 '{{brief}}.' +
                                                 '{{prod_step}}.' +
                                                 '{{data_type}}.' +
                                                 '{{version}}_tid{{task_id|stringformat:\"%s\"}}_00',
                                                 ('DEFAULT_TASK_ID_FORMAT_BASE',))
    DEFAULT_OUTPUT_NAME_MAX_LENGTH = Constant(255)
    DEFAULT_DB_RELEASE_DATASET_NAME_BASE = Constant('ddo.000001.Atlas.Ideal.DBRelease.v')
    DEFAULT_MINIBIAS_NPILEUP = Constant(5)
    DEFAULT_MAX_ATTEMPT = Constant(5)

    INVALID_TASK_ID = Constant(4000000)
    DEFAULT_MAX_FILES_PER_JOB = Constant(20)
    DEFAULT_MAX_NUMBER_OF_JOBS_PER_TASK = Constant(50000)
    DEFAULT_MEMORY = Constant(2000)
    DEFAULT_MAX_EVENTS_PER_GRANULE_JOB = Constant(10000)
    DEFAULT_MEMORY_BASE = Constant(0)
    DEFAULT_SCOUT_SUCCESS_RATE = Constant(5)
    NO_ES_MIN_NUMBER_OF_EVENTS = Constant(50000)
    DEFAULT_MAX_EVENTS_EVGEN_TASK = Constant(50000000)

    LEAVE_LOG_TOKEN = Constant('ddd:.*DATADISK')
    LEAVE_LOG_DESTINATION = Constant('(type=DATADISK)\\(dontkeeplog=True)')
    LEAVE_LOG_TRANSIENT_FLAG = Constant(False)

    DEFAULT_CLOUD = Constant('WORLD')

    DEFAULT_ALLOWED_INPUT_EVENTS_DIFFERENCE = Constant(10)

    DEFAULT_ES_MAX_ATTEMPT = Constant(10)
    DEFAULT_ES_MAX_ATTEMPT_JOB = Constant(10)

    DEFAULT_SC_HASHTAG_SUFFIX = Constant('_sc_102017_mixed_cont')

    DDM_ERASE_EVENT_TYPE = Constant('ERASE')
    DDM_ERASE_STATUS = Constant('erase')
    DDM_LOST_EVENT_TYPE = Constant('LOST')
    DDM_LOST_STATUS = Constant('lost')
    DDM_STAGING_STATUS = Constant('staging')
    DDM_PROGRESS_EVENT_TYPE = Constant('RULE_PROGRESS')
    DATASET_DELETED_STATUS = Constant('Deleted')
    DATASET_TO_BE_DELETED_STATUS = Constant('toBeDeleted')
    MC_DELETED_REPROCESSING_REQUEST_HASHTAG = Constant('MCDeletedReprocessing')

    DEFAULT_TASK_COMMON_OFFSET_HASHTAG_FORMAT = Constant('_tco_{0}')

    DEFAULT_SC_HASHTAGS = {
        'MC16a': ['MC16:MC16a', 'MC15:MC15.*', '.*MC15.*'],
        'MC16b': ['MC16:MC16b'],
        'MC16c': ['MC16:MC16c'],
        'MC16d': ['MC16:MC16d'],
        'MC16e': ['MC16:MC16e'],
        'MC23a': ['MC23:MC23a'],
        'MC23b': ['MC23:MC23b'],
        'MC23c': ['MC23:MC23c'],
        'MC23d': ['MC23:MC23d'],
        'MC23e': ['MC23:MC23e'],

    }

    CAMPAIGNS_INTERCHANGEABLE = {
        'MC16a': ['MC16a'],
        'MC16b': ['MC16b'],
        'MC16c': ['MC16c'],
        'MC16d': ['MC16d', 'MC16c'],
        'MC16e': ['MC16e'],
        'MC23a': ['MC23a'],
        'MC23b': ['MC23b'],
        'MC23c': ['MC23c'],
        'MC23d': ['MC23d', 'MC23c'],
        'MC23e': ['MC23e'],
    }
    DEFAULT_KILL_JOB_CODE = Constant(9)

    ERRORS_TO_REPEAT = ['http status code: 503',
                        'Connection broken: IncompleteRead',
                        '[Errno 111] Connection refused',
                        'QueuePool limit of size 5']
    TRANSIENT_ERROR_ATTEMPTS = 1
    REPEAT_ATTEMPT_MESSAGE = 'Repeated because of a transient error'
    TEMPLATE_TASK_ID = 28948171
    WORKFLOW_VALIDATION_REQUESTS = [43321, 43400]

    FULL_CHAIN = {'GOOGLE': {
        'project_mode': 'GoogleFullChain',
        'hashtag': 'GoogleFullChainTask',
        'request_hashtag_base': 'GoogleFullChainRequest',
        'site': 'GOOGLE_EUW1',
        'nucleus': 'GOOGLE',
        'token': 'dst:GOOGLE_EU'
    }}

    JEDI_FULL_CHAIN = 'JEDIFullChain'

    REPRO_PATCH_HASHTAG = 'ReproPatch'