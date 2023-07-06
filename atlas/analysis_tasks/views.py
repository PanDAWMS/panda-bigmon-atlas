import json
from copy import deepcopy

from django.http import HttpResponseBadRequest
from django.utils import timezone
from rest_framework.response import Response

from jinja2.nativetypes import NativeEnvironment
from rest_framework import status
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated

from atlas.prodtask.models import AnalysisTaskTemplate, TTask, TemplateVariable, InputRequestList, AnalysisStepTemplate, \
    ProductionTask, StepExecution, TRequest, SliceSerializer
from atlas.prodtask.spdstodb import fill_template
from atlas.prodtask.task_views import tasks_serialisation
from rest_framework import serializers


def find_output_base(task_params: dict) -> str:
    output_base = ''
    if 'outDS' in task_params['cliParams']:
        output_base = [x for x in task_params['cliParams'].split('outDS')[1].replace('=', ' ').split(' ') if x][0]
    return output_base


def replace_template_by_variable(task_params: dict, variable: TemplateVariable, root: [str] = None) -> dict:
    if root is None:
        root = []
    for key, value in task_params.items():
        if type(value) == dict:
            task_params[key] = replace_template_by_variable(value, variable, root+[key])
        elif type(value) == str:
            if variable.value in value:
                task_params[key] = value.replace(variable.value, TemplateVariable.get_key_template(variable.name))
                variable.keys.append(TemplateVariable.KEYS_SEPARATOR.join(root+[key]))
        elif type(value) == list:
            for i in range(len(value)):
                if type(value[i]) == str:
                    if variable.value in value[i]:
                        value[i] = value[i].replace(variable.value, TemplateVariable.get_key_template(variable.name))
                        variable.keys.append(TemplateVariable.KEYS_SEPARATOR.join(root+[key, str(i)]))
                elif type(value[i]) == dict:
                    value[i] = replace_template_by_variable(value[i], variable, root+[key+TemplateVariable.KEYS_SEPARATOR+str(i)])
    return task_params


def get_task_params(task_id: int, task_params: dict = None) -> [dict, [TemplateVariable]]:
    task = TTask.objects.get(id=task_id)
    template_variables = []
    if task_params is None:
        task_params = task.jedi_task_parameters
    output_base = find_output_base(task_params)
    task_name = task.name.rstrip('/')
    task_params['taskName'] = task_params['taskName'].replace(task_name, TemplateVariable.get_key_template(TemplateVariable.KEY_NAMES.TASK_NAME))
    template_variables.append(TemplateVariable(TemplateVariable.KEY_NAMES.TASK_NAME, task_name, ['taskName']))
    task_params['userName'] = task_params['userName'].replace(task.username, TemplateVariable.get_key_template(TemplateVariable.KEY_NAMES.USER_NAME))
    template_variables.append(TemplateVariable(TemplateVariable.KEY_NAMES.USER_NAME, task.username, ['userName']))
    task_input = task_params.get('dsForIN')
    output_variable = TemplateVariable(TemplateVariable.KEY_NAMES.OUTPUT_BASE, output_base)
    task_params = replace_template_by_variable(task_params, output_variable, [])
    template_variables.append(output_variable)
    input_variable = TemplateVariable(TemplateVariable.KEY_NAMES.INPUT_BASE, task_input)
    task_params = replace_template_by_variable(task_params, input_variable, [])
    template_variables.append(input_variable)
    template_variables.append(TemplateVariable(TemplateVariable.KEY_NAMES.TASK_PRIORITY, task_params.get('taskPriority',1000),
                                               ['taskPriority'], TemplateVariable.VariableType.INTEGER))
    if 'parent_tid' in task_params:
        template_variables.append(
            TemplateVariable(TemplateVariable.KEY_NAMES.PARENT_ID, task_params['parent_tid'],
                             ['parent_tid'], TemplateVariable.VariableType.INTEGER))
    return task_params, template_variables

def create_pattern_from_task(task_id: int, pattern_description: str, task_params: dict = None) -> AnalysisTaskTemplate:
    task_template, template_variables = get_task_params(task_id, TTask.objects.get(id=task_id).jedi_task_parameters)
    new_pattern = AnalysisTaskTemplate()
    new_pattern.description = pattern_description
    new_pattern.task_parameters = task_params
    new_pattern.software_release = task_params.get('transHome','')
    new_pattern.physics_group = task_params.get('workingGroup','VALI')
    new_pattern.variables_data = template_variables
    new_pattern.build_task = task_id
    new_pattern.save()
    return new_pattern


def create_task_from_pattern(pattern_id: int, task_name: str, task_params: dict) -> TTask:
    pattern = AnalysisTaskTemplate.objects.get(id=pattern_id)
    new_task = TTask()
    return new_task


def create_analy_task_for_slice(requestID: int, slice: int ) -> [int]:
    new_tasks = []
    steps = AnalysisStepTemplate.objects.filter(request=requestID, slice=InputRequestList.objects.get(slice=slice, request=requestID))
    for step in steps:
        if (step.status == AnalysisStepTemplate.STATUS.APPROVED) and (not ProductionTask.objects.filter(step=step.step_production_parent).exists()):
            task_id = TTask().get_id()
            t_task, prod_task = register_analysis_task(step, task_id, task_id)
            t_task.save()
            prod_task.save()
            new_tasks.append(task_id)
    return new_tasks

def monk_create_analy_task_for_slice(requestID: int, slice: int ):
    new_tasks = []
    steps = AnalysisStepTemplate.objects.filter(request=requestID, slice=InputRequestList.objects.get(slice=slice, request=requestID))
    for step in steps:
        if (step.status == AnalysisStepTemplate.STATUS.APPROVED) and (not ProductionTask.objects.filter(step=step.step_production_parent).exists()):
            task_id = TTask().get_id()
            t_task, prod_task = register_analysis_task(step, task_id, task_id)
            return t_task, prod_task


def render_task_template(task_template: dict, variables: [TemplateVariable]) -> dict:
    render_template = deepcopy(task_template)
    key_values = {}
    for variable in variables:
        for key_chain in variable.keys:
            if key_chain not in key_values:
                key_values[key_chain] = {}
            key_values[key_chain].update({variable.name: variable.value})
    rendered_keys = []
    for variable in variables:
        if variable.type == TemplateVariable.VariableType.TEMPLATE:
            for key_chain in variable.keys:
                if key_chain not in rendered_keys:
                    current_node = render_template
                    current_key= ''
                    leaf_parent = None
                    for key in key_chain.split(TemplateVariable.KEYS_SEPARATOR):
                        leaf_parent = current_node
                        if key.isdigit():
                            current_key = int(key)
                        else:
                            current_key = key
                        current_node = current_node[current_key]
                    jinja_env = NativeEnvironment()
                    current_template = jinja_env.from_string(current_node)
                    rendered_template = current_template.render(key_values[key_chain])
                    leaf_parent[current_key] = rendered_template
                    rendered_keys.append(key_chain)

    return render_template


def get_new_name(dataset: str, old_name: str, tag: str):
    prefix = '.'.join(old_name.split('.')[:2])
    return prefix + '.' + dataset + '_' + tag


def register_analysis_task(step_template: AnalysisStepTemplate, task_id: int, parent_tid: int) -> [TTask, ProductionTask]:
    step_template.step_parameters = deepcopy(render_task_template(step_template.step_parameters, step_template.variables_data))
    task = TTask(id=task_id,
                          parent_tid=parent_tid,
                          status=ProductionTask.STATUS.WAITING,
                          total_done_jobs=0,
                          submit_time=timezone.now(),
                          vo=step_template.vo,
                          prodSourceLabel=step_template.prodSourceLabel,
                          name=step_template.step_parameters[step_template.get_variable_key(TemplateVariable.KEY_NAMES.TASK_NAME)],
                          username=step_template.step_parameters[step_template.get_variable_key(TemplateVariable.KEY_NAMES.USER_NAME)],
                          chain_id=parent_tid,
                          _jedi_task_parameters=json.dumps(step_template.step_parameters))

    prod_task = ProductionTask(id=task.id,
                               step=step_template.step_production_parent,
                               request=step_template.request,
                               parent_id=parent_tid,
                               name=task.name,
                               project=step_template.project,
                               phys_group='',
                               provenance='',
                               status=ProductionTask.STATUS.WAITING,
                               total_events=0,
                               total_req_jobs=0,
                               total_done_jobs=0,
                               submit_time=timezone.now(),
                               bug_report=0,
                               priority=task.priority,
                               inputdataset=step_template.input_dataset,
                               timestamp=timezone.now(),
                               vo=step_template.vo,
                               prodSourceLabel=step_template.prodSourceLabel,
                               username=task.username,
                               chain_tid=task.id,
                               #dynamic_jobdef=None,
                               campaign='',
                               subcampaign='',
                               bunchspacing='',
                               total_req_events=-1,
                               #pileup=None,
                               simulation_type='notMC',
                               #is_extension=None,
                               #ttcr_timestamp=None,
                               primary_input=step_template.input_dataset,
                               ami_tag=step_template.task_template.tag,
                               output_formats='')
    return task, prod_task


def create_step_from_template(slice: InputRequestList, template: AnalysisTaskTemplate) -> AnalysisStepTemplate:
    prod_step = StepExecution()
    prod_step.step_template = fill_template(AnalysisStepTemplate.ANALYSIS_STEP_NAME.GROUP_ANALYSIS, template.tag, slice.priority)
    prod_step.request = slice.request
    prod_step.slice = slice
    prod_step.status = StepExecution.STATUS.NOT_CHECKED
    prod_step.priority = slice.priority
    prod_step.step_def_time = timezone.now()
    prod_step.input_events = -1
    prod_step.save()
    step = AnalysisStepTemplate()
    step.name = AnalysisStepTemplate.ANALYSIS_STEP_NAME.GROUP_ANALYSIS
    step.status = AnalysisStepTemplate.STATUS.NOT_CHECKED
    step.task_template = template
    step.step_parameters = template.task_parameters
    step.slice = slice
    step.request = slice.request
    step.variables_data = deepcopy(template.variables_data)
    step.change_variable(TemplateVariable.KEY_NAMES.INPUT_BASE, slice.dataset)
    new_name = get_new_name(slice.dataset, step.get_variable(TemplateVariable.KEY_NAMES.TASK_NAME), template.tag )
    step.change_variable(TemplateVariable.KEY_NAMES.OUTPUT_BASE, new_name)
    step.change_variable(TemplateVariable.KEY_NAMES.TASK_NAME, new_name)
    #step.change_variable(TemplateVariable.KEY_NAMES.USER_NAME, slice.request.manager)
    step.step_parameters[step.get_variable_key(TemplateVariable.KEY_NAMES.TASK_NAME)] = f'{new_name}/'
    #step.step_parameters[step.get_variable_key(TemplateVariable.KEY_NAMES.USER_NAME)] = slice.request.manager
    step.template = template
    step.step_production_parent = prod_step
    step.save()
    prod_step.analysis_step_id = step.id
    prod_step.save()
    return step


def add_analysis_slices_to_request(production_request: TRequest, template: AnalysisTaskTemplate, containers: [str]) -> [int]:
    slices = []
    for container in containers:
        slice = InputRequestList()
        new_slice_number = InputRequestList.objects.filter(request=production_request).count()
        slice.request = production_request
        slice.dataset = container
        slice.priority = template.get_variable(TemplateVariable.KEY_NAMES.TASK_PRIORITY)
        slice.brief = 'Analysis slice'
        slice.slice = new_slice_number
        slice.save()
        step = create_step_from_template(slice, template)
        slices.append(new_slice_number)
    return slices

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def prepare_template_from_task(request):
    try:
        task_id = int(request.query_params.get('task_id'))
        task_template, _ = get_task_params(task_id, TTask.objects.get(id=task_id).jedi_task_parameters)
        return Response(task_template)
    except TTask.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def create_template(request):
    try:
        new_pattern = create_pattern_from_task(int(request.data['taskID']), request.data['description'],
                                               request.data['taskTemplate'])
        return Response(new_pattern.tag,status=status.HTTP_201_CREATED)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class AnalysisTaskTemplateSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalysisTaskTemplate
        fields = '__all__'

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_template(request):
    try:
        template_tag = request.query_params.get('template_tag')
        template = AnalysisTaskTemplate.objects.filter(tag=template_tag).last()
        return Response(AnalysisTaskTemplateSerializer(template).data)
    except AnalysisTaskTemplate.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def analysis_steps_serializer(analysis_steps: [AnalysisStepTemplate]):
    serialized_steps = []
    for step in analysis_steps:
        production_step = step.step_production_parent
        serialized_production_step = {'id':production_step.id,'ami_tag':production_step.step_template.ctag,'status':production_step.status,'production_step':production_step.step_template.step,
                             'production_step_parent':production_step.step_parent_id, 'request':production_step.request_id,'task_config':production_step.get_task_config(),
                             'priority':production_step.priority,'input_events':production_step.input_events, 'project_mode': production_step.get_task_config('project_mode')
                                      }
        tasks = ProductionTask.objects.filter(step=step.step_production_parent, request=step.request)
        serialized_tasks = []
        if tasks:
            serialized_tasks = tasks_serialisation(tasks)
        serialized_step = {
            'id': step.id,
            'name': step.name,
            'status': step.status,
            'step_parameters': step.step_parameters,
            'slice_id': step.slice.id,
            'request_id': step.request.reqid,
            'step_production_parent_id': step.step_production_parent.id,
            'step_analysis_parent_id': step.id,
            'template_name': step.task_template.tag,
        }

        serialized_steps.append({'analysis_step': serialized_step, 'step': serialized_production_step, 'tasks': serialized_tasks})
    return serialized_steps


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_all_patterns(request):
    try:
        if request.query_params.get('status') == 'all':
            patterns = AnalysisTaskTemplate.objects.all()
        else:
            patterns = AnalysisTaskTemplate.objects.filter(status=AnalysisTaskTemplate.STATUS.ACTIVE)
        serialized_patterns = []
        for pattern in patterns:
            serialized_patterns.append(AnalysisTaskTemplateSerializer(pattern).data)
        return Response(serialized_patterns)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def create_analysis_request(request):
    try:
        new_request = TRequest.objects.get(reqid=48149)
        new_request.reqid = None
        new_request.description = request.data['description']
        new_request.save()
        analysis_template_base = request.data['templateBase']
        analysis_template = AnalysisTaskTemplate.objects.get(id=analysis_template_base['id'])
        analysis_template.task_parameters = analysis_template_base['task_parameters']
        add_analysis_slices_to_request(TRequest.objects.get(reqid=new_request.reqid),analysis_template , request.data['inputContainers'])
        return Response(str(new_request.reqid))

    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_analysis_request(request):
    try:
        request_id = int(request.query_params.get('request_id'))
        request = TRequest.objects.get(reqid=request_id)
        slices = InputRequestList.objects.filter(request=request)
        serialized_slices_with_steps = []
        for slice in slices:
            if slice.is_hide is None or slice.is_hide == False:
                analysis_steps = list(AnalysisStepTemplate.objects.filter(request=request, slice=slice).order_by('id'))
                serialized_steps = analysis_steps_serializer(analysis_steps)
                serialized_slices_with_steps.append({'slice': SliceSerializer(slice).data, 'steps': serialized_steps})
        return Response(serialized_slices_with_steps)

    except TRequest.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def clone_analysis_request_slices(request):
    slices = request.data['slices']
    for slice in slices:
        simple_analy_slice_clone(request.data['requestID'], slice)
    return Response({'result':f"{len(slices)} cloned"}, status=status.HTTP_200_OK)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def analysis_request_action(request):
    try:
        action = request.data['action']
        if action == 'submit':
            return submit_analysis_slices(request)
        elif action == 'clone':
            return clone_analysis_request_slices(request)
        else:
            return Response('Unknown action', status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def submit_analysis_slices(request):
    slices = request.data['slices']
    for slice in slices:
        analysis_steps = list(AnalysisStepTemplate.objects.filter(request=request.data['requestID'], slice=InputRequestList.objects.get(slice=slice, request_id=request.data['requestID'])).order_by('id'))
        for step in analysis_steps:
            step.status = AnalysisStepTemplate.STATUS.APPROVED
            prod_step = step.step_production_parent
            prod_step.status = StepExecution.STATUS.APPROVED
            step.save()
            prod_step.save()
    return Response({'result':f"{len(slices)} submitted"}, status=status.HTTP_200_OK)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def save_template_changes(request):
    try:
        template_id= request.data['templateID']
        if request.data['templateBase'] is not None:
            AnalysisTaskTemplate.objects.filter(tag=template_id).update(**request.data['templateBase'])
        template = AnalysisTaskTemplate.objects.filter(tag=template_id).last()
        changes = request.data['params']
        for key in changes['removedFields']:
            template.task_parameters.pop(key)
        for key, value in changes['changes'].items():
            template.task_parameters[key] = value
        template.save()
        return Response(status=status.HTTP_200_OK)
    except AnalysisTaskTemplate.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)
def key_type_from_dict(original_dict, typed_result):

    for key, value in original_dict.items():
        if key not in typed_result:
            typed_result[key] = {}
            typed_result[key]['type'] = set()
            typed_result[key]['nullable'] = False
        if value is None:
            typed_result[key]['nullable'] = True
        if isinstance(value, dict):
            raise Exception(f'Nested dictionaries are not supported {key}')
        elif isinstance(value, list):
            raise Exception('Nested lists are not supported')
        elif isinstance(value, str):
            typed_result[key]['type'].add('str')
        elif isinstance(value, bool):
            typed_result[key]['type'].add('bool')
        elif isinstance(value, int):
            typed_result[key]['type'].add('int')
        elif isinstance(value, float):
            typed_result[key]['type'].add('float')
        else:
            typed_result[key]['type'].add('str')

def get_keys_types_from_task_parametrers(keys_types: dict, array_keys: dict, dict_keys:dict, mse: dict, task_parameters: dict):
    for key, value in task_parameters.items():
        if key != 'multiStepExec':
            if key not in keys_types:
                keys_types[key] = {}
                keys_types[key]['type'] = set()
                keys_types[key]['nullable'] = False

            if value is None:
                keys_types[key]['nullable'] = True

            if isinstance(value, dict):
                if key not in dict_keys:
                    dict_keys[key] = {}
                key_type_from_dict(value, dict_keys[key])
            elif isinstance(value, list):
                if key not in array_keys:
                    array_keys[key] = {}
                for item in value:
                    if isinstance(item, dict):
                        key_type_from_dict(item, array_keys[key])
            else:
                key_type_from_dict({key: value}, keys_types)
        elif key == 'multiStepExec':
                get_keys_types_from_task_parametrers(mse[0], mse[1], mse[2], {}, value)

def simple_analy_slice_clone(request_id: int, slice_number: int) -> int:
    try:
        new_slice = InputRequestList.objects.get(request_id=request_id, slice=slice_number)
        original_slice = InputRequestList.objects.get(request_id=request_id, slice=slice_number)
        step_exec = StepExecution.objects.get(slice=original_slice, request=request_id)
        analy_step = AnalysisStepTemplate.objects.get(step_production_parent=step_exec)
        new_slice.id = None
        new_slice.slice = TRequest.objects.get(reqid=request_id).get_next_slice()
        new_slice.save()
        step_exec.id = None
        step_exec.slice = new_slice
        if step_exec.status == StepExecution.STATUS.APPROVED:
            step_exec.status = StepExecution.STATUS.NOT_CHECKED
        step_exec.save()
        analy_step.id = None
        analy_step.slice = new_slice
        analy_step.step_production_parent = step_exec
        if analy_step.status == AnalysisStepTemplate.STATUS.APPROVED:
            analy_step.status = AnalysisStepTemplate.STATUS.NOT_CHECKED
        analy_step.save()
        return new_slice.slice

    except Exception as e:
        raise Exception(f'Failed to clone slice {slice} of request {request_id}: {str(e)}')
# """
# {
#   taskName= 'text',
#   uniqueTaskName= 'checkbox',
#   vo= 'text',
#   architecture= 'text',
#   transUses= 'text',
#   transHome= 'text',
#   processingType= 'text',
#   prodSourceLabel= 'text',
#   site= 'text',
#   includedSite= 'text',
#   cliParams= 'text',
#   osInfo= 'text',
#   nMaxFilesPerJob= 'number',
#   respectSplitRule= 'checkbox',
#   sourceURL= 'text',
#   dsForIN= 'text',
#   mergeOutput= 'checkbox',
#   userName= 'text',
#   taskType= 'text',
#   taskPriority= 'number',
#   nFilesPerJob= 'number',
#   fixedSandbox= 'text',
#   walltime= 'number',
#   coreCount= 'number',
#   noInput= 'checkbox',
#   nEvents= 'number',
#   nEventsPerJob= 'number',
#   noEmail= 'checkbox',
#   osMatching= 'checkbox',
#   useRealNumEvents= 'checkbox',
#   skipScout= 'checkbox',
#   official= 'checkbox',
#   respectLB= 'checkbox',
#   maxAttempt= 'number',
#   useLocalIO= 'number',
#   workingGroup= 'text',
#   addNthFieldToLFN= 'text',
#   getNumEventsInMetadata= 'checkbox',
#   baseRamCount= 'number',
#   campaign= 'text',
#   cloud= 'text',
#   cpuTimeUnit= 'text',
#   ipConnectivity= 'text',
#   maxFailure= 'number',
#   mergeCoreCount= 'number',
#   nGBPerJob= 'number',
#   noWaitParent= 'checkbox',
#   ramCount= 'number',
#   ramUnit= 'text',
#   reqID= 'number',
#   scoutSuccessRate= 'number',
#   ticketID= 'text',
#   ticketSystemType= 'text',
#   transPath= 'text',
#   ramCountUnit= 'text',
#   container_name= 'text',
#   avoidVP= 'checkbox',
#   runOnInstant= 'checkbox',
#   nEventsPerFile= 'number',
#   parentTaskName= 'text',
#   nFiles= 'number',
#   disableAutoFinish= 'checkbox',
#   failWhenGoalUnreached= 'checkbox',
#   goal= 'text',
#   ttcrTimestamp= 'text',
#   useExhausted= 'checkbox',
#   gshare= 'text',
#   ioIntensity= 'number',
#   ioIntensityUnit= 'text',
#   maxWalltime= 'number',
#   orderByLB= 'checkbox',
#   outDiskCount= 'number',
#   outDiskUnit= 'text',
#   tgtMaxOutputForNG= 'number',
#   inputPreStaging= 'checkbox',
#   noThrottle= 'checkbox',
#   releasePerLB= 'checkbox',
#   toStaging= 'checkbox',
#   nGBPerMergeJob= 'text',
#   nEventsPerInputFile= 'number',
#   reuseSecOnDemand= 'checkbox',
#   allowInputLAN= 'text',
#   cpuTime= 'number',
#   disableReassign= 'checkbox',
#   esConvertible= 'checkbox',
#   maxEventsPerJob= 'number',
#   minGranularity= 'number',
#   nucleus= 'text',
#   skipShortInput= 'checkbox',
#   baseWalltime= 'number',
#   waitInput= 'number',
#   notDiscardEvents= 'checkbox',
#   taskBrokerOnMaster= 'checkbox',
#   noLoopingCheck= 'checkbox',
#   tgtNumEventsPerJob= 'number',
#   countryGroup= 'text',
#   disableAutoRetry= 'number',
# }
#
# """