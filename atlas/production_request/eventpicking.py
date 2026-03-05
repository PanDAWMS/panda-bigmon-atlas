import logging
import os
from collections import defaultdict

from rest_framework import serializers, generics, status
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated

from atlas.ami.views import get_project_by_run
from atlas.analysis_tasks.source_handling import upload_to_rucio
from atlas.celerybackend.celery import app
from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import EventPickingUserRequest, EventPickingProcessing, EventPickingContent, TRequest, \
    TProject, SystemParametersHandler, InputRequestList, StepExecution, ProductionTask, DistributedLock
import phoenixdb

from atlas.prodtask.views import clone_slices, request_clone_slices, set_request_status
from atlas.settings.analysisconf import ANALYSIS_CONF

_jsonLogger = logging.getLogger('prodtask_ELK')

def find_dataset_name(run_number: int, project: str, stream: str, guid: str) -> str:
    ddm = DDM()
    possible_datasets = ddm.find_dataset(f'{project}.{run_number:08d}.{stream}.%.RAW')
    if len(possible_datasets) != 1:
        dataset = [x['name'] for x in ddm.get_did_by_guid(guid) if x['scope'] == project][0]
    else:
        dataset = possible_datasets[0]
    return dataset

def create_file_list(dataset: str, guids: list[str]) -> dict:
    ddm = DDM()
    guids = [guid.lower() for guid in guids]
    dataset_files = ddm.list_files(dataset)
    dataset_file_name_by_guid = {file['guid'].lower():file['name'] for file in dataset_files}
    result = {}
    for guid in guids:
        if guid in dataset_file_name_by_guid:
            result[guid] = dataset_file_name_by_guid[guid]
        else:
            result[guid] = None
    return result

def ep_processing_serialisation(ep_processing: EventPickingProcessing) -> dict:
    result = {
        'id': ep_processing.id,
        'status': ep_processing.status,
        'stream': ep_processing.ep_request.stream,
        'project': ep_processing.project,
        'logs': ep_processing.logs,
        'stats': ep_processing.stats,
        'production_request_id': ep_processing.production_request.reqid if ep_processing.production_request else None,
    }
    if ep_processing.stats == ep_processing.STATUS.RUNNING:
        running_tasks = ProductionTask.objects.filter(reqid=ep_processing.production_request.reqid, status__in=ProductionTask.SYNC_STATUSES).count()
        finished_tasks = ProductionTask.objects.filter(reqid=ep_processing.production_request.reqid, status=ProductionTask.STATUS.FINISHED).count()
        done_tasks = ProductionTask.objects.filter(reqid=ep_processing.production_request.reqid,
                                                       status=ProductionTask.STATUS.DONE).count()
        result['stats'].update({
            'running_tasks': running_tasks,
            'finished_tasks': finished_tasks,
            'done_tasks': done_tasks,
        })
    return result

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_ep_events(request):
    try:
        evvents_type = request.query_params.get('type')
        id = int(request.query_params.get('id'))
        if evvents_type == 'request':
            content = [f'{x[0]} {x[1]}' for x in EventPickingUserRequest.objects.get(id=id).input_file['content']]
            return Response(content, status=status.HTTP_200_OK)
        if evvents_type == 'processing':
            file_events = {}
            for ep_content in EventPickingContent.objects.filter(ep_request=id):
                file_events.update(ep_content.files_events)
            return Response(file_events, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def ep_request_stats(request):
    try:
        jira = request.query_params.get('jira')
        if not jira:
            return Response("Missing 'jira' field in request data", status=status.HTTP_400_BAD_REQUEST)
        ep_requests = list(EventPickingUserRequest.objects.filter(jira__endswith=jira))
        unique_stats_by_stream = {}
        for ep_request in ep_requests:
            if ep_request.stream not in unique_stats_by_stream:
                unique_stats_by_stream[ep_request.stream] = {'runs':set(), 'events':set()}
            for run, event in ep_request.run_events:
                unique_stats_by_stream[ep_request.stream]['runs'].add(run)
                unique_stats_by_stream[ep_request.stream]['events'].add(f'{run}-{event}')
        ep_requests_serialised = []
        for ep_request in ep_requests:
            unique_files = len(set(unique_stats_by_stream[ep_request.stream]['runs']))
            unique_events = len(set(unique_stats_by_stream[ep_request.stream]['events']))
            ep_requests_serialised.append({
                'id': ep_request.id,
                'jira': ep_request.jira,
                'description': ep_request.description,
                'stream': ep_request.stream,
                'unique_files': unique_files,
                'unique_events': unique_events
            })


        ep_productions = EventPickingProcessing.objects.filter(ep_request__in=ep_requests)
        ep_productions_serialised = list(map(ep_processing_serialisation, ep_productions))
        return Response({'requests': ep_requests_serialised, 'productions': ep_productions_serialised}, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def retry_ep_progress(request):
    try:
        id = request.data.get('id')
        ep_progress = EventPickingProcessing.objects.get(id=id)
        ep_request_id = ep_progress.ep_request_id
        ep_progress.delete()
        process_ep_request.delay(ep_request_id, False)
        return Response(f"EP progress with id {id} deleted", status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def submit_ep_request(request):
    try:
        jira = request.data.get('jira')
        if not jira:
            return Response("Missing 'jira' field in request data", status=status.HTTP_400_BAD_REQUEST)
        ep_requests = list(EventPickingUserRequest.objects.filter(jira__endswith=jira))
        number_of_submitted_ep_requests = 0
        for ep_processing in EventPickingProcessing.objects.filter(ep_request__in=ep_requests):
            if ep_processing.status == EventPickingProcessing.STATUS.PICKED:
                ep_processing.status = EventPickingProcessing.STATUS.PREPARING
                ep_processing.save()
                create_ep_production_request.delay(int(ep_processing.id), merge=ep_processing.ep_request.do_merge)
                number_of_submitted_ep_requests += 1
        return Response(f"{number_of_submitted_ep_requests} EP requests submitted for processing", status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def get_ep_request(request):
    try:
        jira = request.data.get('jira')
        if not jira:
            return Response("Missing 'jira' field in request data", status=status.HTTP_400_BAD_REQUEST)
        ep_requests = EventPickingUserRequest.objects.filter(jira__endswith=jira)
        if not ep_requests:
            return Response(None, status=status.HTTP_200_OK)
        else:
            ep_request = ep_requests.last()
            return Response({
                'description': ep_request.description,
                'merge': ep_request.do_merge
            }, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def create_or_update_ep_request(request):
    try:
        jira = request.data.get('jira')
        if not jira:
            return Response("Missing 'jira' field in request data", status=status.HTTP_400_BAD_REQUEST)
        jira = jira.strip()
        if not jira.startswith('http'):
            jira = 'https://its.cern.ch/jira/browse/' + jira.strip('/')
        stream = request.data.get('stream')
        merge = request.data.get('merge')
        submit = request.data.get('submit')
        if not stream:
            return Response("Missing 'stream' field in request data", status=status.HTTP_400_BAD_REQUEST)
        data_format = request.data.get('data_format')
        if EventPickingUserRequest.objects.filter(jira=jira, stream=stream).exists():
            ep_request = EventPickingUserRequest.objects.get(jira=jira, stream=stream, data_format=data_format)
        else:
            ep_request = EventPickingUserRequest(jira=jira, stream=stream, data_format=data_format, input_file={})
        new_content = [(line.split(' ')[0],line.split(' ')[1]) for line in request.data.get('content').split('\n') if line]
        version = 0
        ep_request.requestor = request.user.username
        ep_request.description = request.data.get('description')
        ep_request.save()
        ep_request = EventPickingUserRequest.objects.get(jira=jira, stream=stream, data_format=data_format)
        if ep_request.input_file:
            version = ep_request.input_file['version']
            existing_content = set([tuple(x) for x in ep_request.input_file['content']])
            new_content_set = set(new_content)
            if new_content_set - existing_content:
                version += 1
            new_content = list(existing_content) + list(new_content_set)
        rucio_file_name = f'group.proj-evind.{ep_request.id:08d}.{stream}.{version}.txt'
        ep_request.input_file = {'version': version, 'content': list(set(new_content)), 'rucio':rucio_file_name, 'merge':merge}
        ep_request.save()
        for  ep_processing in EventPickingProcessing.objects.filter(ep_request=ep_request):
            ep_processing.status = EventPickingProcessing.STATUS.GUID_SEARCH
            ep_processing.save()
        upload_ep_file_to_rucio.delay(int(ep_request.id))
        process_ep_request.delay(int(ep_request.id), submit)
        return Response(f"{jira.split('/')[-1]}", status=status.HTTP_200_OK)

    except Exception as e:
        _jsonLogger.error(f'Problem with Event Picking request creation: {e}')
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.task(ignore_result=True)
def upload_ep_file_to_rucio(ep_request_id: int):
    ep_request = EventPickingUserRequest.objects.get(id=ep_request_id)
    file_name = ep_request.input_file['rucio']
    file_path = f'/tmp/{file_name}'
    if file_name:
        with open(f'/tmp/{file_name}', 'w') as f:
            for run,event in ep_request.input_file['content']:
                f.write(f"{run} {event}\n")
    return upload_to_rucio(ANALYSIS_CONF.RUCIO_UPLOAD_SCRIPT, ANALYSIS_CONF.PROXY_PATH, ANALYSIS_CONF.RUCIO_ACCOUNT,
                           file_path, ANALYSIS_CONF.DEFAULT_RSE, SystemParametersHandler.get_ep_config().default_source_dataset, SystemParametersHandler.get_ep_config().default_source_dataset.split(':')[0] )

def get_raw_files_guids_by_run(run: int, project: str, stream: str, events: list[int], batch_size=500) -> list[(str, int)]:
    def chunks(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]
    _jsonLogger.info(f'Search in hbase phoenix for run {run}, project {project}, stream {stream} and total events {len(events)})')
    hbase_phoenix_database_url = SystemParametersHandler.get_ep_config().hbase_url
    with phoenixdb.connect(hbase_phoenix_database_url, autocommit=True, authentication='SPNEGO') as connection:
        with connection.cursor() as cursor:
            dataset_query = (f"SELECT dspid,dstypeid,events FROM AEI.CANONICAL_0 WHERE runno = ? AND "
                             f"project = ? AND streamName = ? AND has_raw = ? AND dataType = ?")
            cursor.execute(dataset_query, (run, project, stream, True, 'AOD'))
            results_dict = cursor.fetchall()
            if len(results_dict) == 0:
                return []
            results_dict.sort(key=lambda x: x[2], reverse=True)
            dspid, dstypeid, _= results_dict[0]
            rows = []
            for batch in chunks(events, batch_size):
                placeholders = ",".join(["?"] * len(batch))
                q2 = f"""
                SELECT pv, eventno
                FROM AEI.EVENTS_0
                WHERE dspid = ?
                  AND dstypeid = ?
                  AND eventno IN ({placeholders})
                """
                params = [dspid, dstypeid] + batch
                cursor.execute(q2, params)
                rows += cursor.fetchall()
            result = []
            for row in rows:
                for guid in row[0]:
                    guid_string = guid.hex()
                    if guid_string.startswith('0800'):
                        result.append((guid_string[4:36], row[1]))
                        break
            _jsonLogger.info(
                f'Found {len(result)} events for run {run}, project {project}, stream {stream} and total events {len(events)})')
            return result



@app.task(ignore_result=True, time_limit=3600*3)
def process_ep_request(ep_request_id: int, submit: bool = False):
    ep_request = EventPickingUserRequest.objects.get(id=ep_request_id)
    events_by_run = defaultdict(list)
    for run_event in ep_request.input_file['content']:
        run, event = run_event
        events_by_run[run].append(event)
    request_to_process = []
    runs_by_project = defaultdict(list)
    for run in events_by_run:
        runs_by_project[get_project_by_run(int(run))].append(run)
    for project in runs_by_project:
        lock_key = f'eventpickingprocessing_{project}_{ep_request_id}'
        if not DistributedLock.wait_and_acquire_lock(lock_key, 3600*3, 3600*3 ):
                _jsonLogger.error(f"Could not acquire lock for project {project} and request {ep_request_id}")
                raise Exception(f"Could not acquire lock for project {project} and request {ep_request_id}")
        try:
            if EventPickingProcessing.objects.filter(ep_request=ep_request, project=project).exists():
                ep_processing = EventPickingProcessing.objects.get(ep_request=ep_request, project=project)
            else:
                ep_processing = EventPickingProcessing(ep_request=ep_request, project=project)
            ep_processing.status = EventPickingProcessing.STATUS.GUID_SEARCH
            ep_processing.save()
            ep_processing = EventPickingProcessing.objects.get(ep_request=ep_request, project=project)
            errors = ''
            runs = 0
            total_files = 0
            total_events = 0
            for run in runs_by_project[project]:
                events = events_by_run[run]
                dataset_base = f'{project}.{int(run)}.{ep_request.stream}'
                ep_content = EventPickingContent(ep_request=ep_processing,
                                                 dataset_base=dataset_base, files_events={})
                if EventPickingContent.objects.filter(ep_request=ep_processing, dataset_base=dataset_base).exists():
                    ep_content = EventPickingContent.objects.get(ep_request=ep_processing, dataset_base=dataset_base)
                try:
                    existing_events = sum(ep_content.files_events.values(), [])
                    new_events = list(set(events) - set(existing_events))
                    if not new_events:
                        runs += 1
                        total_events += len(existing_events)
                        total_files += len(ep_content.files_events.keys())
                        continue
                    guids = get_raw_files_guids_by_run(int(run), project, ep_request.stream, list(map(int,new_events)))
                    # Check that all events are present
                    if len(guids) == 0:
                        raise Exception(f"No events found for run {run}")
                    if len(guids) < len(new_events):
                        missing_events = set(new_events) - set([str(g[1]) for g in guids])
                        raise Exception(f"Missing events for run {run}: {len(missing_events)}")
                    # Check files:
                    unique_guids = list(set([x[0] for x in guids]))
                    number_of_events_per_guid = defaultdict(int)
                    for guid, event in guids:
                        number_of_events_per_guid[guid] += 1
                    dataset = find_dataset_name(int(run), project, ep_request.stream, unique_guids[0])
                    files = create_file_list(dataset, unique_guids)
                    missing_files = sum([1 for x in files.values() if x is None])
                    if missing_files > 0:
                        raise Exception(f"{missing_files} files are missing in dataset {dataset}")
                    # Store the guids in the database
                    events_per_file = defaultdict(list)
                    for guid, event in guids:
                        events_per_file[files[guid]].append(event)
                    for file, current_events in events_per_file.items():
                        if file in ep_content.files_events:
                            existing_events = set(ep_content.files_events[file])
                            ep_content.files_events[file] = list(existing_events.union(current_events))
                        else:
                            ep_content.files_events[file] = current_events
                    ep_content.save()
                    total_events += len(sum(ep_content.files_events.values(), []))
                    runs += 1
                    total_files += len(ep_content.files_events.keys())
                except Exception as e:
                    _jsonLogger.error(f'Error processing run {run} for project {project}: {str(e)}')
                    errors += f"Error processing run {run}: {str(e)}\n"
            if errors:
                ep_processing.status = EventPickingProcessing.STATUS.ERROR
                ep_processing.logs = errors[:4000]
                ep_processing.save()
            else:
                ep_processing.status = EventPickingProcessing.STATUS.PICKED
                stats = ep_processing.stats or {}
                stats.update({'picked':{'runs':runs,'files':total_files,'events':total_events}})
                ep_processing.stats = stats
                ep_processing.save()
                request_to_process.append(ep_processing.id)
        finally:
            DistributedLock.release_lock(lock_key)
    if submit:
        map(create_ep_production_request, request_to_process)

@app.task(ignore_result=True)
def create_ep_production_request(ep_processing_id: int, merge: bool = False, to_submit: bool = False):
    ep_processing = EventPickingProcessing.objects.get(id=ep_processing_id)
    ep_request = ep_processing.ep_request
    production_request = ep_processing.production_request
    pattern_request = SystemParametersHandler.get_ep_config().pattern_request
    if not production_request:
        production_request = request_clone_slices(pattern_request, ep_request.requestor, ep_request.description,
                                                  ep_request.jira, [], ep_processing.project, False)
        ep_processing.production_request = TRequest.objects.get(reqid=production_request)
        production_request = ep_processing.production_request
        production_request.campaign = ep_processing.project
        production_request.save()
        ep_processing.save()
    lock_key = f"ep_productionrequest_{ep_processing.production_request.reqid}"
    if not DistributedLock.wait_and_acquire_lock(lock_key, 600, 1000):
        _jsonLogger.error(f"Could not acquire lock for production request {production_request.reqid}")
        return
    try:
        ep_processing.status = EventPickingProcessing.STATUS.RUNNING
        ep_processing.save()
        ep_contents = EventPickingContent.objects.filter(ep_request=ep_processing)
        pattern_request = SystemParametersHandler.get_ep_config().pattern_request
        pattern_slice = 0
        slices_to_submit = []
        if merge:
            pattern_slice = 1
        for ep_content in ep_contents:
            new_slice_number = clone_slices(pattern_request, production_request.reqid, [pattern_slice], -99, True)[0]
            new_slice = InputRequestList.objects.get(request=production_request, slice=new_slice_number)
            slices_to_submit.append(new_slice)
            project, run, stream = ep_content.dataset_base.split('.')
            new_slice.dataset = find_dataset_name(int(run), project, stream, list(ep_content.files_events.keys())[0])
            new_slice.save()
        if to_submit:
            steps = StepExecution.objects.filter(request=production_request, slice__in=slices_to_submit)
            for step in steps:
                step.status = StepExecution.STATUS.APPROVED
                step.save()
            set_request_status('cron', production_request.reqid, 'approved', 'Auto approval',
                               'Request was automatically approved')
    finally:
        DistributedLock.release_lock(lock_key)


@app.task(ignore_result=True)
def check_running_ep_requests():
    ddm = DDM()
    for ep_request in EventPickingProcessing.objects.filter(status=EventPickingProcessing.STATUS.RUNNING):
        production_request = ep_request.production_request
        tasks = ProductionTask.objects.filter(request=production_request)
        checked_runs = set()
        events = 0
        running_task = False
        for task in tasks:
            if 'evtpick' in task.name:
                if task.status not in ProductionTask.NOT_RUNNING:
                    running_task = True
                    break
                if task.status in [ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED]:
                    checked_runs.add(task.name.split('.')[1])
                    output_dataset = next(task.output_non_log_datasets())
                    events += ddm.dataset_info(output_dataset).events
        if not running_task:
            current_stats = ep_request.stats or {}
            if 'picked' in current_stats and current_stats['picked']['runs'] == len(checked_runs) and current_stats['picked']['events'] == events:
                ep_request.status = EventPickingProcessing.STATUS.DONE
            else:
                ep_request.status = EventPickingProcessing.STATUS.FINISHED
            current_stats.update({'produced':{'runs':len(checked_runs),'events':events}})
            ep_request.stats = current_stats
            ep_request.save()
    return


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def ep_requests(request):
    try:
        ep_requests = list(set(EventPickingUserRequest.objects.all().values_list('jira','description','requestor')))
        result = [{'jira':x[0], 'description': x[1], 'requestor': x[2]} for x in ep_requests]
        return Response(result, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

