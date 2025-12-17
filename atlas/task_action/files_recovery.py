import logging
from dataclasses import asdict

from django.core.cache import cache
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import ProductionTask, JediDatasets
from atlas.task_action.task_management import FileRecoveryCache, FileRecoveryParameters

_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def dataset_lost_file_info(request):
    """
    API endpoint to retrieve lost file information for a given dataset.

    Args:
        request: The HTTP request object.
        dataset_name: The name of the dataset to query.
    Returns:
        A JSON response containing lost file information.
    """
    dataset_name = 'Not defined'
    try:
        ddm = DDM()
        dataset_name = request.query_params.get('dataset')
        dataset_info = ddm.dataset_info(dataset_name)
        if dataset_info.did_type != 'DATASET':
            return Response({'error': f'{dataset_name} is not a dataset'}, status=400)
        task = ProductionTask.objects.get(id=dataset_info.task_id)
        if task.status not in [ProductionTask.STATUS.DONE, ProductionTask.STATUS.FINISHED]:
            return Response({'error': f'Task {task.id} is in status {task.status}, should be done or finished'}, status=400)
        jedi_dataset = JediDatasets.objects.get(id=task.id, datasetname__in= ddm.with_and_without_scope([dataset_name]))
        missing_files = jedi_dataset.nfiles - dataset_info.length
        if missing_files <= 0:
            return Response({'error': f'No lost files in dataset {dataset_name}, files by jedi {jedi_dataset.nfiles} in dataset {dataset_info.length}'}, status=400)
        recreate_parent = False
        if task.input_dataset and not ddm.dataset_exists(task.input_dataset):
            recreate_parent = True
        cache_key = f"FILE_RECOVERY_LOG_{dataset_name}"
        cached_data = cache.get(cache_key)
        recovery_info = None
        if cached_data:
            recovery_info = FileRecoveryCache(
                async_task_id=cached_data['async_task_id'],
                dataset=cached_data['dataset'],
                parameters=FileRecoveryParameters(**cached_data['parameters'])
            )
            recovery_info = asdict(recovery_info)


        return Response({'dataset': dataset_name, 'recoveryInfo': recovery_info, 'recreateParent': recreate_parent,
                         'lost_files': missing_files}, status=200)
    except Exception as e:
        _logger.error(f"Error retrieving lost file info for dataset {dataset_name}: {e}")
        return Response({'error': str(e)}, status=500)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def submit_recovery_request(request):
    """
    API endpoint to submit a file recovery request for a given dataset.

    Args:
        request: The HTTP request object.
        dataset_name: The name of the dataset to recover files from.
    Returns:
        A JSON response confirming the submission of the recovery request.
    """
    dataset_name = 'Not defined'
    try:
        ddm = DDM()
        dataset_name = request.data.get('dataset')

    except Exception as e:
        _logger.error(f"Error submitting recovery request for dataset {dataset_name}: {e}")
        return Response({'error': str(e)}, status=500)