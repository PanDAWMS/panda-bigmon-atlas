
import os
import subprocess
from uuid import uuid1

import requests
import logging

from atlas.prodtask.ddm_api import DDM
from atlas.prodtask.models import AnalysisTaskTemplate
from atlas.settings.analysisconf import ANALYSIS_CONF
_logger = logging.getLogger('prodtaskwebui')
from atlas.celerybackend.celery import app
_jsonLogger = logging.getLogger('prodtask_ELK')

def download_source(source_url, download_dir):
    """Download source file from URL to download_dir.

    Parameters
    ----------
    source : dict
        Source dictionary.
    download_dir : str
        Directory to download file to.

    Returns
    -------
    str
        Path to downloaded file.
    """
    url = source_url
    filename = url.split('/')[-1]
    filepath = os.path.join(download_dir, filename)
    if not os.path.isfile(filepath):
        _logger.info('Downloading {} to {}'.format(url, filepath))
        response = requests.get(url)
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        with open(filepath, 'wb') as f:
            f.write(response.content)
    return filepath

def submit_prun(command: str, parameters: str) -> int|None:
    result = subprocess.run([ANALYSIS_CONF.JEDI_PRUN_SCRIPT, ANALYSIS_CONF.PROXY_PATH, ANALYSIS_CONF.PANDA_ACCOUNT,
                             command, parameters], text=True, capture_output=True)
    if result.returncode != 0:
        raise Exception(f'Failed to submit task to panda: {result.stderr}')
    for line in result.stdout.splitlines():
        if 'new jediTaskID=' in line:
            return int(line.strip().split('=')[1])
    return None

def submit_task_for_rucio_file(rucio_file):
    source_file = download_from_rucio(ANALYSIS_CONF.RUCIO_DOWNLOAD_SCRIPT, ANALYSIS_CONF.PROXY_PATH, ANALYSIS_CONF.RUCIO_ACCOUNT,ANALYSIS_CONF.DEFAULT_DOWNLOAD_DIR, rucio_file)
    return submit_JEDI_source(ANALYSIS_CONF.JEDI_SUBMIT_SCRIPT, ANALYSIS_CONF.PROXY_PATH, ANALYSIS_CONF.PANDA_ACCOUNT,
                              source_file, f'{ANALYSIS_CONF.DEFAULT_SOURCE_DATASET.split(":")[0]}.{uuid1()}')


def upload_source_to_rucio(archive_name, source_panda_cache):
    ddm = DDM()
    if ddm.dataset_exists(':'.join([ANALYSIS_CONF.DEFAULT_SOURCE_DATASET.split(':')[0], archive_name])):
        return ':'.join([ANALYSIS_CONF.DEFAULT_SOURCE_DATASET.split(':')[0], archive_name])
    source_url=f'{source_panda_cache}/cache/{archive_name}'
    file_path = download_source(source_url, ANALYSIS_CONF.DEFAULT_DOWNLOAD_DIR)
    return upload_to_rucio(ANALYSIS_CONF.RUCIO_UPLOAD_SCRIPT, ANALYSIS_CONF.PROXY_PATH, ANALYSIS_CONF.RUCIO_ACCOUNT,
                           file_path, ANALYSIS_CONF.DEFAULT_RSE, ANALYSIS_CONF.DEFAULT_SOURCE_DATASET, ANALYSIS_CONF.DEFAULT_SOURCE_DATASET.split(':')[0] )


def download_from_rucio(script_path, proxy_path, rucio_account, output_dir, file_pfn):
    result = subprocess.run([script_path, proxy_path, rucio_account, output_dir, file_pfn], capture_output=True)
    if result.returncode != 0:
        raise Exception(f'Failed to download file from Rucio: {result.stderr}')
    return os.path.join(output_dir, file_pfn.split(':')[0], file_pfn.split(':')[1])

def upload_to_rucio(script_path, proxy_path, rucio_account, file_path, rse, dataset, scope):
    result = subprocess.run([script_path, proxy_path, rucio_account, rse, file_path, dataset, scope], capture_output=True)
    if result.returncode != 0:
        raise Exception(f'Failed to upload file to Rucio: {result.stderr}')
    return ':'.join([scope, os.path.basename(file_path)])

def submit_JEDI_source(script_path, proxy_path, panda_account, input_tarball, output_dataset):
    result = subprocess.run([script_path, proxy_path, panda_account, input_tarball, output_dataset], text=True, capture_output=True)
    if result.returncode != 0:
        raise Exception(f'Failed to submit source task to panda: {result.stderr}')
    for line in result.stdout.splitlines():
        if 'new jediTaskID=' in line:
            return int(line.strip().split('=')[1])
    return None

@app.task()
def submit_source_to_rucio(analisys_pattern_id: int):
        try:
            analysis_template = AnalysisTaskTemplate.objects.get(id=analisys_pattern_id)
            if not analysis_template.source_tar:
                archive_name = analysis_template.task_parameters['buildSpec']['archiveName']
                source_panda_cache = analysis_template.task_parameters['sourceURL']
                _jsonLogger.info('Upload source file to rucio',extra={'archive_name':archive_name,'source_panda_cache':source_panda_cache})
                result_file = upload_source_to_rucio(archive_name, source_panda_cache)
                analysis_template.source_tar = result_file
                analysis_template.save()
                return result_file
            else:
                return analysis_template.source_tar
        except Exception as e:
            _jsonLogger.error('Failed to upload source file to rucio',extra={'error':str(e)})
            raise e