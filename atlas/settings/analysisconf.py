from dataclasses import dataclass

from .local import ANALYSIS_SETTING_CONF

dataclass()
class ANALYSIS_CONF:
    PROXY_PATH: str = ANALYSIS_SETTING_CONF['proxy_path']
    RUCIO_ACCOUNT: str = ANALYSIS_SETTING_CONF['rucio_account']
    PANDA_ACCOUNT: str = ANALYSIS_SETTING_CONF['panda_account']
    DEFAULT_DOWNLOAD_DIR: str = '/tmp'
    DEFAULT_SOURCE_DATASET: str = 'user.mborodin:user.mborodin.group_analysis_input_cache.ver0'
    DEFAULT_RSE: str = 'CERN-PROD_SCRATCHDISK'
    BASE_SCRIPT_PATH: str = ANALYSIS_SETTING_CONF['base_script_path']
    RUCIO_UPLOAD_SCRIPT: str = f'{BASE_SCRIPT_PATH}/rucio-upload.sh'
    RUCIO_DOWNLOAD_SCRIPT: str = f'{BASE_SCRIPT_PATH}/rucio-download.sh'
    JEDI_SUBMIT_SCRIPT: str = f'{BASE_SCRIPT_PATH}/panda-source-task.sh'
    JEDI_PRUN_SCRIPT: str = f'{BASE_SCRIPT_PATH}/prun-any.sh'