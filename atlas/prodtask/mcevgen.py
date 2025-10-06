import logging
from collections import defaultdict
from os import listdir
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from atlas.ami.client import AMIClient

from atlas.dkb.views import find_jo_by_dsid
from atlas.prodtask.models import MCJobOptions, SystemParametersHandler, HashTag, ProductionTask, DSIDHashtags
from .ddm_api import DDM
from .models import InputRequestList
import yaml

_logger = logging.getLogger('prodtaskwebui')


CVMFS_BASEPATH = '/cvmfs/atlas.cern.ch/repo/sw/Generators/'
JO_PARAMETERS = {'evgenConfig.minevents':'events_per_job','evgenConfig.inputFilesPerJob':'files_per_job','evgenConfig.nEventsPerJob':'events_per_job'}

def parse_jo_file(file_path):
    result = {}
    with open(file_path,'r') as jo_file:
        for jo_file_content_line in jo_file.read().splitlines():
            for param in list(JO_PARAMETERS.keys()):
                if jo_file_content_line.find(param) >= 0:
                    try:
                        if jo_file_content_line.startswith('#'):
                            continue
                        result[JO_PARAMETERS[param]] = int(jo_file_content_line.replace(' ', '').split('=')[-1])
                        break
                    except:  # noqa: E722 - legacy broad except kept
                        pass
    return result

def sync_cvmfs_dsid(dsid: str, base_path=CVMFS_BASEPATH):
    if len(dsid) <= 6:
        base_dsid_path = f'{base_path}/MCJobOptions/{dsid[:3]}xxx/{dsid}'
    else:
        base_dsid_path = f'{base_path}/MCJobOptions/{dsid[:1]}/{dsid[-3:]}xxx/{dsid}'
    dsid_update_values = {}
    for dsid_file in listdir(base_dsid_path):
        if dsid_file.startswith('mc') and dsid_file.endswith('py') and (len(dsid_file.split('.')) == 3):
            dsid_jo_content = parse_jo_file(f'{base_dsid_path}/{dsid_file}')
            dsid_update_values = {'physic_short': dsid_file,
                                    'events_per_job': dsid_jo_content.get('events_per_job', 5000),
                                    'files_per_job': dsid_jo_content.get('files_per_job', 1)}
    if dsid_update_values:
        if MCJobOptions.objects.filter(dsid=int(dsid)).exists():
            new_dsid_jo = MCJobOptions.objects.get(dsid=int(dsid))
        else:
            new_dsid_jo = MCJobOptions()
        new_dsid_jo.physic_short = dsid_update_values['physic_short']
        new_dsid_jo.events_per_job = dsid_update_values['events_per_job']
        new_dsid_jo.files_per_job = dsid_update_values['files_per_job']
        new_dsid_jo.save()
        return new_dsid_jo
    else:
        _logger.error(f'No JO files found for DSID {dsid} in {base_dsid_path}')
        return None

def sync_cvmfs_db(base_path='/cvmfs/atlas.cern.ch/repo/sw/Generators/MCJobOptions/'):
    dsids_parent_dirs = []
    for directory in listdir(base_path):
        if directory.endswith('xxx') and directory[:-3].isdigit():
            dsids_parent_dirs.append(directory)
        elif directory.isdigit():
            for second_level_directory in listdir(base_path+'/'+directory):
                if second_level_directory.endswith('xxx') and second_level_directory[:-3].isdigit():
                    dsids_parent_dirs.append(directory+'/'+second_level_directory)
    dsid_to_update = {}
    for dsids_dir in dsids_parent_dirs:
        for dsid in listdir(base_path+'/'+dsids_dir):
            if dsid.isdigit():
                for dsid_file in listdir(base_path+'/'+dsids_dir+'/'+dsid):
                    if dsid_file.startswith('mc') and dsid_file.endswith('py') and (len(dsid_file.split('.'))==3):
                        dsid_jo_content = parse_jo_file(base_path+'/'+dsids_dir+'/'+dsid+'/'+dsid_file)
                        dsid_to_update[dsid] = {'physic_short':dsid_file,
                                                'events_per_job':dsid_jo_content.get('events_per_job',5000),
                                                'files_per_job':dsid_jo_content.get('files_per_job',1)}
    for dsid in list(dsid_to_update.keys()):
        do_update = False
        if MCJobOptions.objects.filter(dsid=int(dsid)).exists():
            new_dsid_jo = MCJobOptions.objects.get(dsid=int(dsid))
            do_update = (new_dsid_jo.physic_short != dsid_to_update[dsid]['physic_short']) or \
                        (new_dsid_jo.events_per_job != dsid_to_update[dsid]['events_per_job']) or \
                        (new_dsid_jo.files_per_job != dsid_to_update[dsid]['files_per_job'])
        else:
            new_dsid_jo = MCJobOptions()
            new_dsid_jo.dsid = int(dsid)
            do_update = True
        if do_update:
            new_dsid_jo.physic_short = dsid_to_update[dsid]['physic_short']
            new_dsid_jo.events_per_job = dsid_to_update[dsid]['events_per_job']
            new_dsid_jo.files_per_job = dsid_to_update[dsid]['files_per_job']
            new_dsid_jo.save()


def sync_request_jos(production_request):
    slices = InputRequestList.objects.filter(request=production_request)
    for slice in slices:
        if slice.input_data and slice.input_data.isdigit():
            if slice.input_data.startswith('421') or int(slice.input_data) >= 500000:
                if MCJobOptions.objects.filter(dsid=int(slice.input_data)).exists():
                    slice.input_data = slice.input_data + '/' + MCJobOptions.objects.get(
                        dsid=int(slice.input_data)).physic_short
                    slice.save()
            else:
                slice.input_data =  find_jo_by_dsid(slice.input_data )
                slice.save()

GENERATORS_FIRST_DSIDS_NUMBER =generator_first_digit = [
    ("AMPT", 9),
    ("BCVEGPY", 9),
    ("BeamHaloGenerator", 9),
    ("BlackMax", 9),
    ("CalcHep", 9),
    ("Charybdis", 9),
    ("Charybdis2", 9),
    ("CompHep", 9),
    ("CosmicGenerator", 9),
    ("Dire4Pythia8", 9),
    ("Epos", 9),
    ("EvtGen", 9),
    ("FPMC", 9),
    ("Geneva", 9),
    ("HepMCAscii", 9),
    ("Herwig7", 8),
    ("Hijing", 9),
    ("HvyN", 9),
    ("Hydjet", 9),
    ("JHU", 9),
    ("MCFM", 9),
    ("MEtop", 9),
    ("MadGraph", 5),
    ("Matchig", 9),
    ("McAtNlo", 9),
    ("ParticleDecayer", 9),
    ("ParticleGenerator", 9),
    ("ParticleGun", 9),
    ("Phantom", 9),
    ("Photos", 9),
    ("Photospp", 9),
    ("PowHel", 9),
    ("Powheg", 6),
    ("ProtosLHEF", 9),
    ("Pyquen", 9),
    ("Pythia8B", 8),
    ("Pythia8", 8),
    ("ReadMcAscii", 9),
    ("QBH", 9),
    ("QGSJet", 9),
    ("Reldis", 9),
    ("STRINGS", 9),
    ("Sherpa", 7),
    ("Starlight", 9),
    ("SuperChic", 9),
    ("TauolaPP", 9),
    ("Tauolapp", 9),
    ("Tauola", 9),
    ("VBFNLO", 9),
    ("Whizard", 9),
    ("aMcAtNlo", 5),
    ("gg2vv", 9),
    ("gg2ww", 9),
    ("gg2zz", 9),
]

ACRONYMS_GENERATORS = {
    "AMPT": "AMPT",
    "BCV": "BCVEGPY",
    "BeamHaloGenerator": "BeamHaloGenerator",
    "BlackMax": "BlackMax",
    "CalcHep": "CalcHep",
    "Charybdis": "Charybdis",
    "Charybdis2": "Charybdis2",
    "CompHep": "CompHep",
    "CosmicGenerator": "CosmicGenerator",
    "Dire4Pythia8": "Dire4Pythia8",
    "Epos": "Epos",
    "EG": "EvtGen",
    "FPMC": "FPMC",
    "Geneva": "Geneva",
    "HepMC": "HepMCAscii",
    "H7": "Herwig7",
    "Hijing": "Hijing",
    "HvyN": "HvyN",
    "Hydjet": "Hydjet",
    "JHU": "JHU",
    "MCFM": "MCFM",
    "MEtop": "MEtop",
    "MG": "MadGraph",
    "Matchig": "Matchig",
    "McAtNlo": "McAtNlo",
    "PD": "ParticleDecayer",
    "ParticleGenerator": "ParticleGenerator",
    "PG": "ParticleGun",
    "Pm": "Phantom",
    "PH": "PowHel",
    "Ph": "Powheg",
    "ProtosLHEF": "ProtosLHEF",
    "Pyquen": "Pyquen",
    "P8B": "Pythia8B",
    "Py8": "Pythia8",
    "ReadMcAscii": "ReadMcAscii",
    "QBH": "QBH",
    "QGSJet": "QGSJet",
    "Reldis": "Reldis",
    "STRINGS": "STRINGS",
    "Sh": "Sherpa",
    "Starlight": "Starlight",
    "SuperChic": "SuperChic",
    "VBFNLO": "VBFNLO",
    "Whizard": "Whizard",
    "aMC": "aMcAtNlo",
    "gg2vv": "gg2vv",
    "gg2ww": "gg2ww",
    "gg2zz": "gg2zz",
}

def sync_bad_sw_releases(base_file='/cvmfs/atlas.cern.ch/repo/sw/Generators/MCJobOptions/common/BlackList_caches.txt'):
    """
    Syncs the bad software releases from a file to the database.
    The file should contain lines with the format: "bad_sw_release:reason"
    """
    # read the file with csv structure like AthGeneration,   21.6.16, MadGraph, lhapdf-config problem when multiple BOOST versions available
    bad_releases = defaultdict(list)
    with open(base_file, 'r') as file:
        for line in file.readlines():
            if line.startswith('#') or not line.strip():
                continue
            parts = line.split(',')
            if len(parts) < 3:
                _logger.error(f'Invalid line in BlackList_caches.txt: {line.strip()}')
                continue
            sw_release = parts[1].strip()
            generator =  parts[2].strip()
            skipped_dsids = set()
            for acronym, full_name in ACRONYMS_GENERATORS.items():
                if full_name.lower() == generator.lower():
                    skipped_dsids.add(acronym)
            bad_releases[sw_release] = list(set(bad_releases[sw_release]) | skipped_dsids)
    if bad_releases:
        SystemParametersHandler.BadEvgenSoftwareReleases.set_bad_releases(bad_releases)

# =============================
# YAML JOB PARAMETERS (GENERIC CONDITIONS)
# =============================

# Cache structure: { path: (mtime, entries) }
_JOB_PARAMS_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}

_COND_PATTERN = re.compile(r'^\s*(<=|>=|=|<|>)?\s*(\d+)\s*$')
_OP_MAP = {
    '<':  lambda a, b: a < b,
    '>':  lambda a, b: a > b,
    '<=': lambda a, b: a <= b,
    '>=': lambda a, b: a >= b,
    '=':  lambda a, b: a == b,
}


def _parse_condition_expr(expr: str) -> Tuple[str, int]:
    """Parse expressions like '<16000', '>=13600', '14000' (implies '=14000')."""
    m = _COND_PATTERN.match(str(expr))
    if not m:
        raise ValueError(f'Invalid condition expression: {expr!r}')
    op, value = m.group(1), int(m.group(2))
    if not op:
        op = '='
    return op, value


def load_job_parameters_from_yaml(path: str) -> List[Dict[str, Any]]:
    """Load job parameters YAML with generic condition variables.

    Expected structure:
    job_parameters:
      - nEventsPerJob: 10000
        inputFilesPerJob: 11
        conditions:
          - energy: ">=13600"
          - pileup: "<50"

    Returns list of normalized entries.
    """
    if yaml is None:
        raise RuntimeError('PyYAML not installed; cannot load job parameters YAML')
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    mtime = os.path.getmtime(path)
    cache = _JOB_PARAMS_CACHE.get(path)
    if cache and cache[0] == mtime:
        return cache[1]
    with open(path, 'r') as f:
        raw = yaml.safe_load(f) or {}
    if 'job_parameters' not in raw or not isinstance(raw['job_parameters'], list):
        raise ValueError("YAML must contain a 'job_parameters' list")
    entries: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw['job_parameters']):
        if not isinstance(item, dict):
            _logger.warning(f'Skipping non-dict job_parameters entry at index {idx}')
            continue
        n_events = item.get('nEventsPerJob')
        in_files = item.get('inputFilesPerJob')
        try:
            n_events = int(n_events)
            in_files = int(in_files)
        except Exception:
            _logger.warning(f'Entry {idx} has invalid numeric fields; skipping')
            continue
        cond_list = item.get('conditions', []) or []
        if not isinstance(cond_list, list):
            _logger.warning(f'Entry {idx} conditions not list; ignoring conditions')
            cond_list = []
        parsed_conditions: List[Dict[str, Any]] = []
        for c_idx, cond in enumerate(cond_list):
            if not isinstance(cond, dict):
                _logger.warning(f'Condition {c_idx} in entry {idx} not dict; skipping')
                continue
            for var, expr in cond.items():
                try:
                    op, value = _parse_condition_expr(expr)
                    parsed_conditions.append({'variable': var, 'op': op, 'value': value, 'raw': expr})
                except Exception as e:  # log and skip invalid
                    _logger.warning(f'Invalid condition {expr!r} for {var} in entry {idx}: {e}')
        entries.append({
            'n_events_per_job': n_events,
            'input_files_per_job': in_files,
            'conditions': parsed_conditions
        })
    _JOB_PARAMS_CACHE[path] = (mtime, entries)
    return entries


def _conditions_match(entry: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Return True if all conditions in entry match provided context.
    Missing variables -> mismatch."""
    for cond in entry.get('conditions', []):
        var = cond['variable']
        if var not in context:
            return False
        try:
            actual = int(context[var])
        except Exception:
            return False
        op = cond['op']
        value = cond['value']
        cmp_fn = _OP_MAP.get(op)
        if not cmp_fn:
            return False
        if not cmp_fn(actual, value):
            return False
    return True


def match_job_parameters(entries: List[Dict[str, Any]], context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the first entry whose conditions all match the context (AND semantics)."""
    for entry in entries:
        if _conditions_match(entry, context):
            return entry
    return None


def resolve_job_parameters_from_yaml(path: str,
                                     context: Dict[str, Any]) -> Tuple[int, int]:
    """Convenience wrapper: load YAML and resolve matching job parameters.

    context example: {'energy': 13600, 'pileup': 40}
    Returns (events_per_job, files_per_job).
    """
    try:
        entries = load_job_parameters_from_yaml(path)
    except FileNotFoundError as e:
        _logger.warning(f'Job parameters YAML not found: {path}')
        raise e
    except Exception as e:
        _logger.error(f'Failed to load job parameters YAML {path}: {e}')
        raise e
    match = match_job_parameters(entries, context)
    if match:
        return match['n_events_per_job'], match['input_files_per_job']
    raise ValueError(f'Failed to load job parameters YAML {path}')


def read_yaml_cvmfs_dsid(dsid: str, base_path=CVMFS_BASEPATH):
    if len(dsid) <= 6:
        base_dsid_path = f'{base_path}/MCJobOptions/{dsid[:3]}xxx/{dsid}'
    else:
        base_dsid_path = f'{base_path}/MCJobOptions/{dsid[:1]}/{dsid[-3:]}xxx/{dsid}'
    config_file = f'{base_dsid_path}/production_parameters.yaml'
    if os.path.exists(config_file):
        try:
            return load_job_parameters_from_yaml(config_file)
        except Exception as e:
            _logger.error(f'Error reading YAML for DSID {dsid}: {e}')
            return None
    return None


def set_pmg_hashtags(prodsys_hashtag='AMIEvgenPMGHTs'):
    tasks = HashTag.objects.get(hashtag=prodsys_hashtag).tasks
    ami_client = AMIClient()
    ddm = DDM()
    for task in tasks:
        if task.status in ProductionTask.BAD_STATUS:
            task.remove_hashtag(prodsys_hashtag)
        elif task.status in [ProductionTask.STATUS.FINISHED, ProductionTask.STATUS.DONE]:
            task_dsid = int(task.name.split('.')[1])
            evgen_tag = task.name.split('.')[-1].split('_')[0]
            output = next(task.output_non_log_datasets())
            ami_dataset = ddm.get_sample_container_name(output)
            if ami_dataset.startswith('mc16'):
                ami_dataset = ami_dataset.replace('mc16', 'mc15')
            ami_dataset_exists = None
            try:
                ami_dataset_exists = ami_client.ami_get_dataset_info(ami_dataset)
            except Exception as e:
                pass
            if ami_dataset_exists is not None:
                if DSIDHashtags.objects.filter(dsid=task_dsid, etag=evgen_tag).exists():
                    for hashtag in DSIDHashtags.objects.get(dsid=task_dsid, etag=evgen_tag).hashtags:
                        ami_scope, ami_ht  = hashtag.split(':')
                        if ami_ht.startswith('Test'):
                            ami_scope = 'testScope'
                        ami_client.set_ami_hashtag(dataset=ami_dataset, hashtag=ami_ht, scope=ami_scope, pattern="PMG_GLOBAL_SCOPE")
                        _logger.info(f'Set hashtag {hashtag} on DSID {task_dsid} for task {task.id}')
                    task.remove_hashtag(prodsys_hashtag)
    return None