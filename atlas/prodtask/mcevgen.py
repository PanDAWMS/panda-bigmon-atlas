import logging
from collections import defaultdict
from os import listdir

from atlas.dkb.views import find_jo_by_dsid
from atlas.prodtask.models import MCJobOptions, SystemParametersHandler
from .models import InputRequestList
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
                    except:
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
