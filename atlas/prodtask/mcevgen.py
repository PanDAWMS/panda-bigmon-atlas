import logging
from os import listdir

from atlas.dkb.views import find_jo_by_dsid
from atlas.prodtask.models import  MCJobOptions
from .models import InputRequestList
_logger = logging.getLogger('prodtaskwebui')


CVMFS_BASEPATH = '/cvmfs/atlas.cern.ch/repo/sw/Generators/'
JO_PARAMETERS = {'evgenConfig.minevents':'events_per_job','evgenConfig.inputFilesPerJob':'files_per_job'}

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

def sync_cvmfs_db(base_path='/cvmfs/atlas.cern.ch/repo/sw/Generators/MCJobOptions/'):
    dsids_parent_dirs = []
    for directory in listdir(base_path):
        if directory.endswith('xxx') and directory[:-3].isdigit():
            dsids_parent_dirs.append(directory)
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
            print('JO updated %s' %  dsid_to_update[dsid]['physic_short'])
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

