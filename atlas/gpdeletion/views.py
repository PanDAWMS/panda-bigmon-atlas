import glob
import gzip
from dataclasses import dataclass, field
import xml.etree.ElementTree as ET
from django.contrib.auth.models import User
from django.contrib.messages.context_processors import messages
from django.http.response import HttpResponseBadRequest
from rest_framework.generics import get_object_or_404
from rest_framework.parsers import JSONParser

from atlas.ami.client import AMIClient
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats, TRequest, \
    ProductionDataset, GroupProductionDeletionExtension, GroupProductionDeletionProcessing, \
    GroupProductionDeletionRequest, InputRequestList, StepExecution, SystemParametersHandler
from atlas.dkb.views import es_by_keys_nested
from atlas.prodtask.ddm_api import DDM
from datetime import datetime, timedelta
import pytz
from rest_framework import serializers, generics
from django.forms.models import model_to_dict
from rest_framework import status
from atlas.settings import defaultDatetimeFormat
import logging
from django.utils import timezone
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication, BasicAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from atlas.celerybackend.celery import app

from django.core.cache import cache

_logger = logging.getLogger('prodtaskwebui')
_jsonLogger = logging.getLogger('prodtask_ELK')


FORMAT_BASES = ['BPHY', 'EGAM', 'EXOT', 'FTAG', 'HDBS', 'HIGG', 'HION', 'JETM', 'LCALO', 'LLP', 'LLJ', 'MUON', 'NCB', 'PHYS',
                'STDM', 'SUSY', 'TAUP', 'TCAL', 'TLA',  'TOPQ', 'TRIG', 'TRUTH']

CP_FORMATS = ["FTAG", "EGAM", "MUON", "JETM", "TAUP", "IDTR", "TCAL"]


def get_all_formats(format_base):
    return list(TDataFormat.objects.filter(name__startswith='DAOD_' + format_base).values_list('name', flat=True))


LIFE_TIME_DAYS = 60

def collect_stats(format_base, is_real_data):
    formats = get_all_formats(format_base)
    version = 1
    if format_base in CP_FORMATS:
        version = 2
    if is_real_data:
        data_prefix = 'data'
    else:
        data_prefix = 'mc'
    for output_format in formats:
        to_cache = get_stats_per_format(output_format, version, is_real_data)
        result = []
        for ami_tag in to_cache.keys():
            if to_cache[ami_tag]:
                ami_tag_info = GroupProductionAMITag.objects.get(ami_tag=ami_tag)
                skim='noskim'
                if ami_tag_info.skim == 's':
                    skim='skim'
                result.append({'ami_tag':ami_tag,'cache':','.join([ami_tag_info.cache,skim]),
                                                    'containers':to_cache[ami_tag]})

        cache.delete('gp_del_%s_%s_'%(data_prefix,output_format))
        if result:
            cache.set('gp_del_%s_%s_'%(data_prefix,output_format),result,None)


def get_stats_per_format(output_format, version, is_real_data):
    by_tag_stats = {}
    to_cache = {}
    if is_real_data:
        data_prefix = 'data'
    else:
        data_prefix = 'mc'
    samples = GroupProductionDeletion.objects.filter(output_format=output_format)
    for sample in samples:
        if sample.container.startswith(data_prefix):
            if sample.ami_tag not in by_tag_stats:
                by_tag_stats[sample.ami_tag] = {'containers': 0, 'bytes': 0, 'to_delete_containers': 0, 'to_delete_bytes':0}
                to_cache[sample.ami_tag] = []
            if sample.version >= version:
                to_cache[sample.ami_tag].append(GroupProductionDeletionUserSerializer(sample).data)
                by_tag_stats[sample.ami_tag]['containers'] += 1
                by_tag_stats[sample.ami_tag]['bytes'] += sample.size
                if sample.days_to_delete <0:
                    by_tag_stats[sample.ami_tag]['to_delete_containers'] += 1
                    by_tag_stats[sample.ami_tag]['to_delete_bytes'] += sample.size
    current_stats = GroupProductionStats.objects.filter(output_format=output_format, real_data=is_real_data)
    updated_tags = []
    for current_stat in current_stats:
        if current_stat.ami_tag in by_tag_stats.keys():
            current_stat.size = by_tag_stats[current_stat.ami_tag]['bytes']
            current_stat.containers = by_tag_stats[current_stat.ami_tag]['containers']
            current_stat.to_delete_size = by_tag_stats[current_stat.ami_tag]['to_delete_bytes']
            current_stat.to_delete_containers = by_tag_stats[current_stat.ami_tag]['to_delete_containers']
            current_stat.save()
            updated_tags.append(current_stat.ami_tag)
        else:
            current_stat.size = 0
            current_stat.containers = 0
            current_stat.to_delete_size = 0
            current_stat.to_delete_containers = 0
            current_stat.save()
    for tag in by_tag_stats.keys():
        if tag not in updated_tags:
            current_stat, is_created = GroupProductionStats.objects.get_or_create(ami_tag=tag, output_format=output_format, real_data=is_real_data)
            current_stat.size = by_tag_stats[tag]['bytes']
            current_stat.containers = by_tag_stats[tag]['containers']
            current_stat.to_delete_size = by_tag_stats[tag]['to_delete_bytes']
            current_stat.to_delete_containers = by_tag_stats[tag]['to_delete_containers']
            current_stat.save()
    return to_cache

def apply_extension(container, number_of_extension, user, message):
    container = container[container.find(':')+1:]
    gp = GroupProductionDeletion.objects.get(container=container)
    gp_extension = GroupProductionDeletionExtension()
    gp_extension.container = gp
    gp_extension.user = user
    gp_extension.timestamp = timezone.now()
    gp_extension.message = message
    gp_extension.save()
    if (number_of_extension > 0) and (gp.days_to_delete < 0):
        number_of_extension += (gp.days_to_delete // GroupProductionDeletion.EXTENSIONS_DAYS) * -1
    if gp.days_to_delete + (number_of_extension * GroupProductionDeletion.EXTENSIONS_DAYS) > 365:
        number_of_extension = (gp.days_to_delete // GroupProductionDeletion.EXTENSIONS_DAYS) * -1 + 6
    if gp.extensions_number:
        gp.extensions_number += number_of_extension
    else:
        gp.extensions_number = number_of_extension
    if gp.extensions_number < 0:
        gp.extensions_number = 0
    gp.save()
    _logger.info(
        'GP extension by {user} for {container} on {number_of_extension} with messsage {message}'.format(user=user, container=container,
                                                                                   number_of_extension=number_of_extension,message=message))
    _jsonLogger.info('Request for derivation container extension for: {message}'.format(message=message), extra={'user':user,'container':container,'number_of_extension':number_of_extension})


def remove_extension(container):
    gp = GroupProductionDeletion.objects.get(container=container)
    gp.extensions_number = 0
    gp.save()

def form_gp_from_dataset(dataset):
    gp = GroupProductionDeletion()
    dataset = dataset[dataset.find(':')+1:]
    container_name = get_container_name(dataset)
    ami_tag = container_name.split('_')[-1]
    if not GroupProductionAMITag.objects.filter(ami_tag=ami_tag).exists():
        update_tag_from_ami(ami_tag, gp.container.startswith('data'))
    gp.skim = GroupProductionAMITag.objects.get(ami_tag=ami_tag).skim
    gp.container = container_name
    gp.dsid = container_name.split('.')[1]
    gp.output_format = container_name.split('.')[4]
    if gp.container.startswith('data'):
        key_postfix = container_name.split('.')[2]
    else:
        key_postfix = 'mc'
    gp.input_key = '.'.join([str(gp.dsid), gp.output_format, '_'.join(container_name.split('.')[-1].split('_')[:-1]), gp.skim,key_postfix])
    gp.ami_tag = ami_tag
    gp.version = 0
    return gp

def get_existing_datastes(output, ami_tag, ddm):
    tasks = es_by_keys_nested({'ctag': ami_tag, 'output_formats': output})
    if(len(tasks)>0):
        print(ami_tag, len(tasks))
    result = []
    for task in tasks:
        if 'valid' not in task['taskname'] and task['status'] not in ProductionTask.RED_STATUS:
            if not task['output_dataset'] and task['status'] in ProductionTask.NOT_RUNNING:
                datasets = ProductionDataset.objects.filter(task_id=task['taskid'])
                for dataset in datasets:
                    if output in dataset.name:
                        if ddm.dataset_exists(dataset.name):
                            metadata = ddm.dataset_metadata(dataset.name)
                            events = metadata['events']
                            bytes = metadata['bytes']
                            if bytes is None:
                                break
                            result.append({'task': task['taskid'], 'dataset': dataset.name, 'size': bytes,
                                           'task_status': task['status'], 'events': events, 'end_time': task['task_timestamp']})
                        break
            for dataset in task['output_dataset']:
                deleted = False
                try:
                    deleted = dataset['deleted']
                except:
                    print('no deleted', task['taskid'])
                if output == dataset['data_format'] and not deleted and ddm.dataset_exists(dataset['name']):
                    if ('events' not in dataset) or (not dataset['events']):
                        print('no events', task['taskid'])
                        metadata = ddm.dataset_metadata(dataset['name'])
                        events = metadata['events']
                        if not events:
                            events = 0
                        if ('bytes' not in dataset) or dataset['bytes'] == 0:
                            dataset['bytes'] = metadata['bytes']
                    else:
                        events = dataset['events']
                    if task['status'] not in ProductionTask.NOT_RUNNING:
                        production_task = ProductionTask.objects.get(id=int(task['taskid']))
                        if production_task.status != task['status']:
                            print('wrong status', task['taskid'])
                            task['status'] = production_task.status
                    if dataset['bytes'] is None:
                        break
                    result.append({'task': task['taskid'], 'dataset': dataset['name'], 'size': dataset['bytes'],
                                   'task_status': task['status'], 'events': events, 'end_time': task['task_timestamp']})
                    break
    return result



def ami_tags_reduction_w_data(postfix, data=False):
    if 'tid' in postfix:
        postfix = postfix[:postfix.find('_tid')]
    if data:
        return postfix
    new_postfix = []
    first_letter = ''
    for token in postfix.split('_')[:-1]:
        if token[0] != first_letter and not (token[0] == 's' and first_letter == 'a'):
            new_postfix.append(token)
        first_letter = token[0]
    new_postfix.append(postfix.split('_')[-1])
    return '_'.join(new_postfix)

def get_container_name(dataset_name):
    return '.'.join(dataset_name.split('.')[:-1] + [ami_tags_reduction_w_data(dataset_name.split('.')[-1], dataset_name.startswith('data') or ('TRUTH' in dataset_name) )])


def collect_datasets(format_base, data, only_new = False):
    if data:
        prefix = 'data'
    else:
        prefix = 'mc'
    for output in get_all_formats(format_base):
        if only_new:
            if GroupProductionDeletion.objects.filter(output_format=output, container__startswith=prefix).exists():
                continue
        if data:
            fill_db(output, True, True, False)
        else:
            fill_db(output, False, True, False)
            fill_db(output, False, False, False)
    collect_stats(format_base, data)
    return True

def collect_datasets_per_output(output, data, is_skim):

    if is_skim:
        skim = 's'
    else:
        skim = 'n'
    _logger.info(
        'Start collecting containers for {output} {skim}) '.format(output=output, skim=skim))
    ami_tags_cache = list(
        GroupProductionAMITag.objects.filter(real_data=data, skim=skim).values_list('ami_tag', 'cache'))
    ami_tags_cache = list(filter(lambda x: x[1].split('.')[2].isnumeric(), ami_tags_cache))
    ami_tags_cache.sort(reverse=True, key=lambda x: list(map(int, x[1].split('.'))))
    ami_tags = [x[0] for x in ami_tags_cache]
    result = {}
    ddm = DDM()
    for ami_tag in ami_tags:
        for dataset in get_existing_datastes(output, ami_tag, ddm):
            dataset_name = get_container_name(dataset['dataset'])
            dataset_key = dataset_name[:dataset_name.rfind('_')] + '.' + skim
            if dataset_key not in result:
                result[dataset_key] = {'versions': -1}
            if ami_tag not in result[dataset_key]:
                result[dataset_key]['versions'] += 1
                result[dataset_key][ami_tag] = {'datasets': [], 'size': 0, 'events': 0, 'status': 'finished',
                                                'end_time': None, 'version': result[dataset_key]['versions']}
            if dataset['end_time']:
                if not result[dataset_key][ami_tag]['end_time'] or (
                        dataset['end_time'] > result[dataset_key][ami_tag]['end_time']):
                    result[dataset_key][ami_tag]['end_time'] = dataset['end_time']
            result[dataset_key][ami_tag]['datasets'].append(dataset)
            result[dataset_key][ami_tag]['size'] += dataset['size']
            result[dataset_key][ami_tag]['events'] += dataset['events']
            if dataset['task_status'] not in ProductionTask.NOT_RUNNING:
                result[dataset_key][ami_tag]['status'] = 'running'
    return result

def create_single_tag_container(container_name):
    container_name = container_name[container_name.find(':')+1:]
    gp_container =  GroupProductionDeletion.objects.get(container=container_name)
    ddm = DDM()
    if not ddm.dataset_exists(container_name):
        datasets = datassets_from_es(gp_container.ami_tag, gp_container.output_format, gp_container.dsid,container_name,ddm)
        if datasets:
            empty_replica = True
            for es_dataset in datasets:
                if len(ddm.dataset_replicas(es_dataset))>0:
                    empty_replica = False
                    break
            if not empty_replica:
                print(str(datasets),' will be added to ',container_name)
                ddm.register_container(container_name,datasets)

def range_containers(container_key):
    gp_containers = GroupProductionDeletion.objects.filter(input_key=container_key)
    if gp_containers.count() > 1:
        by_amitag = {}
        for gp_container in gp_containers:
            by_amitag[gp_container.ami_tag] = gp_container
        ami_tags_cache = [(x, GroupProductionAMITag.objects.get(ami_tag=x).cache) for x in by_amitag.keys()]
        ami_tags_cache.sort(reverse=True, key=lambda x: list(map(int, x[1].split('.'))))
        ami_tags = [x[0] for x in ami_tags_cache]
        available_tags = ','.join(ami_tags)
        latest = by_amitag[ami_tags[0]]
        version = 0
        if latest.version !=0 or latest.available_tags != available_tags:
            latest.version = 0
            latest.last_extension_time = None
            latest.available_tags = available_tags
            latest.save()
        for ami_tag in ami_tags[1:]:
            if latest.status == 'finished':
                version += 1
            last_extension = max([latest.update_time,by_amitag[ami_tag].update_time])
            if version != by_amitag[ami_tag].version or by_amitag[ami_tag].available_tags != available_tags or by_amitag[ami_tag].last_extension_time!=last_extension:
                by_amitag[ami_tag].last_extension_time = last_extension
                by_amitag[ami_tag].version = version
                by_amitag[ami_tag].available_tags = available_tags
                by_amitag[ami_tag].save()
            latest = by_amitag[ami_tag]
    else:
        gp_container = GroupProductionDeletion.objects.get(input_key=container_key)
        if gp_container.version != 0 or gp_container.available_tags:
            gp_container.version = 0
            gp_container.last_extension_time = None
            gp_container.available_tags = None
            gp_container.save()


def unify_dataset(dataset):
    if(':' in dataset):
        return dataset
    else:
        return dataset.split('.')[0]+':'+dataset

def check_container(container_name, ddm, additional_datasets = None, warning_exists = False):
    container_name = container_name[container_name.find(':')+1:]
    if GroupProductionDeletion.objects.filter(container=container_name).count() >1:
        gp_to_delete =  list(GroupProductionDeletion.objects.filter(container=container_name))
        for gp in gp_to_delete:
            gp.delete()
    if GroupProductionDeletion.objects.filter(container=container_name).exists():
        gp_container =  GroupProductionDeletion.objects.get(container=container_name)
        is_new = False
    else:
        gp_container = form_gp_from_dataset(additional_datasets[0])
        is_new = True
    container_key = gp_container.input_key
    datasets = ddm.dataset_in_container(container_name)
    if additional_datasets:
        for dataset in additional_datasets:
            if dataset not in datasets:

                datasets.append(unify_dataset(dataset))
    events = 0
    bytes = 0
    is_running = False
    datasets += datassets_from_es(gp_container.ami_tag, gp_container.output_format, gp_container.dsid,container_name,ddm,datasets)
    if datasets:
        if warning_exists:
            _logger.warning(
                'Container {container} has datasets which were not found from ES '.format(container=container_name))
            print('Container {container} has datasets which were not found from ES '.format(container=container_name))
        for dataset in datasets:
            metadata = ddm.dataset_metadata(dataset)
            if metadata['events']:
                events += metadata['events']
            if metadata['bytes']:
                bytes += metadata['bytes']
            task_id = metadata['task_id']
            task = ProductionTask.objects.get(id=task_id)
            if task.status not in ProductionTask.NOT_RUNNING:
                is_running = True
        gp_container.events = events
        gp_container.datasets_number = len(datasets)
        gp_container.size = bytes
        if is_running:
            gp_container.status = 'running'
            gp_container.update_time = timezone.now()
        else:
            gp_container.status = 'finished'
            if is_new:
                gp_container.update_time = timezone.now()
                _logger.info(
                    'Container {container} has been added to group production lists '.format(
                        container=gp_container.container))
        gp_container.save()
        range_containers(container_key)
    else:
        _logger.info(
            'Container {container} has been deleted from group production lists '.format(container=container_name))
        rerange_after_deletion(gp_container)

def store_dataset(item):
    for x in ['update_time', 'last_extension_time']:
        if item.get(x):
            item[x] = datetime.strptime(item[x], "%d-%m-%Y %H:%M:%S").replace(tzinfo=pytz.utc)
    gp_container = GroupProductionDeletion(**item)
    if GroupProductionDeletion.objects.filter(container=item['container']).exists():
        gp_container.id = GroupProductionDeletion.objects.get(container=item['container']).id
    gp_container.save()
    return gp_container.id

def do_gp_deletion_update():
    update_for_period(timezone.now()-timedelta(days=2), timezone.now()+timedelta(hours=3))
    cache.set('gp_deletion_update_time',timezone.now(),None)
    for f in FORMAT_BASES:
        collect_stats(f,False)
        collect_stats(f,True)


def update_for_period(time_since, time_till):
    tasks = ProductionTask.objects.filter(timestamp__gte=time_since, timestamp__lte=time_till, provenance='GP')
    containers = {}
    for task in tasks:
        if (task.phys_group not in ['SOFT','VALI']) and ('valid' not in task.name) and (task.status in ['finished','done']):
            datasets = ProductionDataset.objects.filter(task_id=task.id)
            for dataset in datasets:
                if '.log.' not in dataset.name:
                    container_name = get_container_name(unify_dataset(dataset.name))
                    if container_name not in containers:
                        containers[container_name] = []
                    containers[container_name].append(unify_dataset(dataset.name))
    ddm = DDM()
    for container, datasets in containers.items():
        try:
            check_container(container,ddm,datasets)
        except Exception as e:
            _logger.error("problem during gp container check %s" % str(e))
    return True

def redo_all(is_data, exclude_list):
    for base in FORMAT_BASES:
        if base not in exclude_list:
            redo_whole_output(base, is_data)

def redo_whole_output(format_base, is_data):
    if is_data:
        prefix = 'data'
    else:
        prefix = 'mc'
    for output in get_all_formats(format_base):
        _logger.info(
            'Redo {output} for {prefix} '.format(output=output,prefix=prefix))
        print('Redo {output} for {prefix} '.format(output=output,prefix=prefix))
        group_containers = list(GroupProductionDeletion.objects.filter(output_format=output, container__startswith=prefix))
        for group_container in group_containers:
            group_container.previous_container = None
            group_container.save()
            group_container.delete()
        if is_data:
            fill_db(output, True, True, False)
        else:
            fill_db(output, False, True, False)
            fill_db(output, False, False, False)


def redo_format(output, is_data):
    if is_data:
        prefix = 'data'
    else:
        prefix = 'mc'
    _logger.info(
        'Redo {output} for {prefix} '.format(output=output,prefix=prefix))
    print('Redo {output} for {prefix} '.format(output=output,prefix=prefix))
    group_containers = list(GroupProductionDeletion.objects.filter(output_format=output, container__startswith=prefix))
    for group_container in group_containers:
        group_container.previous_container = None
        group_container.save()
        group_container.delete()
    if is_data:
        fill_db(output, True, True, False)
    else:
        fill_db(output, False, True, False)
        fill_db(output, False, False, False)

def datassets_from_es(ami_tag, output_formats, run_number, container, ddm, checked_datasets = []):
    tasks = es_by_keys_nested({'ctag': ami_tag, 'output_formats': output_formats,
                        'run_number': run_number})
    es_datatses = []
    for task in tasks:
        if task['status'] not in ProductionTask.RED_STATUS:
            for dataset in task['output_dataset']:
                deleted = False
                try:
                    deleted = dataset['deleted']
                except:
                    _logger.warning('task {taskid} has no deleted in es'.format(taskid=task['taskid']))
                if (unify_dataset(dataset['name']) not in checked_datasets) and (
                        output_formats in dataset['data_format'] and not deleted) and \
                        (get_container_name(dataset[
                                                'name']) == container) and ddm.dataset_exists(
                    dataset['name']):
                    es_datatses.append(dataset['name'])
    return es_datatses


def rerange_after_deletion(gp_delete_container):
    gp_containers = GroupProductionDeletion.objects.filter(input_key=gp_delete_container.input_key)
    if gp_containers.count() > 1:
        by_amitag = {}
        for gp_container in gp_containers:
            if gp_container != gp_delete_container:
                by_amitag[gp_container.ami_tag] = gp_container
        if len(by_amitag.keys()) == 1:
            ami_tag, gp_container = by_amitag.popitem()
            gp_container.available_tags = gp_container.ami_tag
            gp_container.version = 0
            gp_container.save()
        else:
            ami_tags_cache = [(x, GroupProductionAMITag.objects.get(ami_tag=x).cache.split('-')[0]) for x in by_amitag.keys()]
            ami_tags_cache.sort(reverse=True, key=lambda x: list(map(int, x[1].split('.'))))
            ami_tags = [x[0] for x in ami_tags_cache]
            available_tags = ','.join(ami_tags)
            latest = by_amitag[ami_tags[0]]
            version = 0
            if latest.version !=0 or latest.available_tags != available_tags:
                latest.version = 0
                latest.last_extension_time = None
                latest.available_tags = available_tags
                latest.save()
            for ami_tag in ami_tags[1:]:
                if latest.status == 'finished':
                    version += 1
                last_extension = max([latest.update_time,by_amitag[ami_tag].update_time])
                if version != by_amitag[ami_tag].version or by_amitag[ami_tag].available_tags != available_tags or by_amitag[ami_tag].last_extension_time!=last_extension:
                    by_amitag[ami_tag].last_extension_time = last_extension
                    by_amitag[ami_tag].version = version
                    by_amitag[ami_tag].previous_container = None
                    by_amitag[ami_tag].available_tags = available_tags
                    by_amitag[ami_tag].save()
                latest = by_amitag[ami_tag]
    gp_extensions = GroupProductionDeletionExtension.objects.filter(container=gp_delete_container)
    for gp_extension in gp_extensions:
        gp_extension.delete()
    gp_delete_container.delete()

def fix_update_time(container):
    gp_container = GroupProductionDeletion.objects.get(container=container)
    ddm = DDM()
    gp_container.update_time = ddm.dataset_metadata(container)['updated_at']
    gp_container.save()

def clean_superceeded(do_es_check=True, full=False, format_base = None):
    # for base_format in FORMAT_BASES:
    ddm = DDM()
    if not format_base:
        format_base = FORMAT_BASES
        cache_key = 'ALL'
    else:
        if format_base in FORMAT_BASES:
            cache_key = format_base
            format_base = [format_base]
        else:
            return False
    existed_datasets = []
    for base_format in format_base:
        superceed_version = 1
        if base_format in CP_FORMATS:
            superceed_version = 2
        formats = get_all_formats(base_format)
        for output_format in formats:
            if full:
                existed_containers = GroupProductionDeletion.objects.filter(output_format=output_format)
            else:
                existed_containers = GroupProductionDeletion.objects.filter(output_format=output_format, version__gte=superceed_version)
            for gp_container in existed_containers:
                container_name = gp_container.container
                datasets = ddm.dataset_in_container(container_name)
                delete_container = False
                if len(datasets) == 0:
                    delete_container = True
                    if do_es_check:
                        es_datasets = datassets_from_es(gp_container.ami_tag, gp_container.output_format, gp_container.dsid, gp_container.container, ddm )
                        empty_replica = True
                        if('TRUTH' not in output_format):
                            for es_dataset in es_datasets:
                                if len(ddm.dataset_replicas(es_dataset))>0:
                                    empty_replica = False
                                    break
                        else:
                            empty_replica = False
                        if len(es_datasets) > 0 and not empty_replica:
                            delete_container = False
                            if gp_container.days_to_delete <0 and (gp_container.version >= version_from_format(gp_container.output_format)):
                                existed_datasets += es_datasets
                            if gp_container.version != 0:
                                _logger.error('{container} is empty but something is found'.format(container=container_name))
                else:
                    if (gp_container.days_to_delete < 0) and (gp_container.version >= version_from_format(gp_container.output_format)):
                        existed_datasets += datasets
                if delete_container:
                    try:
                        rerange_after_deletion(gp_container)
                        _logger.info(
                            'Container {container} has been deleted from group production lists '.format(
                                container=container_name))
                    except Exception as e:
                        _logger.error('Container {container} has problem during deletion from group production lists '.format(
                            container=container_name))

    cache.set('dataset_to_delete_'+cache_key,existed_datasets,None)


def clean_containers(changed_containers, output, data, is_skim):
    if is_skim:
        skim = 's'
    else:
        skim = 'n'
    existed_containers = list(GroupProductionDeletion.objects.filter(output_format=output, skim=skim).values_list('container',flat=True))
    ddm=DDM()
    for gp_container in existed_containers:
        if (data and not(gp_container.startswith('data'))) or ((not data) and  gp_container.startswith('data')):
            continue
        if gp_container not in changed_containers:
            check_container(gp_container, ddm, warning_exists=True)


def fill_db(output, data, is_skim, test=True):
    results = collect_datasets_per_output(output, data, is_skim)
    to_db = []
    for sample_key, samples_collection in results.items():
        samples = []
        for ami_tag, sample in samples_collection.items():
            if ami_tag != 'versions':
                sample.update({'ami_tag': ami_tag})
                samples.append(sample)
        samples.sort(key=lambda x: x['version'])
        superceed_time = None
        version = 0
        db_sample_collection = []
        existed_ami_tags = []
        for index, sample in enumerate(samples):
            db_sample = {}
            for x in ['size', 'events', 'status', 'ami_tag']:
                db_sample[x] = sample[x]
            db_sample['datasets_number'] = len(sample['datasets'])
            db_sample['version'] = version
            if superceed_time:
                db_sample['last_extension_time'] = superceed_time
            if db_sample['status'] == 'finished':
                superceed_time = sample['end_time']
                existed_ami_tags.append(sample['ami_tag'])
                version += 1
            elif index > 0:
                db_sample['status'] = 'alarm'
            db_sample['update_time'] = sample['end_time']
            db_sample['container'] = '.'.join(sample_key.split('.')[:-1]) + '_' + sample['ami_tag']
            db_sample['dsid'] = sample_key.split('.')[1]
            db_sample['output_format'] = sample_key.split('.')[4]
            db_sample['skim'] = sample_key.split('.')[-1]
            if data:
                key_postfix = sample_key.split('.')[2]
            else:
                key_postfix = 'mc'
            db_sample['input_key'] = '.'.join([str(db_sample['dsid']), db_sample['output_format'],
                                               sample_key.split('.')[-2], db_sample['skim'],key_postfix])
            db_sample_collection.append(db_sample)
        available_tags = ','.join(existed_ami_tags)
        for db_sample in db_sample_collection:
            if db_sample['version'] >= 1:
                db_sample['available_tags'] = available_tags

        to_db += db_sample_collection
    if test:
        to_db.reverse()
        return to_db
    else:
        to_db.reverse()
        current_key = None
        last_id = None
        changed_containers = []
        _logger.info('Store {total} to DB '.format(total=len(to_db)))
        for index, item in enumerate(to_db):
            try:
                if (item['input_key'] == current_key) and last_id:
                    last_id = store_dataset(item)
                else:
                    last_id = store_dataset(item)
                current_key = item['input_key']
                changed_containers.append(item['container'])
            except Exception as e:
                _logger.error('Error during storing container {error} to DB '.format(error=str(e)))
                print(index)
                return to_db
        clean_containers(changed_containers, output, data, is_skim )
        return to_db


def update_tag_from_ami(tag, is_data=False):
    ami = AMIClient()
    gp_tag = GroupProductionAMITag()
    ami_tag = ami.get_ami_tag(tag)
    gp_tag.cache = ami_tag['cacheName']
    if 'passThrough' in ami_tag:
        gp_tag.skim = 'n'
    else:
        gp_tag.skim = 's'
    gp_tag.ami_tag = tag
    gp_tag.real_data = is_data
    gp_tag.save()


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def gpdetails(request):
    try:
        current_id = request.query_params.get('gp_id')
        gp_container = GroupProductionDeletion.objects.get(id=current_id)
        gp_containers = list(GroupProductionDeletion.objects.filter(input_key=gp_container.input_key))
        containers = [x.data for x in [GroupProductionDeletionSerializer(y) for y in gp_containers]]
        return Response({'id': current_id, 'containers': containers})
    except Exception as e:
        return HttpResponseBadRequest(e)


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def ami_tags_details(request):
    try:
        ami_tags = request.query_params.get('ami_tags').split(',')
        result = {}
        for ami_tag in ami_tags:
            if GroupProductionAMITag.objects.filter(ami_tag=ami_tag).exists():
                ami_tag_details = GroupProductionAMITag.objects.get(ami_tag=ami_tag)
                result.update({ami_tag_details.ami_tag:{'cache': ami_tag_details.cache, 'skim':  ami_tag_details.skim}})
        return Response(result)
    except Exception as e:
        return HttpResponseBadRequest(e)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def gp_container_details(request):
    try:
        result = {}
        container_name = request.query_params.get('container')
        if not GroupProductionDeletion.objects.filter(container=container_name).exists():
            return Response(None)
        ddm = DDM()
        gp_main_container = GroupProductionDeletion.objects.get(container=container_name)
        extensions = GroupProductionDeletionExtension.objects.filter(container=gp_main_container).order_by('id')
        result['extension'] = [ GroupProductionDeletionExtensionSerializer(x).data for x in extensions]
        gp_same_key_containers = GroupProductionDeletion.objects.filter(input_key=gp_main_container.input_key)
        same_key_containers = []
        for gp_container in gp_same_key_containers:
            datasets = ddm.dataset_in_container(gp_container.container)
            datasets += datassets_from_es(gp_container.ami_tag, gp_container.output_format, gp_container.dsid, gp_container.container, ddm,datasets )
            datasets_info = []
            for dataset in datasets:
                metadata = ddm.dataset_metadata(dataset)
                datasets_info.append( {'name':dataset,'events':metadata['events'],'bytes':metadata['bytes'],'task_id':metadata['task_id'] })
            if gp_container == gp_main_container:
                result['main_container'] = {'container': gp_container.container, 'datasets':datasets_info,
                                            'details': GroupProductionDeletionSerializer(gp_container).data}
            else:
                same_key_containers.append({'container': gp_container.container, 'datasets':datasets_info,
                                            'details': GroupProductionDeletionSerializer(gp_container).data})
        result['same_input'] = same_key_containers
        return Response(result)
    except Exception as e:
        return HttpResponseBadRequest(e)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def extension(request):
    try:
        username = request.user.username
        containers = request.data['containers']
        message = request.data['message']
        number_of_extensions = request.data['number_of_extensions']
        for container in containers:
            apply_extension(container['container'],number_of_extensions,username,message)
    except Exception as e:

        return HttpResponseBadRequest(e)
    return Response({'message': 'OK'})

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def extension_api(request):
    """
    Increase by "number_of_extensions"  for each container in "containers" list with "message"
    Post data must contain two fields message and containers, e.g.:
    {"message":"Test","containers":['container1','container2']}\n
    :return is {'containers_extented': number of containers extented,'containers_with_problems': list of containers with problems}
"""
    containers_extended = 0
    containers_with_problems = []
    try:
        username = request.user.username
        containers = request.data['containers']
        message = request.data['message']
        number_of_extensions = request.data.get('number_of_extensions',1)
        for container in containers:
            try:
                apply_extension(container,number_of_extensions,username,message)
                containers_extended += 1
            except Exception as e:
                containers_with_problems.append((container, str(e)))
    except Exception as e:
        return HttpResponseBadRequest(e)
    return Response({'containers_extented': containers_extended,'containers_with_problems': containers_with_problems})

@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def extension_container_api(request):
    """
    Increase by "number_of_extensions"  for each container in "period_container"  with "message"
    Post data must contain two fields message and period_container, e.g.:
    {"message":"Test","period_container":'container'}\n
    :return is {'containers_extented': number of containers extented,'containers_with_problems': list of containers with problems}
"""
    containers_extended = 0
    containers_with_problems = []
    try:
        username = request.user.username
        container = request.data['period_container']
        message = request.data['message']
        number_of_extensions = request.data.get('number_of_extensions',1)
        ddm = DDM()
        datasets = ddm.dataset_in_container(container)
        containers = list(set(map(get_container_name,datasets)))
        for container in containers:
            try:
                apply_extension(container,number_of_extensions,username,message)
                containers_extended += 1
            except Exception as e:
                containers_with_problems.append((container, str(e)))
    except Exception as e:
        return HttpResponseBadRequest(e)
    return Response({'containers_extented': containers_extended,'containers_with_problems': containers_with_problems})


class UnixEpochDateField(serializers.DateTimeField):
    def to_representation(self, value):
        """ Return epoch time for a datetime object or ``None``"""
        import time
        try:
            return int(time.mktime(value.timetuple()))
        except (AttributeError, TypeError):
            return None

    def to_internal_value(self, value):
        import datetime
        return datetime.datetime.fromtimestamp(int(value))

class GroupProductionDeletionExtensionSerializer(serializers.ModelSerializer):

    class Meta:
        model = GroupProductionDeletionExtension
        fields = '__all__'

class GroupProductionDeletionSerializer(serializers.ModelSerializer):
    epoch_last_update_time = UnixEpochDateField(source='last_extension_time')

    class Meta:
        model = GroupProductionDeletion
        fields = '__all__'

class GroupProductionDeletionUserSerializer(serializers.ModelSerializer):
    epoch_last_update_time = UnixEpochDateField(source='last_extension_time')

    class Meta:
        model = GroupProductionDeletion
        fields = ['container','events','available_tags','version','extensions_number','size','epoch_last_update_time','days_to_delete']

class GroupProductionStatsSerializer(serializers.ModelSerializer):
    class Meta:
        model = GroupProductionStats
        fields = '__all__'

class ListGroupProductionStatsView(generics.ListAPIView):
    serializer_class = GroupProductionStatsSerializer
    lookup_fields = ['id', 'ami_tag', 'output_format', 'real_data']

    def get_queryset(self):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        filter = {}
        for field in self.lookup_fields:
            if field == 'real_data' and self.request.query_params.get(field, None):
                if self.request.query_params[field] == '1':
                    filter['real_data'] = True
                else:
                    filter['real_data'] = False
            elif self.request.query_params.get(field, None):  # Ignore empty fields.
                filter[field] = self.request.query_params[field]
        queryset = GroupProductionStats.objects.filter(**filter)
        return queryset

def version_from_format(output_format):
    for base_format in CP_FORMATS:
        if base_format in output_format:
            return 2
    return 1


@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def last_update_time_group_production(request):
    return Response(cache.get('gp_deletion_update_time', timezone.now()).ctime())

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def group_production_datasets_full(request):
    """
        Return the list of containers from cache. If no output_format or base_format are set it returns all containers for  \n
        the data_type. \n
        * output_format: Output format. Example: "DAOD_BPHY1". \n
        * base_format: Base format. Example: "BPHY". \n
        * data_type: 'mc' or 'data', default is 'mc'. Example: "data".
    """

    data_prefix = 'mc'
    if request.query_params.get('data_type'):
        data_prefix = request.query_params.get('data_type')
    formats = []
    if request.query_params.get('output_format'):
        formats = [request.query_params.get('output_format')]
    else:
        if request.query_params.get('base_format'):
            formats = get_all_formats(request.query_params.get('base_format'))
        else:
            for output_format in FORMAT_BASES:
                formats += get_all_formats(output_format)

    result = {'timestamp':str(cache.get('gp_deletion_update_time', timezone.now())),'formats':[]}
    for output_format in formats:
        format_data = cache.get('gp_del_%s_%s_'%(data_prefix,output_format), None)
        if format_data:
            result['formats'].append({'output_format':output_format,'data':format_data})
    return Response(result)

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def all_datasests_to_delete(request):
    """
        Return list of all datasets which are marked to deletion. List is taken from the cache, the cache is updated once a day.\n
        * filter: return datasets with 'filter' value in the name. Example: "DAOD_BPHY1" \n
        * data_type: 'mc' or 'data'. Example: "data".
    """
    result = cache.get('dataset_to_delete_ALL')
    if request.query_params.get('data_type'):
        result = [x for x in result if x.startswith(request.query_params.get('data_type'))]
    if request.query_params.get('filter'):
        result = [x for x in result if request.query_params.get('filter') in x]
    return Response(result)


class ListGroupProductionDeletionForUsersView(generics.ListAPIView):
    """
        Return the list of containers for selected output_format and selected data_type\n
        * output_format: Output format. Example: "DAOD_BPHY1". Required\n
        * data_type: 'mc' or 'data'. Example: "data". Requied
    """

    serializer_class = GroupProductionDeletionUserSerializer
    authentication_classes = [TokenAuthentication, SessionAuthentication, BasicAuthentication]
    permission_classes = [IsAuthenticated]
    lookup_fields = [ 'output_format', 'skim', 'ami_tag','data_type']
    fields = ['container']

    def get_queryset(self):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        filter = {}
        if not self.request.query_params.get('output_format', None):
            return []
        for field in self.lookup_fields:
            if field == 'data_type' and self.request.query_params.get(field, None):
                filter['container__startswith'] = self.request.query_params[field]
            elif field == 'output_format' and self.request.query_params.get(field, None) :
                filter['version__gte'] = version_from_format(self.request.query_params[field])
                filter[field] = self.request.query_params[field]
            elif self.request.query_params.get(field, None):  # Ignore empty fields.
                filter[field] = self.request.query_params[field]
        queryset = GroupProductionDeletion.objects.filter(**filter).order_by('-ami_tag','container')

        return queryset


class ListGroupProductionDeletionView(generics.ListAPIView):

    authentication_classes = [TokenAuthentication, SessionAuthentication, BasicAuthentication]
    permission_classes = [IsAuthenticated]

    serializer_class = GroupProductionDeletionSerializer
    lookup_fields = ['dsid', 'output_format', 'version', 'status', 'skim', 'ami_tag','data_type']

    def get_queryset(self):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        filter = {}
        for field in self.lookup_fields:
            if field == 'data_type' and self.request.query_params.get(field, None):
                filter['container__startswith'] = self.request.query_params[field]
            elif field == 'output_format' and self.request.query_params.get(field, None) and not (self.request.query_params.get('version', None)):
                filter['version__gte'] = version_from_format(self.request.query_params[field])
                filter[field] = self.request.query_params[field]
            elif self.request.query_params.get(field, None):  # Ignore empty fields.
                filter[field] = self.request.query_params[field]
        queryset = GroupProductionDeletion.objects.filter(**filter).order_by('-ami_tag','container')

        return queryset


def collect_tags(start_requests):
    requests = TRequest.objects.filter(request_type='GROUP', reqid__gte=start_requests)
    for request in requests:
        if request.phys_group not in ['VALI', 'SOFT']:
            if 'valid' not in str(request.project):
                if ProductionTask.objects.filter(request=request).exists():
                    task = ProductionTask.objects.filter(request=request).last()
                    if not GroupProductionAMITag.objects.filter(ami_tag=task.ami_tag).exists():
                        print(task.ami_tag)
                        update_tag_from_ami(task.ami_tag,task.name.startswith('data'))



@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def set_datasets_to_delete(request):
    try:
        username = request.user.username
        deadline = datetime.strptime(request.data['deadline'],"%Y-%m-%dT%H:%M:%S.%fZ")
        start_deletion = datetime.strptime(request.data['start_deletion'],"%Y-%m-%dT%H:%M:%S.%fZ")
        user = User.objects.get(username=username)
        if not user.is_superuser:
            return Response('Not enough permissions', status.HTTP_401_UNAUTHORIZED)
        last_record = GroupProductionDeletionRequest.objects.last()
        if deadline.replace(tzinfo=pytz.utc) < last_record.start_deletion:
            return Response('Previous deletion is not yet done', status.HTTP_400_BAD_REQUEST)
        new_deletion_request = GroupProductionDeletionRequest()
        new_deletion_request.username = username
        new_deletion_request.status = 'Waiting'
        new_deletion_request.start_deletion = start_deletion
        new_deletion_request.deadline = deadline
        new_deletion_request.save()
        if deadline.replace(tzinfo=pytz.utc) <= timezone.now():
            check_deletion_request()
    except Exception as e:
        return Response('Problem %s'%str(e), status.HTTP_400_BAD_REQUEST)
    return Response(GroupProductionDeletionRequestSerializer(new_deletion_request).data)

@app.task()
def check_deletion_request():
    if not GroupProductionDeletionRequest.objects.filter(status='Waiting').exists():
        return
    deletion_request = GroupProductionDeletionRequest.objects.filter(status='Waiting').last()
    if datetime.now().replace(tzinfo=pytz.utc) >= deletion_request.deadline:
        containers, total_size = find_containers_to_delete(deletion_request.deadline)
        deletion_request.size = total_size
        deletion_request.containers = len(containers)
        for container in containers:
            gp_processing = GroupProductionDeletionProcessing()
            if GroupProductionDeletionProcessing.objects.filter(container=container).exists():
                gp_processing = GroupProductionDeletionProcessing.objects.filter(container=container).last()
            gp_processing.container = container
            gp_processing.status = 'ToDelete'
            gp_processing.save()
        deletion_request.status = 'Submitted'
        deletion_request.save()
        datasets = cache.get('dataset_to_delete_ALL')
        datasets = [x[x.find(':')+1:] for x in datasets]
        cache.set("datasets_to_be_deleted",datasets, None)
        return

@app.task()
def run_deletion():
    if not GroupProductionDeletionRequest.objects.filter(status__in=['Submitted','Executing']).exists():
        return
    if GroupProductionDeletionRequest.objects.filter(status='Submitted').exists():
        deletion_request = GroupProductionDeletionRequest.objects.filter(status='Submitted').last()
        if datetime.now().replace(tzinfo=pytz.utc) >= deletion_request.start_deletion:
            runDeletion.apply_async(countdown=3600)
            deletion_request.status = 'Executing'
            deletion_request.save()
            return
    if GroupProductionDeletionRequest.objects.filter(status='Executing').exists():
        deletion_request = GroupProductionDeletionRequest.objects.filter(status='Executing').last()
        if not GroupProductionDeletionProcessing.objects.filter(status='ToDelete').exists():
            deletion_request.status = 'Done'
            deletion_request.save()
            return
        else:
            runDeletion.apply_async(countdown=3600)

class GroupProductionDeletionRequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = GroupProductionDeletionRequest
        fields = '__all__'

class ListGroupProductionDeletionRequestsView(generics.ListAPIView):

    authentication_classes = [TokenAuthentication, SessionAuthentication, BasicAuthentication]
    permission_classes = [IsAuthenticated]

    serializer_class = GroupProductionDeletionRequestSerializer
    lookup_fields = ['id']

    def get_queryset(self):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        filter = {}
        for field in self.lookup_fields:
            if self.request.query_params.get(field, None):  # Ignore empty fields.
                filter[field] = self.request.query_params[field]
        queryset = GroupProductionDeletionRequest.objects.filter(**filter).order_by('-timestamp')
        return queryset

def find_containers_to_delete(deletion_day, total_containers=None, size=None):
    days_to_delete = (deletion_day - datetime.now().replace(tzinfo=pytz.utc)).days
    container_to_check = GroupProductionDeletion.objects.filter(version__gte=1)
    containers_to_delete = []
    total_size = 0
    for gp_container in container_to_check:
        if ((gp_container.days_to_delete < days_to_delete) and (gp_container.version >= version_from_format(gp_container.output_format)) and
                (not GroupProductionDeletionProcessing.objects.filter(container=gp_container.container, status='ToDelete').exists())):
            containers_to_delete.append(gp_container.container)
            total_size += gp_container.size
            if size and total_size > size:
                break
            if total_containers and len(containers_to_delete)>=total_containers:
                break
    return containers_to_delete, total_size

@app.task(time_limit=10800)
def runDeletion(lifetime=3600):
    containers_to_delete = GroupProductionDeletionProcessing.objects.filter(status='ToDelete')
    all_datasets = cache.get("datasets_to_be_deleted")
    ddm = DDM()
    for container_to_delete in containers_to_delete:
        datasets = ddm.dataset_in_container(container_to_delete.container)
        datasets = [x[x.find(':')+1:] for x in datasets]
        all_marked = True
        deleted_datasets = 0
        for dataset in datasets:
            if dataset not in all_datasets:
                _logger.error('Dataset {dataset} is not marked for deletion'.format(dataset=dataset))
                _jsonLogger.error('Dataset {dataset} is not marked for deletion'.format(dataset=dataset), extra={'dataset':dataset})
                all_marked = False
        if all_marked:
            for dataset in datasets:
                try:
                        _logger.info('{dataset} is about being deleted'.format(dataset=dataset))
                        _jsonLogger.info('{dataset} is about being deleted'.format(dataset=dataset), extra={'dataset':dataset, 'container':container_to_delete.container})
                        ddm.deleteDataset(dataset, lifetime)
                        deleted_datasets += 1
                except Exception as e:
                    _logger.error('Problem with {dataset} deletion error: {error}'.format(dataset=dataset,error=str(e)))
                    _jsonLogger.error('Problem with {dataset} deletion'.format(dataset=dataset), extra={'dataset':dataset, 'container':container_to_delete.container, 'error':str(e)})
        if deleted_datasets > 0:
            container_to_delete.command_timestamp = timezone.now()
            container_to_delete.deleted_datasets = deleted_datasets
        if len(datasets) == deleted_datasets:
            container_to_delete.status = 'Deleted'
            container_to_delete.save()
        else:
            container_to_delete.status = 'Problematic'
            container_to_delete.save()

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def gpdeletedcontainers(request):
    all_containers = list(GroupProductionDeletionProcessing.objects.filter(status='Deleted').order_by("-timestamp").values('container','timestamp','deleted_datasets'))
    return Response(all_containers)


def find_daod_to_save(daod_lifetime_filepath: str, output_daods_file: str, output_daods_file_ext: str):
    """
    Find all DAODs which are not in the list of DAODs to save
    :param daod_lifetime_filepath: path to file with DAODs to save
    :param output_daods_file: path to file with all DAODs
    :return: list of DAODs to delete
    """
    all_daod_datasets_to_delete = []
    dataset_size = {}
    with  gzip.open(daod_lifetime_filepath, 'rt') as f:
        for line in f:
            all_daod_datasets_to_delete.append(line.strip().split(' ')[0])
            if  line.strip().split(' ')[2] != 'None':
                dataset_size[line.strip().split(' ')[0]] = int(line.strip().split(' ')[2])
    all_containers = GroupProductionDeletion.objects.filter(extensions_number__gte=1).values('container',
                                                                                        'extensions_number',
                                                                                        'last_extension_time',
                                                                                                  'update_time',
                                                                                        'id')
    all_containers_dict = {x['container']: x for x in all_containers}
    containers_by_id = {x['id']: x for x in all_containers}
    extensions = GroupProductionDeletionExtension.objects.all().order_by('id').values('container_id', 'user',
                                                                                           'message', 'id')
    extensions_dict = {}
    for extension in extensions:
        extensions_dict[containers_by_id[extension['container_id']]['container']] = extension
    result = []
    size = 0
    for dataset in all_daod_datasets_to_delete:
        container_name = get_container_name(dataset)
        if container_name in all_containers_dict:
            container_info = all_containers_dict[container_name]
            last_time = container_info['last_extension_time']
            if not last_time:
                last_time = container_info['update_time']
            if ((last_time - timezone.now()).days +
                container_info['extensions_number'] * 60 + 60 > 0):
                if dataset in dataset_size:
                    extension = extensions_dict.get(container_name,{'user': 'none', 'message': 'missing'})
                    result.append((dataset,extension['user'],extension['message'].replace('\n',' ')))
                    size += dataset_size[dataset]
    with open(output_daods_file, 'w') as f:
        for dataset in result:
            f.write('%s\n'%(dataset[0]))
    with open(output_daods_file_ext, 'w') as f:
        for dataset in result:
            f.write('%s %s %s\n'%(dataset[0],dataset[1],dataset[2]))
    return size


@dataclass
class ContainerInfo:
    container: str
    period: str
    version: str
    project_year: str
    ami_tag: str
    output_format: str
    output_base: str
    input_datasets: list = field(default_factory=list)

@dataclass
class PreparedContainer:
    output_containers: [str]
    comment: str
    super_tag: str
    name: str
    missing_containers: [str] = field(default_factory=list)
    not_full_containers: [str] = field(default_factory=list)


    def get_name(self):
        period = self.super_tag.split(',')[0]
        postfix = self.super_tag.split(',')[1]
        base = self.output_containers[0].split('.')[0]
        stream = self.output_containers[0].split('.')[2]
        output = self.output_containers[0].split('.')[4]
        self.name = f'{base}.{period}.{stream}.PhysCont.{output}.{postfix}'
        return self.name

    def to_dict(self):
        return {'output_containers': self.output_containers,
                            'comment': self.comment,
                            'super_tag': self.super_tag,
                            'missing_containers': self.missing_containers,
                            'not_full_containers': self.not_full_containers,
                            'name': self.get_name()}

def check_all_output_dataset_exists(input_container_info: ContainerInfo, ddm: DDM) -> ([str], [str], [str]):
    result_containers = []
    missing_containers = []
    missing_events = []
    for dataset in input_container_info.input_datasets:
        input_container_name = dataset
        if '_tid' in dataset:
            input_container_name = dataset.split('_tid')[0]
        base = '.'.join(dataset.split('.')[0:3])
        output_container = input_container_info.output_base.format(base=base, output_format=input_container_info.output_format,
                                                                   input_tags=input_container_name.split('.')[-1])
        if not ddm.dataset_exists(output_container):
            missing_containers.append(output_container)
            continue
        datasets = ddm.dataset_in_container(output_container)
        if not datasets:
            missing_containers.append(output_container)
            continue
        task_id = ddm.dataset_metadata(datasets[0])['task_id']
        if (ProductionTask.objects.get(id=task_id).status != ProductionTask.STATUS.DONE) and ProductionTask.objects.get(id=task_id).total_files_failed > 0:
            missing_events.append(output_container)
            continue
        result_containers.append(output_container)
    return result_containers, missing_containers, missing_events


def prepare_super_container_creation(production_request_id: int) -> ([ContainerInfo], [ContainerInfo]):
    slices = InputRequestList.objects.filter(request=production_request_id)
    input_containers = {}
    ddm = DDM()
    all_year_containers = {}
    for slice in slices:
        if not slice.is_hide:
            step = StepExecution.objects.filter(slice=slice, request=production_request_id).last()
            if ProductionTask.objects.filter(step=step).exists():
                task = ProductionTask.objects.filter(step=step).last()

                container = slice.dataset.strip('/')
                period = container.split('.')[1]
                version = container.split('.')[-1].split('_')[-1]
                project_year = container.split('.')[0].split('_')[0][-2:]
                ami_tag = task.ami_tag
                output_formats = task.output_formats.split('.')
                output_dataset = task.output_non_log_datasets().__next__()
                output_base = "{base}."+output_dataset.split('.')[3]+".{output_format}.{input_tags}_"+ami_tag
                input_datasets = ddm.dataset_in_container(container)
                for output_format in output_formats:
                    key = '_'.join([container, ami_tag, output_format])
                    if key not in input_containers:
                        input_containers[key] = ContainerInfo(container, period, version, project_year, ami_tag,
                                                              output_format, output_base, input_datasets)
                    key = '_'.join(['periodAllYear', ami_tag, output_format])
                    if key not in all_year_containers:
                        all_year_containers[key] = ContainerInfo(container, 'periodAllYear', version, project_year,
                                                              ami_tag,
                                                              output_format, output_base, input_datasets)
                    else:
                        all_year_containers[key].input_datasets += input_datasets
    return input_containers, all_year_containers


def assemble_super_container(containers_info: [ContainerInfo]):
    ddm = DDM()
    result_dict = []
    for input_container_info in containers_info.values():
        result_containers, missing_containers, not_full_containers = check_all_output_dataset_exists(input_container_info, ddm)
        super_tag = f'{input_container_info.period},grp{input_container_info.project_year}_{input_container_info.version}_{input_container_info.ami_tag}'
        comment = f'{input_container_info.output_format} {super_tag}'
        result_dict.append(PreparedContainer(result_containers, comment, super_tag, missing_containers, not_full_containers))

            # {'output_containers': result_containers,
            #                     'comment': comment,
            #                     'super_tag': super_tag})
    return result_dict


def create_super_container(production_request_id: int, use_grl: str) -> [PreparedContainer]:
    containers_info, all_year_containers = prepare_super_container_creation(production_request_id)
    if use_grl != '':
        grl_runs = get_GRL_from_xml(use_grl)
        for container_info in all_year_containers.values():
            datasets = [x for x in container_info.input_datasets if any([str(run) in x for run in grl_runs])]
            container_info.input_datasets = datasets
        return assemble_super_container(all_year_containers)
    else:
        return assemble_super_container(containers_info)



def check_request_for_grl(production_request_id: int, grl_path: str, all_datasets: [str]):
    grl_runs = get_GRL_from_xml(grl_path)
    verified_datasets = []
    missing_runs = []
    for run in grl_runs:
        datasets = [x for x in all_datasets if run in x]
        if not datasets:
            missing_runs += [run]
        else:
            verified_datasets += datasets
    return verified_datasets, missing_runs

def get_GRL_from_xml(file_path: str) -> [int]:
    grl_xml_root = ET.parse(file_path).getroot()
    run_numbers = []
    for run in grl_xml_root.findall('NamedLumiRange/LumiBlockCollection'):
        run_number = int(run.find('Run').text)
        run_numbers.append(int(run_number))
    return run_numbers

@api_view(['GET'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def physics_container_index(request):
    try:
        request_id = int(request.query_params.get('requestID'))
        if request_id < 1000:
            raise TRequest.DoesNotExist
        production_request = TRequest.objects.get(reqid=request_id)
        if production_request.request_type != 'GROUP':
            raise Exception("Request is not GROUP")
        if not production_request.project.project.startswith('data'):
            raise Exception("Request is not data")
        grl = request.query_params.get('grl')
        grl_used = False
        if not grl:
            grl = SystemParametersHandler.get_grl_default_file().file_by_project.get(production_request.project.project, None)
            containers = create_super_container(request_id, '')
        else:
            containers = create_super_container(request_id, grl)
            grl_used = True
        return Response({'containers':[x.to_dict() for x in containers], 'grl':grl, 'grl_used': grl_used})
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication, SessionAuthentication))
@permission_classes((IsAuthenticated,))
def create_physics_container_in_ami(request):
    try:
        containers = request.data['containers']
        # convert from dict to ContainerInfo dataclass
        containers = [PreparedContainer(**x) for x in containers]
        _jsonLogger.info('Creating period containers', extra={'user':request.user.username})
        user = User.objects.get(username=request.user.username)
        if not user.is_superuser:
            return Response('Not enough permissions', status.HTTP_401_UNAUTHORIZED)
        ami = AMIClient()
        ddm = DDM()
        existing_containers = []
        for container in containers:
                if ddm.dataset_exists(container.get_name()) and len(ddm.dataset_in_container(container.get_name()))>0:
                    existing_containers.append(container.get_name())
        if existing_containers:
            raise Exception('Container(s) already exist(s): %s'%existing_containers)
        for container in containers:
            result = ami.create_physics_container(container.super_tag, container.output_containers, container.comment)
            _logger.info(f'Period container created: {container} {str(result)}')
            _jsonLogger.info('Period container created', extra={'user': request.user.username, 'container':container.get_name()})
        return Response([x.get_name() for x in containers])
    except Exception as e:
        return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)