from django.contrib.messages.context_processors import messages
from django.http.response import HttpResponseBadRequest
from rest_framework.generics import get_object_or_404

from atlas.ami.client import AMIClient
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats, TRequest, \
    ProductionDataset, GroupProductionDeletionExtension
from atlas.dkb.views import es_by_fields, es_by_keys, es_by_keys_nested
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

from django.core.cache import cache

_logger = logging.getLogger('prodtaskwebui')


FORMAT_BASES = ['BPHY', 'EGAM', 'EXOT', 'FTAG', 'HDBS', 'HIGG', 'HION', 'JETM', 'LCALO', 'MUON', 'PHYS',
                'STDM', 'SUSY', 'TAUP', 'TCAL', 'TOPQ', 'TRIG', 'TRUTH']

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
        data_postfix = 'data'
    else:
        data_postfix = 'mc'
    for format in formats:
        by_tag_stats = {}
        samples = GroupProductionDeletion.objects.filter(output_format=format)
        samples_list = []
        for sample in samples:
            if GroupProductionAMITag.objects.get(ami_tag=sample.ami_tag).real_data == is_real_data:
                if sample.ami_tag not in by_tag_stats:
                    by_tag_stats[sample.ami_tag] = {'containers': 0, 'bytes': 0, 'to_delete_containers': 0, 'to_delete_bytes':0}
                if sample.version >= version:
                    samples_list.append(GroupProductionDeletionSerializer(sample).data)
                    by_tag_stats[sample.ami_tag]['containers'] += 1
                    by_tag_stats[sample.ami_tag]['bytes'] += sample.size
                    if (timezone.now() - sample.last_extension_time) > timedelta(days=LIFE_TIME_DAYS):
                        by_tag_stats[sample.ami_tag]['to_delete_containers'] += 1
                        by_tag_stats[sample.ami_tag]['to_delete_bytes'] += sample.size
        #cache.set('GPD'+format+data_postfix,samples_list,None)

        current_stats = GroupProductionStats.objects.filter(output_format=format, real_data=is_real_data)
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
                current_stat.save()
        for tag in by_tag_stats.keys():
            if tag not in updated_tags:
                current_stat = GroupProductionStats(ami_tag=tag, output_format=format, real_data=is_real_data)
                current_stat.size = by_tag_stats[tag]['bytes']
                current_stat.containers = by_tag_stats[tag]['containers']
                current_stat.to_delete_size = by_tag_stats[tag]['to_delete_bytes']
                current_stat.to_delete_containers = by_tag_stats[tag]['to_delete_containers']
                current_stat.save()


def apply_extension(container, number_of_extension, user, message):
    gp = GroupProductionDeletion.objects.get(container=container)
    gp_extension = GroupProductionDeletionExtension()
    gp_extension.container = gp.container
    gp_extension.user = user
    gp_extension.timestamp = timezone.now()
    gp_extension.message = message
    gp_extension.save()
    if gp.extensions_number:
        gp.extensions_number += number_of_extension
    else:
        gp.extensions_number = number_of_extension
    gp.update_time = timezone.now()
    gp.save()
    _logger.info(
        'GP extension by {user} for {container} on {number_of_extension} '.format(user=user, container=container,
                                                                                   number_of_extension=number_of_extension))


def remove_extension(container):
    gp = GroupProductionDeletion.objects.get(container=container)
    gp.extensions_number = 0
    gp.update_time = timezone.now()
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
    gp.input_key = '.'.join([str(gp.dsid), gp.output_format, '_'.join(container_name.split('.')[-1].split('_')[:-1]), gp.skim])
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
    return '.'.join(dataset_name.split('.')[:-1] + [ami_tags_reduction_w_data(dataset_name.split('.')[-1], dataset_name.startswith('data'))])


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
        latest = by_amitag[ami_tags[0]]
        version = 0
        for ami_tag in ami_tags[1:]:
            if latest.status == 'finished':
                version += 1
            if version != by_amitag[ami_tag].version:
                by_amitag[ami_tag].last_extension_time  = latest.update_time
                by_amitag[ami_tag].version = version
                by_amitag[ami_tag].save()
            latest = by_amitag[ami_tag]

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
                if (unify_dataset(dataset['name']) not in checked_datasets) and (
                        output_formats in dataset['data_format'] and not dataset[
                    'deleted']) and \
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
            gp_container.version = 0
            gp_container.save()
        else:
            ami_tags_cache = [(x, GroupProductionAMITag.objects.get(ami_tag=x).cache) for x in by_amitag.keys()]
            ami_tags_cache.sort(reverse=True, key=lambda x: list(map(int, x[1].split('.'))))
            ami_tags = [x[0] for x in ami_tags_cache]
            latest = by_amitag[ami_tags[0]]
            version = 0
            for ami_tag in ami_tags[1:]:
                if latest.status == 'finished':
                    version += 1
                if version != by_amitag[ami_tag].version:
                    by_amitag[ami_tag].last_extension_time = latest.update_time
                    by_amitag[ami_tag].version = version
                    by_amitag[ami_tag].save()
                latest = by_amitag[ami_tag]
    gp_delete_container.delete()


def clean_superceeded(do_es_check=True, format_base = None):
    # for base_format in FORMAT_BASES:
    ddm = DDM()
    if not format_base:
        format_base = FORMAT_BASES
        cache_key = 'ALL'
    else:
        if format_base in FORMAT_BASES:
            cache_key = format_base
        else:
            return False
    existed_datasets = []
    for base_format in format_base:
        superceed_version = 1
        if base_format in CP_FORMATS:
            superceed_version = 2
        formats = get_all_formats(base_format)
        for output_format in formats:
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
                            existed_datasets += es_datasets
                            if('TRUTH' not in output_format):
                                _logger.error('{container} is empty but something is found'.format(container=container_name))
                else:
                    existed_datasets += datasets
                if delete_container:
                    rerange_after_deletion(gp_container)
                    _logger.info(
                        'Container {container} has been deleted from group production lists '.format(
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
            db_sample['input_key'] = '.'.join([str(db_sample['dsid']), db_sample['output_format'],
                                               sample_key.split('.')[-2], db_sample['skim']])
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
        fields = ['container','events','available_tags','version','extensions_number','size','epoch_last_update_time']

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
def all_datasests_to_delete(request):
    return Response(cache.get('dataset_to_delete_ALL'))


class ListGroupProductionDeletionForUsersView(generics.ListAPIView):
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
        queryset = GroupProductionDeletion.objects.filter(**filter)

        return queryset


class ListGroupProductionDeletionView(generics.ListAPIView):
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
            elif field == 'version' and self.request.query_params.get(field, None) and self.request.query_params.get('output_format', None):
                filter['version__gte'] = version_from_format(self.request.query_params[field])
            elif self.request.query_params.get(field, None):  # Ignore empty fields.
                filter[field] = self.request.query_params[field]
        queryset = GroupProductionDeletion.objects.filter(**filter)

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