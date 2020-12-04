from django.http.response import HttpResponseBadRequest
from rest_framework.generics import get_object_or_404

from atlas.ami.client import AMIClient
from atlas.prodtask.models import ActionStaging, ActionDefault, DatasetStaging, StepAction, TTask, \
    GroupProductionAMITag, ProductionTask, GroupProductionDeletion, TDataFormat, GroupProductionStats
from atlas.dkb.views import es_by_fields, es_by_keys
from atlas.prodtask.ddm_api import DDM
from datetime import datetime
import pytz
from rest_framework import serializers, generics
from django.forms.models import model_to_dict
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from atlas.settings import defaultDatetimeFormat
import logging
from django.utils import timezone


_logger = logging.getLogger('prodtaskwebui')


FORMAT_BASES = ['BPHY', 'EGAM', 'EXOT', 'FTAG', 'HDBS', 'HIGG', 'HION', 'JETM', 'LCALO', 'MUON', 'PHYS',
                'STDM', 'SUSY', 'TAUP', 'TCAL', 'TOPQ', 'TRIG', 'TRUTH']

CP_FORMATS = ["FTAG", "EGAM", "MUON", "JETM", "TAUP", "IDTR", "TCAL"]


def get_all_formats(format_base):
    return list(TDataFormat.objects.filter(name__startswith='DAOD_' + format_base).values_list('name', flat=True))


def collect_stats(format_base, is_real_data):
    formats = get_all_formats(format_base)
    version = 1
    if format_base in CP_FORMATS:
        version = 2
    for format in formats:
        by_tag_stats = {}
        samples = GroupProductionDeletion.objects.filter(output_format=format)
        for sample in samples:
            if GroupProductionAMITag.objects.get(ami_tag=sample.ami_tag).real_data == is_real_data:
                if sample.ami_tag not in by_tag_stats:
                    by_tag_stats[sample.ami_tag] = {'containers': 0, 'bytes': 0}
                if sample.version >= version:
                    by_tag_stats[sample.ami_tag]['containers'] += 1
                    by_tag_stats[sample.ami_tag]['bytes'] += sample.size
        current_stats = GroupProductionStats.objects.filter(output_format=format, real_data=is_real_data)
        updated_tags = []
        for current_stat in current_stats:
            if current_stat.ami_tag in by_tag_stats.keys():
                current_stat.size = by_tag_stats[current_stat.ami_tag]['bytes']
                current_stat.containers = by_tag_stats[current_stat.ami_tag]['containers']
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
                current_stat.save()


def get_existing_datastes(output, ami_tag, ddm):
    tasks = es_by_keys({'ctag': ami_tag, 'output_formats': output})
    if(len(tasks)>0):
        print(ami_tag, len(tasks))
    result = []
    for task in tasks:
        if task['status'] not in ProductionTask.RED_STATUS:
            for dataset in task['output_dataset']:
                deleted = False
                try:
                    deleted = dataset['deleted']
                except:
                    print('no deleted', task['taskid'])
                if output in dataset['data_format'] and not deleted and ddm.dataset_exists(dataset['datasetname']):
                    if ('events' not in dataset) or (not dataset['events']):
                        print('no events', task['taskid'])
                        metadata = ddm.dataset_metadata(dataset['datasetname'])
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
                    result.append({'task': task['taskid'], 'dataset': dataset['datasetname'], 'size': dataset['bytes'],
                                   'task_status': task['status'], 'events': events, 'end_time': task['task_timestamp']})
                break
    return result


def ami_tags_reduction(postfix):
    if 'tid' in postfix:
        postfix = postfix[:postfix.find('_tid')]
    new_postfix = []
    first_letter = ''
    for token in postfix.split('_')[:-1]:
        if token[0] != first_letter and not (token[0] == 's' and first_letter == 'a'):
            new_postfix.append(token)
        first_letter = token[0]
    new_postfix.append(postfix.split('_')[-1])
    return '_'.join(new_postfix)


def get_container_name(dataset_name):
    return '.'.join(dataset_name.split('.')[:-1] + [ami_tags_reduction(dataset_name.split('.')[-1])])


def collect_datasets(format_base, data, only_new = False):
    for output in get_all_formats(format_base):
        if only_new:
            if GroupProductionDeletion.objects.filter(output_format=output).exists():
                continue
        if data:
            fill_db(output, True, True, False)
        else:
            fill_db(output, False, True, False)
            fill_db(output, False, False, False)
    collect_stats(format_base, data)
    return True

def collect_datasets_per_output(output, data, is_skim):
    print(output, data, is_skim)

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
    gp_container =  GroupProductionDeletion.objects.get(container=container_name)
    container_key = gp_container.input_key
    datasets = ddm.dataset_in_container(container_name)
    if additional_datasets:
        for dataset in additional_datasets:
            if dataset not in datasets:

                datasets.append(unify_dataset(dataset))
    events = 0
    bytes = 0
    is_running = False
    tasks = es_by_keys({'ctag': gp_container.ami_tag, 'output_formats': gp_container.output_format,
                        'run_number': gp_container.dsid})
    for task in tasks:
        if task['status'] not in ProductionTask.RED_STATUS:
            for dataset in task['output_dataset']:
                if (unify_dataset(dataset['datasetname']) not in datasets) and (gp_container.output_format in dataset['data_format'] and not dataset['deleted']) and \
                        (get_container_name(dataset['datasetname']) == gp_container.container) and ddm.dataset_exists(dataset['datasetname']):
                    datasets.append(unify_dataset(dataset['datasetname']))
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
        if gp_container.status == 'running':
            gp_container.update_time = timezone.now()
        if is_running:
            gp_container.status = 'running'
        else:
            gp_container.status = 'finished'
    else:
        _logger.info(
            'Container {container} has been deleted from group production lists '.format(container=container_name))
    range_containers(container_key)

def store_dataset(item, parent_id=None):
    for x in ['update_time', 'last_extension_time']:
        if item.get(x):
            item[x] = datetime.strptime(item[x], "%d-%m-%Y %H:%M:%S").replace(tzinfo=pytz.utc)
    gp_container = GroupProductionDeletion(**item)
    if GroupProductionDeletion.objects.filter(container=item['container']).exists():
        gp_container.id = GroupProductionDeletion.objects.get(container=item['container']).id
    if parent_id:
        gp_container.previous_container = GroupProductionDeletion.objects.get(id=parent_id)
    gp_container.save()
    return gp_container.id


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
                    last_id = store_dataset(item, last_id)
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


@api_view(['POST'])
def extension(request):
    try:
        print(request.data)
        print(request.user.username)
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


class GroupProductionDeletionSerializer(serializers.ModelSerializer):
    epoch_last_update_time = UnixEpochDateField(source='last_extension_time')

    class Meta:
        model = GroupProductionDeletion
        fields = '__all__'


class ListGroupProductionDeletionView(generics.ListAPIView):
    serializer_class = GroupProductionDeletionSerializer
    lookup_fields = ['dsid', 'output_format', 'version', 'status', 'skim', 'ami_tag']

    def get_queryset(self):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        filter = {}
        for field in self.lookup_fields:
            if self.request.query_params.get(field, None):  # Ignore empty fields.
                filter[field] = self.request.query_params[field]
        queryset = GroupProductionDeletion.objects.filter(**filter)

        return queryset
