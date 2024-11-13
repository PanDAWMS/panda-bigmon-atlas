
import logging
import os
import datetime
import re
import random

import math
from rucio.common.exception import DataIdentifierNotFound
from django.utils import timezone
from ..prodtask.models import ProductionDataset, ProductionTask
from ..getdatasets.models import  TaskProdSys1
from ..settings import dq2client as dq2_settings
from rucio.client import Client

_logger = logging.getLogger('prodtaskwebui')

def number_of_files_in_dataset(dsn):
    ddm = DDM()
    return len(ddm.list_files(dsn)[0])

def name_without_scope(name):
    return name[name.find(':')+1:]

def find_dataset_events(dataset_pattern, ami_tags=None):
        return_list = []
        datasets_prodsys1_db = []
        datasets_prodsys2_db = list(ProductionDataset.objects.extra(where=['name like %s'], params=[dataset_pattern.replace('*','%')]).filter(status__iexact = 'done').values())
        #if not datasets_prodsys2_db:
        #    datasets_prodsys1_db = list(ProductionDatasetsExec.objects.extra(where=['name like %s'], params=[dataset_pattern.replace('*','%')]).exclude(status__iexact = 'deleted').values())
        patterns_for_container = set()
        dataset_dict = {}
        for current_dataset in datasets_prodsys1_db:
            if '_tid' in current_dataset['name']:
                patterns_for_container.add(current_dataset['name'][current_dataset['name'].find(':')+1:current_dataset['name'].rfind('_tid')]+'/')
            else:
                patterns_for_container.add(current_dataset['name'])
            task = TaskProdSys1.objects.get(taskid=current_dataset['taskid'])
            if (task.status not in ['aborted','failed','lost']):
                dataset_dict.update({current_dataset['name'][current_dataset['name'].find(':')+1:]:{'taskid':current_dataset['taskid'],'events':task.total_events}})
        for current_dataset in datasets_prodsys2_db:
            if 'archive' not in current_dataset['name']:
                if '_tid' in current_dataset['name']:
                    patterns_for_container.add(current_dataset['name'][current_dataset['name'].find(':')+1:current_dataset['name'].rfind('_tid')]+'/')
                else:
                    patterns_for_container.add(current_dataset['name'])
                dataset_dict.update({current_dataset['name'][current_dataset['name'].find(':')+1:]:{'taskid':current_dataset['task_id'],'events':current_dataset['events']}})
        datasets_containers = []
        ddm = DDM()
        for pattern_for_container in patterns_for_container:
            if ami_tags:
                ami_tags_in_container = pattern_for_container.replace('/','').split('.')[-1].split('_')
                if len(ami_tags_in_container)>len(ami_tags):
                    try:
                        new_dataset_container = ddm.find_container('.'.join(pattern_for_container.replace('/','').split('.')[:-1])+'.'+'_'.join(ami_tags))
                        if new_dataset_container:
                            if new_dataset_container not in datasets_containers:
                                datasets_containers+=new_dataset_container
                    except:
                        pass
            new_dataset_container = ddm.find_container(pattern_for_container)
            if new_dataset_container not in datasets_containers:
                datasets_containers += ddm.find_container(pattern_for_container)
        containers = datasets_containers
        containers.sort(key=lambda item: (len(item), item))
        # if len(datasets_containers)>1:
        #     containers = [x for x in datasets_containers if x[-1] == '/' ]
        # else:
        #     containers = datasets_containers
        #datasets = [x for x in datasets_containers if x not in containers ]
        for container in containers:

            event_count = 0
            tasks = []
            is_good = False
            datasets_in_container = ddm.dataset_in_container(container)
            for dataset_name in datasets_in_container:
                if dataset_name[dataset_name.find(':')+1:] in list(dataset_dict.keys()):
                    is_good = True
                    item = dataset_dict.pop(dataset_name[dataset_name.find(':')+1:])
                    event_count += item['events']
                    tasks.append(item['taskid'])
            if is_good:
                return_list.append({'dataset_name':container,'events':str(event_count),'tasks':tasks, 'excluded':False})
        if (not return_list) and dataset_dict:
            for dataset in list(dataset_dict.keys()):
                if ddm.dataset_exists(dataset):
                    return_list.append({'dataset_name':dataset,'events':str(dataset_dict[dataset]['events']),'tasks':[dataset_dict[dataset]['taskid']], 'excluded':False})

        return return_list

def tid_from_container(container):
    ddm = DDM(dq2_settings.PROXY_CERT,dq2_settings.RUCIO_ACCOUNT)
    if container[-1]!='/':
        container = container + '/'
    datasets = ddm.dataset_in_container(container)
    return [int(x[x.rfind('tid')+3:x.rfind('_')]) for x in datasets]


def dataset_events(container):
    ddm = DDM(dq2_settings.PROXY_CERT,dq2_settings.RUCIO_ACCOUNT)
    if container[-1]!='/':
        container = container + '/'
    datasets = ddm.dataset_in_container(container)
    result = []
    for dataset in datasets:
        events = 0
        dataset_tid = 0
        if 'tid' in dataset:
            dataset_tid = int(dataset[dataset.rfind('tid')+3:dataset.rfind('_')])
            if ProductionTask.objects.filter(id=dataset_tid).exists():
                events = ProductionTask.objects.get(id=dataset_tid).total_events
            elif TaskProdSys1.objects.filter(taskid=dataset_tid).exists():
                events = TaskProdSys1.objects.get(id=dataset_tid).total_events
        result.append({'dataset':dataset,'events':events, 'tid':dataset_tid})
    return result

def dataset_events_ddm(container):
    ddm = DDM(dq2_settings.PROXY_CERT,dq2_settings.RUCIO_ACCOUNT)
    if container[-1]!='/':
        container = container + '/'
    datasets = ddm.dataset_in_container(container)
    result = []
    for dataset in datasets:
        dataset_tid = 0
        if 'tid' in dataset:
            dataset_tid = int(dataset[dataset.rfind('tid')+3:dataset.rfind('_')])
        dataset_metadata = ddm.dataset_metadata(dataset)
        result.append({'dataset':dataset,'events':dataset_metadata['events'], 'tid':dataset_tid, 'metadata':dataset_metadata})
    return result


class DDM(object):
    """
        Wrapper for atlas ddm systems: dq2/rucio
    """

    @staticmethod
    def rucio_convention(did):
        # Try to extract the scope from the DSN
        if did.find(':') > -1:
            return did.split(':')[0], did.split(':')[1].replace('/','')
        else:
            scope = did.split('.')[0]
            if did.startswith('user') or did.startswith('group'):
                scope = ".".join(did.split('.')[0:2])
            return scope, did.replace('/','')

    def __init__(self, certificate_path=dq2_settings.PROXY_CERT, account=dq2_settings.RUCIO_ACCOUNT, system_name = 'rucio'):
        self.__ddm = None
        if system_name.lower() == 'rucio':
            self.__init_rucio(certificate_path, account)
        else:
            raise NotImplementedError('Only rucio is supported')


    def __init_rucio(self, certificate_path, account):

        os.environ['RUCIO_ACCOUNT'] = account
        os.environ['X509_USER_PROXY'] = certificate_path
        self.__ddm = Client(account=account, auth_type='x509_proxy')


    @property
    def ddm_client(self):
        return self.__ddm

    def upload_file(self, local_file, rse, dataset, register_after_upload=True):
        from rucio.client.uploadclient import UploadClient
        scope, name = self.rucio_convention(dataset)
        _rucio_file = {'path': local_file, 'rse': rse, 'did_scope': scope,'dataset_scope': scope, 'dataset_name': name, 'register_after_upload': register_after_upload}
        upload_client = UploadClient(_client=self.ddm_client, logger=_logger)
        return upload_client.upload([_rucio_file])


    def ping(self):
        return self.__ddm.ping()

    def list_files(self, dsn):
        scope, name = self.rucio_convention(dsn)
        output_list = list(self.__ddm.list_files(scope, name))
        return output_list

    def list_file_long(self, dsn):
        scope, dataset = self.rucio_convention(dsn)
        files = list(self.__ddm.list_files(scope, dataset, long=True))
        return files

    def list_files_name_in_dataset(self, dsn):
        filename_list = list()
        scope, dataset = self.rucio_convention(dsn)
        files = self.__ddm.list_files(scope, dataset, long=False)
        for file_name in [e['name'] for e in files]:
            filename_list.append(file_name)
        return filename_list

    def dataset_exists(self, dsn):
        scope, name = self.rucio_convention(dsn)
        try:
            self.__ddm.get_did(scope, name)
            return True
        except DataIdentifierNotFound:
            return False
        except Exception as e:
            raise e

    def is_dsn_exists_with_rule_or_replica(self, dataset):
        DELITION_PERIOD_HOURS = 24

        scope, dataset = self.rucio_convention(dataset)
        dataset_exists = self.dataset_exists(dataset)
        if not dataset_exists:
            return False
        if not list(self.__ddm.list_dataset_replicas(scope, dataset)) and not(list(self.__ddm.list_did_rules(scope, dataset))):
            return False
        if self.dataset_metadata(dataset).get('expired_at'):
            if ((self.dataset_metadata(dataset).get('expired_at') - timezone.now().replace(tzinfo=None)) <
                    datetime.timedelta(hours=DELITION_PERIOD_HOURS)):
                return False
        return True

    def list_datasets_by_pattern(self, pattern):
        result = list()
        match = re.match(r'^\*', pattern)
        if not match:
            scope, dataset = self.rucio_convention(pattern)
            collection = 'dataset'
            if pattern.endswith('/'):
                collection = 'container'
            filters = {'name': dataset}
            for name in self.__ddm.list_dids(scope, filters, did_type=collection):
                result.append('{0}:{1}'.format(scope, name))
        return result

    def is_dsn_container(self, dataset):
        scope, dataset = self.rucio_convention(dataset)
        metadata = self.__ddm.get_metadata(scope=scope, name=dataset)
        return bool(metadata['did_type'] == 'CONTAINER')

    def list_datasets_in_container(self, container):
        dataset_names = list()

        if container.endswith('/'):
            container = container[:-1]

        scope, container_name = self.rucio_convention(container)

        try:
            if self.__ddm.get_metadata(scope, container_name)['did_type'] == 'CONTAINER':
                for e in self.__ddm.list_content(scope, container_name):
                    dataset = '{0}:{1}'.format(e['scope'], e['name'])
                    if e['type'] == 'DATASET':
                        dataset_names.append(dataset)
                    elif e['type'] == 'CONTAINER':
                        names = self.list_datasets_in_container(dataset)
                        dataset_names.extend(names)
        except DataIdentifierNotFound:
            pass
        return dataset_names
    def is_dsn_dataset(self, dsn):
        scope, dataset = self.rucio_convention(dsn)
        metadata = self.__ddm.get_metadata(scope=scope, name=dataset)
        return bool(metadata['did_type'] == 'DATASET')

    def get_datasets_and_containers(self, input_data_name, datasets_contained_only=False):
        data_dict = {'containers': list(), 'datasets': list()}

        if input_data_name[-1] == '/':
            input_container_name = input_data_name
            input_data_name = input_data_name[:-1]
        else:
            input_container_name = '{0}/'.format(input_data_name)

        # searching containers first
        for name in self.list_datasets_by_pattern(input_container_name):
            if self.is_dsn_container(name):
                if name[-1] == '/':
                    data_dict['containers'].append(name)
                else:
                    data_dict['containers'].append('{0}/'.format(name))

        # searching datasets
        if datasets_contained_only and len(data_dict['containers']):
            for container_name in data_dict['containers']:
                dataset_names = self.list_datasets_in_container(container_name)
                data_dict['datasets'].extend(dataset_names)
        else:
            enable_pattern_search = True
            names = self.list_datasets_by_pattern(input_data_name)
            if len(names) > 0:
                if names[0].split(':')[-1] == input_data_name.split(':')[-1] and self.is_dsn_dataset(names[0]):
                    data_dict['datasets'].append(names[0])
                    enable_pattern_search = False
            if enable_pattern_search:
                for name in self.list_datasets_by_pattern("{0}*".format(input_data_name)):
                    # FIXME
                    is_sub_dataset = \
                        re.match(r"%s.*_(sub|dis)\d*" % input_data_name.split(':')[-1], name.split(':')[-1],
                                 re.IGNORECASE)
                    is_o10_dataset = \
                        re.match(r"%s.*.o10$" % input_data_name.split(':')[-1], name.split(':')[-1], re.IGNORECASE)
                    if not self.is_dsn_container(name) and not is_sub_dataset and not is_o10_dataset:
                        data_dict['datasets'].append(name)

        return data_dict


    def find_dataset(self, pattern, long=False):
        """

        :param pattern: Searching datasets and containers by pattern
        :return:
            list of datasets/containers names
        """
        _logger.debug('Search dataset with pattern: %s' % pattern)
        scope, name = self.rucio_convention(pattern)
        output_datasets = list(self.__ddm.list_dids(scope=scope,filters={'name':name},long=long))
        return output_datasets


    def find_container(self, pattern, long=False):
        _logger.debug('Search container with pattern: %s' % pattern)
        scope, name = self.rucio_convention(pattern)
        output_datasets = list(self.__ddm.list_dids(scope=scope,filters={'name':name},did_type='container',long=long))
        return output_datasets


    def deleteDataset(self, dataset, lifetime = 24*3600):
        scope, name = self.rucio_convention(dataset)
        self.__ddm.set_metadata(scope=scope, name=name, key='lifetime', value=lifetime)

    def datasetSetTransient(self, dataset, transient = False):
        scope, name = self.rucio_convention(dataset)
        self.__ddm.set_metadata(scope=scope, name=name, key='transient', value=transient)

    def keepDataset(self, dataset):
        scope, name = self.rucio_convention(dataset)
        self.__ddm.set_metadata(scope=scope, name=name, key='lifetime', value=None)

    def setLifeTimeTransientDataset(self, dataset, days):
        scope, name = self.rucio_convention(dataset)
        lifetime = 60*24*60*days
        self.__ddm.set_metadata(scope=scope, name=name, key='lifetime', value=lifetime)

    def changeDatasetCampaign(self, dataset, campaign):
        scope, name = self.rucio_convention(dataset)
        self.__ddm.set_metadata(scope=scope, name=name, key='campaign', value=campaign)


    def get_replica_pre_stage_rule_by_rse(self, rse):
        #rse_attr = self.__ddm.list_rse_attributes(rse)
        # return 'type=DATADISK&datapolicynucleus=True', 'type=DATADISK|{source_tape}', rse
        return '{destination_by_tape}', 'type=DATADISK|{source_tape}', rse
        # if rse not in ['CERN-PROD_TEST-CTA', 'CERN-PROD_RAW']:
        #         #return 'cloud=%s&type=DATADISK&datapolicynucleus=True' % rse_attr['cloud'], 'tier=1&type=DATATAPE', rse
        #         return 'type=DATADISK&datapolicynucleus=True', 'type=DATADISK|{source_tape}', rse
        # elif rse == 'CERN-PROD_TEST-CTA':
        #         rse_attr = self.__ddm.list_rse_attributes(rse)
        #         return 'cloud=%s&type=DATADISK&datapolicynucleus=True' % rse_attr['cloud'], 'CERN-PROD_TEST-CTA', rse
        # elif rse == 'CERN-PROD_RAW':
        #     #return 'CERN-PROD_DATADISK', 'CERN-PROD_RAW',  'CERN-PROD_RAW'
        #     return 'type=DATADISK&datapolicynucleus=True', 'type=DATADISK|CERN-PROD_RAW',  'CERN-PROD_RAW'



    def get_replica_pre_stage_rule(self, dataset):
        scope, name = self.rucio_convention(dataset)
        for lock in self.__ddm.get_dataset_locks(scope, name):
            if lock['rse'].find('TAPE') > -1:
                if str(lock['state']) == 'OK':
                    rse_attr = self.__ddm.list_rse_attributes(lock['rse'])
                    return 'cloud=%s&type=DATADISK&datapolicynucleus=True' % rse_attr['cloud'], 'tier=1&type=DATATAPE',lock['rse']
            elif lock['rse'] == 'CERN-PROD_TEST-CTA':
                if str(lock['state']) == 'OK':
                    rse_attr = self.__ddm.list_rse_attributes(lock['rse'])
                    return 'cloud=%s&type=DATADISK&datapolicynucleus=True' % rse_attr['cloud'], 'CERN-PROD_TEST-CTA',lock['rse']


    def add_replication_rule(self, dataset, rse, copies=1, lifetime=30*86400, weight='freespace', activity=None, notify='N', source_replica_expression=None):
        _logger.debug('Create rule for dataset: %s to %s' % (dataset,rse))
        scope, name = self.rucio_convention(dataset)
        self.__ddm.add_replication_rule(dids=[{'scope':scope, 'name':name}], rse_expression=rse, activity=activity, copies=copies,
                                        lifetime=lifetime, weight=weight, notify=notify,source_replica_expression=source_replica_expression, asynchronous=True)

    def change_rule_lifetime(self, rule_id, lifetime):
        self.__ddm.update_replication_rule(rule_id,{'lifetime':lifetime})

    def get_rule(self, rule_id):
        return self.__ddm.get_replication_rule(rule_id)

    def boost_rule(self, rule_id):
        return self.__ddm.update_replication_rule(rule_id, {'boost_rule': True})

    def delete_replication_rule(self, rule_id):
        self.__ddm.delete_replication_rule(rule_id)

    def dataset_is_in_container(self, dataset_name, container_name):
        scope, name = self.rucio_convention(dataset_name)
        container_scope, container_name = self.rucio_convention(container_name)
        return scope+':'+name in [x['scope']+':'+x['name'] for x in self.__ddm.list_content(container_scope, container_name)]

    def dataset_in_container(self, container_name):
        """

        :param container_name:
        :return:
        """
        #_logger.debug('Return dataset list from container: %s' % container_name)
        scope, name = self.rucio_convention(container_name)
        try:
            if self.dataset_metadata(container_name)['did_type'] == 'CONTAINER':
                output_datasets = list(self.__ddm.list_content(scope=scope, name=name))
            else:
                 output_datasets =[{'scope':scope,'name': name}]
        except DataIdentifierNotFound:
                 return []
        except Exception as e:
            raise e
        return [x['scope']+':'+x['name'] for x in output_datasets]

    def staged_size(self, dataset_name):
        scope, name = self.rucio_convention(dataset_name)
        replicas = self.dataset_replicas(dataset_name)
        if self.__datadisk_rse is None:
            self.__datadisk_rse = [y['rse'] for y in self.list_rses('type=DATADISK')]
        data_replicas = [x for x in replicas if x['rse'] in self.__datadisk_rse]
        max_data_replica_size = 0
        if data_replicas:
            max_data_replica_size = max([x['available_bytes'] for x in data_replicas])
        return max_data_replica_size

    def number_of_full_replicas(self, dataset_name):
        replicas = self.dataset_replicas(dataset_name)
        full_replicas = []
        for replica in replicas:
            if replica['available_length'] == replica['length']:
                full_replicas.append(replica)
        return full_replicas

    def only_tape_replica(self, dataset_name):
        replicas = self.full_replicas_per_type(dataset_name)
        rules = list(self.list_dataset_rules(dataset_name))
        rules_expression = []
        for rule in rules:
            if  not (rule['account'] == 'prodsys' and rule['activity'] == 'Staging'):
                rules_expression.append(rule['rse_expression'])
        for replica in replicas['data']:
            if replica['rse'] in rules_expression:
                return False
        return [x['rse'] for x in replicas['tape']]


    def filter_replicas_without_rules(self, original_dataset:str) -> tuple:
        replicas = self.full_replicas_per_type(original_dataset)
        rules = list(self.list_dataset_rules(original_dataset))
        rules_expression = []
        staging_rule = None
        for rule in rules:
            if rule['account'] == 'prodsys' and rule['activity'] == 'Staging':
                staging_rule = rule
            else:
                rules_expression.append(rule['rse_expression'])
        filtered_replicas = {'tape': [], 'data': []}
        data_replica_exists = len(replicas['data']) > 0
        for replica in replicas['tape']:
            if replica['rse'] in rules_expression:
                filtered_replicas['tape'].append(replica)
        if len(replicas['tape']) >= 1 and len(filtered_replicas['tape']) == 0 and len(rules) == 0:
            filtered_replicas['tape'] = replicas['tape']
        for replica in replicas['data']:
            if staging_rule is not None or replica['rse'] in rules_expression:
                filtered_replicas['data'].append(replica)
        all_data_replicas_without_rules = data_replica_exists and len(filtered_replicas['data']) == 0
        return filtered_replicas, staging_rule, all_data_replicas_without_rules

    def get_nevents_per_file(self, dsn):
        number_files = self.get_number_files(dsn)
        if not number_files:
            raise ValueError('Dataset {0} has no files'.format(dsn))
        number_events = self.get_number_events(dsn)
        if not number_files:
            raise ValueError('Dataset {0} has no events or corresponding metadata (nEvents)'.format(dsn))
        return math.ceil(float(number_events) / float(number_files))

    def dataset_replicas(self, dataset_name):
        scope, name = self.rucio_convention(dataset_name)

        return list(self.__ddm.list_dataset_replicas(scope=scope, name=name))

    def get_dataset_rses(self, dsn):
        if not self.is_dsn_dataset(dsn):
            raise Exception('{0} is not dataset'.format(dsn))
        scope, dataset = self.rucio_convention(dsn)
        return [replica['rse'] for replica in self.__ddm.list_dataset_replicas(scope, dataset)]

    def list_file_replicas(self, dataset_name):
        scope, name = self.rucio_convention(dataset_name)
        return list(self.__ddm.list_replicas([{'scope':scope,'name':name}]))

    def choose_random_files(self, list_files, files_number, random_seed=None, previously_used=None):
        lookup_list = [x for x in list_files if x not in (previously_used or [])]
        random.seed(random_seed)
        return random.sample(lookup_list, files_number)

    def staged_percent(self, dataset_name):
        staged = 0
        file_replicas = self.list_file_replicas(dataset_name)
        for x in file_replicas:
            for pfn in x['pfns'].values():
                if pfn['type'] != 'TAPE':
                    staged += 1
                    break
        return staged,len(file_replicas)

    def list_rses(self, filter = ''):
        return self.__ddm.list_rses(filter)

    def get_unavailable_rses(self):
        if self.__unavailable_rses is None:
            self.__unavailable_rses = [x['rse'] for x in self.list_rses() if x['availability'] == 0]
        return self.__unavailable_rses

    __tape_rse = None
    __datadisk_rse = None
    __unavailable_rses = None



    def biggest_datadisk(self, dataset_name):
        replicas = self.dataset_replicas(dataset_name)
        data_replicas = [x for x in replicas if x['rse'] in [y['rse'] for y in self.list_rses('type=DATADISK')]]
        if not data_replicas:
            return None
        biggest_replica = max(data_replicas, key= lambda x: x['available_length'])
        return biggest_replica


    def full_replicas_per_type(self, dataset_name):
        full_replicas = self.number_of_full_replicas(dataset_name)
        if self.__tape_rse is None:
            self.__tape_rse = [y['rse'] for y in self.list_rses('rse_type=TAPE')]
        if self.__datadisk_rse is None:
            self.__datadisk_rse = [y['rse'] for y in self.list_rses('type=DATADISK')]
        data_replicas = [x for x in full_replicas if x['rse'] in self.__datadisk_rse]
        tape_replicas = [x for x in full_replicas if x['rse'] in  self.__tape_rse]
        return {'data':data_replicas,'tape':tape_replicas}

    def dataset_active_datadisk_rule(self, dataset_name):
        scope, name = self.rucio_convention(dataset_name)
        rules = self.__ddm.list_did_rules(scope, name)
        active_rules = [(x) for x in rules if x['rse_expression'] in [y['rse'] for y in self.list_rses('type=DATADISK')] ]
        return active_rules

    def dataset_active_rule_by_rse(self, dataset_name, rse):
        scope, name = self.rucio_convention(dataset_name)
        rules = self.__ddm.list_did_rules(scope, name)
        for rule in rules:
            if rule['rse_expression'] == rse:
                return rule
        return []

    def dataset_active_rule_by_activity(self, dataset_name, activity='Staging'):
        scope, name = self.rucio_convention(dataset_name)
        rules = self.__ddm.list_did_rules(scope, name)
        for rule in rules:
            if rule['activity'] == activity:
                return rule
        return []

    def dataset_active_rule_by_rule_id(self, dataset_name, rule_id):
        scope, name = self.rucio_convention(dataset_name)
        rules = self.__ddm.list_did_rules(scope, name)
        for rule in rules:
            if rule['id'] == rule_id:
                return rule
        return []

    def active_staging_rule(self, dataset_name):
        scope, name = self.rucio_convention(dataset_name)
        rules = self.__ddm.list_did_rules(scope, name)
        for rule in rules:
            if rule['activity'] == 'Staging' and rule['account'] == 'prodsys':
                return rule
        return None

    def list_dataset_rules(self, dataset_name):
        scope, name = self.rucio_convention(dataset_name)
        rules = self.__ddm.list_did_rules(scope, name)
        return rules

    def list_parent_containers(self, dataset_name):
        scope, name = self.rucio_convention(dataset_name)
        return [x['name'] for x in self.__ddm.list_parent_dids(scope, name)]

    def check_only_unavailable_rse(self, dataset_name: str) -> [str]:
        full_replicas = self.full_replicas_per_type(dataset_name)
        if full_replicas['tape']:
            return []
        unavailable_rses = self.get_unavailable_rses()
        for replica in full_replicas['data']:
            if replica['rse'] not in unavailable_rses:
                return []
        return [x['rse'] for x in full_replicas['data']]

    def find_disk_for_tape(self, tape_rse):
        rses = [x for x in list(self.list_rses('type=DATADISK')) if x['rse'].startswith(tape_rse.split('_')[0])]
        if rses:
            return rses[0]
        return None

    def list_locks(self,rule_id):
        return self.__ddm.list_replica_locks(rule_id)

    def dataset_size(self, dataset_name):
        """
        :param dataset_name: name of the dataset
        :return: size of the dataset
        """
        scope, name = self.rucio_convention(dataset_name)
        bytes = self.__ddm.get_metadata(scope=scope,name=name)['bytes']
        return bytes

    def dataset_metadata(self, dataset_name):
        """
        :param dataset_name: name of the dataset
        :return: size of the dataset
        """
        scope, name = self.rucio_convention(dataset_name)
        events = self.__ddm.get_metadata(scope=scope,name=name)
        return events

    def datasets_metadata(self, dataset_names: [str]):
        """
        :param dataset_names: list of dataset names
        :return: list of metadata for each dataset
        """
        datasets_with_scope = list(map(lambda x: {'scope':x[0],'name':x[1]},[self.rucio_convention(dataset) for dataset in dataset_names]))
        return list(self.__ddm.get_metadata_bulk(datasets_with_scope))

    def rse_attr(self, rse):
        return self.__ddm.list_rse_attributes(rse)

    def register_container(self, container_name, datasets=None):
        if container_name.endswith('/'):
            container_name = container_name[:-1]
        scope, name = self.rucio_convention(container_name)
        self.__ddm.add_container(scope=scope, name=name)
        if datasets:
            datasets_with_scopes = list()
            for dataset in datasets:
                dataset_scope, dataset_name = self.rucio_convention(dataset)
                datasets_with_scopes.append({'scope': dataset_scope, 'name': dataset_name})
            self.__ddm.add_datasets_to_container(scope=scope, name=name, dsns=datasets_with_scopes)

    def delete_datasets_from_container(self, container_name, datasets):
        if container_name.endswith('/'):
            container_name = container_name[:-1]
        scope, name = self.rucio_convention(container_name)
        dsns = list()
        for dataset in datasets:
            dataset_scope, dataset_name = self.rucio_convention(dataset)
            dsns.append({'scope': dataset_scope, 'name': dataset_name})
        self.__ddm.detach_dids(scope=scope, name=name, dids=dsns)

    def register_datasets_in_container(self, container_name, datasets):
        if container_name.endswith('/'):
            container_name = container_name[:-1]
        scope, name = self.rucio_convention(container_name)
        datasets_with_scopes = list()
        for dataset in datasets:
            dataset_scope, dataset_name =self.rucio_convention(dataset)
            datasets_with_scopes.append({'scope': dataset_scope, 'name': dataset_name})
        self.__ddm.add_datasets_to_container(scope=scope, name=name, dsns=datasets_with_scopes)

    def list_files_with_scope_in_dataset(self, dsn, skip_short=False):
        filename_list = []
        scope, dataset = self.rucio_convention(dsn)
        files = list(self.__ddm.list_files(scope, dataset, long=False))
        if skip_short:
            sizes = [x['events'] for x in  files]
            sizes.sort()
            filter_size = sizes[-1]
            for file_name in [e['scope']+':'+e['name'] for e in files if e['events'] == filter_size]:
                filename_list.append(file_name)
        else:
            for file_name in [e['scope']+':'+e['name'] for e in files]:
                filename_list.append(file_name)
        return filename_list

    def get_campaign(self, dsn):
        scope, dataset = self.rucio_convention(dsn)
        metadata = self.__ddm.get_metadata(scope=scope, name=dataset)
        return str(metadata['campaign'])

    def register_dataset(self, dsn, files=None, statuses=None, meta=None, lifetime=None):
        """
        :param dsn: the DID name
        :param files: list of file names
        :param statuses: dictionary with statuses, like {'monotonic':True}.
        :param meta: meta-data associated with the data identifier is represented using key/value pairs in a dictionary.
        :param lifetime: DID's lifetime (in seconds).
        """
        scope, name = self.rucio_convention(dsn)
        file_dids = None
        if files:
            file_dids = list()
            for file in files:
                file_scope, file_name = self.rucio_convention(file)
                file_dids.append({'scope': file_scope, 'name': file_name})
        self.__ddm.add_dataset(scope, name, statuses=statuses, meta=meta, lifetime=lifetime, files=file_dids)

    def register_files_in_dataset(self, dsn, files):
        scope, name = self.rucio_convention(dsn)
        dids = list()
        for file_ in files:
            file_scope, file_name = self.rucio_convention(file_)
            dids.append({'scope': file_scope, 'name': file_name})
        self.__ddm.attach_dids(scope, name, dids)

    def get_production_container_name(self, dataset):
        if '_tid' in dataset:
            return dataset[:dataset.rfind('_tid')]
        else:
            return dataset
    def get_number_files(self, dsn):
        number_files = 0
        if self.is_dsn_container(dsn):
            for name in self.list_datasets_in_container(dsn):
                number_files += self.get_number_files_from_metadata(name)
        else:
            number_files += self.get_number_files_from_metadata(dsn)
        return number_files

    def get_number_events(self, dsn):
        scope, dataset = self.rucio_convention(dsn)
        metadata = self.__ddm.get_metadata(scope=scope, name=dataset)
        return int(metadata['events'] or 0)

    def get_number_files_from_metadata(self, dsn):
        scope, dataset = self.rucio_convention(dsn)
        try:
            metadata = self.__ddm.get_metadata(scope=scope, name=dataset)
            return int(metadata['length'] or 0)
        except Exception as ex:
            raise Exception('DDM Error: rucio_client.get_metadata failed ({0}) ({1})'.format(str(ex), dataset))

    @staticmethod
    def ami_tags_reduction_w_data(postfix: str, data=False) -> str:
        if 'tid' in postfix:
            postfix = postfix[:postfix.find('_tid')]
        if data:
            return postfix
        new_postfix = []
        first_letter = ''
        for token in postfix.split('_'):
            if token[0] != first_letter and not (token[0] == 's' and first_letter == 'a'):
                new_postfix.append(token)
            first_letter = token[0]
        return '_'.join(new_postfix)


    def get_sample_container_name(self, dataset_name: str) -> str:
        container_name = '.'.join(dataset_name.split('.')[:-1] + [self.ami_tags_reduction_w_data(dataset_name.split('.')[-1],
                                                                                            dataset_name.startswith(
                                                                                                'data') or (
                                                                                                        'TRUTH' in dataset_name))])
        if dataset_name.startswith('data') or ('TRUTH' in dataset_name):
            return container_name
        postfix = container_name.split('.')[-1]
        if postfix.split('_')[-1].startswith('r'):
            return container_name.replace('merge', 'recon')
        if postfix.split('_')[-1].startswith('e'):
            return container_name.replace('merge', 'evgen')
        if postfix.split('_')[-1].startswith('s') or postfix.split('_')[-1].startswith('a'):
            return container_name.replace('merge', 'simul')
        return container_name

    @staticmethod
    def with_and_without_scope(lfns: [str]) -> [str]:
        return_list = []
        for lfn in lfns:
            return_list.append(lfn)
            if lfn.find(':') > -1:
                return_list.append(lfn.split(':')[1])
            else:
                return_list.append(f'{lfn.split(".")[0]}:{lfn}')
        return return_list
