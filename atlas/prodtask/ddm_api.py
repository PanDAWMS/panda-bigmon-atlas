
import logging
import os

from ..prodtask.models import ProductionDataset
from ..getdatasets.models import ProductionDatasetsExec, TaskProdSys1
from ..settings import dq2client as dq2_settings
from rucio.client import Client

_logger = logging.getLogger('prodtaskwebui')

def number_of_files_in_dataset(dsn):
    ddm = DDM()
    return len(ddm.list_files(dsn)[0])

def find_dataset_events(dataset_pattern):
        return_list = []

        #datasets_prodsys1_db = list(ProductionDatasetsExec.objects.extra(where=['name like %s'], params=[dataset_pattern.replace('*','%')]).exclude(status__iexact = u'deleted').values())
        datasets_prodsys2_db = list(ProductionDataset.objects.extra(where=['name like %s'], params=[dataset_pattern.replace('*','%')]).filter(status__iexact = u'done').values())
        patterns_for_container = set()
        dataset_dict = {}
        # for current_dataset in datasets_prodsys1_db:
        #     if '_tid' in current_dataset['name']:
        #         patterns_for_container.add(current_dataset['name'][current_dataset['name'].find(':')+1:current_dataset['name'].rfind('_tid')]+'/')
        #     else:
        #         patterns_for_container.add(current_dataset['name'])
        #     task = TaskProdSys1.objects.get(taskid=current_dataset['taskid'])
        #     if (task.status not in ['aborted','failed','lost']):
        #         dataset_dict.update({current_dataset['name'][current_dataset['name'].find(':')+1:]:{'taskid':current_dataset['taskid'],'events':task.total_events}})
        for current_dataset in datasets_prodsys2_db:
            if '_tid' in current_dataset['name']:
                patterns_for_container.add(current_dataset['name'][current_dataset['name'].find(':')+1:current_dataset['name'].rfind('_tid')]+'/')
            else:
                patterns_for_container.add(current_dataset['name'])
            dataset_dict.update({current_dataset['name'][current_dataset['name'].find(':')+1:]:{'taskid':current_dataset['task_id'],'events':current_dataset['events']}})
        datasets_containers = []
        ddm = DDM()
        for pattern_for_container in patterns_for_container:
            datasets_containers += ddm.find_container(pattern_for_container)
        containers = datasets_containers
        # if len(datasets_containers)>1:
        #     containers = [x for x in datasets_containers if x[-1] == '/' ]
        # else:
        #     containers = datasets_containers
        #datasets = [x for x in datasets_containers if x not in containers ]
        for container in containers:

            event_count = 0
            is_good = False
            datasets_in_container = ddm.dataset_in_container(container)
            for dataset_name in datasets_in_container:
                if dataset_name[dataset_name.find(':')+1:] in dataset_dict.keys():
                    is_good = True
                    event_count += dataset_dict[dataset_name[dataset_name.find(':')+1:]]['events']
            if is_good:
                return_list.append({'dataset_name':container,'events':str(event_count)})
        if (not return_list) and dataset_dict:
            for dataset in dataset_dict.keys():
                return_list.append({'dataset_name':dataset,'events':str(dataset_dict[dataset]['events'])})

        return return_list

def tid_from_container(container):
    ddm = DDM(dq2_settings.PROXY_CERT,dq2_settings.RUCIO_ACCOUNT)
    if container[-1]!='/':
        container = container + '/'
    datasets = ddm.dataset_in_container(container)
    return map(lambda x: int(x[x.rfind('tid')+3:x.rfind('_')]),datasets)

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
        _logger.debug('Try to auth with account %s and certificate %s'%(certificate_path, account))
        self.__ddm = Client(account=account, ca_cert=certificate_path)


    def ping(self):
        return self.__ddm.ping()

    def list_files(self, dsn):
        scope, name = self.rucio_convention(dsn)
        output_list = list(self.__ddm.list_files(scope, name))
        return output_list

    def find_dataset(self, pattern):
        """

        :param pattern: Searching datasets and containers by pattern
        :return:
            list of datasets/containers names
        """
        _logger.debug('Search dataset with pattern: %s' % pattern)
        scope, name = self.rucio_convention(pattern)
        output_datasets = list(self.__ddm.list_dids(scope=scope,filters={'name':name}))
        return output_datasets


    def find_container(self, pattern):
        _logger.debug('Search container with pattern: %s' % pattern)
        scope, name = self.rucio_convention(pattern)
        output_datasets = list(self.__ddm.list_dids(scope=scope,filters={'name':name},type='container'))
        return output_datasets


    def dataset_in_container(self, container_name):
        """

        :param container_name:
        :return:
        """
        _logger.debug('Return dataset list from container: %s' % container_name)
        scope, name = self.rucio_convention(container_name)
        output_datasets = list(self.__ddm.list_content(scope=scope, name=name))
        return [x['name'] for x in output_datasets]





