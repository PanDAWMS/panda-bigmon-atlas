
import logging
import os
from ..prodtask.models import ProductionDataset
from ..getdatasets.models import ProductionDatasetsExec, TaskProdSys1
from ..settings import dq2client as dq2_settings

_logger = logging.getLogger('ddm_prodtask')


def find_dataset_events(dataset_pattern):
        return_list = []
        ddm = DDM(dq2_settings.PROXY_CERT,dq2_settings.RUCIO_ACCOUNT)
        datasets_prodsys1_db = ProductionDatasetsExec.objects.extra(where=['name like %s'], params=[dataset_pattern.replace('*','%')]).exclude(status__iexact = u'deleted').values()
        datasets_prodsys2_db = ProductionDataset.objects.extra(where=['name like %s'], params=[dataset_pattern.replace('*','%')]).filter(status__iexact = u'done').values()
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
            if '_tid' in current_dataset['name']:
                patterns_for_container.add(current_dataset['name'][current_dataset['name'].find(':')+1:current_dataset['name'].rfind('_tid')]+'/')
            else:
                patterns_for_container.add(current_dataset['name'])
            dataset_dict.update({current_dataset['name'][current_dataset['name'].find(':')+1:]:{'taskid':current_dataset['task_id'],'events':current_dataset['events']}})
        datasets_containers = []
        for pattern_for_container in patterns_for_container:
            datasets_containers += ddm.find_dataset(pattern_for_container).keys()
        containers = datasets_containers
        # if len(datasets_containers)>1:
        #     containers = [x for x in datasets_containers if x[-1] == '/' ]
        # else:
        #     containers = datasets_containers
        #datasets = [x for x in datasets_containers if x not in containers ]
        for container in containers:
            if container[-1]!='/':
                container = container+'/'
            event_count = 0
            is_good = False
            datasets_in_container = ddm.dataset_in_container(container)
            for dataset_name in datasets_in_container:
                if dataset_name[dataset_name.find(':')+1:] in dataset_dict.keys():
                    is_good = True
                    event_count += dataset_dict[dataset_name[dataset_name.find(':')+1:]]['events']
            if is_good:
                return_list.append({'dataset_name':container,'events':str(event_count)})
        # for dataset_name in datasets:
        #     try:
        #         task = TaskProdSys1.objects.get(taskname=dataset_name)
        #         if (task.status not in ['aborted','failed','lost']):
        #             return_list.append({'dataset_name':dataset_name,'events':str(task.total_events)})
        #     except:
        #         try:
        #             dataset_in_db = ProductionDataset.objects.get(name=dataset_name)
        #             if dataset_in_db.status == 'done':
        #                  return_list.append({'dataset_name':dataset_name,'events':str(-1)})
        #         except:
        #             pass
        return return_list

class DDM(object):
    """
        Wrapper for atlas ddm systems: dq2/rucio
    """


    def __init__(self, certificate_path, account, system_name = 'dq2'):
        self.__ddm = None
        if system_name.lower() == 'dq2':
            self.__init_dq2(certificate_path, account)
        else:
            raise NotImplementedError('Only dq2 is supported')

    def __init_dq2(self, certificate_path, account):
        try:
            from dq2.clientapi.DQ2 import DQ2
        except:
            raise ImportError('No dq2 lib')
        os.environ['RUCIO_ACCOUNT'] = account
        os.environ['X509_USER_PROXY'] = certificate_path
        _logger.debug('Try to auth with account %s and certificate %s'%(certificate_path, account))
        self.__ddm =  DQ2(force_backend='rucio')


    def find_dataset(self, pattern):
        """

        :param pattern: Searching datasets and containers by pattern
        :return:
            list of datasets/containers names
        """
        _logger.debug('Search dataset with pattern: %s' % pattern)
        output_datasets = self.__ddm.listDatasets(dsn=pattern,onlyNames=True)
        return output_datasets

    def dataset_in_container(self, container_name):
        """

        :param container_name:
        :return:
        """
        _logger.debug('Return dataset list from container: %s' % container_name)
        output_datasets = self.__ddm.listDatasetsInContainer(container_name)
        return output_datasets





