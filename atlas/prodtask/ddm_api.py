
import logging
import os
from ..prodtask.models import ProductionDataset
from ..getdatasets.models import ProductionDatasetsExec, TaskProdSys1
from ..settings import dq2client as dq2_settings

_logger = logging.getLogger('ddm_prodtask')


def find_dataset_events(dataset_pattern):
        return_list = []
        ddm = DDM(dq2_settings.PROXY_CERT,dq2_settings.RUCIO_ACCOUNT)
        datasets_containers = ddm.find_dataset(dataset_pattern.replace('%','*'))
        containers = [x for x in datasets_containers if x[-1] == '/' ]
        datasets = [x for x in datasets_containers if x not in containers ]
        for container in containers:
            event_count = 0
            is_good = False
            datasets_in_container = ddm.dataset_in_container(container)
            for dataset_name in datasets_in_container:
                if dataset_name in datasets:
                    datasets.remove(dataset_name)
                try:
                    dataset = ProductionDatasetsExec.objects.get(name=dataset_name)
                    task = TaskProdSys1.objects.get(taskid=dataset.taskid)
                    if (task.status not in ['aborted','failed','lost']):
                        event_count += task.total_events
                        is_good = True

                except:
                    try:
                        dataset_in_db = ProductionDataset.objects.get(name=dataset_name)
                        if dataset_in_db.status == 'done':
                            if dataset_in_db.events:
                                if dataset_in_db.events > 0:
                                    event_count += task.total_events
                                    is_good = True

                    except:
                        pass

            if is_good:
                return_list.append({'dataset_name':container,'events':str(event_count)})
        for dataset_name in datasets:
            try:
                task = TaskProdSys1.objects.get(taskname=dataset_name)
                if (task.status not in ['aborted','failed','lost']):
                    return_list.append({'dataset_name':dataset_name,'events':str(task.total_events)})
            except:
                try:
                    dataset_in_db = ProductionDataset.objects.get(name=dataset_name)
                    if dataset_in_db.status == 'done':
                         return_list.append({'dataset_name':dataset_name,'events':str(-1)})
                except:
                    pass
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
        _logger.debug('Try to auth with account %s and certificate %s'%(certificate_path, account))
        self.__ddm =  DQ2(certificate=certificate_path)


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





