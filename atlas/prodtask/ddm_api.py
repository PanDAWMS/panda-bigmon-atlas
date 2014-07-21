
import logging
import os

_logger = logging.getLogger('ddm_prodtask')

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





