import json
import logging
import os

import requests

from ..settings import amiclient as ami_settings

_logger = logging.getLogger('prodtaskwebui')

class AMIException(Exception):
    def __init__(self, errors):
        self.errors = errors
        self.message = '\n'.join(errors)
        super(AMIException, self).__init__(self.message)

    def has_error(self, error):
        for e in self.errors or []:
            if str(error).lower() in e.lower():
                return True

class AMIClient(object):
    def __init__(self, base_url=ami_settings.AMI_API_V2_BASE_URL, cert=ami_settings.CERTIFICAT ):
        """Initializes new instance of AMIClient class

        :param cert: path to certificate or to proxy
        :param base_url: AMI REST API base url
        """

        try:
            self._verify_server_cert = True
            self._base_url = base_url
            response = None
            try:
                response = requests.get('{0}token/certificate'.format(self._base_url), cert=cert, verify=ami_settings.CA_CERTIFICATES)
            except ConnectionError as ex:
                _logger.exception('AMI authentication error: {0}'.format(str(ex)))
            if (response is not None and response.status_code != requests.codes.ok) :
                _logger.warning('Access token acquisition error ({0})'.format(response.status_code))
                response.raise_for_status()
            self._headers = {'Content-Type': 'application/json', 'AMI-Token': response.text}
            _logger.info('AMIClient, currentUser={0}'.format(self.get_current_user()))
        except Exception as ex:
            _logger.exception('AMI initialization failed: {0}'.format(str(ex)))

    def _get_url(self, command):
        return '{0}command/{1}/json'.format(self._base_url, command)

    @staticmethod
    def _get_rows(content, rowset_type=None):
        rows = list()
        if 'AMIMessage' not in content:
            return content
        if 'rowset' not in content['AMIMessage']:
            return content['AMIMessage']
        for rowset in content['AMIMessage']['rowset']:
            if rowset_type is None or rowset.get('@type') == rowset_type:
                for row in rowset['row']:
                    row_dict = dict()
                    for field in row.get('field', []):
                        row_dict.update({field['@name']: field.get('$', 'NULL')})
                    rows.append(row_dict)
        return rows

    @staticmethod
    def raise_for_errors(content):
        errors = list()
        for error in [e.get('$') for e in content['AMIMessage'].get('error', [])]:
            if error is not None:
                errors.append(error)
        if len(errors) > 0:
            raise AMIException(errors)

    def _post_command(self, command, rowset_type=None, **kwargs):
        url = self._get_url(command)
        response = requests.post(url, headers=self._headers, data=json.dumps(kwargs), verify=ami_settings.CA_CERTIFICATES)
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        content = json.loads(response.content)
        self.raise_for_errors(content)
        return self._get_rows(content, rowset_type)

    def _get_command(self, command):
        url = self._get_url(command).replace('json','help/json')
        #url='https://ami.in2p3.fr/AMI/api/'
        response = requests.get(url, headers=self._headers, verify=ami_settings.CA_CERTIFICATES)
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        content = json.loads(response.content)
        return content

    def create_physics_container(self, super_tag: str, contained_datasets: [str], creation_comment: str):
        datasets = []
        for dataset in contained_datasets:
            if ':' not in dataset:
                datasets.append(dataset)
            else:
                datasets.append(dataset.split(':')[1])
        datasets_str = ','.join(datasets)
        return self._post_command('COMAPopulateSuperProductionDataset', None, superTag=super_tag, containedDatasets=datasets_str,
                                  separator=',', creationComment=creation_comment, selectionType='run_config', rucioRegistration='yes')
    def get_current_user(self):
        result = self._post_command('GetUserInfo')
        return str(result[0]['AMIUser'])

    def get_ami_tag(self, tag_name):
        return self._post_command('AMIGetAMITagInfo', 'amiTagInfo', amiTag=tag_name)[0]

    def set_ami_tag_invalid(self, tag_name):
        return self._post_command('SetAMITagStatus', None, amiTag=tag_name, status='invalid')

    def ami_sw_tag_by_cache(self, cache):
        query = \
            "SELECT * WHERE `SWRELEASE` = '{0}'".format(cache)

        return self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='SWTAG_VIEW',
                                  mql='{0}'.format(query))


    def ami_image_by_sw(self, swtag):
        query = \
            "SELECT * WHERE `IMAGEREPOSITORYSWTAG` = '{0}'".format(swtag)

        return self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='IMAGE_VIEW',
                                  mql='{0}'.format(query))

    def ami_image_by_name(self, image_name):
        query = \
            "SELECT * WHERE `IMAGENAME` = '{0}'".format(image_name)

        return self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='IMAGE_VIEW',
                                  mql='{0}'.format(query))

    def ami_cmtconfig_by_image_by_name(self, image_name):
        query = \
            "SELECT * WHERE `IMAGENAME` = '{0}'".format(image_name)

        container =  self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='IMAGE_VIEW',
                                  mql='{0}'.format(query))[0]

        sw_tag = container['IMAGEREPOSITORYSWTAG']

        query = \
            "SELECT * WHERE `TAGNAME` = '{0}'".format(sw_tag)

        sw_tag_dict = self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='SWTAG_VIEW',
                                  mql='{0}'.format(query))[0]

        return  sw_tag_dict['IMAGEARCH'] + '-' + sw_tag_dict['IMAGEPLATFORM'] + '-' + sw_tag_dict['IMAGECOMPILER']


    def ami_list_tags(self, trf_name, trf_release):
        query = \
            "SELECT * WHERE (`transformationName` = '{0}') and (`cacheName` = '{1}')".format(trf_name, trf_release)

        return self._post_command('SearchQuery',
                                  catalog='AMITags:production',
                                  entity='V_AMITags',
                                  mql='{0}'.format(query))