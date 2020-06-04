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
                response = requests.get('{0}token/certificate'.format(self._base_url), cert=cert)
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
        response = requests.post(url, headers=self._headers, data=json.dumps(kwargs))
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        content = json.loads(response.content)
        self.raise_for_errors(content)
        return self._get_rows(content, rowset_type)

    def get_current_user(self):
        result = self._post_command('GetUserInfo')
        return str(result[0]['AMIUser'])

    def get_ami_tag(self, tag_name):
        return self._post_command('AMIGetAMITagInfo', 'amiTagInfo', amiTag=tag_name)[0]