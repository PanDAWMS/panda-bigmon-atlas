import json
import logging
import os

import requests

from ..settings import cricclient as cric_settings

_logger = logging.getLogger('prodtaskwebui')



class CRICClient(object):
    def __init__(self, base_url=cric_settings.CRIC_BASE_URL, cert=cric_settings.CERTIFICAT ):
        """Initializes new instance of CRICClient class

        :param cert: path to certificate or to proxy
        :param base_url: CRIC REST API base url
        """

        self.cert = cert
        self._base_url = base_url


    def _get_url(self, command):
        return '{0}/{1}/query/?json'.format(self._base_url, command)

    def _get_command(self, command):
        url = self._get_url(command)
        response = requests.get(url, cert=self.cert, verify='/etc/ssl/certs/CERN-bundle.pem')
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        content = json.loads(response.content)
        return content

    def get_storageunit(self):
        return self._get_command('core/storageunit')

    def get_pandaresource(self):
        return self._get_command('atlas/pandaresource')

    def get_ddmendpoint(self):
        return self._get_command('atlas/ddmendpoint')

    def get_panda_queues(self):
        return self._get_command('atlas/pandaqueue')

    def get_panda_sites(self):
        return self._get_command('atlas/site')