import json
import logging
import os
import urllib.parse

from django.core.cache import cache
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


    def _get_url(self, command, postfix=''):
        if 'cache' in command:
            return urllib.parse.urljoin(self._base_url,command)
        else:
            return '{0}/{1}/query/?json{2}'.format(self._base_url, command, postfix)

    def _get_command(self, command, postfix=''):
        url = self._get_url(command, postfix)
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

    def get_panda_queues(self, postfix=''):
        return self._get_command('atlas/pandaqueue', postfix)

    def get_panda_sites(self):
        return self._get_command('atlas/site')

    def get_ddmendpointstatus(self):
        return self._get_command('atlas/ddmendpointstatus')

    def get_blacklisted_rses(self):
        rses = self.get_ddmendpointstatus()
        return list(rses.keys())

    def get_ddm_endpoint_wan(self, endpoint):
        if cache.get(f'ddm_status_{endpoint}'):
            return cache.get(f'ddm_status_{endpoint}')
        all_endpoints_status = self.get_ddmendpointstatus()
        if endpoint in all_endpoints_status:
            status = {'status': all_endpoints_status[endpoint]['read_wan']['status']['probe'],
                      'endpoint': endpoint,
                      'reason': all_endpoints_status[endpoint]['read_wan']['status']['reason'],
                      'expiration': all_endpoints_status[endpoint]['read_wan']['status']['expiration']}
            cache.set(f'ddm_status_{endpoint}', status, 3600)
            return status
        return None

    def list_site_sw_containers(self, site_name):
        sites_tags = self.get_panda_queues('&preset=tags')
        if (site_name in sites_tags) and ('nightlies' not in sites_tags[site_name]['cvmfs']):
            return [x['container_name'] for x in sites_tags[site_name]['tags'] if x['container_name']]
        return []

    def get_sites(self):
        panda_resources = self.get_panda_queues()
        return list(panda_resources.keys())

    def get_swreleases(self):
        try:
            result = self._get_command('core/swrelease')
            if (type(result) is dict) and ('error' in result):
                raise RuntimeError(result['error'])
            return result
        except (requests.exceptions.RequestException,RuntimeError) as ex:
            return self._get_command('cache/swreleases.json')

    def get_cmtconfig(self, cache):
        """
        :param cache: string in format 'CacheName-CacheRelease', for example, 'AtlasProduction-20.20.7.1'
        :return: list of available values of cmtconfig
        """
        release = cache.split('-')[-1]
        project = cache.split('-')[0]
        cmtconfig_list = list()
        swreleases = self.get_swreleases()
        for swrelease in swreleases:
            if swrelease['release'] == release and swrelease['project'] == project:
                cmtconfig_list.append(swrelease['cmtconfig'])
        return cmtconfig_list