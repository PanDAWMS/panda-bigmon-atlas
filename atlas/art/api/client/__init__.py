__author__ = 'Misha Borodin'
__email__ = 'mborodin@cern.ch'

import json
import requests


class Client(object):
    BASE_URL = 'https://prodtask-dev.cern.ch'

    def __init__(self, auth_key, verify_ssl_cert=False, base_url=None):
        self.verify_ssl_cert = verify_ssl_cert
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = self.BASE_URL
        self.api_url = '/art/api/'
        self.headers = {'Content-Type': 'application/json',
                        'Authorization': "Token %s" % auth_key}

    def _get_action_list(self):
        return ['create_atr_task']

    def _create_request(self, action, body):
        action_list = self._get_action_list()
        if not action in action_list:
            raise Exception("Invalid action: %s (%s)" % (action, str(action_list)))

        url = "%s%s%s/" % (self.base_url, self.api_url, action)

        response = requests.post(url, headers=self.headers, data=json.dumps(body), verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.ok:
            return  json.loads(response.content)
        elif response.status_code == requests.codes.unauthorized:
            raise Exception("Access denied")
        else:
            raise Exception("Invalid HTTP response code: %d" % response.status_code)

    def create_atr_task(self, task_id, nightly_release,project,platform,nightly_tag,sequence_tag,package):
        body = {'task_id':task_id,'nightly_release':nightly_release,'project':project,'platform':platform,
                'nightly_tag':nightly_tag,'sequence_tag':sequence_tag,'package':package}
        return self._create_request('create_atr_task', body)

