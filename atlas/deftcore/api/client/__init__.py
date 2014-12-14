__author__ = 'Dmitry Golubkov'
__email__ = 'dmitry.v.golubkov@cern.ch'

import json
import requests
import urllib


class Client(object):
    BASE_URL = 'https://aipanda015.cern.ch'

    def __init__(self, auth_user, auth_key, verify_ssl_cert=False, base_url=None):
        self.verify_ssl_cert = verify_ssl_cert
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = self.BASE_URL
        self.api_url = '/api/v1/request/'
        self.api_search_task_url = '/api/v1/task/'
        self.headers = {'Content-Type': 'application/json',
                        'Authorization': "ApiKey %s:%s" % (auth_user, auth_key)}

    def _get_action_list(self):
        url = "%s%sactions/" % (self.base_url, self.api_url)
        response = requests.get(url, headers=self.headers, verify=self.verify_ssl_cert)
        if response.status_code == requests.codes.ok:
            return json.loads(response.content)['result']
        else:
            raise Exception("Invalid HTTP response code: %d" % response.status_code)

    def _create_request(self, action, owner, body):
        action_list = self._get_action_list()
        if not action in action_list:
            raise Exception("Invalid action: %s (%s)" % (action, str(action_list)))

        url = "%s%s" % (self.base_url, self.api_url)

        data = {'action': action, 'owner': owner, 'body': "%s" % json.dumps(body)}

        response = requests.post(url, headers=self.headers, data=json.dumps(data), verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.created:
            api_request_object = json.loads(response.content)
            return api_request_object['id']
        elif response.status_code == requests.codes.unauthorized:
            raise Exception("Access denied")
        else:
            raise Exception("Invalid HTTP response code: %d" % response.status_code)

    def get_status(self, request_id):
        url = "%s%s%s/" % (self.base_url, self.api_url, request_id)
        response = requests.get(url, headers=self.headers, verify=self.verify_ssl_cert)
        if response.status_code == requests.codes.ok:
            status_string = json.loads(response.content)['status']
            if status_string:
                return json.loads(status_string)
        elif response.status_code == requests.codes.unauthorized:
            raise Exception("Access denied")
        else:
            raise Exception("Invalid HTTP response code: %d" % response.status_code)

    def create_task_chain(self, owner, step_id, max_number_of_steps=None, debug_mode=False):
        body = {'step_id': step_id, 'max_number_of_steps': max_number_of_steps, 'debug_mode': debug_mode}
        return self._create_request('create_task_chain', owner, body)

    def clone_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('clone_task', owner, body)

    def abort_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('abort_task', owner, body)

    def finish_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('finish_task', owner, body)

    def reassign_task_to_site(self, owner, task_id, site):
        body = {'task_id': task_id, 'site': site, 'cloud': None}
        return self._create_request('reassign_task', owner, body)

    def reassign_task_to_cloud(self, owner, task_id, cloud):
        body = {'task_id': task_id, 'site': None, 'cloud': cloud}
        return self._create_request('reassign_task', owner, body)

    def change_task_priority(self, owner, task_id, priority):
        body = {'task_id': task_id, 'priority': priority}
        return self._create_request('change_task_priority', owner, body)

    def retry_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('retry_task', owner, body)

    def increase_attempt_number(self, owner, task_id, increment):
        body = {'task_id': task_id, 'increment': increment}
        return self._create_request('increase_attempt_number', owner, body)

    def change_task_ram_count(self, owner, task_id, ram_count):
        body = {'task_id': task_id, 'ram_count': ram_count}
        return self._create_request('change_task_ram_count', owner, body)

    def change_task_wall_time(self, owner, task_id, wall_time):
        body = {'task_id': task_id, 'wall_time': wall_time}
        return self._create_request('change_task_wall_time', owner, body)

    def add_task_comment(self, owner, task_id, comment):
        body = {'task_id': task_id, 'comment_body': comment}
        return self._create_request('add_task_comment', owner, body)

    def _search_task(self, filter_dict):
        if len(filter_dict.keys()):
            filter_dict.update({'limit': 0})
        filter_string = urllib.urlencode(filter_dict)
        url = "%s%s?%s" % (self.base_url, self.api_search_task_url, filter_string)
        response = requests.get(url, headers=self.headers, verify=self.verify_ssl_cert)
        if response.status_code == requests.codes.ok:
            return json.loads(response.content)
        else:
            raise Exception("Invalid HTTP response code: %d" % response.status_code)

    def search_task_by_id(self, task_id):
        filter_dict = dict()
        if not task_id is None:
            filter_dict.update({'id': task_id})
        return self._search_task(filter_dict)

    def search_task_by_parent_id(self, parent_id):
        filter_dict = dict()
        if not parent_id is None:
            filter_dict.update({'parent_id': parent_id})
        return self._search_task(filter_dict)

    def search_task_by_chain_id(self, chain_id):
        filter_dict = dict()
        if not chain_id is None:
            filter_dict.update({'chain_id': chain_id})
        return self._search_task(filter_dict)

    def search_task_by_name(self, taskname):
        filter_dict = dict()
        if not taskname is None:
            filter_dict.update({'name__icontains': taskname})
        return self._search_task(filter_dict)
