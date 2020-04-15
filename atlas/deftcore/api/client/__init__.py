__author__ = 'Dmitry Golubkov'

import json
import requests
import urllib


class Client(object):
    def __init__(self, auth_user, auth_key, verify_ssl_cert=False, base_url='https://aipanda034.cern.ch'):
        self.base_url = base_url
        self.verify_ssl_cert = verify_ssl_cert
        self.api_url = '/api/v1/request/'
        self.api_search_task_url = '/api/v1/task/'
        self.headers = {'Content-Type': 'application/json',
                        'Authorization': 'ApiKey {0}:{1}'.format(auth_user, auth_key)}

    def _get_action_list(self):
        url = '{0}{1}actions/'.format(self.base_url, self.api_url)
        response = requests.get(url, headers=self.headers, verify=self.verify_ssl_cert)
        if response.status_code == requests.codes.ok:
            return json.loads(response.content)['result']
        else:
            raise Exception('Invalid HTTP response code: {0}'.format(response.status_code))

    def _get_tags(self,tf,cache):
        url = '{0}{1}tags/{2}/{3}'.format(self.base_url, self.api_url,tf,cache)
        response = requests.get(url, headers=self.headers, verify=self.verify_ssl_cert)
        if response.status_code == requests.codes.ok:
            return json.loads(response.content)['tags']
        else:
            raise Exception('Invalid HTTP response code: {0}'.format(response.status_code))

    def _create_request(self, action, owner, body):
        action_list = self._get_action_list()
        if not action in action_list:
            raise Exception('Invalid action: {0} ({1})'.format(action, str(action_list)))

        url = "%s%s" % (self.base_url, self.api_url)

        data = {'action': action, 'owner': owner, 'body': json.dumps(body)}

        response = requests.post(url, headers=self.headers, data=json.dumps(data), verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.created:
            api_request_object = json.loads(response.content)
            return api_request_object['id']
        elif response.status_code == requests.codes.unauthorized:
            raise Exception('Access denied')
        else:
            raise Exception('Invalid HTTP response code: {0}'.format(response.status_code))

    def get_status(self, request_id):
        url = '{0}{1}{2}/'.format(self.base_url, self.api_url, request_id)
        response = requests.get(url, headers=self.headers, verify=self.verify_ssl_cert)
        if response.status_code == requests.codes.ok:
            status_string = json.loads(response.content)['status']
            if status_string:
                return json.loads(status_string)
        elif response.status_code == requests.codes.unauthorized:
            raise Exception('Access denied')
        else:
            raise Exception('Invalid HTTP response code: {0}'.format(response.status_code))

    def create_task_chain(self, owner, step_id, max_number_of_steps=None, debug_mode=False):
        body = {'step_id': step_id, 'max_number_of_steps': max_number_of_steps, 'debug_mode': debug_mode}
        return self._create_request('create_task_chain', owner, body)



    def clone_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('clone_task', owner, body)

    def abort_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('abort_task', owner, body)

    def finish_task(self, owner, task_id, soft=False):
        body = {'task_id': task_id, 'soft': soft}
        return self._create_request('finish_task', owner, body)

    def reassign_task_to_site(self, owner, task_id, site, mode=None):
        body = {'task_id': task_id, 'site': site, 'mode': mode}
        return self._create_request('reassign_task', owner, body)

    def reassign_task_to_cloud(self, owner, task_id, cloud, mode=None):
        body = {'task_id': task_id, 'cloud': cloud, 'mode': mode}
        return self._create_request('reassign_task', owner, body)

    def reassign_task_to_nucleus(self, owner, task_id, nucleus, mode=None):
        body = {'task_id': task_id, 'nucleus': nucleus, 'mode': mode}
        return self._create_request('reassign_task', owner, body)

    def reassign_jobs(self, owner, task_id, for_pending=False, first_submission=None):
        body = {'task_id': task_id, 'for_pending': for_pending, 'first_submission': first_submission}
        return self._create_request('reassign_jobs', owner, body)

    def change_task_priority(self, owner, task_id, priority):
        body = {'task_id': task_id, 'priority': priority}
        return self._create_request('change_task_priority', owner, body)

    def retry_task(self, owner, task_id, discard_events=False):
        body = {'task_id': task_id, 'discard_events': discard_events}
        return self._create_request('retry_task', owner, body)

    def pause_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('pause_task', owner, body)

    def resume_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('resume_task', owner, body)

    def reassign_task_to_share(self, owner, task_id, share, reassign_running=False):
        body = {'task_id': task_id, 'share': share, 'reassign_running': reassign_running}
        return self._create_request('reassign_task_to_share', owner, body)

    def trigger_task_brokerage(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('trigger_task_brokerage', owner, body)

    def avalanche_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('avalanche_task', owner, body)

    def increase_attempt_number(self, owner, task_id, increment):
        body = {'task_id': task_id, 'increment': increment}
        return self._create_request('increase_attempt_number', owner, body)

    def change_task_ram_count(self, owner, task_id, ram_count):
        body = {'task_id': task_id, 'ram_count': ram_count}
        return self._create_request('change_task_ram_count', owner, body)

    def change_task_wall_time(self, owner, task_id, wall_time):
        body = {'task_id': task_id, 'wall_time': wall_time}
        return self._create_request('change_task_wall_time', owner, body)

    def change_task_cpu_time(self, owner, task_id, cpu_time):
        body = {'task_id': task_id, 'cpu_time': cpu_time}
        return self._create_request('change_task_cpu_time', owner, body)

    def change_task_split_rule(self, owner, task_id, rule_name, rule_value):
        body = {'task_id': task_id, 'rule_name': rule_name, 'rule_value': rule_value}
        return self._create_request('change_task_split_rule', owner, body)

    def change_task_attribute(self, owner, task_id, attr_name, attr_value):
        body = {'task_id': task_id, 'attr_name': attr_name, 'attr_value': attr_value}
        return self._create_request('change_task_attribute', owner, body)

    def add_task_comment(self, owner, task_id, comment):
        body = {'task_id': task_id, 'comment_body': comment}
        return self._create_request('add_task_comment', owner, body)

    def abort_unfinished_jobs(self, owner, task_id, code=9):
        body = {'task_id': task_id, 'code': code}
        return self._create_request('abort_unfinished_jobs', owner, body)

    def obsolete_task(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('obsolete_task', owner, body)

    def obsolete_entity(self, owner, tasks, force=False):
        body = {'tasks': ','.join([str(e) for e in tasks]), 'force': force}
        return self._create_request('obsolete_entity', owner, body)

    def clean_task_carriages(self, owner, task_id, output_formats):
        body = {'task_id': task_id, 'output_formats': '.'.join(output_formats)}
        return self._create_request('clean_task_carriages', owner, body)

    def kill_job(self, owner, task_id, job_id, code=None, keep_unmerged=False):
        body = {'task_id': task_id, 'job_id': job_id, 'code': code, 'keep_unmerged': keep_unmerged}
        return self._create_request('kill_job', owner, body)

    def kill_jobs(self, owner, task_id, jobs, code=None, keep_unmerged=False):
        body = {'task_id': task_id,
                'jobs': ','.join([str(e) for e in jobs]),
                'code': code,
                'keep_unmerged': keep_unmerged}
        return self._create_request('kill_jobs', owner, body)

    def set_job_debug_mode(self, owner, task_id, job_id, debug_mode=True):
        body = {'task_id': task_id, 'job_id': job_id, 'debug_mode': debug_mode}
        return self._create_request('set_job_debug_mode', owner, body)

    def _search_task(self, filter_dict):
        if len(filter_dict.keys()):
            filter_dict.update({'limit': 0})
        filter_string = urllib.urlencode(filter_dict)
        url = '{0}{1}?{2}'.format(self.base_url, self.api_search_task_url, filter_string)
        response = requests.get(url, headers=self.headers, verify=self.verify_ssl_cert)
        if response.status_code == requests.codes.ok:
            return json.loads(response.content)
        else:
            raise Exception('Invalid HTTP response code: {0}'.format(response.status_code))

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

    def create_slice_tier0(self, owner, slice_dict, steps_list):
        body = {'slice_dict': slice_dict, 'steps_list': steps_list}
        return self._create_request('create_slice_tier0', owner, body)

    def set_ttcr(self, owner, ttcr_dict):
        body = {'ttcr_dict': ttcr_dict}
        return self._create_request('set_ttcr', owner, body)

    def set_ttcj(self, owner, ttcj_dict):
        body = {'ttcj_dict': ttcj_dict}
        return self._create_request('set_ttcj', owner, body)

    def reload_input(self, owner, task_id):
        body = {'task_id': task_id}
        return self._create_request('reload_input', owner, body)