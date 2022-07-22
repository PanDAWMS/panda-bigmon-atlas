import requests
import json
import copy
import logging

from atlas.auth.websso.ssocookies import SSOCookies
from atlas.settings.jiraclient import JIRA_CONFIG
_logger = logging.getLogger('prodtaskwebui')


class JIRAClient(object):
    def __init__(self, sso_cookies=None):
        self.sso_cookies = sso_cookies

    def authorize(self):
        try:
            self.sso_cookies = SSOCookies(
                JIRA_CONFIG['auth_url'],
                pem_cert_file_path=JIRA_CONFIG['cert'],
                pem_cert_key_path=JIRA_CONFIG['cert_key']
            ).get()
            return self.sso_cookies
        except Exception as ex:
            raise Exception('JIRAClient: SSO authentication error: {0}'.format(str(ex)))

    def create_issue(self, summary, description):
        if not self.sso_cookies:
            raise Exception('JIRAClient: not authorized')

        issue = copy.deepcopy(JIRA_CONFIG['issue_template'])
        issue['fields']['summary'] = issue['fields']['summary'] % summary
        issue['fields']['description'] = issue['fields']['description'] % description

        headers = {'Content-type': 'application/json'}

        response = requests.post(JIRA_CONFIG['issue_url'],
                                 data=json.dumps(issue),
                                 headers=headers,
                                 cookies=self.sso_cookies,
                                 verify=JIRA_CONFIG['verify_ssl_certificates'])

        if response.status_code != requests.codes.created:
            response.raise_for_status()

        result = json.loads(response.content)

        return result['key']

    def delete_issue(self, issue_key, delete_sub_issues=True):
        if not self.sso_cookies:
            raise Exception('JIRAClient: not authorized')

        issue_url = '{0}{1}?deleteSubtasks={2}'.format(
            JIRA_CONFIG['issue_url'],
            issue_key,
            str(delete_sub_issues).lower()
        )

        response = requests.delete(issue_url,
                                   cookies=self.sso_cookies,
                                   verify=JIRA_CONFIG['verify_ssl_certificates'])

        if response.status_code != requests.codes.no_content:
            response.raise_for_status()

        return True

    def create_sub_issue(self, parent_issue_key, summary, description):
        if not self.sso_cookies:
            raise Exception('JIRAClient: not authorized')

        issue = copy.deepcopy(JIRA_CONFIG['sub_issue_template'])
        issue['fields']['summary'] = issue['fields']['summary'] % summary
        issue['fields']['description'] = issue['fields']['description'] % description
        issue['fields']['parent']['key'] = issue['fields']['parent']['key'] % parent_issue_key

        headers = {'Content-type': 'application/json'}

        response = requests.post(JIRA_CONFIG['issue_url'],
                                 data=json.dumps(issue),
                                 headers=headers,
                                 cookies=self.sso_cookies,
                                 verify=JIRA_CONFIG['verify_ssl_certificates'])

        if response.status_code != requests.codes.created:
            response.raise_for_status()

        result = json.loads(response.content)

        return result['key']

    def log_exception(self, issue_key, exception, log_msg=None):
        try:
            if not log_msg:
                log_msg = '{0}: {1}'.format(type(exception).__name__, str(exception))
            _logger.exception(log_msg)
            self.add_issue_comment(issue_key, log_msg)
        except Exception as ex:
            if _logger:
                _logger.exception('log_exception failed: {0}'.format(str(ex)))

    def add_issue_comment(self, issue_key, comment_body):
        if not self.sso_cookies:
            raise Exception('JIRAClient: not authorized')

        comment = JIRA_CONFIG['issue_comment_template'].copy()
        comment['body'] = comment['body'] % comment_body

        headers = {'Content-type': 'application/json'}
        comment_url = '{0}{1}/comment'.format(JIRA_CONFIG['issue_url'], issue_key)

        response = requests.post(comment_url,
                                 data=json.dumps(comment),
                                 headers=headers,
                                 cookies=self.sso_cookies,
                                 verify=JIRA_CONFIG['verify_ssl_certificates'])

        if response.status_code != requests.codes.created:
            response.raise_for_status()

        return True

    def close_issue(self, issue_key, comment):
        if not self.sso_cookies:
            raise Exception('JIRAClient: not authorized')

        issue_close_request = copy.deepcopy(JIRA_CONFIG['issue_close_template'])
        issue_close_request['update']['comment'][0]['add']['body'] = \
            issue_close_request['update']['comment'][0]['add']['body'] % comment

        headers = {'Content-type': 'application/json'}
        transitions_url = '{0}{1}/transitions'.format(JIRA_CONFIG['issue_url'], issue_key)

        response = requests.post(transitions_url,
                                 data=json.dumps(issue_close_request),
                                 headers=headers,
                                 cookies=self.sso_cookies,
                                 verify=JIRA_CONFIG['verify_ssl_certificates'])

        if response.status_code != requests.codes.no_content:
            response.raise_for_status()

        return True