__version__ = '0.5.5'

import os.path
import io
import re
import urllib.parse
import pycurl
import html.parser
import base64
import getpass


# noinspection PyUnresolvedReferences, PyBroadException
class SSOCookies(object):
    def __init__(self, url, pem_cert_file_path=None, pem_cert_key_path=None, encoding='utf-8'):
        self.user_agent_cert = 'curl-sso-certificate/{0} (Mozilla)'.format(__version__)
        self.adfs_ep = '/adfs/ls'
        self.auth_error = 'HTTP Error 401.2 - Unauthorized'
        self.encoding = encoding

        if not pem_cert_file_path or not pem_cert_key_path:
            raise Exception('SSOCookies: certificate and/or private key file is not specified')

        if pem_cert_file_path:
            if not os.path.isfile(pem_cert_file_path):
                raise Exception('SSOCookies: certificate file {0} is not found'.format(pem_cert_file_path))
        if pem_cert_key_path:
            if not os.path.isfile(pem_cert_key_path):
                raise Exception('SSOCookies: key file {0} is not found'.format(pem_cert_key_path))

        self.curl = pycurl.Curl()
        self.curl.setopt(self.curl.COOKIEFILE, '')
        self.curl.setopt(self.curl.USERAGENT, self.user_agent_cert)
        self.curl.setopt(self.curl.SSLCERT, pem_cert_file_path)
        self.curl.setopt(self.curl.SSLCERTTYPE, 'PEM')
        self.curl.setopt(self.curl.SSLKEY, pem_cert_key_path)
        self.curl.setopt(self.curl.SSLKEYTYPE, 'PEM')
        self.curl.setopt(self.curl.FOLLOWLOCATION, 1)
        self.curl.setopt(self.curl.UNRESTRICTED_AUTH, 1)
        self.curl.setopt(self.curl.HEADER, 0)
        self.curl.setopt(self.curl.SSL_VERIFYPEER, 0)
        self.curl.setopt(self.curl.SSL_VERIFYHOST, 0)
        self.curl.setopt(self.curl.URL, url)

        _, effective_url = self._request()

        if self.adfs_ep not in effective_url:
            raise Exception('SSOCookies: the service does not support CERN SSO')

        self.curl.setopt(self.curl.URL, effective_url)

        response, effective_url = self._request()

        if self.auth_error in response:
            raise Exception('SSOCookies: authentication error')

        result = re.search('form .+?action="([^"]+)"', response)
        service_provider_url = result.groups()[0]
        form_params = re.findall('input type="hidden" name="([^"]+)" value="([^"]+)"', response)
        form_params = [(item[0], html.unescape(item[1])) for item in form_params]

        self.curl.setopt(self.curl.URL, service_provider_url)
        self.curl.setopt(self.curl.POSTFIELDS, urllib.parse.urlencode(form_params))
        self.curl.setopt(self.curl.POST, 1)

        self._request()

        self.cookie_list = self.curl.getinfo(self.curl.INFO_COOKIELIST)

    def _request(self):
        response = io.BytesIO()
        self.curl.setopt(self.curl.WRITEFUNCTION, response.write)
        self.curl.perform()
        response = response.getvalue().decode(self.encoding)
        effective_url = self.curl.getinfo(self.curl.EFFECTIVE_URL)
        return response, effective_url

    def get(self):
        cookies = {}
        for item in self.cookie_list:
            name = item.split('\t')[5]
            value = item.split('\t')[6]
            cookies.update({name: value})
        return cookies

    def extract_username(self):
        try:
            cookies = self.get()
            return base64.b64decode(cookies['FedAuth']).split(',')[1].split('\\')[-1]
        except Exception:
            return getpass.getuser()
