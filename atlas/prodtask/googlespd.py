
import logging
import httplib2
import os

from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

PATH_TO_SERVER_CREDENTIALS = '/data/client_server_criedent.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

_logger = logging.getLogger('googleSP')

class GSP(object):
    def get_credentials(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name('/data/client_server_criedent.json', SCOPES)
        return credentials


    def __init__(self):
        self.__credential = self.get_credentials()


    def update_spreadsheet(self,spreadsheetId,sheetName,range,dataToStore ):
        http = self.__credential.authorize(httplib2.Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
        service = discovery.build('sheets', 'v4', http=http,
                                  discoveryServiceUrl=discoveryUrl)
        rangeName = '%s!%s'%(sheetName,range)
        result = None
        try:
            result = service.spreadsheets().values().update(spreadsheetId=spreadsheetId, range=rangeName,
                                                            valueInputOption='RAW', body= {'values':dataToStore}).execute()
        except Exception,e:
            print str(e)
            _logger.error("Problem with google API %s",str(e))
        return result
