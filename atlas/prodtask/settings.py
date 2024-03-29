
import os
from atlas.settings.local import  ADMIN_MAILS
DJANGO_PATH = os.path.split( os.path.split( os.path.realpath(__file__) )[0] )[0]

APP_SETTINGS = {
'prodtask.files'  : { 'status_json': #'http://atlas-project-mc-production.web.cern.ch/atlas-project-mc-production/requests/status.json' , #
                                                 DJANGO_PATH+os.sep+'prodtask'+os.sep+'status.json',
                    'panda_links': 'D:/DEV/deft-ui/branches/sgayazov/bigpandamon/prodtask/panda_links.csv'},

'prodtask.default.email.list' : ADMIN_MAILS,
'prodtask.email.from' : ADMIN_MAILS[0]
}
