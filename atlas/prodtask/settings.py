
import os
from django.conf import settings
DJANGO_PATH = os.path.split( os.path.split( os.path.realpath(__file__) )[0] )[0]

APP_SETTINGS = {
'prodtask.files'  : { 'status_json': #'http://atlas-project-mc-production.web.cern.ch/atlas-project-mc-production/requests/status.json' , #
                                                 DJANGO_PATH+os.sep+'prodtask'+os.sep+'status.json',
                    'panda_links': 'D:/DEV/deft-ui/branches/sgayazov/bigpandamon/prodtask/panda_links.csv'},

'prodtask.default.email.list' : settings.ADMIN_MAILS,
'prodtask.email.from' : settings.ADMIN_MAILS[0]
}
