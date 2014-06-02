
import os

DJANGO_PATH = os.path.split( os.path.split( os.path.realpath(__file__) )[0] )[0]

APP_SETTINGS = {
'prodtask.files'  : { 'status_json': #'http://atlas-project-mc-production.web.cern.ch/atlas-project-mc-production/requests/status.json' , #
                                                 DJANGO_PATH+os.sep+'prodtask'+os.sep+'status.json',
                    'panda_links': 'D:/DEV/deft-ui/branches/sgayazov/bigpandamon/prodtask/panda_links.csv'},
'prodtask.auth'   : { 'user': 'bigpandamontestuser',
                    'password': 'Y8NLCmjHROqMIRWk'},
'prodtask.default.email.list' : ['mborodin@cern.ch','dmitry.v.golubkov@cern.ch'],
'prodtask.email.from' : 'mborodin@cern.ch'
}
