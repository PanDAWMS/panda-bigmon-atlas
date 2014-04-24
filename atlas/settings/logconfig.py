"""
    atlas logconfig
"""

from .local import LOG_ROOT
from core.common.settings.logconfig import LOGGING, LOG_SIZE, appendLogger

# init logger
# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
#LOG_ROOT = '/data/bigpandamon_virtualhosts/atlas/logs/'


### More Django related logging
### table_datatable
appendLogger('table_datatable')

### django_datatable
appendLogger('django_datatable')


### ProdSys2 logging
### prodtaskwebui
appendLogger('prodtaskwebui')

### postproduction
appendLogger('postproduction')


### ATLAS BigPanDAmon logging
### todoview
appendLogger('todoview')


