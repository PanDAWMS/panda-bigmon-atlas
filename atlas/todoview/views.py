""" 
views

"""

import logging
import sys, traceback
from django.shortcuts import render_to_response
from django.template import RequestContext
from core.common.utils import getPrefix, getContextVariables
from django.core.urlresolvers import reverse

#_logger = logging.getLogger(__name__)
_logger = logging.getLogger('todoview')


# Create your views here.
def todoTaskDescription(request, taskid="1"):
    """ 
        placeholder for implementation of  view with ID "TODO-task-description(taskid)":
    """
#    _logger.debug('reverse:' + str(reverse('todoview:todoTaskDescription')))
    _logger.debug('taskid:' + str(taskid))
    try:
        _logger.debug('reverse(ExtraTodoTaskDescription):' + str(reverse('ExtraTodoTaskDescription', args=(taskid,))))
    except:
        _logger.debug('reverse(ExtraTodoTaskDescription) failed:' + str(traceback.format_exc()))
    try:
        _logger.debug('reverse(todoview:todoTaskDescription):' + str(reverse('todoview:todoTaskDescription', args=(taskid,))))
    except:
        _logger.debug('reverse(todoview:todoTaskDescription) failed:' + str(traceback.format_exc()))
    data = {
            'prefix': getPrefix(request),
            'taskid': taskid,
        }
    data.update(getContextVariables(request))
    return render_to_response('todoview/todo-task-description.html', data, RequestContext(request))

