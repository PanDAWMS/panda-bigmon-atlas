""" 
views

"""

import logging
from django.shortcuts import render_to_response
from django.template import RequestContext
from core.common.utils import getContextVariables

_logger = logging.getLogger(__name__)

# Create your views here.
def index(request):
    """
    Index page view
    
    """
    data = {}
    data.update(getContextVariables(request))
    return render_to_response('atlas/_index_grid.html', data, RequestContext(request))


