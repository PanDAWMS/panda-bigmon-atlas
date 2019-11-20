""" 
views

"""

import logging
from django.shortcuts import render_to_response, render
from django.template import RequestContext

from atlas.common.utils import getContextVariables

_logger = logging.getLogger(__name__)

# Create your views here.
def index(request):
    """
    Index page view
    
    """
    # data = {}
    # data.update(getContextVariables(request))
    # return render_to_response('atlas/_index_grid.html', data, RequestContext(request))


    return render(request, 'atlas/_index_grid.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Prodtask home page',
    })