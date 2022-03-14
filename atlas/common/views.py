""" 
views

"""

import logging
from django.shortcuts import  render


_logger = logging.getLogger(__name__)

# Create your views here.
def index(request):
    """
    Index page view
    
    """



    return render(request, 'atlas/_index_grid.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Prodtask home page',
    })