""" 
views

"""

import logging

from django.contrib.auth.decorators import login_required
from django.shortcuts import  render

from atlas.settings import OIDC_LOGIN_URL

_logger = logging.getLogger(__name__)

# Create your views here.
@login_required(login_url=OIDC_LOGIN_URL)
def index(request):
    """
    Index page view
    
    """



    return render(request, 'atlas/_index_grid.html', {
        'active_app': 'prodtask',
        'pre_form_text': 'Prodtask home page',
    })