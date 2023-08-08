import logging


from django.shortcuts import render

from django.contrib.auth.decorators import login_required

from atlas.settings import OIDC_LOGIN_URL

_logger = logging.getLogger('prodtaskwebui')

@login_required(login_url=OIDC_LOGIN_URL)
def index(request, path=''):
    if request.method == 'GET':

        return render(request, 'frontenddjango/_index_frontend.html', {
                'active_app': 'ng',
                'pre_form_text': 'ng',
                'title': 'ng',
                'parent_template': '_base_ng.html',
            })