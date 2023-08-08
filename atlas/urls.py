from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView
from django.urls import re_path, include
from .settings import OIDC_LOGIN_URL
import atlas.settings

import atlas.auth.oidcsso.views
from django.contrib import admin
admin.autodiscover()
import atlas.common.views as atlas_common_views

app_name='prodtask'

common_patterns = [
    ### robots.txt
    re_path('^robots\.txt$', TemplateView.as_view(template_name='robots.txt', content_type='text/plain')),


    ### Applications
    re_path(r'^$', atlas_common_views.index, name='index'),

#    re_path(r'^graphics/', include('core.graphics.urls')),
    re_path(r'^prodtask/', include(('atlas.prodtask.urls','prodtask'), namespace='prodtask')),
    re_path(r'^prodjob/', include(('atlas.prodjob.urls','prodjob'), namespace='prodjob')),
    re_path(r'^reqtask/', include(('atlas.reqtask.urls','reqtask'), namespace='reqtask')),
    re_path(r'^gdpconfig/', include(('atlas.gdpconfig.urls','gdpconfig'), namespace='gdpconfig')),
    re_path(r'^getdatasets/', include(('atlas.getdatasets.urls','getdatasets'), namespace='getdatasets')),
    re_path(r'^dkb/', include(('atlas.dkb.urls','dkb'), namespace='dkb')),
    re_path(r'^ami/', include(('atlas.ami.urls','ami'), namespace='ami')),
  re_path(r'^api/', include(('atlas.prodtask_api.urls', 'prodtask_api'), namespace='prodtask_api')),
  re_path(r'^prestage/', include(('atlas.prestage.urls','prestage'), namespace='prestage')),
    re_path(r'^request_pattern/', include(('atlas.request_pattern.urls','request_pattern'), namespace='request_pattern')),
    re_path(r'^production_request/', include(('atlas.production_request.urls','production_request'), namespace='production_request')),
    re_path(r'^'+OIDC_LOGIN_URL, atlas.auth.oidcsso.views.login, name='sso_login'),

                      re_path(r'^special_workflows/',include(('atlas.special_workflows.urls', 'special_workflows'), namespace='special_workflows')),

    re_path(r'^admin/doc/', include('django.contrib.admindocs.urls')),
    re_path(r'^gpdeletion/', include(('atlas.gpdeletion.urls', 'gpdeletion'), namespace='gpdeletion')),

                      ### Uncomment the next line to enable the admin:
    re_path(r'^admin/',  admin.site.urls),
     re_path(r'^ng/',
         include(('atlas.frontenddjango.urls', 'frontenddjango'), namespace='frontenddjango')),

                  ] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


urlpatterns = common_patterns

