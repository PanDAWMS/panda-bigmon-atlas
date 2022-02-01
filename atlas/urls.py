from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView
from django.conf.urls import *

import atlas.settings


from django.contrib import admin
admin.autodiscover()
import atlas.common.views as atlas_common_views

app_name='prodtask'

common_patterns = [
    ### robots.txt
    url('^robots\.txt$', TemplateView.as_view(template_name='robots.txt', content_type='text/plain')),


    ### Applications
    url(r'^$', atlas_common_views.index, name='index'),

#    url(r'^graphics/', include('core.graphics.urls')),
    url(r'^prodtask/', include(('atlas.prodtask.urls','prodtask'), namespace='prodtask')),
    url(r'^prodjob/', include(('atlas.prodjob.urls','prodjob'), namespace='prodjob')),
    url(r'^reqtask/', include(('atlas.reqtask.urls','reqtask'), namespace='reqtask')),
    url(r'^gdpconfig/', include(('atlas.gdpconfig.urls','gdpconfig'), namespace='gdpconfig')),
    url(r'^getdatasets/', include(('atlas.getdatasets.urls','getdatasets'), namespace='getdatasets')),
    url(r'^dkb/', include(('atlas.dkb.urls','dkb'), namespace='dkb')),
    url(r'^ami/', include(('atlas.ami.urls','ami'), namespace='ami')),
  url(r'^api/', include(('atlas.prodtask_api.urls', 'prodtask_api'), namespace='prodtask_api')),
  url(r'^prestage/', include(('atlas.prestage.urls','prestage'), namespace='prestage')),
    url(r'^request_pattern/', include(('atlas.request_pattern.urls','request_pattern'), namespace='request_pattern')),
    url(r'^production_request/', include(('atlas.production_request.urls','production_request'), namespace='production_request')),


                      url(r'^special_workflows/',include(('atlas.special_workflows.urls', 'special_workflows'), namespace='special_workflows')),

    url(r'^admin/doc/', include('django.contrib.admindocs.urls')),
    url(r'^gpdeletion/', include(('atlas.gpdeletion.urls', 'gpdeletion'), namespace='gpdeletion')),

                      ### Uncomment the next line to enable the admin:
    url(r'^admin/',  admin.site.urls),
     url(r'^ng/',
         include(('atlas.frontenddjango.urls', 'frontenddjango'), namespace='frontenddjango')),

                  ] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


urlpatterns = common_patterns

