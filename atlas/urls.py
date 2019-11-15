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
    url(r'^prestage/', include(('atlas.prestage.urls','prestage'), namespace='prestage')),

    url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    ### Uncomment the next line to enable the admin:
    url(r'^admin/',  admin.site.urls),


] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


urlpatterns = common_patterns

