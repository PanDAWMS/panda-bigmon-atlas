from django.conf import settings
from django.conf.urls.static import static

from core.common.urls import *

import atlas.settings

import atlas.common.views as atlas_common_views

common_patterns = patterns('',
    ### the front page
    url(r'^$', atlas_common_views.index, name='index'),


    ### Applications
    url(r'^htcondorjobs', include('core.htcondor.urls')),
    url(r'^job', include('core.pandajob.urls')),
    url(r'^resource', include('core.resource.urls')),
###     url(r'^api-auth', include('core.api.urls')),


    ### UI elements
    url(r'^api/datatables', include('core.table.urls')),
#    url(r'^graphics/', include('core.graphics.urls')),
#    url(r'^task/', include('core.task.urls')),


#    ### TEST/Playground
#    url(r'^test_playground/$', common_views.testing, name='testing'),
#    url(r'^htc4/$', htcondor_views.list3HTCondorJobs, name='htc4'),
#    url(r'^pan4/$', pandajob_views.list3PandaJobs, name='pan4'),


    ### Django Admin
    ### Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    ### Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),


) + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


urlpatterns = patterns('',)
urlpatterns += common_patterns

