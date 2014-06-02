from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView

from core.common.urls import *

import atlas.settings

import core.pandajob.views as pandajob_views
import atlas.common.views as atlas_common_views
#import atlas.todoview.views as atlas_todo_views

common_patterns = patterns('',
    ### robots.txt
    url('^robots\.txt$', TemplateView.as_view(template_name='robots.txt', content_type='text/plain')),

    ### the front page
#obsoleted.2014-05-01.jschovan#    url(r'^$', atlas_common_views.index, name='index'),
    url(r'^$', include('core.pandajob.urls_pandajob_mainpage')),

    ### Applications
    url(r'^htcondorjobs', include('core.htcondor.urls')),
#obsoleted.2014-05-01.jschovan#    url(r'^job', include('core.pandajob.urls')),
    url(r'^jobs/', include('core.pandajob.urls_pandajob_jobs')),
    url(r'^job/', include('core.pandajob.urls_pandajob_singlejob')),
    url(r'^dash/', include('core.pandajob.urls_pandajob_dash')),
    url(r'^support/', include('core.pandajob.urls_pandajob_support')),

#obsoleted.2014-05-01.jschovan#    url(r'^u', include('core.pandajob.urls_users', namespace='user')),
    url(r'^users/', include('core.pandajob.urls_pandajob_users')),
    url(r'^user/', include('core.pandajob.urls_pandajob_singleuser')),

    url(r'^tasks/', include('core.pandajob.urls_pandajob_tasks')),
    url(r'^task/', include('core.pandajob.urls_pandajob_singletask')),

    url(r'^sites/', include('core.pandajob.urls_pandajob_sites')),
    url(r'^site/', include('core.pandajob.urls_pandajob_singlesite')),

    url(r'^resource', include('core.resource.urls')),
###     url(r'^api-auth', include('core.api.urls')),


    ### UI elements
    url(r'^api/datatables', include('core.table.urls')),
#    url(r'^graphics/', include('core.graphics.urls')),
    url(r'^prodtask/', include('atlas.prodtask.urls', namespace='prodtask')),
    url(r'^getdatasets/', include('atlas.getdatasets.urls', namespace='getdatasets')),

    url(r'^todo', include('atlas.todoview.urls', namespace='todoview')),

#    ### TEST/Playground
#    url(r'^test_playground/$', common_views.testing, name='testing'),
#    url(r'^htc4/$', htcondor_views.list3HTCondorJobs, name='htc4'),
#    url(r'^pan4/$', pandajob_views.list3PandaJobs, name='pan4'),
#    url(r'^api-jediJobsInTask', include('core.api.jedi.jobsintask.urls')),
#    url(r'^jediJobsInTask/', pandajob_views.jediJobsInTask, name='jediJobsInTask'),
#    url(r'^api/jedi/', include('core.api.jedi.jobsintask.urls')),
#    url(r'^api/user/', include('core.api.user.urls')),

#obsoleted.2014-05-01.jschovan#    url(r'^api-jedi', include('core.api.jedi.jobsintask.urls')),
#obsoleted.2014-05-01.jschovan#    url(r'^api-user', include('core.api.user.urls')),

    ### Django Admin
    ### Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    ### Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),

    ### Django.js
    url(r'^djangojs/', include('djangojs.urls')),

) + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


urlpatterns = patterns('',)
urlpatterns += common_patterns

#### django-debug-toolbar
#if settings.DEBUG:
#    import debug_toolbar
#    urlpatterns += patterns('',
#        url(r'^__debug__/', include(debug_toolbar.urls)),
#    )

