from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^$', 'atlas.prodjob.views.request_jobs',       name='request_jobs'),
    url(r'^jobs_action/$', 'atlas.prodjob.views.jobs_action', name='jobs_action'),
)


