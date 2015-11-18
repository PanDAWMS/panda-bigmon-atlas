from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^$', 'atlas.taskmon.views.request_jobs',       name='request_jobs'),
    url(r'^jobs_action/$', 'atlas.taskmon.views.jobs_action', name='jobs_action'),
    url(r'^get_tasks/$', 'atlas.taskmon.views.get_tasks', name='get_tasks'),
)


