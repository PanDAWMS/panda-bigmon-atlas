from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^$', 'atlas.taskmon.views.request_tasks',       name='request_tasks'),
    url(r'^tasks_action/$', 'atlas.taskmon.views.tasks_action', name='tasks_action'),
    url(r'^get_tasks/$', 'atlas.taskmon.views.get_tasks', name='get_tasks'),
)


