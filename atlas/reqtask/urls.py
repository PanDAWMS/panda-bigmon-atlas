from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^$', 'atlas.reqtask.views.request_tasks',       name='request_tasks'),
    url(r'^(?P<rid>\d+)/$', 'atlas.reqtask.views.request_tasks',       name='request_tasks_rid'),
    #url(r'^(?P<rid>\d+)/$|^$', 'atlas.reqtask.views.request_tasks',       name='request_tasks'),
    url(r'^tasks_action/$', 'atlas.reqtask.views.tasks_action', name='tasks_action'),
    url(r'^get_tasks/$', 'atlas.reqtask.views.get_tasks', name='get_tasks'),
)


