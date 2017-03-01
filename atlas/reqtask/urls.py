from django.conf.urls import  include, url
from atlas.reqtask import views
urlpatterns = [
    url(r'^$', views.request_tasks,       name='request_tasks'),
    url(r'^(?P<rid>\d+)/$', views.request_tasks,       name='request_tasks_rid'),
    url(r'^(?P<rid>\d+)/(?P<slices>\w+)$', views.request_tasks_slices,       name='request_tasks_slices'),
    #url(r'^(?P<rid>\d+)/$|^$', 'atlas.reqtask.views.request_tasks',       name='request_tasks'),
    url(r'^tasks_action/$', views.tasks_action, name='tasks_action'),
    url(r'^get_tasks/$', views.get_tasks, name='get_tasks'),
]


