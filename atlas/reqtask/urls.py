from django.conf.urls import  url
from atlas.reqtask.views import *

app_name='reqtask'


urlpatterns = [
                       url(r'^$', request_tasks,       name='request_tasks'),
                       url(r'^(?P<rid>\d+)/$', request_tasks,       name='request_tasks_rid'),
                       url(r'^(?P<rid>\d+)/(?P<slices>\w+)/$', request_tasks_slices,       name='request_tasks_slices'),
                       url(r'^hashtags/(?P<hashtag_formula>[\w|&|\-|!|\|]+)/$', tasks_hashtags, name='tasks_hashtags'),
                       url(r'^tasks_action/$', tasks_action, name='tasks_action'),
                       url(r'^get_tasks/$', get_tasks, name='get_tasks'),
                       url(r'^by_url/$', request_tasks_by_url, name='request_tasks_by_url'),
                       url(r'^recent/(?P<days>\d+)$', request_recent_tasks, name='request_recent_tasks'),
]


