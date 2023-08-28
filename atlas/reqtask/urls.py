from django.urls import  re_path
from atlas.reqtask.views import *

app_name='reqtask'


urlpatterns = [
                       re_path(r'^$', request_tasks,       name='request_tasks'),
                       re_path(r'^(?P<rid>\d+)/$', request_tasks,       name='request_tasks_rid'),
                       re_path(r'^(?P<rid>\d+)/(?P<slices>\w+)/$', request_tasks_slices,       name='request_tasks_slices'),
                       re_path(r'^hashtags/(?P<hashtag_formula>[\w|&|\-|!|\|]+)/$', tasks_hashtags, name='tasks_hashtags'),
                       re_path(r'^tasks_action/$', tasks_action, name='tasks_action'),
                       re_path(r'^get_tasks/$', get_tasks, name='get_tasks'),
                       re_path(r'^by_url/$', request_tasks_by_url, name='request_tasks_by_url'),
                       re_path(r'^recent/(?P<days>\d+)$', request_recent_tasks, name='request_recent_tasks'),
]


