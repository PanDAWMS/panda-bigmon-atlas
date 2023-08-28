from django.urls import  re_path
from atlas.special_workflows.views import *

app_name='special_workflows'


urlpatterns = [


    re_path(r'^$', index, name='index'),
    re_path(r'^idds_postproc/(?P<production_request>\d+)/$', idds_postproc, name='idds_postproc'),
    re_path(r'^idds_postproc_save/(?P<step_id>\d+)/$', idds_postproc_save, name='idds_postproc_save'),
    re_path(r'^idds_get_patterns/$', idds_get_patterns, name='idds_get_patterns'),
    re_path(r'^idds_tasks/(?P<production_request>\d+)/$', idds_tasks, name='idds_tasks'),


]