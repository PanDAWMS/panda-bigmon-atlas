from django.conf.urls import  url
from atlas.special_workflows.views import *

app_name='special_workflows'


urlpatterns = [


    url(r'^$', index, name='index'),
    url(r'^idds_postproc/(?P<production_request>\d+)/$', idds_postproc, name='idds_postproc'),
    url(r'^idds_postproc_save/(?P<step_id>\d+)/$', idds_postproc_save, name='idds_postproc_save'),
    url(r'^idds_get_patterns/$', idds_get_patterns, name='idds_get_patterns'),
    url(r'^idds_tasks/(?P<production_request>\d+)/$', idds_tasks, name='idds_tasks'),


]