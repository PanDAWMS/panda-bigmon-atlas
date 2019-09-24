from django.conf.urls import patterns, include, url
from atlas.prestage.views import *

urlpatterns = patterns('',
    url(r'^step_action/(?P<wstep_id>\d+)/$', step_action,name='step_action'),
    url(r'^step_action_in_request/(?P<reqid>\d+)/$', step_action_in_request, name='step_action'),
    url(r'^prestage_by_tape/$',prestage_by_tape,name='prestage_by_tape'),
    url(r'^prestage_by_tape/(?P<reqid>\d+)/$', prestage_by_tape,name='prestage_by_tape'),
    url(r'^finish_action/(?P<action>\w+)/(?P<action_id>\d+)/$', finish_action,name='finish_action'),

)


