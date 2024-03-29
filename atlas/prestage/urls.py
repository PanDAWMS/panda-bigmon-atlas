from django.urls import re_path
from atlas.prestage.views import *

app_name='prestage'


urlpatterns = [
    re_path(r'^step_action/(?P<wstep_id>\d+)/$', step_action,name='step_action'),
    re_path(r'^step_action_in_request/(?P<reqid>\d+)/$', step_action_in_request, name='step_action_in_request'),
    re_path(r'^prestage_by_tape/$',prestage_by_tape,name='prestage_by_tape'),
    re_path(r'^prestage_by_tape_queued/$',prestage_by_tape_with_limits,name='prestage_by_tape_queued'),
    #url(r'^prestage_by_tape/(?P<reqid>\d+)/$', prestage_by_tape,name='prestage_by_tape_by_request'),
    re_path(r'^finish_action/(?P<action>\w+)/(?P<action_id>\d+)/$', finish_action,name='finish_action'),
    re_path(r'^todelete_action_in_request/(?P<reqid>\d+)/$', todelete_action_in_request,name='todelete_action_in_request'),
    re_path(r'^derivation_requests/$', derivation_requests,name='derivation_requests'),


]


