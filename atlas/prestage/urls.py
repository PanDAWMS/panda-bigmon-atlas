from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^step_action/(?P<wstep_id>\d+)/$', 'atlas.prestage.views.step_action',name='step_action'),
    url(r'^step_action_in_request/(?P<reqid>\d+)/$', 'atlas.prestage.views.step_action_in_request', name='step_action'),
    url(r'^prestage_by_tape/$', 'atlas.prestage.views.prestage_by_tape',name='prestage_by_tape'),
    url(r'^prestage_by_tape/(?P<reqid>\d+)/$', 'atlas.prestage.views.prestage_by_tape',name='prestage_by_tape'),
    url(r'^finish_action/(?P<action>\w+)/(?P<action_id>\d+)/$', 'atlas.prestage.views.finish_action',name='finish_action'),

)

