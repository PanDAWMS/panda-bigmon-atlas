from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^$', 'atlas.gdpconfig.views.gdpconfig',       name='gdpconfig'),
    #url(r'^(?P<rid>\d+)/$', 'atlas.gdpconfig.views.request_tasks',       name='request_tasks_rid'),
    #url(r'^(?P<rid>\d+)/$|^$', 'atlas.reqtask.views.request_tasks',       name='request_tasks'),
    url(r'^config_action/(?P<action>\w+)/$', 'atlas.gdpconfig.views.config_action', name='config_action'),
    #url(r'^config_action/$', 'atlas.gdpconfig.views.config_action', name='config_action'),
    url(r'^get_config/$', 'atlas.gdpconfig.views.get_config', name='get_config'),
)


