from django.conf.urls import  include, url

urlpatterns =[
    url(r'^$', 'atlas.gdpconfig.views.gdpconfig',       name='gdpconfig'),
    url(r'^fairshare/$', 'atlas.gdpconfig.views.fairshare',       name='fairshare'),
    url(r'^config_action/(?P<action>\w+)/$', 'atlas.gdpconfig.views.config_action', name='config_action'),
    url(r'^fairshare_action/(?P<action>\w+)/$', 'atlas.gdpconfig.views.fairshare_action', name='fairshare_action'),
    url(r'^get_config/$', 'atlas.gdpconfig.views.get_config', name='get_config'),
    url(r'^get_fairshare/$', 'atlas.gdpconfig.views.get_fairshare', name='get_fairshare'),
    url(r'^login/$', 'atlas.auth.views.login', name='login'),
    url(r'^logout/$', 'atlas.auth.views.logout', name='logout'),
]


