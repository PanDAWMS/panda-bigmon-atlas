from django.conf.urls import  include, url
from atlas.gdpconfig import views
from atlas.auth import views as auth_view
urlpatterns =[
    url(r'^$', views.gdpconfig,       name='gdpconfig'),
    url(r'^fairshare/$', views.fairshare,       name='fairshare'),
    url(r'^config_action/(?P<action>\w+)/$', views.config_action, name='config_action'),
    url(r'^fairshare_action/(?P<action>\w+)/$', views.fairshare_action, name='fairshare_action'),
    url(r'^get_config/$', views.get_config, name='get_config'),
    url(r'^get_fairshare/$', views.get_fairshare, name='get_fairshare'),
    url(r'^login/$', auth_view.login, name='login'),
    url(r'^logout/$', auth_view.logout, name='logout'),
]


