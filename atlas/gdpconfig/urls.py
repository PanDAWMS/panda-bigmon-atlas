from django.conf.urls import url
from atlas.gdpconfig.views import *
from atlas.auth.views import login,logout

urlpatterns = [
    url(r'^$', gdpconfig, name='gdpconfig'),
    url(r'^fairshare/$', fairshare, name='fairshare'),
    url(r'^config_action/(?P<action>\w+)/$', config_action, name='config_action'),
    url(r'^fairshare_action/(?P<action>\w+)/$', fairshare_action, name='fairshare_action'),
    url(r'^get_config/$', get_config, name='get_config'),
    url(r'^get_fairshare/$', get_fairshare, name='get_fairshare'),
    url(r'^login/$', login, name='login'),
    url(r'^logout/$', logout, name='logout'),
    url(r'^global_share/$', global_share, name='global_share'),
    url(r'^global_share_tree/$', global_share_tree, name='global_share_tree'),
    url(r'^global_share_change/$', global_share_change, name='global_share_change')

]


