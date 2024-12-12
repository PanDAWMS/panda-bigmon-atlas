from django.urls import re_path
from atlas.gdpconfig.views import *
from atlas.auth.views import login,logout

app_name='gdpconfig'


urlpatterns = [
    re_path(r'^$', gdpconfig, name='gdpconfig'),
    re_path(r'^config_action/(?P<action>\w+)/$', config_action, name='config_action'),
    re_path(r'^get_config/$', get_config, name='get_config'),
    re_path(r'^login/$', login, name='login'),
    re_path(r'^logout/$', logout, name='logout'),
    re_path(r'^global_share/$', global_share, name='global_share'),
    re_path(r'^global_share_tree/$', global_share_tree, name='global_share_tree'),
    re_path(r'^global_share_change/$', global_share_change, name='global_share_change'),
    re_path(r'^get_json_param/$', get_json_param, name='get_json_param'),
    re_path(r'^save_json_param/$', save_json_param, name='save_json_param')

]


