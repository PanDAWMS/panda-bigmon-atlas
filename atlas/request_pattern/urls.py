from django.urls import  re_path
from atlas.request_pattern.views import *

app_name='request_pattern'


urlpatterns = [

    re_path(r'^pattern_list/$', pattern_list, name='pattern_list'),
    re_path(r'^clone_pattern/$', clone_pattern, name='clone_pattern'),
    re_path(r'^slice_pattern_steps/(?P<slice>\d+)/$', slice_pattern_steps, name='slice_pattern_steps'),
    re_path(r'^slice_pattern/(?P<slice>\d+)/$', slice_pattern, name='slice_pattern'),
    re_path(r'^slice_pattern_save_steps/(?P<slice>\d+)/$', slice_pattern_save_steps, name='slice_pattern_save_steps'),
    re_path(r'^pattern_list_with_obsolete/$', pattern_list_with_obsolete, name='pattern_list_with_obsolete'),

    re_path(r'^$', index, name='index'),

]