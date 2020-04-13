from django.conf.urls import  url
from atlas.request_pattern.views import *

app_name='request_pattern'


urlpatterns = [

    url(r'^pattern_list/$', pattern_list, name='pattern_list'),
    url(r'^clone_pattern/$', clone_pattern, name='clone_pattern'),
    url(r'^slice_pattern_steps/(?P<slice>\d+)/$', slice_pattern_steps, name='slice_pattern_steps'),
    url(r'^slice_pattern/(?P<slice>\d+)/$', slice_pattern, name='slice_pattern'),
    url(r'^slice_pattern_save_steps/(?P<slice>\d+)/$', slice_pattern_save_steps, name='slice_pattern_save_steps'),
    url(r'^pattern_list_with_obsolete/$', pattern_list_with_obsolete, name='pattern_list_with_obsolete'),

    url(r'^$', index, name='index'),

]