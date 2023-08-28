from django.urls import  re_path
from atlas.dkb.views import *

app_name='dkb'


urlpatterns = [

    re_path(r'^es_task_search/$', es_task_search, name='es_task_search'),
    re_path(r'^es_task_search_analy/$',es_task_search_analy, name='es_task_search_analy'),
    re_path(r'^search_string_to_url/$', search_string_to_url, name='search_string_to_url'),
    re_path(r'^tasks_from_list/$',tasks_from_list, name='tasks_from_list'),
    re_path(r'^deriv_output_proportion/(?P<project>\w+)/(?P<ami_tag>[\w|,]+)/$',deriv_output_proportion, name='deriv_output_proportion'),
    re_path(r'^$', index, name='index'),
    re_path(r'^test_name/$',test_name, name='test_name'),
    re_path(r'^output_hashtag_stat/$', output_hashtag_stat, name='output_hashtag_stat'),

]


