from django.conf.urls import  url
from atlas.dkb.views import *

app_name='dkb'


urlpatterns = [

    url(r'^es_task_search/$', es_task_search, name='es_task_search'),
    url(r'^es_task_search_analy/$',es_task_search_analy, name='es_task_search_analy'),
    url(r'^search_string_to_url/$', search_string_to_url, name='search_string_to_url'),
    url(r'^tasks_from_list/$',tasks_from_list, name='tasks_from_list'),
    url(r'^deriv_output_proportion/(?P<project>\w+)/(?P<ami_tag>[\w|,]+)/$',deriv_output_proportion, name='deriv_output_proportion'),
    url(r'^$', index, name='index'),
    url(r'^index2/$', index2, name='index2'),
    url(r'^test_name/$',test_name, name='test_name'),
    url(r'^step_hashtag_stat/$', step_hashtag_stat, name='step_hashtag_stat'),
    url(r'^output_hashtag_stat/$', output_hashtag_stat, name='output_hashtag_stat'),
    url(r'^deriv_request_stat/$', deriv_request_stat, name='deriv_request_stat')

]


