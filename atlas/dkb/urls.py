from django.conf.urls import patterns, include, url

urlpatterns = patterns('',

    url(r'^es_task_search/$', 'atlas.dkb.views.es_task_search', name='es_task_search'),
    url(r'^es_task_search_analy/$', 'atlas.dkb.views.es_task_search_analy', name='es_task_search_analy'),
    url(r'^search_string_to_url/$', 'atlas.dkb.views.search_string_to_url', name='search_string_to_url'),
    url(r'^tasks_from_list/$', 'atlas.dkb.views.tasks_from_list', name='tasks_from_list'),
    url(r'^deriv_output_proportion/(?P<project>\w+)/(?P<ami_tag>[\w|,]+)$', 'atlas.dkb.views.deriv_output_proportion', name='deriv_output_proportion'),

    url(r'^$', 'atlas.dkb.views.index', name='index'),
    url(r'^index2/$', 'atlas.dkb.views.index2', name='index2'),
    url(r'^test_name/$', 'atlas.dkb.views.test_name', name='test_name'),
    url(r'^step_hashtag_stat/$', 'atlas.dkb.views.step_hashtag_stat', name='step_hashtag_stat'),
    url(r'^output_hashtag_stat/$', 'atlas.dkb.views.output_hashtag_stat', name='output_hashtag_stat'),
    url(r'^deriv_request_stat/$', 'atlas.dkb.views.deriv_request_stat', name='deriv_request_stat')

)


