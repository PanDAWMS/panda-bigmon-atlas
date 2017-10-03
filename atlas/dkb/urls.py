from django.conf.urls import patterns, include, url

urlpatterns = patterns('',

    url(r'^es_task_search/$', 'atlas.dkb.views.es_task_search', name='es_task_search'),
    url(r'^search_string_to_url/$', 'atlas.dkb.views.search_string_to_url', name='search_string_to_url'),

    url(r'^$', 'atlas.dkb.views.index', name='index')

)


