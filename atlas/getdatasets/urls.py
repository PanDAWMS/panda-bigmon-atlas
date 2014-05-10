from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^req22/', 'atlas.getdatasets.views.request_data_form_to_table2',       name='request_data_form_to_table2'),
    url(r'^req2/', 'atlas.getdatasets.views.request_data_form_to_table',       name='request_data_form_to_table'),
    url(r'^req/', 'atlas.getdatasets.views.request_data',       name='request_data'),
    url(r'^db/', 'atlas.getdatasets.views.request_data_table',       name='request_data_table'),
    url(r'^dq2/', 'atlas.getdatasets.views.request_data_dq2',        name='request_data_dq2'),
)


