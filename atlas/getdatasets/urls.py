from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^$', 'atlas.getdatasets.views.request_data_form',       name='request_data_form'),
)


