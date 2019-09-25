from django.conf.urls import patterns, url
from atlas.getdatasets.views import request_data_form

urlpatterns = patterns('',
    url(r'^$', request_data_form,       name='request_data_form'),
)


