from django.conf.urls import  include, url

urlpatterns = [
    url(r'^$', 'atlas.getdatasets.views.request_data_form',       name='request_data_form'),
]


