from django.conf.urls import url
from atlas.getdatasets.views import request_data_form

urlpatterns = [
    url(r'^$', request_data_form,       name='request_data_form'),
]


