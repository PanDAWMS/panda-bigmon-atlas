from django.urls import re_path
from atlas.getdatasets.views import request_data_form

app_name='getdatasets'


urlpatterns = [
    re_path(r'^$', request_data_form,       name='request_data_form'),
]


