from django.conf.urls import  url
from atlas.prodtask_api.views import *

app_name='prodtask_api'


urlpatterns = [


    url(r'^create_slice/$', create_slice, name='create_slice'),
    url(r'^test_api/$', test_api, name='test_api'),


]