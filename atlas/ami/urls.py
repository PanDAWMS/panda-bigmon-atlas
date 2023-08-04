from django.urls import re_path
from atlas.ami.views import *

app_name='ami'


urlpatterns = [
    re_path(r'^ami_tag/(?P<amitag>\w+)/$', amitag,name='amitag'),
    re_path(r'^sw_containers_by_amitag/(?P<amitag>\w+)/$', sw_containers_by_amitag,name='sw_containers_by_amitag'),



]