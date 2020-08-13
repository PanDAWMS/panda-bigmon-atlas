from django.conf.urls import url
from atlas.ami.views import *

app_name='ami'


urlpatterns = [
    url(r'^ami_tag/(?P<amitag>\w+)/$', amitag,name='amitag'),
    url(r'^sw_containers_by_amitag/(?P<amitag>\w+)/$', sw_containers_by_amitag,name='sw_containers_by_amitag'),



]