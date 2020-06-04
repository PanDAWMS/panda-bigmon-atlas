from django.conf.urls import url
from atlas.ami.views import *

app_name='ami'


urlpatterns = [
    url(r'^ami_tag/(?P<amitag>\w+)/$', amitag,name='amitag'),



]