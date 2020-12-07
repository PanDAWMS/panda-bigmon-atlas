from django.conf.urls import  url
from atlas.frontenddjango.views import *

app_name='frontenddjango'


urlpatterns = [

    url(r'^$', index, name='index'),
    url(r'^(?P<path>.*)/$', index, name='index'),



]