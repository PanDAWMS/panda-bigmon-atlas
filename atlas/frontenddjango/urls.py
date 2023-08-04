from django.urls import  re_path
from atlas.frontenddjango.views import *

app_name='frontenddjango'


urlpatterns = [

    re_path(r'^$', index, name='index'),
    re_path(r'^(?P<path>.*)/$', index, name='index'),



]