from django.conf.urls import url
from atlas.gpdeletion.views import *

app_name='gpdeletion'


urlpatterns = [
        url(r'^gpdeletions/$', ListGroupProductionDeletionView.as_view(), name='gpdeletions'),
        url(r'^extension/$', extension, name='extension'),
        url(r'^gpdetails/$', gpdetails,
            name='gpdetails'),

]


