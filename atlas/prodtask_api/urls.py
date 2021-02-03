from django.conf.urls import  url

from atlas.gpdeletion.views import ListGroupProductionDeletionForUsersView, all_datasests_to_delete
from atlas.prodtask_api.views import *

app_name='prodtask_api'


urlpatterns = [


    url(r'^create_slice/$', create_slice, name='create_slice'),
    url(r'^test_api/$', test_api, name='test_api'),

    url(r'^gp_deletions_containers/$', ListGroupProductionDeletionForUsersView.as_view(), name='gp_deletions_containers'),
    url(r'^datasests_to_delete/$', all_datasests_to_delete, name='all_datasests_to_delete'),



]