from django.conf.urls import  url

from atlas.gpdeletion.views import ListGroupProductionDeletionForUsersView, all_datasests_to_delete, extension_api, extension_container_api, group_production_datasets_full
from atlas.prodtask_api.views import *
from atlas.special_workflows.views import request_results, clone_active_learning_request

app_name='prodtask_api'


urlpatterns = [


    url(r'^create_slice/$', create_slice, name='create_slice'),
    url(r'^test_api/$', test_api, name='test_api'),

    url(r'^gp_deletions_containers/$', ListGroupProductionDeletionForUsersView.as_view(), name='gp_deletions_containers'),
    url(r'^datasests_to_delete/$', all_datasests_to_delete, name='all_datasests_to_delete'),
    url(r'^gp_extension/$', extension_api, name='extension_api'),
    url(r'^gp_extension_period_container/$', extension_container_api, name='extension_container_api'),
    url(r'^gp_deletions_containers_cached/$', group_production_datasets_full, name='group_production_datasets_full'),
    url(r'^production_requet_results/(?P<production_request>\d+)/$', request_results, name='request_results'),
    url(r'^clone_AL_request/$', clone_active_learning_request, name='clone_active_learning_request'),

]