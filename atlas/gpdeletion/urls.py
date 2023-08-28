from django.urls import re_path
from atlas.gpdeletion.views import *

app_name='gpdeletion'


urlpatterns = [
        re_path(r'^gpdeletions/$', ListGroupProductionDeletionView.as_view(), name='gpdeletions'),
        re_path(r'^gpstats/$', ListGroupProductionStatsView.as_view(), name='gpstats'),
        re_path(r'^gpdeletionrequests/$', ListGroupProductionDeletionRequestsView.as_view(), name='gpdeletionrequests'),
        re_path(r'^gpdeletionrequestsask/$', set_datasets_to_delete,name='set_datasets_to_delete'),
        re_path(r'^extension/$', extension, name='extension'),
        re_path(r'^gpdetails/$', gpdetails,name='gpdetails'),
        re_path(r'^ami_tags_details/$', ami_tags_details,name='ami_tags_details'),
        re_path(r'^gp_container_details/$', gp_container_details,name='gp_container_details'),
        re_path(r'^last_update_time_group_production/$', last_update_time_group_production,name='last_update_time_group_production'),
        re_path(r'^gpdeletedcontainers/$', gpdeletedcontainers,
            name='gpdeletedcontainers')


]


