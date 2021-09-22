from django.conf.urls import url
from atlas.gpdeletion.views import *

app_name='gpdeletion'


urlpatterns = [
        url(r'^gpdeletions/$', ListGroupProductionDeletionView.as_view(), name='gpdeletions'),
        url(r'^gpstats/$', ListGroupProductionStatsView.as_view(), name='gpstats'),
        url(r'^gpdeletionrequests/$', ListGroupProductionDeletionRequestsView.as_view(), name='gpdeletionrequests'),
        url(r'^gpdeletionrequestsask/$', set_datasets_to_delete,name='set_datasets_to_delete'),
        url(r'^extension/$', extension, name='extension'),
        url(r'^gpdetails/$', gpdetails,name='gpdetails'),
        url(r'^ami_tags_details/$', ami_tags_details,name='ami_tags_details'),
        url(r'^gp_container_details/$', gp_container_details,name='gp_container_details'),
        url(r'^last_update_time_group_production/$', last_update_time_group_production,name='last_update_time_group_production')
]


