from django.conf.urls import url
from atlas.production_request.views import *

app_name='production_request'


urlpatterns = [

    url(r'^prepare_slice/$', prepare_slice,name='prepare_slice'),
    url(r'^steps_for_requests/$', get_steps_api,name='get_steps_api'),
    url(r'^save_slice/$', save_slice,name='save_slice'),
    url(r'^collect_steps_by_jira/$', collect_steps_by_jira,name='collect_steps_by_jira'),
    url(r'^info_by_jira/$', info_by_jira,name='info_by_jira')


]

