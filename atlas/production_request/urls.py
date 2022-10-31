from django.conf.urls import url
from atlas.production_request.views import *

app_name='production_request'


urlpatterns = [

    url(r'^prepare_slice/$', prepare_slice,name='prepare_slice'),
    url(r'^steps_for_requests/$', get_steps_api,name='get_steps_api'),
    url(r'^save_slice/$', save_slice,name='save_slice'),
    url(r'^collect_steps_by_jira/$', collect_steps_by_jira,name='collect_steps_by_jira'),
    url(r'^info_by_jira/$', info_by_jira,name='info_by_jira'),
    url(r'^task/$', production_task,name='production_task'),
    url(r'^task_action_logs/$', production_task_action_logs, name='production_task_action_logs'),
    url(r'^production_task_hs06/$', production_task_hs06, name='production_task_hs06'),
    url(r'^production_error_logs/$', production_error_logs, name='production_error_logs'),
    url(r'^production_task_extensions/$', production_task_extensions, name='production_task_extensions'),
    url(r'^task_action/$', task_action, name='task_action'),
    url(r'^reassign_entities/$', get_reassign_entities, name='get_reassign_entities')


]

