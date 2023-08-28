from django.urls import re_path
from atlas.production_request.views import *

app_name='production_request'


urlpatterns = [

    re_path(r'^prepare_slice/$', prepare_slice,name='prepare_slice'),
    re_path(r'^steps_for_requests/$', get_steps_api,name='get_steps_api'),
    re_path(r'^save_slice/$', save_slice,name='save_slice'),
    re_path(r'^collect_steps_by_jira/$', collect_steps_by_jira,name='collect_steps_by_jira'),
    re_path(r'^info_by_jira/$', info_by_jira,name='info_by_jira'),
    re_path(r'^task/$', production_task,name='production_task'),
    re_path(r'^task_action_logs/$', production_task_action_logs, name='production_task_action_logs'),
    re_path(r'^production_task_hs06/$', production_task_hs06, name='production_task_hs06'),
    re_path(r'^production_error_logs/$', production_error_logs, name='production_error_logs'),
    re_path(r'^production_task_extensions/$', production_task_extensions, name='production_task_extensions'),
    re_path(r'^task_action/$', task_action, name='task_action'),
    re_path(r'^reassign_entities/$', get_reassign_entities, name='get_reassign_entities'),
    re_path(r'^derivation_input/$', derivation_input, name='derivation_input'),
    re_path(r'^production_task_for_request/$', production_task_for_request, name='production_task_for_request'),
    re_path(r'^production_request_info/$', production_request_info, name='production_request_info'),



]

