from django.urls import re_path

from atlas.production_request.eventpicking import ep_request_stats, submit_ep_request, \
    get_ep_request, retry_ep_progress, get_ep_events, produced_datasets_list, register_ep_container
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
    re_path(r'^rule_action_logs/$', production_rule_action_logs, name='production_rule_action_logs'),

    re_path(r'^production_task_hs06/$', production_task_hs06, name='production_task_hs06'),
    re_path(r'^production_error_logs/$', production_error_logs, name='production_error_logs'),
    re_path(r'^production_task_extensions/$', production_task_extensions, name='production_task_extensions'),
    re_path(r'^task_action/$', task_action, name='task_action'),
    re_path(r'^reassign_entities/$', get_reassign_entities, name='get_reassign_entities'),
    re_path(r'^derivation_input/$', derivation_input, name='derivation_input'),
    re_path(r'^production_task_for_request/$', production_task_for_request, name='production_task_for_request'),
    re_path(r'^production_request_info/$', production_request_info, name='production_request_info'),
    re_path(r'^production_tasks_by_bigpanda_url/$', production_tasks_by_bigpanda_url, name='production_tasks_by_bigpanda_url'),
    re_path(r'^prepare_horizontal_transition/$', prepare_horizontal_transition,
            name='prepare_horizontal_transition'),
    re_path(r'^prepare_horizontal_transition/$', prepare_horizontal_transition,
            name='prepare_horizontal_transition'),
    re_path(r'^submit_horizontal_transition/$', submit_horizontal_transition_async,
            name='submit_horizontal_transition_async'),
    re_path(r'^pmg_request_verification/$', pmg_request_verification,
            name='pmg_request_verification'),
    re_path(r'^pmg_approve/$', pmg_approve,
            name='pmg_approve'),
    re_path(r'^ep_request_stats/$', ep_request_stats,
            name='ep_request_stats'),
    re_path(r'^submit_ep_request/$', submit_ep_request,
            name='submit_ep_request'),
    re_path(r'^retry_ep_progress/$', retry_ep_progress,
            name='retry_ep_progress'),
    re_path(r'^get_ep_request/$', get_ep_request,
            name='get_ep_request'),
    re_path(r'^get_ep_events/$', get_ep_events,
            name='get_ep_events'),
    re_path(r'^get_ep_result_datasets/$', produced_datasets_list,
            name='get_ep_result_datasets'),
    re_path(r'^register_ep_container/$', register_ep_container, name='register_ep_container'),

]

