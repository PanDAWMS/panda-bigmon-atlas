from django.conf.urls import  url

from atlas.analysis_tasks.views import prepare_template_from_task, create_template, get_template, get_analysis_request, \
    save_template_changes, get_all_patterns, create_analysis_request, analysis_request_action, \
    get_analysis_request_stat, get_analysis_request_output_datasets_names
from atlas.gpdeletion.views import ListGroupProductionDeletionForUsersView, all_datasests_to_delete, extension_api, extension_container_api, group_production_datasets_full
from atlas.prodtask_api.views import *
from atlas.special_workflows.views import request_results, clone_active_learning_request
from atlas.task_action.task_management import tasks_action

app_name='prodtask_api'


urlpatterns = [


    url(r'^create_slice/$', create_slice, name='create_slice'),
    url(r'^test_api/$', test_api, name='test_api'),

    url(r'^tasks_action/$', tasks_action, name='tasks_action'),

    url(r'^gp_deletions_containers/$', ListGroupProductionDeletionForUsersView.as_view(), name='gp_deletions_containers'),
    url(r'^datasests_to_delete/$', all_datasests_to_delete, name='all_datasests_to_delete'),
    url(r'^gp_extension/$', extension_api, name='extension_api'),
    url(r'^gp_extension_period_container/$', extension_container_api, name='extension_container_api'),
    url(r'^gp_deletions_containers_cached/$', group_production_datasets_full, name='group_production_datasets_full'),
    url(r'^production_requet_results/(?P<production_request>\d+)/$', request_results, name='request_results'),
    url(r'^clone_AL_request/$', clone_active_learning_request, name='clone_active_learning_request'),
    url(r'^recreate_deleted_dataset/$', recreate_delete_dataset, name='recreate_delete_dataset'),
    url(r'^is_stage_rule_stuck_because_of_tape/$', is_stage_rule_stuck_because_of_tape,
        name='is_stage_rule_stuck_because_of_tape'),
    url(r'^prepare_template_from_task/$', prepare_template_from_task, name='prepare_template_from_task'),
    url(r'^create_template/$', create_template, name='create_template'),
    url(r'^get_template/$', get_template, name='get_template'),
    url(r'^save_template_changes/$', save_template_changes, name='save_template_changes'),
    url(r'^get_all_templates/$', get_all_patterns, name='get_all_patterns'),

    url(r'^create_analysis_request/$', create_analysis_request, name='create_analysis_request'),
    url(r'^analysis_request_action/$', analysis_request_action, name='analysis_request_action'),
    url(r'^analysis_request_stats/$', get_analysis_request_stat, name='get_analysis_request_stat'),

    url(r'^get_analysis_request/$', get_analysis_request, name='get_analysis_request'),
    url(r'^get_analysis_request_output_datasets_names/$', get_analysis_request_output_datasets_names, name='get_analysis_request_output_datasets_names'),






]