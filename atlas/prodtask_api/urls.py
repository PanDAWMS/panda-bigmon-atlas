from django.urls import  re_path

from atlas.analysis_tasks.views import prepare_template_from_task, create_template, get_template, get_analysis_request, \
    save_template_changes, get_all_patterns, create_analysis_request, analysis_request_action, \
    get_analysis_request_stat, get_analysis_request_output_datasets_names, get_analysis_task_preview, \
    get_analysis_pattern_view, get_derivation_slices, get_analysis_request_hashtags, \
    add_delete_analysis_request_hashtag, get_analysis_scopes_by_user
from atlas.gpdeletion.views import ListGroupProductionDeletionForUsersView, all_datasests_to_delete, extension_api, \
    extension_container_api, group_production_datasets_full, physics_container_index, create_physics_container_in_ami
from atlas.prestage.views import data_carousel_config
from atlas.prodtask_api.views import *
from atlas.production_request.views import form_pattern_for_derivation_request_extension, extend_derivation_request, \
    pattern_slices_derivation_request, mc_subcampaign_stats, production_request_outputs
from atlas.special_workflows.views import request_results, clone_active_learning_request
from atlas.task_action.task_management import tasks_action

app_name='prodtask_api'


urlpatterns = [


    re_path(r'^create_slice/$', create_slice, name='create_slice'),
    re_path(r'^test_api/$', test_api, name='test_api'),

    re_path(r'^tasks_action/$', tasks_action, name='tasks_action'),

    re_path(r'^gp_deletions_containers/$', ListGroupProductionDeletionForUsersView.as_view(), name='gp_deletions_containers'),
    re_path(r'^datasests_to_delete/$', all_datasests_to_delete, name='all_datasests_to_delete'),
    re_path(r'^gp_extension/$', extension_api, name='extension_api'),
    re_path(r'^gp_extension_period_container/$', extension_container_api, name='extension_container_api'),
    re_path(r'^gp_deletions_containers_cached/$', group_production_datasets_full, name='group_production_datasets_full'),
    re_path(r'^production_requet_results/(?P<production_request>\d+)/$', request_results, name='request_results'),
    re_path(r'^clone_AL_request/$', clone_active_learning_request, name='clone_active_learning_request'),
    re_path(r'^recreate_deleted_dataset/$', recreate_delete_dataset, name='recreate_delete_dataset'),
    re_path(r'^is_stage_rule_stuck_because_of_tape/$', is_stage_rule_stuck_because_of_tape,
        name='is_stage_rule_stuck_because_of_tape'),
    re_path(r'^prepare_template_from_task/$', prepare_template_from_task, name='prepare_template_from_task'),
    re_path(r'^create_template/$', create_template, name='create_template'),
    re_path(r'^get_template/$', get_template, name='get_template'),
    re_path(r'^save_template_changes/$', save_template_changes, name='save_template_changes'),
    re_path(r'^get_all_templates/$', get_all_patterns, name='get_all_patterns'),

    re_path(r'^create_analysis_request/$', create_analysis_request, name='create_analysis_request'),
    re_path(r'^analysis_request_action/$', analysis_request_action, name='analysis_request_action'),
    re_path(r'^analysis_request_stats/$', get_analysis_request_stat, name='get_analysis_request_stat'),

    re_path(r'^get_analysis_request/$', get_analysis_request, name='get_analysis_request'),
    re_path(r'^get_analysis_request_output_datasets_names/$', get_analysis_request_output_datasets_names, name='get_analysis_request_output_datasets_names'),
    re_path(r'^get_analysis_task_preview/$', get_analysis_task_preview, name='get_analysis_task_preview'),
    re_path(r'^get_analysis_pattern_view/$', get_analysis_pattern_view, name='get_analysis_pattern_view'),
    re_path(r'^get_derivation_slices/$', get_derivation_slices, name='get_derivation_slices'),
    re_path(r'^get_analysis_request_hashtags/$', get_analysis_request_hashtags, name='get_analysis_request_hashtags'),
    re_path(r'^get_analysis_scopes_by_user/$', get_analysis_scopes_by_user, name='get_analysis_scopes_by_user'),
    re_path(r'^add_delete_analysis_request_hashtag/$', add_delete_analysis_request_hashtag, name='add_delete_analysis_request_hashtag'),
    re_path(r'^data_carousel_config/$', data_carousel_config, name='data_carousel_config'),
    re_path(r'^form_pattern_for_derivation_request_extension/$', form_pattern_for_derivation_request_extension, name='form_pattern_for_derivation_request_extension'),
    re_path(r'^extend_derivation_request/$', extend_derivation_request, name='extend_derivation_request'),
    re_path(r'^pattern_slices_derivation_request/$', pattern_slices_derivation_request, name='pattern_slices_derivation_request'),
    re_path(r'^physics_container_index/$', physics_container_index,
            name='physics_container_index'),
    re_path(r'^mc_subcampaign_stats/$', mc_subcampaign_stats,
            name='mc_subcampaign_stats'),
    re_path(r'^create_physic_containers/$', create_physics_container_in_ami,
            name='create_physic_containers'),
    re_path(r'^deft_request/(?P<request_id>\d+)/$', deft_legacy_request,
            name='deft_legacy_request'),
    re_path(r'^production_request_outputs/(?P<requestID>\d+)/$', production_request_outputs,
            name='production_request_outputs'),
    re_path(r'^dsid_info/$', dsid_info,
            name='dsid_info'),



]