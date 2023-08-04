from django.urls import re_path

from atlas.auth.views import login,logout
from atlas.prodtask.request_views import ProductionRequestAPI
from ..prodtask.train_views import TrainLoad, TrainLoads, TrainLoadByTrain

from atlas.prodtask import views,request_views,check_duplicate,open_ended,waiting_actions,task_views,task_manage_views,\
    retryerros,step_manage_views,train_views,report_view,hashtag,selection

app_name='prodtask'


urlpatterns = [

    re_path(r'^step_template_table/$', views.step_template_table, name='step_template_table'),
    re_path(r'^step_template/(?P<rid>\d+)/$', views.step_template_details, name='step_template'),

    re_path(r'^step_execution_table/$', views.step_execution_table, name='step_execution_table'),
    re_path(r'^stepex/(?P<rid>\d+)/$', views.stepex_details, name='step_execution'),

    re_path(r'^inputlist_with_request/(?P<rid>\d+)/$', views.input_list_approve, name='input_list_approve'),
    re_path(r'^input_list_approve_full/(?P<rid>\d+)/$', views.input_list_approve_full, name='input_list_approve_full'),

    re_path(r'^production_dataset_table/$', views.production_dataset_table,    name='production_dataset_table'),
    re_path(r'^production_dataset/(?P<name>.+)/$', views.production_dataset_details,  name='production_dataset'),

    re_path(r'^request_table/$',                request_views.request_table,   name='request_table'),
    re_path(r'^request_table_js/$',               request_views.request_table_js,   name='request_table_js'),
    re_path(r'^request/(?P<rid>\d+)/$',         request_views.request_details, name='request'),
    re_path(r'^request_clone2/(?P<reqid>\d+)/$',   request_views.request_clone2,   name='request_clone2'),
    re_path(r'^request_clone_async/(?P<reqid>\d+)/$',   request_views.request_clone_async,   name='request_clone_async'),

    re_path(r'^request_update/(?P<reqid>\d+)/$',  request_views.request_update,  name='request_update'),

    re_path(r'^request_comments/(?P<reqid>\d+)/$',  request_views.request_comments,  name='request_comments'),


    re_path(r'^make_user_as_owner/(?P<reqid>\d+)/$',  request_views.make_user_as_owner,  name='make_user_as_owner'),
    re_path(r'^check_user_exists/$',  request_views.check_user_exists,  name='check_user_exists'),
    re_path(r'^request_create_new/$', request_views.request_create_new_spds, name='request_create_new_spds'),

    re_path(r'^request_create/$',               request_views.request_create,  name='request_create'),
    re_path(r'^find_datasets_by_pattern/$',     request_views.find_datasets_by_pattern,  name='find_datasets_by_pattern'),
    re_path(r'check_request_group/$',     request_views.check_request_group,  name='check_request_group'),
    re_path(r'^dpd_request_create/$',               request_views.dpd_request_create,  name='dpd_request_create'),
    re_path(r'^reprocessing_object_form/(?P<reqid>\d+)/$', request_views.reprocessing_object_form, name='reprocessing_object_form'),
    re_path(r'^eventindex_request_create/$', request_views.eventindex_request_create,  name='eventindex_request_create'),
    re_path(r'^reprocessing_request_create/$',               request_views.reprocessing_request_create,
        name='reprocessing_request_create'),
    re_path(r'^hlt_request_create/$',              request_views.hlt_request_create,
        name='hlt_request_create'),
    re_path(r'^mcpattern_table/$',                     request_views.mcpattern_table,  name='mcpattern_table'),
    re_path(r'^mcpattern_create/$',                    request_views.mcpattern_create,  name='mcpattern_create'),
    re_path(r'^mcpattern_clone/(?P<pattern_id>\d+)/$', request_views.mcpattern_create,  name='mcpattern_create'),
    re_path(r'^mcpattern_update/(?P<pattern_id>\d+)/$',request_views.mcpattern_update,  name='mcpattern_update'),

    re_path(r'^mcpriority_table/$',                     request_views.mcpriority_table,  name='mcpriority_table'),
    re_path(r'^mcpriority_create/$',                    request_views.mcpriority_create,  name='mcpriority_create'),
    re_path(r'^mcpriority_update/(?P<pattern_id>\d+)/$',request_views.mcpriority_update,  name='mcpriority_update'),

    re_path(r'^make_default_duplicate_page/$',check_duplicate.make_default_duplicate_page,  name='make_default_duplicate_page'),
    re_path(r'^make_open_ended/(?P<reqid>\d+)/$',open_ended.make_open_ended,  name='make_open_ended'),
    re_path(r'^close_open_ended/(?P<reqid>\d+)/$',open_ended.close_open_ended,  name='close_open_ended'),

    re_path(r'^push_check/(?P<reqid>\d+)/$',open_ended.push_check,  name='push_check'),
    re_path(r'^tape_load_page/$',waiting_actions.tape_load_page,  name='tape_load_page'),

    re_path(r'^short_hlt_form/$',request_views.short_hlt_form,  name='short_hlt_form'),
    re_path(r'^hlt_form_prepare_request/$',request_views.hlt_form_prepare_request,  name='hlt_form_prepare_request'),
    re_path(r'^short_valid_form/$',request_views.short_valid_form, name='short_valid_form'),
    re_path(r'^valid_form_prepare_request/$',request_views.valid_form_prepare_request, name='valid_form_prepare_request'),
    re_path(r'^status_history/(?P<reqid>\d+)/$',request_views.status_history, name='status_history'),
    re_path(r'^check_extend_request/(?P<reqid>\d+)/$',request_views.check_extend_request, name='check_extend_request'),
    re_path(r'^extend_request/(?P<reqid>\d+)/$',request_views.extend_request, name='extend_request'),
    re_path(r'^do_mc_management_approve/(?P<reqid>\d+)/$',request_views.do_mc_management_approve, name='do_mc_management_approve'),
    re_path(r'^do_mc_management_cancel/(?P<reqid>\d+)/$',request_views.do_mc_management_cancel, name='do_mc_management_cancel'),
    re_path(r'^change_production_request_status/(?P<reqid>\d+)/(?P<new_status>[\w-]+)/$',request_views.change_production_request_status, name='change_production_request_status'),

    re_path(r'^close_deft_ref/(?P<reqid>\d+)/$',request_views.close_deft_ref, name='close_deft_ref'),
    re_path(r'^task_table/$',               task_views.task_table, name='task_table'),
    re_path(r'^task/(?P<rid>\d+)/$',        task_views.task_details, name='task'),
    re_path(r'^task_old/(?P<rid>\d+)/$', task_views.task_old_details, name='task_old'),

    re_path(r'^task_clone/(?P<rid>\d+)/$',  task_views.task_clone, name='task_clone'),
    re_path(r'^task_update/(?P<rid>\d+)/$', task_views.task_update, name='task_update'),
    re_path(r'^slice_by_task/(?P<task_id>\d+)/$',        task_views.slice_by_task, name='slice_by_task'),

    re_path(r'^task_create/$',              task_views.task_create, name='task_create'),
    re_path(r'^unmerged_datasets_to_delete/$',              task_views.unmerged_datasets_to_delete, name='unmerged_datasets_to_delete'),
    re_path(r'^special_datasets_to_delete/$',              task_views.special_datasets_to_delete, name='special_datasets_to_delete'),
    re_path(r'^unmerge_datasets_not_deleted/$', task_views.unmerge_datasets_not_deleted, name='unmerge_datasets_not_deleted'),




    re_path(r'^task_stat_by_req/(?P<rid>\d+)/$', task_views.task_status_stat_by_request, name='task_status_stat_by_request'),

    re_path(r'^descent_tasks/(?P<task_id>\d+)/$', task_views.descent_tasks, name='descent_tasks'),
    re_path(r'^predefinition_action/(?P<wstep_id>\d+)/$', waiting_actions.predefinition_action, name='predefinition_action'),
    re_path(r'^finish_action/(?P<wstep_id>\d+)/$', waiting_actions.finish_action, name='finish_action'),
    re_path(r'^cancel_action/(?P<wstep_id>\d+)/$', waiting_actions.cancel_action, name='cancel_action'),
    re_path(r'^task_chain_view/(?P<task_id>\d+)/$', task_views.task_chain_view, name='task_chain_view'),
    re_path(r'^form_task_chain/(?P<task_id>\d+)/$', task_views.form_task_chain, name='form_task_chain'),
    re_path(r'^sync_request_tasks/(?P<reqid>\d+)/$', task_views.sync_request_tasks, name='sync_request_tasks'),

    re_path(r'^task_manage/$',              task_manage_views.task_manage, name='task_manage'),
    re_path(r'^task_manage/actions/(?P<action>\w+)/$', task_manage_views.tasks_action, name='tasks_action'),
    re_path(r'^task_action_ext/(?P<action>\w+)/$', task_manage_views.task_action_ext, name='task_action_ext'),
    re_path(r'^task_action_ext/$', task_manage_views.task_action_ext, name='task_action_ext'),
    re_path(r'^task_chain_obsolete_action/$', task_manage_views.task_chain_obsolete_action, name='task_chain_obsolete_action'),
    re_path(r'^task_manage/same_slice_tasks/$', task_manage_views.get_same_slice_tasks, name='same_slice_tasks'),

    re_path(r'^step_approve/(?P<stepexid>\d+)/(?P<reqid>\d+)/(?P<sliceid>\d+)/$', views.step_approve, name='step_approve'),

    re_path(r'^request_steps_approve/(?P<reqid>\d+)/(?P<approve_level>\d+)/(?P<waiting_level>\d+)/$', views.request_steps_approve,
        name='request_steps_approve'),

    re_path(r'^request_steps_approve_split/(?P<reqid>\d+)/(?P<approve_level>\d+)/(?P<waiting_level>\d+)/$', views.request_steps_approve_split,
        name='request_steps_approve_split'),
    re_path(r'^request_reprocessing_steps_create/(?P<reqid>\d+)/$', views.request_reprocessing_steps_create,name='request_reprocessing_steps_create'),
    re_path(r'^make_test_request/(?P<reqid>\d+)/$', views.make_test_request,
        name='make_test_request'),
    re_path(r'^check_slices_for_request_split/(?P<production_request>\d+)/$', views.check_slices_for_request_split,
        name='check_slices_for_request_split'),

    re_path(r'^make_request_fast/(?P<reqid>\d+)/$', views.make_request_fast,
        name='make_request_fast'),
    re_path(r'^prestage/$', views.pre_stage_approve, name='pre_stage_approve'),
    re_path(r'^ongoing_prestage/$', views.pre_stage_approved, name='pre_stage_approved'),
    re_path(r'redirect_to_value/(?P<site_name>\w+)/$', views.redirect_to_value,
        name='redirect_to_value'),
    re_path(r'^request_steps_save/(?P<reqid>\d+)/$', views.request_steps_save, name='request_steps_save'),
    re_path(r'^request_steps_save_async/(?P<reqid>\d+)/$', views.request_steps_save_async, name='request_steps_save_async'),
    re_path(r'^retry_errors_list/', retryerros.retry_errors_list, name='retry_errors_list'),
    re_path(r'^retry_errors_edit/(?P<retry_errors_id>\d+)/$', retryerros.retry_errors_edit, name='retry_errors_edit'),
    re_path(r'^retry_errors_delete/(?P<retry_errors_id>\d+)/$', retryerros.retry_errors_delete, name='retry_errors_delete'),
    re_path(r'^retry_errors_clone/(?P<retry_errors_id>\d+)/$', retryerros.retry_errors_clone, name='retry_errors_clone'),
    re_path(r'^retry_errors_create/$', retryerros.retry_errors_create, name='retry_errors_create'),
    re_path(r'^tag_info/(?P<tag_name>\w+)/$', step_manage_views.tag_info, name='tag_info'),
    re_path(r'^get_tag_formats/(?P<reqid>\d+)/$', step_manage_views.get_tag_formats, name='get_tag_formats'),
    re_path(r'^project_mode_from_tag/(?P<reqid>\d+)/$', step_manage_views.step_params_from_tag, name='project_mode_from_tag'),
    re_path(r'^update_project_mode/(?P<reqid>\d+)/$', step_manage_views.update_project_mode, name='update_project_mode'),
    re_path(r'^reject_steps/(?P<reqid>\d+)/(?P<step_filter>\w+)/$', step_manage_views.reject_steps, name='reject_steps'),
    re_path(r'^clone_slices_in_req/(?P<reqid>\d+)/(?P<step_from>[-+]?\d+)/(?P<make_link_value>[01])/$', step_manage_views.clone_slices_in_req, name='clone_slices_in_req'),
    re_path(r'^reject_slices_in_req/(?P<reqid>\d+)/$', step_manage_views.reject_slices_in_req, name='reject_slices_in_req'),
    re_path(r'^hide_slices_in_req/(?P<reqid>\d+)/$', step_manage_views.hide_slices_in_req, name='hide_slices_in_req'),
    re_path(r'^request_train_patterns/(?P<reqid>\d+)/$', step_manage_views.request_train_patterns, name='request_train_patterns'),
    re_path(r'^obsolete_old_deleted_tasks/(?P<reqid>\d+)/$', step_manage_views.obsolete_old_deleted_tasks, name='obsolete_old_deleted_tasks'),
    re_path(r'^async_obsolete_old_deleted_tasks/(?P<reqid>\d+)/$', step_manage_views.async_obsolete_old_deleted_tasks, name='async_obsolete_old_deleted_tasks'),
    re_path(r'^input_with_slice_errors/(?P<reqid>\d+)/$', step_manage_views.input_with_slice_errors, name='input_with_slice_errors'),



    re_path(r'^find_parent_slices/(?P<reqid>\d+)/(?P<parent_request>\d+)/$', step_manage_views.find_parent_slices, name='find_parent_slices'),
    re_path(r'^async_find_parent_slices/(?P<reqid>\d+)/(?P<parent_request>\d+)/$', step_manage_views.async_find_parent_slices, name='async_find_parent_slices'),


    re_path(r'^change_request_priority/(?P<reqid>\d+)/(?P<old_priority>[-+]?\d+)/(?P<new_priority>[-+]?\d+)/$', views.change_request_priority, name='change_request_priority'),

    re_path(r'^form_tasks_from_slices/(?P<request_id>\d+)/$', step_manage_views.form_tasks_from_slices, name='form_tasks_from_slices'),
    re_path(r'^test_tasks_from_slices/$', step_manage_views.test_tasks_from_slices, name='test_tasks_from_slices'),
    re_path(r'^retry_slices/(?P<reqid>\d+)/$', step_manage_views.retry_slices, name='retry_slices'),
    re_path(r'^split_slices_in_req/(?P<reqid>\d+)/$', step_manage_views.split_slices_in_req, name='split_slices_in_req'),
    re_path(r'^split_slices_by_tid/(?P<reqid>\d+)/$', step_manage_views.split_slices_by_tid, name='split_slices_by_tid'),
    re_path(r'^split_slices_by_output/(?P<reqid>\d+)/$', step_manage_views.split_slices_by_output, name='split_slices_by_output'),

    re_path(r'^add_request_comment/(?P<reqid>\d+)/$', step_manage_views.add_request_comment, name='add_request_comment'),
    re_path(r'^dataset_slice_info/(?P<reqid>\d+)/(?P<slice_number>\d+)/$', step_manage_views.dataset_slice_info, name='dataset_slice_info'),
    re_path(r'^add_request_hashtag/(?P<reqid>\d+)/$', hashtag.add_request_hashtag, name='add_request_hashtag'),
    re_path(r'^add_task_hashtag/(?P<taskid>\d+)/$', hashtag.add_task_hashtag, name='add_task_hashtag'),

    re_path(r'^test_auth_for_api/(?P<param>\w+)/$', step_manage_views.test_auth_for_api, name='test_auth_for_api'),
    re_path(r'^test_auth_for_api2/(?P<param>\w+)/$', step_manage_views.test_auth_for_api2, name='test_auth_for_api2'),
    re_path(r'^get_ami_tag_list/(?P<reqid>\d+)/$', step_manage_views.get_ami_tag_list, name='get_ami_tag_list'),
    re_path(r'^get_steps_bulk_info/(?P<reqid>\d+)/$', step_manage_views.get_steps_bulk_info, name='get_steps_bulk_info'),
    re_path(r'^set_steps_bulk_info/(?P<reqid>\d+)/$', step_manage_views.set_steps_bulk_info, name='set_steps_bulk_info'),
    re_path(r'^get_slices_bulk_info/(?P<reqid>\d+)/$', step_manage_views.get_slices_bulk_info, name='get_slices_bulk_info'),
    re_path(r'^test_celery_task/(?P<reqid>\d+)/$', step_manage_views.test_celery_task, name='test_celery_task'),
    re_path(r'^celery_task_status/(?P<celery_task_id>[\w-]+)/$', step_manage_views.celery_task_status, name='celery_task_status'),

    re_path(r'^change_parent/(?P<reqid>\d+)/(?P<new_parent>[-+]?\d+)/$', step_manage_views.change_parent, name='change_parent'),
    re_path(r'^slice_steps/(?P<reqid>\d+)/(?P<slice_number>[-+]?\d+)/$', step_manage_views.slice_steps, name='slice_steps'),

    re_path(r'^make_report/(?P<production_request_type>\w+)/(?P<number_of_days>\d)/$', report_view.make_report, name='make_report'),
    re_path(r'^make_default_report/$', report_view.make_default_report, name='make_default_report'),
    re_path(r'^find_input_datasets/(?P<reqid>\d+)/$', views.find_input_datasets, name='find_input_datasets'),
    re_path(r'^replace_relation_to_input_slices/(?P<reqid>\d+)/$', step_manage_views.replace_relation_to_input_slices, name='replace_relation_to_input_slices'),

    re_path(r'^task_about/$', views.about, name='about'),


    re_path(r'^$', views.home, name='home'),

    re_path(r'^merge_trains/$', train_views.merge_trains, name='merge_trains'),
    re_path(r'^train_luanch/$', train_views.train_luanch, name='train_luanch'),
    re_path(r'^trains_to_merge/$', train_views.trains_to_merge, name='trains_to_merge'),
    re_path(r'^train_create/$', train_views.train_create, name='train_create'),
    re_path(r'^pattern_train_list/$', train_views.pattern_train_list, name='pattern_train_list'),
    re_path(r'^create_request_as_child/$', train_views.create_request_as_child, name='create_request_as_child'),
    re_path(r'^add_pattern_to_list/$', train_views.add_pattern_to_list, name='add_pattern_to_list'),
    re_path(r'^remove_pattern_in_list/$', train_views.remove_pattern_in_list, name='remove_pattern_in_list'),

    re_path(r'^submit_child_derivation/(?P<reqid>\d+)/$', train_views.submit_child_derivation,
        name='submit_child_derivation'),
    re_path(r'^save_derivation_phys_pattern/$', train_views.save_derivation_phys_pattern,
        name='save_derivation_phys_pattern'),

    re_path(r'^get_derivation_phys_pattern/$', train_views.get_derivation_phys_pattern,
        name='get_derivation_phys_pattern'),

    re_path(r'^create_request_from_train/(?P<train_id>\d+)/$', train_views.create_request_from_train, name='create_request_from_train'),
    re_path(r'^get_pattern_from_request/(?P<reqid>\d+)/$', train_views.get_pattern_from_request, name='get_pattern_from_request'),
    re_path(r'^trains_list/$', train_views.trains_list, name='trains_list'),
    re_path(r'^trains_list_full/$', train_views.trains_list_full, name='trains_list_full'),
    re_path(r'^train/(?P<train_id>[0-9]+)/$', train_views.train_edit, name='train_edit'),
    re_path(r'^close_train/(?P<train_id>[0-9]+)/$', train_views.close_train, name='close_train'),
    re_path(r'^reopen_train/(?P<train_id>[0-9]+)/$', train_views.reopen_train, name='reopen_train'),

    re_path(r'^check_slices_for_trains/$', train_views.check_slices_for_trains, name='check_slices_for_trains'),
    re_path(r'^train_as_child/(?P<reqid>\d+)/$', train_views.train_as_child, name='train_as_child'),
    #url(r'^production_request_api/$', ProductionRequestAPI.as_view(),name='production_request_api'),

    re_path(r'^trainloads/$', TrainLoads.as_view(), name='trainloads'),
    re_path(r'^trainloads/(?P<pk>[0-9]+)/$', TrainLoad.as_view(),name='trainload'),
    re_path(r'^trainloadsbytrain/$', TrainLoadByTrain.as_view(),name='trainloadsbytrain'),
    re_path(r'^trainloadsbytrain/(?P<train>[0-9]+)/$', TrainLoadByTrain.as_view(),name='trainloadsbytrain'),
    re_path(r'^request_progress_general/(?P<reqids>[\w|,]+)/$', selection.request_progress_general, name='request_progress_general'),
    re_path(r'^request_hashtags/(?P<hashtags>[\w|,]+)/$', hashtag.request_hashtags, name='request_hashtags'),
    re_path(r'^request_progress_main/', selection.request_progress_main, name='request_progress_main'),
    re_path(r'^task_chain/', selection.task_chain, name='task_chain'),

    re_path(r'^request_hashtags_outputs/(?P<hashtag_string>[\w,\-]+)/$', hashtag.request_hashtags_main_with_hashtag, name='request_hashtags_main_with_hashtag'),

    re_path(r'^request_hashtags_main/', hashtag.request_hashtags_main, name='request_hashtags_main'),

    re_path(r'^set_hashtag_for_tasks/', hashtag.set_hashtag_for_tasks, name='set_hashtag_for_tasks'),
    re_path(r'^set_hashtag_for_containers/', hashtag.set_hashtag_for_containers, name='set_hashtag_for_containers'),

    re_path(r'^tasks_statistic_steps/', hashtag.tasks_statistic_steps, name='tasks_statistic_steps'),
    re_path(r'^request_hashtag_monk/', selection.request_hashtag_monk, name='request_hashtag_monk'),
    re_path(r'^hashtagslists/', hashtag.hashtagslists, name='hashtagslists'),
    re_path(r'^hashtags_campaign_lists/', hashtag.hashtags_campaign_lists, name='hashtags_campaign_lists'),
    re_path(r'^hashtags_by_request/(?P<reqid>\d+)/$', hashtag.hashtags_by_request, name='hashtags_by_request'),
    re_path(r'^remove_hashtag_request/(?P<reqid>\d+)/$', hashtag.remove_hashtag_request, name='remove_hashtag_request'),

    re_path(r'^campaign_steps/', hashtag.campaign_steps, name='campaign_steps'),
    re_path(r'^request_hashtags_campaign/', hashtag.request_hashtags_campaign, name='request_hashtags_campaign'),

    re_path(r'^tasks_hashtag/$', hashtag.tasks_hashtag, name='tasks_hashtag'),
    re_path(r'^tasks_requests/$', hashtag.tasks_requests, name='tasks_requests'),

    re_path(r'^assembled_train/(?P<train_id>[0-9]+)/$', train_views.assembled_train, name='assembled_train'),

    re_path(r'^login/$', login, name='login'),
    re_path(r'^logout/$', logout, name='logout'),

    re_path(r'^userinfo/$', views.userinfo, name='userinfo'),


]
