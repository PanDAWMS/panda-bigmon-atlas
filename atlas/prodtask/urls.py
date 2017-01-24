from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()
from ..prodtask.train_views import TrainLoad, TrainLoads
# from ..prodtask.selection import request_progress_general
urlpatterns = patterns('',
    # Examples:

    #url(r'^syncButton/(?P<rid>\d+)/$', 'atlas.prodtask.views.syncButton', name='syncButton'),

    url(r'^step_template_table/$', 'atlas.prodtask.views.step_template_table', name='step_template_table'),
    url(r'^step_template/(?P<rid>\d+)/$', 'atlas.prodtask.views.step_template_details', name='step_template'),
    
    url(r'^step_execution_table/$', 'atlas.prodtask.views.step_execution_table', name='step_execution_table'),
    url(r'^stepex/(?P<rid>\d+)/$', 'atlas.prodtask.views.stepex_details', name='step_execution'),
    
    url(r'^inputlist_with_request/(?P<rid>\d+)/$', 'atlas.prodtask.views.input_list_approve', name='input_list_approve'),
    url(r'^input_list_approve_full/(?P<rid>\d+)/$', 'atlas.prodtask.views.input_list_approve_full', name='input_list_approve_full'),

    #url(r'^taskpriority_table/$', 'task.views.taskpriority_table', name='taskpriority_table'),

    url(r'^production_dataset_table/$', 'atlas.prodtask.views.production_dataset_table',    name='production_dataset_table'),
    url(r'^production_dataset/(?P<name>.+)/$', 'atlas.prodtask.views.production_dataset_details',  name='production_dataset'),
    
    url(r'^request_table/$',                'atlas.prodtask.request_views.request_table',   name='request_table'),
    url(r'^request/(?P<rid>\d+)/$',         'atlas.prodtask.request_views.request_details', name='request'),
    #url(r'^request_clone/(?P<reqid>\d+)/$',   'atlas.prodtask.request_views.request_clone',   name='request_clone'),
    url(r'^request_clone2/(?P<reqid>\d+)/$',   'atlas.prodtask.request_views.request_clone2',   name='request_clone2'),
    url(r'^request_update/(?P<reqid>\d+)/$',  'atlas.prodtask.request_views.request_update',  name='request_update'),

    url(r'^request_comments/(?P<reqid>\d+)/$',  'atlas.prodtask.request_views.request_comments',  name='request_comments'),


    url(r'^make_user_as_owner/(?P<reqid>\d+)/$',  'atlas.prodtask.request_views.make_user_as_owner',  name='make_user_as_owner'),

    url(r'^request_create/$',               'atlas.prodtask.request_views.request_create',  name='request_create'),
    url(r'^find_datasets_by_pattern/$',     'atlas.prodtask.request_views.find_datasets_by_pattern',  name='find_datasets_by_pattern'),

    url(r'^dpd_request_create/$',               'atlas.prodtask.request_views.dpd_request_create',  name='dpd_request_create'),
    url(r'^reprocessing_object_form/(?P<reqid>\d+)/$',               'atlas.prodtask.request_views.reprocessing_object_form',
        name='reprocessing_object_form'),
    url(r'^eventindex_request_create/$',               'atlas.prodtask.request_views.eventindex_request_create',  name='eventindex_request_create'),
    url(r'^reprocessing_request_create/$',               'atlas.prodtask.request_views.reprocessing_request_create',
        name='reprocessing_request_create'),
    url(r'^hlt_request_create/$',               'atlas.prodtask.request_views.hlt_request_create',
        name='hlt_request_create'),
    url(r'^mcpattern_table/$',                     'atlas.prodtask.request_views.mcpattern_table',  name='mcpattern_table'),
    url(r'^mcpattern_create/$',                    'atlas.prodtask.request_views.mcpattern_create',  name='mcpattern_create'),
    url(r'^mcpattern_clone/(?P<pattern_id>\d+)/$', 'atlas.prodtask.request_views.mcpattern_create',  name='mcpattern_create'),
    url(r'^mcpattern_update/(?P<pattern_id>\d+)/$','atlas.prodtask.request_views.mcpattern_update',  name='mcpattern_update'),

    url(r'^mcpriority_table/$',                     'atlas.prodtask.request_views.mcpriority_table',  name='mcpriority_table'),
    url(r'^mcpriority_create/$',                    'atlas.prodtask.request_views.mcpriority_create',  name='mcpriority_create'),
    url(r'^mcpriority_update/(?P<pattern_id>\d+)/$','atlas.prodtask.request_views.mcpriority_update',  name='mcpriority_update'),

    url(r'^make_default_duplicate_page/$','atlas.prodtask.check_duplicate.make_default_duplicate_page',  name='make_default_duplicate_page'),
    url(r'^make_open_ended/(?P<reqid>\d+)/$','atlas.prodtask.open_ended.make_open_ended',  name='make_open_ended'),
    url(r'^close_open_ended/(?P<reqid>\d+)/$','atlas.prodtask.open_ended.close_open_ended',  name='close_open_ended'),

    url(r'^push_check/(?P<reqid>\d+)/$','atlas.prodtask.open_ended.push_check',  name='push_check'),
    url(r'^short_hlt_form/$','atlas.prodtask.request_views.short_hlt_form',  name='short_hlt_form'),
    url(r'^hlt_form_prepare_request/$','atlas.prodtask.request_views.hlt_form_prepare_request',  name='hlt_form_prepare_request'),
    url(r'^short_valid_form/$','atlas.prodtask.request_views.short_valid_form',  name='short_valid_form'),
    url(r'^valid_form_prepare_request/$','atlas.prodtask.request_views.valid_form_prepare_request',  name='valid_form_prepare_request'),
    url(r'^status_history/(?P<reqid>\d+)/$','atlas.prodtask.request_views.status_history',  name='status_history'),
    url(r'^check_extend_request/(?P<reqid>\d+)/$','atlas.prodtask.request_views.check_extend_request',  name='check_extend_request'),
    url(r'^extend_request/(?P<reqid>\d+)/$','atlas.prodtask.request_views.extend_request',  name='extend_request'),
    url(r'^do_mc_management_approve/(?P<reqid>\d+)/$','atlas.prodtask.request_views.do_mc_management_approve',  name='do_mc_management_approve'),
    url(r'^do_mc_management_cancel/(?P<reqid>\d+)/$','atlas.prodtask.request_views.do_mc_management_cancel',  name='do_mc_management_cancel'),
    url(r'^change_production_request_status/(?P<reqid>\d+)/(?P<new_status>[\w-]+)$','atlas.prodtask.request_views.change_production_request_status',  name='change_production_request_status'),

    url(r'^close_deft_ref/(?P<reqid>\d+)/$','atlas.prodtask.request_views.close_deft_ref',  name='close_deft_ref'),
    url(r'^task_table/$',               'atlas.prodtask.task_views.task_table',         name='task_table'),
    url(r'^task/(?P<rid>\d+)/$',        'atlas.prodtask.task_views.task_details',       name='task'),
    url(r'^task_clone/(?P<rid>\d+)/$',  'atlas.prodtask.task_views.task_clone',         name='task_clone'),
    url(r'^task_update/(?P<rid>\d+)/$', 'atlas.prodtask.task_views.task_update',        name='task_update'),
    url(r'^task_create/$',              'atlas.prodtask.task_views.task_create',        name='task_create'),
    url(r'^task_stat_by_req/(?P<rid>\d+)/$', 'atlas.prodtask.task_views.task_status_stat_by_request',        name='task_status_stat_by_request'),

    url(r'^descent_tasks/(?P<task_id>\d+)/$', 'atlas.prodtask.task_views.descent_tasks', name='descent_tasks'),
    
    url(r'^task_manage/$',              'atlas.prodtask.task_manage_views.task_manage', name='task_manage'),
    url(r'^task_manage/actions/(?P<action>\w+)/$', 'atlas.prodtask.task_manage_views.tasks_action', name='tasks_action'),
    url(r'^task_action_ext/(?P<action>\w+)/$', 'atlas.prodtask.task_manage_views.task_action_ext', name='task_action_ext'),
    url(r'^task_action_ext/$', 'atlas.prodtask.task_manage_views.task_action_ext', name='task_action_ext'),
    url(r'^task_manage/same_slice_tasks/$', 'atlas.prodtask.task_manage_views.get_same_slice_tasks', name='same_slice_tasks'),

    url(r'^step_approve/(?P<stepexid>\d+)/(?P<reqid>\d+)/(?P<sliceid>\d+)/$', 'atlas.prodtask.views.step_approve', name='step_approve'),

    url(r'^request_steps_approve/(?P<reqid>\d+)/(?P<approve_level>\d+)/(?P<waiting_level>\d+)/$', 'atlas.prodtask.views.request_steps_approve',
        name='request_steps_approve'),

    url(r'^request_steps_approve_split/(?P<reqid>\d+)/(?P<approve_level>\d+)/(?P<waiting_level>\d+)/$', 'atlas.prodtask.views.request_steps_approve_split',
        name='request_steps_approve_split'),
    url(r'^request_reprocessing_steps_create/(?P<reqid>\d+)/$', 'atlas.prodtask.views.request_reprocessing_steps_create',
        name='request_reprocessing_steps_create'),
    url(r'^make_test_request/(?P<reqid>\d+)/$', 'atlas.prodtask.views.make_test_request',
        name='make_test_request'),
    url(r'^check_slices_for_request_split/(?P<production_request>\d+)/$', 'atlas.prodtask.views.check_slices_for_request_split',
        name='check_slices_for_request_split'),

    url(r'^make_request_fast/(?P<reqid>\d+)/$', 'atlas.prodtask.views.make_request_fast',
        name='make_request_fast'),

    url(r'redirect_to_value/(?P<site_name>\w+)/$', 'atlas.prodtask.views.redirect_to_value',
        name='redirect_to_value'),
    url(r'^request_steps_save/(?P<reqid>\d+)/$', 'atlas.prodtask.views.request_steps_save', name='request_steps_save'),

    url(r'^retry_errors_list/', 'atlas.prodtask.retryerros.retry_errors_list', name='retry_errors_list'),
    url(r'^retry_errors_edit/(?P<retry_errors_id>\d+)/$', 'atlas.prodtask.retryerros.retry_errors_edit', name='retry_errors_edit'),
    url(r'^retry_errors_delete/(?P<retry_errors_id>\d+)/$', 'atlas.prodtask.retryerros.retry_errors_delete', name='retry_errors_delete'),
    url(r'^retry_errors_clone/(?P<retry_errors_id>\d+)/$', 'atlas.prodtask.retryerros.retry_errors_clone', name='retry_errors_clone'),
    url(r'^retry_errors_create/$', 'atlas.prodtask.retryerros.retry_errors_create', name='retry_errors_create'),
    url(r'^tag_info/(?P<tag_name>\w+)/$', 'atlas.prodtask.step_manage_views.tag_info', name='tag_info'),
    url(r'^get_tag_formats/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.get_tag_formats', name='get_tag_formats'),
    url(r'^project_mode_from_tag/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.step_params_from_tag', name='project_mode_from_tag'),
    url(r'^update_project_mode/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.update_project_mode', name='update_project_mode'),
    url(r'^reject_steps/(?P<reqid>\d+)/(?P<step_filter>\w+)/$', 'atlas.prodtask.step_manage_views.reject_steps', name='reject_steps'),
    url(r'^clone_slices_in_req/(?P<reqid>\d+)/(?P<step_from>[-+]?\d+)/(?P<make_link_value>[01])/$', 'atlas.prodtask.step_manage_views.clone_slices_in_req', name='clone_slices_in_req'),
    url(r'^reject_slices_in_req/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.reject_slices_in_req', name='reject_slices_in_req'),
    url(r'^hide_slices_in_req/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.hide_slices_in_req', name='hide_slices_in_req'),
    url(r'^change_request_priority/(?P<reqid>\d+)/(?P<old_priority>[-+]?\d+)/(?P<new_priority>[-+]?\d+)/$', 'atlas.prodtask.views.change_request_priority', name='change_request_priority'),

    url(r'^form_tasks_from_slices/(?P<request_id>\d+)/$', 'atlas.prodtask.step_manage_views.form_tasks_from_slices', name='form_tasks_from_slices'),
    url(r'^test_tasks_from_slices/$', 'atlas.prodtask.step_manage_views.test_tasks_from_slices', name='test_tasks_from_slices'),
    url(r'^retry_slices/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.retry_slices', name='retry_slices'),
    url(r'^split_slices_in_req/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.split_slices_in_req', name='split_slices_in_req'),
    url(r'^add_request_comment/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.add_request_comment', name='add_request_comment'),
    url(r'^add_request_hashtag/(?P<reqid>\d+)/$', 'atlas.prodtask.hashtag.add_request_hashtag', name='add_request_hashtag'),
    url(r'^test_auth_for_api/(?P<param>\w+)/$', 'atlas.prodtask.step_manage_views.test_auth_for_api', name='test_auth_for_api'),
    url(r'^test_auth_for_api2/(?P<param>\w+)/$', 'atlas.prodtask.step_manage_views.test_auth_for_api2', name='test_auth_for_api2'),
    url(r'^get_ami_tag_list/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.get_ami_tag_list', name='get_ami_tag_list'),
    url(r'^get_steps_bulk_info/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.get_steps_bulk_info', name='get_steps_bulk_info'),
    url(r'^set_steps_bulk_info/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.set_steps_bulk_info', name='set_steps_bulk_info'),
    url(r'^get_slices_bulk_info/(?P<reqid>\d+)/$', 'atlas.prodtask.step_manage_views.get_slices_bulk_info', name='get_slices_bulk_info'),

    url(r'^slice_steps/(?P<reqid>\d+)/(?P<slice_number>[-+]?\d+)/$', 'atlas.prodtask.step_manage_views.slice_steps', name='slice_steps'),

    url(r'^make_report/(?P<production_request_type>\w+)/(?P<number_of_days>\d)/$', 'atlas.prodtask.report_view.make_report', name='make_report'),
    url(r'^make_default_report/$', 'atlas.prodtask.report_view.make_default_report', name='make_default_report'),
    url(r'^find_input_datasets/(?P<reqid>\d+)/$', 'atlas.prodtask.views.find_input_datasets', name='find_input_datasets'),
    url(r'^task_about/$', 'atlas.prodtask.views.about', name='about'),


    url(r'^$', 'atlas.prodtask.views.home', name='home'),
    url(r'^train_create/$', 'atlas.prodtask.train_views.train_create', name='train_create'),
    url(r'^pattern_train_list/$', 'atlas.prodtask.train_views.pattern_train_list', name='pattern_train_list'),
    url(r'^create_request_as_child/$', 'atlas.prodtask.train_views.create_request_as_child', name='create_request_as_child'),
    url(r'^add_pattern_to_list/$', 'atlas.prodtask.train_views.add_pattern_to_list', name='add_pattern_to_list'),
    url(r'^remove_pattern_in_list/$', 'atlas.prodtask.train_views.remove_pattern_in_list', name='remove_pattern_in_list'),

    url(r'^create_request_from_train/(?P<train_id>\d+)/$', 'atlas.prodtask.train_views.create_request_from_train', name='create_request_from_train'),
    url(r'^get_pattern_from_request/(?P<reqid>\d+)/$', 'atlas.prodtask.train_views.get_pattern_from_request', name='get_pattern_from_request'),
    url(r'^trains_list/$', 'atlas.prodtask.train_views.trains_list', name='trains_list'),
    url(r'^trains_list_full/$', 'atlas.prodtask.train_views.trains_list_full', name='trains_list_full'),
    url(r'^train/(?P<train_id>[0-9]+)/$', 'atlas.prodtask.train_views.train_edit', name='train_edit'),
    url(r'^close_train/(?P<train_id>[0-9]+)/$', 'atlas.prodtask.train_views.close_train', name='close_train'),
    url(r'^reopen_train/(?P<train_id>[0-9]+)/$', 'atlas.prodtask.train_views.reopen_train', name='reopen_train'),

    url(r'^check_slices_for_trains/$', 'atlas.prodtask.train_views.check_slices_for_trains', name='check_slices_for_trains'),
    url(r'^train_as_child/(?P<reqid>\d+)/$', 'atlas.prodtask.train_views.train_as_child', name='train_as_child'),


    url(r'^trainloads/$', TrainLoads.as_view(), name='trainloads'),
    url(r'^trainloads/(?P<pk>[0-9]+)/$', TrainLoad.as_view(),name='trainload'),
    url(r'^request_progress_general/(?P<reqids>[\w|,]+)/$', 'atlas.prodtask.selection.request_progress_general', name='request_progress_general'),
    url(r'^request_hashtags/(?P<hashtags>[\w|,]+)/$', 'atlas.prodtask.hashtag.request_hashtags', name='request_hashtags'),
    url(r'^request_progress_main/', 'atlas.prodtask.selection.request_progress_main', name='request_progress_main'),
    url(r'^request_hashtags_main/', 'atlas.prodtask.hashtag.request_hashtags_main', name='request_hashtags_main'),
    url(r'^tasks_statistic_steps/', 'atlas.prodtask.hashtag.tasks_statistic_steps', name='tasks_statistic_steps'),
    url(r'^request_hashtag_monk/', 'atlas.prodtask.selection.request_hashtag_monk', name='request_hashtag_monk'),
    url(r'^hashtagslists/', 'atlas.prodtask.hashtag.hashtagslists', name='hashtagslists'),
    url(r'^tasks_hashtag/$', 'atlas.prodtask.hashtag.tasks_hashtag', name='tasks_hashtag'),
    url(r'^tasks_requests/$', 'atlas.prodtask.hashtag.tasks_requests', name='tasks_requests'),

    url(r'^assembled_train/(?P<train_id>[0-9]+)/$', 'atlas.prodtask.train_views.assembled_train', name='assembled_train'),

    url(r'^login/$', 'atlas.auth.views.login', name='login'),
    url(r'^logout/$', 'atlas.auth.views.logout', name='logout'),

    url(r'^userinfo/$', 'atlas.prodtask.views.userinfo', name='userinfo'),


    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    #url(r'^admin/', include(admin.site.urls)),
)
