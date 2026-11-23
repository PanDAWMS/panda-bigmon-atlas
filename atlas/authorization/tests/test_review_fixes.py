import ast
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from atlas.authorization.tests.support import ATLAS, functions, task, task_manager, user
from atlas.authorization import principals, resources, service
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import PermissionDenied


class DelegationTests(unittest.TestCase):
    def test_external_service_can_act_for_analysis_user_without_local_account(self):
        anonymous = AnonymousUser()
        manager = task_manager(anonymous, {1: task(owner='Asserted Full Name', name='user.unrelated.task', is_analy=True)})
        execute = Mock(return_value=(True, 'ok'))
        ns = functions('prodtask/task_manage_views.py', ['task_action_ext'],
            ALLOWED_FOR_EXTERNAL_API=['abort', 'finish', 'change_priority', 'retry'],
            TaskManagementAuthorisation=lambda: manager, TaskActionExecutor=Mock(), do_jedi_action=execute,
            Response=lambda data: data, HttpResponseForbidden=lambda: 'forbidden',
            logger=Mock(), _jsonLogger=Mock())
        request = SimpleNamespace(user=user('bigpanda_api'), data={
            'task': 1, 'username': 'remote-user', 'userfullname': 'Asserted Full Name', 'parameters': []})
        with patch.object(principals, 'user_for_username', return_value=anonymous), patch.object(service, 'logger') as log:
            self.assertEqual(ns.task_action_ext(request, 'abort')['result'], 'OK')
            execute.assert_called_once()
            self.assertEqual(log.mock_calls, [])
            execute.reset_mock()
            request.user = user('ordinary-user', superuser=True)
            self.assertEqual(ns.task_action_ext(request, 'abort'), 'forbidden')
            execute.assert_not_called()
            request.user = user('bigpanda_api')
            request.data['userfullname'] = 'Wrong Name'
            self.assertEqual(ns.task_action_ext(request, 'abort')['result'], 'FAILED')
            execute.assert_not_called()
            decision = log.info.call_args.kwargs['extra']
            self.assertEqual((decision['actor'], decision['effective_user']), ('bigpanda_api', 'remote-user'))
            self.assertNotIn('Asserted Full Name', str(decision))
            self.assertNotIn('Wrong Name', str(decision))
            # A supplied name on the non-delegation path still cannot create identity.
            self.assertFalse(manager.tasks_action_authorisation([1], 'remote-user', 'abort', [],
                                                                'Asserted Full Name')[0].user_allowed)

    def test_delegated_identity_has_no_invented_privileges_and_preserves_local_account_status(self):
        actor = user('bigpanda_api')
        with patch.object(principals, 'user_for_username', return_value=AnonymousUser()):
            identity, groups = principals.delegated_task_user_rights(actor, 'remote-user', 'abort', 'Asserted Full Name')
        self.assertEqual(groups, [])
        self.assertFalse(identity.is_superuser)
        self.assertFalse(resources.can_task(identity, groups, task(owner='someone'), 'abort'))
        self.assertTrue(resources.can_task(identity, groups, task(owner='Asserted Full Name', is_analy=True),
                                          'retry', [None] * 5 + [True]))
        for active in (False, True):
            local = user('remote-user', active=active, groups=['PHYS'])
            with patch.object(principals, 'user_for_username', return_value=local):
                identity, groups = principals.delegated_task_user_rights(actor, 'remote-user', 'abort', 'Asserted Full Name')
            self.assertEqual(groups, ['PHYS'])
            self.assertEqual(resources.can_task(identity, groups, task(owner='Asserted Full Name',
                name='user.unrelated.task', is_analy=True), 'abort'), active)

    def test_direct_delegation_helper_rejects_unauthorized_actor_and_action(self):
        for actor, action in ((user(), 'abort'), (user('bigpanda_api'), 'obsolete')):
            with patch.object(principals, 'user_for_username') as lookup:
                with self.assertRaises(PermissionDenied):
                    principals.delegated_task_user_rights(actor, 'remote-user', action, 'Name')
                lookup.assert_not_called()


class GroupScopeTests(unittest.TestCase):
    def test_creation_defers_group_authorization_until_submission(self):
        for principal, expected in ((user(superuser=True), True),
                                    (user(iam=['IAM:atlas/OTHER/production']), True), (user(), False)):
            with self.subTest(superuser=principal.is_superuser, expected=expected):
                model = Mock()
                model.objects.get.return_value = SimpleNamespace(reqid=1001, request_type='ANALYSIS', save=Mock())
                template = Mock()
                template.objects.get.return_value.get_variable.return_value = 'group.OTHER'
                users = Mock()
                users.objects.get.return_value = principal
                ns = functions('analysis_tasks/views.py', ['create_analysis_request', 'check_user_group'],
                    TRequest=model, AnalysisTaskTemplate=template, User=users,
                    Response=lambda data, status=200: SimpleNamespace(data=data, status_code=status),
                    status=SimpleNamespace(HTTP_403_FORBIDDEN=403, HTTP_500_INTERNAL_SERVER_ERROR=500),
                    check_tag_source=Mock(), send_new_request_mail=Mock(), reverse=Mock(return_value='/request/1001'),
                    PermissionDenied=PermissionDenied, _jsonLogger=Mock(),
                    TemplateVariable=SimpleNamespace(KEY_NAMES=SimpleNamespace(OUTPUT_SCOPE='scope')))
                request = SimpleNamespace(user=principal, build_absolute_uri=Mock(return_value='url'), data={
                    'scope': 'group.OTHER', 'requestExtID': 1001, 'templateBase': {'id': 1, 'task_parameters': {}}})
                response = ns.create_analysis_request(request)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(model.objects.get.return_value.save.call_count, 1)
                if expected:
                    self.assertTrue(ns.check_user_group('group.OTHER', principal.username, 'OTHER'))
                else:
                    with self.assertRaises(PermissionDenied):
                        ns.check_user_group('group.OTHER', principal.username, 'OTHER')
                with self.assertRaises(PermissionDenied):
                    ns.check_user_group('group.OTHER', principal.username, 'WRONG')
        self.assertFalse(resources.can_scope(user(superuser=True), 'user.someone-else'))
        # Container access retains its separate membership requirement.
        self.assertFalse(resources.can_scope(user(superuser=True), 'group.OTHER'))

    def test_creation_accepts_another_user_scope(self):
        for principal in (user(), user(superuser=True)):
            model = Mock()
            model.objects.get.return_value = SimpleNamespace(reqid=1001, request_type='ANALYSIS', save=Mock())
            template = Mock()
            template.objects.get.return_value.get_variable.return_value = 'user.someone_else'
            ns = functions('analysis_tasks/views.py', ['create_analysis_request'],
                TRequest=model, AnalysisTaskTemplate=template,
                Response=lambda data, status=200: SimpleNamespace(data=data, status_code=status),
                status=SimpleNamespace(HTTP_500_INTERNAL_SERVER_ERROR=500),
                check_tag_source=Mock(), send_new_request_mail=Mock(), reverse=Mock(return_value='/request/1001'),
                _jsonLogger=Mock(), TemplateVariable=SimpleNamespace(KEY_NAMES=SimpleNamespace(OUTPUT_SCOPE='scope')))
            request = SimpleNamespace(user=principal, build_absolute_uri=Mock(return_value='url'), data={
                'scope': 'user.someone_else', 'requestExtID': 1001, 'templateBase': {'id': 1, 'task_parameters': {}}})
            self.assertEqual(ns.create_analysis_request(request).status_code, 200)
            model.objects.get.return_value.save.assert_called_once()

    def test_step_verification_retains_only_the_legacy_scope_restrictions(self):
        variables = SimpleNamespace(KEY_NAMES=SimpleNamespace(OUTPUT_SCOPE='scope', INPUT_BASE='input',
            WORKING_GROUP='working', DESTINATION='destination'), JOB_TO_TASK_PARAMETERS={'destination': 'dest'})
        for principal in (user(), user(superuser=True), user(iam=['IAM:atlas/OTHER/production'])):
            users = Mock()
            users.objects.get.return_value = principal
            ns = functions('analysis_tasks/views.py', ['verify_step', 'check_user_group'],
                AnalysisStepTemplate=object, DDM=object, User=users,
                TemplateVariable=variables, PermissionDenied=PermissionDenied)
            for scope, working_group, allowed in (
                    ('user.someone_else', None, True), ('user.someone_else', 'OTHER', False),
                    ('group.OTHER', 'OTHER', bool(principal.is_superuser or principal.groups.all())),
                    ('group.OTHER', 'WRONG', False)):
                with self.subTest(superuser=principal.is_superuser, scope=scope, working_group=working_group):
                    step = SimpleNamespace(get_variable=lambda name: scope if name == 'scope' else '',
                        step_production_parent=SimpleNamespace(step_parent=None),
                        step_parameters={'working': working_group})
                    if allowed:
                        ns.verify_step(step, Mock(), principal.username)
                    elif scope.startswith('group'):
                        with self.assertRaises(PermissionDenied):
                            ns.verify_step(step, Mock(), principal.username)
                    else:
                        with self.assertRaisesRegex(Exception, 'does not match user scope'):
                            ns.verify_step(step, Mock(), principal.username)

    def test_request_scope_picker_includes_all_physics_groups_without_duplicates(self):
        users = Mock()
        users.objects.get.return_value = user(iam=['IAM:atlas/phys-z/production', 'IAM:atlas/proj-known/production'])
        groups = Mock()
        groups.objects.all.return_value = [SimpleNamespace(name=name) for name in (
            'IAM:atlas/phys-z/production', 'IAM:atlas/phys-a/production', 'IAM:atlas/proj-other/production',
            'IAM:atlas/phys-reader/reader', 'unrelated')]
        cache = Mock()
        cache.get.return_value = None
        ns = functions('analysis_tasks/views.py', ['all_groups', 'scopes_by_user'],
                       User=users, Group=groups, cache=cache)
        self.assertEqual(ns.scopes_by_user('alice'),
                         ['user.alice', 'group.phys-z', 'group.proj-known', 'group.phys-a'])
        cache.set.assert_called_once_with('all_group_scopes', ['group.phys-a', 'group.phys-z'], 3600*5*24)


class RequestTableTests(unittest.TestCase):
    def test_approval_metadata_error_is_not_overwritten_by_policy_grant(self):
        # Exercise the actual embedded branch without building the whole page.
        path = ATLAS / 'prodtask/views.py'
        tree = ast.parse(path.read_text())
        view = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'request_table_view')
        body = next(n.body for n in ast.walk(view) if isinstance(n, ast.Try) and any(
            isinstance(s, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'autorized_change_request'
                                              for t in s.targets) for s in n.body))
        assignment = next(n for n in body if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == 'autorized_change_request' for t in n.targets)
            and isinstance(n.value, ast.BoolOp))
        index = body.index(assignment)
        approval_branch = body[index - 1]
        self.assertIsInstance(approval_branch, ast.If)
        code = ast.Module(body=[ast.Assign(targets=[ast.Name(id='autorized_change_request', ctx=ast.Store())],
                                           value=ast.Constant(True)), approval_branch, assignment], type_ignores=[])
        code = compile(ast.fix_missing_locations(code), str(path), 'exec')
        for metadata_error, policy_grant in ((True, True), (True, False), (False, True), (False, False)):
            statuses = Mock()
            stamp = SimpleNamespace(ctime=lambda: 'date')
            statuses.objects.filter.return_value = [SimpleNamespace(owner='manager', timestamp=stamp)]
            if metadata_error:
                statuses.objects.filter.side_effect = RuntimeError('metadata unavailable')
            policy = Mock(return_value=policy_grant)
            namespace = dict(request=SimpleNamespace(user=user()), RequestStatus=statuses, can_request=policy,
                             cur_request=SimpleNamespace(reqid=1, request_type='MC', phys_group='PHYS', cstatus='approved'))
            exec(code, namespace)
            self.assertEqual(namespace['autorized_change_request'], not metadata_error and policy_grant)
            if not metadata_error:
                self.assertFalse(policy.call_args.kwargs['audit'])


class LoggingTests(unittest.TestCase):
    def test_only_action_failures_are_logged(self):
        with patch.object(service, 'logger') as logger:
            self.assertTrue(resources.can_access(user(superuser=True), 'job', 'debug'))
            self.assertEqual(logger.mock_calls, [])
            self.assertFalse(resources.can_access(user(), 'job', 'debug'))
            self.assertEqual(logger.info.call_args.kwargs['extra']['outcome'], 'deny')
            with patch.object(service, 'get_engine', side_effect=RuntimeError('unavailable')):
                self.assertFalse(resources.can_access(user(superuser=True), 'job', 'debug'))
            self.assertEqual(logger.error.call_args.kwargs['extra']['outcome'], 'error')


    def test_capability_helpers_are_silent_but_submitted_task_denial_is_logged(self):
        principal = user()
        manager = task_manager(principal, {1: task(owner='someone_else', is_group_production=True)})
        with patch.object(service, 'logger') as logger:
            capabilities = manager.task_capabilities(1, principal.username)
            self.assertFalse(capabilities['abort'])
            self.assertEqual(resources.request_group_capabilities(principal, ['PHYS', 'HION']), [])
            self.assertEqual(resources.manageable_shares(principal, [{'name': 'child', 'parent': 'MC root'}]), [])
            self.assertEqual(logger.mock_calls, [])
            self.assertEqual(manager.task_action_authorisation(1, principal.username, 'abort'), (True, False))
            logger.info.assert_called_once()
            self.assertEqual(logger.info.call_args.kwargs['extra']['action'], 'abort')

    def test_silent_checks_keep_the_same_decisions_and_fail_closed(self):
        req = SimpleNamespace(reqid=1, request_type='MC')
        with patch.object(service, 'logger') as logger:
            self.assertTrue(resources.can_access(user(superuser=True), 'job', 'debug', audit=False))
            self.assertFalse(resources.can_request(user(), req, 'resend_email', audit=False))
            self.assertFalse(resources.can_scope(user(), 'group.OTHER', audit=False))
            with patch.object(service, 'get_engine', side_effect=RuntimeError('unavailable')):
                self.assertFalse(resources.can_access(user(superuser=True), 'job', 'debug', audit=False))
            self.assertEqual(logger.mock_calls, [])

    def test_container_scope_listing_is_silent_for_allowed_and_denied_scopes(self):
        users = Mock()
        ns = functions('analysis_tasks/scopes.py', ['container_scopes_by_user'], User=users, re=__import__('re'))
        with patch.object(service, 'logger') as logger:
            for active in (True, False):
                users.objects.get.return_value = user(iam=['IAM:atlas/OTHER/production'], active=active)
                self.assertEqual(ns.container_scopes_by_user('alice'),
                                 ['user.alice', 'group.OTHER'] if active else [])
            self.assertEqual(logger.mock_calls, [])

    def test_group_creation_preflight_is_silent(self):
        ns = functions('prodtask/request_views.py', ['check_request_group'], Response=lambda data: data)
        with patch.object(service, 'logger') as logger:
            for principal, expected in ((user(), False), (user(groups=['MCCOORD']), True)):
                self.assertEqual(ns.check_request_group(SimpleNamespace(user=principal, data={'group': 'HION'})), expected)
            self.assertEqual(logger.mock_calls, [])

    def test_train_capability_is_silent_but_close_denial_is_logged(self):
        trains = Mock()
        ns = functions('prodtask/train_views.py', ['train_edit', 'close_train'],
            TrainProduction=trains, TRequest=SimpleNamespace(PHYS_GROUPS=[]),
            render=lambda request, template, context: context, HttpResponseForbidden=lambda data: data)
        request = SimpleNamespace(user=user())
        with patch.object(service, 'logger') as logger:
            self.assertFalse(ns.train_edit(request, 1)['allow_close'])
            self.assertEqual(logger.mock_calls, [])
            self.assertEqual(ns.close_train(request, 1), 'Permission denied')
            logger.info.assert_called_once()
            self.assertEqual(logger.info.call_args.kwargs['extra']['decision_source'], 'train.close')

    def test_retry_parameter_preparation_is_silent(self):
        users = Mock()
        users.objects.get.return_value = user()
        ns = functions('task_action/task_management.py', ['permission_specific_params'], User=users)
        with patch.object(service, 'logger') as logger:
            self.assertEqual(ns.permission_specific_params('alice', 'retry', [None] * 5 + [False]),
                             ('retry', [None] * 5 + [False]))
            self.assertEqual(logger.mock_calls, [])
