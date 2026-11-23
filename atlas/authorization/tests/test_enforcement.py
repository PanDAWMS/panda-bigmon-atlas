import ast
import json
import re
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from atlas.authorization import resources, service, principals
from atlas.authorization.engine import AuthorizationEngine, parse_policy
from atlas.authorization.matchers import match_constraints
from atlas.authorization.tests.support import ATLAS, functions, user, task, task_manager


class EngineTests(unittest.TestCase):
    def test_constraints_require_all_fields(self):
        constraints = '{"priority":[500,999],"share":["A","B"]}'
        self.assertTrue(match_constraints({'priority': '700', 'share': 'A'}, constraints))
        for params in ({'priority': 700}, {'priority': 700, 'share': 'C'},
                       {'priority': 1000, 'share': 'A'}, {'priority': True, 'share': 'A'}):
            self.assertFalse(match_constraints(params, constraints))

    def test_json_loader_composition_and_provenance(self):
        with TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ('one.csv', 'two.csv')]
            paths[0].write_text('# comment\np, role, thing, update, {"x":1,"y":"a,b"}, {"priority":[1,5]}, allow\n')
            paths[1].write_text('p, other, thing, read, {}, {}, allow\n')
            engine = AuthorizationEngine(paths)
            allowed, refs = engine.evaluate(['role'], 'thing', 'update', {'x': 1, 'y': 'a,b'}, {'priority': 3})
            self.assertTrue(allowed)
            self.assertTrue(refs[0].endswith('one.csv:2'))
            self.assertTrue(engine.enforce(['other'], 'thing', 'read', {}, {}))
            self.assertEqual(engine.bundle_hash, AuthorizationEngine(paths).bundle_hash)
            paths[1].write_text('p, other, thing, read, {}, {}, deny\n')
            with self.assertRaisesRegex(ValueError, 'two.csv:1'):
                AuthorizationEngine(paths)
        for invalid in ('p, r, o, a, {}, {"x":[9,1]}, allow', 'p, r, o, a, [], {}, allow'):
            with self.assertRaises(ValueError):
                parse_policy(invalid)

    def test_no_human_identity_rules_and_no_reserved_role_injection(self):
        engine = AuthorizationEngine()
        rules = [rule for rule in engine.references if rule[0].startswith('user:')]
        self.assertEqual({rule[0] for rule in rules}, {'user:bigpanda_api'})
        self.assertEqual({rule[1] for rule in rules}, {'delegation'})
        for groups in ([], ['MCCOORD'], ['DPD'], ['user:person'], ['task-owner'], ['prodsys:superuser']):
            self.assertEqual(principals.roles_for(user('person'), groups), principals.roles_for(user('renamed'), groups))
        self.assertFalse(resources.can_task(user(groups=['prodsys:superuser']), ['prodsys:superuser'], task(owner='other'), 'abort'))

    def test_fail_closed_and_logging_is_independent(self):
        with patch.object(service, 'get_engine', side_effect=RuntimeError('unavailable')):
            self.assertFalse(resources.can_access(user(superuser=True), 'job', 'debug'))
        with patch.object(service, 'logger') as logger:
            self.assertFalse(resources.can_access(user(), 'unknown', 'unknown'))
            self.assertEqual(logger.info.call_args.kwargs['extra']['outcome'], 'unmapped')
            self.assertFalse(resources.can_access(user(superuser=True), 'job', 'debug', facts=lambda: 1 / 0))
            self.assertEqual(logger.error.call_args.kwargs['extra']['outcome'], 'error')
            logger.info.side_effect = RuntimeError('disk full')
            logger.debug.side_effect = RuntimeError('disk full')
            self.assertTrue(resources.can_access(user(superuser=True), 'job', 'debug'))
            self.assertFalse(resources.can_access(user(), 'job', 'debug'))

    def test_stale_shadow_or_off_setting_cannot_disable_enforcement(self):
        from django.test import override_settings
        for mode in ('off', 'shadow', 'enforce'):
            with override_settings(AUTHORIZATION_MODE=mode):
                self.assertFalse(resources.can_access(user(), 'job', 'debug'))

    def test_unauthenticated_and_inactive_principals_deny(self):
        for authenticated, active in ((False, True), (True, False)):
            principal = user(superuser=True, authenticated=authenticated, active=active)
            self.assertFalse(resources.can_access(principal, 'job', 'debug'))
            self.assertFalse(resources.can_create_request(principal, 'PHYS'))


class TaskTests(unittest.TestCase):
    def test_task_permissions_boundaries_and_state(self):
        for identity, group_prod, action, priority in product(
                ('outsider', 'owner', 'member', 'DPD', 'MCCOORD', 'superuser'), (False, True),
                ('abort', 'reassign_to_site', 'change_priority'), (570, 571)):
            principal = user(groups=[{'member': 'PHYS'}.get(identity, identity)] if identity in ('member', 'DPD', 'MCCOORD') else [],
                             superuser=identity == 'superuser')
            resource = task(owner='alice' if identity == 'owner' else 'other', is_group_production=group_prod)
            manager = task_manager(principal, {1: resource})
            eligible, allowed = manager.task_action_authorisation(1, principal.username, action, [priority])
            expected = identity in ('DPD', 'MCCOORD', 'superuser') or (identity in ('owner', 'member') and
                (not group_prod or (action != 'reassign_to_site' and (action != 'change_priority' or priority <= 570))))
            self.assertTrue(eligible)
            self.assertEqual(allowed, expected, (identity, group_prod, action, priority))
        manager = task_manager(user(superuser=True), {1: task(status='done')})
        self.assertEqual(manager.task_action_authorisation(1, 'alice', 'change_priority', [1]), (False, False))
        self.assertEqual(manager.task_action_authorisation(1, 'alice', 'unknown', []), (False, False))

    def test_analysis_relationships_retry_and_modifiers(self):
        principal = user()
        resource = task(owner='Alice User', name='user.alice.test', is_analy=True, status='failed')
        manager = task_manager(principal, {1: resource})
        self.assertEqual(manager.task_action_authorisation(1, 'alice', 'retry', []), (True, True))
        self.assertEqual(manager.task_action_authorisation(1, 'alice', 'retry', [None]*5+[True]), (True, False))
        self.assertTrue(resources.can_task(user(superuser=True), [], resource, 'retry', [None]*5+[True]))
        self.assertFalse(resources.can_task(user('other'), [], task(owner='someone'), 'abort'))
        self.assertFalse(resources.can_task(user('other'), [], task(owner='Claimed Name',
            name='user.someone.task', is_analy=True), 'abort', fullname='Claimed Name'))

    def test_ui_capabilities_use_policy_bounds_but_do_not_authorize_arbitrary_parameters(self):
        resource = task(is_group_production=True)
        manager = task_manager(user(), {1: resource})
        capabilities = manager.task_capabilities(1, 'alice')
        self.assertTrue(capabilities['change_priority'])
        self.assertFalse(capabilities['reassign_to_site'])
        self.assertEqual(manager.task_action_authorisation(1, 'alice', 'change_priority', [571]), (True, False))

    def test_replication_batch_and_worker_recheck(self):
        manager = task_manager(user(), {1: task(), 2: task(owner='other')})
        checks = manager.tasks_action_authorisation([1, 2], 'alice', 'abort')
        self.assertEqual([r.user_allowed for r in checks], [True, False])
        self.assertFalse(manager.rules_action_authorisation(['dataset'], 'alice', 'bypass_queue')[0].user_allowed)
        worker = functions('task_action/task_management.py', ['async_action'],
            ActionType=SimpleNamespace(RULE_ACTION='rule', TASK_ACTION='task'),
            TaskManagementAuthorisation=lambda: manager, TaskActionExecutor=Mock(),
            DistributedLock=Mock(), List=list, Dict=dict, Optional=__import__('typing').Optional)
        result = worker.async_action(Mock(), 'task', 'abort', 'alice', '', [1, 2], [])
        self.assertFalse(result[0]['return_code'])
        worker.TaskActionExecutor.assert_not_called()

    def test_delegation_uses_service_policy_and_never_human_allowlist(self):
        for name in ('other', 'bigpanda_api'):
            request = SimpleNamespace(user=user(name), data={'username': 'target'})
            for action in ('abort', 'finish', 'retry', 'change_priority', 'obsolete'):
                self.assertEqual(resources.can_delegate(request, action), name == 'bigpanda_api' and action != 'obsolete')


class ResourceTests(unittest.TestCase):
    def test_step_save_denial_precedes_every_write_and_ignores_queued_superuser_flag(self):
        from django.http import HttpResponse
        principal = user('outsider')
        req = SimpleNamespace(reqid=1, manager='owner', request_type='MC', phys_group='PHYS', cstatus='waiting')
        model = Mock(PHYS_GROUPS=[('PHYS', 'Physics')])
        model.objects.get.return_value = req
        lock = Mock()
        lock.acquire_lock.return_value = True
        write = Mock()
        ns = functions('prodtask/views.py', ['request_steps_approve_or_save', 'request_steps_approve_or_save_async'],
            TRequest=model, DistributedLock=lock, save_slice_changes=write,
            _jsonLogger=Mock(), _logger=Mock(), form_json_request_dict=lambda *a: {},
            form_request_log=lambda *a: '', json=json, time=lambda: 0,
            HttpResponse=HttpResponse, user_for_username=lambda n: principal)
        payload = {'2': {}, '17': {}, '-1': {}}
        expected = {'missing_tags': [], 'slices': [], 'wrong_slices': [],
                    'double_trf': [], 'error_slices': [], 'no_action_slices': [2, 17, -1],
                    'success': True, 'new_status': 'waiting', 'removed_input': [],
                    'fail_slice_save': '', 'error_approve_message': 'Permission denied'}
        with patch.object(principals, 'user_for_username', return_value=user('owner')):
            response = ns.request_steps_approve_or_save(
                SimpleNamespace(user=principal, body=json.dumps(payload).encode()), 1, -1)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response['Content-Type'], 'application/json')
            self.assertEqual(json.loads(response.content), expected)
            lock.release_lock.assert_called_once_with('steps_modificaiton_1')
            lock.release_lock.reset_mock()
            result = ns.request_steps_approve_or_save_async(Mock(), payload, 'outsider', True, 1, -1)
            self.assertIsInstance(result, str)
            self.assertEqual(json.loads(result), dict(expected, async_name='save_slices'))
            lock.release_lock.assert_called_once_with('steps_modificaiton_1')
        write.assert_not_called()

    def test_job_and_task_privilege_rules_belong_to_task_superusers(self):
        engine = AuthorizationEngine()
        rules = [(row, refs) for row, refs in engine.references.items()
                 if row[0] == 'prodsys:superuser' and row[1] in ('job', 'task_privilege')]
        self.assertEqual(len(rules), 4)
        for row, refs in rules:
            self.assertTrue(all('task_superusers.csv:' in ref for ref in refs))
            self.assertTrue(engine.enforce(['prodsys:superuser'], row[1], row[2], {}, {}))
            self.assertFalse(engine.enforce(['subject'], row[1], row[2], {}, {}))

    def test_train_close_and_reopen_writes_enforce_capability_policy(self):
        for superuser, action in product((False, True), ('close_train', 'reopen_train')):
            model = Mock()
            ns = functions('prodtask/train_views.py', ['close_train', 'reopen_train'],
                           TrainProduction=model, HttpResponseForbidden=lambda *a: 403,
                           train_edit=lambda *a: 'ok')
            result = getattr(ns, action)(SimpleNamespace(user=user(superuser=superuser)), 1)
            self.assertEqual(result, 'ok' if superuser else 403)
            self.assertEqual(model.objects.get.call_count, int(superuser))
            self.assertEqual(model.objects.get.return_value.save.call_count, int(superuser))

    def test_scope_write_ignores_old_capability_list_on_denial(self):
        serializer = SimpleNamespace(is_valid=Mock(), validated_data={
            'scope': 'group.OTHER', 'requestID': 1001, 'name': 'out', 'datasets': ['ds']})
        ddm = Mock()
        ns = functions('analysis_tasks/views.py', ['register_analysis_output_container'],
            AnalysisOutputContainerSerializer=lambda **kw: serializer, DDM=ddm,
            container_scopes_by_user=lambda name: ['group.OTHER'], Response=lambda data, status=None: (data, status),
            status=SimpleNamespace(HTTP_403_FORBIDDEN=403, HTTP_500_INTERNAL_SERVER_ERROR=500))
        self.assertEqual(ns.register_analysis_output_container(SimpleNamespace(user=user(superuser=True), data={}))[1], 403)
        ddm.assert_not_called()

    def test_pmg_batch_checks_all_permissions_before_first_mutation(self):
        model = Mock()
        model.STATUS = SimpleNamespace(REGISTERED='registered', HOLD='hold', CANCELLED='cancelled', WAITING='waiting')
        model.objects.filter.return_value = [SimpleNamespace(reqid=1, request_type='MC'),
                                            SimpleNamespace(reqid=2, request_type='MC')]
        write = Mock()
        ns = functions('production_request/views.py', ['pmg_approve'], TRequest=model, set_request_status=write,
            Response=lambda data, status=None: (data, status), _logger=Mock(),
            status=SimpleNamespace(HTTP_403_FORBIDDEN=403, HTTP_500_INTERNAL_SERVER_ERROR=500))
        engine = SimpleNamespace(bundle_hash='test', operations={('request', 'pmg_register')},
                                 evaluate=Mock(side_effect=[(True, []), (False, [])]))
        with patch.object(service, 'get_engine', return_value=engine):
            response = ns.pmg_approve(SimpleNamespace(user=user(), data={'jira': 'JIRA'}))
        self.assertEqual(response[1], 403)
        write.assert_not_called()

    def test_config_failure_blocks_mutation_even_for_existing_admin(self):
        principal = user(superuser=True, groups=['MCCOORD'])
        handler = SimpleNamespace(PARAMETERS_NAMES=SimpleNamespace(MCWorkflowRequest='mc', AVAILABLE_PMG_HASHTAGS='pmg'),
            set_mc_workflow_request=Mock(), AvailablePMGHashtags=Mock())
        ns = functions('prodtask_api/views.py', ['set_config_parameter'], SystemParametersHandler=handler,
            TaskManagementAuthorisation=Mock(), task_user_rights=lambda n: (principal, ['MCCOORD']),
            Response=lambda data, status=None: (data, status),
            status=SimpleNamespace(HTTP_401_UNAUTHORIZED=401, HTTP_500_INTERNAL_SERVER_ERROR=500))
        with patch.object(service, 'get_engine', side_effect=RuntimeError('broken')):
            self.assertEqual(ns.set_config_parameter(SimpleNamespace(user=principal, data={}), 'mc')[1], 401)
        handler.set_mc_workflow_request.assert_not_called()

    def test_request_group_capabilities_and_manager_gate(self):
        for groups, superuser in product(([], ['DPD'], ['MCCOORD'], ['PHYS']), (False, True)):
            principal = user(groups=groups, superuser=superuser)
            expected = ['PHYS', 'OTHER'] if superuser or set(groups) & {'DPD', 'MCCOORD'} else ['PHYS'] if groups else []
            self.assertEqual(resources.request_group_capabilities(principal, ['PHYS', 'OTHER']), expected)
        for superuser in (False, True):
            self.assertFalse(resources.can_access(user('other', superuser=superuser), 'request', 'create_slice', facts={'same_manager': False}))

    def test_coordinators_can_create_requests_for_every_configured_physics_group(self):
        # Read the actual choices without importing the production DB models.
        tree = ast.parse((ATLAS / 'prodtask/models.py').read_text())
        model = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'TRequest')
        choices = next(node.value for node in model.body if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == 'PHYS_GROUPS' for target in node.targets))
        groups = ast.literal_eval(choices.generators[0].iter)
        self.assertIn('HION', groups)
        coordinator = user(groups=['MCCOORD'])
        for group in groups:
            with self.subTest(group=group):
                self.assertTrue(resources.can_create_request(coordinator, group))
        self.assertEqual(resources.request_group_capabilities(coordinator, groups), groups)
        self.assertFalse(resources.can_create_request(user(), 'HION'))
        self.assertFalse(resources.can_create_request(user(groups=['MCCOORD'], active=False), 'HION'))

    def test_steps_permissions_have_no_username_bypass(self):
        for request_type, same_manager, manager_coordinator, member, asynchronous in product(
                ('MC', 'GROUP', 'OTHER'), (False, True), (False, True), (False, True), (False, True)):
            principal = user(groups=['PHYS'] if member else [])
            req = SimpleNamespace(reqid=1, manager='alice' if same_manager else 'other', request_type=request_type, phys_group='PHYS')
            expected = ((same_manager or manager_coordinator) if request_type == 'MC' else
                        (member or asynchronous) if request_type == 'GROUP' else True)
            with patch.object(principals, 'user_for_username', return_value=user(groups=['MCCOORD'] if manager_coordinator else [])):
                self.assertEqual(resources.can_save_steps(principal, req, physics_groups=['PHYS'], asynchronous=asynchronous), expected)

    def test_global_share_actual_writer_batch_is_atomic_for_permissions(self):
        parents = {'daod': 'Derivations', 'repro': 'Reprocessing', 'MC evgen': 'MC', 'mc_child': 'MC evgen'}
        for groups, names, expected in ((['DPD'], ['daod'], True), (['DPD', 'REPR'], ['daod', 'repro'], False),
                (['MCCOORD'], ['MC evgen', 'mc_child'], False), (['MCCOORD'], ['mc_child'], True), ([], ['daod'], False)):
            principal = user(groups=groups)
            model = Mock()
            model.objects.get.side_effect = lambda name: SimpleNamespace(name=name, parent=parents[name], value=10)
            view = functions('gdpconfig/views.py', ['global_share_change'], GlobalShare=model,
                             Response=lambda data: data, _logger=Mock(), _jsonLogger=Mock())
            result = view.global_share_change(SimpleNamespace(user=principal, data=dict.fromkeys(names, 50)))
            self.assertEqual(result['result'], 'OK' if expected else 'FAILED')
            self.assertEqual(model.objects.filter.return_value.update.call_count, len(names) if expected else 0)
        self.assertFalse(resources.can_share_batch(user(superuser=True), [{'name': 'x', 'parent': 'other'}]))
        self.assertTrue(resources.can_share_batch(user(editor=True), [{'name': 'x', 'parent': 'other'}]))

    def test_scopes_lists_and_write_denials(self):
        principal = user(iam=['IAM:atlas/PHYS/production'])
        model = Mock()
        model.objects.get.return_value = principal
        scope_api = functions('analysis_tasks/scopes.py', ['container_scopes_by_user'], User=model, re=re)
        self.assertEqual(scope_api.container_scopes_by_user('alice'), ['user.alice', 'group.PHYS'])
        for scope, expected in (('user.alice', True), ('user.other', False), ('group.PHYS', True), ('group.OTHER', False)):
            self.assertEqual(resources.can_scope(principal, scope), expected)
        self.assertFalse(resources.can_scope(user(superuser=True), 'group.OTHER'))
        self.assertFalse(resources.can_scope(user(superuser=True), 'group.PHYS', action='submit_group', working_group='OTHER'))
        with patch.object(service, 'get_engine', side_effect=RuntimeError('broken')):
            self.assertEqual(scope_api.container_scopes_by_user('alice'), [])

    def test_config_and_privileges_use_current_policy(self):
        for superuser, groups in product((False, True), ([], ['MCCOORD'])):
            principal = user(groups=groups, superuser=superuser)
            self.assertEqual(resources.can_access(principal, 'system_parameter', 'mc_workflow'), superuser or bool(groups))
            self.assertEqual(resources.can_access(principal, 'retry_error', 'manage'), superuser)
            self.assertEqual(resources.can_access(principal, 'train', 'close'), superuser)
            self.assertEqual(resources.can_access(principal, 'job', 'debug'), superuser)
        self.assertFalse(resources.can_access(user(superuser=True), 'gdpconfig', 'update'))
        self.assertTrue(resources.can_access(user(editor=True), 'gdpconfig', 'update'))
        self.assertTrue(resources.can_access(user(), 'request', 'obsolete_deleted_outputs',
                                             facts={'coordinator_required': False}))
        self.assertFalse(resources.can_access(user(), 'request', 'obsolete_deleted_outputs',
                                              facts={'coordinator_required': True}))


class StartupTests(unittest.TestCase):
    def test_invalid_bundle_prevents_startup(self):
        from atlas.authorization.apps import AuthorizationConfig
        with patch.object(service, 'get_engine', side_effect=ValueError('bad policy')):
            with self.assertRaises(ValueError):
                AuthorizationConfig.ready(Mock())

    def test_django_admin_uses_its_standard_site_without_policy_rows(self):
        from django.contrib import admin
        from django.contrib.admin.sites import AdminSite
        self.assertIs(admin.site.__class__, AdminSite)
        engine = AuthorizationEngine()
        self.assertFalse(any(resource.startswith('admin_') for resource, action in engine.operations))


if __name__ == '__main__':
    unittest.main()
