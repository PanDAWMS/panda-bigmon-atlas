"""Real policy and isolated production-gate regressions; no external services."""

import ast
from itertools import product
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from atlas.authorization import shadow, stage2
from atlas.authorization.engine import AuthorizationEngine

ATLAS = Path(__file__).parents[2]


def functions(path, names, **namespace):
    tree = ast.parse((ATLAS / path).read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    for node in nodes:
        node.decorator_list = []  # Authentication itself is unchanged.
    namespace.update({name: getattr(stage2, name) for name in dir(stage2) if name.startswith('observe_')})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ATLAS / path), 'exec'), namespace)
    return SimpleNamespace(**namespace)


def user(name='alice', *, superuser=False, editor=False, iam=()):
    principal = SimpleNamespace(username=name, is_superuser=superuser, groups=Mock(), user_permissions=Mock())
    principal.groups.all.return_value = [SimpleNamespace(name=n) for n in iam]
    principal.user_permissions.filter.return_value.exists.return_value = editor
    return principal


class Stage2Tests(unittest.TestCase):
    def setUp(self):
        mode = patch.object(shadow, 'get_mode', return_value='shadow')
        mode.start()
        self.addCleanup(mode.stop)
        logger = patch.object(shadow, 'logger')
        self.log = logger.start()
        self.addCleanup(logger.stop)

    def event(self):
        event = self.log.info.call_args.kwargs['extra']
        self.assertEqual(event['outcome'], 'match', event)
        return event

    def test_human_names_do_not_produce_roles_or_policy_exceptions(self):
        for groups in ([], ['MCCOORD'], ['DPD'], ['user:person'], ['task-owner']):
            self.assertEqual(stage2.roles_for(user('person'), groups),
                             stage2.roles_for(user('renamed'), groups))
            self.assertFalse(any(role.startswith('user:') for role in stage2.roles_for(user('person'), groups)))
        engine = AuthorizationEngine()
        identity_rules = [rule for rule in engine.references if rule[0].startswith('user:')]
        self.assertEqual({rule[0] for rule in identity_rules}, {'user:bigpanda_api'})
        self.assertEqual({rule[1] for rule in identity_rules}, {'delegation'})
        self.assertEqual({rule[2] for rule in identity_rules}, {'abort', 'finish', 'change_priority', 'retry'})

    def test_removed_human_exceptions_do_not_grant_privileges(self):
        engine = AuthorizationEngine()
        roles = stage2.roles_for(user('person'))
        for obj_type, action, facts in (('request', 'approve', {'restricted_mc': True}),
                                        ('train', 'close', {}), ('retry_error', 'manage', {})):
            self.assertFalse(engine.enforce(roles, obj_type, action, facts, {}))
        self.assertTrue(engine.enforce(stage2.roles_for(user(), ['MCCOORD']),
                                       'request', 'approve', {'restricted_mc': True}, {}))
        self.assertFalse(engine.enforce(['user:bigpanda_api'], 'request', 'approve', {'restricted_mc': True}, {}))

    def test_request_creation_grants_and_denials(self):
        for superuser, groups, group in product((False, True), ([], ['DPD'], ['ALL'], ['PHYS']), ('PHYS', 'OTHER')):
            principal = user(superuser=superuser)
            ns = functions('prodtask/request_views.py', ['check_request_group'],
                           check_allow_request_create=lambda u: groups, Response=lambda data, **kw: data)
            expected = superuser or 'DPD' in groups or 'ALL' in groups or group in groups
            self.assertEqual(ns.check_request_group(SimpleNamespace(user=principal, data={'group': group})), expected)
            self.assertEqual(self.event()['legacy_allowed'], expected)

    def test_request_approval_and_registered_workflow_state(self):
        for name, superuser, groups, request_type, phys_group in product(
                ('alice', 'legacy-coordinator'), (False, True), ([], ['MCCOORD']), ('MC', 'GROUP'), ('PHYS', 'VALI')):
            principal = user(name, superuser=superuser)
            req = SimpleNamespace(reqid=1, request_type=request_type, phys_group=phys_group, cstatus='approved')
            ns = functions('prodtask/views.py', ['request_approve_status'],
                           SimpleNamespace=SimpleNamespace, MC_COORDINATORS=['legacy-coordinator'],
                           egroup_permissions=lambda name: groups, _logger=Mock())
            result = ns.request_approve_status(req, SimpleNamespace(user=principal))
            expected = request_type != 'MC' or phys_group == 'VALI' or name == 'legacy-coordinator' or superuser or 'MCCOORD' in groups
            self.assertEqual(result, 'approved' if expected else None)
            event = self.log.info.call_args.kwargs['extra']
            policy_expected = request_type != 'MC' or phys_group == 'VALI' or superuser or 'MCCOORD' in groups
            self.assertEqual(event['legacy_allowed'], expected)
            self.assertEqual(event['casbin_allowed'], policy_expected)
            self.assertEqual(event['outcome'], 'match' if expected == policy_expected else 'mismatch')
        # Registered is a workflow result, not a permission denial.
        parent = Mock()
        parent.objects.filter.return_value.exists.return_value = True
        parent.objects.filter.return_value.__getitem__ = Mock(return_value=SimpleNamespace(train_id=7))
        tags = Mock()
        tags.objects.filter.return_value.exists.return_value = True
        tags.objects.filter.return_value.__getitem__ = Mock(return_value='tag')
        links = Mock()
        links.objects.filter.return_value.values_list.return_value = [10]
        trains = Mock()
        trains.objects.get.return_value.pattern_request_id = 10
        ns = functions('prodtask/views.py', ['request_approve_status'], SimpleNamespace=SimpleNamespace,
                       MC_COORDINATORS=[], egroup_permissions=lambda n: [], _logger=Mock(),
                       ParentToChildRequest=parent, HashTag=tags, HashTagToRequest=links, TrainProduction=trains)
        req = SimpleNamespace(reqid=1, request_type='GROUP', phys_group='PHYS', cstatus='waiting')
        self.assertEqual(ns.request_approve_status(req, SimpleNamespace(user=user())), 'registered')
        self.assertTrue(self.event()['casbin_allowed'])

    def test_steps_sync_async_preserve_manager_and_group_differences(self):
        for asynchronous, superuser, same_manager, manager_coordinator, membership, request_type, validation in product(
                (False, True), (False, True), (False, True), (False, True), (False, True),
                ('MC', 'GROUP', 'OTHER'), (False, True)):
            principal = user(superuser=superuser)
            req = SimpleNamespace(reqid=1, manager='alice' if same_manager else 'other',
                                  request_type=request_type, phys_group='VALI' if validation else 'PHYS')
            expected = True
            if not validation and request_type == 'MC' and not same_manager:
                expected = superuser or manager_coordinator
            if not asynchronous and not validation and request_type == 'GROUP':
                expected = superuser or membership
            stage2.observe_request_steps(principal, req, expected, groups=lambda: ['PHYS'] if membership else [],
                                         manager_groups=lambda: ['MCCOORD'] if manager_coordinator else [],
                                         physics_groups=['PHYS', 'DPD'], asynchronous=asynchronous)
            self.event()

    def test_global_share_actual_writer_and_capabilities(self):
        shares = {'daod': 'Derivations', 'repro': 'Reprocessing', 'MC evgen': 'MC',
                  'mc_child': 'MC evgen', 'deep': 'mc_child'}
        for groups, editor, superuser in product(
                ([], ['DPD'], ['REPR'], ['MCCOORD'], ['DPD', 'REPR']), (False, True), (False, True)):
            for names in ([], ['daod'], ['repro'], ['daod', 'repro'], ['MC evgen'], ['mc_child'],
                          ['MC evgen', 'mc_child'], ['deep']):
                with self.subTest(groups=groups, editor=editor, superuser=superuser, names=names):
                    principal = user(superuser=superuser, editor=editor)
                    model = Mock()
                    model.objects.get.side_effect = lambda name: SimpleNamespace(name=name, parent=shares[name], value=10)
                    ns = functions('gdpconfig/views.py', ['global_share_change'], GlobalShare=model,
                                   task_user_rights=lambda name: (principal, groups),
                                   Response=lambda data: data, _logger=Mock(), _jsonLogger=Mock())
                    expected = editor or ('DPD' in groups and all(shares[n] == 'Derivations' for n in names)) or (
                        'REPR' in groups and all(shares[n] == 'Reprocessing' for n in names)) or (
                        'MCCOORD' in groups and (all(shares[n] in stage2.MC_SHARES for n in names)
                                                or all(n in stage2.MC_SHARES for n in names)))
                    result = ns.global_share_change(SimpleNamespace(user=principal, data=dict.fromkeys(names, 50)))
                    self.assertEqual(result['result'], 'OK' if expected else 'FAILED')
                    self.assertEqual(model.objects.filter.return_value.update.call_count, len(names) if expected else 0)
                    self.assertEqual(self.event()['legacy_allowed'], expected)
            ns = functions('gdpconfig/global_share_api.py', ['manageable_global_shares'],
                           MC_LEVEL2_SHARES=stage2.MC_SHARES, task_user_rights=lambda name: (principal, groups))
            result = ns.manageable_global_shares(principal, [{'name': n, 'parent': p} for n, p in shares.items()])
            self.assertEqual(result, [n for n, p in shares.items() if editor or ('DPD' in groups and p == 'Derivations')
                                     or ('REPR' in groups and p == 'Reprocessing')
                                     or ('MCCOORD' in groups and (n in stage2.MC_SHARES or p in stage2.MC_SHARES))])
            for call in self.log.info.call_args_list[-len(shares):]:
                self.assertEqual(call.kwargs['extra']['outcome'], 'match')

    def test_analysis_scope_enumeration_and_submission(self):
        import re
        for superuser, iam in product((False, True), ([], ['IAM:atlas/PHYS/production'],
                                                    ['IAM:atlas/sub/group/production'], ['atlas/PHYS/production'])):
            principal = user(superuser=superuser, iam=iam)
            model = Mock()
            model.objects.get.return_value = principal
            ns = functions('analysis_tasks/scopes.py', ['container_scopes_by_user'], User=model, re=re)
            scopes = ns.container_scopes_by_user('alice')
            expected = ['user.alice'] + [f'group.{n[len("IAM:atlas/"):-len("/production")]}' for n in iam if n.startswith('IAM:atlas/')]
            self.assertEqual(scopes, expected)
            self.event()
            for scope in ('user.alice', 'user.other', 'group.PHYS', 'group.OTHER', 'group.sub/group'):
                stage2.observe_scope(principal, scope, scope in scopes, group_names=iam)
                self.event()
            ns = functions('analysis_tasks/views.py', ['check_user_group'], User=model)
            for working in ('PHYS', 'OTHER'):
                expected = (superuser or 'IAM:atlas/PHYS/production' in iam) and working == 'PHYS'
                if expected:
                    self.assertTrue(ns.check_user_group('group.PHYS', 'alice', working))
                else:
                    with self.assertRaises(Exception):
                        ns.check_user_group('group.PHYS', 'alice', working)
                self.assertEqual(self.event()['legacy_allowed'], expected)

    def test_scope_registration_denial_never_mutates_ddm(self):
        principal = user(superuser=True)  # Superuser has no scope bypass here.
        serializer = SimpleNamespace(is_valid=Mock(), validated_data={
            'scope': 'group.OTHER', 'requestID': 1001, 'name': 'out', 'datasets': ['ds']})
        ddm = Mock()
        ns = functions('analysis_tasks/views.py', ['register_analysis_output_container'],
                       AnalysisOutputContainerSerializer=lambda **kw: serializer,
                       container_scopes_by_user=lambda name: ['user.alice'],
                       Response=lambda data, status=None: (data, status),
                       status=SimpleNamespace(HTTP_403_FORBIDDEN=403, HTTP_500_INTERNAL_SERVER_ERROR=500), DDM=ddm)
        self.assertEqual(ns.register_analysis_output_container(SimpleNamespace(user=principal, data={}))[1], 403)
        ddm.assert_not_called()
        self.assertFalse(self.event()['casbin_allowed'])

    def test_configuration_bindings_and_no_implicit_superuser_gdp_access(self):
        for superuser, editor in product((False, True), (False, True)):
            principal = user(superuser=superuser, editor=editor)
            ns = functions('gdpconfig/views.py', ['config_action'], json=__import__('json'),
                           HttpResponse=lambda data: data)
            result = ns.config_action(SimpleNamespace(user=principal, body=b''), 'update')
            self.assertIn('Request data is empty' if editor else 'Permission denied', result)
            self.assertEqual(self.event()['casbin_allowed'], editor)
        for groups in ([], ['MCCOORD'], ['unrelated']):
            stage2.observe_access(user(), 'system_parameter', 'mc_workflow', 'MCCOORD' in groups,
                                  groups=groups, source='test')
            self.event()
        for name, superuser in product(('alice', 'other'), (False, True)):
            principal = user(name, superuser=superuser)
            stage2.observe_access(principal, 'retry_error', 'manage', superuser, source='test')
            self.event()
            stage2.observe_access(principal, 'retry_error', 'create_submit', True, source='test')
            self.event()

    def test_stage2_failure_and_off_leave_actual_gate_unchanged(self):
        principal = user()
        ns = functions('prodtask/request_views.py', ['check_request_group'],
                       check_allow_request_create=lambda u: [], Response=lambda data, **kw: data)
        for failure in (RuntimeError('engine unavailable'), None):
            with patch.object(shadow, 'get_engine', side_effect=failure), \
                    patch.object(shadow.logger, 'info', side_effect=RuntimeError('logging broken')):
                self.assertFalse(ns.check_request_group(SimpleNamespace(user=principal, data={'group': 'PHYS'})))
        with patch.object(shadow, 'get_mode', return_value='off'), patch.object(shadow, 'get_engine') as engine:
            self.assertFalse(ns.check_request_group(SimpleNamespace(user=principal, data={'group': 'PHYS'})))
            engine.assert_not_called()

    def test_workflow_parameter_actual_gates_use_coordinator_group(self):
        for groups, superuser, parameter in product(([], ['MCCOORD'], ['unrelated']), (False, True), ('mc', 'pmg')):
            principal = user(superuser=superuser)
            handler = SimpleNamespace(PARAMETERS_NAMES=SimpleNamespace(MCWorkflowRequest='mc', AVAILABLE_PMG_HASHTAGS='pmg'),
                                      set_mc_workflow_request=Mock(), AvailablePMGHashtags=Mock())
            ns = functions('prodtask_api/views.py', ['set_config_parameter'], SystemParametersHandler=handler,
                           TaskManagementAuthorisation=Mock(), task_user_rights=lambda n: (principal, groups),
                           MCWorkflowRequest=Mock(), Response=lambda data, status=None: (data, status),
                           status=SimpleNamespace(HTTP_401_UNAUTHORIZED=401, HTTP_404_NOT_FOUND=404,
                                                  HTTP_500_INTERNAL_SERVER_ERROR=500))
            result = ns.set_config_parameter(SimpleNamespace(user=principal, data={'levels': []}), parameter)
            expected = superuser or 'MCCOORD' in groups
            self.assertEqual(result[1], None if expected else 401)
            self.assertEqual(handler.set_mc_workflow_request.call_count +
                             handler.AvailablePMGHashtags.set_available_hashtags.call_count, int(expected))
            self.assertEqual(self.event()['legacy_allowed'], expected)

    def test_pmg_login_only_legacy_endpoint_and_manager_only_slice(self):
        principal = user()
        for action in ('register', 'hold', 'cancel'):
            model = Mock()
            model.STATUS = SimpleNamespace(REGISTERED='registered', HOLD='hold', CANCELLED='cancelled', WAITING='waiting')
            model.objects.filter.return_value = [SimpleNamespace(reqid=1), SimpleNamespace(reqid=2)]
            write = Mock()
            ns = functions('production_request/views.py', ['pmg_approve'], TRequest=model, set_request_status=write,
                           Response=lambda data, **kw: data, _logger=Mock())
            result = ns.pmg_approve(SimpleNamespace(user=principal, data={'jira': 'JIRA', 'action': action}))
            self.assertEqual(result['requestIDs'], [1, 2])
            self.assertEqual(write.call_count, 2)
            self.event()
        for name, superuser in product(('alice', 'other'), (False, True)):
            principal = user(name, superuser=superuser)
            model = Mock()
            model.objects.get.return_value = SimpleNamespace(reqid=1, manager='alice', cstatus='approved')
            slices = Mock()
            slices.objects.filter.return_value.count.return_value = 0
            clone = Mock(return_value=4)
            ns = functions('prodtask_api/views.py', ['create_slice'], TRequest=model, InputRequestList=slices,
                           clone_pattern_slice=clone, HttpResponseForbidden=lambda: 'forbidden',
                           HttpResponseBadRequest=lambda e: str(e), Response=lambda data: data)
            result = ns.create_slice(SimpleNamespace(user=principal, data={
                'production_request': 1, 'pattern_slice': 1, 'pattern_request': 2, 'steps': {}}))
            self.assertEqual(result, {'slice': 4, 'request': 1} if name == 'alice' else 'forbidden')
            self.assertEqual(clone.call_count, int(name == 'alice'))
            self.event()

    def test_shadow_denial_cannot_stop_allowed_share_write(self):
        principal = user(editor=True)
        model = Mock()
        model.objects.get.return_value = SimpleNamespace(name='share', parent='anything', value=10)
        ns = functions('gdpconfig/views.py', ['global_share_change'], GlobalShare=model,
                       Response=lambda data: data, _logger=Mock(), _jsonLogger=Mock())
        engine = SimpleNamespace(bundle_hash='test', operations={('global_share', 'update_batch')},
                                 evaluate=lambda *args: (False, []))
        with patch.object(shadow, 'get_engine', return_value=(engine, None)):
            self.assertEqual(ns.global_share_change(SimpleNamespace(user=principal, data={'share': 20})), {'result': 'OK'})
        model.objects.filter.return_value.update.assert_called_once_with(value=20)
        self.assertEqual(self.log.info.call_args.kwargs['extra']['outcome'], 'mismatch')

    def test_retry_create_post_preserves_ungated_write_but_management_denies(self):
        principal = user()
        model = Mock()
        form = Mock()
        form.is_valid.return_value = True
        form.cleaned_data = {}
        ns = functions('prodtask/retryerros.py', ['retry_errors_clone_create', 'retry_errors_delete'],
                       RetryErrorsForm=lambda *a, **kw: form, RetryErrors=model,
                       HttpResponseRedirect=lambda path: path, model_to_dict=lambda obj: {}, _logger=Mock())
        self.assertEqual(ns.retry_errors_clone_create(SimpleNamespace(user=principal, method='POST', POST={}), None, 'create'),
                         '/prodtask/retry_errors_list')
        model.return_value.save.assert_called_once()
        self.event()
        self.assertEqual(ns.retry_errors_delete(SimpleNamespace(user=principal, method='GET'), 1), '/prodtask/retry_errors_list')
        model.objects.get.assert_not_called()
        self.assertFalse(self.event()['legacy_allowed'])


if __name__ == '__main__':
    unittest.main()
