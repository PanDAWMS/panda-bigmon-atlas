import ast
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import List
import unittest
from unittest.mock import Mock, patch

from atlas.authorization.engine import AuthorizationEngine, parse_policy
from atlas.authorization.matchers import match_constraints
from atlas.authorization import shadow


def legacy_class():
    """Execute the actual permission class without importing Oracle/Celery clients.

    Only external data access is stubbed; the legacy permission methods and new
    observation hook are compiled unchanged from their production source.
    """
    path = Path(__file__).parents[2] / 'task_action/task_management.py'
    nodes = [n for n in ast.parse(path.read_text()).body if isinstance(n, ast.ClassDef)
             and n.name in ('TaskManagementAuthorisation', 'TaskActionAllowed', 'RuleActionAllowed')]
    namespace = dict(__name__=__name__, dataclass=dataclass, User=SimpleNamespace, List=List,
                     observe_task=shadow.observe_task, observe_rule=shadow.observe_rule)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    cls = namespace['TaskManagementAuthorisation']
    return cls.__new__(cls)


class PolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = AuthorizationEngine()
        cls.legacy = legacy_class()

    def test_legacy_task_permission_matrix(self):
        for analy, group_prod, identity, action in product(
                (False, True), (False, True),
                ('outsider', 'owner', 'group', 'request_group', 'DPD', 'MCCOORD', 'superuser'),
                ('abort', 'reassign_to_site', 'change_priority')):
            for priority in (-1, 570, 571):
                with self.subTest(analy=analy, group_prod=group_prod, identity=identity,
                                  action=action, priority=priority):
                    user = SimpleNamespace(username='alice', first_name='Alice', last_name='User',
                                           is_superuser=identity == 'superuser')
                    task = SimpleNamespace(id=1, owner='Alice User' if analy else 'alice',
                                           name='user.alice.test', is_analy=analy,
                                           is_group_production=group_prod, phys_group='PHYS',
                                           request_phys_group='REQ')
                    if identity != 'owner':
                        task.owner, task.name = 'someone', 'user.someone.test'
                    groups = {'group': ['PHYS'], 'request_group': ['REQ'],
                              'DPD': ['DPD'], 'MCCOORD': ['MCCOORD']}.get(identity, [])
                    expected = self.legacy.user_authorization(user, groups, task, action, [priority], None)
                    args = shadow.task_context(user, groups, task, action, [priority], None)
                    self.assertEqual(self.engine.enforce(args[0], 'task', action, args[1], args[2]), expected)

    def test_supplied_fullname_compatibility(self):
        user = SimpleNamespace(username='x', first_name='', last_name='', is_superuser=False)
        task = SimpleNamespace(owner='Legacy Name', name='user.other.task', is_analy=True,
                               is_group_production=False, phys_group='', request_phys_group='')
        roles, obj, params = shadow.task_context(user, [], task, 'abort', [], 'Legacy Name')
        self.assertTrue(self.engine.enforce(roles, 'task', 'abort', obj, params))

    def test_rules_delegation_and_default_denial(self):
        for action in ('alter_source_replication_rule', 'bypass_queue', 'change_destination',
                       'recovery_lost_files', 'retire_unused_dc'):
            self.assertTrue(self.engine.enforce(['prodsys:superuser'], 'replication_rule', action, {}, {}))
            self.assertFalse(self.engine.enforce(['DPD'], 'replication_rule', action, {}, {}))
        for action in ('abort', 'finish', 'change_priority', 'retry'):
            self.assertTrue(self.engine.enforce(['user:bigpanda_api'], 'delegation', action, {}, {}))
            self.assertFalse(self.engine.enforce(['prodsys:superuser'], 'delegation', action, {}, {}))
        self.assertFalse(self.engine.enforce(['user:bigpanda_api'], 'delegation', 'obsolete', {}, {}))
        self.assertFalse(self.engine.enforce(['prodsys:superuser'], 'task', 'unknown', {}, {}))

    def test_constraints_require_all_fields(self):
        constraints = '{"priority":[500,999],"share":["A","B"]}'
        self.assertTrue(match_constraints({'priority': '700', 'share': 'A'}, constraints))
        for params in ({'priority': 700}, {'priority': 700, 'share': 'C'},
                       {'priority': 1000, 'share': 'A'}, {'priority': True, 'share': 'A'}):
            self.assertFalse(match_constraints(params, constraints))

    def test_loader_json_validation_composition_and_provenance(self):
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
        for invalid in ('p, r, o, a, {}, {"x":[9,1]}, allow',
                        'p, r, o, a, [], {}, allow', 'p, r, o, a, {}, {}, allow, extra'):
            with self.assertRaises(ValueError):
                parse_policy(invalid)


class ShadowTests(unittest.TestCase):
    def setUp(self):
        self.mode = patch.object(shadow, 'get_mode', return_value='shadow')
        self.mode.start()
        self.addCleanup(self.mode.stop)

    def observe(self, **kwargs):
        args = dict(actor='alice', obj_type='task', object_id=1, action='abort',
                    legacy_allowed=False, context=lambda: (['prodsys:superuser'], {}, {}), source='test')
        args.update(kwargs)
        return shadow.observe(**args)

    def test_mismatch_and_state_gate(self):
        with patch.object(shadow, 'logger') as log:
            self.assertIsNone(self.observe())
            event = log.info.call_args.kwargs['extra']
            self.assertEqual(event['outcome'], 'mismatch')
            self.assertTrue(event['casbin_allowed'])
            self.assertTrue(event['policy_bundle_hash'])
            self.assertTrue(event['matching_policy_references'])
            self.observe(action_allowed=False)
            self.assertEqual(log.info.call_args.kwargs['extra']['outcome'], 'match')

    def test_unmapped_errors_off_and_logging_failure(self):
        with patch.object(shadow, 'logger') as log:
            self.observe(action='unknown')
            self.assertEqual(log.info.call_args.kwargs['extra']['outcome'], 'unmapped')
            with patch.object(shadow, 'get_engine', return_value=(None, 'BadPolicy')):
                self.observe()
                self.assertEqual(log.info.call_args.kwargs['extra']['outcome'], 'error')
            self.observe(context=Mock(side_effect=RuntimeError('bad facts')))
            self.assertEqual(log.info.call_args.kwargs['extra']['outcome'], 'error')
            log.info.side_effect = RuntimeError('disk full')
            self.assertIsNone(self.observe())
        with patch.object(shadow, 'get_mode', return_value='off'), patch.object(shadow, 'get_engine') as engine:
            self.observe()
            engine.assert_not_called()

    def test_legacy_results_survive_casbin_and_logging_failures(self):
        legacy = legacy_class()
        legacy.allowed_task_actions = {'running': ['abort']}
        task = SimpleNamespace(id=1, status='running', owner='alice', name='user.alice.test',
                               is_analy=False, is_group_production=False, phys_group='PHYS', request_phys_group='REQ')
        user = SimpleNamespace(username='alice', first_name='', last_name='', is_superuser=False)
        check = legacy._TaskManagementAuthorisation__task_action_check
        with patch.object(shadow, 'get_engine', side_effect=RuntimeError('broken')), \
                patch.object(shadow.logger, 'info', side_effect=RuntimeError('disk full')):
            self.assertEqual(check(task, user, [], 'abort', [], None), (True, True))
            user.username = 'outsider'
            self.assertEqual(check(task, user, [], 'abort', [], None), (True, False))
            self.assertEqual(check(task, user, [], 'finish', [], None), (False, False))

    def test_batch_and_replication_results_are_unchanged(self):
        legacy = legacy_class()
        legacy.allowed_task_actions = {'running': ['abort']}
        user = SimpleNamespace(username='alice', first_name='', last_name='', is_superuser=False)
        tasks = {i: SimpleNamespace(id=i, status='running', owner=owner, name='prod.task',
                                   is_analy=False, is_group_production=False,
                                   phys_group='PHYS', request_phys_group='REQ')
                 for i, owner in ((1, 'alice'), (2, 'someone'))}
        legacy.get_task_info = tasks.__getitem__
        with patch.dict(legacy.tasks_action_authorisation.__globals__,
                        task_user_rights=lambda *args: (user, [])), \
                patch.object(shadow, 'logger') as log:
            results = legacy.tasks_action_authorisation([1, 2], 'alice', 'abort', [])
            self.assertEqual([(r.id, r.action_allowed, r.user_allowed) for r in results],
                             [(1, True, True), (2, True, False)])
            self.assertEqual(log.info.call_count, 2)
            for superuser in (False, True):
                user.is_superuser = superuser
                results = legacy.rules_action_authorisation(['one', 'two'], 'alice', 'bypass_queue')
                self.assertEqual([(r.dataset, r.user_allowed) for r in results],
                                 [('one', superuser), ('two', superuser)])

    def test_delegation_view_keeps_legacy_gate_and_executes_once(self):
        path = Path(__file__).parents[2] / 'prodtask/task_manage_views.py'
        node = next(n for n in ast.parse(path.read_text()).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'task_action_ext')
        node.decorator_list = []  # Authentication decorators themselves are unchanged.
        import json
        action = Mock(return_value=(True, 'ok'))
        management = Mock()
        management.tasks_action_authorisation.return_value = [
            SimpleNamespace(user_allowed=True, action_allowed=True)]
        namespace = dict(observe_delegation=shadow.observe_delegation,
                         ALLOWED_FOR_EXTERNAL_API=['abort', 'finish', 'change_priority', 'retry'],
                         HttpResponseForbidden=lambda: 'forbidden', Response=lambda x: x,
                         TaskManagementAuthorisation=lambda: management,
                         TaskActionExecutor=Mock(), do_jedi_action=action,
                         logger=Mock(), _jsonLogger=Mock(), json=json)
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
        view = namespace['task_action_ext']
        request = SimpleNamespace(user=SimpleNamespace(username='outsider'),
                                  data={'username': 'alice', 'task': 1})
        with patch.object(shadow, 'logger') as log:
            self.assertEqual(view(request, 'abort'), 'forbidden')
            action.assert_not_called()
            self.assertFalse(log.info.call_args.kwargs['extra']['legacy_allowed'])
            request.user.username = 'bigpanda_api'
            self.assertEqual(view(request, 'abort')['result'], 'OK')
            action.assert_called_once()
            event = log.info.call_args.kwargs['extra']
            self.assertEqual((event['actor'], event['effective_user']), ('bigpanda_api', 'alice'))
            self.assertEqual(event['outcome'], 'match')
            with patch.object(shadow, 'get_engine', side_effect=RuntimeError('broken')):
                self.assertEqual(view(request, 'retry')['result'], 'OK')
            self.assertEqual(action.call_count, 2)

    def test_reserved_roles_cannot_come_from_permission_codenames(self):
        user = SimpleNamespace(username='outsider', is_superuser=False)
        task = SimpleNamespace(owner='other', is_analy=False, is_group_production=False,
                               phys_group='PHYS', request_phys_group='REQ')
        roles, obj, params = shadow.task_context(user, ['prodsys:superuser', 'task-owner'], task, 'abort', [], None)
        self.assertFalse(AuthorizationEngine().enforce(roles, 'task', 'abort', obj, params))


if __name__ == '__main__':
    unittest.main()
