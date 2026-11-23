import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from atlas.authorization import principals, resources
from atlas.authorization.engine import AuthorizationEngine
from atlas.authorization.tests.support import functions, task, task_manager, user


class MCCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.coordinator = user(groups=['MCCOORD'])

    def test_all_request_and_task_policy_actions_are_granted_on_mc_resources(self):
        engine = AuthorizationEngine()
        facts = {'request_type': 'MC', 'tasktype': 'prod'}
        for resource, action in engine.operations:
            if resource in ('request', 'task'):
                with self.subTest(resource=resource, action=action):
                    self.assertTrue(engine.enforce(['MCCOORD'], resource, action, facts, {}))
        self.assertFalse(engine.enforce(['MCCOORD'], 'task', 'unknown', facts, {}))
        self.assertFalse(self.coordinator.is_superuser)

    def test_coordinator_can_save_mc_steps_without_owner_or_manager_membership(self):
        req = SimpleNamespace(reqid=1, manager='someone_else', request_type='MC', phys_group='HION')
        with patch.object(principals, 'user_for_username', return_value=user('someone_else')):
            for asynchronous in (False, True):
                self.assertTrue(resources.can_save_steps(self.coordinator, req, physics_groups=['HION'], asynchronous=asynchronous))
                self.assertFalse(resources.can_save_steps(user(), req, physics_groups=['HION'], asynchronous=asynchronous))
        # Resource facts cannot replace the actual type to get an MC grant.
        req.request_type = 'GROUP'
        self.assertFalse(resources.can_request(self.coordinator, req, 'resend_email', facts={'request_type': 'MC'}))

    def test_slice_creation_is_allowed_for_any_mc_request_but_not_unrelated_requests(self):
        for request_type in ('MC', 'GROUP', 'REPROCESSING', 'ANALYSIS'):
            model = Mock()
            model.objects.get.return_value = SimpleNamespace(reqid=1, manager='someone_else',
                                                             request_type=request_type, cstatus='approved')
            slices = Mock()
            slices.objects.filter.return_value.count.return_value = 0
            clone = Mock(return_value=7)
            ns = functions('prodtask_api/views.py', ['create_slice'], TRequest=model, InputRequestList=slices,
                clone_pattern_slice=clone, HttpResponseForbidden=lambda: 'forbidden',
                HttpResponseBadRequest=lambda error: str(error), Response=lambda data: data)
            response = ns.create_slice(SimpleNamespace(user=self.coordinator, data={
                'production_request': 1, 'pattern_slice': 1, 'pattern_request': 2, 'steps': {}}))
            self.assertEqual(response, {'slice': 7, 'request': 1} if request_type == 'MC' else 'forbidden')
            self.assertEqual(clone.call_count, int(request_type == 'MC'))

    def test_resending_email_is_mc_scoped(self):
        for request_type in ('MC', 'GROUP', 'REPROCESSING', 'ANALYSIS'):
            model = Mock()
            model.objects.get.return_value = SimpleNamespace(reqid=1, request_type=request_type,
                manager='someone_else', info_field=lambda name: '')
            send = Mock()
            ns = functions('prodtask/request_views.py', ['resend_email'], TRequest=model, User=Mock(),
                           form_and_send_email=send, HttpResponseRedirect=lambda path: path)
            self.assertEqual(ns.resend_email(SimpleNamespace(user=self.coordinator, method='GET'), 1), '/')
            self.assertEqual(send.call_count, int(request_type == 'MC'))

    def test_mc_task_administrative_actions_and_state_restrictions(self):
        resource = task(owner='someone_else', request_type='MC', phys_group='HION')
        manager = task_manager(self.coordinator, {1: resource})
        for action, params in (('change_priority', [1000]), ('reassign_to_site', ['SITE']),
                               ('retry', [None] * 5 + [True]), ('set_debug_jobs', [])):
            self.assertEqual(manager.task_action_authorisation(1, 'alice', action, params), (True, True))
        resource.status = 'done'
        self.assertEqual(manager.task_action_authorisation(1, 'alice', 'change_priority', [1000]), (False, False))

    def test_mc_task_privileges_do_not_extend_to_other_task_types(self):
        for request_type, analy in (('GROUP', False), ('REPROCESSING', False), ('ANALYSIS', True),
                                     ('', False), ('MC', True)):
            resource = task(owner='someone_else', name='user.someone_else.task', request_type=request_type, is_analy=analy)
            self.assertFalse(resources.can_access(self.coordinator, 'task_privilege', 'retry_override',
                                                 facts=resources.task_facts(resource)))
            self.assertEqual(resources.can_task(self.coordinator, ['MCCOORD'], resource, 'retry', [None] * 5 + [True]),
                             resources.can_task(self.coordinator, ['MCCOORD'], resource, 'retry', []))
        for resource, action in (('gdpconfig', 'update'), ('job', 'kill_without_task'),
                                 ('retry_error', 'manage'), ('replication_rule', 'bypass_queue'), ('train', 'close')):
            self.assertFalse(resources.can_access(self.coordinator, resource, action))

    def test_request_type_is_loaded_from_the_task_request_relation(self):
        manager = task_manager(self.coordinator, {})
        original = type(manager).get_task_info
        for request_type, request_id, expected in (('MC', 1, True), ('GROUP', 1, False),
                ('REPROCESSING', 1, False), ('ANALYSIS', 1, False), ('MC', 300, False), (None, 1, False)):
            model = Mock()
            model.objects.filter.return_value.exists.return_value = True
            model.objects.values.return_value.get.return_value = {
                'username': 'someone_else', 'name': 'user.someone_else.task', 'status': 'running',
                'request_id': request_id, 'phys_group': 'PHYS', 'request__phys_group': 'PHYS',
                'request__request_type': request_type}
            with patch.dict(original.__globals__, ProductionTask=model):
                resource = original(manager, 1)
            self.assertEqual(resource.request_type, request_type or '')
            self.assertEqual(resources.can_access(self.coordinator, 'task_privilege', 'retry_override',
                                                   facts=resources.task_facts(resource)), expected)

    def test_retry_parameter_preparation_is_scoped_to_the_whole_batch(self):
        manager = task_manager(self.coordinator, {1: task(request_type='MC'), 2: task(request_type='GROUP')})
        model = Mock()
        model.objects.get.return_value = self.coordinator
        ns = functions('task_action/task_management.py', ['permission_specific_params'], User=model,
                       TaskManagementAuthorisation=lambda: manager, task_facts=resources.task_facts)
        for task_ids, expected in (([1], True), ([1, 2], False), ([2], False), (None, False)):
            action, params = ns.permission_specific_params('alice', 'retry', [None] * 5 + [False], task_ids=task_ids)
            self.assertEqual(action, 'retry')
            self.assertEqual(params[5], expected)

    def test_job_debug_preflights_all_task_scopes_before_execution(self):
        manager = task_manager(self.coordinator, {1: task(request_type='MC'), 2: task(request_type='GROUP')})
        execute = Mock()
        ns = functions('prodjob/views.py', ['jobs_action'], TaskManagementAuthorisation=lambda: manager,
            TaskActionExecutor=Mock(), do_jedi_action=execute, task_facts=resources.task_facts,
            json=json, HttpResponse=lambda data: data, _logger=Mock())
        for ids, expected_count in (([1], 1), ([1, 2], 0), ([2], 0)):
            execute.reset_mock()
            request = SimpleNamespace(user=self.coordinator, body=json.dumps({
                'jobs': [{'taskid': task_id, 'pandaid': task_id * 10} for task_id in ids], 'parameters': []}))
            result = json.loads(ns.jobs_action(request, 'set_debug_jobs'))
            self.assertEqual(execute.call_count, expected_count)
            if not expected_count:
                self.assertEqual(result['exception'], 'Permission denied')

    def test_superuser_job_debug_does_not_require_local_task_metadata(self):
        manager = Mock()
        manager.get_task_info.side_effect = AssertionError('No local task metadata')
        execute = Mock()
        ns = functions('prodjob/views.py', ['jobs_action'], TaskManagementAuthorisation=lambda: manager,
            TaskActionExecutor=Mock(), do_jedi_action=execute, task_facts=resources.task_facts,
            json=json, HttpResponse=lambda data: data, _logger=Mock())
        request = SimpleNamespace(user=user(superuser=True), body=json.dumps({
            'jobs': [{'taskid': 1, 'pandaid': 10}], 'parameters': []}))
        result = json.loads(ns.jobs_action(request, 'set_debug_jobs'))
        self.assertIsNone(result[0]['exception'])
        execute.assert_called_once()
        manager.get_task_info.assert_not_called()
