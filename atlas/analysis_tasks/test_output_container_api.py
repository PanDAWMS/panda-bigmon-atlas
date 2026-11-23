from unittest.mock import Mock, patch

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory, force_authenticate

from atlas.analysis_tasks.scopes import container_scopes_by_user
from atlas.analysis_tasks.views import get_analysis_container_scopes, register_analysis_output_container


class ContainerScopesTests(SimpleTestCase):
    @patch('atlas.analysis_tasks.scopes.User')
    def test_only_membership_in_production_groups_grants_group_scopes(self, user):
        user.objects.get.return_value.username = 'analyst'
        user.objects.get.return_value.is_authenticated = True
        user.objects.get.return_value.is_active = True
        user.objects.get.return_value.is_superuser = False
        user.objects.get.return_value.groups.all.return_value = [
            Mock(name='unused') for _ in range(4)]
        for group, name in zip(user.objects.get.return_value.groups.all.return_value,
                               ['IAM:atlas/phys-test/production', 'IAM:atlas/phys-other/reader',
                                'unrelated', 'IAM:atlas/proj-test/production']):
            group.name = name
        self.assertEqual(container_scopes_by_user('analyst'),
                         ['user.analyst', 'group.phys-test', 'group.proj-test'])


class AnalysisOutputContainerApiTests(SimpleTestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.data = {'requestID': '1234', 'scope': 'user.analyst', 'name': 'results',
                     'datasets': ['dataset.one']}

    def post(self, data=None, authenticated=True):
        request = self.factory.post('/api/register_analysis_output_container/',
                                    self.data if data is None else data, format='json')
        if authenticated:
            force_authenticate(request, user=Mock(username='analyst', is_authenticated=True))
        return register_analysis_output_container(request)

    @patch('atlas.analysis_tasks.views.container_scopes_by_user', return_value=['user.analyst', 'group.phys-test'])
    def test_scope_endpoint_uses_the_shared_membership_lookup(self, scopes):
        request = self.factory.get('/api/get_analysis_container_scopes/')
        force_authenticate(request, user=Mock(username='analyst', is_authenticated=True))
        response = get_analysis_container_scopes(request)
        self.assertEqual(response.data, ['user.analyst', 'group.phys-test'])
        scopes.assert_called_once_with('analyst')

    @patch('atlas.analysis_tasks.views.DDM')
    @patch('atlas.analysis_tasks.views.collect_all_output_datasets', return_value=['dataset.one', 'dataset.two'])
    @patch('atlas.analysis_tasks.views.can_scope', return_value=True)
    def test_registers_or_extends_container_for_selected_outputs(self, scopes, outputs, ddm):
        for scope in ['user.analyst', 'group.phys-test']:
            for exists in [False, True]:
                with self.subTest(scope=scope, exists=exists):
                    ddm.reset_mock()
                    ddm.return_value.dataset_exists.return_value = exists
                    response = self.post({**self.data, 'scope': scope})
                    self.assertEqual(response.status_code, 200)
                    method = (ddm.return_value.register_datasets_in_container if exists
                              else ddm.return_value.register_container)
                    method.assert_called_once_with(f'{scope}.results', ['dataset.one'])
        outputs.assert_called_with(1234)

    @patch('atlas.analysis_tasks.views.DDM')
    @patch('atlas.analysis_tasks.views.collect_all_output_datasets', return_value=['dataset.one'])
    @patch('atlas.analysis_tasks.views.can_scope', side_effect=lambda user, scope: scope == 'user.analyst')
    def test_rejects_unauthorised_scope_and_unrelated_datasets(self, scopes, outputs, ddm):
        self.assertEqual(self.post({**self.data, 'scope': 'group.other'}).status_code, 403)
        self.assertEqual(self.post({**self.data, 'datasets': ['other.dataset']}).status_code, 400)
        ddm.assert_not_called()

    @patch('atlas.analysis_tasks.views.DDM')
    def test_requires_authentication_and_valid_nonempty_input(self, ddm):
        self.assertEqual(self.post(authenticated=False).status_code, 401)
        for changes in [{'name': ''}, {'name': 'scope:bad'}, {'datasets': []}, {'requestID': 1}]:
            with self.subTest(changes=changes):
                self.assertEqual(self.post({**self.data, **changes}).status_code, 400)
        ddm.assert_not_called()
