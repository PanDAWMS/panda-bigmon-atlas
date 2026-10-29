"""Read-only API coverage. No share change endpoint is called by these tests."""

from decimal import Decimal
from unittest.mock import Mock, patch

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory, force_authenticate

from atlas.gdpconfig.global_share_api import global_share_tree_v2, manageable_global_shares


class GlobalSharePermissionsTests(SimpleTestCase):
    shares = [
        {'name': 'Derivations', 'parent': None, 'value': 20},
        {'name': 'DAOD', 'parent': 'Derivations', 'value': 100},
        {'name': 'DAOD child', 'parent': 'DAOD', 'value': 100},
        {'name': 'Reprocessing', 'parent': None, 'value': 20},
        {'name': 'Repro child', 'parent': 'Reprocessing', 'value': 100},
        {'name': 'MC', 'parent': None, 'value': 60},
        {'name': 'MC evgen', 'parent': 'MC', 'value': 25},
        {'name': 'MC merge', 'parent': 'MC', 'value': 25},
        {'name': 'MC root', 'parent': 'MC', 'value': 25},
        {'name': 'MC simul', 'parent': 'MC', 'value': 25},
        {'name': 'MC child', 'parent': 'MC simul', 'value': 100},
        {'name': 'MC grandchild', 'parent': 'MC child', 'value': 100},
    ]

    def setUp(self):
        self.user = Mock(username='share-reader')
        self.user.user_permissions.filter.return_value.exists.return_value = False

    @patch('atlas.gdpconfig.global_share_api.task_user_rights')
    def test_config_editor_can_manage_every_share(self, rights):
        self.user.user_permissions.filter.return_value.exists.return_value = True
        self.assertEqual(manageable_global_shares(self.user, self.shares), [share['name'] for share in self.shares])
        self.user.user_permissions.filter.assert_called_once_with(name='Can edit GDPConfig')
        rights.assert_not_called()

    @patch('atlas.gdpconfig.global_share_api.task_user_rights')
    def test_group_permissions_match_legacy_scopes(self, rights):
        cases = [
            ([], set()),
            (['unrelated'], set()),
            (['DPD'], {'DAOD'}),
            (['REPR'], {'Repro child'}),
            (['MCPROD'], {'MC evgen', 'MC merge', 'MC root', 'MC simul', 'MC child'}),
            (['DPD', 'REPR'], {'DAOD', 'Repro child'}),
            (['DPD', 'REPR', 'MCPROD'], {'DAOD', 'Repro child', 'MC evgen', 'MC merge', 'MC root', 'MC simul', 'MC child'}),
        ]
        for groups, expected in cases:
            with self.subTest(groups=groups):
                rights.return_value = (self.user, groups)
                self.assertEqual(set(manageable_global_shares(self.user, self.shares)), expected)
        rights.assert_called_with('share-reader')


class GlobalShareTreeApiTests(SimpleTestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.user = Mock(username='share-reader', is_authenticated=True)
        self.user.user_permissions.filter.return_value.exists.return_value = False

    @patch('atlas.gdpconfig.global_share_api.task_user_rights')
    @patch('atlas.gdpconfig.global_share_api.GlobalShare')
    def test_tree_includes_all_shares_and_separate_editable_names(self, model, rights):
        model.objects.order_by.return_value.values.return_value = [
            {'name': 'DAOD', 'parent': 'Derivations', 'value': Decimal('100')},
            {'name': 'Derivations', 'parent': '', 'value': Decimal('30')},
            {'name': 'Other', 'parent': None, 'value': Decimal('70')},
        ]
        rights.return_value = (self.user, ['DPD'])
        request = self.factory.get('/gdpconfig/global_share_tree_v2/')
        force_authenticate(request, user=self.user)
        response = global_share_tree_v2(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {
            'shares': [
                {'name': 'DAOD', 'parent': 'Derivations', 'value': 100},
                {'name': 'Derivations', 'parent': None, 'value': 30},
                {'name': 'Other', 'parent': None, 'value': 70},
            ],
            'manageable_shares': ['DAOD'],
        })

    @patch('atlas.gdpconfig.global_share_api.task_user_rights')
    @patch('atlas.gdpconfig.global_share_api.GlobalShare')
    def test_read_only_user_still_receives_every_share(self, model, rights):
        model.objects.order_by.return_value.values.return_value = GlobalSharePermissionsTests.shares
        rights.return_value = (self.user, [])
        request = self.factory.get('/gdpconfig/global_share_tree_v2/')
        force_authenticate(request, user=self.user)
        response = global_share_tree_v2(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data['shares']), len(GlobalSharePermissionsTests.shares))
        self.assertEqual(response.data['manageable_shares'], [])

    @patch('atlas.gdpconfig.global_share_api.GlobalShare')
    def test_anonymous_request_is_rejected_before_reading_shares(self, model):
        response = global_share_tree_v2(self.factory.get('/gdpconfig/global_share_tree_v2/'))
        self.assertEqual(response.status_code, 403)
        model.objects.order_by.assert_not_called()
