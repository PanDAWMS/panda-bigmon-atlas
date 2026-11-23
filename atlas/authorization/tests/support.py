import ast
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import List
from unittest.mock import Mock

from atlas.authorization import principals, resources

ATLAS = Path(__file__).parents[2]


def functions(path, names, **namespace):
    """Execute production gates with external models/clients supplied by tests."""
    tree = ast.parse((ATLAS / path).read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    for node in nodes:
        node.decorator_list = []
    namespace.update({name: getattr(resources, name) for name in dir(resources)
                      if name.startswith('can_') or name in ('manageable_shares', 'request_group_capabilities')})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ATLAS / path), 'exec'), namespace)
    return SimpleNamespace(**namespace)


def user(name='alice', groups=(), *, superuser=False, editor=False, iam=(), authenticated=True, active=True, staff=False):
    principal = SimpleNamespace(username=name, first_name='Alice', last_name='User',
        is_authenticated=authenticated, is_active=active, is_superuser=superuser, is_staff=staff,
        groups=Mock(), user_permissions=Mock())
    permission_groups = []
    for code in groups:
        group = SimpleNamespace(name='group-' + code, permissions=Mock())
        group.permissions.all.return_value = [SimpleNamespace(name=f'has_{code}_permissions', codename=code)]
        group.permissions.select_related.return_value = []
        permission_groups.append(group)
    for name in iam:
        group = SimpleNamespace(name=name, permissions=Mock())
        group.permissions.all.return_value = []
        group.permissions.select_related.return_value = []
        permission_groups.append(group)
    principal.groups.all.return_value = permission_groups
    principal.user_permissions.filter.return_value.exists.return_value = editor
    principal.user_permissions.select_related.return_value = []
    return principal


def task(task_id=1, owner='alice', **kwargs):
    fields = dict(id=task_id, owner=owner, name='prod.task', status='running',
                  is_analy=False, is_group_production=False, phys_group='PHYS', request_phys_group='REQ')
    fields.update(kwargs)
    return SimpleNamespace(**fields)


def task_manager(principal, tasks):
    path = ATLAS / 'task_action/task_management.py'
    tree = ast.parse(path.read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name in
             ('TaskManagementAuthorisation', 'TaskActionAllowed', 'RuleActionAllowed')]
    statuses = SimpleNamespace(FINISHED='finished', DONE='done', FAILED='failed', BROKEN='broken',
                               OBSOLETE='obsolete', ABORTING='aborting', TOABORT='toabort')
    namespace = dict(__name__=__name__, dataclass=dataclass, List=List, User=SimpleNamespace,
        ProductionTask=SimpleNamespace(ALL_JEDI_STATUS=['running'] + list(vars(statuses).values()),
                                       NOT_RUNNING=['finished', 'done', 'failed', 'broken', 'obsolete'], STATUS=statuses),
        can_task=resources.can_task, can_rule=resources.can_rule,
        task_parameter_candidates=resources.task_parameter_candidates,
        delegated_task_user_rights=principals.delegated_task_user_rights,
        task_user_rights=lambda *args: (principal, resources.group_codes(principal)))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    manager = namespace['TaskManagementAuthorisation']()
    manager.get_task_info = tasks.__getitem__
    return manager
