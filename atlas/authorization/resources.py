"""Trusted resource facts and policy-backed application permission APIs."""

import re
import json
from functools import lru_cache

from .principals import DelegatedIdentity, group_codes, roles_for
from .service import authorize


def task_parameter_candidates(action):
    """Witness values for a priority form, derived from policy bounds.

    This is only used for displaying capabilities. Every submitted value is
    evaluated again by the normal task gate before execution.
    """
    candidates = [[]]
    if action == 'change_priority':
        try:
            from .service import get_engine
            for row in get_engine().enforcer.get_policy():
                if row[1:3] != ['task', action]:
                    continue
                bound = json.loads(row[4]).get('priority')
                if isinstance(bound, dict):
                    candidates.append([bound.get('min', bound.get('max'))])
                elif isinstance(bound, list) and bound:
                    candidates.append([bound[0]])
        except Exception:
            pass  # The regular policy gate below still fails closed.
    return candidates


def can_access(user, obj_type, action, *, object_id=None, groups=None, editor=None,
               facts=None, source=''):
    def context():
        actual_groups = group_codes(user) if groups is None else groups() if callable(groups) else groups
        direct_editor = (user.user_permissions.filter(name='Can edit GDPConfig').exists()
                         if editor is None and obj_type in ('gdpconfig', 'global_share') else bool(editor))
        obj = facts() if callable(facts) else facts
        return roles_for(user, actual_groups, direct_editor), obj or {}, {}
    return authorize(actor=user.delegated_by if isinstance(user, DelegatedIdentity) else user.username,
                     effective_user=user.username, obj_type=obj_type, object_id=object_id,
                     action=action, context=context, source=source)


def can_request(user, production_request, action, *, facts=None, source=''):
    def context_facts():
        obj = dict((facts() if callable(facts) else facts) or {})
        obj['request_type'] = production_request.request_type
        return obj
    return can_access(user, 'request', action, object_id=production_request.reqid,
                      facts=context_facts, source=source)


def task_facts(task):
    return {'tasktype': 'analy' if task.is_analy else 'prod',
            'request_type': getattr(task, 'request_type', ''),
            'group_production': task.is_group_production}


def task_context(user, groups, task, action, params, user_fullname):
    roles = roles_for(user, groups)
    if not roles:
        return [], {}, {}
    if not task.is_analy:
        if user.username == task.owner:
            roles.append('task-owner')
        if task.phys_group in groups or task.request_phys_group in groups:
            roles.append('task-group-member')
    else:
        name_parts = task.name.split('.')
        if (f'{user.first_name} {user.last_name}' == task.owner
                or (isinstance(user, DelegatedIdentity) and user.asserted_fullname
                    and user.asserted_fullname == task.owner)
                or (len(name_parts) > 1 and name_parts[1] == user.username)):
            roles.append('analysis-owner')
        if task.phys_group in groups:
            roles.append('analysis-group-member')
    params = params or []
    normalized = {'priority': params[0]} if action == 'change_priority' and params else {}
    return roles, task_facts(task), normalized


def can_task(user, groups, task, action, params=None, fullname=None):
    return authorize(actor=user.delegated_by if isinstance(user, DelegatedIdentity) else user.username,
                     effective_user=user.username, obj_type='task', object_id=task.id, action=action,
                     context=lambda: task_context(user, groups, task, action, params, fullname),
                     source='task_management.task_action')


def can_rule(user, dataset, action):
    return can_access(user, 'replication_rule', action, object_id=dataset,
                      source='task_management.rules_action')


def can_delegate(request, action):
    def context():
        roles = roles_for(request.user)
        if roles and request.user.username == 'bigpanda_api':
            roles.append('user:bigpanda_api')
        return roles, {}, {}
    return authorize(actor=request.user.username, obj_type='delegation', object_id='task',
                     action=action, context=context, source='task_action_ext.delegation')


def can_create_request(user, group, groups=None):
    def facts():
        codes = group_codes(user) if groups is None else groups() if callable(groups) else groups
        return {'group_member': group in codes}
    return can_access(user, 'request', 'create', object_id=group, groups=groups, facts=facts,
                      source='request.create')


def request_group_capabilities(user, candidates):
    @lru_cache(maxsize=1)
    def codes():
        return group_codes(user)
    return [group for group in candidates if can_create_request(user, group, groups=codes)]


def can_save_steps(user, production_request, *, physics_groups, asynchronous=False):
    from .principals import user_for_username
    def facts():
        return {'is_mc': production_request.request_type == 'MC',
                'is_group': production_request.request_type == 'GROUP',
                'validation': production_request.phys_group == 'VALI',
                'same_manager': user.username == production_request.manager,
                'manager_is_coordinator': 'MCCOORD' in group_codes(user_for_username(production_request.manager)),
                'physics_group_member': bool(set(group_codes(user)) & set(physics_groups))}
    return can_request(user, production_request, 'save_steps_async' if asynchronous else 'save_steps',
                       facts=facts, source='request.save_steps')


MC_SHARES = frozenset(('MC evgen', 'MC merge', 'MC root', 'MC simul'))


def share_facts(shares):
    return {'all_derivations': all(s['parent'] == 'Derivations' for s in shares),
            'all_reprocessing': all(s['parent'] == 'Reprocessing' for s in shares),
            'all_mc_parents': all(s['parent'] in MC_SHARES for s in shares),
            'all_mc_names': all(s['name'] in MC_SHARES for s in shares)}


def can_share_batch(user, shares):
    return can_access(user, 'global_share', 'update_batch', object_id=[s['name'] for s in shares],
                      facts=lambda: share_facts(shares), source='gdpconfig.global_share_change')


def manageable_shares(user, shares):
    @lru_cache(maxsize=1)
    def identity_roles():
        return roles_for(user, group_codes(user), user.user_permissions.filter(name='Can edit GDPConfig').exists())
    return [share['name'] for share in shares if authorize(
        actor=user.username, obj_type='global_share', action='manage', object_id=share['name'],
        context=lambda: (identity_roles(), share_facts([share]), {}), source='gdpconfig.manageable_global_shares')]


def scope_context(user, scope, group_names, working_group=None, check_working_group=False):
    parts = scope.split('.', 1)
    iam_groups = {match.group(1) for name in group_names
                  if (match := re.fullmatch(r'IAM:atlas/(.+)/production', name))}
    group_name = scope.split('.')[1] if check_working_group else parts[-1]
    return roles_for(user), {'own_scope': scope == f'user.{user.username}',
        'iam_member': (f'IAM:atlas/{group_name}/production' in group_names
                       if check_working_group else group_name in iam_groups),
        'group_scope': parts[0] == 'group' and len(parts) == 2,
        'working_group_matches': not check_working_group or working_group == group_name}, {}


def can_scope(user, scope, *, action='use', working_group=None, group_names=None):
    return authorize(actor=user.username, obj_type='analysis_scope', object_id=scope, action=action,
        context=lambda: scope_context(user, scope, [g.name for g in user.groups.all()]
                                      if group_names is None else group_names,
                                      working_group, action == 'submit_group'), source='analysis_scope')
