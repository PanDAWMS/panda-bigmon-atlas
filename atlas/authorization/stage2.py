"""Resource facts for stage 2. Observers never return permission decisions."""

from functools import wraps
import re

from .shadow import observe


def best_effort(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            function(*args, **kwargs)
        except Exception:
            pass
    return wrapped


def roles_for(user, groups=(), editor=False):
    # Raw permission codenames cannot inject reserved relationship roles.
    roles = [role for role in ('DPD', 'REPR', 'MCCOORD', 'ALL') if role in groups]
    roles.append('subject')
    if user.is_superuser:
        roles.append('prodsys:superuser')
    if editor:
        roles.append('gdpconfig-editor')
    return roles


@best_effort
def observe_access(user, obj_type, action, legacy_allowed, *, object_id=None,
                   groups=(), editor=False, facts=None, source):
    def context():
        actual_groups = groups() if callable(groups) else groups
        obj = facts() if callable(facts) else facts
        return roles_for(user, actual_groups, editor), obj or {}, {}
    observe(actor=user.username, obj_type=obj_type, object_id=object_id,
            action=action, legacy_allowed=legacy_allowed, context=context, source=source)


@best_effort
def observe_request_create(user, group, groups, legacy_allowed):
    observe_access(user, 'request', 'create', legacy_allowed, object_id=group,
                   groups=groups, facts={'group_member': group in groups},
                   source='request_views.check_request_group')


@best_effort
def observe_request_group_capabilities(user, groups, candidates):
    for group in candidates:
        observe_access(user, 'request', 'create',
                       user.is_superuser or 'ALL' in groups or 'DPD' in groups or group in groups,
                       object_id=group, groups=groups, facts={'group_member': group in groups},
                       source='request_views.check_allow_request_create')


@best_effort
def observe_request_steps(user, production_request, legacy_allowed, *, groups,
                          manager_groups, physics_groups, asynchronous=False):
    def facts():
        return {'is_mc': production_request.request_type == 'MC',
                'is_group': production_request.request_type == 'GROUP',
                'validation': production_request.phys_group == 'VALI',
                'same_manager': user.username == production_request.manager,
                'manager_is_coordinator': 'MCCOORD' in manager_groups(),
                'physics_group_member': bool(set(groups()) & set(physics_groups))}
    observe_access(user, 'request', 'save_steps_async' if asynchronous else 'save_steps',
                   legacy_allowed, object_id=production_request.reqid, facts=facts,
                   source='request_steps_approve_or_save_async' if asynchronous else
                          'request_steps_approve_or_save')


MC_SHARES = frozenset(('MC evgen', 'MC merge', 'MC root', 'MC simul'))


def share_context(user, shares, groups, editor):
    # These describe the entire batch. Per-object grants cannot be combined
    # across roles or across MC parent/name permission scopes.
    return roles_for(user, groups, editor), {
        'all_derivations': all(s['parent'] == 'Derivations' for s in shares),
        'all_reprocessing': all(s['parent'] == 'Reprocessing' for s in shares),
        'all_mc_parents': all(s['parent'] in MC_SHARES for s in shares),
        'all_mc_names': all(s['name'] in MC_SHARES for s in shares),
    }, {}


@best_effort
def observe_share_batch(user, shares, groups, editor, legacy_allowed):
    observe(actor=user.username, obj_type='global_share', object_id=[s['name'] for s in shares],
            action='update_batch', legacy_allowed=legacy_allowed,
            context=lambda: share_context(user, shares, groups, editor),
            source='gdpconfig.global_share_change')


@best_effort
def observe_share_capabilities(user, shares, groups, editor, manageable):
    for share in shares:
        observe(actor=user.username, obj_type='global_share', object_id=share['name'],
                action='manage', legacy_allowed=share['name'] in manageable,
                context=lambda: share_context(user, [share], groups, editor),
                source='gdpconfig.manageable_global_shares')


def scope_context(user, scope, group_names, working_group=None, check_working_group=False):
    parts = scope.split('.', 1)
    iam_groups = {match.group(1) for name in group_names
                  if (match := re.match(r'^IAM:atlas/(.+)/production$', name))}
    # check_user_group historically uses only the second dot-separated token.
    group_name = scope.split('.')[1] if check_working_group else parts[-1]
    return roles_for(user), {
        'own_scope': scope == f'user.{user.username}',
        'iam_member': (f'IAM:atlas/{group_name}/production' in group_names
                       if check_working_group else group_name in iam_groups),
        'group_scope': parts[0] == 'group' and len(parts) == 2,
        'working_group_matches': not check_working_group or working_group == group_name,
    }, {}


@best_effort
def observe_scope(user, scope, legacy_allowed, *, group_names, action='use', working_group=None,
                  source='analysis_tasks.register_analysis_output_container'):
    observe(actor=user.username, obj_type='analysis_scope', object_id=scope,
            action=action, legacy_allowed=legacy_allowed,
            context=lambda: scope_context(user, scope,
                                          group_names() if callable(group_names) else group_names,
                                          working_group, action == 'submit_group'),
            source=source)


@best_effort
def observe_scope_list(user, scopes, group_names):
    for scope in scopes:
        observe_scope(user, scope, True, group_names=group_names, action='list',
                      source='analysis_tasks.container_scopes_by_user')
