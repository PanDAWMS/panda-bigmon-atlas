"""Best-effort observation only: this module never supplies an access decision."""

from functools import lru_cache
import logging
from time import perf_counter

logger = logging.getLogger('prodtask_ELK')


def get_mode():
    from django.conf import settings
    return getattr(settings, 'AUTHORIZATION_MODE', 'shadow') if settings.configured else 'shadow'


@lru_cache(maxsize=1)
def get_engine():
    # Cache failures too, avoiding repeated initialization on every request.
    try:
        from .engine import AuthorizationEngine
        return AuthorizationEngine(), None
    except Exception as error:
        return None, type(error).__name__


def observe(*, actor, obj_type, object_id, action, legacy_allowed, context,
            source, action_allowed=True, effective_user=None):
    """context is lazy so even fact construction cannot affect legacy behavior."""
    try:
        mode = get_mode()
        if mode == 'off':
            return
        started = perf_counter()
        event = dict(decision_source=source, actor=actor,
                     effective_user=effective_user or actor, object_type=obj_type,
                     object_id=object_id, action=action, legacy_allowed=bool(legacy_allowed),
                     action_allowed=bool(action_allowed), casbin_allowed=None,
                     policy_bundle_hash=None, matching_policy_references=[])
        try:
            if mode != 'shadow':
                raise ValueError('Only off and shadow modes are supported')
            engine, error = get_engine()
            if engine is None:
                event['initialization_error'] = error
                raise RuntimeError(error)
            event['policy_bundle_hash'] = engine.bundle_hash
            roles, obj, params = context()
            event['policy_roles'] = roles
            event['object_facts'] = obj
            # Only a parsed numeric priority is useful for discrepancy analysis;
            # never include arbitrary action arguments or caller-supplied names.
            if 'priority' in params:
                try:
                    event['requested_priority'] = int(params['priority'])
                except (TypeError, ValueError, OverflowError):
                    event['priority_parse_error'] = True
            permitted, references = engine.evaluate(roles, obj_type, action, obj, params)
            event['casbin_allowed'] = bool(action_allowed and permitted)
            event['matching_policy_references'] = references
            event['outcome'] = ('unmapped' if (obj_type, action) not in engine.operations else
                                'match' if event['casbin_allowed'] == bool(legacy_allowed) else 'mismatch')
        except Exception as error:
            event.update(outcome='error', error_type=type(error).__name__)
        event['evaluation_duration_ms'] = round((perf_counter() - started) * 1000, 3)
        logger.info('authorization_shadow', extra=event)
    except Exception:
        # Includes logging/formatting failures. No shadow failure may change access.
        pass


def task_context(user, groups, task, action, params, user_fullname):
    # Reserved relationship roles are derived here, never read from codenames.
    roles = [role for role in ('DPD', 'MCCOORD') if role in groups]
    if user.is_superuser:
        roles.append('prodsys:superuser')
    if not task.is_analy:
        if user.username == task.owner:
            roles.append('task-owner')
        if task.phys_group in groups or task.request_phys_group in groups:
            roles.append('task-group-member')
    else:
        # Preserve legacy identity comparisons, including supplied full name,
        # solely for compatibility. It is not a newly authenticated identity.
        if (f'{user.first_name} {user.last_name}' == task.owner
                or (user_fullname and user_fullname == task.owner)
                or task.name.split('.')[1] == user.username):
            roles.append('analysis-owner')
        if task.phys_group in groups:
            roles.append('analysis-group-member')
    normalized = {'priority': params[0]} if action == 'change_priority' and params else {}
    return roles, {'tasktype': 'analy' if task.is_analy else 'prod',
                   'group_production': task.is_group_production}, normalized


def observe_task(user, groups, task, action, params, fullname, action_allowed, user_allowed):
    observe(actor=user.username, obj_type='task', object_id=task.id, action=action,
            legacy_allowed=action_allowed and user_allowed, action_allowed=action_allowed,
            context=lambda: task_context(user, groups, task, action, params, fullname),
            source='task_management.task_action')


def observe_rule(user, dataset, action, allowed):
    observe(actor=user.username, obj_type='replication_rule', object_id=dataset,
            action=action, legacy_allowed=allowed,
            context=lambda: (['prodsys:superuser'] if user.is_superuser else [], {}, {}),
            source='task_management.rules_action')


def observe_delegation(request, action, allowed):
    try:
        observe(actor=request.user.username, obj_type='delegation', object_id='task',
                action=action, legacy_allowed=allowed,
                effective_user=request.data.get('username') if allowed else None,
                context=lambda: ([f'user:{request.user.username}'], {}, {}),
                source='task_action_ext.delegation')
    except Exception:
        pass
