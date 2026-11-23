"""Authoritative policy decisions. Errors and unmapped operations deny access."""

from functools import lru_cache
import logging
from time import perf_counter

logger = logging.getLogger('prodtask_ELK')


@lru_cache(maxsize=1)
def get_engine():
    from .engine import AuthorizationEngine
    return AuthorizationEngine()


def authorize(*, actor, obj_type, action, context, object_id=None, source='', effective_user=None, audit=True):
    started = perf_counter()
    allowed = False
    event = dict(actor=actor, effective_user=effective_user or actor,
                 object_type=obj_type, object_id=object_id, action=action,
                 decision_source=source, policy_bundle_hash=None,
                 matching_policy_references=[])
    try:
        engine = get_engine()
        event['policy_bundle_hash'] = engine.bundle_hash
        roles, obj, params = context()
        allowed, references = engine.evaluate(roles, obj_type, action, obj, params)
        allowed = bool(allowed)
        event.update(policy_roles=roles, object_facts=obj,
                     matching_policy_references=references,
                     outcome='allow' if allowed else 'deny')
        if (obj_type, action) not in engine.operations:
            event['outcome'] = 'unmapped'
    except Exception as error:
        allowed = False
        event.update(outcome='error', error_type=type(error).__name__)
    event.update(allowed=allowed, evaluation_duration_ms=round((perf_counter() - started) * 1000, 3))
    try:
        # Capability probes use the same decisions without producing audit noise.
        if audit and not allowed:
            if event['outcome'] == 'error':
                logger.error('authorization_decision', extra=event)
            else:
                logger.info('authorization_decision', extra=event)
    except Exception:
        # Logging availability must never grant access or undo a policy denial.
        pass
    return allowed
