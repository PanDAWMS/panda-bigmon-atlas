# Casbin shadow authorization (stage 1)

Authentication and legacy authorization remain authoritative. No observer returns
an access decision or executes an action. Casbin grants, denials, missing packages,
invalid policies, evaluation errors and logging failures cannot change the legacy
result. There is deliberately no enforcement mode in this implementation.

Install dependencies using the project's normal installation process. Before
deployment, validate the bundled policies and run the isolated tests:

```sh
python -m atlas.authorization.engine
python -m unittest discover -s atlas/authorization/tests -v
```

`AUTHORIZATION_MODE = 'shadow'` is the default. Override it with `'off'` in local
Django settings to disable evaluation. Any other value emits an error event and
continues using legacy authorization. Models and policies load lazily once per
process. Initialization failures are cached too. Restart web and Celery workers
after policy changes or fixing initialization failures. There are no policy
database tables, external policy services, runtime mutations or network lookups.

## Coverage

The observer is called from `TaskManagementAuthorisation.__task_action_check`
(both single and batch entry points), `rules_action_authorisation`, and the
`task_action_ext` delegation gate. Both grants and denials are observed. Existing
batch rejection/partial-execution behavior and response formats are unchanged.
Task-state eligibility (`action_allowed`, including the failed-analysis retry
exception) remains domain logic. The shadow result is state eligibility AND the
Casbin user permission, compared with the same legacy conjunction. A superuser
policy cannot override state eligibility.

The older `check_action_allowed` / `get_permissions` UI helpers, request approval,
request creation, job-only superuser checks, privileged retry parameter rewriting,
and other configuration/view checks are not instrumented in stage 1. No claim of
complete application parity should be made from these logs.

Existing `task_user_rights` supplies role facts from Django group permission
codenames and IAM production groups. Only DPD and MCCOORD are copied as policy
roles; group membership is compared with resource groups to derive relationship
roles. Ownership comparisons preserve legacy behavior, including delegated
caller-supplied full names. This is compatibility data, not a new authentication
mechanism. User-supplied roles and object facts are never passed to the engine.
Task action events identify the effective subject; the separate delegation event
records the authenticated service account and requested effective user.

## Portable model and policies

The Django-independent engine exposes:

```python
engine.enforce(roles, obj_type, act, obj, params)  # bool
engine.evaluate(roles, obj_type, act, obj, params)  # bool, matching row references
```

The model follows https://github.com/PanDAWMS/panda-authz/blob/main/docs/policy-format.md,
using `r.params` in the matcher. There is no dependency on that package.

```text
p, role, obj_type, act, obj_constraints, act_constraints, allow
```

Files use the documented inline-JSON notation, with comments on separate lines.
The loader understands commas inside JSON, validates rows before use, and retains
source filename/line references. Unknown operations deny and are logged as unmapped.
Only `allow` is supported. Multiple files compose as a union of grants: file order
does not implement overrides and a local rule cannot narrow a shared grant.

Object constraints require every key to exist and match its JSON value and type.
Parameter constraints require every key to match: two numeric values specify an
inclusive range and a nonempty string list specifies membership. Numeric values
use legacy integer conversion; missing, boolean and invalid values do not match.
An empty constraint object imposes no restriction. Unlike the upstream matcher,
matching one field does not bypass the remaining constraints.

The local, documented extension `{"priority":{"max":570}}` represents the legacy
upper bound without inventing a lower bound; `min` is also supported. Consumers
sharing these rows must implement this extension. The task adapter currently
supplies only the priority parameter because it is the only parameter constrained
by this legacy user permission gate. This is not a general request validator.

Shared files contain production and analysis task permissions. Resource-scoped
`task-owner`, `task-group-member`, `analysis-owner` and `analysis-group-member`
roles describe relationships for this object only; never persist or accept them
as global identity roles. Object facts are `tasktype` (`prod` or `analy`) and
`group_production`. Action names retain the existing task API vocabulary.

ProdSys files bind the local superuser flag to task and replication-rule grants,
and the authenticated `bigpanda_api` identity to the four external delegation
actions. Replication-rule grants are superuser-only, preserving current behavior.

## Audit stream and rollout

`authorization_shadow` uses the existing JSON logger setup and writes to
`LOG_ROOT/json/logfile.authorization_shadow`. Each evaluated object/gate produces
an INFO record with `outcome` (`match`, `mismatch`, `unmapped`, `error`), actor,
effective user, source, object identifier, action, legacy and shadow results,
state eligibility, policy roles, object facts, parsed requested priority, policy
bundle hash, matching row references and elapsed time.
Errors include their exception type; initialization failures also record their
underlying type. Logs do not contain tokens, full names, raw request bodies or
arbitrary action parameters. There is no request correlation ID in stage 1.

Aggregate event counts by source/action/outcome and policy hash. Investigate both
mismatch directions and all errors/unmapped operations. Zero mismatches does not
demonstrate parity for operations with no observations. Stage 1 does not switch
automatically to enforcement: a later change must explicitly enable enforcement
after coverage and known legacy differences have been reviewed.

Tests exercise real Casbin and the actual legacy permission class compiled from
source, with external data access stubbed, avoiding imports of Oracle and Celery
clients. They require no database, remote calls or task execution.
