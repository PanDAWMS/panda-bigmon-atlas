# Casbin shadow authorization (stages 1 and 2)

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

Stage 2 additionally observes these legacy gates and capabilities:

| Area | Observed decisions |
| --- | --- |
| Requests | Creation-group check and returned group capabilities; MC approval/status helper; synchronous and asynchronous step-save permission gates; manager-only slice creation; large submission exceptions; priority and approval UI capabilities; privileged deleted-output obsolescence; email resend; train close capability |
| PMG requests | The newer PMG register/hold/cancel endpoint's login-only permission, separately from the older restricted MC approval helper |
| Global shares | Writer authorization for the entire batch, plus per-share manageable capabilities in the read API |
| Analysis scopes | Returned scope capabilities; actual container-registration scope grants and denials; group submission including the working-group match |
| Configuration | GDPConfig and JSON writes; MC workflow and PMG hashtag parameters; retry-error form/edit/delete and POST creation; dataset recovery submission; DPD group-production deletion/container administration |

All hooks are best effort; evaluation and logging exceptions cannot change
legacy responses, exceptions, mutations or returned capability lists. Domain
validation remains separate. In particular, the `registered` result for merge
train requests is a workflow transition, not an authorization denial.

The stage 2 policies deliberately preserve these differences:

- The step-save MC check examines **the request manager's** MCCOORD membership.
  It does not grant based on the acting user's MCCOORD role. The synchronous
  GROUP step-save check requires membership in any physics group (or DPD);
  the asynchronous path has no corresponding group gate.
- A single global-share grant must cover the whole batch. DPD and REPR grants
  cannot combine to authorize a mixed batch. MCCOORD's all-parent and all-name
  scopes cannot combine either. Superuser status alone grants no share access.
  The direct `Can edit GDPConfig` permission grants access; inherited permissions
  are not added implicitly. Empty batches retain the legacy role-based result.
- Workflow configuration checks use the `MCCOORD` coordinator group.
- Scope enumeration and container creation have no superuser bypass. Group
  submission allows superusers but still requires the requested working group
  to match. Enumeration can compare only returned candidates; attempted uses
  also compare denied scopes. There is no invented universe of scope names.
- Retry-error POST create/clone and the newer PMG approval endpoint currently
  have no role gate beyond login. They have explicit, separate policy actions;
  this migration does not silently restrict them.

Human username exceptions are deliberately absent from the new policies. Role
construction uses group memberships, direct permissions, resource relationships
and the superuser flag. A renamed user with the same roles has the same grants.
MC approval uses MCCOORD; the removed train-close and retry-management exceptions
do not introduce replacement group grants implicitly. Their remaining policies
require the superuser flag. Legacy checks can still honor their old username
exceptions: these produce shadow mismatches until the users have a role that
the corresponding policy grants. No legacy decisions are changed here.

The older `check_action_allowed` / `get_permissions` task UI helpers, job-only
superuser checks, privileged retry parameter rewriting, Django admin checks and
endpoints without a legacy authorization gate are outside these stages. The
request creation capability check is not a newly enforced gate on every request
writer. FIRST_ADOPTERS presentation routing is not treated as authorization.
No claim of complete application parity should be made from these logs.

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
and the authenticated `bigpanda_api` service account to the four external
delegation actions. That explicitly retained service account is the sole
identity-specific grant; it does not add username roles to other authorization
areas. Audit identities, resource ownership and personal scopes still identify
users, but do not encode individual exceptions. Replication-rule grants are
superuser-only, preserving current behavior.

## Audit stream and rollout

The `authorization_shadow` message uses the existing `prodtask_ELK` JSON logger
and writes to `LOG_ROOT/json/logfile.prodtask_ELK`. Each evaluated object/gate produces
an INFO record with `outcome` (`match`, `mismatch`, `unmapped`, `error`), actor,
effective user, source, object identifier, action, legacy and shadow results,
state eligibility, policy roles, object facts, parsed requested priority, policy
bundle hash, matching row references and elapsed time.
Errors include their exception type; initialization failures also record their
underlying type. Logs do not contain tokens, full names, raw request bodies or
arbitrary action parameters. There is no request correlation ID in these stages.

Aggregate event counts by source/action/outcome and policy hash. Investigate both
mismatch directions and all errors/unmapped operations. Zero mismatches does not
demonstrate parity for operations with no observations. Shadow mode does not switch
automatically to enforcement: a later change must explicitly enable enforcement
after coverage and known legacy differences have been reviewed.

Tests exercise real Casbin and the actual legacy permission class compiled from
source, with external data access stubbed, avoiding imports of Oracle and Celery
clients. Stage 2 tests similarly execute production permission gates against
stubbed models and clients, checking batch mutation counts, denial paths, workflow
results and failure isolation. They require no database or remote calls.
