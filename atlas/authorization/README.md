# Casbin authorization

Casbin is authoritative for application permission decisions. Authentication,
login routes, OIDC, token, Basic and session backends are unchanged. There is no
legacy fallback or shadow/off mode. Old AUTHORIZATION_MODE overrides are ignored.

## Running and deploying

Install the project dependencies, validate the bundle, and run its tests:

```sh
python -m atlas.authorization.engine
python -m unittest discover -s atlas/authorization/tests -v
```

The dedicated authorization app validates the engine during startup. Missing
Casbin, an invalid model or malformed policy prevents startup. At runtime,
unknown operations, identity/fact lookup errors and evaluation failures deny.
Audit-log failures do not alter a decision. No database migration is required.
Restart web and Celery workers when deploying this change or updating policies;
each process caches its validated enforcer. Policies have no runtime mutations
and require no network lookup or external authorization package.

## Application boundaries

`service.authorize` is the central decision API. `principals.py` reads identity,
group and direct-permission facts; `resources.py` builds trusted resource facts
and exposes `can_access`, `can_task`, `can_scope`, and other application gates.
All grants are in the checked-in policies. Anonymous and inactive principals
receive no roles. Usernames are used for audit and resource ownership, never
human allowlists. The retained external API service account is the only explicit
identity grant and can delegate only its four policy actions.

After that delegation check passes, the service can assert a subject's username
and full name, including for an analysis user without a local Django account.
An ephemeral delegated identity holds those claims without creating an account.
Unknown subjects receive no group or superuser assignments; existing accounts
retain their actual roles and active status. Service-asserted full names can
establish analysis ownership only on this authorized delegation path. Regular
calls cannot grant themselves access by supplying a full name. Delegated audit
events identify both the authenticated service and the effective subject.

The covered boundaries include task and replication-rule actions, delegated API
actions, old task permission helpers, request creation/approval/step saves,
request and train capabilities and writes, global-share updates, analysis scope
selection and submission, configuration and retry management, dataset recovery,
group-production administration and job-only privileges. Django admin uses its
standard site and permission checks, outside this application policy bundle.
Authentication backends are not changed.

Task-state eligibility, input validation and workflow transitions remain domain
logic. A superuser grant cannot make an invalid task action executable. Older UI
action aliases are derived from actual task permissions. Priority form visibility
uses candidate values from policy bounds; each submitted priority is checked
again at execution. Batch share updates require one grant to cover the whole
batch; permissions from different roles or MC scopes cannot be combined.

Permission checks run before the protected writes, including step-save changes,
PMG batches and train-close mutations. Queued action workers
check current permissions again before executing. Historical queued superuser
flags are accepted for message compatibility but are not trusted for decisions.
Step-save permission denials retain the legacy response contract: HTTP 200 JSON
for the synchronous caller and a JSON string with `async_name: save_slices` for
the worker. `error_approve_message` reports the denial, `slices` is empty and all
requested slices are reported in `no_action_slices`; no writes are performed.
Analysis ownership uses authenticated identity data or the authorized service's
asserted identity. The analysis request scope picker includes all configured
physics-group scopes; the container scope list uses the scope policies.

## Current policy behavior

- MCCOORD members can create requests for every physics group, as can DPD,
  ALL and superusers. Other users require membership in the requested group.
  These policies back the creation capability checks; the legacy
  `request_clone_or_create` writer intentionally does not enforce that group gate.
- MCCOORD is an administrative role for requests with `request_type == MC` and
  their production tasks. `mc_coordinators.csv` grants all defined request and
  task actions in that scope, including step saves for another manager, slice
  creation, email resend, privileged retry options and task-linked job debug.
  Classification comes from the stored request type, not a request-body claim or
  a global superuser flag. Existing ordinary grants remain unchanged; the new
  administrative privileges do not extend to GROUP, REPROCESSING or ANALYSIS
  tasks, unrelated settings, or jobs without a verified MC task. Task-state
  restrictions and input validation continue to apply.
- MC approval uses MCCOORD, without individual coordinator exceptions. Train
  close/reopen and retry management retain their configured superuser grants.
- MC step-save permission uses the request manager's coordinator membership;
  the synchronous GROUP check requires physics-group or DPD membership. The
  asynchronous policy retains the former absence of that GROUP restriction.
- Global-share and GDPConfig editing use the direct Can edit GDPConfig assignment
  or scoped DPD/REPR/MCCOORD grants. Superuser status alone does not grant access.
- Analysis request creation retains its login-only permission boundary. Group
  authorization runs during step verification: superusers can submit without
  membership, while workingGroup must still match. User-scope submission retains
  its empty-workingGroup requirement without an additional ownership check.
  Container-scope access requires the user's own scope or IAM production-group
  membership, with no implicit superuser bypass.
- PMG register/hold/cancel and retry-error POST create/clone retain explicit
  subject-role grants from the approved policies. This cutover does not add a
  role restriction to those actions. Edit/delete permissions remain separate.
- Job debug and job kills without a task use their explicit administrative
  policies. Superuser job debug does not require local task metadata; MCCOORD
  job debug still requires a verified MC task. Jobs without a task remain
  superuser-only. The retry modifier is automatically enabled for superusers
  and MCCOORD batches of verified MC tasks. An explicitly supplied modifier
  does not add another permission gate; normal task authorization still applies.

## Model and policy format

The Django-independent engine follows the PanDA policy format at
https://github.com/PanDAWMS/panda-authz/blob/main/docs/policy-format.md:

```python
engine.enforce(roles, obj_type, act, obj, params)
engine.evaluate(roles, obj_type, act, obj, params)  # bool and source references
```

```text
p, role, obj_type, act, obj_constraints, act_constraints, allow
```

The loader supports inline JSON, including commas, validates each row and tracks
filename/line references. Files compose as a union of grants. Order is not an
override mechanism, and local policies cannot narrow a broad shared grant.
All object constraints must match exactly, including JSON types. All parameter
constraints in a row must match: a numeric pair is an inclusive range, a string
list means membership. Missing, boolean or invalid numeric parameters do not
match. The documented min/max extension, such as {"priority":{"max":570}},
represents one-sided bounds. Consumers sharing these rules need those semantics.
An empty constraint object imposes no restriction.

Scoped ownership/membership roles apply only to the current resource. Raw
permission codenames cannot inject reserved relationship or superuser roles.
There are no human identity grants in the bundle.

## Audit and validation

The authorization_decision message uses the existing prodtask_ELK JSON logger.
Events contain actor, effective user, resource, action, source, allowed result,
policy bundle hash, matching policy references, safe resource facts and elapsed
time. Successful authorization decisions are DEBUG, denials and unmapped actions
are INFO, and evaluation failures are ERROR. Outcomes are allow, deny, unmapped
or error. Tokens, caller-supplied full
names and arbitrary action parameters are not logged.

Tests use real Casbin and execute production gate functions with external models
and clients stubbed. They cover denial-before-write, batch behavior, queued-role
revocation, stale settings, policy/fact/logging failures, identity restrictions,
priority limits, authorized service assertions and standard Django admin setup
without a live Oracle or JEDI call.
