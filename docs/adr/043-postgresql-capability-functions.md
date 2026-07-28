# ADR-043: PostgreSQL capability functions and least-privilege runtime roles

## Status

Proposed. Tracked in [#452](https://github.com/hardbyte/awa/issues/452) as the tightening follow-up
to [#91](https://github.com/hardbyte/awa/issues/91). ADR-042's caller-owned completion function is
the first application of this direction; this ADR does not claim that the ordinary runtime can yet
operate without direct table privileges.

## Context

Awa currently treats the database runtime as one trusted principal. The documented `awa_runtime`
role can enqueue, claim, execute, maintain, and administer jobs. It receives broad DML and
`TRUNCATE` privileges on Awa tables plus blanket function execution because compatibility triggers
and most helpers execute as `SECURITY INVOKER`. The split from `awa_migrator` still protects schema
ownership, but compromise of any runtime surface grants broad data-plane mutation authority.

ADR-042 adds a narrower application-worker principal. Its planned `complete_job` entry point runs as
`SECURITY DEFINER`, so an application transaction can complete one guarded attempt without joining
`awa_runtime` or receiving direct Awa-table privileges. That creates a natural question: should the
whole PostgreSQL function surface become `SECURITY DEFINER` so all runtime table grants can be
removed?

PostgreSQL makes that blanket conversion unsafe. A definer function executes with its owner's
privileges, newly created functions receive `EXECUTE` for `PUBLIC` by default, and a writable
`search_path` can redirect unqualified objects. Awa also has generic schema installers, transition
helpers, dynamic SQL, introspection routines, and internal functions whose arguments were designed
for trusted callers rather than as authorization boundaries. Making every routine a definer would
turn all of them into privileged entry points.

The desired long-term property is therefore not “all functions are definers.” It is:

> Runtime principals can perform only named Awa capabilities. Direct internal-table access and
> execution of internal or migrator helpers are denied.

## Decision

### Capability entry points, not blanket definer conversion

Awa will migrate toward a function-mediated privilege boundary. A small, allowlisted set of
role-specific entry points may be `SECURITY DEFINER`; the rest of the PostgreSQL routine surface
does not become elevated merely because it is a function.

Every installed routine belongs to exactly one class in a machine-readable capability manifest:

| Class | Security mode | Who may execute | Contract |
| --- | --- | --- | --- |
| Public capability | Hardened `SECURITY DEFINER` when it crosses into private tables | Exact documented role grants | Stable under ADR-036; initial names are `insert_job` and `complete_job` |
| Binary-coupled runtime capability | Hardened `SECURITY DEFINER` | Exact producer, executor, maintenance, callback, or admin role | Versioned with the schema/binary compatibility window; not a public SQL API |
| Internal helper | `SECURITY INVOKER` by default | Definer owner and migrator only | Internal; callable by a capability function but not by runtime logins |
| Read-only inspection surface | `SECURITY INVOKER` unless private-table mediation is required | Explicit read roles | Stable only when listed by ADR-036 |
| Migration, DDL, installer, or arbitrary-schema helper | `SECURITY INVOKER` | Migrator/operator only | Never elevated to a runtime capability |
| Trigger function | Chosen per trigger boundary | No direct `EXECUTE` grant is required for trigger firing | `SECURITY DEFINER` only when the trigger intentionally mediates writes from a less-privileged relation |

`awa.install_queue_storage_substrate` remains invoker and migrator-only. A caller-controlled schema,
relation, function, operator, or SQL fragment is disqualifying for a runtime definer entry point.
Such work stays behind the migration boundary even when identifiers are quoted safely.

Internal invoker helpers still work when called by a definer entry point: they inherit the effective
privileges of that entry point's owner. Keeping them non-executable by runtime logins limits the
number of routines that must be reviewed as privilege-escalation boundaries.

### Roles express capabilities

The long-term split is finer than today's combined `awa_runtime` login:

| Principal | Intended capabilities |
| --- | --- |
| Producer | Enqueue and inspect its enqueue result; no claim, completion, maintenance, or admin mutation |
| Executor | Claim, heartbeat, callback-wait transition, retry, and guarded completion for dispatched work |
| Maintenance | Promote, rescue, rotate, prune, reconcile, and run cron/metadata maintenance |
| Admin reader | Read documented operational views and status functions |
| Admin mutator | Cancel, retry, pause, drain, and submit audited batch operations |
| Callback ingress | Resolve only a valid callback token through the callback contract |
| Application finalizer | Execute only the ADR-042 completion capability inside application transactions |

One deployment may grant several capability roles to one login for operational simplicity. The
database ACL remains compositional, so callback-only and maintenance-only deployments do not
silently inherit the full worker or admin surface.

### A bounded execution owner

Definer functions should not be owned by a login, superuser, or schema owner. The strict deployment
profile uses a dedicated `NOLOGIN` execution-owner role that:

- has no `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `BYPASSRLS`, or role-membership inheritance;
- does not own the Awa schema and cannot create or replace functions;
- receives only the table, sequence, and function privileges required by the allowlisted capability
  implementations; and
- cannot grant its own privileges onward.

The migrator creates or replaces routines, transfers each definer entry point to this execution
owner, and applies the manifest ACL in the same migration transaction. The migrator is the only
login allowed to assume the execution owner for ownership changes; ordinary runtime logins cannot
become or inherit it. Later replacements run under that migration authority or transfer ownership
to the migrator and back inside the migration transaction.

A single-role development install remains supported. In that profile function mediation provides a
stable call shape but does not claim privilege separation. `awa doctor` reports the difference
between compatible and strict role configurations.

### Mandatory definer hardening

Every `SECURITY DEFINER` entry point must satisfy all of these conditions:

1. Set a fixed `search_path` containing only `pg_catalog`, trusted Awa schemas, and `pg_temp` last.
2. Schema-qualify every Awa object and security-relevant function, operator, type, and sequence.
3. Accept no caller-controlled SQL identifier or fragment. Dynamic SQL is absent from runtime
   definers; a separately reviewed exception must resolve an allowlisted catalog identity rather
   than interpolate caller text.
4. Perform one bounded capability and enforce its own row-, token-, queue-, and attempt-level
   guards. Possession of `EXECUTE` is not authority to mutate arbitrary jobs.
5. Never execute DDL, `SET ROLE`, privilege changes, or transaction control.
6. Return only the documented result. Errors with correctness meaning use stable SQLSTATE and abort
   the caller transaction when ignoring the result would be unsafe.
7. Revoke `PUBLIC` in the same transaction that creates the function, then grant `EXECUTE` by exact
   `regprocedure` signature to the intended capability roles.
8. Have its owner, `prosecdef`, `proconfig`, language, volatility, body hash, and ACL checked by the
   catalog audit and `awa doctor`.

These rules follow PostgreSQL's guidance for safely writing
[`SECURITY DEFINER`](https://www.postgresql.org/docs/current/sql-createfunction.html) functions.
PostgreSQL [trigger execution](https://www.postgresql.org/docs/current/trigger-definition.html)
follows the invoking role unless the trigger function is itself a definer, so trigger
classification is part of the same audit rather than an implicit exception.

### Public names and internal names

Only entry points listed in [`docs/stability.md`](../stability.md) are public SQL contracts.

Public functions use domain names, not implementation or rollout suffixes. The initial v1 surface
is:

- `awa.insert_job(...)` for SQL producers; and
- `<queue_storage_schema>.complete_job(...)` for caller-owned completion.

`complete_job` names the user-visible job transition even though its token and stale guard identify
one exact attempt. This matches Awa's existing Rust lifecycle terminology while the arguments keep
the attempt boundary explicit. `complete_attempt` would overemphasize a storage fact; `finalize_job`
would conflict with Awa's broader use of finalization for success, failure, retry, and cancellation.

The `_compat` suffix is reserved for internal cross-representation or rolling-upgrade shims such as
today's `insert_job_compat` and `delete_job_compat`. `_runtime` is reserved for binary-coupled
helpers. Neither suffix appears in a new public contract.

Each public definer name has one exact input signature. Awa does not overload it or add a second
variant distinguished only by defaultable parameters: that complicates exact ACLs and PostgreSQL
[function resolution](https://www.postgresql.org/docs/current/typeconv-func.html). Extensible options
belong inside the one versioned request shape; callers use explicit types, and `awa doctor` resolves
one exact `regprocedure`.

The unversioned names are the v1 contract. Their signatures, results, errors, and schema-version
semantics are frozen under ADR-036. Compatible implementation changes retain the name and signature.
A breaking contract introduces `insert_job_v2` or `complete_job_v2`, keeps v1 through the documented
deprecation window, and grants each exact signature independently. The unversioned v1 name never
silently retargets to a breaking implementation.

Binary-coupled capability functions may use explicit schema-version suffixes or be replaced in an
expand migration. They are called only by binaries inside the supported compatibility window and
must not be presented as general SQL APIs.

### Direct COPY is an explicit boundary

Queue-storage direct COPY currently writes storage relations directly. It cannot participate in a
strict no-table-grant profile merely because other operations move behind functions. The
implementation must choose and benchmark one of these shapes before strict producer grants become
the default:

- an append-only ingress/staging relation with `INSERT` only and a hardened promotion boundary;
- a bulk capability function with a portable encoded input; or
- a separately named trusted-throughput profile that retains narrow direct-table privileges and
  documents the larger blast radius.

The compatibility temp-table COPY path is not automatically safe for a definer function: temporary
schemas are caller-writable and are therefore excluded from object resolution. E5-style throughput,
WAL, latency, and dead-tuple evidence decides the production shape. The privilege design must not
silently disable or slow the existing bulk path.

### Additive rollout and revocation

The transition follows ADR-041 and never begins by revoking privileges:

1. **Inventory.** Generate the routine/trigger manifest from a migrated schema and classify every
   executable object, including custom queue-storage-schema templates.
2. **Expand.** Add hardened capability entry points, the bounded owner contract, catalog diagnostics,
   and new binary paths. Existing direct DML and invoker functions continue to work for N-1.
3. **Exercise.** Current binaries use only capability paths in the strict-role integration matrix.
   Mixed-version rehearsal proves N-1 remains functional with the broad legacy grants.
4. **Enable.** After every connected runtime advertises the capability surface, operators apply a
   generated grant plan that adds exact capability-role grants before revoking broad table/function
   grants. Revocation is never inferred from schema version alone.
5. **Fence.** `awa doctor` and startup checks reject a strict-role declaration when required
   capabilities, owners, or ACLs drift. A returning old binary fails on denied legacy DML rather
   than partially operating.
6. **Contract.** A later minor removes retired direct-DML compatibility paths and legacy blanket
   grant guidance.

Custom queue-storage schemas receive the same manifest and ownership rules. An installer cannot
make arbitrary schemas runtime-definer targets; the migrator materializes and audits each schema
before activation.

## Validation

Acceptance requires:

- catalog tests that fail on an unclassified function or trigger, a definer owned by an excessive
  role, `PUBLIC EXECUTE`, an unsafe `search_path`, an unexpected ACL, or a definer with dynamic SQL;
- negative privilege tests for every principal, including cross-capability attempts, direct table
  DML/`TRUNCATE`, installer execution, stale tokens, forged job identifiers, temporary-object
  shadowing, and operator/function shadowing;
- the full Rust and Python lifecycle suites under the strict profile with no direct internal-table
  grants;
- separate producer, executor, maintenance-only, callback-only, admin-reader, admin-mutator, and
  application-finalizer integration cells;
- custom-schema and transaction-pooler coverage;
- a real N-1 expand/use/tighten rehearsal under ADR-041; and
- direct-COPY replacement benchmarks before removing its privileged compatibility profile.

The catalog inventory and expected ACL manifest are release artifacts. `awa doctor --json` reports
the selected profile, missing and excessive grants, owner properties, unsafe definers, and the exact
remediation plan without applying it.

## Consequences

### Positive

- Compromise of one deployable role no longer implies arbitrary Awa-table mutation.
- PostgreSQL privileges align with Awa's producer, executor, maintenance, admin, callback, and
  caller-finalizer contracts.
- Public SQL compatibility functions receive one explicit versioning and hardening boundary.
- Internal helpers remain refactorable and do not all become permanent security-sensitive APIs.

### Negative

- The Rust and Python runtime SQL must be consolidated behind capability functions; this is a large
  compatibility and performance project, not a migration-flag change.
- Every definer function becomes security-critical code with catalog, ACL, negative-test, and
  ownership obligations.
- Operators that want strict separation must provision the bounded execution owner and capability
  roles. Single-role installs remain compatible but do not receive the isolation guarantee.
- Direct COPY needs a measured replacement or an explicitly less-isolated profile.

## Alternatives considered

### Convert every Awa function and trigger to `SECURITY DEFINER`

Rejected. It would elevate generic internal, dynamic-SQL, arbitrary-schema, transition, repair, and
DDL helpers. It also multiplies the number of objects whose default `PUBLIC EXECUTE`, owner,
`search_path`, and argument authorization can become a privilege-escalation bug. Least privilege is
achieved by fewer capability gateways, not by giving every function owner authority.

### Keep broad invoker privileges permanently

Compatible and simple, but it leaves callback, maintenance, worker, producer, and admin processes
with indistinguishable database authority. It remains the transition profile, not the long-term
strict profile.

### Use row-level security instead of functions

Rejected as the primary boundary. Many Awa operations span append-only ledgers, partitions,
sequences, `TRUNCATE`, advisory locks, and guarded multi-relation transitions. RLS cannot express
the capability transaction by itself and introduces owner/bypass behavior that still requires a
trusted function layer.

### Give each runtime role direct grants only on the tables it usually touches

Useful as an interim reduction, but triggers, lifecycle transitions, maintenance, and representation
changes make the table set an unstable implementation detail. Capability grants express the stable
operation while allowing storage internals to evolve.

## Relationship to other ADRs

- **ADR-027/028:** callback ingress and maintenance-only deployments receive distinct database
  capability roles rather than sharing full runtime authority.
- **ADR-036:** only listed compatibility entry points are stable public SQL surfaces.
- **ADR-041:** broad-to-strict grants roll out through expand, capability evidence, operator-visible
  tightening, and a later contract phase.
- **ADR-042:** `complete_job` is the first hardened application-finalizer capability and must
  obey this ADR's naming, ownership, ACL, and diagnostic rules.
