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

Every installed Awa routine, in every class, has `EXECUTE` revoked from `PUBLIC`. The manifest
records the exact positive grants for callable surfaces; internal helpers, migration/DDL/install
routines, and trigger functions receive no runtime grant. The migrator applies the revocation in
the routine's creating transaction and configures default privileges so a new routine cannot
accidentally reintroduce `PUBLIC EXECUTE`. Positive grants are manifest-driven and apply only after
the function's final owner is set: the migrator may apply them in the same administrative
transaction when the target roles are configured, otherwise it emits the exact-signature grant
plan that the operator must apply before enablement. `awa doctor` treats any manifest/ACL
disagreement, including `PUBLIC EXECUTE` on an invoker or internal routine, as drift.

`awa.install_queue_storage_substrate` remains invoker and migrator-only. A caller-controlled schema,
relation, function, operator, or SQL fragment is disqualifying for a runtime definer entry point.
Such work stays behind the migration boundary even when identifiers are quoted safely. The one
runtime relation-dispatch shape permitted by this ADR is the bounded maintenance-partition protocol
below: it accepts an integer ring slot, not an identifier, and resolves only manifest-listed Awa
children through verified catalog identity.

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

Role membership is itself part of the authorization boundary. The capability manifest and
`awa doctor` compute the complete transitive membership closure from every runtime, application,
callback, and admin login, following both privileges inherited through membership and every nested
`SET ROLE` path. A strict profile rejects any direct or transitive path to the execution owner,
migrator, schema owner, or another role that can reach them, except for the explicitly allowlisted
migration principal and ownership-change path. Checking only direct membership is insufficient:
PostgreSQL role graphs can convey authority through intermediate `INHERIT` or `SET` memberships.

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
become or inherit it through any direct or transitive `INHERIT`/`SET ROLE` chain. Later replacements
run under that migration authority or transfer ownership to the migrator and back inside the
migration transaction.

A single-role development install remains supported. In that profile function mediation provides a
stable call shape but does not claim privilege separation. `awa doctor` reports the difference
between compatible and strict role configurations.

### Mandatory definer hardening

Every `SECURITY DEFINER` entry point must satisfy all of these conditions:

1. Set a fixed `search_path` containing only `pg_catalog`, trusted Awa schemas, and `pg_temp` last.
2. Schema-qualify every Awa object and security-relevant function, operator, type, and sequence.
3. Accept no caller-controlled SQL identifier or fragment. Dynamic SQL is absent from runtime
   definers except for a manifest-declared maintenance partition dispatcher that satisfies the
   bounded protocol below. Catalog lookup or identifier allowlisting alone is not an exception.
4. Perform one bounded capability and enforce its own row-, token-, queue-, and attempt-level
   guards. Possession of `EXECUTE` is not authority to mutate arbitrary jobs.
5. Never execute DDL, `SET ROLE`, privilege changes, or transaction control. The maintenance
   dispatcher may execute only `LOCK TABLE ONLY ... ACCESS EXCLUSIVE` and
   `TRUNCATE TABLE ONLY ... CONTINUE IDENTITY RESTRICT` against the verified partition set;
   partition creation, attachment, detachment, alteration, and removal stay behind the migrator
   boundary.
6. Return only the documented result. Errors with correctness meaning use stable SQLSTATE and abort
   the caller transaction when ignoring the result would be unsafe.
7. Revoke `PUBLIC` in the same transaction that creates the function, then grant `EXECUTE` by exact
   `regprocedure` signature to the intended capability roles.
8. Have its owner, `prosecdef`, `proconfig`, language, volatility, body hash, ACL, dynamic-relation
   policy, and audit-principal source checked by the catalog audit and `awa doctor`. The audit
   covers the definer's transitively reachable routine and trigger closure, not the root body
   alone: helpers and triggers execute under the entry point's effective privileges.
9. Never use `current_user` as the acting principal in an audit record: a definer rewrites it to the
   execution owner. An audited capability records `session_user`, or accepts an explicit actor value
   whose binding to the authenticated caller is validated by the documented ingress boundary. The
   manifest declares which source is authoritative.

These rules follow PostgreSQL's guidance for safely writing
[`SECURITY DEFINER`](https://www.postgresql.org/docs/current/sql-createfunction.html) functions.
PostgreSQL [trigger execution](https://www.postgresql.org/docs/current/trigger-definition.html)
follows the invoking role unless the trigger function is itself a definer, so trigger
classification is part of the same audit rather than an implicit exception.

### Bounded maintenance partition dispatch

Queue, receipt, claim, and terminal-ring reclamation cannot be implemented as a static SQL function:
the selected child relation varies by ring slot, and the existing protocol takes an
`ACCESS EXCLUSIVE` lock before `TRUNCATE`. Leaving those operations as direct maintenance-role
privileges would preserve the broadest runtime authority that this ADR is intended to remove.

A strict maintenance profile therefore permits one narrowly reviewed relation-dispatch policy,
recorded in the capability manifest as `awa_ring_slot_reclaim_v1`. A function using that policy:

1. accepts only the logical ring identity and an integer slot/generation; it accepts no schema,
   relation, operator, command, or SQL text from the caller;
2. locks the authoritative ring metadata, checks the slot against the configured width and expected
   generation/state, and derives a bounded set of relation families fixed by the function body;
3. resolves each target through `pg_catalog`, then verifies its OID, namespace, owner, partition
   attachment, parent, and manifest-listed relation family before constructing any statement;
4. renders identifiers only from those verified catalog rows, under the fixed trusted `search_path`;
5. applies the documented short transaction-local `lock_timeout` and locks only that verified set
   in the global storage lock order, as `LOCK TABLE ONLY` so a descendant outside the verified set
   is never locked implicitly;
6. after the locks are held, revalidates each target's OID, namespace, owner, partition attachment,
   parent, relation family, and ACL against the catalog before rendering or executing any further
   dynamic statement — the step-3 checks ran unlocked, so a concurrent detach, rename, or swap
   between verification and lock acquisition must abort here (having locked a swapped relation is
   recoverable; truncating one is not);
7. rechecks reclaimability and executes only
   `TRUNCATE TABLE ONLY ... CONTINUE IDENTITY RESTRICT`, so descendants, owned sequences, and
   foreign-key dependants can never be reached beyond the verified OID set. The reclaimability
   proofs, rescue-cursor resets, and rollup-delta appends are static SQL routed through the
   partitioned parents with slot predicates, so partition pruning selects the child without a
   rendered identifier; `LOCK TABLE` and `TRUNCATE` remain the only dynamically rendered
   statements. If implementation evidence shows parent-routed proofs are inadequate, the manifest
   may extend this policy to read-only `SELECT` against the same verified OID set — an explicit
   manifest and body-hash revision, never an implicit widening; and
8. fails closed on a missing, renamed, detached, unexpectedly owned, out-of-range, or excessive
   target at either validation point. It never falls back to a caller-derived name, a wider
   relation scan, `CASCADE`, or `RESTART IDENTITY`.

The execution owner receives `TRUNCATE` only on the manifest-listed ring children, plus the static
table privileges the protocol itself needs: `SELECT` on the partitioned parents for the proofs,
`UPDATE` on the ring-slot metadata rows it resets, and `INSERT` on the rollup-delta ledgers it
appends. The catalog audit recognizes the exception only when the
manifest marker, exact function identity and body hash, relation-family allowlist, owner, and ACL all
match. Any other runtime definer containing dynamic SQL or DDL remains invalid. A deployment that
cannot install this dispatcher may retain an explicitly named trusted-maintenance profile with
narrow direct privileges, but that profile is not the strict no-table-grant profile.

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

The `_compat` suffix is reserved for internal cross-representation or rolling-upgrade shims in new
designs. Today's `insert_job_compat` is a transitional exception because the current normative
stability map already covers it; it remains covered until `insert_job` ships and the declared
migration/deprecation step completes. `delete_job_compat` and other unlisted compatibility helpers
remain internal. `_runtime` is reserved for binary-coupled helpers. Neither suffix appears in a new
public contract.

At any installed schema version, each public definer name has one exact input signature. Awa does
not overload it or add a second variant distinguished only by defaultable parameters: that
complicates exact ACLs and PostgreSQL
[function resolution](https://www.postgresql.org/docs/current/typeconv-func.html). The v1
`insert_job` signature has one final `opts jsonb DEFAULT '{}'::jsonb` parameter as its extension
point. That default belongs to the sole exact signature; it does not authorize another overload.
New optional keys may be added compatibly, unknown keys fail with the contract's stable error rather
than being ignored, and changing an existing key's meaning or making a new key required is a
breaking contract change. Callers use explicit types, and `awa doctor` resolves the one exact
`regprocedure` including its `jsonb` argument.

These are stable APIs, not immutable artifacts. Compatible changes retain the clean name and
signature. A breaking improvement is allowed under ADR-036: it requires a reviewed contract
decision, a changelog entry, an upgrade path, and deprecation where feasible. Schema-backed changes
also follow ADR-041 expand/migrate/contract and prove the supported mixed-version window.

When old and new contracts must coexist, the expand phase may add a temporary, separately granted
name such as `insert_job_v2` or `complete_job_v2`; it must not create an overload of the clean name.
After callers have migrated and the old contract has completed its deprecation window, a declared
breaking release may make the clean domain name canonical for the successor. That change is an
operator-visible contract migration, never an automatic retarget based only on schema version.
Version suffixes are migration tools, not a requirement to preserve every historical contract or
accumulate permanent public names.

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
  role, `PUBLIC EXECUTE` on any Awa routine, an unsafe `search_path`, an unexpected ACL, an audited
  definer that derives its actor from `current_user`, or dynamic SQL without the exact
  `awa_ring_slot_reclaim_v1` manifest declaration and reviewed body hash;
- body-validation fixtures for each prohibited hardening-rule operation: static and dynamic DDL
  outside the exact maintenance `LOCK`/`TRUNCATE` exception, `SET ROLE`, privilege changes
  (`GRANT`, `REVOKE`, ownership/default-privilege changes), and transaction control. Validation
  traverses the transitively reachable routine and trigger closure of each definer entry point
  under its effective execution context — every internal helper a definer calls, and every trigger
  its writes can fire, executes with the owner's privileges, so a prohibited operation in a helper
  or trigger body is a violation of the calling definer, not only of the helper. Catalog tests,
  `awa doctor`, and strict-profile startup must each reject and identify the exact function and
  operation class, so a static statement cannot bypass the dynamic-SQL check and a helper cannot
  bypass root-only validation;
- maintenance-dispatch tests covering valid reclaim, forged and out-of-range slots, stale
  generations, detached/wrong-parent/wrong-owner relations, unexpected manifest targets, bounded
  lock timeout, and concurrent rotation; only the verified child OIDs may be locked or truncated.
  Negative fixtures prove `LOCK TABLE` without `ONLY` and `TRUNCATE` without
  `ONLY ... CONTINUE IDENTITY RESTRICT` are rejected, that an inheritance descendant or
  foreign-key dependant attached to a verified child is never locked or truncated, and that a
  relation detached, renamed, or swapped between unlocked verification and lock acquisition fails
  the post-lock revalidation instead of being truncated;
- role-graph tests on the oldest and newest PostgreSQL majors in the documented support window —
  a pair that must straddle PostgreSQL 16's membership-option change — that compute
  version-correct transitive inherited-privilege and `SET ROLE` closure and reject every
  non-allowlisted runtime, application, callback, or admin path to the execution owner, migrator, or
  schema owner, including paths through multiple intermediate roles and PostgreSQL 16+'s membership
  option semantics;
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
