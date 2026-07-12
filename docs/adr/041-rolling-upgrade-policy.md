# ADR-041: Rolling upgrade policy

## Status

Proposed for adoption before 0.7.

The current implementation work is split across the 0.6.2 forward-schema
guard ([#428](https://github.com/hardbyte/awa/pull/428)), migration safety
([#426](https://github.com/hardbyte/awa/pull/426)), and the staged v042 ring
authority change ([#425](https://github.com/hardbyte/awa/pull/425)). The
migration-authoring checklist is in
[`docs/development.md`](../development.md#authoring-schema-migrations), and
automated mixed-version rehearsal is tracked by
[#427](https://github.com/hardbyte/awa/issues/427).

## Context

Awa is infrastructure in the request path of other systems. Its normal release
process therefore needs to support incremental fleet replacement while work
continues. Migration order is not reliably controlled across all deployments:
an operator may migrate before rolling workers, roll workers before migrating,
or have both actions overlap.

The 0.6 to 0.7 work exposed three independent failure classes:

1. A pre-guard 0.6 binary could mistake a newer schema for a legacy schema,
   rewrite `awa.schema_version`, and reapply old migrations. This was a
   destructive fail-open path, not a compatibility limitation
   ([#392](https://github.com/hardbyte/awa/issues/392)).
2. The first v042 design removed columns still used by 0.6 binaries. That made
   migration order part of the correctness contract and prevented a mixed
   fleet.
3. An authority flip introduces a different boundary from schema expansion.
   Even if old and new binaries can share the expanded schema, a returning old
   binary must not write the retired representation after the flip.

The 2026-07-12 production-style rehearsal established useful baseline evidence:
transactional migration under load, hard-kill recovery, scheduled work, and
exact reconciliation across 50,772 jobs. It did **not** run old and new workers
concurrently. It is therefore evidence for migration and recovery behavior,
not proof of the mixed-version contract defined here.

v042 was redesigned around an additive schema, explicit authority, and a
runtime flip. It is the first application of this policy, but not a claim that
every policy requirement was already met. In particular, during a migrate-first
interval with only 0.6.2 workers, crash and heartbeat rescue remain available,
but deadline rescue for newly written compact claim batches resumes only when a
0.7 maintenance runtime starts. That transitional limitation is documented in
the 0.7 upgrade guide and must be covered by the rehearsal in #427. Future
changes must either preserve maintenance semantics in N-1 or document and
approve an explicit policy exception before release.

## Decision

### 1. Rolling is the default release contract

Schema and representation changes are designed for a rolling deployment. A
change that cannot support one uses the exception process in Decision 8; it is
not described as rolling merely because failures are loud.

For a release N expand migration, these orderings must be supported:

- N-1 binaries operating the N schema before the fleet flip;
- N binaries operating the N-1 schema while migration is pending;
- N-1 and N binaries operating concurrently during the rollout; and
- migration running before, during, or after binary replacement.

"Operating" means preserving job lifecycle and maintenance semantics, not only
starting successfully. Claim, complete, retry, cancel, scheduled and cron work,
heartbeat recovery, deadline recovery, pruning, and operator counts are all in
scope when the changed representation affects them.

### 2. Representation changes use expand, flip, and contract

The phases are separate release actions:

- **Expand:** Add the new representation and its compatibility machinery. Keep
  the old representation authoritative for an upgraded database. Seed the new
  representation from the old one. Fresh installs may start on the new
  representation because no earlier binary can have observed that database.
- **Flip:** After the fleet proves capability, reconcile the new representation
  from the old authoritative state, verify equivalence, atomically change
  authority, and install the old-binary fence.
- **Contract:** Remove the retired representation in a later minor release, no
  earlier than the release after the flip is universal. The contract migration
  has its own N-1 analysis; it is not automatically entitled to an exclusive
  window.

Migration does not perform an irreversible runtime flip. Schema version proves
which SQL committed, not which binaries are live.

### 3. Expand migrations may declare a runtime version floor

Sometimes compatibility with the expanded schema first ships in an N-1 patch.
The preferred preflight is then a minimum runtime version, not a refusal of all
live runtimes.

`MIGRATION_RUNTIME_VERSION_FLOORS` maps a migration to the oldest verified
runtime version. Before applying that migration, the built-in migrator:

- considers only fresh heartbeats;
- treats older, missing, or unparseable versions as below the floor;
- names incompatible instances in the refusal;
- holds a table lock that prevents runtime registration or heartbeat writes
  between the check and migration commit; and
- allows an explicit `--allow-live-runtimes` override.

This gate uses `runtime_instances.version`. That value is acceptable here as a
defense-in-depth check over a documented patch prerequisite: the migration is
still additive, and the operator can override the check after independent
verification. It is not sufficient evidence for an irreversible flip.

External migration systems do not execute the Rust preflight. Their release
procedure must enforce the same version floor and race boundary, or enforce the
documented ordering by other means.

### 4. Flips use capability evidence

An irreversible flip requires evidence written by binaries that contain the
new behavior. The current pattern is:

- `runtime_instances.binary_version`, introduced with the capability-aware
  schema;
- `awa.semver_rank()` with fail-closed parsing; and
- a per-feature minimum version function such as
  `awa.ring_authority_min_flip_version()`.

A fresh heartbeat with a NULL, unparseable, or below-minimum capability value
blocks the flip. A stability interval before auto-flip reduces the chance that
a temporarily absent old process is mistaken for a completed rollout. Manual
override is explicit and separate from the migration override.

The v010/v014 `storage_capability` and `transition_role` enums are not a general
capability registry. They encode the 0.5 to 0.6 storage transition and have
dynamic, transition-specific meaning. New representation changes do not extend
that vocabulary.

### 5. The old representation is authoritative until flip

Compatibility shadow writes are not sufficient at the flip boundary. Under a
mixed fleet, the final writer may be an N-1 process that updates only the old
representation.

The flip must therefore:

1. take the same locks used by old-representation writers;
2. read authority only after those locks are held;
3. treat the old representation as source of truth;
4. reconcile the complete new representation through that final state;
5. verify exact equivalence; and
6. change authority and install the fence in the same transaction.

For v042, the locked `current_slot` and `generation` columns are the source of
truth. `awa.flip_ring_authority()` backfills every missing ledger generation
and verifies the ledger cursor before promoting it. This closes both the final
old-writer race and the authority-read TOCTOU found by the lock-order model.

### 6. A flip includes a database-enforced fence

After flip, a returning pre-flip binary must fail before it can misroute or
prune work. Application logging alone is not a fence.

The fence must cover the actual old write paths and be verified with the real
N-1 artifact. For v042, setting only `current_slot = -1` was insufficient: an
old rotator could compute `-1 -> 0`. The implemented fence poisons both cursor
fields and legacy prune metadata, then uses a database trigger to reject an
old-style cursor advance after ledger authority is active.

Rollback to N-1 is supported before flip. After flip, pre-flip binaries are
deliberately rejected; rollback remains within flip-aware releases.

### 7. Version and migration handling fail closed

- A binary that observes `MAX(schema_version) > CURRENT_VERSION` refuses before
  legacy detection, normalization, or any schema write.
- Unknown capability and version values do not satisfy gates.
- Built-in migrations are serialized and transactional, so other sessions see
  the schema before or after the migration set, not an intermediate committed
  version. External runners that commit each version separately must account
  for intermediate-version refusal.
- Overrides are named operator actions and are never defaults.

### 8. Exclusive migrations are an exception

`EXCLUSIVE_WINDOW_MIGRATIONS` remains available for a migration with no
practical rolling-compatible shape. Adding an entry requires:

- an ADR or equivalent design review explaining why expand/flip/contract and a
  runtime version floor are insufficient;
- justification in the migration header;
- a CHANGELOG callout and release-specific operator procedure; and
- tests for refusal, override, and stale-heartbeat behavior.

A release should not contain an exclusive migration merely to simplify
implementation. Contract migrations must first be evaluated against the normal
N-1 contract and capability fences; they are not presumed exclusive.

### 9. Validation precedes release

Any migration that changes a hot-path structure or stored representation must
pass a mixed-version rehearsal using released N-1 artifacts and the current
build. At minimum it covers:

- live enqueue and a banked backlog throughout migration;
- N-1-only operation on the expanded schema;
- old and new workers claiming concurrently;
- failing and retrying jobs, scheduled jobs, and cron fires;
- in-flight jobs held across migration;
- hard kills with crash and heartbeat rescue asserted;
- deadline-overrun rescue when receipt representations change;
- migrate-first, binary-first, and overlapping orderings;
- the flip, old-binary fence, and supported rollback boundary; and
- exact reconciliation: every accepted job and scheduled fire accounted for
  once, with nothing stuck.

When a state machine or lock order changes, TLA+ models cover mixed-version
interleavings. Tests must exercise real old artifacts because source-level
compatibility helpers cannot prove frozen binary behavior.

Evidence is recorded with the release or in the benchmarking repository. #427
tracks automation of this matrix.

## Consequences

- Operators normally roll Awa minor releases without draining the whole fleet.
  An exceptional release may still require a documented exclusive procedure.
- Representation changes span at least two releases and carry compatibility
  code, dual representations, and fence state until contract.
- Rollback has a precise boundary: N-1 before flip, flip-aware binaries after
  flip.
- Migration authors must prove maintenance-path parity, not only foreground
  job operations.
- Releases pay for mixed-version tests and, where relevant, larger model state
  spaces. #427 should make that cost repeatable rather than optional.

## Alternatives considered

- **Always require an exclusive window for representation changes.** Simpler
  for implementation, but makes deployment ordering a correctness dependency
  and does not address fail-open old binaries.
- **Use schema version as flip evidence.** Rejected because migration and fleet
  replacement are independent events.
- **Use `runtime_instances.version` for flips.** Rejected as capability proof;
  retained only for overridable expand-migration floors.
- **Reuse storage transition enums.** Rejected because their meanings are tied
  to one state machine.
- **Leave mixed-version validation manual and informal.** Rejected because the
  2026-07-12 rehearsal did not cover concurrent versions, exactly the behavior
  this policy needs to establish.

## Relationship to other ADRs

This ADR generalizes ADR-040's authority design, strengthens ADR-036's public
binary/schema skew contract, and retains ADR-037's refusal-first migration
discipline without reusing its transition-specific state. It constrains future
storage ADRs and migration reviews.
