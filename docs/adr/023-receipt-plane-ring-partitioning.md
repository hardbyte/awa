# ADR-023: Receipt Plane Ring Partitioning

## Status

Accepted

## Context

ADR-019 committed the queue storage engine to a vacuum-aware discipline:
every hot table reclaims space by partition rotation and `TRUNCATE`, not by
row-level `DELETE` or `UPDATE` churn. Queue entries, terminal entries, active
leases, deferred jobs, and DLQ entries all follow that contract. The ADR-019
validation artifact recorded 276 exact dead tuples across the entire schema
after a 1000/s soak — `queue_lanes=19, ready=0, done=255, leases=2,
attempt_state=0` — consistent with the design intent.

The experimental receipt-backed short-job path introduced after that
validation adds three tables:

- `lease_claims` — append-only claim receipts (durable claim history)
- `lease_claim_closures` — append-only closure tombstones
- `open_receipt_claims` — a bounded "currently live receipt-backed attempt"
  frontier, introduced so rescue and queue-count queries would not degrade
  into anti-joins against unbounded claim history

`lease_claims` and `lease_claim_closures` honour the ADR-019 contract:
they are insert-only. `open_receipt_claims` does not. Its design is one
`INSERT` per claim and one `DELETE` per completion, so every completion
produces a dead tuple on that table.

Measurements on the current branch confirm this is the remaining MVCC
source. In `benchmarks/portable/results/custom-20260424T065828Z-227187`
(28-minute clean phase at ~800/s per replica, receipts on), the
`open_receipt_claims` heap held a median of 28,390 dead tuples and a peak
of 93,789 while every other hot table stayed under 100 dead tuples. The
autovacuum floor is `autovacuum_naptime=60s`, which is global, so per-table
thresholds do not move the median: a measured before/after run with
per-table knobs on `open_receipt_claims` left that median unchanged.
Knobs treat the symptom; they cannot remove the `DELETE` from the hot
path.

The short-term posture has been to ship with
`experimental_lease_claim_receipts` off, which reduces
`open_receipt_claims` to zero rows in production. That is not the
destination. The receipt-backed short-job path is the only way to retire
per-claim mutable lease row churn on the common path, and making it the
default is a 0.6 release goal. Delivering that goal without violating
ADR-019 requires bringing `open_receipt_claims` onto the same rotation-and-
prune discipline as the rest of the engine.

## Design goals and non-goals

Guided by the 0.6 priorities from ADR-019:

- Preserve at-least-once delivery. No claim may be lost across any crash,
  restart, or partition-truncation boundary.
- Preserve stale-writer protection by `(job_id, run_lease)`. Completion
  must still lose cleanly against a rescue or cancel on the same attempt.
- Keep the claim hot path at least as fast as the current receipt path,
  not slower. The fix must reduce, not add, per-claim and per-completion
  work.
- Eliminate the remaining MVCC churn source. Steady-state dead tuples on
  the receipt plane should be zero once rotation catches up.
- Finish the vacuum-aware story before 0.6 ships; hold the quality bar
  rather than the timeline.

Non-goals:

- Do not change the heartbeat / deadline / callback-timeout rescue
  contract. Those continue to live on `attempt_state` and `active_leases`.
- Do not change the external API or the `(job_id, run_lease)` stale-writer
  guard.
- Do not introduce any new reservation or pre-start state. The
  `lease-plane-redesign-spike` record shows that direction has been tried
  and rejected repeatedly on cost grounds.

## Decision

Apply ADR-019's rotation-and-prune pattern to the receipt plane. Remove
`open_receipt_claims` as a distinct table.

### Physical layout

1. `lease_claims` becomes `PARTITIONED BY LIST (claim_slot)` with a small
   fixed set of child partitions (`lease_claims_0..N-1`).
2. `lease_claim_closures` becomes `PARTITIONED BY LIST (claim_slot)` with
   matching children. A closure row lives in the same `claim_slot` as its
   originating claim.
3. A new control-plane pair `claim_ring_state` and `claim_ring_slots`
   coordinates rotation, mirroring the existing `lease_ring_state` and
   `lease_ring_slots`.
4. `open_receipt_claims` is deleted. Its indexes and the schema-install
   backfill are dropped.

### Hot path

- Claim: append to the current `lease_claims` child partition. No other
  row is written on the receipt path. The claim result carries
  `claim_slot` through to the worker so the completion path can target the
  matching closure partition.
- Complete: append a closure row to the `lease_claim_closures` child
  partition for the same `claim_slot`, then append the terminal row to
  `done_entries` / `dlq_entries` / `deferred_jobs` as today.
- Neither step performs any `UPDATE` or `DELETE` on the receipt plane.
- Short-job fast path becomes strictly cheaper: one insert at claim
  (previously two) and one insert at completion (previously one insert
  and one delete).

### Open-claim queries

Every read that currently targets `open_receipt_claims` becomes a bounded
scan over the active `lease_claims` child partitions, anti-joined with the
matching `lease_claim_closures` children:

- "Is `(job_id, run_lease)` still open?" — PK lookup into active claim
  partitions, anti-join closures. Used by the completion guard and by
  `load_job` on receipt-backed attempts.
- "Scan stale receipt claims for rescue." — range scan on
  `claimed_at < cutoff` in active claim partitions, anti-join closures.
- "Count in-flight receipt-backed attempts." — count active-partition
  rows minus matching closure rows.

Active partitions are bounded by the claim-ring rotation window, which is
sized so that even worst-case throughput keeps the anti-join surface
smaller than what `open_receipt_claims` used to hold dynamically.

### Rotation and prune

- The maintenance leader owns `claim_ring_state` rotation on a cadence
  chosen to keep the active scan surface bounded. Initial target: rotate
  at the same cadence as the queue ring so claim partitions age out
  roughly in step with the ready / done partitions they reference.
- A claim-slot partition may be truncated only when every claim in it is
  either:
  - represented by a closure row in the corresponding
    `lease_claim_closures` partition, or
  - rescued through the existing receipt-rescue path immediately before
    prune takes `ACCESS EXCLUSIVE` on the partition.
- Prune order mirrors `prune_oldest` and `prune_oldest_leases`:
  1. `FOR UPDATE` on `claim_ring_state`.
  2. `FOR UPDATE` on the target `claim_ring_slots` row.
  3. `SET LOCAL lock_timeout = '50ms'`.
  4. `ACCESS EXCLUSIVE` on both partitions (claims and closures).
  5. Liveness recheck: rescue any still-open claims, then `TRUNCATE`.
- Partition truncation never races with claim because claim always writes
  to the ring's current slot and rotation advances the current slot
  atomically under the same lock order.

### Invariants preserved

- At-least-once delivery: a partition cannot truncate while live claims
  remain in it. Rescue is the gating step, not the prune itself.
- `(job_id, run_lease)` stale-writer protection: the authoritative record
  is still `lease_claims + lease_claim_closures`. Adding partitioning
  changes where those rows live, not what they mean.
- Heartbeat / deadline / callback-timeout rescue: unchanged. Those
  continue to run against `attempt_state` and `active_leases`.

### Migration

This is a breaking schema change even though the external API does not
change.

1. A new migration creates `claim_ring_state`, `claim_ring_slots`, and the
   partitioned `lease_claims` / `lease_claim_closures` shapes.
2. Existing `lease_claims` and `lease_claim_closures` rows are rewritten
   into the current slot of the new partitioned parents.
3. `open_receipt_claims` remains readable through the migration window so
   in-flight attempts are not stranded. Subsequent claim and complete
   operations target only the partitioned tables; reads on rescue and
   counts consult both sources until the window closes.
4. `open_receipt_claims` and its indexes are dropped once no schema
   revision in active use still consults it.

TLA+ coverage (`AwaSegmentedStorage`, `AwaStorageLockOrder`) is extended
to model the claim-ring rotation and the rescue-before-truncate
precondition, parallel to the existing lease-ring model.

## Validation

Success criteria for this redesign, measured on the long-horizon portable
harness used for the ADR-019 baseline:

- `open_receipt_claims` is absent from the schema. Steady-state dead
  tuples across the queue-storage schema return to the ADR-019-validation
  shape: low hundreds, concentrated in ring-state singletons.
- Throughput on the `1x32`, `4x8`, and soak profiles is no worse than the
  current branch and should improve slightly because claim and completion
  each drop a table touch.
- Crash-under-load recovers cleanly. The rescue-before-truncate path is
  exercised by the existing crash-recovery scenarios and by a new test
  that drives rescue concurrently with partition prune.
- `experimental_lease_claim_receipts` flips on by default for 0.6 with no
  dead-tuple regression relative to the ADR-019 validation run.

## Consequences

### Positive

- The 0.6 vacuum-aware story becomes complete: no hot table relies on
  `DELETE` or `UPDATE` for reclamation.
- The short-job hot path drops one `INSERT` at claim and one `DELETE` at
  completion. Per-job database work is strictly reduced.
- Autovacuum tuning on `open_receipt_claims` becomes dead code and is
  removed. The per-table HOT tuning on the small ring-state and head
  tables stays because it addresses a separate per-row UPDATE class that
  this ADR does not change.
- Receipts become the default short-job path for 0.6, retiring the
  per-claim mutable lease row on the common path.

### Negative

- This is a breaking schema migration.
- "Currently open" queries move from a single bounded-frontier lookup to
  a bounded anti-join across a small number of active partitions. Query
  planning needs spot-checking once the partition count is chosen.
- Rescue gains a partition-aware variant and must run before prune takes
  `ACCESS EXCLUSIVE`. The interaction point is small but adds a
  prune-path precondition not present for `ready` / `done`.

## Alternatives Considered

### Per-table autovacuum tuning on `open_receipt_claims`

Rejected. A measured 28-minute soak with aggressive per-table thresholds
and cost knobs left the median dead-tuple count on this table unchanged
relative to the unmodified baseline (`custom-20260424T041700Z-278649`
vs. `custom-20260424T065828Z-227187`). The rate-limiter is
`autovacuum_naptime=60s`, which is global, and the table is already
eligible for vacuum on every wake-up. Per-table knobs improve vacuum
efficiency when it runs but do not change the steady-state floor.

### Documented operational `autovacuum_naptime` reduction

Rejected as the primary fix. Lowering `autovacuum_naptime` globally
improves reclamation cadence but pushes configuration requirements onto
operators and applies to every table in their database. It contradicts
the ADR-019 principle that vacuum-awareness is a property of the schema
design, not of operator tuning.

### Awa-owned periodic VACUUM on `open_receipt_claims`

Rejected. A background `VACUUM` loop would mask the churn but keeps a hot
`DELETE` on the common completion path and leaves the table as a
permanent architectural outlier. The rest of the engine has no equivalent
self-vacuum loop.

### UPDATE-based soft close on `open_receipt_claims`

Rejected. Marking a `closed_at` column and sweeping closed rows
periodically keeps the table live-bounded, and a HOT-eligible `UPDATE`
avoids a new heap tuple. But the sweep still performs `DELETE`s in batch,
index bloat still tracks throughput, and the architectural outlier
persists.

### Ship 0.6 with receipts off

Rejected. Shipping with receipts off lets 0.6 hit the dead-tuple budget
today, but it leaves the short-job path on the mutable `leases` ring and
defers the work tracked in `lease-plane-redesign-spike.md`. ADR-019's
vacuum-aware intent is only satisfied when receipts are on by default and
do not regress the dead-tuple budget. This ADR is the path to that
posture.

## Relationship to Earlier ADRs

- ADR-019 established the vacuum-aware discipline. This ADR applies that
  discipline to the one remaining hot table that did not follow it.
- ADR-013 (run-lease-guarded finalization) is unchanged. The
  authoritative record for `(job_id, run_lease)` staleness moves from a
  bounded mutable frontier to partitioned append-only tables; the
  guarantee does not.
- `lease-plane-redesign-spike.md` identifies `open_receipt_claims` as the
  compromise that unblocked the receipt-backed path. This ADR is the
  follow-through that the spike anticipated.

## Implementation status

### Phase 1 — TLA+ spec ahead of code (complete)

The TLA+ storage model now covers the claim-ring redesign in advance of
any Rust changes. Additions:

- `AwaSegmentedStorage.tla` variables: `claimSegmentOf`, `claimOpen`,
  `claimClosed`, `claimSegments`, `claimSegmentCursor`.
- `AwaSegmentedStorage.tla` actions: `Claim` opens a receipt; every
  attempt-ending transition closes one; `ParkToWaiting` keeps receipts
  open; `RescueStaleReceipt` models Tier-A force-close;
  `RotateClaimSegments` and `PruneClaimSegment(seg)` mirror the
  lease-ring rotation/prune pattern.
- `AwaSegmentedStorage.tla` invariants added: `OneOpenClaimSegment`,
  `ClaimCursorIsOpen`, `PrunedClaimSegmentsAreEmpty`, `NoLostClaim`,
  `ClaimOpenAndClosedDisjoint`, `OpenClaimHasSegment`,
  `ClosedClaimHasSegment`.
- `AwaSegmentedStorageRaces.tla` extended with a claim-segment snapshot
  in `claimIntent`, matching rotate/prune actions, and a
  `PrunedClaimSegmentsAreEmpty` invariant — the race-exposing config
  still trips (naive commit exposes claim/rotate/prune race), and both
  safe-commit configs stay clean.
- `AwaStorageLockOrder.tla` adds `ClaimRingStateResource`,
  `ClaimRingSlotResource`, `ClaimChildResource`,
  `ClosureChildResource`; the claim path's `ClaimPlan` takes a `FOR
  SHARE` on `claim_ring_state` and a `RowExclusive` on the claim child;
  `CompletePlan`, `RotateClaimsPlan`, and `PruneClaimsPlan` are added.
  `NoDeadlock` remains clean at 9,680 distinct states (up from 2,076 —
  reflecting the new transaction kinds and slots).

TLC runs recorded at Phase 1:

- `AwaSegmentedStorage.cfg`: 2.3M distinct states, all invariants pass.
- `AwaSegmentedStorageInterleavings.cfg` (2 workers): 4.7M distinct
  states, all invariants pass.
- `AwaSegmentedStorageTrace.cfg`: snooze trace accepted cleanly.
- `AwaSegmentedStorageTraceBroken.cfg`: broken trace rejected with
  expected deadlock at `traceIdx = 2`.
- `AwaSegmentedStorageRaces.cfg`: race exposed
  (`PrunedLeaseSegmentsAreEmpty` violated; the claim-ring invariant is
  at parallel risk and would trip with a different state-space order).
- `AwaSegmentedStorageRacesSafe.cfg`: safe commit, clean.
- `AwaSegmentedStorageRacesMultiWorker.cfg`: safe commit with 3 workers,
  clean.
- `AwaStorageLockOrder.cfg`: 9,680 distinct states, clean.
- `AwaStorageLockOrderDeadlockDemo.cfg`: still trips `NoDeadlock` in 5
  steps.

All nine runs are wired into `.github/workflows/nightly-chaos.yml` as a
dedicated `tla-storage` job that gates nightly runs and is added to the
failure-notification list. Running locally: `./correctness/run-tlc.sh
storage/<Spec>.tla [<Cfg>.cfg]`.

`correctness/storage/MAPPING.md` records the action → Rust-function
correspondence; claim-ring rows are marked "pending Phase N" until the
matching code lands.

### Phase 2 — Claim-ring control plane (complete)

Additive, data-plane-quiescent infrastructure that gives the runtime the
control surface the Phase 3+ migration will attach data to. At end of
Phase 2 the claim ring exists, rotates, and is scheduled by maintenance,
but nothing is written to it on the hot path yet.

Shipped:

- `QueueStorageConfig::claim_slot_count` (default `8`, minimum `2`).
  Thread through to `awa-bench` via `CLAIM_SLOT_COUNT`.
- `prepare_schema()` adds `{schema}.claim_ring_state` and
  `{schema}.claim_ring_slots` with the same fillfactor and autovacuum
  knobs already applied to the lease-ring control plane, seeds the
  singleton at `(0, 0, claim_slot_count)`, and seeds one open slot plus
  `slot_count - 1` uninitialized slots.
- `reset()` re-seeds the ring and includes the new tables in its
  `TRUNCATE` list so test-cycle resets are clean.
- `QueueStorage::rotate_claims(pool)` and
  `QueueStorage::prune_oldest_claims(pool)` mirror the existing
  `rotate_leases` / `prune_oldest_leases` methods. Phase 2
  `prune_oldest_claims` deliberately returns `PruneOutcome::Noop`
  because the data partitions don't exist yet; Phase 3 swaps in the
  real partition-truncate body.
- Maintenance leader wires `claim_rotate_interval` into the `select!`
  loop and invokes `rotate_queue_storage_claims` alongside the queue
  and lease rotation ticks. Default cadence is `queue_rotate_interval`;
  tests and benches override via
  `ClientBuilder::claim_rotate_interval`.

Validation:

- `cargo fmt --all --check` clean.
- `cargo build --workspace` on `awa-model`, `awa-worker`, `awa`, and
  `awa-testing` passes. (`awa-cli` has a pre-existing compile error
  unrelated to ADR-023.)
- `test_claim_ring_rotates_and_prunes_empty` drives the full Phase 2
  contract: one rotation cycle through every slot (0→1→2→3→0,
  generation 0→4), `prune_oldest_claims` stays a `Noop`, `reset()` and
  `prepare_schema()` stay idempotent.
- Full `cargo test --test queue_storage_runtime_test` — all 42 tests
  pass, confirming the additive posture didn't regress any existing
  lifecycle path.
- TLC unchanged from Phase 1; the data-plane specs still describe the
  invariants the Rust code will take on in Phase 3+.

### Phase 3 — Partition `lease_claims` + `lease_claim_closures`; propagate `claim_slot` (complete)

Both receipt-plane tables are now `PARTITIONED BY LIST (claim_slot)`
with one child per ring slot. Claims and their matching closures
co-locate in the same partition, so Phase 5's prune-and-truncate will
reclaim both children together.

Shipped:

- `lease_claims` and `lease_claim_closures` are partitioned parents
  with `lease_claims_0..N-1` and `lease_claim_closures_0..N-1`
  children.
- PK on both is `(claim_slot, job_id, run_lease)`; a secondary
  non-unique `(job_id, run_lease)` index keeps completion / rescue /
  materialize paths fast when `claim_slot` isn't in hand.
- The existing stale-receipt index is recreated on the partitioned
  parent and cascades to every child.
- `ClaimedEntry::claim_slot: i32` is now populated by the claim CTE,
  which reads `claim_ring_state.current_slot` alongside the lease-ring
  cursor. `claim_slot` rides along through `ClaimedRuntimeJob` to the
  completion path, which writes closures into the matching partition.
- `open_receipt_claims` gains a `claim_slot` column so the
  completion / rescue paths know which closure partition to target.
  (Dropped entirely in Phase 5; Phase 4 already eliminates reads from
  it.)
- Idempotent in-place migration: `prepare_schema()` detects
  pre-Phase-3 regular tables, renames them `_legacy`, creates the
  partitioned parents + children, rewrites rows into the current
  `claim_ring_state.current_slot`, then drops the legacy tables.

Validation:

- `cargo fmt --all --check`, `cargo build --workspace`, `cargo clippy`
  all clean (the two pre-existing clippy warnings unrelated to this
  work are unchanged).
- Three new runtime tests land the partition invariants:
  `test_lease_claim_partition_routing`,
  `test_lease_claim_rotation_isolation`,
  `test_lease_claim_migration_preserves_rows`.
- Full `queue_storage_runtime_test` suite: 45 tests pass (all 42
  pre-existing receipt-mode tests + Phase 2's claim-ring smoke + 3
  Phase 3 additions).
- Runtime inspection confirms the partition shape: `pg_class` shows
  `relkind='p'` on the parents and `relkind='r'` on each numbered
  child, with `lease_claims_N_pkey`,
  `lease_claims_N_materialized_at_claimed_at_job_id_idx`, and
  `lease_claims_N_job_id_run_lease_idx` (plus the closure pair) on
  every child partition.

### Phase 4 — Rewrite the `open_receipt_claims` SQL sites (complete)

After Phase 4 the runtime never reads or writes `open_receipt_claims`
on the hot path. The partitioned `lease_claims` table is the
authoritative record of "currently open"; every query that used to
target `open_receipt_claims` now does a bounded anti-join against
`lease_claim_closures`. The table still exists — dropped in Phase 5 —
but is untouched by the hot path.

Rewritten sites (originally six, one more surfaced under test as the
`load_job` receipt branch — plus two closely related heartbeat/progress
upsert paths):

- `queue_counts_exact` `live_running` CTE: anti-join-based count of
  receipt-backed attempts instead of a `count(*) FROM
  open_receipt_claims` subquery.
- `ensure_running_leases_from_receipts_tx`: `claim_refs` CTE now
  selects from `lease_claims` anti-joined with
  `lease_claim_closures`; the trailing `removed_open` DELETE is
  dropped.
- `upsert_attempt_state_heartbeat_from_receipts_tx` and
  `upsert_attempt_state_progress_from_receipts_tx`: same anti-join
  pattern for the upsert paths that materialize `attempt_state`.
- `close_open_receipt_claim_tx`: `target` CTE sources the open claim
  from `lease_claims` anti-joined with closures, routes the closure
  INSERT to the matching partition via `claim_slot`.
- `rescue_stale_receipt_claims_tx`: `stale_claims` CTE similarly
  rewritten; `removed_open` DELETE dropped.
- `complete_runtime_batch` receipt branch: the `completed` CTE now
  carries `(claim_slot, job_id, run_lease)` triples directly from
  `ClaimedEntry`, so the closure INSERT routes by partition key
  without reading `open_receipt_claims` at all. Strictly cheaper than
  the pre-Phase-4 path.
- `load_job` receipt branch: reports receipt-backed attempts as
  Running by anti-joining `lease_claims` with closures and excluding
  any that already have a materialized lease row.

Phase 4f: the claim CTE no longer emits the `opened AS (INSERT INTO
open_receipt_claims ...)` sibling. The partitioned `lease_claims`
insert is the sole authoritative claim record.

Validation:

- `cargo fmt --all --check`, `cargo build`, `cargo test` clean.
- `test_phase4_no_writes_to_open_receipt_claims` locks in the
  invariant: a full claim + complete cycle on a receipt-backed short
  job leaves `open_receipt_claims` at zero rows throughout.
- Full `queue_storage_runtime_test` suite: 46/46 pass (Phase 3's 45 +
  the new Phase 4 regression test).
- The test helper `open_receipt_claim_count` is now a derived
  anti-join query — all existing assertions that used to count
  `open_receipt_claims` rows continue to pass because they were really
  checking the "currently open receipt" invariant, which now lives in
  `lease_claims` minus `lease_claim_closures`.

### Phase 5 — Delete `open_receipt_claims` (pending)

### Phase 6 — Make receipts the default, land the validation artifact (pending)
