# AwaSegmentedStorage — Rust correspondence

This doc pins each TLA+ action in `AwaSegmentedStorage.tla` to the Rust
code and SQL that implements it. It is intended as a mechanical cross-check
as names and internals evolve.

File references are at the time of writing (2026-04-21). This table maps the
logical storage names used in ADR-019 onto the current Rust / SQL
implementation.

## Variable mapping

| TLA+ variable | Rust / SQL equivalent |
|---|---|
| `readyEntries` | `{schema}.ready_entries` parent partitioned table |
| `deferredEntries` | `{schema}.deferred_entries` |
| `waitingEntries` | `{schema}.waiting_entries` |
| `terminalEntries` | `{schema}.done_entries` (ADR-019 target name: `terminal_entries`) |
| `dlqEntries` | `{schema}.dlq_entries` |
| `activeLeases` | `{schema}.leases` (ADR-019: `active_leases`) |
| `attemptState` | `{schema}.attempt_state` |
| `runLease[j]` | `run_lease` column on the lease/ready/deferred row |
| `taskLease[w][j]` | `ctx.job.run_lease` snapshot captured at claim time in `awa-worker/src/executor.rs` |
| `heartbeatFresh` | `heartbeat_at` on the lease row + the maintenance cutoff (see `rescue_stale_heartbeats` in `queue_storage.rs:5528`) |
| `laneState.appendSeq` / `claimSeq` | `{schema}.queue_enqueue_heads.next_seq` / `{schema}.queue_claim_heads.claim_seq` |
| `readySegmentCursor` etc. | `{schema}.queue_ring_state.current_slot` / `lease_ring_state.current_slot` |
| `readySegments[seg]` state | partition presence + contents (`open` ≈ current write target, `sealed` ≈ rotated out but not pruned, `pruned` ≈ TRUNCATEd) |
| `claimSegmentOf[j]` | the `claim_slot` column on the job's `{schema}.lease_claims` row (ADR-023); closure rows in `{schema}.lease_claim_closures` share the same `claim_slot`. Pending Phase 2+ implementation. |
| `claimOpen` | set of `(job_id, run_lease)` pairs with a claim row but no matching closure row in the current claim-ring partitions. Derived at query time; pending Phase 4 cutover. |
| `claimClosed` | set of `(job_id, run_lease)` pairs with a matching closure row in the current claim-ring partitions. Pending Phase 4. |
| `claimSegments[seg]` state | same semantics as other segment families; `{schema}.claim_ring_state.current_slot` identifies the open partition, `{schema}.claim_ring_slots(slot)` tracks per-partition generation. Seeded in `prepare_schema`; rotated by `rotate_claims`. |
| `claimSegmentCursor` | `{schema}.claim_ring_state.current_slot`. |

The TLA+ model does not represent the cold completed-history rollup cache.
Rust currently stores that in `{schema}.queue_terminal_rollups`, with
`queue_lanes.pruned_completed_count` kept only as a transitional legacy source
for backfill / fallback reads during upgrades.

## Action mapping

| TLA+ action | Rust function | SQL / DDL |
|---|---|---|
| `EnqueueReady(j)` | `QueueStorage::insert_ready` / producer insert path | `INSERT INTO {schema}.ready_entries ... UPSERT {schema}.queue_enqueue_heads` (single tx) |
| `EnqueueDeferred(j)` | `QueueStorage::insert_deferred` | `INSERT INTO {schema}.deferred_entries ...` |
| `PromoteDeferred(j)` | maintenance promote loop in `awa-worker/src/maintenance.rs::promote_due_deferred_jobs` | `DELETE FROM deferred_entries ... INSERT INTO ready_entries ...` in one tx |
| `AdvanceClaimCursor` | claim path gap-skipping after rescue/prune holes | inside `claim_ready_runtime` PL/pgSQL function; logical `UPDATE queue_claim_heads SET claim_seq = claim_seq + 1 WHERE no row at claim_seq` |
| `Claim(w, j)` | `QueueStorage::claim_runtime_batch` → dispatcher (`awa-worker/src/dispatcher.rs`) | `claim_ready_runtime(...)` server-side fn: lane selection reads `queue_lanes`, serialization uses `FOR UPDATE OF queue_claim_heads SKIP LOCKED`, then inserts receipt/lease rows and advances `queue_claim_heads` |
| `MaterializeAttemptState(j)` | `QueueStorage::upsert_attempt_state` on first progress / callback / long-path transition | `INSERT INTO attempt_state ... ON CONFLICT (job_id, run_lease) DO NOTHING` |
| `Heartbeat(j)` | `heartbeat_tick` in `awa-worker/src/heartbeat.rs` | `UPDATE leases SET heartbeat_at = now() WHERE job_id = $1 AND run_lease = $2` |
| `LoseHeartbeat(j)` | implicit — time passes without a heartbeat UPDATE; maintenance rescue sees a stale cutoff | (no action in real code; represents age) |
| `ProgressFlush(j)` | `QueueStorage::flush_progress` (`queue_storage.rs:4746`) | `UPDATE attempt_state SET progress = ... WHERE job_id = $1 AND run_lease = $2` guarded by running/waiting_external state |
| `ParkToWaiting(w, j)` | `QueueStorage::wait_external` from executor `WaitForCallback` | delete from leases, insert into waiting_entries, preserve attempt_state |
| `ResumeWaitingToReady(j)` | `QueueStorage::resume_external` on callback success | `DELETE FROM waiting_entries`, `INSERT INTO ready_entries`, clear attempt_state |
| `TimeoutWaitingToReady(j)` | maintenance callback rescue with attempts remaining (`awa-worker/src/maintenance.rs::rescue_expired_callbacks`) | rescue SQL with attempt increment |
| `TimeoutWaitingToDlq(j)` | maintenance callback rescue with exhausted attempts | rescue SQL routes to `dlq_entries` instead of re-enqueue |
| `FastComplete(w, j)` | `QueueStorage::complete_runtime_batch` short path (no attempt_state hydrate) | `DELETE FROM leases`, `INSERT INTO done_entries` carrying the claim-time snapshot; no `ready_entries` read |
| `StatefulComplete(w, j)` | `QueueStorage::complete_runtime_batch` + attempt_state delete | same as above plus `DELETE FROM attempt_state` |
| `FailToDlq(w, j)` | `QueueStorage::fail_to_dlq_terminal` via executor terminal failure path | `DELETE FROM leases`, `DELETE FROM attempt_state`, `INSERT INTO dlq_entries` in one tx |
| `RetryToDeferred(w, j)` | `QueueStorage::retry_runtime` on `JobError::RetryAfter` / `Snooze` | `DELETE FROM leases`, `INSERT INTO deferred_entries` |
| `RescueToReady(j)` | `rescue_stale_heartbeats` / `rescue_expired_deadlines` in maintenance (`queue_storage.rs:5528, 5611`) | `DELETE FROM leases ... RETURNING ...; INSERT INTO ready_entries ...` |
| `CancelWaitingToTerminal(j)` | admin cancel path in `awa-model/src/admin.rs` for a waiting job | `DELETE FROM waiting_entries`, `INSERT INTO done_entries` with cancel reason |
| `StaleCompleteRejected(w, j)` | `complete_runtime_batch` returning `CompletionOutcome::IgnoredStale` | `UPDATE leases ... WHERE run_lease = $2` matching 0 rows |
| `MoveFailedToDlq(j)` | `QueueStorage::move_failed_to_dlq` (admin) | `DELETE FROM done_entries ... INSERT INTO dlq_entries ...` guarded by state=failed |
| `RetryFromDlq(j)` | `QueueStorage::retry_from_dlq` (`queue_storage.rs:5509` region) | CTE: `DELETE FROM dlq_entries RETURNING ...` + `INSERT INTO ready_entries ...` with `run_lease = 0`; unique-conflict handled by `sync_unique_claim` |
| `PurgeDlq(j)` | `QueueStorage::purge_dlq_job` / bulk `purge_dlq` | `DELETE FROM dlq_entries WHERE ...` |
| `RotateReadySegments` | maintenance `rotate_ready` (`awa-worker/src/maintenance.rs`) | `UPDATE queue_ring_state SET current_slot = next` + partition attach/detach |
| `RotateDeferredSegments` / `RotateWaitingSegments` / `RotateLeaseSegments` / `RotateDlqSegments` | parallel maintenance rotate functions per family | analogous `UPDATE *_ring_state` |
| `PruneReadySegment(seg)` | maintenance `prune_oldest` for the ready family (`queue_storage.rs:5994`) | `SELECT ... WHERE slot = $1` check, then `TRUNCATE {schema}.ready_segment_N` in a separate tx; Rust also updates `{schema}.queue_terminal_rollups` after a successful terminal-segment prune |
| `PruneDeferredSegment` / `PruneWaitingSegment` / `PruneLeaseSegment` / `PruneDlqSegment` | parallel prune paths per family | `TRUNCATE {schema}.X_segment_N` with active-row check |
| `RotateClaimSegments` | maintenance `QueueStorage::rotate_claims` (`awa-model/src/queue_storage.rs`) wired via `Maintenance::rotate_queue_storage_claims` at the `claim_rotate_interval` tick | `UPDATE claim_ring_state SET current_slot = next, generation = next_gen` with compare-and-swap on `(current_slot, generation)` |
| `PruneClaimSegment(seg)` | maintenance `QueueStorage::prune_oldest_claims` (Phase 2: returns `Noop` because the partitions don't exist yet; Phase 3+ swaps in the real partition-truncate body) | Phase 3 target: `FOR UPDATE` on `claim_ring_state`, `FOR UPDATE` on `claim_ring_slots[slot]`, `ACCESS EXCLUSIVE` on both `{schema}.lease_claims_N` and `{schema}.lease_claim_closures_N`, rescue-before-truncate for any still-open claim in the partition, then `TRUNCATE` both children |
| `RescueStaleReceipt(j)` | receipt rescue scan integrated with `prune_oldest_claims` (pending Phase 3+) | anti-join `lease_claims` against `lease_claim_closures` over the active partitions; close stragglers by appending to `lease_claim_closures` |

## Invariant mapping

| TLA+ invariant | Rust enforcement |
|---|---|
| `ActiveLeasesSubsetReadyEntries` | every `leases` row FK-references `ready_entries(queue, priority, lane_seq)` (check the CREATE TABLE DDL in the `install` fn) |
| `WaitingHasNoLiveLease` | `wait_external` path deletes the lease before inserting into waiting_entries |
| `AttemptStateRequiresLeaseOrWaiting` | `attempt_state` is only upserted inside `upsert_attempt_state` which asserts the lease/waiting row exists |
| `FreshHeartbeatRequiresLease` | `heartbeat_at` is a column on `leases`; once the lease row is deleted (retry/complete/rescue/park), the heartbeat is gone too |
| `TerminalHasNoLiveRuntime` | `complete_runtime_batch` / `fail_to_dlq` clear every other family in the same tx before inserting terminal/dlq |
| `DlqHasNoLiveRuntime` | same, for dlq path |
| `DlqAndTerminalDisjoint` | `move_failed_to_dlq` uses `DELETE FROM done_entries ... RETURNING` then `INSERT INTO dlq_entries` in one tx; no intermediate state where both hold the same job_id |
| `StaleCompleteRejected` precondition | `WHERE run_lease = $2 AND state = 'running'` clauses on every completion UPDATE |
| `ReadyLaneSeqUnique` | `UNIQUE(queue, priority, lane_seq)` on `ready_entries` child partitions |
| `ClaimCursorBounded` | `queue_lanes.claim_seq <= queue_lanes.append_seq` should be a CHECK constraint (currently implicit; worth adding) |
| `PrunedXSegmentsAreEmpty` | per-family prune requires no live-row precondition before TRUNCATE |
| `PrunedClaimSegmentsAreEmpty` (ADR-023) | `prune_oldest_claims` requires no open claim in the partition before TRUNCATE; rescue-before-truncate closes stragglers in the same transaction |
| `NoLostClaim` (ADR-023) | receipts and their closures both live in `claim_slot`-partitioned tables; partitions only truncate once all their receipts are closed, so no open claim is physically dropped |
| `ClaimOpenAndClosedDisjoint` (ADR-023) | closure insertion and receipt-clearing are a single transaction; a partition's receipt+closure pair is either both present or both dropped by `TRUNCATE` |
| `LaneStateConsistent` | live availability is derived from `{schema}.ready_entries` plus `{schema}.queue_claim_heads`; completed totals are *not* maintained as hot counters. Rust derives them from live `done_entries` plus the cold `{schema}.queue_terminal_rollups` cache, with `queue_lanes.pruned_completed_count` read only as a transitional legacy fallback |

## Local runtime note

The TLA+ storage model does not represent local worker-capacity accounting.
Rust now releases local queue capacity immediately after handler execution and
progress snapshotting, while durable completion continues asynchronously
through the completion batcher. That changes throughput and scheduling
behavior, but it does not change the modeled storage safety boundary because
the `run_lease`-guarded finalization and rescue semantics are unchanged.

## Known modelling gaps with implementation implications

### Claim vs Rotate race — resolved by checked commit on lease rotation state

The race-exposure spec
[`AwaSegmentedStorageRaces.tla`](./AwaSegmentedStorageRaces.tla) proves
that a claim that snapshots the lease segment cursor without further
synchronisation can land a lease in a segment that has since been
rotated and pruned.

**Status in the implementation: mitigated.** The current Rust code no
longer takes `FOR SHARE` on `lease_ring_state`. Instead:

- claim reads the current lease slot / generation from `lease_ring_state`
  inside the claim statement and writes that generation into the claim
- `rotate_leases` advances `lease_ring_state` with a compare-and-swap update
  on `(current_slot, generation)`
- `prune_oldest_leases` derives the oldest initialized slot from
  `lease_ring_state`, locks the child partition, then rechecks that the slot
  is not current before truncating

So the race still exists at the abstract spec level, but the production
implementation closes it by treating `lease_ring_state` as a checked-commit
cursor rather than an unlocked hint. The race spec remains valuable because
it proves that weakening that discipline would reintroduce the bug.

### prune_oldest (ready) check-then-act — resolved

The spec's PruneLeaseSegment transition also captures the analogous
concern on `prune_oldest` (for ready partitions) at
`queue_storage.rs:6252`.

**Status in the implementation: mitigated.** The prune path:

1. `FOR UPDATE` on `queue_ring_state` to serialise against concurrent
   rotates (`queue_storage.rs:6265`)
2. `FOR UPDATE` on the target `queue_ring_slots` row
   (`queue_storage.rs:6280`)
3. `LOCK TABLE ... IN ACCESS EXCLUSIVE MODE` on the ready and done
   partition children (`queue_storage.rs:6297`) — this blocks the
   AccessShare lock that `claim_ready_runtime` takes when reading
   `{schema}.ready_entries_%s`, forcing prune to wait for in-flight
   claims to commit (or bail via the 50 ms `lock_timeout`)
4. Only AFTER the lock is held does the count-active-leases check
   run inside the same transaction — so any lease inserted by a
   concurrent claim will be visible to the check

All prune paths set `SET LOCAL lock_timeout = '50ms'` so they abort
gracefully under contention rather than stalling.

So the "check-then-act" framing is inaccurate: the Rust code is
"lock-then-check-then-act", with the lock being the load-bearing part.

### Role of the race spec going forward

The spec plus `AwaSegmentedStorageRaces.cfg` (race-exposing) and
`AwaSegmentedStorageRacesSafe.cfg` (checked-commit) is a regression
harness. If any future refactor weakens the checked-commit discipline on
`lease_ring_state`, or weakens the `ACCESS EXCLUSIVE` on the partition
children, the race spec will still produce a counterexample and the safe
spec will still pass — making the invariant the checked-commit enforces a
clear statement of what the SQL coordination is buying.

### Lock-order regression harness

`AwaStorageLockOrder.tla` (see [`README.md`](./README.md)) is the
complementary positive artifact: it models the Postgres locks
directly and checks that no interleaving of claim / rotate-leases /
prune-leases / rotate-ready / prune-ready transactions produces a
waits-for cycle. Current result: 2,076 distinct states, no
deadlock. A deliberately-broken demo config
(`AwaStorageLockOrderDeadlockDemo.cfg`) confirms the deadlock
detector fires when a cycle exists.

Together the two specs cover complementary risks:
- `AwaSegmentedStorageRaces` catches data-level races that would
  occur if the locks were removed — proves the locks are necessary
- `AwaStorageLockOrder` catches deadlock-order bugs that would
  occur if the lock ordering were changed — proves the current
  ordering is safe

## Trace validation

`AwaSegmentedStorageTrace.tla` takes a hand-transcribed sequence of
events from a queue-storage runtime test and verifies each transition
is a legal firing of the corresponding base spec action. It is a
single-threaded replay harness — one step at a time, no exploration
of interleavings — but it catches:

- **transcription errors**: if the transcribed sequence does not
  correspond to any valid base spec behaviour, TLC reports deadlock
  at the first failing step, and the traceIdx variable names the
  event that could not fire
- **spec regressions**: if a future edit to the base spec tightens a
  precondition, an existing trace that used to pass will now fail;
  TLC reports deadlock at the newly-rejected step
- **inherited invariant regressions**: every safety invariant from
  AwaSegmentedStorage is checked at every step of the replay, so a
  trace that sneaks through an invalid intermediate state is caught

### Transcribing a new trace

Pick a test in `awa/tests/queue_storage_runtime_test.rs` whose
lifecycle is clear. Typical shape: one enqueue, one or two claims,
a terminal transition (complete / fail-to-dlq / cancel / etc.),
optionally a retry-from-deferred or retry-from-dlq round trip.

1. Read the test and its custom Worker impl. Work out the sequence of
   **logical** transitions the test exercises — not the individual
   SQL statements. The correspondence table above maps test-level
   concepts (snooze, terminal failure, callback timeout) to base
   spec actions.
2. Write the sequence as a `<<...>>` tuple of event records in the
   TLA file. Each event has an `action` field (the action name as a
   string) and the arguments that action takes: `job` for most
   events, plus `worker` for events that take `(w, j)`. See the
   `SnoozeTrace` and `BrokenTrace` operators for shape.
3. Add a specification in the TLA file:
   `SpecYourTrace == TraceInit /\ [][TraceNextFor(YourTrace)]_<<vars, traceIdx>>`.
4. Add a negative-witness invariant:
   `YourTraceIncomplete == traceIdx < Len(YourTrace)`.
5. Add a config file (e.g. `AwaSegmentedStorageTraceYours.cfg`) with
   `SPECIFICATION SpecYourTrace` and `INVARIANTS ... YourTraceIncomplete`.
6. Run with `./correctness/run-tlc.sh storage/AwaSegmentedStorageTrace.tla storage/AwaSegmentedStorageTraceYours.cfg`.
   Expected outcome for a valid trace: `Invariant YourTraceIncomplete
   is violated` (the positive witness that the trace was fully consumed).

### What the checker does not catch

- **Races that require concurrent transactions.** The trace replay is
  single-threaded. If a test's behaviour depends on a
  rotate-mid-claim interleaving, the trace spec won't exercise that
  path — use `AwaSegmentedStorageRaces` for race concerns.
- **Timing-dependent maintenance steps.** The sample traces omit
  heartbeat and rotate/prune events because they are noise the tests
  tolerate. If a test's correctness DEPENDS on a specific
  rotate-then-claim ordering, transcribe those events in too.
- **Events outside the transcribed set.** If a test fires an action
  the harness doesn't know about (e.g. a future `RetryFromDeferred`
  variant we haven't modelled), extend the disjunction in
  `TraceStep` to include it.

### Current traces

- `SnoozeTrace`: 6 events — EnqueueReady → Claim → RetryToDeferred →
  PromoteDeferred → Claim → FastComplete. Accepts cleanly with 7
  states (1 init + 6 steps). Transcribed from
  `test_queue_storage_runtime_snooze`.
- `BrokenTrace`: same 6 events but with steps 3 and 4 swapped so
  PromoteDeferred fires before RetryToDeferred. TLC reports deadlock
  at traceIdx = 2 (after EnqueueReady + Claim, before the
  out-of-order PromoteDeferred). Confirms the checker rejects invalid
  traces.

### Bulk ops atomicity

`bulk_retry_from_dlq` / `purge_dlq` / `bulk_move_failed_to_dlq` run as
single transactions in the Rust code. The spec models them as independent
`\E j \in Jobs : RetryFromDlq(j)` firings. This is a strictly weaker
claim (safety invariants hold under any interleaving, including the ones
a real tx would prevent).

If a bulk-level invariant becomes interesting — e.g., "a retry-bulk that
sees a unique conflict on any row leaves all rows intact" — add a
`bulkScope: SUBSET Jobs` variable and express the op as a single atomic
action over that set.

### Heartbeat time abstraction

`heartbeatFresh` is a set (fresh or not). Real heartbeats are timestamps
with a maintenance cutoff. The spec's `LoseHeartbeat(j)` is enabled any
time the lease exists — it doesn't model "the cutoff moved". For the
safety invariants this is fine; the abstraction is conservative. A
liveness-oriented refinement would need an explicit time variable.

### Unique-claim keys

The Rust `retry_from_dlq` contract says: if a replacement owns the
unique-claim slot, the retry returns `UniqueConflict` and leaves the DLQ
row intact (tested in `awa/tests/queue_storage_runtime_test.rs::
test_queue_storage_retry_from_dlq_surfaces_unique_conflict`). The spec
has no unique keys, so it simply allows `RetryFromDlq(j)` whenever
`j \in dlqEntries`. A refinement adding `uniqueKey: Jobs -> UniqueKeys`
and a `uniqueClaim: UniqueKeys -> Jobs \cup {NoJob}` variable could check
the invariant directly.

## ADR-023 implementation status (Phase 3: partitioned receipt plane)

The TLA+ specs now cover the ADR-023 claim-ring redesign ahead of the
Rust implementation landing. In both the base spec and the race / lock
specs:

- `claimSegmentOf`, `claimOpen`, `claimClosed`, `claimSegments`,
  `claimSegmentCursor` track the receipt plane parallel to the existing
  lease plane.
- `Claim` now appends a receipt into the current claim segment.
  Attempt-ending transitions (`FastComplete`, `StatefulComplete`,
  `FailToDlq`, `RetryToDeferred`, `RescueToReady`,
  `CancelWaitingToTerminal`, `TimeoutWaitingToDlq`,
  `TimeoutWaitingToReady`, `ResumeWaitingToReady`) append a closure row
  in the same partition.
- `ParkToWaiting` does NOT close the receipt — the attempt is still
  alive in callback wait; resume/timeout/cancel close it.
- `RescueStaleReceipt(j)` models Tier-A receipt rescue: force-close a
  straggler receipt whose attempt is no longer on the ready / leased /
  waiting lifecycle. This is the rescue-before-truncate precondition
  that `prune_oldest_claims` will invoke.
- `RotateClaimSegments` and `PruneClaimSegment(seg)` parallel the
  lease-ring rotation/prune pattern.
- `AwaStorageLockOrder` adds `ClaimRingStateResource`,
  `ClaimRingSlotResource`, `ClaimChildResource`, `ClosureChildResource`;
  `ClaimPlan` now includes `claim_ring_state FOR SHARE` and
  `RowExclusive` on the claim child; `CompletePlan`, `RotateClaimsPlan`,
  and `PruneClaimsPlan` are added.
- `AwaSegmentedStorageRaces` adds `claimSeg` to the claim-intent
  snapshot and exposes the claim-ring version of the naive commit race.

Invariants added:

- `OneOpenClaimSegment`, `ClaimCursorIsOpen`,
  `PrunedClaimSegmentsAreEmpty` (every segment family shape).
- `NoLostClaim`: every open receipt's segment is not pruned.
- `ClaimOpenAndClosedDisjoint`, `OpenClaimHasSegment`,
  `ClosedClaimHasSegment`: the receipt-lifecycle bookkeeping is sound.

Model checking results as of Phase 1:

- `AwaSegmentedStorage.cfg`: 2.3M distinct states, clean.
- `AwaSegmentedStorageInterleavings.cfg` (2 workers): 4.7M distinct
  states, clean.
- `AwaSegmentedStorageTrace.cfg`: snooze trace accepted cleanly.
- `AwaSegmentedStorageTraceBroken.cfg`: broken trace rejected with the
  expected deadlock at traceIdx = 2.
- `AwaSegmentedStorageRaces.cfg`: race exposed
  (`PrunedLeaseSegmentsAreEmpty` violated — the naive commit lets a row
  land in a pruned segment). Claim-ring race has the same shape and
  would trip `PrunedClaimSegmentsAreEmpty` if the state-space search
  hit it first.
- `AwaSegmentedStorageRacesSafe.cfg`: safe commit, clean.
- `AwaSegmentedStorageRacesMultiWorker.cfg`: safe commit with 3 workers,
  clean.
- `AwaStorageLockOrder.cfg`: 9,680 distinct states, clean (up from
  2,076 pre-ADR-023 because claim-ring transaction kinds are added).
- `AwaStorageLockOrderDeadlockDemo.cfg`: still trips `NoDeadlock` in 5
  steps, confirming the detector works.

Rust function columns in the action/variable tables above are marked
"pending Phase N" until the corresponding code lands.

### Phase 3 — partitioned lease_claims + lease_claim_closures (landed)

ADR-023 Phase 3 converts both receipt-plane tables to `PARTITIONED BY
LIST (claim_slot)` with one child per ring slot. Claims and their
closures co-locate in the same partition, so Phase 5 prune can truncate
both children in lock-step.

- `lease_claims` and `lease_claim_closures` are partitioned parents
  (`relkind = 'p'`); `lease_claims_0..N-1` and
  `lease_claim_closures_0..N-1` are the children.
- PK on both is `(claim_slot, job_id, run_lease)` to satisfy the
  "partition key must be in the PK" Postgres rule; a secondary
  non-unique index on `(job_id, run_lease)` keeps completion / rescue /
  materialize paths that don't carry `claim_slot` efficient.
- The existing `idx_..._lease_claims_stale (materialized_at,
  claimed_at, job_id)` index is recreated on the partitioned parent
  and propagates to every child.
- `ClaimedEntry` gains `claim_slot: i32`; the claim CTE reads
  `claim_ring_state.current_slot` alongside the lease-ring cursor and
  `RETURNS`  it; `open_receipt_claims` gains a `claim_slot` column so
  close / rescue paths know which partition to write closures into.
- In-place migration: `prepare_schema()` detects pre-Phase-3 regular
  tables, renames them `_legacy`, creates the partitioned parents and
  children, rewrites rows into the current `claim_ring_state.current_slot`,
  then drops the legacy tables. Idempotent on re-run.
- Three runtime tests lock this in:
  `test_lease_claim_partition_routing` (claim + closure both land in
  the current ring slot), `test_lease_claim_rotation_isolation`
  (post-rotation claims land in a different partition; existing rows
  are not moved), `test_lease_claim_migration_preserves_rows` (legacy
  data migrates cleanly, `prepare_schema` stays idempotent after).
- Full `queue_storage_runtime_test` suite: all 45 tests pass (42
  pre-existing receipt-mode tests + 3 Phase 3 additions).

### Phase 2 — claim-ring control plane (landed)

The claim-ring control plane is live in the Rust runtime as additive,
data-plane-quiescent infrastructure:

- `QueueStorageConfig::claim_slot_count` (default `8`, minimum `2`).
- `prepare_schema()` creates `{schema}.claim_ring_state` and
  `{schema}.claim_ring_slots` with the same fillfactor and autovacuum
  knobs the ADR-023 small ring-state tables already use, seeds the
  singleton row at `(0, 0, claim_slot_count)`, and seeds one open slot
  plus `slot_count - 1` uninitialized slots. `reset()` re-seeds the
  same shape.
- `QueueStorage::rotate_claims(pool)` and
  `QueueStorage::prune_oldest_claims(pool)` mirror `rotate_leases` and
  `prune_oldest_leases`. Phase 2 `prune_oldest_claims` returns
  `PruneOutcome::Noop` because the data partitions don't exist yet;
  Phase 3 adds the partition-truncate body.
- The maintenance leader schedules the claim-ring tick alongside the
  queue and lease rings. `claim_rotate_interval` defaults to
  `queue_rotate_interval`; tests and benches can override via
  `ClientBuilder::claim_rotate_interval`.

Unit coverage: `test_claim_ring_rotates_and_prunes_empty` in
`awa/tests/queue_storage_runtime_test.rs` drives the claim ring through
one full cycle (current_slot 0→1→2→3→0, generation 0→4), asserts
`prune_oldest_claims` stays a `Noop`, and verifies `reset()` +
`prepare_schema()` idempotency.

The full `queue_storage_runtime_test` suite (42 tests) passes with the
Phase 2 changes in place, confirming the additive-only posture didn't
regress any existing lifecycle path.
