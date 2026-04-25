# AwaSegmentedStorage — Rust correspondence

This doc pins each TLA+ action in `AwaSegmentedStorage.tla` to the Rust
code and SQL that implements it. It is intended as a mechanical cross-check
as names and internals evolve.

Line numbers in this doc refer to `awa-model/src/queue_storage.rs`
unless stated otherwise. They drift under active development; treat
them as a hint and re-grep for the function name if the line is wrong.
This table maps the logical storage names used in ADR-019 / ADR-023
onto the current Rust / SQL implementation.

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
| `heartbeatFresh` | `heartbeat_at` on the lease row + the maintenance cutoff (see `rescue_stale_heartbeats` in `queue_storage.rs:8575`) |
| `laneState.appendSeq` / `claimSeq` | `{schema}.queue_enqueue_heads.next_seq` / `{schema}.queue_claim_heads.claim_seq` |
| `readySegmentCursor` etc. | `{schema}.queue_ring_state.current_slot` / `lease_ring_state.current_slot` |
| `readySegments[seg]` state | partition presence + contents (`open` ≈ current write target, `sealed` ≈ rotated out but not pruned, `pruned` ≈ TRUNCATEd) |
| `claimSegmentOf[j]` | the `claim_slot` column on the job's `{schema}.lease_claims` row (ADR-023); closure rows in `{schema}.lease_claim_closures` share the same `claim_slot`. |
| `claimOpen` | set of `(job_id, run_lease)` pairs with a claim row but no matching closure row in the current claim-ring partitions. Derived at query time via the `lease_claims` ⨝ `lease_claim_closures` anti-join. |
| `claimClosed` | set of `(job_id, run_lease)` pairs with a matching closure row in the current claim-ring partitions. |
| `claimSegments[seg]` state | same semantics as other segment families; `{schema}.claim_ring_state.current_slot` identifies the open partition, `{schema}.claim_ring_slots(slot)` tracks per-partition generation. Seeded in `prepare_schema`; rotated by `rotate_claims`. |
| `claimSegmentCursor` | `{schema}.claim_ring_state.current_slot`. |

The TLA+ model does not represent the cold completed-history rollup cache.
Rust currently stores that in `{schema}.queue_terminal_rollups`, with
`queue_lanes.pruned_completed_count` kept only as a transitional legacy source
for backfill / fallback reads during upgrades.

## Action mapping

| TLA+ action | Rust function | SQL / DDL |
|---|---|---|
| `EnqueueReady(j)` | `QueueStorage::insert_ready_rows_tx` (`queue_storage.rs:3470`); producer entry points are `enqueue_batch` / `enqueue_runtime_rows` (`:3937, 3965`) | `INSERT INTO {schema}.ready_entries ... UPSERT {schema}.queue_enqueue_heads` (single tx) |
| `EnqueueDeferred(j)` | `QueueStorage::insert_deferred_rows_tx` (`queue_storage.rs:3625`) | `INSERT INTO {schema}.deferred_entries ...` |
| `PromoteDeferred(j)` | maintenance promote loop in `awa-worker/src/maintenance.rs::promote_due_state` | `DELETE FROM deferred_entries ... INSERT INTO ready_entries ...` in one tx |
| `AdvanceClaimCursor` | claim path gap-skipping after rescue/prune holes | inside the inline claim CTE; logical `UPDATE queue_claim_heads SET claim_seq = claim_seq + 1 WHERE no row at claim_seq` |
| `Claim(w, j)` | `QueueStorage::claim_runtime_batch` (`queue_storage.rs:4145`) → `claim_runtime_batch_with_aging_for_instance` (`:4504`) → dispatcher (`awa-worker/src/dispatcher.rs`) | inline claim CTE: lane selection via `FOR UPDATE OF queue_claim_heads SKIP LOCKED`; bare reads of `lease_ring_state` and `claim_ring_state` (no FOR SHARE/UPDATE — rotate's CAS UPDATE on `(current_slot, generation)` plus the partition busy-check provides the conflict detection); INSERT into `lease_claims_<claim_slot>` (receipts mode) or `leases_<lease_slot>` (legacy mode); UPDATE `queue_claim_heads` |
| `MaterializeAttemptState(j)` | `QueueStorage::upsert_attempt_state_from_receipts_tx` (`queue_storage.rs:6243`) and `upsert_attempt_state_progress_from_receipts_tx` (`:6307`) | `INSERT INTO attempt_state ... ON CONFLICT (job_id, run_lease) DO NOTHING` |
| `Heartbeat(j)` | `heartbeat_tick` in `awa-worker/src/heartbeat.rs` | `UPDATE leases SET heartbeat_at = now() WHERE job_id = $1 AND run_lease = $2` |
| `LoseHeartbeat(j)` | implicit — time passes without a heartbeat UPDATE; maintenance rescue sees a stale cutoff | (no action in real code; represents age) |
| `ProgressFlush(j)` | `QueueStorage::flush_progress` (`queue_storage.rs:7802`) | `UPDATE attempt_state SET progress = ... WHERE job_id = $1 AND run_lease = $2` guarded by running/waiting_external state |
| `ParkToWaiting(w, j)` | `QueueStorage::enter_callback_wait` (`queue_storage.rs:7291`) from executor `WaitForCallback` | delete from leases, insert into waiting_entries, preserve attempt_state |
| `ResumeWaitingToReady(j)` | admin / callback resume (lives in `awa-model/src/admin.rs:2695`) on callback success | `DELETE FROM waiting_entries`, `INSERT INTO ready_entries`, clear attempt_state |
| `TimeoutWaitingToReady(j)` | maintenance callback rescue with attempts remaining (`awa-worker/src/maintenance.rs::rescue_expired_callbacks`) | rescue SQL with attempt increment |
| `TimeoutWaitingToDlq(j)` | maintenance callback rescue with exhausted attempts | rescue SQL routes to `dlq_entries` instead of re-enqueue |
| `FastComplete(w, j)` | `QueueStorage::complete_runtime_batch` (`queue_storage.rs:4677`) short path (no attempt_state hydrate) | receipts mode: `INSERT INTO lease_claim_closures` (no `DELETE FROM leases`); legacy mode: `DELETE FROM leases` + `INSERT INTO done_entries` carrying the claim-time snapshot |
| `StatefulComplete(w, j)` | `QueueStorage::complete_runtime_batch` + `DELETE FROM attempt_state` | same as above plus `DELETE FROM attempt_state` |
| `FailToDlq(w, j)` | `QueueStorage::fail_to_dlq` (`queue_storage.rs:8088`) / `fail_terminal` (`:8055`) via executor terminal failure path | `DELETE FROM leases`, `DELETE FROM attempt_state`, `INSERT INTO dlq_entries` in one tx |
| `RetryToDeferred(w, j)` | `QueueStorage::retry_after` (`queue_storage.rs:7945`) / `snooze` (`:7981`) on `JobError::RetryAfter` / `Snooze` | `DELETE FROM leases`, `INSERT INTO deferred_entries` |
| `RescueToReady(j)` | `rescue_stale_heartbeats` (`queue_storage.rs:8575`) / `rescue_expired_deadlines` (`:8688`) in maintenance | `DELETE FROM leases ... RETURNING ...; INSERT INTO ready_entries ...` |
| `CancelWaitingToTerminal(j)` | admin cancel path in `awa-model/src/admin.rs` for a waiting job; receipt-only and running-lease branches in `cancel_job_tx` (`queue_storage.rs:5501`) | waiting branch: `DELETE FROM waiting_entries` + `INSERT INTO done_entries` with cancel reason. See also `CancelRunningToTerminal`, `CancelReceiptOnlyToTerminal` for the running-job branches. |
| `StaleCompleteRejected(w, j)` | `complete_runtime_batch` returning `CompletionOutcome::IgnoredStale` | `UPDATE leases ... WHERE run_lease = $2` matching 0 rows |
| `MoveFailedToDlq(j)` | `QueueStorage::move_failed_to_dlq` (`queue_storage.rs:8127`); admin entry in `awa-model/src/dlq.rs:170` | `DELETE FROM done_entries ... INSERT INTO dlq_entries ...` guarded by state=failed |
| `RetryFromDlq(j)` | `QueueStorage::retry_from_dlq` (`queue_storage.rs:8254`) | CTE: `DELETE FROM dlq_entries RETURNING ...` + `INSERT INTO ready_entries ...` with `run_lease = 0`; unique-conflict handled by `sync_unique_claim` |
| `PurgeDlq(j)` | `purge_dlq_job` / `purge_dlq` in `awa-model/src/dlq.rs:382, 423` | `DELETE FROM dlq_entries WHERE ...` |
| `RotateReadySegments` | maintenance `rotate_ready` (`awa-worker/src/maintenance.rs`) | `UPDATE queue_ring_state SET current_slot = next` + partition attach/detach |
| `RotateDeferredSegments` / `RotateWaitingSegments` / `RotateLeaseSegments` / `RotateDlqSegments` | parallel maintenance rotate functions per family | analogous `UPDATE *_ring_state` |
| `PruneReadySegment(seg)` | maintenance `prune_oldest` for the ready family (`queue_storage.rs:9080`) | `FOR UPDATE` on `queue_ring_state` and `queue_ring_slots[slot]`, then `LOCK TABLE ... ACCESS EXCLUSIVE`, recheck active rows, then `TRUNCATE`; Rust also updates `{schema}.queue_terminal_rollups` after a successful terminal-segment prune |
| `PruneDeferredSegment` / `PruneWaitingSegment` / `PruneLeaseSegment` / `PruneDlqSegment` | parallel prune paths per family (lease prune at `queue_storage.rs:9208`) | `TRUNCATE {schema}.X_segment_N` with active-row check |
| `RotateClaimSegments` | maintenance `QueueStorage::rotate_claims` (`queue_storage.rs:9333`), wired via `Maintenance::rotate_queue_storage_claims` at the `claim_rotate_interval` tick | `FOR UPDATE` on `claim_ring_state`, busy-check both child partitions, then `UPDATE claim_ring_state SET current_slot = next, generation = next_gen` with compare-and-swap on `(current_slot, generation)` |
| `PruneClaimSegment(seg)` | `QueueStorage::prune_oldest_claims` (`queue_storage.rs:9433`) | `FOR UPDATE` on `claim_ring_state`, `FOR UPDATE` on `claim_ring_slots[slot]`, `SET LOCAL lock_timeout = '50ms'`, `LOCK TABLE` `lease_claims_N` and `lease_claim_closures_N` `IN ACCESS EXCLUSIVE MODE`, recheck not-current, anti-join check that every claim has a closure (`PartitionTruncateSafety`), then `TRUNCATE` both children |
| `RescueStaleReceipt(j)` | `rescue_stale_receipt_claims_tx` (`queue_storage.rs:6672`), invoked from maintenance `rescue_stale_heartbeats`. Excludes claims already materialized into `leases` so the lease-side rescue path owns those. | anti-join `lease_claims` against `lease_claim_closures` and against `leases` over the active partitions; close stragglers by appending to `lease_claim_closures` (rescue closure outcome `'rescued'`) |
| `CancelRunningToTerminal(j)` | `cancel_job_tx` lease branch (`queue_storage.rs:5501`, ~line 5581) | `DELETE FROM leases ... RETURNING`, `insert_done_rows_tx` (state = `cancelled`), `close_receipt_tx` (writes the `'cancelled'` closure into the matching claim partition), `pg_notify('awa:cancel', ...)` |
| `CancelReceiptOnlyToTerminal(j)` | `cancel_job_tx` receipt-only branch (`queue_storage.rs:5621`) | `SELECT ... FROM lease_claims FOR UPDATE OF claims SKIP LOCKED` → `insert_done_rows_tx` → `INSERT INTO lease_claim_closures` → defensive `DELETE FROM leases` (sweeps any concurrent materialization) → `pg_notify` |

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
`queue_storage.rs:9080`.

**Status in the implementation: mitigated.** The prune path:

1. `FOR UPDATE` on `queue_ring_state` to serialise against concurrent
   rotates
2. `FOR UPDATE` on the target `queue_ring_slots` row
3. `SET LOCAL lock_timeout = '50ms'`, then `LOCK TABLE ... IN ACCESS
   EXCLUSIVE MODE` on the ready and done partition children — this
   blocks the AccessShare lock that the claim CTE takes when reading
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
directly and checks that no interleaving of claim / complete /
close-receipt / rescue-receipts / ensure-running / cancel /
rotate-leases / prune-leases / rotate-ready / prune-ready /
rotate-claims / prune-claims transactions produces a waits-for cycle.
Current result: 39,040 distinct states, no deadlock. A
deliberately-broken demo config
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

## ADR-023 receipt-plane coverage

The TLA+ specs cover the ADR-023 claim-ring shape end-to-end. In both
the base spec and the race / lock specs:

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
- `AwaStorageLockOrder` includes `ClaimRingStateResource`,
  `ClaimRingSlotResource`, `ClaimChildResource`, `ClosureChildResource`,
  and `ClaimReceiptsPlan` / `ClaimLegacyPlan` for the two execution
  modes with `RowExclusive` on the appropriate child. The
  `*_ring_state` reads in the claim CTE are bare `SELECT`s in Rust
  (no `FOR SHARE`); claim is serialised against rotate via the
  rotator's CAS UPDATE on `(current_slot, generation)`, not via
  row-level locks. `CompletePlan`, `RotateClaimsPlan`,
  `PruneClaimsPlan`, `CloseReceiptPlan`, `RescueReceiptsPlan`,
  `EnsureRunningPlan`, and `CancelReceiptPlan` round out the
  receipt-plane transactions.
- `AwaSegmentedStorageRaces` adds `claimSeg` to the claim-intent
  snapshot and exposes the claim-ring version of the naive commit race.

Invariants added:

- `OneOpenClaimSegment`, `ClaimCursorIsOpen`,
  `PrunedClaimSegmentsAreEmpty` (every segment family shape).
- `NoLostClaim`: every open receipt's segment is not pruned.
- `ClaimOpenAndClosedDisjoint`, `OpenClaimHasSegment`,
  `ClosedClaimHasSegment`: the receipt-lifecycle bookkeeping is sound.

Model checking results:

- `AwaSegmentedStorage.cfg`: 2,326,528 distinct states, clean. Admin
  cancel actions (`CancelRunningToTerminal`,
  `CancelReceiptOnlyToTerminal`) clear all workers' `taskLease`
  snapshots since admin cancel has no worker context, preserving
  `TaskLeaseBounded`.
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
- `AwaStorageLockOrder.cfg`: 39,040 distinct states, clean. Models
  the receipts and legacy claim modes separately, plus
  `CloseReceiptPlan`, `RescueReceiptsPlan`, `EnsureRunningPlan`,
  `CancelReceiptOnlyPlan`, and `CancelRunningPlan`.
- `AwaStorageLockOrderDeadlockDemo.cfg`: trips `NoDeadlock` in 5
  steps, confirming the detector works.
- `AwaDeadTupleContract.cfg`: 1 distinct state, clean. The four
  ASSUME-style architectural-contract checks
  (`HotPartitionedTablesUseTruncate`,
  `PartitionTruncateTablesAreReclaimed`,
  `AppendOnlyAcceptsOnlyInsert`,
  `RowVacuumTablesNotTruncated`) all hold for the current schema
  and transaction list. Workflow: when adding a new table to
  `prepare_schema()` or a new SQL site to queue_storage.rs, register
  it in `AwaDeadTupleContract.tla` with the correct reclaim kind /
  hotness / mutation list. An `open_receipt_claims`-style proposal
  (hot table, RowVacuum reclaim, INSERT+DELETE traffic) would fire
  `HotPartitionedTablesUseTruncate` at parse time — verified by
  adding the row temporarily and watching TLC report
  the assumption violation, then removing it.

### Receipt-plane shape

- `lease_claims` and `lease_claim_closures` are partitioned parents
  (`relkind = 'p'`); `lease_claims_0..N-1` and
  `lease_claim_closures_0..N-1` are the children. PK on both is
  `(claim_slot, job_id, run_lease)` (partition-key-in-PK), with a
  secondary `(job_id, run_lease)` index for completion / rescue /
  materialize paths that don't carry `claim_slot` in hand.
- The "currently open" set is derived at query time as
  `lease_claims` ⨝̸ `lease_claim_closures` over the active partitions.
  Sites: the running-count CTE in `queue_counts_exact`,
  `ensure_running_leases_from_receipts_tx`, the heartbeat / progress
  upsert paths, `close_receipt_tx`, `rescue_stale_receipt_claims_tx`,
  `complete_runtime_batch`'s receipt branch, and `load_job`. None of
  these touch `open_receipt_claims`.
- `ClaimedEntry` carries `claim_slot: i32` so completion can route
  the closure INSERT to the matching partition without an extra
  lookup.
- `prepare_schema()` drops `open_receipt_claims` on every install
  (refusing if it has rows). `reset()` does the same and clears any
  `_legacy` tables left over from a partial migration.
- `claim_slot_count` (default 8, minimum 2) sets the partition count.
  `claim_rotate_interval` (defaulting to `queue_rotate_interval`)
  drives the rotation cadence; `ClientBuilder::claim_rotate_interval`
  overrides per-test or per-bench.
- `rotate_claims` advances the cursor with a `FOR UPDATE` on
  `claim_ring_state` and a busy-check on both child partitions of the
  next slot — the rotation invariant is "the slot we're flipping
  onto must be empty". `prune_oldest_claims` walks the full
  ring-state → slot-row → child `ACCESS EXCLUSIVE` lock sequence and
  refuses to TRUNCATE while any claim in the partition lacks a
  matching closure (`PartitionTruncateSafety`).

### Coverage

`queue_storage_runtime_test` (49 tests) covers the lifecycle:
partition routing, rotation isolation, partition migration on schema
upgrade, the rotate / prune busy-and-safety predicates, admin cancel
of running attempts, the open-receipt-claims absence invariant, the
full short-job lifecycle, and the rescue paths for
heartbeat / deadline / receipt-only attempts.

`receipt_plane_chaos_test` (4 `#[ignore]`-d nightly tests) covers
flood / concurrency / lock-order scenarios: rescue throughput under
overload, prune-skips-active under concurrent traffic, the
`ACCESS EXCLUSIVE` barrier between TRUNCATE and concurrent inserts,
and admin-cancel-during-materialize orphan-lease cleanup.
