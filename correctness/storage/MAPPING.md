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
| `deferredEntries` | `{schema}.deferred_jobs` |
| `waitingLeases` | subset of `{schema}.leases` rows with `state = 'waiting_external'`; there is no waiting table |
| `terminalEntries` | `{schema}.done_entries` (ADR-019 target name: `terminal_entries`). ADR-026 makes ready-backed terminal rows narrow: duplicated immutable body fields hydrate from the retained `{schema}.ready_entries` row until queue prune reclaims both. |
| `dlqEntries` | `{schema}.dlq_entries` |
| `activeLeases` | live rows in `{schema}.leases`, including both `running` and `waiting_external` |
| `attemptState` | `{schema}.attempt_state` |
| `runLease[j]` | `run_lease` column on the lease/ready/deferred row |
| `taskLease[w][j]` | `ctx.job.run_lease` snapshot captured at claim time in `awa-worker/src/executor.rs` |
| `heartbeatFresh` | `heartbeat_at` on the lease row + the maintenance cutoff (see `rescue_stale_heartbeats` in `queue_storage.rs:8575`) |
| `laneState.appendSeq` / `claimSeq` | `{schema}.queue_enqueue_heads.next_seq` / `{schema}.queue_claim_heads.claim_seq`, keyed by `(queue, priority, enqueue_shard)` |
| `readySegmentCursor` etc. | `{schema}.queue_ring_state.current_slot` / `lease_ring_state.current_slot` |
| `readySegments[seg]` state | partition presence + contents (`open` ≈ current write target, `sealed` ≈ rotated out but not pruned, `pruned` ≈ TRUNCATEd) |
| `claimSegmentOf[<<j, r>>]` | the `claim_slot` column on the `(job_id, run_lease)` claim row in `{schema}.lease_claims` (ADR-023); closure rows in `{schema}.lease_claim_closures` share the same `claim_slot`. The spec keys claim bookkeeping by `(job, run_lease)` rather than by `job` alone so that an old attempt's receipt survives the next claim into a newer partition — Rust's `(claim_slot, job_id, run_lease)` triplet is the actual partition-side unique key, but the model abstracts the `claim_slot` half away into `claimSegmentOf`. |
| `claimOpen` | set of `(job_id, run_lease)` pairs with a claim row but no matching closure row in the current claim-ring partitions. Derived at query time via the `lease_claims` ⨝ `lease_claim_closures` anti-join. Open receipt-only claims retain a ready row for body hydration but are excluded from `CurrentReady`; retained ready rows behind the claim cursor are also not current-ready. |
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
| `EnqueueReady(j)` | `QueueStorage::insert_ready_rows_tx` and `QueueStorage::insert_ready_rows_copy_tx`; producer entry points include `enqueue_batch`, `enqueue_runtime_rows`, `enqueue_params_batch`, and `enqueue_params_copy` | reserve `{schema}.queue_enqueue_heads.next_seq`, sync enqueue-time uniqueness claims, append to `{schema}.ready_entries` via INSERT or COPY, and notify logical queues in one tx |
| `EnqueueDeferred(j)` | `QueueStorage::insert_deferred_rows_tx` and `QueueStorage::insert_deferred_rows_copy_tx`; producer entry points include `enqueue_params_batch` and `enqueue_params_copy` | allocate job ids, sync enqueue-time uniqueness claims, append to `{schema}.deferred_jobs` via INSERT or COPY in one tx |
| `PromoteDeferred(j)` | maintenance promote loop in `awa-worker/src/maintenance.rs::promote_due_state` | `DELETE FROM deferred_jobs ... INSERT INTO ready_entries ...` in one tx |
| `AdvanceClaimCursor` | claim path gap-skipping after rescue/prune holes | inside the inline claim CTE; logical `UPDATE queue_claim_heads SET claim_seq = claim_seq + 1 WHERE no row at claim_seq` |
| `Claim(w, j)` | `QueueStorage::claim_runtime_batch` (`queue_storage.rs:4145`) → `claim_runtime_batch_with_aging_for_instance` (`:4504`) → dispatcher (`awa-worker/src/dispatcher.rs`) | inline claim CTE: lane selection via `FOR UPDATE OF queue_claim_heads SKIP LOCKED`; bare reads of `lease_ring_state` and `claim_ring_state` (no FOR SHARE/UPDATE — rotate's CAS UPDATE on `(current_slot, generation)` plus the partition busy-check provides the conflict detection); INSERT into `lease_claims_<claim_slot>` (receipts mode) or `leases_<lease_slot>` (legacy mode); UPDATE `queue_claim_heads` |
| `MaterializeAttemptState(j)` | `QueueStorage::upsert_attempt_state_from_receipts_tx` (`queue_storage.rs:6243`) and `upsert_attempt_state_progress_from_receipts_tx` (`:6307`) | `INSERT INTO attempt_state ... ON CONFLICT (job_id, run_lease) DO NOTHING` |
| `Heartbeat(j)` | `heartbeat_tick` in `awa-worker/src/heartbeat.rs` | `UPDATE leases SET heartbeat_at = now() WHERE job_id = $1 AND run_lease = $2` |
| `LoseHeartbeat(j)` | implicit — time passes without a heartbeat UPDATE; maintenance rescue sees a stale cutoff | (no action in real code; represents age) |
| `ProgressFlush(j)` | `QueueStorage::flush_progress` (`queue_storage.rs:7802`) | `UPDATE attempt_state SET progress = ... WHERE job_id = $1 AND run_lease = $2` guarded by running/waiting_external state |
| `ParkToWaiting(w, j)` | `QueueStorage::enter_callback_wait` (`queue_storage.rs:7291`) from executor `WaitForCallback` | `UPDATE leases SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL`; attempt_state is preserved |
| `ResumeWaitingToRunning(j)` | `QueueStorage::complete_external(..., resume = true)` (`queue_storage.rs:7444`) | `UPDATE leases SET state = 'running', callback_id = NULL, callback_timeout_at = NULL, heartbeat_at = clock_timestamp()` plus callback result upsert in `attempt_state` |
| `TimeoutWaitingToReady(j)` | maintenance callback rescue with attempts remaining (`awa-worker/src/maintenance.rs::rescue_expired_callbacks`) | delete the waiting lease and append a fresh `ready_entries` row |
| `TimeoutWaitingToDlq(j)` | maintenance callback rescue with exhausted attempts | delete the waiting lease and insert `dlq_entries` |
| `FastComplete(w, j)` | `QueueStorage::complete_runtime_batch` short path (no attempt_state hydrate) | receipts mode: one SQL statement inserts `lease_claim_closures` and the ready-backed narrow `done_entries` fact from the claimed runtime snapshot, with missed/materialized receipts falling back to the general transaction path; legacy mode: `DELETE FROM leases`; terminal reads hydrate immutable body fields through the retained ready row |
| `StatefulComplete(w, j)` | `QueueStorage::complete_runtime_batch` + `DELETE FROM attempt_state` | same as above plus `DELETE FROM attempt_state`; terminal reads hydrate the immutable body through the retained ready row |
| `FailToDlq(w, j)` | `QueueStorage::fail_to_dlq` (`queue_storage.rs:8088`) / `fail_terminal` (`:8055`) via executor terminal failure path | `DELETE FROM leases`, `DELETE FROM attempt_state`, `INSERT INTO dlq_entries` in one tx |
| `RetryToDeferred(w, j)` | `QueueStorage::retry_after` (`queue_storage.rs:7945`) / `snooze` (`:7981`) on `JobError::RetryAfter` / `Snooze` | `DELETE FROM leases`, `INSERT INTO deferred_jobs` |
| `RescueToReady(j)` | `rescue_stale_heartbeats` (`queue_storage.rs:8575`) / `rescue_expired_deadlines` (`:8688`) in maintenance | `DELETE FROM leases ... RETURNING ...; INSERT INTO ready_entries ...` |
| `CancelWaitingToTerminal(j)` | waiting branch in `cancel_job_tx` (`queue_storage.rs:5501`) | `DELETE FROM leases WHERE state IN ('running', 'waiting_external')`, hydrate from `ready_entries`, insert `done_entries`, close the matching receipt |
| `StaleCompleteRejected(w, j)` | `complete_runtime_batch` returning `CompletionOutcome::IgnoredStale` | `UPDATE leases ... WHERE run_lease = $2` matching 0 rows |
| `MoveFailedToDlq(j)` | `QueueStorage::move_failed_to_dlq` (`queue_storage.rs:8127`); admin entry in `awa-model/src/dlq.rs:170` | hydrate then `DELETE FROM done_entries`, delete the retained ready backing row, and `INSERT INTO dlq_entries` guarded by state=failed |
| `RetryFromDlq(j)` | `QueueStorage::retry_from_dlq` (`queue_storage.rs:8254`) | CTE: `DELETE FROM dlq_entries RETURNING ...` + `INSERT INTO ready_entries ...` (Rust resets `run_lease` to 0 because the new claim row will live in a different `claim_slot` partition; the spec keeps `run_lease` monotonic per-job since it abstracts away `claim_slot` from the receipt key); unique-conflict handled by `sync_unique_claim` |
| `PurgeDlq(j)` | `purge_dlq_job` / `purge_dlq` in `awa-model/src/dlq.rs:382, 423` | `DELETE FROM dlq_entries WHERE ...` |
| `RotateReadySegments` | maintenance `rotate_ready` (`awa-worker/src/maintenance.rs`) | `UPDATE queue_ring_state SET current_slot = next` + partition attach/detach |
| `RotateLeaseSegments` | `QueueStorage::rotate_leases` | `UPDATE lease_ring_state` with child-partition busy check |
| `PruneReadySegment(seg)` | maintenance `prune_oldest` for the ready/terminal queue family (`queue_storage.rs:9080`) | `FOR UPDATE` on `queue_ring_state` and `queue_ring_slots[slot]`, then `LOCK TABLE ... ACCESS EXCLUSIVE`, recheck active rows, then `TRUNCATE`; ready-backed terminal rows are reclaimed with their retained ready bodies, and Rust updates `{schema}.queue_terminal_rollups` after successful terminal prune accounting |
| `PruneLeaseSegment` | `QueueStorage::prune_oldest_leases` (`queue_storage.rs:9208`) | `TRUNCATE` the selected `leases_N` child only after active-row checks |
| `RotateClaimSegments` | maintenance `QueueStorage::rotate_claims` (`queue_storage.rs:9333`), wired via `Maintenance::rotate_queue_storage_claims` at the `claim_rotate_interval` tick | `FOR UPDATE` on `claim_ring_state`, busy-check both child partitions, then `UPDATE claim_ring_state SET current_slot = next, generation = next_gen` with compare-and-swap on `(current_slot, generation)` |
| `PruneClaimSegment(seg)` | `QueueStorage::prune_oldest_claims` (`queue_storage.rs:9433`) | `FOR UPDATE` on `claim_ring_state`, `FOR UPDATE` on `claim_ring_slots[slot]`, `SET LOCAL lock_timeout = '50ms'`, `LOCK TABLE` `lease_claims_N` and `lease_claim_closures_N` `IN ACCESS EXCLUSIVE MODE`, recheck not-current, anti-join check that every claim has a closure (`PartitionTruncateSafety`), then `TRUNCATE` both children |
| `RescueStaleReceipt(j, r)` | `rescue_stale_receipt_claims_tx` (`queue_storage.rs:6672`), invoked from maintenance `rescue_stale_heartbeats`. Excludes claims already materialized into `leases` so the lease-side rescue path owns those. The spec takes the explicit `(j, r)` so concurrent rescue / re-claim races are reachable: rescue can fire on an old attempt's `(j, r_old)` receipt while `(j, r_old + 1)` already has an open receipt in a newer partition. | anti-join `lease_claims` against `lease_claim_closures` and against `leases` over the active partitions; close stragglers by appending to `lease_claim_closures` (rescue closure outcome `'rescued'`) |
| `CancelRunningToTerminal(j)` | `cancel_job_tx` lease branch (`queue_storage.rs:5501`, ~line 5581) | `DELETE FROM leases ... RETURNING`, `insert_done_rows_tx` (state = `cancelled`), `close_receipt_tx` (writes the `'cancelled'` closure into the matching claim partition), `pg_notify('awa:cancel', ...)` |
| `CancelReceiptOnlyToTerminal(j)` | `cancel_job_tx` receipt-only branch (`queue_storage.rs:5621`) | `SELECT ... FROM lease_claims FOR UPDATE OF claims SKIP LOCKED` → `insert_done_rows_tx` → `INSERT INTO lease_claim_closures` → defensive `DELETE FROM leases` (sweeps any concurrent materialization) → `pg_notify` |

## Invariant mapping

| TLA+ invariant | Rust enforcement |
|---|---|
| `ActiveLeasesSubsetReadyEntries` | every `leases` row FK-references `ready_entries(queue, priority, enqueue_shard, lane_seq)` (check the CREATE TABLE DDL in the `install` fn) |
| `WaitingIsLeaseState` | `waiting_external` is represented by a row in `leases`, not by a separate waiting table |
| `AttemptStateRequiresLiveLease` | callback/progress attempt state is associated with a live lease row and is deleted on terminal/retry/rescue paths |
| `FreshHeartbeatRequiresLease` | `heartbeat_at` is a column on `leases`; `enter_callback_wait` clears it for waiting leases |
| `TerminalHasNoLiveRuntime` | `complete_runtime_batch` / terminal cancel/fail paths clear live runtime state before inserting terminal; a retained ready row is body storage, not live availability, because `CurrentReady` anti-joins terminal rows |
| `TerminalHasRetainedReadyBody` | ADR-026 ready-backed terminal rows keep the ready row until terminal delete or queue prune; `load_job`, retry, DLQ move, and discard hydrate before removing the terminal fact |
| `DlqHasNoLiveRuntime` | same, for dlq path |
| `DlqAndTerminalDisjoint` | `move_failed_to_dlq` uses `DELETE FROM done_entries ... RETURNING` then `INSERT INTO dlq_entries` in one tx; no intermediate state where both hold the same job_id |
| `StaleCompleteRejected` precondition | `WHERE run_lease = $2 AND state = 'running'` clauses on every completion UPDATE |
| `ReadyLaneSeqUnique` | `UNIQUE(queue, priority, enqueue_shard, lane_seq)` on `ready_entries` child partitions; `AwaSegmentedStorage` checks the per-shard uniqueness shape and `AwaShardedPrune` checks the cross-shard duplicate-`lane_seq` case |
| `ClaimCursorBounded` | `queue_lanes.claim_seq <= queue_lanes.append_seq` should be a CHECK constraint (currently implicit; worth adding) |
| `PrunedXSegmentsAreEmpty` | ready, lease, terminal, and claim prune require no-live-row preconditions before TRUNCATE; queue-slot prune checks pending ready rows by `(queue, priority, enqueue_shard, lane_seq)` and reclaims matching terminal rows with their retained ready bodies; `deferred_jobs` and `dlq_entries` are unpartitioned backlog row-vacuum tables and are covered by `AwaDeadTupleContract` |
| `PrunedClaimSegmentsAreEmpty` (ADR-023) | `prune_oldest_claims` requires no open claim in the partition before TRUNCATE; rescue-before-truncate closes stragglers in the same transaction |
| `NoLostClaim` (ADR-023) | receipts and their closures both live in `claim_slot`-partitioned tables; partitions only truncate once all their receipts are closed, so no open claim is physically dropped |
| `ClaimOpenAndClosedDisjoint` (ADR-023) | closure insertion and receipt-clearing are a single transaction; a partition's receipt+closure pair is either both present or both dropped by `TRUNCATE` |
| `LaneStateConsistent` | live availability is derived from `{schema}.queue_enqueue_heads.next_seq` minus `{schema}.queue_claim_heads.claim_seq` for the same `(queue, priority, enqueue_shard)` lane, plus exact scans of live ready/receipt/lease rows where needed. Rust's hot-path queue signal path (`QueueStorage::queue_claimer_signal`) and exact admin reads use the same shard-aware head identity rather than a mutable `available_count` cache. Completed totals are *not* maintained as hot counters — Rust derives them from live `done_entries` plus the cold `{schema}.queue_terminal_rollups` cache, with `queue_lanes.pruned_completed_count` read only as a transitional legacy fallback |

## Public read and compatibility surfaces

`AwaSegmentedStorage.tla` models storage state, not every SQL projection over
that state. Public and admin reads are therefore refinement obligations on the
SQL implementation:

- `awa.jobs` / `awa.jobs_compat()` is the compatibility view used by SQL
  adapters and operational queries. Migration
  `awa-model/migrations/v019_queue_storage_jobs_compat_shard_joins.sql`
  keeps queue-storage joins shard-aware: available rows join claim heads by
  `(queue, priority, enqueue_shard)`, and lease-backed rows join ready bodies
  by `(queue, priority, enqueue_shard, lane_seq)`.
- `awa-model/src/admin.rs::queue_storage_current_jobs_cte`,
  `awa-model/src/admin.rs::state_counts`, and
  `awa-worker/src/client.rs::health_check` are the Rust read-side equivalents.
  They must preserve the same enqueue-shard predicates or multi-shard queues
  can overcount available rows or hydrate a row from the wrong shard.
- `test_queue_storage_multi_shard_public_available_counts_are_exact` is the
  code-level regression test for this projection boundary. A future TLA+
  projection model could make this formal by deriving `JobsCompatAvailable`
  from the storage variables and asserting it equals `CurrentReady`; the
  current storage model stops at the underlying lifecycle and prune state.

## Storage-transition model mapping

`AwaStorageTransition.tla` maps to the transition SQL in
`awa-model/migrations/v010_storage_transition_prep.sql`,
`awa-model/migrations/v012_queue_storage_compat.sql`, and the executor
gate in `awa-model/migrations/v014_storage_transition_role.sql`, plus
the worker role/effective-storage resolution in
`awa-worker/src/client.rs`.

| TLA+ variable / action | Rust / SQL equivalent |
|---|---|
| `state`, `currentEngine`, `preparedEngine` | `awa.storage_transition_state.state`, `current_engine`, `prepared_engine` |
| `preparedSchemaReady` | `queue_storage_schema_ready()` / SQL checks for `{schema}.queue_ring_state`, `ready_entries`, and `leases` |
| `oldCanonicalLive` | live `awa.runtime_instances` rows with `storage_capability = 'canonical'` |
| `autoPreMixedLive` | a 0.6 `TransitionWorkerRole::Auto` runtime that resolved effective storage to canonical before mixed transition; it reports `queue_storage` while prepared and `canonical_drain_only` once routing flips |
| `queueTargetLive` | `TransitionWorkerRole::QueueStorageTarget`; the only modeled runtime population that can execute queue-storage work immediately after mixed transition |
| `canonicalBacklog` | `awa.canonical_live_backlog()` over `jobs_hot` and `scheduled_jobs` |
| `queueRows` | queue-storage row existence checks in `storage_abort()` across `ready_entries`, `deferred_jobs`, `leases`, `attempt_state`, `done_entries`, and `dlq_entries` |
| `PrepareQueueStorage` | `awa.storage_prepare('queue_storage', details)` |
| `PrepareSchema` | `QueueStorage::prepare_schema()` plus transition details naming the schema |
| `EnterMixedTransition` | `awa.storage_enter_mixed_transition()` and insertion of `runtime_storage_backends('queue_storage', schema)` |
| `ProducerEnqueueCanonical` | `insert_job_compat()` path before `active_queue_storage_schema()` is set |
| `ProducerEnqueueQueueStorage` | `insert_job_compat()` path after mixed transition activates the queue-storage backend |
| `DrainCanonical` | canonical-drain workers continuing to complete `jobs_hot` / `scheduled_jobs` backlog |
| `Finalize` | `awa.storage_finalize()`: requires `canonical_live_backlog() = 0` and no live `canonical` / `canonical_drain_only` runtimes |
| `AbortMixed` | `awa.storage_abort()`: rejects rollback while live `queue_storage` runtimes or queue-storage rows exist |

The model deliberately keeps `MixedHasQueueExecutor` as an entry-gate
property, not a permanent liveness invariant: a queue-storage target can
stop after the transition. As of v014 the SQL gate enforces
`transition_role = 'queue_storage_target' AND storage_capability =
'queue_storage'`, which is the same property `LiveQueueExecutor > 0`
expresses in the model — `AwaStorageTransition.cfg` (with
`RequireQueueExecutorOnEnter = TRUE`) is the configuration that matches
production. `AwaStorageTransitionCurrentGate.cfg` is retained as a
historical reproducer of the pre-v014 gap, where
`storage_capability = 'queue_storage'` alone was used and the
`MixedHasQueueExecutor` invariant could fail because an
`autoPreMixedLive` runtime satisfied the gate pre-flip and downgraded to
drain-only post-flip.

## Local runtime note

The TLA+ storage model does not represent local worker-capacity accounting.
Rust now releases local queue capacity immediately after handler execution and
progress snapshotting, while durable completion continues asynchronously
through the completion batcher. That changes throughput and scheduling
behavior, but it does not change the modeled storage safety boundary because
the `run_lease`-guarded finalization and rescue semantics are unchanged.

## Producer batching note

The TLA+ storage model treats `EnqueueReady` and `EnqueueDeferred` as
logical per-job state transitions. Rust may batch the SQL implementation
of producer side effects: allocating a contiguous lane sequence range,
syncing enqueue-time `job_unique_claims` with one array-backed statement,
and inserting rows with multi-row `INSERT` or COPY. Those batching choices
refine the same logical actions as long as they commit in the same
transaction as the ready/deferred append.

Uniqueness itself is intentionally outside this storage model: duplicate
rejection is covered by Rust integration tests around `job_unique_claims`.
The model's enqueue preconditions start after a job has been admitted to
the storage state, so batching uniqueness claims changes implementation
granularity rather than the modeled lifecycle, lane, lease, or prune
invariants.

## Enqueue-shard prune note

`AwaSegmentedStorage.tla` models a single `(queue, priority, enqueue_shard)`
lane. That keeps the lifecycle state-space small and is enough for per-shard
FIFO, lease, receipt, terminal, DLQ, and prune safety. It does not model two
shards whose `lane_seq` values collide by design.

`AwaShardedPrune.tla` is the focused cross-shard companion. It models two
independent shard counters and the queue-ring prune anti-join. The production
predicate is:

```sql
done.ready_generation = ready.ready_generation
AND done.queue = ready.queue
AND done.priority = ready.priority
AND done.enqueue_shard = ready.enqueue_shard
AND done.lane_seq = ready.lane_seq
```

Dropping `done.enqueue_shard = ready.enqueue_shard` is exactly the historical
bug reproduced by `AwaShardedPruneBroken.cfg`.

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
It also models the striped producer path that updates multiple physical
queue lanes in a stable order, while the current runtime claim path claims
only one physical stripe per transaction. A deliberately-broken demo config
(`AwaStorageLockOrderDeadlockDemo.cfg`) confirms the deadlock detector fires
when a cycle exists, and `AwaStorageLockOrderOldStripedClaimDeadlock.cfg`
captures the historical unsafe shape where one logical claim transaction
walked multiple physical stripes in the opposite order from enqueue.

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
- `ReceiptRescueTrace`: 3 events — EnqueueReady →
  SeedOpenReceiptOnlyClaim → RescueStaleReceipt. Accepts cleanly with
  4 states. `SeedOpenReceiptOnlyClaim` is trace-only scaffolding for
  the implementation's receipt-only window before lease materialization;
  the base storage spec's `Claim` action materializes a lease immediately.
- `RunningCancelTrace`: 3 events — EnqueueReady → Claim →
  CancelRunningToTerminal. Accepts cleanly with 4 states.
- `DlqRetryTrace`: 6 events — EnqueueReady → Claim → FailToDlq →
  RetryFromDlq → Claim → FastComplete. Accepts cleanly with 7 states.
- `ReceiptOnlyCancelTrace`: 3 events — EnqueueReady →
  SeedOpenReceiptOnlyClaim → CancelReceiptOnlyToTerminal. Accepts
  cleanly with 4 states. Models `awa::admin::cancel` racing with a
  freshly-opened receipt before the lease row materialises;
  complements `RunningCancelTrace` (active lease) and
  `ReceiptRescueTrace` (stale receipt left by a vanished worker).
- `CallbackWaitTrace`: 6 events — EnqueueReady → Claim →
  MaterializeAttemptState → ParkToWaiting → ResumeWaitingToRunning →
  StatefulComplete. Accepts cleanly with 7 states. Covers the
  external-callback `WaitForCallback` round-trip; distinct from
  `SnoozeTrace`, which never enters `waiting_external`.
- `DlqPurgeTrace`: 4 events — EnqueueReady → Claim → FailToDlq →
  PurgeDlq. Accepts cleanly with 5 states. Validates the operator
  `awa dlq purge` path; complements `DlqRetryTrace` which revives the
  row instead of removing it.
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
  `TimeoutWaitingToReady`) append a closure row
  in the same partition.
- `ParkToWaiting` does NOT close the receipt — the attempt is still
  alive in callback wait. `ResumeWaitingToRunning` keeps the same receipt
  open; timeout/cancel/terminal resolution close it.
- `RescueStaleReceipt(j, r)` models Tier-A receipt rescue: force-close
  a straggler receipt whose attempt is no longer alive — either the job
  has moved past `r` to a newer attempt (`runLease[j] > r`) or the job
  is fully off the ready / leased / waiting lifecycle. This is the
  rescue-before-truncate precondition that `prune_oldest_claims` will
  invoke. The `(j, r)` keying lets the model reach race orderings where
  rescue closes an old attempt's receipt *concurrently with* a newer
  Claim having already opened a fresh receipt under `(j, r+1)`.
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

- `AwaSegmentedStorage.cfg`: 33,920 distinct states, clean. Admin
  cancel actions (`CancelRunningToTerminal`,
  `CancelReceiptOnlyToTerminal`) clear all workers' `taskLease`
  snapshots since admin cancel has no worker context, preserving
  `TaskLeaseBounded`.
- `AwaSegmentedStorageInterleavings.cfg` (2 workers): 74,432 distinct
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
- `AwaShardedPrune.cfg`: 1,028 distinct states, clean. This checks the
  fixed queue-ring prune predicate that matches ready rows to done rows by
  `(enqueue_shard, lane_seq)`.
- `AwaShardedPruneBroken.cfg`: expected failure. It ignores
  `enqueue_shard` in the ready/done match and produces the counterexample
  where shard 0's completed `lane_seq = 1` row masks shard 1's still-pending
  `lane_seq = 1` row.
- `AwaStorageLockOrder.cfg`: 69,057 distinct states, clean. Models
  the receipts and legacy claim modes separately, plus
  `CloseReceiptPlan`, `RescueReceiptsPlan`, `EnsureRunningPlan`,
  `CancelReceiptOnlyPlan`, and `CancelRunningPlan`.
- `AwaStorageLockOrderDeadlockDemo.cfg`: trips `NoDeadlock` in 5
  steps, confirming the detector works.
- `AwaDeadTupleContract.cfg`: 1 distinct state, clean. The
  ASSUME-style architectural-contract checks
  (`HotTablesAreNotRowVacuum`, `PartitionTruncateTablesAreReclaimed`,
  `WarmTablesDocumentTheirBound`,
  `BacklogRowVacuumTablesDocumentTheirBound`,
  `OnlyBoundedKindsHaveBoundedBy`,
  `AppendOnlyAcceptsOnlyInsert`, `VacuumKindTablesNotTruncated`) all
  hold for the current schema and transaction list. Workflow: when
  adding a new table to `prepare_schema()` or a new SQL site to
  queue_storage.rs, register it in `AwaDeadTupleContract.tla` with
  the correct reclaim kind, hotness, optional `bounded_by`, and
  mutation list. An `open_receipt_claims`-style proposal (hot table,
  RowVacuum reclaim, INSERT+DELETE traffic) fires
  `HotTablesAreNotRowVacuum` at parse time; a `Warm` table without a
  declared `bounded_by` fires `WarmTablesDocumentTheirBound`.

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
