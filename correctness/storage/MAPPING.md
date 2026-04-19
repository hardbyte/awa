# AwaSegmentedStorage — Rust correspondence

This doc pins each TLA+ action in `AwaSegmentedStorage.tla` to the Rust
code and SQL that implements it. It is intended as a mechanical cross-check
as names and internals drift on the `feature/vacuum-aware-storage-redesign`
line.

File references are at the time of writing (2026-04-19). When the
prototype renames (e.g. `leases` → `active_leases`, `done_entries` →
`terminal_entries`) land per ADR-019, update this table accordingly.

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
| `laneState.appendSeq` / `claimSeq` | `{schema}.queue_lanes.append_seq` / `claim_seq` |
| `readySegmentCursor` etc. | `{schema}.queue_ring_state.current_slot` / `lease_ring_state.current_slot` |
| `readySegments[seg]` state | partition presence + contents (`open` ≈ current write target, `sealed` ≈ rotated out but not pruned, `pruned` ≈ TRUNCATEd) |

## Action mapping

| TLA+ action | Rust function | SQL / DDL |
|---|---|---|
| `EnqueueReady(j)` | `QueueStorage::insert_ready` / producer insert path | `INSERT INTO {schema}.ready_entries ... UPDATE {schema}.queue_lanes SET append_seq = append_seq + 1` (single tx) |
| `EnqueueDeferred(j)` | `QueueStorage::insert_deferred` | `INSERT INTO {schema}.deferred_entries ...` |
| `PromoteDeferred(j)` | maintenance promote loop in `awa-worker/src/maintenance.rs::promote_due_deferred_jobs` | `DELETE FROM deferred_entries ... INSERT INTO ready_entries ...` in one tx |
| `AdvanceClaimCursor` | claim path gap-skipping after rescue/prune holes | inside `claim_ready_runtime` PL/pgSQL function (`queue_storage.rs:1647`); logical `UPDATE queue_lanes SET claim_seq = claim_seq + 1 WHERE no row at claim_seq` |
| `Claim(w, j)` | `QueueStorage::claim_runtime_batch` → dispatcher (`awa-worker/src/dispatcher.rs`) | `claim_ready_runtime(...)` server-side fn: `SELECT ... FROM queue_lanes FOR UPDATE`, `INSERT INTO leases`, `UPDATE queue_lanes` in one step |
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
| `PruneReadySegment(seg)` | maintenance `prune_oldest` for the ready family (`queue_storage.rs:5994`) | `SELECT ... WHERE slot = $1` check, then `TRUNCATE {schema}.ready_segment_N` in a separate tx (see "Known modelling gaps" below) |
| `PruneDeferredSegment` / `PruneWaitingSegment` / `PruneLeaseSegment` / `PruneDlqSegment` | parallel prune paths per family | `TRUNCATE {schema}.X_segment_N` with active-row check |

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
| `LaneStateConsistent` | `queue_lanes.ready_count` / `leased_count` are maintained by the claim/complete paths (**note**: the redesign branch has a known accounting inconsistency here — see ADR-019 review) |

## Known modelling gaps with implementation implications

### Claim vs Rotate race — and prune check-then-act

Both were exposed in a single TLC trace from the companion spec
[`AwaSegmentedStorageRaces.tla`](./AwaSegmentedStorageRaces.tla). See
`README.md` for the trace and the safe-refinement variant.

The race, in terms of Rust code:

1. `claim_ready_runtime` at `queue_storage.rs:1647` locks the
   `queue_lanes` row (for the job's queue+priority) via `FOR UPDATE`.
2. It reads `lease_ring_state.current_slot` — **no lock** is taken on
   `lease_ring_state` at this point (or it's not obvious from the
   surrounding code that one is).
3. It inserts into `{schema}.leases` tagged with the read slot value.

Concurrently, the maintenance leader's `rotate_leases` path does not
touch `queue_lanes`, so it is free to execute between (2) and (3). If
both rotate and the subsequent `prune_oldest_leases` fire in that
window, the insert at (3) lands in a segment that has since transitioned
through sealed → pruned.

The safe fix at the spec level is to re-read `leaseSegments[leaseSeg] =
"open"` at commit time (see `CommitClaimChecked`). At the Rust level,
any of the following would be equivalent:

- re-read `lease_ring_state.current_slot` under the `queue_lanes` lock
  and abort if it has moved
- take a share lock (`FOR SHARE`) on `lease_ring_state` across the
  read+insert
- reshape the claim into a single CTE that reads the slot and inserts
  atomically (the natural snapshot of one statement)

Until one of these is confirmed in the SQL, the race is a real
merge-blocker per my earlier review — the model has now converted it
from "unproven" to "observable if the above guarantees are absent."

### prune_oldest check-then-act race

The race spec's PruneLeaseSegment transition actually captures this
already. In state 5 of the violating trace, `PruneLeaseSegment(1)`
fires despite a pending `claimIntent[w1].leaseSeg = 1`, because the
pending claim hasn't committed, so `activeLeases = {}` and the prune
precondition `\A j : leaseSegmentOf[j] = seg => j \notin activeLeases`
passes vacuously. This is structurally the same as the Rust
`prune_oldest` (`queue_storage.rs:5994`) reading the active-leases
count outside the TRUNCATE tx.

Fixing the claim side closes both races: once CommitClaim either
aborts (because the segment is no longer open) or lands on the current
segment, prune can no longer race against an in-flight lease on the
old segment.

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
