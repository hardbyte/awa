# Awa Correctness Models

This directory contains TLA+ correctness models for the Awa worker runtime.

It contains small TLA+ models for the coordination protocol behind the worker
runtime. The goal is to check the concurrency invariants that are easy to miss
with integration tests:

- a rescued or cancelled job cannot be finalized by a stale worker
- graceful shutdown stops new claims before drain and keeps heartbeat alive
- a non-abandoned running job always has reserved capacity
- weighted overflow never exceeds the global cap
- a contending queue eventually receives overflow capacity under the modeled fairness assumptions
- drain timeout / abandonment is modeled explicitly so shutdown-to-zero behavior can be explored without violating the safety invariants

What is modeled:

- abstract job states
- explicit attempt / lease identity for running attempts
- two worker instances with separate local state and shared database-facing job state
- worker ownership plus a separate per-instance in-flight task registry
- reservation vs claim vs execution as separate protocol stages
- service lifecycles for dispatchers, heartbeat, and maintenance
- leader failover for maintenance
- abstract heartbeat freshness and deadline expiry
- guarded finalize rejection for stale completions
- bounded batch reservations per worker
- lightweight per-instance, per-queue dispatch budgets
- local permits plus weighted overflow permits with derived contention
- drain timeout that can abandon jobs mid-drain so another instance must recover them

What is intentionally not modeled:

- SQL text / `SKIP LOCKED`
- LISTEN/NOTIFY wakeups
- Python bridge details
- real-time token bucket math for rate limiting
- unbounded retry histories; `AwaExtended` caps attempts so TLC can close the
  state graph
- full advisory-lock mechanics; leadership is an abstract exclusive token
- exact permit identity per task attempt; the model is job-centric and approximates task-held capacity closely enough to check the protocol invariants
- **priority ordering and starvation.** No model includes a `priority` variable.
  Priority is a scheduling heuristic, not a safety invariant — all formal
  invariants (NoDuplicateClaim, TaskLeaseBounded, RunningHasPermit, etc.)
  are independent of dispatch order. Cross-priority fairness is enforced by
  the maintenance leader's priority aging task (see ADR-005)
- **MVCC, vacuum, and storage reclamation.** The models do not attempt to
  represent Postgres heap cleanup, autovacuum timing, or horizon pinning from
  long-lived snapshots. Those are operational/performance concerns rather than
  protocol-safety invariants, and are covered by runtime benchmarks instead

## Files

- `AwaCore.tla` / `AwaCore.cfg`: focused model for rescue, admin cancel, and
  stale completion protection
- `storage/AwaSegmentedStorage.tla` / `storage/AwaSegmentedStorage.cfg`: focused
  segmented-storage model covering `ready_entries`, `deferred_entries`,
  `waiting_entries`, `active_leases`, optional `attempt_state`,
  `terminal_entries`, `dlq_entries`, queue-local append/claim cursors, and
  segment rotation/prune safety for all five families. Heartbeat freshness
  lives on `active_leases` (matching the Rust implementation) so short-job
  rescue is reachable. DLQ modelling covers both executor-side `FailToDlq`
  and admin-side `MoveFailedToDlq`, plus `RetryFromDlq` (with `run_lease`
  reset) and `PurgeDlq`. See [`storage/MAPPING.md`](storage/MAPPING.md) for
  the TLA+ ↔ Rust correspondence table.
- `storage/AwaSegmentedStorageInterleavings.cfg`: alternate two-worker config
  for the same segmented-storage spec, used to exercise stale completion and
  waiting/resume interleavings without changing the base safety model
- `storage/AwaSegmentedStorageRaces.tla` / `storage/AwaSegmentedStorageRaces.cfg`
  / `storage/AwaSegmentedStorageRacesSafe.cfg`: focused race-exposure spec
  that splits `Claim` into `BeginClaim` / `CommitClaim` to model the claim
  path's cursor-read-without-lock behaviour. The race config produces a
  counterexample trace (claim snapshots segment, rotate+prune fire, commit
  lands lease in pruned segment — simultaneously the claim-vs-rotate race
  and the prune check-then-act race). The safe config uses a checked
  commit and passes. Mitigated in production by `FOR SHARE` on
  `lease_ring_state` and `ACCESS EXCLUSIVE` on partition children — see
  `storage/MAPPING.md` for the full analysis.
- `storage/AwaStorageLockOrder.tla` / `storage/AwaStorageLockOrder.cfg` /
  `storage/AwaStorageLockOrderDeadlockDemo.cfg`: lock-ordering protocol
  spec. Models each storage-engine transaction (claim, rotate-leases,
  prune-leases, rotate-ready, prune-ready) as an ordered sequence of
  Postgres lock acquisitions with a shared/exclusive compatibility matrix.
  Checks `NoDeadlock` via a waits-for cycle detector. The main config
  passes cleanly (39,040 distinct states), confirming the current
  lock ordering is deadlock-free. The demo config uses a deliberately
  cycle-creating plan pair to prove the detector itself works.
- `storage/AwaSegmentedStorageTrace.tla` /
  `storage/AwaSegmentedStorageTrace.cfg` /
  `storage/AwaSegmentedStorageTraceBroken.cfg`: trace-validation
  harness. Takes a hand-transcribed sequence of events from a
  queue-storage runtime test and verifies each transition is a legal
  firing of the corresponding base spec action. The snooze trace
  (6 events) is accepted; a deliberately-broken variant trips deadlock
  at traceIdx = 2 to confirm the checker rejects invalid sequences.
- `AwaExtended.tla` / `AwaExtended.cfg`: multi-instance model for shutdown
  sequencing, split permit/claim/execute stages, leader failover, weighted
  overflow capacity, bounded batch behavior, abstract rate limiting, and
  post-timeout abandonment / recovery
- `AwaBatcher.tla` / `AwaBatcher.cfg` / `AwaBatcherLiveness.cfg`: completion
  batcher model verifying that the async batched completion path (handler →
  batcher buffer → DB flush) preserves lease-guarded finalization, at-most-once
  completion, and no-loss-on-shutdown, including the direct fallback path after
  batcher failure
- `AwaCbk.tla` / `AwaCbk.cfg` / `AwaCbkLiveness.cfg`: external callback
  resolution model for the three-way race between complete/fail, timeout rescue,
  and heartbeat rescue with Postgres row-lock semantics
- `AwaDispatchClaim.tla` / `AwaDispatchClaimOld.cfg` /
  `AwaDispatchClaimNew.cfg`: focused dispatcher-claim model; includes retry
  cycles so `attempt > 1` is exercised as a legitimate path; the old config
  intentionally fails the `NoDuplicateClaim` invariant and the new config
  re-checks availability at claim time — matching the production dispatch
  query's `WHERE state = 'available'` guard in both the subquery and the
  UPDATE
- `AwaViewTrigger.tla` / `AwaViewTrigger.cfg` / `AwaViewTriggerOld.cfg`:
  INSTEAD OF UPDATE trigger concurrency model for the `awa.jobs` UNION ALL
  view; the trigger implements UPDATE as DELETE + INSERT for cross-table
  moves, and the v006 fix adds a version check (state, run_lease,
  callback_id) on the DELETE so concurrent callers can't both succeed on
  state-changing operations; the old config models the v001 bug from #132
- `AwaCron.tla` / `AwaCron.cfg` / `AwaCronLiveness.cfg`: cron double-fire
  prevention under leader failover with CAS on `last_enqueued_at`
- `Dockerfile`: Docker-first TLC environment
- `run-tlc.sh`: convenience wrapper for running TLC from the repo root

## Running TLC

From the repository root:

```bash
./correctness/run-tlc.sh core/AwaCore.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla storage/AwaSegmentedStorageInterleavings.cfg
./correctness/run-tlc.sh storage/AwaSegmentedStorageRaces.tla storage/AwaSegmentedStorageRaces.cfg
./correctness/run-tlc.sh storage/AwaSegmentedStorageRaces.tla storage/AwaSegmentedStorageRacesSafe.cfg
./correctness/run-tlc.sh storage/AwaStorageLockOrder.tla
./correctness/run-tlc.sh storage/AwaStorageLockOrder.tla storage/AwaStorageLockOrderDeadlockDemo.cfg
./correctness/run-tlc.sh storage/AwaSegmentedStorageTrace.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorageTrace.tla storage/AwaSegmentedStorageTraceBroken.cfg
./correctness/run-tlc.sh core/AwaBatcher.tla
./correctness/run-tlc.sh core/AwaBatcher.tla core/AwaBatcherLiveness.cfg
./correctness/run-tlc.sh protocol/AwaExtended.tla
./correctness/run-tlc.sh races/AwaCbk.tla
./correctness/run-tlc.sh races/AwaCbk.tla races/AwaCbkLiveness.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimOld.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimNew.cfg
./correctness/run-tlc.sh races/AwaViewTrigger.tla
./correctness/run-tlc.sh races/AwaViewTrigger.tla races/AwaViewTriggerOld.cfg
./correctness/run-tlc.sh races/AwaCron.tla races/AwaCronLiveness.cfg
```

Or directly:

```bash
docker build -t awa-tlaplus -f correctness/Dockerfile correctness
docker run --rm -v "$PWD/correctness:/work" awa-tlaplus \
  -config /work/AwaExtended.cfg /work/AwaExtended.tla
```

## Model Notes

`AwaCore` is the smallest useful model. It now encodes a minimal lease-guarded
finalization protocol:

- `Claim` increments a durable `lease`
- `StartTask` snapshots that lease into `taskLease`
- `FinalizeAccepted` requires `taskLease = lease`
- `FinalizeRejected` models the late-completion cleanup path after rescue,
  cancel, or reclaim

That maps much more closely to the Rust `run_lease` guard than the older
owner-only core model.

Like the extended model, the core model bounds lease growth (`MaxLease == 2`)
so TLC explores a finite reclaim/finalize surface instead of an unbounded loop.

`AwaExtended` adds:

- `Instances = {"i1", "i2"}` with per-instance `inFlight`, `taskLease`,
  `cancelRequested`, `rateBudget`, and service lifecycles
- shared database-facing job state via `jobState`, `attempt`, `lease`, and `dbOwner`
- `dispatchersAlive`, `heartbeatAlive`, `maintenanceAlive`, and
  `shutdownPhase = "running" | "stop_claim" | "draining" | "stopped"`
- `permitHolder[j]` / `permitKind[j]` distinct from execution ownership
- `heartbeatFresh[j]` and `deadlineExpired[j]`
- `leader` as the abstract maintenance lease holder
- local permit floors via `MinWorkers`
- weighted overflow via `Weight`, `GlobalOverflow`, and derived queue contention
- `BatchMax` and per-instance `rateBudget[i][q]` as bounded abstractions of
  dispatcher batching and queue-level rate limiting
- `DeferredRowsIdle`, which captures the hot/deferred storage split by requiring
  `retryable` jobs to have no live owner, permit, or in-flight task
- `DrainTimeout(i)`, `Abandoned(j)`, and `RecoverableAbandoned(j)` so one
  instance can abandon a running or claimed attempt and another still-running
  instance must rescue it

To keep the state graph finite, `AwaExtended` bounds retries with
`MaxAttempts == 2`. Admin cancel remains covered in `AwaCore`; the extended
model is deliberately focused on the shutdown / rescue / permit / fairness
protocol rather than re-exploring the full cancel surface.

`AwaSegmentedStorage` focuses on the proposed storage split behind the
vacuum-aware runtime direction rather than the full worker lifecycle protocol.
It treats `ready_entries` as runnable queue records, `deferred_entries` as a
separate promoted family, `waiting_entries` as parked callback/timeout work,
`active_leases` as the narrow claim surface, `attempt_state` as an optional
per-attempt mutable row, and `terminal_entries` as reclaimable completion
history. It also models `lane_state` with explicit append/claim cursors and a
gap-skipping claim advance. The key invariants are that `attempt_state` can
only exist for leased or waiting jobs, waiting jobs hold no live lease,
deferred jobs hold no live runtime state, and ready/deferred/waiting/lease
segments cannot be pruned while they still own live rows.

`AwaBatcher` models the async completion path between handler return and DB
update. In the real system (`awa-worker/src/completion.rs`), completed jobs
are queued in a sharded in-memory buffer and flushed to the database in
batches of up to 512 every 1ms. This introduces a window where a job has
completed in the handler but not yet in the database — during which
maintenance can rescue the job and a new worker can re-claim it.

### Mapping to Rust code

| TLA+ variable | Rust equivalent |
|---------------|-----------------|
| `jobState`, `owner`, `lease` | `awa.jobs_hot` row: `state`, implicit owner, `run_lease` column |
| `taskLease[w][j]` | `ctx.job.run_lease` snapshot captured at claim time (`executor.rs`) |
| `handlerPhase[w][j]` | Executor control flow after `worker.perform()` returns |
| `batcherPending` | `CompletionBatcherWorker.pending: Vec<CompletionRequest>` (`completion.rs`) |
| `shutdownPhase` | `dispatch_cancel` → `service_cancel` → join sequence (`client.rs:720-765`) |
| `dbCompletions` | Ghost variable (model-only) for checking `AtMostOneCompletion` |

| TLA+ action | Rust code |
|-------------|-----------|
| `Claim` | `dispatcher.poll_once()` — `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED) ... SET state='running', run_lease=run_lease+1` |
| `HandlerComplete` | `completion_batcher.complete(job.id, job.run_lease)` (`executor.rs:364`) |
| `BatcherFlushSuccess` | Flush SQL: `UPDATE ... WHERE run_lease=$2 AND state='running'` (`completion.rs:150`) |
| `BatcherFlushStale` | Same SQL, `RETURNING` returns 0 rows (job rescued between enqueue and flush) |
| `BatcherFlushFail` | `pool.execute()` error → `Err` sent to handler via oneshot channel |
| `DirectComplete*` | `direct_complete_job()` fallback after batcher failure (`executor.rs:819`) |
| `DirectCompleteFail` | `direct_complete_job()` returns `Err` — job stays `running`, rescued by heartbeat |
| `Rescue` | Heartbeat/deadline rescue in `maintenance.rs` |
| `Promote` | `scheduled_jobs` → `jobs_hot` promotion CTE |
| `ResetHandler` | `in_flight.remove((job_id, run_lease))` after completion path finishes (`executor.rs:324`) |

### What it verifies

- The `run_lease` SQL guard prevents stale batcher flushes from overwriting a
  re-claimed job (`BatcherFlushStale`)
- When the batcher flush fails (DB connection error), the handler falls back
  to direct single-job completion, which also applies the lease guard
  (`DirectCompleteSuccess` / `DirectCompleteStale`)
- A job is DB-completed at most once regardless of path (`AtMostOneCompletion`)
- Shutdown drains all pending batcher requests before exiting — the
  `BatcherDrainStart` transition requires all `taskLease` values to be zero
  and all handlers to be in `idle` or `done` phase
- When both batch and direct completion fail (`DirectCompleteFail`), the
  handler exits cleanly ("done") and the job stays `running` in the DB,
  relying on heartbeat `Rescue` → `Promote` to retry
- Under fairness, every `pending` handler eventually reaches `done` or `idle`
  (`PendingEventuallyResolved`)

### Modeling note

The initial model run caught a sequencing issue: `BatcherDrainStart` originally
only required `handlerPhase ∈ {idle, done}`, but a handler could be `idle` with
`taskLease > 0` (claimed but handler not yet returned). Tightening the guard to
also require `taskLease = 0` matches the real system's `service_cancel` ordering
where it only fires after all `job_set` tasks complete (`client.rs:742-752`).

## Checked Invariants

`AwaCore.cfg` checks:

- `TypeOK`
- `RunningOwned`
- `NonRunningUnowned`
- `TaskLeaseBounded`

`AwaExtended.cfg` checks:

- `TypeOK`
- `RunningHasPermit`
- `DBOwnerRequiresRunning`
- `CurrentOwnerConsistent`
- `TaskLeaseBounded`
- `TerminalReleasesPermit`
- `DeferredRowsIdle`
- `LocalCapacitySafe`
- `OverflowCapacitySafe`
- `BatchBounded`
- `RateBudgetBounded`
- `NoClaimAfterStopClaim`
- `HeartbeatUntilDrained`
- `ServicePhaseConsistency`
- `LeaderConsistent`
- `StoppedInstancesQuiescent`

`AwaBatcher.cfg` checks:

- `TypeOK`
- `AtMostOneCompletion`
- `RunningOwned`
- `NonRunningUnowned`
- `PendingRequestHasValidLease`
- `ShutdownDrainedBatcher`
- `ShutdownHandlersDone`

`AwaBatcherLiveness.cfg` additionally checks:

- `PendingEventuallyResolved`

`AwaExtended.cfg` also checks:

- `I1DrainEventuallyStops`
- `I1Q1OverflowProgress`

`AwaExtended.tla` also defines `RecoverableAbandoned(j)` and
`AbandonedJobsEventuallyLeaveRunning`, but that liveness property is not
enabled in `AwaExtended.cfg`. In a finite two-instance model TLC can always
choose to shut the last surviving instance down as well, so eventual rescue
needs an extra environment assumption such as "some instance remains running".

## Mapping Back To The Rust Runtime

- `dbOwner[j]` corresponds to the worker attempt that can still satisfy the SQL
  `WHERE state = 'running' AND run_lease = ...` guard
- `inFlight[i][j]` and `taskLease[i][j]` approximate each instance's local
  executor registry keyed by `(job_id, run_lease)`; the Rust runtime currently
  implements this as a sharded local registry rather than a single global lock
- `jobState[j] = "retryable"` corresponds to the row living in
  `awa.scheduled_jobs`; the hot table only holds runnable / running / terminal
  rows
- `permitHolder[j]` is the reserved capacity backing the current claim or
  execution attempt for that job row
- `cancelRequested[i][j]` approximates the per-instance in-flight cancellation
  signal that a handler observes through `ctx.is_cancelled()`
- `shutdownPhase` plus the service booleans capture the intended ordering:
  stop dispatchers -> drain with heartbeat and maintenance alive -> either
  finish cleanly or time out and abandon
- `leader` approximates the maintenance leader selected by advisory lock

## Known Divergences

- The model shares only abstract database state and leadership between
  instances. It does not model actual SQL, `SKIP LOCKED`, or trigger-driven
  wakeups.
- Leadership is an abstract exclusive token, not the full advisory-lock
  protocol.
- Drain timeout is modeled as an explicit transition rather than wall-clock
  time.
- `AwaExtended` only models two instances, so abandonment liveness is checked
  only as protocol structure, not as an enabled TLC liveness property, because
  the model intentionally allows the entire cluster to shut down.
- Permit ownership is modeled at the job-row level. This is accurate enough for
  the checked invariants, but it is still an abstraction of the real Rust task
  handles and `DispatchPermit` lifetimes.

## Bugs the Models Did Not Catch

### Dispatcher stale-candidate double claim (v0.5.1-alpha.0, #134)

The hot-path dispatcher claim SQL selected candidate IDs from `awa.jobs_hot`,
then later locked and updated those rows to `running`. The locking/update step
did not re-check `state = 'available'`, so a row that was chosen while
available could still be claimed again after another worker had already moved it
to `running`.

In production terms the bad transition was:

1. worker A claims `available -> running`, incrementing `attempt` and
   `run_lease`
2. worker B still has the same row in its stale candidate set
3. worker B locks the row later and performs `running -> running`, incrementing
   `attempt` and `run_lease` again

That produced exactly the observed flake in the callback failover chaos test:
one attempt-1 handler lost ownership before `register_callback()`, the same job
was re-dispatched as attempt 2 and completed, and the test got stuck at
`{"waiting_external": 11, "completed": 1}` before failover even started.

**Why the TLA+ model missed it:**

1. **The abstraction boundary is above SQL candidate staleness.**
   `AwaExtended.tla` models `Reserve*` and `ClaimReserved` as acting on the
   current logical row state. `ClaimReserved(i, j)` requires `jobState[j] =
   "available"` at claim time, which is the behavior the SQL should have had.
   It does not model a two-phase SQL path of:
   `select candidate ids -> later lock row -> later update row` with the row
   state changing in between.

2. **The model assumed the claim step revalidated availability.**
   The checked invariants reason about the logical claim transition
   `available -> running`. The real bug was that the implementation violated
   that assumption by omitting the final `state = 'available'` recheck during
   the locked update.

3. **Generated traces already showed the shape once the abstraction was too
   weak.** Some historical `AwaExtended` trace artifacts contain
   `running -> running` / `attempt+1` transitions. Those traces are a sign that
   the model's reserve/claim split was permissive enough to allow the same bad
   shape, but the checked invariants did not explicitly forbid it.

**Fix:** The dispatcher SQL re-checks `state = 'available'` both in the
`FOR UPDATE SKIP LOCKED` subquery and in the outer `UPDATE ... WHERE` clause,
preventing a stale candidate from claiming a row that has already transitioned.

### Concurrent UPDATE race on the `awa.jobs` view (v0.5.1, #132)

The `awa.jobs` UNION ALL view's INSTEAD OF UPDATE trigger implemented UPDATE
as DELETE + INSERT. The DELETE matched only on `id`, so when two concurrent
callers (e.g., two `resume_external` calls) raced on the same callback_id:

1. Both callers found the row in the view (both saw `callback_id = X`)
2. Both entered the trigger with identical `OLD` records
3. Transaction A's DELETE succeeded; transaction B's DELETE (after A committed)
   found and deleted A's freshly re-inserted row
4. Both INSERTs succeeded — both callers observed success

This violated at-most-once callback resolution: two callers both returned
a `JobRow` for the same callback, with A's state change silently lost.

**Why the TLA+ models missed it:**

1. **Wrong abstraction boundary.** The models explicitly do not cover SQL
   text, trigger mechanics, or the `awa.jobs` compatibility view
   (see Known Divergences above). `AwaCbk` models a logical row with
   row-lock semantics — it assumes a plain Postgres UPDATE that blocks,
   re-evaluates, and returns 0 rows if the WHERE clause no longer matches.
   The DELETE+INSERT trigger with stale `OLD` inputs is not representable
   in that abstraction.

2. **Assumes row-level UPDATE atomicity.** In `AwaCbk.tla`, blocked
   callback operations wait on `rowLock`, then re-evaluate preconditions
   after the lock is released. This accurately models a normal row UPDATE.
   It does not model "executor picked a row from a UNION ALL view, passed
   a snapshot `OLD` to a trigger function, which then did a DELETE + INSERT
   carrying that stale snapshot."

3. **Does not track per-operation results.** The model checks `AtMostOnce
   Resolution` (at most one DB state transition), but this race can produce
   a plausible final DB state while still letting two callers observe success.
   That is an API linearizability bug. The model does not track per-caller
   return values (Success vs CallbackNotFound), so it had no way to detect
   the symptom.

**Fix (v006 migration):** The trigger's DELETE now checks `state`, `run_lease`,
and `callback_id` from `OLD`. After a concurrent transaction modifies the row,
the second caller's DELETE matches 0 rows → `RETURN NULL` → 0 rows in
`RETURNING` → `CallbackNotFound`. This restores the optimistic concurrency
semantics that the TLA+ model assumes.
