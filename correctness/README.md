# Awa Correctness Models

This directory contains the TLA+ models used to check concurrency and protocol
invariants in Awa.

## Layout

- `core/`: smallest safety-focused models for lease guards, rescue, admin
  cancel, and completion batching
- `protocol/`: larger protocol models for shutdown, permits, rescue, and
  leader failover
- `races/`: focused models for specific bug classes and tricky interleavings
- `Dockerfile`: Docker image for TLC
- `run-tlc.sh`: convenience wrapper for running TLC from the repo root

## Running TLC

From the repository root:

```bash
./correctness/run-tlc.sh core/AwaCore.tla
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
  -config /work/protocol/AwaExtended.cfg /work/protocol/AwaExtended.tla
```

## Choosing A Model

- start with `core/AwaCore.tla` for lease-guard and stale-finalization safety
- use `protocol/AwaExtended.tla` for shutdown, permit, rescue, and failover
  protocol questions
- use `races/` for feature-specific or bug-specific interleavings that sit below
  the abstraction boundary of the larger protocol models

## Notes

- The larger models intentionally abstract above raw SQL text, `SKIP LOCKED`,
  and trigger mechanics.
- When a bug lives below that boundary, prefer adding a small focused race
  model rather than forcing the entire SQL detail into `AwaExtended`.

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
| `Claim` | `dispatcher.claim_jobs()` — `UPDATE SET state='running', run_lease=run_lease+1` |
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

**Fix:** The dispatcher SQL now re-checks `jobs.state = 'available'` both in the
locked `claimed` CTE and in the final `UPDATE ... WHERE` clause. This restores
the claim semantics that `AwaExtended` was already intending to model.

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
