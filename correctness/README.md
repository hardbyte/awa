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

## Files

- `AwaCore.tla` / `AwaCore.cfg`: focused model for rescue, admin cancel, and
  stale completion protection
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
- `AwaCron.tla` / `AwaCron.cfg` / `AwaCronLiveness.cfg`: cron double-fire
  prevention under leader failover with CAS on `last_enqueued_at`
- `Dockerfile`: Docker-first TLC environment
- `run-tlc.sh`: convenience wrapper for running TLC from the repo root

## Running TLC

From the repository root:

```bash
./correctness/run-tlc.sh AwaCore.tla
./correctness/run-tlc.sh AwaExtended.tla
./correctness/run-tlc.sh AwaBatcher.tla
./correctness/run-tlc.sh AwaBatcher.tla AwaBatcherLiveness.cfg
./correctness/run-tlc.sh AwaCbk.tla
./correctness/run-tlc.sh AwaCbk.tla AwaCbkLiveness.cfg
./correctness/run-tlc.sh AwaCron.tla AwaCronLiveness.cfg
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
| `Claim` | `dispatcher.claim_jobs()` — `UPDATE SET state='running', run_lease=run_lease+1` |
| `HandlerComplete` | `completion_batcher.complete(job.id, job.run_lease)` (`executor.rs:364`) |
| `BatcherFlushSuccess` | Flush SQL: `UPDATE ... WHERE run_lease=$2 AND state='running'` (`completion.rs:150`) |
| `BatcherFlushStale` | Same SQL, `RETURNING` returns 0 rows (job rescued between enqueue and flush) |
| `BatcherFlushFail` | `pool.execute()` error → `Err` sent to handler via oneshot channel |
| `DirectComplete*` | `direct_complete_job()` fallback after batcher failure (`executor.rs:819`) |
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
