# Awa Correctness Models

This directory intentionally uses the requested `corectness/` spelling.

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
- `Dockerfile`: Docker-first TLC environment
- `run-tlc.sh`: convenience wrapper for running TLC from the repo root

## Running TLC

From the repository root:

```bash
./corectness/run-tlc.sh AwaCore.tla
./corectness/run-tlc.sh AwaExtended.tla
```

Or directly:

```bash
docker build -t awa-tlaplus -f corectness/Dockerfile corectness
docker run --rm -v "$PWD/corectness:/work" awa-tlaplus \
  -config /work/AwaExtended.cfg /work/AwaExtended.tla
```

## Model Notes

`AwaCore` is the smallest useful model. It encodes the ownership rule that
fixes the v0.1 stale-completion bug: only the current owner of a `running` job
may finalize it. Rescue and admin cancel clear ownership, so any late worker
completion becomes impossible in the model.

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
- `DrainTimeout(i)`, `Abandoned(j)`, and `RecoverableAbandoned(j)` so one
  instance can abandon a running or claimed attempt and another still-running
  instance must rescue it

To keep the state graph finite, `AwaExtended` bounds retries with
`MaxAttempts == 2`. Admin cancel remains covered in `AwaCore`; the extended
model is deliberately focused on the shutdown / rescue / permit / fairness
protocol rather than re-exploring the full cancel surface.

## Checked Invariants

`AwaCore.cfg` checks:

- `TypeOK`
- `RunningOwned`
- `NonRunningUnowned`

`AwaExtended.cfg` checks:

- `TypeOK`
- `RunningHasPermit`
- `DBOwnerRequiresRunning`
- `CurrentOwnerConsistent`
- `TaskLeaseBounded`
- `TerminalReleasesPermit`
- `LocalCapacitySafe`
- `OverflowCapacitySafe`
- `BatchBounded`
- `RateBudgetBounded`
- `NoClaimAfterStopClaim`
- `HeartbeatUntilDrained`
- `ServicePhaseConsistency`
- `LeaderConsistent`
- `StoppedInstancesQuiescent`

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
  `WHERE state = 'running'` guard
- `inFlight[i][j]` and `taskLease[i][j]` approximate each instance's local
  executor registry
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
