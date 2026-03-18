# Awa Correctness Models

This directory intentionally uses the requested `corectness/` spelling.

It contains small TLA+ models for the coordination protocol behind the worker
runtime. The goal is to check the concurrency invariants that are easy to miss
with integration tests:

- a rescued or cancelled job cannot be finalized by a stale worker
- graceful shutdown stops new claims before drain and keeps heartbeat alive
- a running job always has reserved capacity
- weighted overflow never exceeds the global cap
- a contending queue eventually receives overflow capacity under the modeled
  fairness assumptions

What is modeled:

- abstract job states
- explicit attempt / lease identity for running attempts
- worker ownership plus a separate in-flight task registry
- reservation vs claim vs execution as separate protocol stages
- service lifecycles for dispatchers, heartbeat, and maintenance
- abstract heartbeat freshness and deadline expiry
- guarded finalize rejection for stale completions
- bounded batch reservations per worker
- lightweight per-queue dispatch budgets
- local permits plus weighted overflow permits with derived contention

What is intentionally not modeled:

- SQL text / `SKIP LOCKED`
- LISTEN/NOTIFY wakeups
- Python bridge details
- real-time token bucket math for rate limiting
- unbounded retry histories; `AwaExtended` caps attempts so TLC can close the
  state graph

## Files

- `AwaCore.tla` / `AwaCore.cfg`: focused model for rescue, admin cancel, and
  stale completion protection
- `AwaExtended.tla` / `AwaExtended.cfg`: extends the protocol with shutdown
  sequencing, split permit/claim/execute stages, weighted overflow capacity,
  bounded batch behavior, and abstract rate limiting
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

- `attempt[j]`, `lease[j]`, and `taskLease[j]`
- `dispatchersAlive`, `heartbeatAlive`, and `maintenanceAlive`
- `shutdownPhase = "running" | "stop_claim" | "draining" | "stopped"`
- `dbOwner[j]`, `taskWorker[j]`, and `inFlight[j]`
- `permitHolder[j]` / `permitKind[j]` distinct from execution ownership
- `cancelRequested[j]` as an attempt-scoped cancellation signal
- `heartbeatFresh[j]` and `deadlineExpired[j]`
- local permit floors via `MinWorkers`
- weighted overflow via `Weight`, `GlobalOverflow`, and derived queue contention
- `BatchMax` and `rateBudget[q]` as bounded abstractions of dispatcher batching
  and queue-level rate limiting

To keep the state graph finite, `AwaExtended` bounds retries with
`MaxAttempts == 2`. Admin cancel remains covered in `AwaCore`; the extended
model is deliberately focused on the shutdown / rescue / permit / fairness
protocol rather than re-exploring the full cancel surface. It also simplifies
shutdown by stopping `maintenanceAlive` at `ShutdownBegin`; that is narrower
than the Rust implementation, where maintenance stays up through drain.

## Checked Invariants

`AwaCore.cfg` checks:

- `TypeOK`
- `RunningOwned`
- `NonRunningUnowned`

`AwaExtended.cfg` checks:

- `TypeOK`
- `RunningHasPermit`
- `InFlightMatchesTaskWorker`
- `InFlightStateConsistent`
- `CurrentOwnerConsistent`
- `AttemptAndLeaseMonotone`
- `TerminalReleasesPermit`
- `LocalCapacitySafe`
- `OverflowCapacitySafe`
- `BatchBounded`
- `RateBudgetBounded`
- `NoClaimAfterStopClaim`
- `HeartbeatUntilDrained`
- `ServicePhaseConsistency`

`AwaExtended.cfg` also checks:

- `DrainEventuallyStops`
- `Q2OverflowProgress`

## Mapping Back To The Rust Runtime

- `owner[j]` corresponds to the worker attempt allowed to call
  `complete_job()`
- `dbOwner[j]` corresponds to the worker attempt that can still satisfy the SQL
  `WHERE state = 'running'` guard
- `taskWorker[j]` / `inFlight[j]` approximate the local executor registry
- `permitHolder[j]` is the reserved capacity backing a claim or execution
- `cancelRequested[j]` approximates the in-flight cancellation signal that a
  handler observes through `ctx.is_cancelled()`
- `shutdownPhase` plus the service booleans capture the intended ordering:
  stop dispatchers and modeled maintenance -> drain with heartbeat alive ->
  stop heartbeat only after running jobs are gone
