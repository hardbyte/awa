# Awa Correctness Models

This directory intentionally uses the requested `corectness/` spelling.

It contains small TLA+ models for the coordination protocol behind the worker
runtime. The goal is to check the concurrency invariants that are easy to miss
with integration tests:

- a rescued or cancelled job cannot be finalized by a stale worker
- graceful shutdown stops new claims before drain and keeps heartbeat alive
- a running job always has reserved capacity
- weighted overflow never exceeds the global cap

What is modeled:

- abstract job states
- worker ownership of a running attempt
- reservation vs claim vs execution as separate protocol stages
- service lifecycles for dispatchers, heartbeat, and maintenance
- attempt-scoped cancellation signals
- local permits plus weighted overflow permits with derived contention

What is intentionally not modeled:

- SQL text / `SKIP LOCKED`
- LISTEN/NOTIFY wakeups
- Python bridge details
- real-time token bucket math for rate limiting

## Files

- `AwaCore.tla` / `AwaCore.cfg`: focused model for rescue, admin cancel, and
  stale completion protection
- `AwaExtended.tla` / `AwaExtended.cfg`: extends the protocol with shutdown
  sequencing, split permit/claim/execute stages, and weighted overflow capacity
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

- `dispatchersAlive`, `heartbeatAlive`, and `maintenanceAlive`
- `shutdownPhase = "running" | "stop_claim" | "draining" | "stopped"`
- `permitHolder[j]` distinct from `owner[j]`
- `cancelRequested[j]` as an attempt-scoped cancellation signal
- local permit floors via `MinWorkers`
- weighted overflow via `Weight`, `GlobalOverflow`, and derived queue contention

The weighted allocator is still modeled as a safety-oriented transition rule,
not a full fairness proof. The model now derives overflow contention from
backlog plus local saturation instead of treating demand as an unconstrained
input action. It still does not attempt to prove strong fairness or real-time
throughput behavior.

## Checked Invariants

`AwaCore.cfg` checks:

- `TypeOK`
- `RunningOwned`
- `NonRunningUnowned`

`AwaExtended.cfg` checks:

- `TypeOK`
- `RunningOwned`
- `NonRunningUnowned`
- `RunningHasPermit`
- `NonRunningHasNoPermit`
- `LocalCapacitySafe`
- `OverflowCapacitySafe`
- `NoClaimAfterStopClaim`
- `HeartbeatUntilDrained`
- `ServicePhaseConsistency`
- `ExecutingHasOwner`

## Mapping Back To The Rust Runtime

- `owner[j]` corresponds to the worker attempt allowed to call
  `complete_job()`
- `permitHolder[j]` is the reserved capacity backing a claim or execution
- `cancelRequested[j]` approximates the in-flight cancellation signal that a
  handler observes through `ctx.is_cancelled()`
- `shutdownPhase` plus the service booleans capture the intended ordering:
  stop dispatchers and maintenance -> drain with heartbeat alive ->
  stop heartbeat only after running jobs are gone
