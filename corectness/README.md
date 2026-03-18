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
- shutdown phases
- cancellation flags exposed via `ctx.is_cancelled()`
- local permits plus weighted overflow permits

What is intentionally not modeled:

- SQL text / `SKIP LOCKED`
- LISTEN/NOTIFY wakeups
- Python bridge details
- real-time token bucket math for rate limiting

## Files

- `AwaCore.tla` / `AwaCore.cfg`: focused model for rescue, admin cancel, and
  stale completion protection
- `AwaExtended.tla` / `AwaExtended.cfg`: extends the protocol with shutdown
  phases, heartbeat sequencing, and weighted overflow capacity
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

- `shutdownPhase = "running" | "stop_claim" | "draining" | "stopped"`
- `heartbeatMode = "alive" | "stopped"`
- local permit floors via `MinWorkers`
- weighted overflow via `Weight`, `GlobalOverflow`, and per-queue `demand`

The weighted allocator is modeled as a transition rule, not a full liveness
proof. The model checks safety properties such as capacity bounds and
permit-before-claim. It does not attempt to prove strong fairness or real-time
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
- `HeartbeatUntilDrained`

`NoClaimAfterStopClaim` is encoded in the transition relation: the claim actions
require `shutdownPhase = "running"`.

## Mapping Back To The Rust Runtime

- `owner[j]` corresponds to the worker attempt allowed to call
  `complete_job()`
- `cancelFlag[w]` corresponds to what `ctx.is_cancelled()` should observe
- `permitKind[j]` mirrors whether a running job consumed local or overflow
  capacity
- `shutdownPhase` captures the intended ordering:
  stop claims -> drain jobs -> stop heartbeat
