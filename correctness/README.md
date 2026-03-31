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
