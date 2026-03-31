# Core Models

These are the smallest safety-oriented models.

## Files

- `AwaCore.tla` / `AwaCore.cfg`
  Covers lease-guarded finalization, rescue, admin cancel, and stale completion
  rejection.
- `AwaBatcher.tla` / `AwaBatcher.cfg` / `AwaBatcherLiveness.cfg`
  Covers the async completion batching path, stale flush rejection, fallback
  direct completion, and shutdown drain.

## When To Use

- use `AwaCore` for questions about `run_lease` and stale workers
- use `AwaBatcher` for bugs between handler completion and durable DB update

## Commands

```bash
./correctness/run-tlc.sh core/AwaCore.tla
./correctness/run-tlc.sh core/AwaBatcher.tla
./correctness/run-tlc.sh core/AwaBatcher.tla core/AwaBatcherLiveness.cfg
```
