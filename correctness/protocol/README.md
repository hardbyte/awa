# Protocol Models

These models cover larger runtime protocols that involve multiple services or
instances.

## Files

- `AwaExtended.tla` / `AwaExtended.cfg`
  Covers two-instance shutdown sequencing, split reserve/claim/execute stages,
  leader failover, permits, weighted overflow, rescue, abandonment, and bounded
  rate limiting.

## When To Use

- use `AwaExtended` for shutdown, rescue, leader failover, and permit protocol
  questions
- do not use it for raw SQL or low-level `SKIP LOCKED` races; those belong in a
  smaller focused model under `races/`

## Command

```bash
./correctness/run-tlc.sh protocol/AwaExtended.tla
```
