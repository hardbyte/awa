# Protocol Models

These models cover larger runtime protocols that involve multiple services or instances.

## Files

- `AwaExtended.tla` / `AwaExtended.cfg` Covers two-instance shutdown sequencing, split reserve/claim/execute stages, leader failover, permits, weighted overflow, rescue, abandonment, and bounded rate limiting.

- `AwaExtendedHungHandlers.cfg` / `AwaExtendedHungHandlersNoDrop.cfg` One-job configs under `HungHandlerSpec`, where a rescued handler may never return. The first checks the worker gives the slot back by dropping such a handler (#546); the second turns dropping off and is the expected `RescuedSlotsReleased` counterexample.

## When To Use

- use `AwaExtended` for shutdown, rescue, leader failover, and permit protocol questions
- do not use it for raw SQL or low-level `SKIP LOCKED` races; those belong in a smaller focused model under `races/`

## Command

```bash
./correctness/run-tlc.sh protocol/AwaExtended.tla
```
