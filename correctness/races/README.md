# Race Models

These models cover feature-specific or bug-specific interleavings that are too
low-level or too specialized for the larger protocol model.

## Files

- `AwaCbk.tla` / `AwaCbk.cfg` / `AwaCbkLiveness.cfg`
  Covers callback completion/fail, timeout rescue, heartbeat rescue, and stale
  token rejection.
- `AwaCron.tla` / `AwaCron.cfg` / `AwaCronLiveness.cfg`
  Covers cron double-fire prevention under leader failover.
- `AwaDispatchClaim.tla` / `AwaDispatchClaimOld.cfg` /
  `AwaDispatchClaimNew.cfg`
  Focused proof for issue #134. The old config reproduces the stale-candidate
  double-claim; the new config models the availability re-check and passes.

## When To Use

- use these models when the bug sits below the abstraction boundary of
  `AwaExtended`
- prefer adding a small focused model here over expanding the state space of a
  larger model unless the behavior is truly protocol-wide

## Commands

```bash
./correctness/run-tlc.sh races/AwaCbk.tla
./correctness/run-tlc.sh races/AwaCbk.tla races/AwaCbkLiveness.cfg
./correctness/run-tlc.sh races/AwaCron.tla races/AwaCronLiveness.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimOld.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimNew.cfg
```
