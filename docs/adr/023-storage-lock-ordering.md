# ADR-023: Storage Lock-Ordering Protocol

## Status

Accepted

## Context

ADR-019 introduced rotating segments for ready / deferred / waiting / lease /
terminal / dlq families, each with its own cursor row and partition children.
The claim path reads the lease-segment cursor, inserts a lease row, and
updates `queue_lanes`; the rotate path mutates the cursor; the prune path
empties a sealed partition. Without careful locking, a claim that snapshotted
the cursor before a rotate could land a lease in a segment that the same
transaction subsequently observes as pruned.

The first segmented-storage TLA+ model (`AwaSegmentedStorageRaces.tla`)
confirmed this: a naive two-phase claim combined with rotate + prune
produces a counterexample in six steps where the final lease lands in a
pruned partition.

## Decision

Protect the claim / rotate / prune interaction with Postgres row and table
locks, with a strict acquisition order and compatibility contract:

1. **Claim** takes:
   - `FOR UPDATE` on the `(queue, priority)` row of `{schema}.queue_lanes`
     (per-lane serialisation)
   - `FOR SHARE` on the `{schema}.lease_ring_state` row (held across both
     the cursor read and the lease insert)
   - `INSERT` (row-exclusive) into `{schema}.leases`

2. **Rotate leases** takes:
   - `FOR UPDATE` on `{schema}.lease_ring_state` (incompatible with claim's
     `FOR SHARE`, so rotate waits for in-flight claims to commit)

3. **Prune leases** takes:
   - `FOR UPDATE` on `{schema}.lease_ring_state`
   - `FOR UPDATE` on the target row of `{schema}.lease_ring_slots`
   - `LOCK TABLE {schema}.leases_<slot> IN ACCESS EXCLUSIVE MODE`
   - count leases inside the same transaction before `TRUNCATE`
   - `SET LOCAL lock_timeout = '50ms'` so prune aborts gracefully under
     contention rather than stalling the maintenance loop

The analogous protocol applies to the ready / done partitions via
`queue_ring_state`, `queue_ring_slots`, and the ready-child `ACCESS
EXCLUSIVE` lock.

## Consequences

### Positive

- Claims never land in a sealed or pruned partition because rotate waits
  for `FOR SHARE` to clear before advancing the cursor.
- Prune is guaranteed to see every in-flight lease because the
  active-leases count runs after `ACCESS EXCLUSIVE` is held.
- Maintenance loops don't stall on long-running claims; `lock_timeout`
  forces a graceful abort and the next tick retries.
- The lock-ordering protocol is checkable via `AwaStorageLockOrder.tla`,
  which runs a waits-for cycle detector across a shared/exclusive lock
  matrix. The main spec passes; a deliberately cycle-creating demo
  config confirms the detector works.

### Negative

- Four rows/tables per storage family acquire locks in a fixed order; a
  future refactor that reorders them risks introducing deadlock. The
  TLA+ spec is the regression harness here.
- `FOR SHARE` across a statement means claims cannot interleave with
  rotate; in practice rotate runs at most every few seconds so this is
  not a throughput concern.

## Relationship to ADR-019

This ADR captures a decision that is implicit in ADR-019's implementation
but was never written up separately. ADR-019 describes the segmented
layout; this ADR describes the lock contract that makes claim / rotate /
prune race-free on that layout. Both TLA+ artifacts referenced above
(`AwaSegmentedStorageRaces`, `AwaStorageLockOrder`) are part of the
ADR-019 correctness story; see
[correctness/storage/MAPPING.md](../../correctness/storage/MAPPING.md)
for the TLA+-action-to-Rust-function correspondence.
