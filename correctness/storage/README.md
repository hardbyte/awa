# Segmented Storage Model

`AwaSegmentedStorage` is a focused TLA+ model for the proposed 0.6+ segmented
runtime storage layout.

It uses the naming set discussed for the redesign:

- `ready_entries`
- `deferred_entries`
- `waiting_entries`
- `active_leases`
- `attempt_state`
- `terminal_entries`
- `dlq_entries`
- `lane_state`
- `ready_segments` / `ready_segment_cursor`
- `deferred_segments` / `deferred_segment_cursor`
- `waiting_segments` / `waiting_segment_cursor`
- `lease_segments` / `lease_segment_cursor`
- `dlq_segments` / `dlq_segment_cursor`

What it models:

- append-only enqueue into `ready_entries`
- enqueue into `deferred_entries` plus promotion back to `ready_entries`
- parking into `waiting_entries` plus callback/timeout resume back to `ready_entries`
- callback-timeout rescue on exhausted attempts landing directly in `dlq_entries`
- claim into `active_leases`
- explicit `lane_state` append/claim cursors with gap-skipping claim advancement
- lazy materialization of `attempt_state`
- short-job completion without `attempt_state`
- stateful completion after heartbeat/progress/callback activity
- retry flow back into `deferred_entries`
- rescue flow that re-enqueues at the tail of `ready_entries`
- executor-side terminal failure routed directly into `dlq_entries`
- admin-initiated move from `terminal_entries` to `dlq_entries`
- `retry_from_dlq` round trip back to `ready_entries` with `run_lease` reset to 0
- admin purge of DLQ rows
- stale completion rejection via per-worker lease snapshots
- segment rotation and prune safety for ready, deferred, waiting, lease, **and dlq** segment families
- a second config with two workers to exercise interleavings on the same storage invariants

Heartbeat freshness is tracked at the `active_leases` level (not
`attempt_state`) to match the Rust implementation. Short jobs that claim and
lose their heartbeat before materialising `attempt_state` are rescuable by
the model — confirmed by TLC coverage showing `RescueToReady` firing in
the single-worker config.

Key safety checks include:

- waiting jobs hold no active lease
- deferred, waiting, and DLQ jobs have no runnable `laneSeq`
- `attempt_state` only exists for leased or waiting jobs
- claim cursors never move behind live runnable rows
- DLQ rows hold no live runtime (no lease, attempt_state, waiting entry, etc.)
- `dlq_entries` and `terminal_entries` are disjoint (a job is in exactly one
  terminal family at a time)
- pruned DLQ segments hold no live DLQ rows (mirror of the other family
  prune-safety invariants)

What it intentionally does not model:

- SQL planner behavior
- MVCC horizons or autovacuum timing
- queue priorities and fairness
- unique-claim key semantics; the Rust `retry_from_dlq` unique-conflict
  contract (see `awa-model/src/dlq.rs::retry_from_dlq`) is enforced at the
  SQL layer via `release_queue_storage_unique_claim` and is out of scope here
- liveness/fairness properties; this spec is still safety-oriented

Run it with:

```bash
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla storage/AwaSegmentedStorageInterleavings.cfg
```

The checked-in configs are intentionally small so TLC completes quickly in CI-like
environments:

- `AwaSegmentedStorage.cfg`: 1 job, 1 worker — ~79k distinct states, ~2s
- `AwaSegmentedStorageInterleavings.cfg`: 1 job, 2 workers — ~170k distinct states, ~5s

That is enough to exercise waiting/resume, stale completion rejection, retry,
rescue (including short-job rescue), DLQ round-trip, and queue-family prune
safety, but not multi-job fairness or priority-aging liveness.

## DLQ coverage

Action coverage from a `-coverage 1` run of the base config confirms each
new DLQ transition is reached:

| Action | States |
|---|---:|
| `FailToDlq` | 41,472 |
| `RotateDlqSegments` | 38,144 |
| `PruneDlqSegment` | 39,680 |
| `TimeoutWaitingToDlq` | 9,216 |
| `RescueToReady` | 6,912 |
| `PurgeDlq` | 4,608 |
| `MoveFailedToDlq` | 3,072 |
| `RetryFromDlq` | 1,536 |

`RescueToReady` firing in a single-worker config confirms the heartbeat-fix
path (short jobs, no `attempt_state`) is reachable — the previous spec's
`FreshHeartbeatRequiresAttemptState` subset constraint had silently excluded
that path from exploration.

## Mapping to Rust code

See [`MAPPING.md`](./MAPPING.md) for the action-by-action correspondence
between TLA+ transitions and the Rust implementation, including the SQL
statements that enforce each guard.

## Race-exposure companion spec

[`AwaSegmentedStorageRaces.tla`](./AwaSegmentedStorageRaces.tla) refines the
Claim action into a two-step `BeginClaim(w, j)` / `CommitClaim(w)` with a
per-worker `claimIntent` snapshot, and enables `RotateLeaseSegments` /
`PruneLeaseSegment` to fire between the steps. Two configs:

- [`AwaSegmentedStorageRaces.cfg`](./AwaSegmentedStorageRaces.cfg):
  naive `CommitClaim` (uses the snapshotted segment without re-check).
  **TLC finds `PrunedLeaseSegmentsAreEmpty` violated in 6 steps** — the
  trace shows Init → EnqueueReady → BeginClaim (snapshot seg 1) →
  RotateLeaseSegments (seg 1 → sealed) → PruneLeaseSegment(1) (seg 1 →
  pruned, precondition passes because the pending claim isn't committed
  yet so `activeLeases = {}`) → CommitClaim lands a lease in segment 1,
  now pruned. This is simultaneously the claim-vs-rotate race and the
  prune check-then-act race.
- [`AwaSegmentedStorageRacesSafe.cfg`](./AwaSegmentedStorageRacesSafe.cfg):
  `CommitClaimChecked` re-reads `leaseSegments[leaseSeg] = "open"` at
  commit time. TLC completes 22 distinct states with no violations.

Run either with `./correctness/run-tlc.sh`. The race-exposing config is
expected to produce a counterexample; the safe config is expected to pass.

What this proves: the race my code review flagged is real at the spec's
abstraction level, and it is sufficient for the Rust claim path to
re-check the lease segment state under the `queue_lanes` row lock before
inserting the lease row. Either re-reading `lease_ring_state` under the
lock or share-locking it across the insert would satisfy the safe
refinement.

What this does not prove: whether the real Rust/SQL implementation
actually has the race. That requires inspecting `claim_ready_runtime` at
`awa-model/src/queue_storage.rs:1647` to confirm whether the cursor is
re-read or share-locked. This model simply says: if the implementation
does not do that, the race is observable.

## Known modelling gaps

See [`../README.md`](../README.md) for the full Known Divergences list.
Specific to this spec:
- **DLQ bulk ops are modelled as quantified single-row actions.** The Rust
  `bulk_retry_from_dlq` / `purge_dlq` paths are transactionally atomic across
  the matching rows; the spec explores arbitrary interleavings of individual
  `RetryFromDlq(j)` / `PurgeDlq(j)` which is a strictly weaker claim. Safety
  invariants hold under both framings; a refinement with a `BulkScope` set
  variable could tighten this.
- **Unique-claim conflicts on `retry_from_dlq`.** The Rust contract says
  retry-from-dlq returns `UniqueConflict` and leaves the DLQ row intact when
  a replacement owns the unique slot. The model doesn't have unique keys, so
  TLC simply explores `RetryFromDlq(j)` whenever the precondition holds.
  Preserving-the-DLQ-row-on-conflict is enforced in SQL
  (`awa-model/src/queue_storage.rs::sync_unique_claim`).
