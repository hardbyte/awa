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
- `terminal_segments` / `terminal_segment_cursor`

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
- segment rotation and prune safety for ready, deferred, waiting, lease, dlq, **and terminal** segment families (retention by partition rotation rather than row-by-row cleanup, per ADR-019)
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
- pruned terminal segments hold no live terminal rows; terminal rotation
  is deadlock-free and respects the same open/sealed/pruned lifecycle as
  the other families

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

- `AwaSegmentedStorage.cfg`: 1 job, 1 worker — ~324k distinct states, ~10s
- `AwaSegmentedStorageInterleavings.cfg`: 1 job, 2 workers — ~688k distinct states, ~24s

That is enough to exercise waiting/resume, stale completion rejection, retry,
rescue (including short-job rescue), DLQ round-trip, terminal-family rotation
and prune, and queue-family prune safety, but not multi-job fairness or
priority-aging liveness.

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

## Terminal family coverage

The terminal family mirrors the DLQ family's segment lifecycle. From the
same `-coverage 1` run:

| Action | States |
|---|---:|
| `FastComplete` (now tags `terminalSegmentOf`) | 55,296 |
| `StatefulComplete` (now tags `terminalSegmentOf`) | 110,592 |
| `CancelWaitingToTerminal` (now tags `terminalSegmentOf`) | 36,864 |
| `MoveFailedToDlq` (now clears `terminalSegmentOf`) | 18,432 |
| `RotateTerminalSegments` | 158,720 |
| `PruneTerminalSegment` | 158,720 |

This removes the previous gap where `terminal_entries` was the only
"monotonic-growing" set in the spec — it now shares the rotate/prune
reclaim story with the other families, matching the ADR-019 retention
model.

## Mapping to Rust code

See [`MAPPING.md`](./MAPPING.md) for the action-by-action correspondence
between TLA+ transitions and the Rust implementation, including the SQL
statements that enforce each guard.

## Lock-order companion spec

[`AwaStorageLockOrder.tla`](./AwaStorageLockOrder.tla) models each
storage-engine transaction (claim, rotate-leases, prune-leases,
rotate-ready, prune-ready) as an ordered sequence of Postgres lock
acquisitions, with a simplified shared/exclusive compatibility matrix
that captures the cases relevant to deadlock analysis. Invariants:

- `NoDeadlock`: the waits-for graph is acyclic
- `LockCompatibility`: no two incompatible locks on the same resource
- `HeldOnlyByRunningTxs`: committed transactions hold no locks
- `NoGlobalStall`: there is always some running transaction that can
  make progress (a stall-free-safety-check stand-in for liveness)

Configs:

- [`AwaStorageLockOrder.cfg`](./AwaStorageLockOrder.cfg): main run
  against the real Rust lock plans — **2,076 distinct states, clean**.
  This is the positive artifact saying the current SQL lock ordering
  is deadlock-free and the lock compatibility contract holds.
- [`AwaStorageLockOrderDeadlockDemo.cfg`](./AwaStorageLockOrderDeadlockDemo.cfg):
  sanity harness using a deliberately cycle-creating pair of plans —
  **NoDeadlock tripped in 5 steps** (confirms the checker works).

Run:

```bash
./correctness/run-tlc.sh storage/AwaStorageLockOrder.tla
./correctness/run-tlc.sh storage/AwaStorageLockOrder.tla storage/AwaStorageLockOrderDeadlockDemo.cfg
```

Coverage note: the plans model the lock steps that actually appear in
the Rust SQL (`FOR UPDATE` / `FOR SHARE` / `LOCK TABLE ACCESS
EXCLUSIVE` / the implicit AccessShare of SELECT on partition
children). They do NOT model implicit table-level locks beyond what
is named, or Postgres's lock-timeout / deadlock-detector abort choice.
The spec treats a waits-for cycle as a safety violation, which is
conservative — Postgres would abort one transaction and let the other
proceed. For our purposes "this sequence of lock requests could
produce a cycle" is the thing we want to catch, regardless of how
the runtime resolves it.

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
abstraction level. Either re-reading `lease_ring_state` under a lock or
share-locking it across the insert would satisfy the safe refinement.

**Status in the Rust implementation: mitigated.** `claim_ready_runtime`
(`awa-model/src/queue_storage.rs:1740-1746`) takes `FOR SHARE` on
`lease_ring_state` across both the cursor read and the lease insert.
`rotate_leases` (line 6194) and `prune_oldest_leases` (line 6398) take
`FOR UPDATE` on the same row, which is incompatible with `FOR SHARE` —
so they wait for in-flight claims to commit. `prune_oldest` for ready
segments additionally takes `ACCESS EXCLUSIVE` on the ready partition
and counts leases inside the prune transaction. The race spec thus
functions as a regression harness: if any of those locks are weakened,
the race re-appears. See [`MAPPING.md`](./MAPPING.md) for the full
lock-interaction analysis.

## Trace-validation harness

[`AwaSegmentedStorageTrace.tla`](./AwaSegmentedStorageTrace.tla)
extends the base spec with a replay harness that verifies a concrete
sequence of events transcribed from a real queue-storage runtime
test is accepted by the spec. The current checked-in traces:

- `SnoozeTrace` (6 events: enqueue → claim → retry-to-deferred →
  promote → claim → fast-complete) — accepted cleanly, 7 states.
  Transcribed from `test_queue_storage_runtime_snooze`.
- `BrokenTrace` (same events with steps 3 and 4 swapped) —
  rejected with a deadlock at traceIdx = 2, proving the harness
  catches invalid sequences.

Run:

```bash
./correctness/run-tlc.sh storage/AwaSegmentedStorageTrace.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorageTrace.tla storage/AwaSegmentedStorageTraceBroken.cfg
```

Expected outcomes:

- Snooze config: TLC reports `Invariant SnoozeTraceIncomplete is
  violated` — this is the **positive witness** that the trace was
  fully consumed (traceIdx reached Len(Trace) = 6).
- Broken config: TLC reports `Deadlock reached` at traceIdx = 2 —
  the third event (`PromoteDeferred`) has no matching enabled spec
  action.

See [`MAPPING.md`](./MAPPING.md#trace-validation) for the full
description of how to transcribe and add a new trace.

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
