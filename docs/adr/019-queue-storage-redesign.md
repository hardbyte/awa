# ADR-019: Queue Storage Engine

## Status

Proposed

## Context

The canonical Awa storage model keeps the queue row and the lifecycle state
machine in the same mutable heap row. Claim, heartbeat, callback wait, retry,
completion, and cleanup all update or delete that row. Under overlapping
readers, that shape produces avoidable MVCC churn and dead tuples on the hot
path.

ADR-012 reduced the problem by splitting hot and deferred storage, but it did
not remove the core issue: the dispatch path still mutates the same execution
rows repeatedly. Benchmarking showed that this leaves a persistent gap to
append-only queue designs on dead tuples, and eventually on throughput under
overlap.

## Decision

Adopt a single queue storage engine built around append-only queue records, a
narrow fast-rotating execution lease table, and an optional per-attempt
`attempt_state` row for mutable runtime data that cannot stay in the immutable
queue record.

### Terminology

The intended storage vocabulary is:

- Queue plane: `ready_entries`, `terminal_entries`
- Execution plane: `active_leases`, `attempt_state`
- Control plane: `lane_state`, `ready_segments`, `ready_segment_cursor`,
  `lease_segments`, `lease_segment_cursor`

The current prototype still uses some older physical names (`leases`,
`done_entries`, `queue_lanes`, and ring-state helper tables). That naming
should converge toward the terminology above before the redesign is treated as
finished. The intent of this ADR is the conceptual storage model, not the
prototype-era names.

### Physical layout

1. `ready_entries`

- Immediate jobs are appended to rotating ready partitions.
- Ready rows are immutable once written.
- Claim order is driven by queue-local lane metadata rather than scanning a
  large mutable heap.

2. `active_leases`

- Claim inserts a narrow lease row keyed by `(job_id, run_lease)`.
- Leases keep only dispatch and rescue-critical fields: ready reference,
  attempt identity, lane ordering, heartbeat/deadline state, and callback
  token/timeout.
- Lease partitions rotate much faster than queue partitions and are pruned
  independently.

3. `attempt_state`

- `attempt_state` is optional and keyed by `(job_id, run_lease)`.
- It exists only for mutable per-attempt data that cannot remain in the
  immutable queue row: current progress, callback expressions, and a temporary
  callback result for resumed handlers.
- Short jobs must be able to claim and complete without touching
  `attempt_state` at all.
- Transition paths delete `attempt_state` immediately when an attempt leaves
  `running` or `waiting_external`.

4. `deferred_jobs`

- Scheduled and retryable jobs live outside the ready ring.
- Promotion appends them back into `ready_entries` when due.
- Deferred rows stay out of the hot claim path until they are runnable.

5. `terminal_entries`

- Completion appends a terminal history row.
- Done partitions are append-only and can be pruned by retention policy rather
  than row-by-row cleanup.

6. `dlq_entries`

- Dead-letter handling is part of the storage engine, not an add-on.
- Terminal failures can move directly into the DLQ instead of the main done
  history.
- Operators can inspect, retry, and purge DLQ rows through the model/admin
  surface.

7. `lane_state` and segment cursor tables

- Queue-local cursor metadata lives in a tiny hot table.
- `lane_state` owns enqueue and claim cursors plus low-churn rollups needed to
  preserve counts across terminal-segment prune.
- Completion must not update `lane_state` on every terminal transition; the
  hot path only mutates claim/enqueue control state.
- Ready and lease segment cursor tables tell dispatch and maintenance which
  physical segment is active, claimable, or prunable.
- Rotation state for queue and lease segments is owned by the maintenance
  leader.

### Lifecycle mapping

- Enqueue immediate job: append to `ready_entries`.
- Claim: read `lane_state`, claim the next ready entries, insert
  `active_leases`, increment `run_lease`, and advance the lane cursor based on
  the rows actually selected.
- Heartbeat / callback wait: update the active lease row only.
- Progress flush / callback expressions / callback result: create or update an
  `attempt_state` row only when needed.
- Retry or snooze: delete the active lease row, merge any `attempt_state`,
  append to `deferred_jobs`.
- Complete: delete the active lease row, ignore or merge any `attempt_state`
  as needed, append to `terminal_entries`.
- Terminal failure with DLQ enabled: delete the active lease row, merge any
  `attempt_state`, append to
  `dlq_entries`.

### Attempt state invariants

- There is at most one live `attempt_state` row per active attempt.
- `attempt_state` is guarded by `(job_id, run_lease)` so stale writers from a
  rescued or retried attempt cannot mutate the next attempt.
- Dispatch must not join `attempt_state`; claim performance should be
  insensitive to `attempt_state` size.
- `attempt_state` must not store history arrays or long-retention terminal
  state. Error history remains in immutable queue payload.
- Rescue scans inspect lease and `attempt_state` tables, not queue history.

Healthy operating signals for this design are:

- `attempt_state` row count roughly tracks active long-running attempts, not
  total queue depth or total completions.
- short jobs complete without creating an `attempt_state` row.
- prune and cleanup horizons for `attempt_state` are immediate or very short.

### Maintenance ownership

The leader-elected maintenance service owns:

- promoting due deferred jobs
- rescuing stale heartbeats, deadlines, and callback timeouts
- rotating and pruning queue partitions
- rotating and pruning lease partitions
- queue depth publication
- DLQ retention cleanup

All prune paths remain best-effort and use short lock timeouts.

## Implementation

The current prototype/runtime wiring lives in:

- `awa-model/src/queue_storage.rs`
- `awa-model/src/dlq.rs`
- `awa-worker/src/client.rs`
- `awa-worker/src/dispatcher.rs`
- `awa-worker/src/executor.rs`
- `awa-worker/src/maintenance.rs`
- `awa/tests/queue_storage_runtime_test.rs`
- `awa/tests/benchmark_test.rs`

The runtime validation on this branch covers:

- `RetryAfter`
- `Snooze`
- callback wait + `complete_external`
- terminal failure to DLQ
- callback-timeout rescue to DLQ
- DLQ get/list/retry flow plus job-dump DLQ metadata

## Validation

Latest local full-runtime benchmark runs on this branch:

| Runtime path | Throughput | Pickup p50 | Pickup p95 | Pickup p99 | Exact final dead |
|---|---:|---:|---:|---:|---:|
| canonical runtime | `8087/s` | `5.595 ms` | `102.805 ms` | `218.468 ms` | not sampled |
| queue storage runtime | `6886/s` | `3.983 ms` | `43.553 ms` | `64.706 ms` | `86` |

Latest exact dead-tuple breakdown for the queue storage run
(using current prototype table names):

- `queue_lanes = 63`
- `ready = 0`
- `done = 0`
- `leases = 23`
- `attempt_state = 0`
- `total = 86`

These numbers matter more than the earlier storage-only spike because they come
through the real dispatcher, executor, callback, maintenance, and DLQ paths.

## Consequences

### Positive

- Dead tuples are bounded by the lease rotation window and the tiny `lane_state`
  cache,
  not by total queue history.
- Queue throughput remains competitive with the canonical runtime while
  substantially reducing claim-tail latency under the benchmark workload.
- DLQ becomes a first-class storage concern instead of an afterthought bolted
  onto the hot table model.
- Retention moves from row-by-row cleanup toward partition rotation and prune.

### Negative

- This is a breaking storage redesign rather than an incremental tweak.
- Admin and producer paths must become backend-aware instead of assuming the
  canonical hot/deferred tables are the only physical layout.
- Priority aging and other maintenance operations that once mutated a hot row
  directly need storage-specific implementations against `lane_state` and
  segments.
- Long-running readers can still delay best-effort partition prune.

## Alternatives Considered

### Keep the canonical hot/deferred split

Rejected. It helps planner shape, but it does not remove the lifecycle-row MVCC
churn that causes dead tuples on the hot path.

### Rotate mutable canonical rows directly

Rejected. Rotating the same mutable state-machine rows preserves the core
problem; it only spreads churn across more tables.

### Append-only history plus a single global lease heap

Rejected. This improves queue-row churn, but the lease heap still bloats in
proportion to total runtime churn.

### Two user-facing storage modes

Rejected. The storage engine can use multiple physical table families without
exposing multiple public queue models.

## Relationship to Earlier ADRs

- ADR-012 becomes historical background rather than the target architecture.
- ADR-003 still defines the heartbeat/deadline rescue policy; queue storage only
  changes where those timestamps live.
- ADR-014 still defines the user-facing progress model; queue storage changes
  the physical persistence path from dedicated columns to `attempt_state` and
  `active_leases`.
