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
rows repeatedly. Benchmarks on the issue-169 workload showed that this leaves a
large gap to append-only queue designs on dead tuples, and eventually on
throughput under overlap.

## Decision

Adopt a single queue storage engine built around append-only queue records and a
narrow, fast-rotating lease table.

### Physical layout

1. `ready_entries`

- Immediate jobs are appended to rotating ready partitions.
- Ready rows are immutable once written.
- Claim order is driven by queue-local lane metadata rather than scanning a
  large mutable heap.

2. `leases`

- Claim inserts a narrow lease row keyed by `(job_id, run_lease)`.
- Heartbeats, callback wait state, and explicit progress flushes update the
  lease row instead of rewriting the ready row.
- Lease partitions rotate much faster than queue partitions and are pruned
  independently.

3. `deferred_jobs`

- Scheduled and retryable jobs live outside the ready ring.
- Promotion appends them back into `ready_entries` when due.
- Deferred rows stay out of the hot claim path until they are runnable.

4. `done_entries`

- Completion appends a terminal history row.
- Done partitions are append-only and can be pruned by retention policy rather
  than row-by-row cleanup.

5. `dlq_entries`

- Dead-letter handling is part of the storage engine, not an add-on.
- Terminal failures can move directly into the DLQ instead of the main done
  history.
- Operators can inspect, retry, and purge DLQ rows through the model/admin
  surface.

6. `queue_lanes` and ring state tables

- Queue-local cursor and depth metadata live in a tiny hot table.
- Dispatch reserves ranges from lane metadata, then materializes leases from
  those ranges.
- Rotation state for queue and lease rings is owned by the maintenance leader.

### Lifecycle mapping

- Enqueue immediate job: append to `ready_entries`.
- Claim: reserve a lane range, insert lease rows, increment `run_lease`.
- Heartbeat / callback wait / progress flush: update the lease row only.
- Retry or snooze: delete the lease row, append to `deferred_jobs`.
- Complete: delete the lease row, append to `done_entries`.
- Terminal failure with DLQ enabled: delete the lease row, append to
  `dlq_entries`.

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
| canonical runtime | `5401/s` | `2.226 ms` | `53.880 ms` | `76.358 ms` | not sampled |
| queue storage runtime | `5801/s` | `3.652 ms` | `9.429 ms` | `39.164 ms` | `336` |

Latest exact dead-tuple breakdown for the queue storage run:

- `queue_lanes = 75`
- `ready = 0`
- `done = 0`
- `leases = 261`
- `total = 336`

These numbers matter more than the earlier storage-only spike because they come
through the real dispatcher, executor, callback, maintenance, and DLQ paths.

## Consequences

### Positive

- Dead tuples are bounded by the lease rotation window and the tiny lane cache,
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
  directly need storage-specific implementations against queue lanes/segments.
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
  the physical persistence path from dedicated columns to lease/runtime payload.
