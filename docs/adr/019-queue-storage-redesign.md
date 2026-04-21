# ADR-019: Queue Storage Engine

## Status

Accepted

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

This ADR uses the following storage vocabulary:

- Queue plane: `ready_entries`, `terminal_entries`
- Execution plane: `active_leases`, `attempt_state`
- Control plane: `lane_state`, `ready_segments`, `ready_segment_cursor`,
  `lease_segments`, `lease_segment_cursor`

The implementation and migrations use these physical names:

- `terminal_entries` maps to `{schema}.done_entries`
- `active_leases` maps to `{schema}.leases`
- `lane_state` maps to `{schema}.queue_lanes`
- enqueue heads map to `{schema}.queue_enqueue_heads`
- claim heads map to `{schema}.queue_claim_heads`

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

- Queue-local lane metadata is intentionally split so one row family does not
  absorb every enqueue and claim mutation.
- `lane_state` itself is reduced to stable per-lane identity and legacy
  fallback fields. The mutable enqueue cursor lives in `queue_enqueue_heads`
  and the mutable claim cursor lives in `queue_claim_heads`.
- Availability is derived from `ready_entries` plus the claim head, not from a
  hot cached `available_count` field on `lane_state`.
- Completed-history rollups live in a separate cold cache table so prune can
  preserve queue counts without serializing on the hot `lane_state` rows.
- Completion must not update `lane_state` on every terminal transition; the
  hot path only mutates claim/enqueue control state. Per-completion
  `running` / `completed` counter updates on a single busy lane collapse
  throughput by an order of magnitude under benchmark, so `running` is
  derived from `active_leases` and terminal counts come from live
  `terminal_entries` plus the post-prune rollup rather than from a hot
  completion counter. That rollup is written in the prune transaction, but to
  a separate cold table rather than to `lane_state`, so the prune path does
  not serialize on the same hot rows as claim and enqueue.
- Ready and lease segment cursor tables tell dispatch and maintenance which
  physical segment is active, claimable, or prunable.
- Rotation state for queue and lease segments is owned by the maintenance
  leader.
- This split is an explicit response to the remaining MVCC hotspot discovered
  during long-horizon event benchmarks: a single hot `queue_lanes` row per
  `(queue, priority)` recreated dead-tuple pressure under pinned-reader
  workloads even after the rest of the storage engine moved to append-only
  structures. Separate enqueue/claim heads reduce that mutation frequency and
  leave `lane_state` cold enough to survive long-running readers.

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

All prune paths remain best-effort and use short lock timeouts. The explicit
lock contract — claim takes `FOR SHARE` on the lease segment cursor, rotate
and prune take `FOR UPDATE` plus `ACCESS EXCLUSIVE` on the segment child, and
prune rechecks liveness inside the lock-holding transaction — is its own
decision in [ADR-023](023-storage-lock-ordering.md).

### Hot-path requirements

The decision above pins two hot-path requirements that surface repeatedly
in implementation:

- Claim runs as a single server-side step: one SQL function locks
  `lane_state`, selects the oldest runnable segment-local entry, and inserts
  the `active_leases` row. Dispatch must not round-trip between the cursor
  read and the lease insert.
- Short successful completion carries the immutable claim-time job snapshot
  through the completion batcher so the terminal append does not reload
  `ready_entries`. The batcher shape is its own decision in
  [ADR-025](025-completion-batcher-snapshot-passthrough.md).

## Validation

Recorded local 5k-job runtime soak: **9,537 jobs/s**, **3.671 ms pickup p50**,
**22.013 ms pickup p95**, **417 exact final dead tuples** (canonical runtime
under the same workload: 9,686 jobs/s, 38.998 ms p95, dead tuples not
sampled). The full command log, raw output, and per-table dead-tuple
breakdown are in
[`bench/019-queue-storage-validation-2026-04-19.md`](bench/019-queue-storage-validation-2026-04-19.md).

The repo also maintains a phase-driven portable comparison harness under
`benchmarks/portable/`. That harness records producer, subscriber, and
end-to-end latency on a shared timebase while also sampling throughput, queue
depth, and dead tuples over time. Current engineering runs consistently place
Awa ahead of PgQue on subscriber and end-to-end latency and ahead on sustained
throughput in the event-delivery scenarios, while PgQue keeps a lower
dead-tuple profile. See [docs/benchmarking.md](../benchmarking.md) for the
current comparison methodology and caveats.

The current pressure frontier after the split-head change is the lease plane:
`queue_lanes` is no longer the dominant MVCC hotspot, but the mutable
`active_leases` family still absorbs steady insert/delete churn and heartbeat
updates. The follow-up design work is tracked in
[`lease-plane-redesign-spike.md`](../lease-plane-redesign-spike.md).

Spec-level safety is checked by the segmented-storage TLA+ family —
`AwaSegmentedStorage`, `AwaSegmentedStorageRaces`, `AwaStorageLockOrder`,
`AwaSegmentedStorageTrace` — under
[`correctness/storage/`](../../correctness/storage/). The TLA+ action →
Rust function correspondence is in
[`correctness/storage/MAPPING.md`](../../correctness/storage/MAPPING.md).

## Consequences

### Positive

- Dead tuples are bounded by the lease rotation window and the tiny
  `lane_state` cache, not by total queue history.
- Queue throughput is now near-parity with the canonical runtime while still
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
