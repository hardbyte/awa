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

## Design goals and non-goals

This ADR is guided by the following `0.6` priorities:

- Preserve Postgres-native **transactional enqueue**. A job created alongside
  application data must commit or roll back with that data, with no outbox
  gap.
- Prefer **no lost work under failure** over benchmark wins. Crashes,
  restarts, network partitions, and worker death must not silently drop
  runnable jobs.
- State the delivery contract honestly: **at-least-once**, not marketing
  “exactly once”. Retries must be safe, idempotency should be encouraged, and
  stale writers must be rejected by `(job_id, run_lease)`.
- Keep the **claim path fast under contention** with short transactions,
  queue-local cursors, and `SKIP LOCKED`-style claiming rather than global
  head-of-line blocking.
- Keep **lease / heartbeat recovery**, **retry discipline**, and
  **observability** as first-class runtime concerns rather than operational
  afterthoughts.
- Remain **vacuum-friendly** by keeping historical state out of the hot claim
  path and avoiding row-rewrite-heavy designs.

Non-goals and filters:

- Do not claim “exactly once”.
- Do not turn Awa into a replayable event log or Kafka replacement; the
  design remains a job queue with one active attempt owner at a time.
- Do not introduce optimizations that weaken “no lost work under failure”, even
  if they benchmark well.
- Do not solve contention by moving back to a single hot mutable jobs table or
  by holding long-lived transactions around claim/execute/ack.

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

- The common path still records every claim, but the implementation can now do
  that in two stages:
  - append-only `lease_claims` receipts for zero-deadline short jobs
  - a bounded `open_receipt_claims` frontier for currently-live receipt-backed
    attempts
  - lazy materialization into `active_leases` when the attempt needs the
    mutable execution path
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
- Lease prune order is derived from `lease_ring_state(current_slot,
  generation, slot_count)`. The older `{schema}.lease_ring_slots` table
  remains as transitional compatibility/inspection state, but it is no longer
  updated on every rotation and is not the authoritative ordering source.
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
- Short zero-deadline claim: append a `lease_claims` receipt instead of an
  immediate `active_leases` row.
- Short zero-deadline rescue before first heartbeat: close the stale receipt
  append-only and requeue it without first materializing a mutable
  `active_leases` row.
- Runtime reads that need the current live receipt-backed set (`queue_counts`,
  receipt rescue, and receipt-backed `load_job`) read
  `open_receipt_claims`, not the full append-only claim history.
- First heartbeat or progress flush for a receipt-backed attempt: lazily
  materialize the claim into `attempt_state` while keeping the claim on the
  append-only receipt path.
- Callback registration or other lease-specific mutation for a receipt-backed
  attempt: lazily materialize the claim into `active_leases`.
- Heartbeat after lease materialization / callback wait: update the active
  lease row only.
- Progress flush / callback expressions / callback result: create or update
  `attempt_state` only when needed.
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
- short zero-deadline jobs can also complete without ever creating a mutable
  `active_leases` row.
- the live receipt frontier stays bounded to open receipt-backed attempts
  rather than growing with total claims and closures.
- short zero-deadline jobs that never heartbeat can be rescued from
  `lease_claims` after the grace window without first creating
  `active_leases`.
- heartbeat/progress-only long attempts can stay on `lease_claims` plus
  `attempt_state` without creating `active_leases`.
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
lock contract is:

- claim takes `FOR UPDATE OF claims SKIP LOCKED` on `queue_claim_heads` while
  it advances the claim cursor and inserts the claim
- rotate publishes the next lease slot with a compare-and-swap update on
  `lease_ring_state`
- prune-ready takes `FOR UPDATE` on `queue_ring_state`, `FOR UPDATE` on the
  target `queue_ring_slots` row, and `ACCESS EXCLUSIVE` on the child
  partitions before it rechecks liveness and truncates
- prune-leases derives the oldest initialized slot from `lease_ring_state`,
  locks the child partition `ACCESS EXCLUSIVE`, rechecks that the slot is not
  current, then truncates if it is empty

That order is deliberate. The TLA+ storage race / lock-order models exist to
prove that claim, rotate, and prune cannot interleave into “claim lands in a
pruned segment” behavior.

Rotation and prune policy is also part of this decision:

- lease segments rotate quickly because lease churn is the remaining hot-path
  source
- ready / deferred / waiting / terminal / dlq segments rotate more slowly and
  are primarily retention-driven
- prune walks sealed segments oldest-first
- prune uses `SET LOCAL lock_timeout = '50ms'` and returns gracefully when a
  reader or writer still holds the segment
- long-lived readers can still pin a segment horizon, so operators are
  expected to run analytical reads on replicas and keep primary-side
  `statement_timeout` discipline

Retention is therefore a rotation-and-prune story, not a row-by-row delete
story. Queue retention knobs, DLQ retention, and segment counts all compose
within that operational policy.

### Hot-path requirements

The decision above pins two hot-path requirements that surface repeatedly
in implementation:

- Claim runs as a single server-side step: one SQL function locks
  `lane_state`, selects the oldest runnable segment-local entry, and inserts
  the `active_leases` row. Dispatch must not round-trip between the cursor
  read and the lease insert.
- Short successful completion carries the immutable claim-time job snapshot
  through the completion batcher so the terminal append does not reload
  `ready_entries`.
- The completion batcher therefore owns a claim-time snapshot pass-through
  path: short completions append terminal rows from the already-carried
  immutable claim snapshot, while stale completions still lose via the
  `(job_id, run_lease)` guarded delete against the live execution row.
- Local worker capacity is released when handler execution ends and the
  progress snapshot is captured; durable completion then continues through the
  detached completion batcher path. This keeps execution capacity tied to
  active handler work rather than to completion flush latency while leaving
  the `run_lease`-guarded rescue boundary unchanged.
- Crash safety for the batcher relies on the normal rescue path, not an
  auxiliary replay log. If an in-memory completion batch is lost with the
  worker process, the live attempt remains visible to heartbeat/deadline
  rescue and is reclaimed that way.

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
updates. The current implementation now includes an experimental short-job
receipt path (`lease_claims` plus lazy materialization) that substantially
reduces dead tuples for zero-deadline short jobs. Long-horizon profiling also
showed that the append-only history alone was not enough: open claims had to
be tracked in a bounded `open_receipt_claims` frontier so rescue and queue
counts would not degrade into history scans. Further lease-plane work is still
tracked in [`lease-plane-redesign-spike.md`](../lease-plane-redesign-spike.md).

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
- Queue storage has to replace some old row-rewrite maintenance patterns with
  storage-specific equivalents, such as claim-time effective priority aging
  instead of physically rewriting ready rows.
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
