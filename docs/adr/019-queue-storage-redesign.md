# ADR-019: Queue Storage Engine

## Status

Accepted. The receipt-plane portion of this design (`open_receipt_claims` as a bounded live-frontier table) is **superseded by [ADR-023: Receipt Plane Ring Partitioning](023-receipt-plane-ring-partitioning.md)**, which replaces the flat `open_receipt_claims` table with partitioned `lease_claims` / `lease_claim_closures` reclaimed by ring rotation + `TRUNCATE`. The rest of ADR-019 (queue plane, lease ring, ready/done partitioning) is unchanged. References to `open_receipt_claims` below are kept verbatim as historical context — read them as "the live frontier", which is now derived from the anti-join.

## Context

The canonical Awa storage model keeps the queue row and the lifecycle state machine in the same mutable heap row. Claim, heartbeat, callback wait, retry, completion, and cleanup all update or delete that row. Under overlapping readers, that shape produces avoidable MVCC churn and dead tuples on the hot path.

ADR-012 reduced the problem by splitting hot and deferred storage, but it did not remove the core issue: the dispatch path still mutates the same execution rows repeatedly. Benchmarking showed that this leaves a persistent gap to append-only queue designs on dead tuples, and eventually on throughput under overlap.

## Design goals and non-goals

This ADR is guided by the following `0.6` priorities:

- Preserve Postgres-native **transactional enqueue**. A job created alongside application data must commit or roll back with that data, with no outbox gap.
- Prefer **no lost work under failure** over benchmark wins. Crashes, restarts, network partitions, and worker death must not silently drop runnable jobs.
- State the delivery contract honestly: **at-least-once**, not marketing “exactly once”. Retries must be safe, idempotency should be encouraged, and stale writers must be rejected by `(job_id, run_lease)`.
- Keep the **claim path fast under contention** with short transactions, queue-local cursors, and `SKIP LOCKED`-style claiming rather than global head-of-line blocking.
- Keep **lease / heartbeat recovery**, **retry discipline**, and **observability** as first-class runtime concerns rather than operational afterthoughts.
- Remain **vacuum-friendly** by keeping historical state out of the hot claim path and avoiding row-rewrite-heavy designs.

Non-goals and filters:

- Do not claim “exactly once”.
- Do not turn Awa into a replayable event log or Kafka replacement; the design remains a job queue with one active attempt owner at a time.
- Do not introduce optimizations that weaken “no lost work under failure”, even if they benchmark well.
- Do not solve contention by moving back to a single hot mutable jobs table or by holding long-lived transactions around claim/execute/ack.

## Decision

Adopt a single queue storage engine built around append-only queue records, a small ready-tombstone ledger for rare out-of-band ready mutations, a narrow fast-rotating execution lease table, and an optional per-attempt `attempt_state` row for mutable runtime data that cannot stay in the immutable queue record.

### Terminology

This ADR uses the following storage vocabulary:

- Queue plane: `ready_entries`, `ready_tombstones`, `terminal_entries`
- Execution plane: `active_leases`, `attempt_state`
- Control plane: `lane_state`, `ready_segments`, `ready_segment_cursor`, `lease_segments`, `lease_segment_cursor`

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
- Claim order is driven by queue-local lane metadata rather than scanning a large mutable heap.

2. `ready_tombstones`

- Unclaimed cancellation, priority aging, and similar ready-lane mutations append a tombstone keyed by ready segment generation and lane identity.
- Claim and exact-count paths anti-join tombstones and treat a tombstoned head lane as committed spent evidence for safe claim-cursor advancement.
- Tombstones are reclaimed by truncating the matching ready segment; they do not create row-vacuum work on the hot ready table.

3. `active_leases`

- The common path still records every claim, but the implementation can now do that in two stages:
  - append-only `lease_claims` receipts for every claim (the receipt path is now the default, not a zero-deadline-only short cut)
  - **historical:** a bounded `open_receipt_claims` frontier for currently-live receipt-backed attempts. ADR-023 replaces this with partitioned `lease_claims` / `lease_claim_closures`; "currently open" is derived as an anti-join over the active claim partitions.
  - lazy materialization into `active_leases` when the attempt needs the mutable execution path
- Leases keep only dispatch and rescue-critical fields: ready reference, attempt identity, lane ordering, heartbeat/deadline state, and callback token/timeout.
- Lease partitions rotate much faster than queue partitions and are pruned independently.

3. `attempt_state`

- `attempt_state` is optional and keyed by `(job_id, run_lease)`.
- It exists only for mutable per-attempt data that cannot remain in the immutable queue row: current progress, callback expressions, and a temporary callback result for resumed handlers.
- Short jobs must be able to claim and complete without touching `attempt_state` at all.
- Transition paths delete `attempt_state` immediately when an attempt leaves `running` or `waiting_external`.

4. `deferred_jobs`

- Scheduled and retryable jobs live outside the ready ring.
- Promotion appends them back into `ready_entries` when due.
- Deferred rows stay out of the hot claim path until they are runnable.

5. `terminal_entries`

- Completion appends a terminal history row.
- Done partitions are append-only and are pruned with their ready segment once the segment is sealed and no active lease or pending ready row still references it.

6. `dlq_entries`

- Dead-letter handling is part of the storage engine, not an add-on.
- Terminal failures can move directly into the DLQ instead of the main done history.
- Operators can inspect, retry, and purge DLQ rows through the model/admin surface.

7. `lane_state` and segment cursor tables

- Queue-local lane metadata is intentionally split so one row family does not absorb every enqueue and claim mutation.
- `lane_state` itself is reduced to stable per-lane identity and legacy fallback fields. `queue_enqueue_heads` and `queue_claim_heads` remain as stable lane registries and lock targets; the mutable enqueue and claim cursors live in PostgreSQL sequences referenced by those rows.
- Availability counts have two grades, both cheap. The dispatcher hot path reads `sum(enqueue_sequence_cursor - claim_sequence_cursor)` from the lane heads — two PK reads plus sequence state reads per lane, no scan over `ready_entries`. Admin / UI calls (`queue_counts`) scan `ready_entries WHERE lane_seq >= claim_sequence_cursor` anti-joined with `ready_tombstones` for an exact result. The dispatcher tolerates transient over-count from committed gaps, uncommitted sequence reservations, and tombstoned unclaimed rows; the cursor is only advanced after committed claims/cancels and only across committed spent/tombstoned prefixes, so it can lag but cannot skip visible ready work. **historical:** earlier iterations cached this as `queue_lanes.available_count` (a third counter mutated on every enqueue / claim / completion); long-horizon profiling under pinned xmin showed the cache was the dominant dead-tuple source, so v016 removed it in favour of the head-difference derivation.
- Completed-history rollups live in a separate cold cache table so prune can preserve queue counts without serializing on the hot `lane_state` rows.
- Completion must not update `lane_state` on every terminal transition; the hot path only mutates claim/enqueue control state. `running` is derived from active leases/receipts. ADR-026 terminal counts use append-only `queue_terminal_count_deltas` on the terminal path, folded `queue_terminal_live_counts` for sealed retained segments, and the post-prune `queue_terminal_rollups` cache. The permanent rollup is written in the prune transaction to a separate cold table rather than to `lane_state`, so prune does not serialize on the same hot rows as claim and enqueue.
- Ready and lease segment cursor tables tell dispatch and maintenance which physical segment is active, claimable, or prunable.
- Lease prune order is derived from `lease_ring_state(current_slot, generation, slot_count)`. The older `{schema}.lease_ring_slots` table remains as transitional compatibility/inspection state, but it is no longer updated on every rotation and is not the authoritative ordering source.
- Rotation state for queue and lease segments is owned by the maintenance leader.
- This split is an explicit response to the remaining MVCC hotspot discovered during long-horizon event benchmarks: a single hot `queue_lanes` row per `(queue, priority)` recreated dead-tuple pressure under pinned-reader workloads even after the rest of the storage engine moved to append-only structures. Separate enqueue/claim heads reduce that mutation frequency and leave `lane_state` cold enough to survive long-running readers.

### Lifecycle mapping

- Enqueue immediate job: append to `ready_entries`.
- Claim: lock the queue's `queue_claim_heads` row, join the matching `queue_enqueue_heads` row, select the next ready entries anti-joined with `ready_tombstones`, increment `run_lease`, append a receipt into `lease_claims` by default, and advance the claim cursor based on committed spent evidence plus the rows actually selected after commit.
- Receipt-backed deadlines: per-queue deadlines live on `lease_claims.deadline_at` until the attempt materializes into the lease plane for callbacks, progress, or other mutable state.
- Receipt rescue before materialization: close the stale receipt append-only and requeue it without first materializing a mutable `leases` row.
- Runtime reads that need the current live receipt-backed set (`queue_counts`, receipt rescue, and receipt-backed `load_job`) consult only the active claim-ring partitions, not the full append-only claim history. **historical:** ADR-019 used a bounded `open_receipt_claims` table for this; ADR-023 derives the same set as an anti-join over partitioned `lease_claims` / `lease_claim_closures`.
- First heartbeat or progress flush for a receipt-backed attempt: lazily materialize the claim into `attempt_state` while keeping the claim on the append-only receipt path.
- Callback registration or other lease-specific mutation for a receipt-backed attempt: lazily materialize the claim into `active_leases`.
- Heartbeat after lease materialization / callback wait: update the active lease row only.
- Progress flush / callback expressions / callback result: create or update `attempt_state` only when needed.
- Retry or snooze: delete the active lease row, merge any `attempt_state`, append to `deferred_jobs`.
- Complete: delete the active lease row, ignore or merge any `attempt_state` as needed, append to `terminal_entries`.
- Terminal failure with DLQ enabled: delete the active lease row, merge any `attempt_state`, append to `dlq_entries`.

### Attempt state invariants

- There is at most one live `attempt_state` row per active attempt.
- `attempt_state` is guarded by `(job_id, run_lease)` so stale writers from a rescued or retried attempt cannot mutate the next attempt.
- Dispatch must not join `attempt_state`; claim performance should be insensitive to `attempt_state` size.
- `attempt_state` must not store history arrays or long-retention terminal state. Error history remains in immutable queue payload.
- Rescue scans inspect lease and `attempt_state` tables, not queue history.

Healthy operating signals for this design are:

- `attempt_state` row count roughly tracks active long-running attempts, not total queue depth or total completions.
- short jobs complete without creating an `attempt_state` row.
- short zero-deadline jobs can also complete without ever creating a mutable `active_leases` row.
- the live receipt frontier stays bounded to open receipt-backed attempts rather than growing with total claims and closures.
- short zero-deadline jobs that never heartbeat can be rescued from `lease_claims` after the grace window without first creating `active_leases`.
- heartbeat/progress-only long attempts can stay on `lease_claims` plus `attempt_state` without creating `active_leases`.
- prune and cleanup horizons for `attempt_state` are immediate or very short.

### Maintenance ownership

The leader-elected maintenance service owns:

- promoting due deferred jobs
- rescuing stale heartbeats, deadlines, and callback timeouts
- rotating and pruning queue partitions
- rotating and pruning lease partitions
- rotating and pruning claim-ring partitions (added by ADR-023; replaces the ADR-019 `open_receipt_claims` cleanup path)
- queue depth publication
- DLQ retention cleanup

All prune paths remain best-effort and use short lock timeouts. The explicit lock contract is:

- claim takes `FOR UPDATE OF claims SKIP LOCKED` on `queue_claim_heads` while it advances the claim cursor and inserts the claim
- rotate publishes the next lease slot with a compare-and-swap update on `lease_ring_state`
- prune-ready takes `FOR UPDATE` on `queue_ring_state`, `FOR UPDATE` on the target `queue_ring_slots` row, and `ACCESS EXCLUSIVE` on the child partitions before it rechecks liveness and truncates
- prune-leases derives the oldest initialized slot from `lease_ring_state`, locks the child partition `ACCESS EXCLUSIVE`, rechecks that the slot is not current, then truncates if it is empty
- prune-claim-ring (added by ADR-023) takes `FOR UPDATE` on `claim_ring_state`, `FOR UPDATE` on the target `claim_ring_slots` row, and `ACCESS EXCLUSIVE` on both the matching `lease_claims_*` and `lease_claim_closures_*` partitions, verifies every claim in the slot has a matching closure, then `TRUNCATE`s both partitions in lockstep. Open claims make prune skip the slot until normal completion or the separate receipt-rescue scans close them.

That order is deliberate. The TLA+ storage race / lock-order models exist to prove that claim, rotate, and prune cannot interleave into “claim lands in a pruned segment” behavior.

Rotation and prune policy is also part of this decision:

- lease and claim-ring segments rotate quickly because their churn is the remaining hot-path source
- ready / terminal segments rotate more slowly than the lease and claim rings; deferred and DLQ rows are plain backlog/hold tables with their own promotion, retry, purge, and retention cleanup paths
- prune walks sealed segments oldest-first
- prune uses `SET LOCAL lock_timeout = '50ms'` and returns gracefully when a reader or writer still holds the segment
- long-lived readers can still pin a segment horizon, so operators are expected to run analytical reads on replicas and keep primary-side `statement_timeout` discipline

Ordinary queue-storage terminal history is therefore a rotation-and-prune story, not a row-by-row delete story. DLQ retention remains a bounded cleanup pass against the separate `dlq_entries` hold table, and the canonical compatibility path still has row-retention cleanup.

### Hot-path requirements

The decision above pins two hot-path requirements that surface repeatedly in implementation:

- Claim runs as a single server-side step: one SQL function locks `queue_claim_heads`, selects the oldest runnable segment-local entry for the shard-qualified lane, and appends either a `lease_claims` receipt or a materialized `leases` row. Dispatch must not round-trip between the cursor read and the claim insert.
- Short successful completion carries the immutable claim-time job snapshot through the completion batcher so the terminal append does not reload `ready_entries`.
- The completion batcher therefore owns a claim-time snapshot pass-through path: short completions append terminal rows from the already-carried immutable claim snapshot, while stale completions still lose via the `(job_id, run_lease)` guarded delete against the live execution row.
- Local worker capacity is released when handler execution ends and the progress snapshot is captured; durable completion then continues through the detached completion batcher path. This keeps execution capacity tied to active handler work rather than to completion flush latency while leaving the `run_lease`-guarded rescue boundary unchanged.
- Crash safety for the batcher relies on the normal rescue path, not an auxiliary replay log. If an in-memory completion batch is lost with the worker process, the live attempt remains visible to heartbeat/deadline rescue and is reclaimed that way.

## Validation

Recorded local 5k-job runtime soak: **9,537 jobs/s**, **3.671 ms pickup p50**, **22.013 ms pickup p95**, **417 exact final dead tuples** (canonical runtime under the same workload: 9,686 jobs/s, 38.998 ms p95, dead tuples not sampled). The full command log, raw output, and per-table dead-tuple breakdown are in [`bench/019-queue-storage-validation-2026-04-19.md`](bench/019-queue-storage-validation-2026-04-19.md).

The phase-driven portable comparison harness lives in a separate repo: [postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking). That harness records producer, subscriber, and end-to-end latency on a shared timebase while also sampling throughput, queue depth, and dead tuples over time. Recent runs place Awa ahead of pgque on end-to-end latency and on sustained throughput in clean-phase scenarios, while pgque holds a comparable dead-tuple profile (both are append-only / partition-rotated). See [`SYSTEM_COMPARISONS.md`](https://github.com/hardbyte/postgresql-job-queue-benchmarking/blob/main/SYSTEM_COMPARISONS.md) for the per-system architectural notes and [docs/benchmarking.md](../benchmarking.md) for awa's own regression methodology.

The current pressure frontier after the split-head change is the lease plane: `queue_lanes` is no longer the dominant MVCC hotspot, but the mutable `active_leases` family still absorbs steady insert/delete churn and heartbeat updates. The current implementation now includes a short-job receipt path (`lease_claims` plus lazy materialization) that substantially reduces dead tuples for zero-deadline short jobs. Long-horizon profiling also showed that the append-only history alone was not enough: open claims had to be tracked explicitly so rescue and queue counts would not degrade into history scans. The original ADR-019 design used a bounded `open_receipt_claims` table for this; ADR-023 replaces it with partitioned `lease_claims` / `lease_claim_closures` where "currently open" is derived at query time as an anti-join over the active partitions, eliminating the last MVCC churn source on the receipt plane. Further lease-plane work is still tracked in [`lease-plane-redesign-spike.md`](../archive/0.6-storage-design/lease-plane-redesign-spike.md). The remaining queue-level coordination controls are implemented as bounded claimers, queue striping (`queue_stripe_count`), and per-queue enqueue-head sharding (`queue_meta.enqueue_shards`). The archived [`bounded-claimers-plan.md`](../archive/0.6-storage-design/bounded-claimers-plan.md) and [`queue-striping-plan.md`](../archive/0.6-storage-design/queue-striping-plan.md) capture design history; current operator guidance lives in [`configuration.md`](../configuration.md#queue-storage-tuning).

Spec-level safety is checked by the segmented-storage TLA+ family — `AwaSegmentedStorage`, `AwaSegmentedStorageRaces`, `AwaStorageLockOrder`, `AwaSegmentedStorageTrace` — under [`correctness/storage/`](../../correctness/storage/). The TLA+ action → Rust function correspondence is in [`correctness/storage/MAPPING.md`](../../correctness/storage/MAPPING.md).

## Consequences

### Positive

- Dead tuples are bounded by the lease rotation window and the tiny `lane_state` cache, not by total queue history.
- Queue throughput is now near-parity with the canonical runtime while still substantially reducing claim-tail latency under the benchmark workload.
- DLQ becomes a first-class storage concern instead of an afterthought bolted onto the hot table model.
- Retention moves from row-by-row cleanup toward partition rotation and prune.

### Negative

- This is a breaking storage redesign rather than an incremental tweak.
- Admin and producer paths must become backend-aware instead of assuming the canonical hot/deferred tables are the only physical layout.
- Queue storage has to replace some old row-rewrite maintenance patterns with storage-specific equivalents, such as claim-time effective priority aging instead of physically rewriting ready rows.
- Long-running readers can still delay best-effort partition prune.

## Alternatives Considered

### Keep the canonical hot/deferred split

Rejected. It helps planner shape, but it does not remove the lifecycle-row MVCC churn that causes dead tuples on the hot path.

### Rotate mutable canonical rows directly

Rejected. Rotating the same mutable state-machine rows preserves the core problem; it only spreads churn across more tables.

### Append-only history plus a single global lease heap

Rejected. This improves queue-row churn, but the lease heap still bloats in proportion to total runtime churn.

### Two user-facing storage modes

Rejected. The storage engine can use multiple physical table families without exposing multiple public queue models.

## Relationship to Earlier ADRs

- ADR-012 becomes historical background rather than the target architecture.
- ADR-003 still defines the heartbeat/deadline rescue policy; queue storage only changes where those timestamps live.
- ADR-014 still defines the user-facing progress model; queue storage changes the physical persistence path from dedicated columns to `attempt_state` and `active_leases`.
