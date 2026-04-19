# ADR-019: Vacuum-Aware Segmented Storage

## Status

Proposed

## Context

Recent overlap-reader and queue-churn benchmarks exposed the core weakness in
Awa's current `jobs_hot` storage model: the queue row is both the dispatch
record and the lifecycle state machine. That means claim, heartbeat, progress
flush, callback wait, retry, completion, and cleanup all rewrite or delete the
same heap row while overlapping readers pin old MVCC horizons.

That shape is fundamentally hostile to the workload exercised by the
`test_mvcc_horizon_overlap_benchmark` family in
[docs/benchmarking.md](/Users/brian/dev/awa/docs/benchmarking.md#L45).

Earlier storage spikes showed three important things:

- rotating mutable queue rows does not solve the problem
- append-only queue storage plus the same mutable runtime table still leaves
  the hot table bloated
- the dead-tuple collapse only starts once the queue row stops being the
  lifecycle state machine

The missing piece was the claim path. A naive anti-join claim ledger solved
dead tuples but destroyed throughput.

## Decision

Replace the single mutable `jobs_hot` model with one segmented storage engine:

1. `queue segments`

- append-only `ready` / `deferred` / `waiting` segment families
- each family rotates in a small ring of partitions
- segments are reclaimed with maintenance-owned best-effort `TRUNCATE`

2. `lease ring`

- claims insert into a narrow `leases` table partitioned by a much faster
  rotation ring than the queue segments
- completion deletes from the current lease partition
- the lease ring is pruned independently of queue retention

3. `queue lane cache`

- per-queue cursor and counters live in a tiny hot table
- claim uses a queue-local cursor/range allocator, not a history anti-join
- admin/runtime queue depth reads come from cache tables, not long scans of the
  prunable segment ring

4. `runtime sidecar`

- only jobs that actually need mutable runtime state materialize it
- heartbeat, deadline extension, callback registration, and explicit progress
  flush live in a small sidecar keyed by `(job_id, run_lease)` or equivalent
- short jobs that start and finish before their first heartbeat never touch
  that table

5. `maintenance ownership`

- the existing leader-elected maintenance service rotates queue segments
- the same service rotates and prunes the lease ring on a shorter cadence
- all prune paths use short `lock_timeout` and retry later on conflict

## Prototype Evidence

The prototype implementation lives in
[vacuum_aware.rs](/Users/brian/dev/awa/awa-model/src/vacuum_aware.rs#L1) with a
benchmark harness in
[vacuum_aware_storage_benchmark_test.rs](/Users/brian/dev/awa/awa/tests/vacuum_aware_storage_benchmark_test.rs#L1).

Common benchmark shape:

- steady producer at `4000/s`
- claim batch size `256`
- `4s` baseline, `8s` overlap, `4s` cooldown
- one overlapping `REPEATABLE READ` reader unless noted otherwise
- exact final dead tuples measured with `pgstattuple`

### Queue Ring + Lease Ring Results

| Queue ring | Lease ring | Overlap throughput | Claim p99 | Exact final dead | Exact lease dead |
|---|---|---:|---:|---:|---:|
| `16 x 1000ms` | `16 x 250ms` | `4025/s` | `11.738 ms` | `14,046` | `13,800` |
| `16 x 1000ms` | `8 x 100ms` | `4025/s` | `12.708 ms` | `1,846` | `1,600` |
| `16 x 1000ms` | `4 x 50ms` | `4075/s` | `11.006 ms` | `75` | `0` |
| `16 x 1000ms` | `4 x 50ms` | `4050/s` | `12.102 ms` | `73` | `0` |
| `16 x 1000ms` + history reader | `4 x 50ms` | `4050/s` | `59.672 ms` | `73` | `0` |

Important observations:

- Ready/done partitions stayed at exact `0` dead tuples in every run.
- Once the lease ring cycle dropped to `4 slots x 50ms`, the lease partitions
  also ended at exact `0` dead tuples in repeated runs.
- Residual dead tuples came from the tiny `queue_lanes` cache table
  (`~73-75` exact rows), not from queue churn.
- `pg_stat_user_tables.n_dead_tup` overstated final dead tuples badly on the
  fast lease-ring runs; exact `pgstattuple` counts are the right measure for
  this work.
- A long history reader still blocked queue-segment prune attempts, but it did
  not reintroduce lease churn or hot dead tuples.

## Why This Closes the Gap

The critical change is that dead tuples now scale with the lease-cycle window,
not with the entire queue history.

For the hot claim/complete path:

- enqueue is append-only
- claim is append-only into a narrow lease partition
- complete deletes from a lease partition that is about to be truncated
- the queue row itself is never rewritten

That is why the prototype kept roughly `4k/s` overlap throughput while pushing
exact final dead tuples down from five figures to double digits.

## Production Mapping For Awa

The benchmark prototype only models the hot immediate path, so the production
design needs one more split:

### Fast path

- immediate enqueue lands in append-only `ready` segments
- claim inserts a lease row into the fast lease ring
- complete appends a terminal record and removes the lease row

### Long-running path

- if a job reaches its first heartbeat, registers a callback, or performs an
  explicit progress flush, create a runtime-sidecar row for that attempt
- update-heavy fields (`heartbeat_at`, `deadline_at`, callback metadata,
  current progress snapshot) live only there
- the update-heavy table now scales with active long-running concurrency, not
  total throughput

This keeps no-op and short jobs off the mutable path entirely while still
supporting Awa's current semantics from
[docs/architecture.md](/Users/brian/dev/awa/docs/architecture.md#L228).

## Consequences

### Positive

- closes the dead-tuple problem on the throughput path
- preserves competitive throughput and latency in the prototype
- makes maintenance-owned pruning practical for the hot path
- separates queue retention concerns from running-attempt churn

### Negative

- requires a breaking storage migration for `0.6`
- admin/history queries can no longer treat one mutable table as the source of
  truth
- callback, retry, and progress flows need explicit control-plane tables
- queue analytics must move to cache tables or offline/history reads

## Alternatives Considered

### Rotate Mutable `jobs_hot` Rows Directly

Rejected. Rotating mutable queue rows across buckets preserves the core
problem: the same rows still absorb claim, heartbeat, retry, completion, and
cleanup churn.

### Append-Only Queue Segments With One Global Lease Table

Rejected. This improved queue-table bloat, but the global lease table still
accumulated dead tuples proportional to total runtime churn.

### Two Public Storage Modes

Rejected. The storage engine can use multiple physical table families without
exposing multiple user-facing queue modes. The migration cost is real, but the
operational model should remain single-mode.

## Notes

- The hot-path prototype does **not** prove that an update-heavy heartbeat table
  is free. It proves that the throughput path can be taken off the update-heavy
  table entirely.
- The recommended public model is still one engine, not two user-facing modes.
  The internal storage can and should use multiple table families.
