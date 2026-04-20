# ADR-025: Completion Batcher with Claim-Time Snapshot Pass-Through

## Status

Accepted

## Context

Under ADR-019, completing a job atomically appends a `terminal_entries` row
and deletes the `active_leases` row. The naive implementation — re-read the
`ready_entries` row for payload, then write the terminal row — adds an extra
read per completion on what should be the tightest finishing path. For short
jobs (no `attempt_state` materialised, no callback activity) this extra read
is pure overhead: every field the terminal append needs was already in
scope at claim time.

## Decision

Carry the immutable claim-time snapshot (kind, queue, priority, args,
max_attempts, run_lease, attempted_at, created_at, unique_key,
unique_states, payload) through the in-memory completion batcher and into
the terminal append SQL, so short-job completion never re-reads
`ready_entries`.

The batcher lives in `awa-worker/src/completion.rs`. Its shape:

- **Per-shard buffer**: `CompletionWorker` owns a sharded `Vec<CompletionRequest>`
  to reduce contention between executor tasks.
- **Flush trigger**: max-batch-size (default 512) or flush-interval (default
  ~1 ms), whichever fires first.
- **SQL**: `QueueStorage::complete_runtime_batch` takes the batched
  snapshots and writes:
  - one `INSERT INTO {schema}.terminal_entries` with the carried fields,
  - one `DELETE FROM {schema}.active_leases WHERE (job_id, run_lease) IN ...`,
  in a single transaction, guarded by `run_lease` so stale completions
  match zero rows.
- **Fallback paths**: the non-runtime paths (`complete_claimed_batch`,
  `complete_job_batch_by_id`) still re-read when called with incomplete
  metadata, so admin and migration tooling that lack a claim snapshot
  continue to work.

## Consequences

### Positive

- Short-job terminal append is a single round-trip with no `ready_entries`
  read — the central performance claim in ADR-019.
- The `run_lease` guard still holds because it's checked in the DELETE,
  not the INSERT.
- Attempt-state rows (when they exist) are cleaned up in the same
  transaction via a guarded DELETE, so `attempt_state` invariants from
  the TLA+ model continue to hold.

### Negative

- `CompletionRequest` carries optional `runtime_job` and `claim` fields;
  a caller that forgets to populate them silently degrades to the
  re-read fallback. The dispatcher always populates them; admin paths
  consciously don't. Worth a refactor to encode shape as an enum
  (`{Bare, Claimed, Runtime}`) so the dispatch is structural rather
  than option-field-check.
- Mixed-shape batches (some `runtime_job = Some`, some `None`) would
  require partitioning before flush; currently the dispatcher always
  emits homogeneous shape per `RuntimeStorage` variant so the issue
  is latent. Still worth guarding structurally.
- Worker crash with an unflushed batch in memory: the in-flight permits
  are still held and heartbeats are still running, so rescue reclaims
  those jobs via the normal heartbeat-staleness path. There is no
  on-disk replay log for the batcher itself — the heartbeat rescue is
  the replay mechanism.

## Relationship to ADR-019 and ADR-013

ADR-019 stated the intent ("short successful completion carries the
immutable claim-time job snapshot through the completion batcher so the
terminal append path does not have to reload `ready_entries`") but didn't
pin down the batcher shape or the crash-recovery story. This ADR fills
that gap.

The `run_lease` guard that makes stale completions safe is ADR-013. The
completion batcher preserves that guard because the DELETE against
`active_leases` matches on `(job_id, run_lease)` — a stale batch entry
simply affects zero rows and is discarded.

See [ADR-019](019-queue-storage-redesign.md) and
[ADR-013](013-run-lease-and-guarded-finalization.md).
