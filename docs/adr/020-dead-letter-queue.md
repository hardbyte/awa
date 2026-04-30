# ADR-020: First-Class Dead Letter Queue

## Status

Accepted

## Context

The queue-storage redesign keeps runnable work, lease churn, and control-plane
metadata on separate paths. Permanently failed jobs still need a durable,
operator-facing home that is not part of the claim path and that can outlive
normal failed-row retention windows.

Without a dedicated DLQ:

- terminal failures stay mixed with ordinary terminal history
- forensic retention competes with routine cleanup
- retry and purge tooling has no explicit operator target
- UI, CLI, and Python admin surfaces cannot distinguish "historical failure"
  from "actionable dead-letter entry"

In the queue-storage model, the right home for that state is a separate
append-only table inside the active queue-storage schema.

## Decision

Add a first-class Dead Letter Queue as `{schema}.dlq_entries` inside the active
queue-storage backend. DLQ entries are not claimable. They are populated from
terminal failure paths or explicit admin moves, retained independently from the
main terminal history, and surfaced consistently through Rust, Python, CLI,
REST, and Web UI APIs.

### Separate table, not a new live state

DLQ rows are stored in `{schema}.dlq_entries`, not in `ready_entries`,
`deferred_jobs`, or `terminal_entries`, and not as a new dispatchable
`job_state`.
The row keeps the failed job snapshot plus:

- `dlq_reason`
- `dlq_at`
- `original_run_lease`

This keeps dead-letter history off the runnable and lease-maintenance paths.

### Population paths

There are two ways DLQ rows are created:

1. Runtime routing for terminal failures on DLQ-enabled queues.
2. Operator-initiated bulk or single-row moves of already-failed terminal rows.

Runtime routing is the primary path. Terminal failures and exhausted
callback-timeout failures move directly into `dlq_entries` so the active
attempt leaves the hot execution plane immediately.

Admin moves operate on already-failed terminal history, allowing operators to
backfill older failures into the DLQ after enabling policy on an existing
queue.

### Retry and purge

Retry deletes the DLQ row and reinserts a fresh job into queue storage:

- immediate retry inserts a new ready entry
- future `run_at` retry inserts a deferred entry
- `attempt` resets to `0`
- `run_lease` resets to `0`
- attempt-scoped mutable execution data is cleared

Purge deletes the DLQ row permanently.

Bulk retry and purge require at least one of `kind`, `queue`, or `tag`, unless
the caller explicitly opts in with `allow_all=true`. This prevents empty-filter
 actions from reviving or deleting the entire DLQ by mistake.

### Retention

DLQ retention is independent from ordinary terminal retention.

- default global DLQ retention: `30 days`
- per-queue override: `RetentionPolicy.dlq`
- maintenance performs bounded cleanup passes against `dlq_entries`

This keeps forensic retention separate from the shorter-lived cleanup horizon
for normal completed/failed/cancelled history.

### Admin surfaces

The DLQ is a first-class operator surface:

- CLI: `awa dlq ...`
- Python: `list_dlq`, `get_dlq_job`, `retry_from_dlq`, bulk retry/move/purge
- REST: `/api/dlq...`
- Web UI: `/dlq` list and `/dlq/:id` detail

`GET /api/jobs/:id` also exposes DLQ metadata when a job has already moved to
the DLQ, so direct links and admin lookups continue to work after routing.

## Consequences

### Positive

- dead-letter history stays off the runnable path
- DLQ retention is decoupled from normal terminal retention
- retry/purge operations have an explicit, auditable target
- UI, CLI, and Python tooling can treat DLQ rows as a first-class operator
  state rather than inferring from generic failed history

### Negative

- operators now have two terminal-history surfaces to understand:
  ordinary terminal history and DLQ history
- admin tooling must deliberately surface when a job has moved to the DLQ
- bulk actions need stronger safety guards because the DLQ is intentionally
  operator-facing and long-lived

## References

- [019-queue-storage-redesign](019-queue-storage-redesign.md)
- `awa-model/src/dlq.rs`
- `awa-model/src/queue_storage.rs`
- `awa-worker/src/executor.rs`
- `awa-worker/src/maintenance.rs`
