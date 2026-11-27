# Dead Letter Queue

The Dead Letter Queue (DLQ) holds jobs that have exhausted all of their retry attempts. It is the recovery surface for terminal failures: an operator can inspect what failed, decide whether the underlying problem has been fixed, and either retry or purge.

## When a job lands in the DLQ

Per-queue policy controls DLQ behaviour. When `dlq_enabled = true` on a queue, a job that reaches its `max_attempts` ceiling without succeeding is moved into `dlq_entries` rather than left as a `failed` terminal row. Attempts that end in a crash, a missed deadline or a callback timeout count toward that ceiling too; see [Poison jobs](concepts/job-lifecycle.md#poison-jobs). The job's final error history, args, payload, and lifecycle metadata travel with it; the DLQ row is a faithful snapshot of the job at the moment it gave up.

A queue with `dlq_enabled = false` (the default before 0.6) leaves terminal failures as `failed` rows in the canonical / queue-storage tables. They can still be inspected and re-queued manually, but they do not flow through the DLQ-specific commands and dashboards.

The DLQ is **not** a job state — it is a separate physical surface (`awa.dlq_entries` in the queue-storage schema). A job in the DLQ does not sit on the dispatchable path and cannot be claimed by a worker until it is retried out of the DLQ. See [ADR-020](adr/020-dead-letter-queue.md) for the design rationale.

## Inspecting

```bash
# Total DLQ depth across all queues, then per-queue
awa dlq depth
awa dlq depth --queue billing

# List recent DLQ rows (paginated by id descending)
awa dlq list --limit 20
awa dlq list --queue billing --kind charge_card --limit 50
awa dlq list --tag urgent --before-dlq-at 2026-04-30T00:00:00Z
```

The web UI's **DLQ** tab (the one served by `awa serve`) shows the same data with filters and per-row drill-down.

Each DLQ row carries:

- `dlq_reason` — why the job was dead-lettered:
    - `max_attempts_exhausted`: the handler failed (an error or a panic) on its last attempt.
    - `terminal_error`: the handler raised a terminal error, the arguments did not deserialize, or no worker handles the kind.
    - `heartbeat_stale`: the worker stopped heartbeating during the last attempt.
    - `deadline_exceeded`: the last attempt outlived the queue's `deadline_duration`.
    - `callback_timeout`: the last attempt's callback wait timed out.
    - Anything else is the reason an operator passed to `awa dlq move`.
- `dlq_at` — when the job entered the DLQ.
- `original_run_lease` — the run-lease the job held at the moment it failed.
- The full error history from the `payload.errors[]` array.
- Original args, payload, queue, kind, and lifecycle timestamps.

## Retrying

```bash
# Retry a single DLQ row
awa dlq retry 800042

# Retry every matching row in bulk
awa dlq retry-bulk --queue billing
awa dlq retry-bulk --kind charge_card --tag manual_review

# Retry the entire DLQ (safety guard requires --all)
awa dlq retry-bulk --all

# Wait for the retry to finish and print its counters
awa dlq retry-bulk --queue billing --wait
```

`awa dlq retry-bulk` submits a `retry_from_dlq` [batch operation](#large-sets-batch-operations) and prints its id and state; the maintenance leader retries the rows a chunk at a time while a worker runs. Follow it with `awa batch-ops get <id>`, or pass `--wait` to poll it until it finishes and print how many rows it processed, skipped and errored (the command exits non-zero if the operation fails).

Retry moves the row out of `dlq_entries` and back into the runnable path with `attempt = 0` and `run_lease = 0`, ready for fresh processing. The retry is recorded in the job's error history; the original failure trail is preserved.

The CLI retries with the stored queue, priority, and immediate `run_at`. The programmatic APIs can override those when reviving a DLQ row; use Python `retry_from_dlq(job_id, priority=..., queue=..., run_at=...)` or Rust `RetryFromDlqOpts` when recovery should promote, demote, reroute, or delay the job.

## Moving existing terminal failures into the DLQ

If you turn on `dlq_enabled` for a queue that already has accumulated `failed` rows, the existing terminal failures stay where they are. To pull them into the DLQ for inspection:

```bash
# Move all failed rows on a queue into the DLQ
awa dlq move --queue billing --reason "audit_2026_q2" --all

# Move only a specific kind
awa dlq move --kind charge_card --reason "audit_2026_q2"
```

The `--reason` is recorded in `dlq_reason` so you can distinguish bulk historical moves from runtime-driven dead-lettering.

## Purging

```bash
# Purge by filter
awa dlq purge --queue billing --kind charge_card

# Purge everything (safety guard requires --all)
awa dlq purge --all
```

`awa dlq purge` submits a `purge_dlq` batch operation the same way and takes `--wait`.

Purging is destructive: the rows are deleted from `dlq_entries` and not recoverable through the DLQ tools. Use with care; prefer `retry-bulk` if you might want the data back.

## Large sets: batch operations

A durable batch operation is applied by the maintenance leader a chunk at a time; it records each row as processed, skipped or errored, and can be previewed, watched and cancelled. A retried row whose unique key a newer job holds is recorded as errored and the rest are retried. Purging releases the purged rows' unique claims. `awa dlq retry-bulk` and `awa dlq purge` submit one; `awa dlq move` acts on every matching row in one transaction, so for a large set submit a `move_to_dlq` operation instead.

```bash
# Retry dead-lettered rows on a queue
awa batch-ops submit --op-kind retry_from_dlq --filter '{"queue":"billing"}'

# Purge dead-lettered rows of a kind
awa batch-ops submit --op-kind purge_dlq --filter '{"kind":"charge_card"}'

# Move failed rows into the DLQ with a reason
awa batch-ops submit --op-kind move_to_dlq --filter '{"queue":"billing"}' --spec '{"reason":"triage"}'
```

The same kinds are in the admin UI's batch operations page, `POST /api/batch-ops` and the Python client's `submit_batch_operation`. See [ADR-030](adr/030-batch-operations.md#bulk-action-operations) for each kind's eligible jobs.

The admin UI's DLQ page submits its **Retry matching** and **Purge matching** actions as `retry_from_dlq` and `purge_dlq` batch operations, and links to the batch operations page to follow them. `POST /api/dlq/retry-matching` and `/api/dlq/purge-matching` do the same: they take `{"filter": {...}}` (or `{"filter": {}, "all": true}` for the whole DLQ), answer `202 Accepted` with the submitted operation, and leave the rows to the maintenance leader, so the DLQ changes only while a worker runs maintenance. The 0.6 endpoints `/api/dlq/bulk-retry` and `/bulk-purge` and the Python client's `purge_dlq_before` still act in one transaction and report a count.

## Retention

The maintenance leader periodically prunes DLQ rows older than the configured retention window. See [Configuration](configuration.md) for the `dlq_retention_*` knobs. Retention runs alongside the rotation / prune work for the queue and lease rings, so a busy DLQ does not delay queue-plane reclamation.

## Programmatic access

Rust callers use `awa_model::dlq` directly, or `awa::model::dlq` through the facade crate, for DLQ list, retry, move, and purge helpers. Admin helpers such as `awa::admin::fail_to_dlq`, `move_failed_to_dlq`, and `bulk_move_failed_to_dlq` route failed terminal jobs into the DLQ.

Python callers use direct client methods: `list_dlq`, `get_dlq_job`, `dlq_depth`, `dlq_depth_by_queue`, `retry_from_dlq`, `bulk_retry_from_dlq`, `move_failed_to_dlq`, `purge_dlq`, `purge_dlq_before`, and `purge_dlq_job`. `bulk_retry_from_dlq(...)` and `purge_dlq(...)` submit a `retry_from_dlq` or `purge_dlq` batch operation (`submitted_by: "python"`) and return it as `submit_batch_operation` does; follow it with `get_batch_operation`. `purge_dlq_before(...)` deletes, in one transaction, the matching rows that sort before a `(before_dlq_at, before_id)` cursor from `list_dlq` and returns the count; it needs at least one of `before_id` and `before_dlq_at`. `purge_dlq_job(...)` purges one DLQ row by job ID.

## See also

- [ADR-020 — Dead Letter Queue](adr/020-dead-letter-queue.md) — design and trade-offs.
- [Configuration](configuration.md) — `dlq_enabled` per queue, retention knobs.
- [Troubleshooting](troubleshooting.md) — diagnosing why a particular job reached the DLQ.
