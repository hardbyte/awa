# Dead Letter Queue

The Dead Letter Queue (DLQ) holds jobs that have exhausted all of their retry attempts. It is the recovery surface for terminal failures: an operator can inspect what failed, decide whether the underlying problem has been fixed, and either retry or purge.

## When a job lands in the DLQ

Per-queue policy controls DLQ behaviour. When `dlq_enabled = true` on a queue, a job that reaches its `max_attempts` ceiling without succeeding is moved into `dlq_entries` rather than left as a `failed` terminal row. Attempts that end in a handler panic, a crash, a missed deadline or a callback timeout count toward that ceiling too. The job's final error history, args, payload, and lifecycle metadata travel with it; the DLQ row is a faithful snapshot of the job at the moment it gave up.

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

# Retry every matching row, 1,000 rows per transaction
awa dlq retry-bulk --queue billing
awa dlq retry-bulk --kind charge_card --tag manual_review

# Retry the entire DLQ (safety guard requires --all)
awa dlq retry-bulk --all

# Smaller transactions, and stop after 50,000 rows in total
awa dlq retry-bulk --queue billing --batch-size 500 --limit 50000
```

Retry moves the row out of `dlq_entries` and back into the runnable path with `attempt = 0` and `run_lease = 0`, ready for fresh processing. The retry is recorded in the job's error history; the original failure trail is preserved.

### Draining a large DLQ

Bulk DLQ commands follow the shared [bulk admin model](bulk-admin.md).

`retry-bulk` retries in batches: each batch is its own transaction of at most `--batch-size` rows (1,000 by default), taken in job ID order, and the command repeats until no matching row is left or `--limit` rows have been retried. The run is not atomic: rows retried by earlier batches stay retried if a later batch fails. Batches skip rows another transaction holds without passing them, so two drains of the same filter never retry a row twice and a held row is never left behind; when every remaining matching row is held, for example by another drain's batch that has not committed, the command waits and tries again, and gives up with exit code 1 after 60 seconds without progress.

If a row's unique key is held by a live job, that row is skipped and stays in the DLQ, and the rest of the run carries on. The command prints how many rows it skipped and the first 10 of their job IDs, and exits with code 2. Retry or purge those jobs on their own once the live job finishes.

Prefer batches over calling `retry` once per job. Each retry is a separate enqueue into the job's lane and waits for that lane's in-flight enqueues to commit, so running many single-job retries in parallel slows the producers on the same queue. A batch revives each lane's rows with one reservation.

The web UI's **Retry matching** action on the DLQ page retries in batches of 1,000 the same way, shows how many rows it has retried, can be stopped between batches, and names skipped jobs when it finishes. The HTTP route `POST /api/dlq/bulk-retry` runs every batch itself and returns `{"count", "skipped", "conflicts", "done"}`. With `batch_size` it runs one batch from an optional `cursor` and also returns the `cursor` for the next request; `"busy": true` means every remaining matching row is held by another transaction and the request should be repeated after a short wait.

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

Purging is destructive: the rows are deleted from `dlq_entries` and not recoverable through the DLQ tools. Use with care; prefer `retry-bulk` if you might want the data back.

## Retention

The maintenance leader periodically prunes DLQ rows older than the configured retention window. See [`docs/configuration.md`](configuration.md) for the `dlq_retention_*` knobs. Retention runs alongside the rotation / prune work for the queue and lease rings, so a busy DLQ does not delay queue-plane reclamation.

## Programmatic access

Rust callers use `awa_model::dlq` directly, or `awa::model::dlq` through the facade crate, for DLQ list, retry, move, and purge helpers. `dlq::bulk_retry_from_dlq_batch(pool, &filter, allow_all, batch_size, cursor)` retries one batch and returns a `BulkBatch` with `count`, `skipped`, `conflicts`, `busy`, `done` and the `cursor` for the next batch; `awa::model::bulk::run_batches` drives a whole run. `bulk_retry_from_dlq` still retries every matching row in one transaction and aborts on a unique conflict; in 0.7 it runs in batches. Bulk DLQ calls ignore `ListDlqFilter::limit`, which is the page size of `list_dlq`; cap a run with `BulkRunOptions::limit`.

The module-level functions look up the active queue-storage schema and build a new `QueueStorage` on each call. Long-running tooling that makes many calls, such as a script that retries DLQ rows one at a time, should build one `QueueStorage` with `QueueStorage::from_existing_schema` and call its methods, such as `retry_from_dlq` and `bulk_retry_from_dlq_batch`, instead. Admin helpers such as `awa::admin::fail_to_dlq`, `move_failed_to_dlq`, and `bulk_move_failed_to_dlq` route failed terminal jobs into the DLQ.

Python callers use direct client methods: `list_dlq`, `get_dlq_job`, `dlq_depth`, `dlq_depth_by_queue`, `retry_from_dlq`, `bulk_retry_from_dlq`, `move_failed_to_dlq`, `purge_dlq`, and `purge_dlq_job`. `purge_dlq(...)` is the bulk purge helper; `purge_dlq_job(...)` purges one DLQ row by job ID. `bulk_retry_from_dlq(...)` retries every matching row, `batch_size` rows per transaction (1,000 by default), and returns the count. A row whose unique key a live job holds is skipped; after every other row is retried it raises `UniqueConflict` naming the skipped jobs. Rows another transaction holds are waited for, for up to 60 seconds, then it raises `DatabaseError`. These wrap the same SQL helpers the CLI uses.

## See also

- [ADR-020 — Dead Letter Queue](adr/020-dead-letter-queue.md) — design and trade-offs.
- [configuration.md](configuration.md) — `dlq_enabled` per queue, retention knobs.
- [troubleshooting.md](troubleshooting.md) — diagnosing why a particular job reached the DLQ.
