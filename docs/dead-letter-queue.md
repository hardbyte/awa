# Dead Letter Queue

The Dead Letter Queue (DLQ) holds jobs that have exhausted all of their retry
attempts. It is the recovery surface for terminal failures: an operator can
inspect what failed, decide whether the underlying problem has been fixed,
and either retry or purge.

## When a job lands in the DLQ

Per-queue policy controls DLQ behaviour. When `dlq_enabled = true` on a
queue, a job that reaches its `max_attempts` ceiling without succeeding is
moved into `dlq_entries` rather than left as a `failed` terminal row. The
job's final error history, args, payload, and lifecycle metadata travel with
it; the DLQ row is a faithful snapshot of the job at the moment it gave up.

A queue with `dlq_enabled = false` (the default before 0.6) leaves terminal
failures as `failed` rows in the canonical / queue-storage tables. They can
still be inspected and re-queued manually, but they do not flow through the
DLQ-specific commands and dashboards.

The DLQ is **not** a job state — it is a separate physical surface
(`awa.dlq_entries` in the queue-storage schema). A job in the DLQ does not
sit on the dispatchable path and cannot be claimed by a worker until it is
retried out of the DLQ. See [ADR-020](adr/020-dead-letter-queue.md) for the
design rationale.

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

The web UI's **DLQ** tab (the one served by `awa serve`) shows the same data
with filters and per-row drill-down.

Each DLQ row carries:
- `dlq_reason` — short string identifying why the job was dead-lettered
  (e.g., `max_attempts`, `manual`, `deadline_expired`).
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
```

Retry moves the row out of `dlq_entries` and back into the runnable path
with `attempt = 0` and `run_lease = 0`, ready for fresh processing. The
retry is recorded in the job's error history; the original failure trail is
preserved.

## Moving existing terminal failures into the DLQ

If you turn on `dlq_enabled` for a queue that already has accumulated
`failed` rows, the existing terminal failures stay where they are. To pull
them into the DLQ for inspection:

```bash
# Move all failed rows on a queue into the DLQ
awa dlq move --queue billing --reason "audit_2026_q2" --all

# Move only a specific kind
awa dlq move --kind charge_card --reason "audit_2026_q2"
```

The `--reason` is recorded in `dlq_reason` so you can distinguish bulk
historical moves from runtime-driven dead-lettering.

## Purging

```bash
# Purge by filter
awa dlq purge --queue billing --kind charge_card

# Purge everything (safety guard requires --all)
awa dlq purge --all
```

Purging is destructive: the rows are deleted from `dlq_entries` and not
recoverable through the DLQ tools. Use with care; prefer `retry-bulk` if
you might want the data back.

## Retention

The maintenance leader periodically prunes DLQ rows older than the
configured retention window. See [`docs/configuration.md`](configuration.md)
for the `dlq_retention_*` knobs. Retention runs alongside the rotation /
prune work for the queue and lease rings, so a busy DLQ does not delay
queue-plane reclamation.

## Programmatic access

The Rust API exposes the same operations through `awa::admin::dlq` (or via
`awa-worker`'s `Client::dlq()` accessor). The Python bindings expose the
same surface as `client.dlq()`; both wrap the same SQL helpers the CLI uses.

## See also

- [ADR-020 — Dead Letter Queue](adr/020-dead-letter-queue.md) — design and
  trade-offs.
- [configuration.md](configuration.md) — `dlq_enabled` per queue, retention
  knobs.
- [troubleshooting.md](troubleshooting.md) — diagnosing why a particular job
  reached the DLQ.
