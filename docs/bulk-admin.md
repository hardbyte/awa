# Bulk admin operations

Admin operations that change every job matching a filter run in bounded batches from the CLI, the web UI, the HTTP API and Python. No transaction changes more than `batch_size` rows, so a large retry, drain, discard, move or purge no longer holds locks on every matching row at once or leaves one very large transaction for vacuum.

## The model

- **Per-transaction bound.** Each batch is its own transaction of at most `batch_size` rows. The default is 1,000 (`awa_model::bulk::DEFAULT_BATCH_SIZE`), the same size `drain_queue` has always used on queue storage.
- **Total cap.** `limit` stops a run after that many rows have changed. Without it, the run continues until no matching row is left.
- **Order.** Rows are taken in job ID order, and a cursor records how far the run has got, so each batch starts where the previous one stopped.
- **Not atomic.** Rows changed by earlier batches stay changed if a later batch fails or the run is stopped.
- **Unique conflicts are skipped and reported.** A job whose retry would violate a unique claim held by a live job is skipped, counted and left as it was, and the run carries on. The first 10 skipped job IDs are reported. The cursor moves past a skipped job, so a run tries it once; a new run tries it again.
- **Held rows are never skipped.** A batch skips rows another transaction holds (`FOR UPDATE SKIP LOCKED`), so two runs over the same rows never change a row twice. The cursor never moves past a held row: when every remaining matching row is held, the batch changes nothing and reports `busy`, and the run waits and tries again. It gives up after 60 seconds without progress.

## Operations

| Operation | CLI | HTTP | Python | Rust batch function |
| --- | --- | --- | --- | --- |
| Retry failed jobs by kind or queue | `awa job retry-failed` | — | `retry_failed(..., batch_size=)` | `admin::retry_failed_by_kind_batch`, `retry_failed_by_queue_batch` |
| Discard failed jobs by kind | `awa job discard` | — | `discard_failed(kind, batch_size=)` | `admin::discard_failed_batch` |
| Drain a queue | `awa queue drain` | `POST /api/queues/{queue}/drain` | `drain_queue(queue, batch_size=)` | `admin::drain_queue_batch` |
| Retry or cancel jobs by ID | — | `POST /api/jobs/bulk-retry`, `/api/jobs/bulk-cancel` | — | `admin::bulk_retry_in_batches`, `bulk_cancel_in_batches` |
| Retry DLQ rows | `awa dlq retry-bulk` | `POST /api/dlq/bulk-retry` | `bulk_retry_from_dlq(..., batch_size=)` | `dlq::bulk_retry_from_dlq_batch` |
| Move failed jobs into the DLQ | `awa dlq move` | `POST /api/dlq/bulk-move` | `bulk_move_failed_to_dlq(..., batch_size=)` | `dlq::bulk_move_failed_to_dlq_batch` |
| Purge DLQ rows | `awa dlq purge` | `POST /api/dlq/bulk-purge` | `purge_dlq(..., batch_size=)` | `dlq::purge_dlq_batch` |
| Purge finished batch operations | `awa batch-ops purge --batch-size` | — | — | `batch_operations::purge_batch_operations_before` |

### CLI

Every command above takes `--batch-size` (default 1,000) and `--limit`. Each batch prints a progress line to stderr and the total goes to stdout. The command exits 2 when it skipped jobs over a unique conflict, naming the first 10, and 1 when it gave up on rows another transaction held.

`awa batch-ops purge` keeps its old behaviour by default: one call that purges at most 1,000 operations, or `--limit` operations. `--batch-size N` purges in transactions of `N` until none match. `--limit` is now hidden from help but still accepted.

### HTTP

The bulk routes take an optional `batch_size`. Without it, the server runs every batch and returns `count`, `skipped`, `conflicts` and `done`. With it, the route runs one batch, resuming at an optional `cursor`, and also returns `busy` and the `cursor` for the next request; the web UI uses this to show progress and to stop between batches. A unique conflict no longer fails the request: it is reported in `skipped` and `conflicts`. The drain route takes `batch_size`, `stage` and `after_job_id` as query parameters and still returns `drained`. `GET /api/dlq/count` counts matching DLQ rows up to 10,000 and reports `capped` above that.

The explicit-ID routes `/api/jobs/bulk-retry` and `/api/jobs/bulk-cancel` work in transactions of 1,000 IDs and still return the changed jobs. A job missing from the response was not changed, for example because a live job holds its unique key.

### Python

`discard_failed`, `drain_queue`, `bulk_retry_from_dlq`, `bulk_move_failed_to_dlq` and `purge_dlq` run in batches of `batch_size` (1,000 when unset) and return the count. When rows were skipped over a unique conflict, the call raises `UniqueConflict` after the run, naming the skipped jobs. When it gives up on held rows, it raises `DatabaseError`.

`retry_failed` keeps its single transaction when `batch_size` is unset, because its result lists every retried job. With `batch_size`, it runs in batches; the result's `jobs` is empty and `matched` is the retried count.

### Rust

The library functions that take a filter or an ID list and change every match, such as `admin::retry_failed_by_kind`, `admin::bulk_retry`, `dlq::bulk_retry_from_dlq` and `dlq::purge_dlq`, keep their signatures and still run in one transaction in 0.6. Their batched counterparts take `batch_size` and a `BulkCursor` and return a `BulkBatch`; `bulk::run_batches` drives a run with a total `limit`, the busy wait and a progress callback. In 0.7 the library functions run in batches too.

Bulk DLQ calls ignore `ListDlqFilter::limit`. It is the page size of `list_dlq`, so a caller that reuses a list filter for a bulk call would otherwise stop at one page. Cap a run with `BulkRunOptions::limit` or the CLI's `--limit`.

## Engines

On canonical storage, a batch updates its rows with one statement. When that statement hits a unique conflict, the batch is redone one row at a time, each in a savepoint, so only the conflicting rows are skipped.

On queue storage, `drain_queue_batch` drains the whole queue in one call, in transactions of `batch_size` jobs, and returns `done`; it skips jobs another transaction holds, as it did before.

## Not batched

- `admin::refresh_admin_metadata` and `recompute_dirty_admin_metadata` are SQL functions in the schema; changing them needs a schema patch.
- `awa storage rebuild-terminal-counters` rebuilds the counters in one transaction so they are never read half-rebuilt.
- The storage transition (`awa storage prepare`, `enter-mixed-transition`, `finalize`, `abort`) and lane sequence repair are migrations of the deployment, not bulk admin.
- Retention cleanup, `dlq::cleanup_dlq`, rescue, promotion, prune and the batch operations runner were already bounded.
