# Bulk admin operations

Admin operations that change every job matching a filter run in bounded batches from the CLI, the web UI, the HTTP API and Python. No transaction changes more than `batch_size` rows, so a large retry, drain, discard, move or purge no longer holds locks on every matching row at once or leaves one very large transaction for vacuum.

## The model

- **Per-transaction bound.** Each batch is its own transaction of at most `batch_size` rows. The default is 1,000 (`awa_model::bulk::DEFAULT_BATCH_SIZE`), the same size `drain_queue` has always used on queue storage.
- **Total cap.** `limit` stops a run after that many rows have changed. Without it, the run continues until no matching row is left.
- **Order.** Rows are taken in job ID order, and a cursor records how far the run has got, so each batch starts where the previous one stopped.
- **Scope.** A run covers the rows that matched when it started. Its first batch records the next job ID in the cursor (`before_job_id`), and no batch of the run touches a job with that ID or a higher one, so jobs enqueued during the run are left alone and a run against a busy producer still finishes. A job retried by the run that fails again is behind the cursor and is not retried a second time. A job that existed when the run started but only came to match during it, such as a running job that fails into the DLQ, is included if the run has not yet passed its ID.
- **Not atomic.** Rows changed by earlier batches stay changed if a later batch fails or the run is stopped. Every surface accounts for them: metrics are recorded and the web UI's caches invalidated as each batch commits, the CLI prints the rows changed and the jobs skipped before it reports the error, Python exceptions carry the counts, and in Rust `run_batches` and the explicit-ID calls return a `BulkError` holding what committed.
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

Every command above takes `--batch-size` (default 1,000) and `--limit`. Each batch prints a progress line to stderr and the total goes to stdout. The command exits 2 when it skipped jobs over a unique conflict, naming the first 10, and 1 when it gave up on rows another transaction held; when both happen it names the skipped jobs and exits 1.

`awa batch-ops purge` keeps its old behaviour by default: one call that purges at most 1,000 operations, or `--limit` operations. `--batch-size N` purges in transactions of `N` until none match. `--limit` is now hidden from help but still accepted.

### HTTP

The bulk routes take an optional `batch_size`. Without it, the server runs every batch and returns `count`, `skipped`, `conflicts` and `done`. With it, the route runs one batch, resuming at an optional `cursor`, and also returns `busy` and the `cursor` for the next request; the web UI uses this to show progress and to stop between batches. A unique conflict no longer fails the request: it is reported in `skipped` and `conflicts`. The drain route takes `batch_size` and the cursor's `stage`, `after_job_id` and `before_job_id` as query parameters and still returns `drained`. `GET /api/dlq/count` counts matching DLQ rows up to 10,000 and reports `capped` above that.

The explicit-ID routes `/api/jobs/bulk-retry` and `/api/jobs/bulk-cancel` work in transactions of 1,000 IDs (250 for cancel on queue storage). `/api/jobs/bulk-retry` now returns an object, `{jobs, skipped, conflicts}`, instead of an array of jobs, so a client sees which jobs a live job's unique key blocked; `/api/jobs/bulk-cancel` still returns the cancelled jobs. A job missing from either response was not changed. Every route rejects a `batch_size` above 10,000 with 400, including the drain route's query string; the CLI and Python reject one too, at parse time.

### Python

`discard_failed`, `drain_queue`, `bulk_retry_from_dlq`, `bulk_move_failed_to_dlq` and `purge_dlq` run in batches of `batch_size` (1,000 when unset) and return the count. When rows were skipped over a unique conflict, the call raises `UniqueConflict` after the run, naming the skipped jobs. When it gives up on held rows, it raises `DatabaseError`. Any exception a batched call raises, including a database error partway through, has `count`, `skipped` and `conflicts` attributes describing the batches that committed before it.

`retry_failed` keeps its single transaction when `batch_size` is unset, because its result lists every retried job. With `batch_size`, it runs in batches; the result's `jobs` is empty and `matched` is the retried count.

### Rust

The library functions that take a filter or an ID list and change every match, such as `admin::retry_failed_by_kind`, `admin::bulk_retry`, `dlq::bulk_retry_from_dlq` and `dlq::purge_dlq`, keep their signatures and still run in one transaction in 0.6. Their batched counterparts take `batch_size` and a `BulkCursor` and return a `BulkBatch`; `bulk::run_batches` drives a run with a total `limit`, the busy wait and a progress callback. In 0.7 the library functions run in batches too.

Bulk DLQ calls ignore `ListDlqFilter::limit`. It is the page size of `list_dlq`, so a caller that reuses a list filter for a bulk call would otherwise stop at one page. Cap a run with `BulkRunOptions::limit` or the CLI's `--limit`.

## Engines

On canonical storage, a batch updates its rows with one statement. When that statement hits a unique conflict, the batch is redone one row at a time, each in a savepoint, so only the conflicting rows are skipped.

On queue storage, `drain_queue_batch` cancels at most `batch_size` jobs per call, like every other batch, and takes the queue's stripes (`queue#N`) with it. Waiting jobs are cancelled through the per-job cancel. Scheduled and retryable jobs are cancelled a chunk at a time, and the cursor stops before one another transaction holds. Available jobs are cancelled from each lane's claim cursor; a batch that finds only rows another transaction holds reports `busy`. The library function `admin::drain_queue` still drains the whole queue in one call.

Cancel by ID on queue storage works in transactions of at most 250 jobs (`queue_storage::CANCEL_BY_ID_CHUNK`, applied under any larger `batch_size`), in job ID order with duplicates removed. Cancelling by ID costs several milliseconds a job on a large lane, and the transaction holds the lanes' claim heads, so 1,000 jobs held them for seconds.

### Pause between batches on queue storage

A queue-storage batch reads the ring partitions and holds `ACCESS SHARE` on them until it commits. Ring prune needs `ACCESS EXCLUSIVE` on a sealed partition and waits for it at most 320 ms before it reports `blocked`. Batches run back to back leave it no gap, so a long run stops ring prune for its whole length: cancelling a 200,000-job lane by ID in 1,000-job chunks of about 7.7 s each kept the claim ring from advancing for 495 s, with every claim-ring prune `blocked`. This is the [prune starvation under claim load](troubleshooting.md#ring-prune-stays-blocked-under-claim-load) of #721, caused by one operator run.

`run_batches` therefore waits 100 ms (`bulk::DEFAULT_QUEUE_STORAGE_PAUSE`) after each queue-storage batch that made progress, and the explicit-ID calls wait as long between chunks. A prune that starts in the gap gets its lock on its first try. Canonical storage has no rings to prune, so its batches do not pause. Set `BulkRunOptions::pause_between_batches` to change the pause, or to `Some(Duration::ZERO)` to turn it off.

A drain reads more than one table, and a job can move between them while it runs: a waiting job whose callback times out becomes retryable, and a scheduled or retryable job is promoted to available. On both engines the drain takes waiting jobs first, then scheduled and retryable jobs, then available jobs, so a job that moves is reached after it moves. Discard on queue storage takes failed jobs before DLQ rows, the direction a failed job moves.

## Not batched

- `admin::refresh_admin_metadata` and `recompute_dirty_admin_metadata` are SQL functions in the schema; changing them needs a schema patch.
- `awa storage rebuild-terminal-counters` rebuilds the counters in one transaction so they are never read half-rebuilt.
- The storage transition (`awa storage prepare`, `enter-mixed-transition`, `finalize`, `abort`) and lane sequence repair are migrations of the deployment, not bulk admin.
- Retention cleanup, `dlq::cleanup_dlq`, rescue, promotion, prune and the batch operations runner were already bounded.
