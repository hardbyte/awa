# awa-worker

Worker runtime for the [Awa](https://crates.io/crates/awa) Postgres-native job queue.

This crate provides:
- `Client` / `ClientBuilder` — configure queues, register workers, start the runtime
- `JobContext` — handler context with cancellation, progress tracking, callback registration
- `JobEvent<T>` / `UntypedJobEvent` — post-commit lifecycle hooks for cleanup and notifications
- `JobResult` / `JobError` — handler return types (completed, retry, snooze, cancel, wait-for-callback)
- `QueueConfig` — per-queue concurrency, rate limiting, weighted mode
- Dispatcher, heartbeat, maintenance, and completion batcher internals

You don't usually need to depend on this crate directly — [`awa`](https://crates.io/crates/awa) re-exports everything.

## Cancellation Semantics

Cancellation in Awa is cooperative.

- Rust handlers can poll `ctx.is_cancelled()`.
- Python handlers can poll `job.is_cancelled()`.
- The runtime flips that flag for:
  - graceful shutdown
  - stale-heartbeat rescue
  - deadline rescue

That lets long-running handlers stop work gracefully and return an explicit
result like `JobResult::Cancel(...)`, `JobResult::RetryAfter(...)`, or
`awa.Cancel(...)` in Python.

There is an important distinction between:

- **handler/runtime cancellation signals**
  - the in-memory cancellation flag becomes `true`
  - the handler can observe cancellation while it is still running
- **admin/job-state cancellation**
  - `admin::cancel(...)` / `client.cancel(...)` marks the job `cancelled` in storage
  - pending or waiting jobs transition immediately
  - a running handler is not currently guaranteed to see its in-memory
    cancellation flag flipped by admin cancel alone

If a running job is cancelled in storage and the handler keeps running, its
later completion/retry/cancel attempt is treated as stale and ignored.

## License

MIT OR Apache-2.0
