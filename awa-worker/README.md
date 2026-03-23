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

## License

MIT OR Apache-2.0
