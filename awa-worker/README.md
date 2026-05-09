# awa-worker

Worker runtime for the [Awa](https://crates.io/crates/awa) Postgres-native
job queue: dispatch, claim, heartbeat, completion-batching, maintenance,
and lifecycle hooks.

Most Rust applications depend on the [`awa`](https://crates.io/crates/awa)
facade and never reach for this crate directly. Use `awa-worker` when
you need the runtime types without the re-export shim — typically when
you are building higher-level frameworks or alternate transports on
top of Awa.

## What's in here

- **Client** — `Client` and `ClientBuilder` configure queues, register
  handlers, attach lifecycle hooks, and start the runtime. `HealthCheck`,
  `QueueHealth`, `QueueCapacity`, and `TransitionWorkerRole` expose the
  observability and transition-aware capabilities.
- **Job context** — `JobContext` (with cancellation, structured
  progress, and callback registration), `CallbackToken`,
  `CallbackGuard`.
- **Handler results** — `JobResult` (`Completed`, `RetryAfter`,
  `Snooze`, `Cancel`, `WaitForCallback`) and `JobError`. Implement
  the `Worker` trait directly or register typed closures with
  `register_handler`.
- **Queue configuration** — `QueueConfig` (concurrency, weighted mode,
  rate limiting, per-claim deadlines), `RateLimit`.
- **Lifecycle hooks** — `JobEvent<T>` and `UntypedJobEvent` fire when
  handler execution starts and after guarded finalization commits, useful
  for cache invalidation, notifications, and metrics emission.
- **HTTP worker** — `HttpWorker`, `HttpWorkerConfig`, `HttpWorkerMode`
  dispatch jobs to serverless endpoints over HTTP with HMAC-BLAKE3
  signing. See [ADR-018](../docs/adr/018-http-worker.md).
- **Maintenance** — the elected maintenance leader runs rescue, promotion,
  queue/lease/claim ring rotation and prune, DLQ cleanup, descriptor cleanup,
  cron evaluation, and queue-health publication.
- **Metrics** — `AwaMetrics` exposes the runtime metric surface for
  Prometheus / OTel scrapers.

## Capabilities

- **Vacuum-aware queue storage** — workers default to the queue-storage
  engine: append-only ready/terminal partitions, rotating lease and receipt
  rings, and separate deferred/DLQ tables described in
  [ADR-019](../docs/adr/019-queue-storage-redesign.md) and
  [ADR-023](../docs/adr/023-receipt-plane-ring-partitioning.md).
- **Dead Letter Queue** — terminal failures land in `dlq_entries` for
  any queue with `dlq_enabled` set. Per-queue policy is configured
  through `QueueConfig` and the `dlq_enabled_by_default` builder
  setting; see [`docs/dead-letter-queue.md`](../docs/dead-letter-queue.md).
- **Descriptor catalog** — `ClientBuilder::queue_descriptor` and
  `job_kind_descriptor` declare display name, owner, tags, and docs
  URL alongside the worker. The runtime syncs these to
  `queue_descriptors` / `job_kind_descriptors` on start
  ([ADR-022](../docs/adr/022-descriptor-catalog.md)).
- **Per-claim deadlines** — `QueueConfig::deadline_duration` writes
  `lease_claims.deadline_at` on claim. Expired claims are force-closed
  by the rescue path with `'deadline_expired'`.
- **Priority aging** — applied at claim time on the queue-storage engine
  ([ADR-005](../docs/adr/005-priority-aging.md)).
- **Heartbeat + deadline rescue** — two independent rescue paths cover
  crash and runaway failure modes
  ([ADR-003](../docs/adr/003-heartbeat-deadline-hybrid.md)).

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

- **handler cancellation signals**
  - the in-memory cancellation flag becomes `true`
  - the handler can observe cancellation while it is still running
- **admin/job-state cancellation**
  - `admin::cancel(...)` / `client.cancel(...)` marks the job `cancelled` in storage
  - pending or waiting jobs transition immediately
  - if the exact running attempt is still alive on a worker process, the
    matching handler also sees its in-memory cancellation flag flip

If a running job is cancelled in storage and the handler keeps running, its
later completion/retry/cancel attempt is treated as stale and ignored.

## See also

- [Architecture overview](../docs/architecture.md)
- [Configuration reference](../docs/configuration.md)
- [Deployment](../docs/deployment.md)

## License

MIT OR Apache-2.0
