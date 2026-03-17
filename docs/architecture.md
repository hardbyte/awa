# Awa Architecture Overview

## System Overview

Awa (Maori: river) is a Postgres-native background job queue providing durable, transactional job processing for Rust and Python. Postgres is the sole infrastructure dependency -- there is no Redis, RabbitMQ, or other broker. All queue state lives in Postgres, and all coordination uses Postgres primitives: `FOR UPDATE SKIP LOCKED` for dispatch, advisory locks for leader election, `LISTEN/NOTIFY` for wakeup, and transactions for atomic enqueue.

The Rust runtime owns all queue machinery -- polling, heartbeating, crash recovery, and dispatch. Python workers are callbacks invoked by this runtime via PyO3, inheriting Rust-grade reliability without reimplementing queue internals.

## Crate Structure

```
awa (workspace)
├── awa-macros        proc-macro crate: #[derive(JobArgs)] and CamelCase→snake_case
├── awa-model         Core types, SQL, migrations, insert/admin APIs
│   └── depends on: awa-macros, sqlx, blake3, serde, chrono
├── awa-worker        Runtime: Client, Dispatcher, Executor, Heartbeat, Maintenance, Metrics
│   └── depends on: awa-model, sqlx, tokio, opentelemetry
├── awa               Facade crate re-exporting awa-model + awa-worker
│   └── depends on: awa-model, awa-macros, awa-worker
├── awa-testing       Test utilities (TestClient, WorkResult)
│   └── depends on: awa-model, awa-worker
├── awa-cli           CLI binary: migrations, job/queue admin
│   └── depends on: awa-model, clap
└── awa-python        PyO3 cdylib: Python bindings (separate Cargo workspace)
    └── depends on: awa-model, awa-worker, pyo3, pyo3-async-runtimes
```

### Dependency Graph

```
awa-macros  (proc-macro, no runtime deps)
    │
    ▼
awa-model   (core types + SQL, re-exports awa-macros::JobArgs)
    │
    ├──────────────┐
    ▼              ▼
awa-worker     awa-cli
    │
    ├──────────────┐
    ▼              ▼
awa (facade)   awa-testing
                   │
                   ▼
              awa-python (PyO3 bridge, separate workspace)
```

`awa-python` is excluded from the main workspace because it has its own `pyproject.toml` and build toolchain (maturin).

## Job Lifecycle State Machine

As defined in PRD section 6.1, jobs follow this state machine:

```
INSERT ──► scheduled ──► available ──► running ──► completed
               │              ▲           │
               │              │           ├──► retryable ──► available (via promotion)
               │              │           │
               │              │           ├──► failed (max attempts exhausted or terminal error)
               │              │           │
               │              │           └──► cancelled (by handler or admin)
               │              │
               │              └── (promotion: run_at <= now())
               │
               └── (run_at in future)
```

**States:**

| State | Description |
|---|---|
| `scheduled` | Future-dated job, waiting for `run_at` |
| `available` | Ready for dispatch |
| `running` | Claimed by a worker, executing |
| `completed` | Successfully finished (terminal) |
| `retryable` | Failed but eligible for retry after backoff |
| `failed` | Exhausted max attempts or terminal error (terminal) |
| `cancelled` | Cancelled by handler or admin (terminal) |

Terminal states (`completed`, `failed`, `cancelled`) have no further transitions. The maintenance service eventually deletes them based on configurable retention periods (default: 24h for completed, 72h for failed/cancelled).

## Data Flow

### Insert (Producer)

```
Application code
    │
    ▼
awa_model::insert() / insert_with() / insert_many()
    │
    ▼
INSERT INTO awa.jobs (...) VALUES (...)
    │
    ├── state = 'available' (if run_at is now or omitted)
    ├── state = 'scheduled' (if run_at is in the future)
    ├── unique_key computed via BLAKE3 (if UniqueOpts provided)
    └── TRIGGER: pg_notify('awa:<queue>', job_id) fires on available+immediate jobs
```

Insert accepts a `PgExecutor`, so it works inside an existing transaction -- the job becomes visible only when the outer transaction commits. This is the "transactional enqueue" pattern (PRD section 1).

### Poll and Claim (Dispatcher)

Each queue has a `Dispatcher` that runs a poll loop:

```
Dispatcher::run()
    │
    ├── LISTEN awa:<queue>        (PgListener for instant wakeup)
    │
    └── loop:
        ├── Wait for NOTIFY or poll_interval (default 200ms)
        ├── Check semaphore permits (max_workers limit)
        └── poll_once():
            │
            ▼
            CTE claim query (claim.sql):
              WITH claimed AS (
                SELECT id FROM awa.jobs
                WHERE state='available' AND queue=$1 AND run_at<=now()
                ORDER BY GREATEST(1, priority - FLOOR(...)) ASC, run_at, id
                LIMIT $2
                FOR UPDATE SKIP LOCKED       ◄── concurrent-safe dispatch
              )
              UPDATE awa.jobs SET state='running', attempt=attempt+1, ...
            │
            ▼
            For each claimed job → executor.execute(job)
```

`FOR UPDATE SKIP LOCKED` ensures that multiple workers polling the same queue never claim the same job (PRD section 6.2).

### Execute (Executor)

```
JobExecutor::execute(job)
    │
    ▼
tokio::spawn(async {
    in_flight.insert(job_id)           ◄── register for heartbeat
    │
    ▼
    worker.perform(&job, &ctx)         ◄── dispatch to registered handler
    │
    ▼
    complete_job(pool, job, result)     ◄── UPDATE state based on outcome
    │
    ├── Ok(Completed)    → state = 'completed'
    ├── Ok(RetryAfter)   → state = 'retryable', run_at = now() + duration
    ├── Ok(Snooze)       → state = 'scheduled', attempt -= 1
    ├── Ok(Cancel)       → state = 'cancelled'
    ├── Err(Terminal)    → state = 'failed'
    └── Err(Retryable)   → state = 'retryable' (with backoff) or 'failed' (if max attempts)
    │
    ▼
    in_flight.remove(job_id)
})
```

Backoff uses a database-side function `awa.backoff_duration(attempt, max_attempts)` implementing exponential backoff with jitter, capped at 24 hours (PRD section 6.3).

### Complete and Retry

When a retryable job's backoff elapses, the maintenance service promotes it back to `available`:

```
UPDATE awa.jobs SET state = 'available'
WHERE state = 'retryable' AND run_at <= now()
```

## Crash Recovery Model

Awa uses a hybrid approach with two independent crash recovery mechanisms, each catching a different failure mode (see ADR-003 for rationale).

### 1. Heartbeat Staleness (Crash Detection)

- The `HeartbeatService` runs on every worker instance (not leader-elected).
- Every 30 seconds (configurable), it batch-updates `heartbeat_at = now()` for all in-flight job IDs on this worker.
- The maintenance leader (leader-elected via `pg_try_advisory_lock`) periodically scans for running jobs where `heartbeat_at < now() - 90s` and transitions them to `retryable`.
- **Catches:** Worker process crash, OOM kill, network partition, pod eviction.

### 2. Hard Deadline (Runaway Protection)

- At claim time, `deadline_at = now() + deadline_duration` is set (default: 5 minutes).
- The maintenance leader periodically scans for running jobs where `deadline_at < now()` and transitions them to `retryable`.
- **Catches:** Infinite loops, hung I/O, deadlocks, GIL-blocked Python handlers that prevent heartbeat updates.

### Leader Election

Maintenance tasks (heartbeat rescue, deadline rescue, scheduled promotion, cleanup) run on a single leader instance elected via Postgres advisory lock (`pg_try_advisory_lock(0x4157415f4d41494e)`). The lock is session-scoped -- it auto-releases if the leader's connection drops. Non-leaders retry every 10 seconds.

## Python Integration via PyO3

The `awa-python` crate provides a native Python module (`_awa`) built with PyO3 and maturin:

```
Python process                         Rust runtime inside `awa-python`
──────────────────────────────────     ─────────────────────────────────────

client = awa.Client(url)        ───►   PgPool::connect()
await client.insert(...)        ───►   raw INSERT / migration helpers
await client.health_check()     ───►   awa_worker::Client::health_check()

@client.worker(SendEmail, queue="email")
async def handle(job):                  handler + task locals registered
    ...

client.start([("email", 10)])    ───►   awa_worker::Client::start()
                                         ├── Dispatcher (`SKIP LOCKED` + LISTEN/NOTIFY)
                                         ├── HeartbeatService
                                         ├── MaintenanceService
                                         └── PythonWorker bridge
                                             └── into_future_with_locals(...)

await client.shutdown()          ───►   graceful drain + cancellation
```

Key design decisions:

- **Single runtime:** Python workers do not run a separate poller. They register callbacks, then delegate polling, heartbeats, maintenance, and shutdown to `awa-worker`.
- **Async bridge:** `pyo3_async_runtimes::tokio::future_into_py` converts Rust futures to Python awaitables. Registered Python handlers are driven from Rust via `into_future_with_locals`, using task-local event loop state captured when the handler is registered.
- **Heartbeats survive GIL blocks:** Heartbeat writes run on a dedicated Rust tokio task that never acquires the GIL. Even if a Python handler blocks the GIL (e.g., CPU-bound work in a sync call), heartbeats continue uninterrupted.
- **Type bridging:** Python dataclasses and pydantic BaseModels are serialized to `serde_json::Value` via `model_dump(mode="json")` or `dataclasses.asdict()`. On dispatch, the bridge reconstructs the typed args object before invoking the handler and exposes `job.is_cancelled()` from the shared Rust cancellation flag.

## Observability

### Structured Logging

All components emit structured tracing spans via the `tracing` crate. Key span fields: `job_id`, `kind`, `queue`, `attempt`, `error`.

### OpenTelemetry Metrics

The `AwaMetrics` struct (in `awa-worker/src/metrics.rs`) publishes OTel metrics via the global meter provider. Callers configure their exporter (Prometheus, OTLP, etc.) before starting the client.

| Metric | Type | Description |
|---|---|---|
| `awa.jobs.inserted` | Counter | Total jobs inserted |
| `awa.jobs.completed` | Counter | Total jobs completed successfully |
| `awa.jobs.failed` | Counter | Total jobs that failed terminally |
| `awa.jobs.retried` | Counter | Total jobs marked retryable |
| `awa.jobs.cancelled` | Counter | Total jobs cancelled |
| `awa.jobs.claimed` | Counter | Total jobs claimed for execution |
| `awa.jobs.duration` | Histogram | Job execution duration in seconds |
| `awa.jobs.in_flight` | UpDownCounter | Current in-flight jobs |
| `awa.heartbeat.batches` | Counter | Total heartbeat batch updates |
| `awa.maintenance.rescues` | Counter | Total jobs rescued by maintenance |

All metrics carry `awa.job.kind` and `awa.job.queue` attributes for per-job-type and per-queue dashboards.

### Queue Statistics (SQL)

The `stats.sql` query and `admin::queue_stats()` function provide per-queue depth, lag, and throughput metrics directly from Postgres, suitable for Grafana dashboards or CLI monitoring (`awa queue stats`).

## Deployment Model

Awa workers are stateless processes. All state lives in Postgres.

### Kubernetes

```
┌──────────────────────────────────────────────┐
│ Kubernetes Cluster                           │
│                                              │
│  ┌─────────────┐  ┌─────────────┐           │
│  │ Worker Pod 1 │  │ Worker Pod 2 │  ...     │
│  │  Dispatcher  │  │  Dispatcher  │          │
│  │  Heartbeat   │  │  Heartbeat   │          │
│  │  Maintenance │  │  Maintenance │          │
│  └──────┬───────┘  └──────┬───────┘          │
│         │                  │                  │
│         └────────┬─────────┘                  │
│                  ▼                            │
│         ┌────────────────┐                    │
│         │   PostgreSQL   │                    │
│         │  (awa schema)  │                    │
│         └────────────────┘                    │
└──────────────────────────────────────────────┘
```

- **Horizontal scaling:** Add more worker pods. `SKIP LOCKED` ensures no double dispatch.
- **Leader election:** Only one pod runs maintenance tasks at a time via `pg_try_advisory_lock`. If the leader dies, another pod acquires the lock within 10 seconds.
- **Graceful shutdown:** `Client::shutdown(timeout)` cancels all tasks and waits for in-flight jobs to complete. In Kubernetes, set `terminationGracePeriodSeconds` to match the drain timeout.
- **No sticky state:** Workers can be rescheduled to any node. The Postgres connection pool is the only external dependency.
- **Queue assignment:** Different deployments can handle different queues by configuring `ClientBuilder::queue()`, enabling workload isolation (e.g., CPU-heavy jobs on dedicated node pools).
