# Awa Architecture Overview

## System Overview

Awa (Maori: river) is a Postgres-native background job queue providing durable, transactional job processing for Rust and Python. Postgres is the sole infrastructure dependency -- there is no Redis, RabbitMQ, or other broker. All queue state lives in Postgres, and all coordination uses Postgres primitives: `FOR UPDATE SKIP LOCKED` for dispatch, advisory locks for leader election, `LISTEN/NOTIFY` for wakeup, and transactions for atomic enqueue.

The Rust runtime owns all queue machinery -- polling, heartbeating, crash recovery, and dispatch. Python workers are callbacks invoked by this runtime via PyO3, inheriting Rust-grade reliability without reimplementing queue internals.

## Crate Structure

```
awa (workspace)
├── awa-macros        proc-macro crate: #[derive(JobArgs)] and CamelCase→snake_case
├── awa-model         Core types, SQL, migrations, insert/admin/cron APIs
│   └── depends on: awa-macros, sqlx, blake3, serde, chrono, chrono-tz, croner
├── awa-worker        Runtime: Client, Dispatcher, Executor, Heartbeat, Maintenance, Metrics
│   └── depends on: awa-model, sqlx, tokio, opentelemetry, croner, chrono-tz
├── awa               Facade crate re-exporting awa-model + awa-worker
│   └── depends on: awa-model, awa-macros, awa-worker
├── awa-testing       Test utilities (TestClient, WorkResult)
│   └── depends on: awa-model, awa-worker
├── awa-cli           CLI binary: migrations, job/queue/cron admin
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

Jobs carry an optional `progress` JSONB column that handlers can write during execution. Progress is cleared to NULL on completion but preserved across all other transitions (retry, snooze, cancel, fail, rescue), enabling checkpoint-based resumption on retry.

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
    ├── `awa.jobs` is a compatibility view
    ├── available/immediate rows route to `awa.jobs_hot`
    ├── future `scheduled` / `retryable` rows route to `awa.scheduled_jobs`
    ├── unique_key computed via BLAKE3 (if UniqueOpts provided)
    └── TRIGGER: pg_notify('awa:<queue>', '') fires on hot available jobs

`awa.jobs` preserves raw SQL compatibility for tests, admin queries, and
non-Rust producers, but dispatch and promotion use the physical tables directly
so the planner only touches the hot runnable set on the execution path.
```

Insert accepts a `PgExecutor`, so it works inside an existing transaction -- the job becomes visible only when the outer transaction commits. This is the "transactional enqueue" pattern (PRD section 1).

### Batch Insert via COPY

For high-throughput ingestion (10K+ jobs), `insert_many_copy` uses PostgreSQL's COPY protocol via a staging table approach (see ADR-008):

```
insert_many_copy(conn, jobs)
    │
    ├── CREATE TEMP TABLE awa_copy_staging (...) ON COMMIT DROP
    ├── COPY awa_copy_staging FROM STDIN (CSV)       ◄── no triggers, no constraints
    ├── INSERT INTO awa.jobs SELECT ... FROM staging
    └── unique rows use per-row savepoints to skip duplicates without aborting the batch
        RETURNING *
```

Accepts `&mut PgConnection`, so it works within caller-managed transactions. `insert_many_copy_from_pool` is a convenience wrapper that manages its own transaction.

### Poll and Claim (Dispatcher)

Each queue has a `Dispatcher` that runs a poll loop:

```
Dispatcher::run()
    │
    ├── LISTEN awa:<queue>        (PgListener for instant wakeup)
    │
    └── loop:
        ├── Wait for NOTIFY or poll_interval (default 200ms)
        └── poll_once():
            │
            ├── Pre-acquire permits (non-blocking: semaphore or overflow pool)
            ├── Apply rate limit (truncate if throttled)
            │
            ▼
            CTE claim query:
              WITH candidates AS (
                (SELECT id, priority, run_at FROM awa.jobs_hot
                 WHERE state='available' AND queue=$1 AND priority=1 AND run_at<=now()
                 ORDER BY run_at, id LIMIT ... )
                UNION ALL ...
              ),
              claimed AS (
                SELECT jobs.id
                FROM awa.jobs_hot AS jobs
                JOIN candidates ON candidates.id = jobs.id
                ORDER BY GREATEST(1, candidates.priority - FLOOR(...)) ASC,
                         candidates.run_at, candidates.id
                LIMIT $2
                FOR UPDATE OF jobs SKIP LOCKED   ◄── concurrent-safe dispatch
              )
              UPDATE awa.jobs_hot SET state='running', attempt=attempt+1, run_lease=run_lease+1, ...
            │
            ├── Release excess permits (if DB returned fewer jobs)
            ├── Consume rate limit tokens
            ▼
            For each claimed job + permit → executor.execute(job)
```

Permits are pre-acquired before the DB claim to guarantee every `running` job has a reserved execution slot. `FOR UPDATE SKIP LOCKED` ensures that multiple workers polling the same queue never claim the same job (PRD section 6.2).

The dispatcher intentionally applies priority aging only over a bounded
candidate window rather than over the full available backlog. That keeps the
claim query planner-friendly under large hot backlogs while preserving the
fairness effect of aging.

### Execute (Executor)

```
JobExecutor::execute(job)
    │
    ▼
tokio::spawn(async {
    in_flight.insert((job_id, run_lease))  ◄── register exact attempt for heartbeat/cancel
    │
    ▼
    worker.perform(&job, &ctx)         ◄── dispatch to registered handler
    │
    ▼
    complete_job(pool, job, &result)    ◄── lease-guarded finalize, batched for Completed
    │
    ├── Ok(true):  state transitioned → record metrics
    ├── Ok(false): already rescued/cancelled → skip metrics
    └── Err:       DB error → log error
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

### Progress Tracking

Handlers can report structured progress during execution via an in-memory buffer that is flushed to Postgres on each heartbeat cycle and atomically with state transitions.

```
Handler code (sync)                    Heartbeat service (async, periodic)
─────────────────                      ────────────────────────────────────
ctx.set_progress(50, "halfway")  ──►   ProgressState { latest, generation }
ctx.update_metadata({"cursor":N}) ──►  (generation bumped on each mutation)
                                            │
                                            ▼
                                       heartbeat_once():
                                         ├── Tier 1: jobs without pending progress
                                         │   UPDATE jobs_hot SET heartbeat_at = now() ...
                                         └── Tier 2: jobs with pending progress
                                             UPDATE jobs_hot SET heartbeat_at = now(),
                                                                 progress = v.progress ...
                                             (ack generation on success)

ctx.flush_progress()             ──►   Direct UPDATE jobs_hot SET progress = $2
                                       WHERE id = $1 AND run_lease = $3
                                       (immediate, stronger than heartbeat)
```

**Storage:** The `progress` column is a nullable JSONB on both `jobs_hot` and `scheduled_jobs`, structured as `{"percent": 0-100, "message": "...", "metadata": {...}}`. The `metadata` sub-object is shallow-merged on each `update_metadata` call — top-level keys overwrite, nested objects are replaced.

**Buffer design:** Each in-flight job has an `Arc<Mutex<ProgressState>>` shared between the handler and the heartbeat service. The buffer tracks a `generation` counter (bumped on mutation) and an `acked_generation` (advanced when Postgres confirms the write). The heartbeat service snapshots pending progress into an `in_flight` field before flushing, preventing double-snapshots and enabling retry on failure without data loss. `std::sync::Mutex` is used (not tokio) because the critical section is pure in-memory JSON assembly with no async work.

**Lifecycle semantics:**

| Transition | Progress value |
|---|---|
| Completed | `NULL` (ephemeral — job succeeded) |
| RetryAfter / Retryable error | Preserved (checkpoint for next attempt) |
| Snooze | Preserved |
| Cancel | Preserved (operator inspection) |
| WaitForCallback | Preserved |
| Failed (terminal or exhausted) | Preserved (operator inspection) |
| Rescue (stale heartbeat / deadline / callback timeout) | Preserved (implicit via view trigger) |

On retry, the handler can read the previous attempt's checkpoint from `ctx.job.progress` and resume work from where it left off.

### State Guard on Completion

Every running attempt carries a durable `run_lease` token that is incremented at claim time. Heartbeats, callback registration, and finalization all match on `id`, `state = 'running'`, and `run_lease`, so a stale worker cannot mutate a newer running attempt of the same job ID. If `rows_affected() == 0`, the job was already rescued, reclaimed, or cancelled — the stale result is silently discarded. Metrics are only recorded when the guarded transition succeeds.

Successful `Completed` outcomes are flushed through a small batched finalizer. The worker does not release local in-flight tracking or capacity until the batch flush acknowledges success or stale rejection, so shutdown drain and heartbeat semantics still match the correctness model. Locally, in-flight attempts are tracked in a sharded registry keyed by `(job_id, run_lease)` rather than a single global lock, which preserves the lease model while reducing executor/heartbeat contention.

### Complete and Retry

When a retryable or scheduled job becomes due, the maintenance service moves it
from the cold deferred table back into the hot runnable table. This uses
partial due-time indexes on `awa.scheduled_jobs` and promotes small ordered
batches (`run_at ASC, id ASC`) so large scheduled frontiers do not require
scanning the execution table:

```
WITH due AS (
    DELETE FROM awa.scheduled_jobs
    WHERE id IN (
        SELECT id
        FROM awa.scheduled_jobs
        WHERE state = 'retryable' AND run_at <= now()
        ORDER BY run_at ASC, id ASC
        LIMIT ...
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
)
INSERT INTO awa.jobs_hot (...)
SELECT ..., 'available', ...
FROM due
```

Uniqueness now uses a tiny claims table (`awa.job_unique_claims`) rather than a
partial unique index on the full jobs storage. Rows only reserve a claim when
their current state is inside their `unique_states` bitmask, so the hot and
deferred tables can share one uniqueness boundary without keeping all jobs in a
single heap.

## Queue Concurrency Modes

Awa supports two concurrency modes, selected at build time (see ADR-011):

### Hard-Reserved (Default)

Each queue owns an independent semaphore with `max_workers` permits. Simple and predictable — queues cannot interfere with each other.

### Weighted (Global Pool)

Enabled by `ClientBuilder::global_max_workers(N)`. Each queue gets a guaranteed `min_workers` local semaphore plus access to a shared `OverflowPool` for additional capacity. Overflow is allocated proportionally to per-queue `weight` values using a work-conserving weighted fair-share algorithm.

The dispatcher uses a **permit-before-claim** flow: permits are pre-acquired (non-blocking) before claiming jobs from the database, ensuring every job marked `running` has a reserved execution slot.

### Per-Queue Rate Limiting

An optional token bucket rate limiter can be configured per queue (see ADR-010). When set, the dispatcher gates the batch size by available tokens, preventing downstream systems from being overwhelmed. Rate limiting composes with both concurrency modes.

## Graceful Shutdown

Shutdown uses a phased lifecycle with separate cancellation domains:

1. **Stop dispatchers** (`dispatch_cancel`) — no new jobs are claimed
2. **Signal in-flight cancellation** — handlers see `ctx.is_cancelled() == true`
3. **Wait for dispatchers** to exit their poll loops
4. **Drain in-flight jobs** via `JoinSet` — heartbeat and maintenance remain alive during drain to prevent false rescue
5. **Stop services** (`service_cancel`) — heartbeat and maintenance shut down

This ensures in-flight jobs complete (or timeout) with heartbeats still running, preventing other workers from rescuing jobs that are still actively executing.

## Periodic/Cron Jobs

Awa supports periodic job scheduling via the `PeriodicJob` API. Schedules are defined in application code, synced to an `awa.cron_jobs` table, and evaluated by the maintenance leader. See ADR-007 for design rationale.

### Registration

```rust
let client = Client::builder(pool)
    .queue("default", QueueConfig::default())
    .register::<DailyReport, _, _>(handle_daily_report)
    .periodic(
        PeriodicJob::builder("daily_report", "0 9 * * *")
            .timezone("Pacific/Auckland")
            .build(&DailyReport { format: "pdf".into() })?
    )
    .build()?;
```

```python
client.periodic(
    name="daily_report",
    cron_expr="0 9 * * *",
    args_type=DailyReport,
    args=DailyReport(format="pdf"),
    timezone="Pacific/Auckland",
)
```

Cron expressions and timezones are validated eagerly at registration time via the `croner` crate and `chrono-tz`.

### Scheduler Flow (Leader-Only)

```
MaintenanceService (leader)
    │
    ├── Every 60s: sync_periodic_jobs_to_db()
    │   └── UPSERT all registered schedules (additive, no deletes)
    │
    ├── Every 1s: evaluate_cron_schedules()
    │   ├── SELECT * FROM awa.cron_jobs
    │   ├── For each: compute latest fire time ≤ now, after last_enqueued_at
    │   └── If due: atomic CTE (mark last_enqueued_at + INSERT INTO awa.jobs)
    │
    └── Every 30s: leader liveness check
        └── SELECT 1 on leader connection (break to re-election if dead)
```

### Crash Safety

The atomic enqueue CTE combines the schedule update and job insertion into a single statement. If the process crashes mid-transaction, Postgres rolls back both. The `IS NOT DISTINCT FROM` clause on `last_enqueued_at` acts as a compare-and-swap, preventing double-fires across leader failovers.

### Multi-Deployment Safety

Sync is additive (UPSERT only). Multiple deployments sharing the same database will not delete each other's schedules. Stale schedules can be removed via `awa cron remove <name>`.

## Crash Recovery Model

Awa uses a hybrid approach with two independent crash recovery mechanisms, each catching a different failure mode (see ADR-003 for rationale).

### 1. Heartbeat Staleness (Crash Detection)

- The `HeartbeatService` runs on every worker instance (not leader-elected).
- Every 30 seconds (configurable), it batch-updates `heartbeat_at = now()` for all in-flight `(job_id, run_lease)` pairs on this worker.
- The maintenance leader (leader-elected via `pg_try_advisory_lock`) periodically scans for running jobs where `heartbeat_at < now() - 90s` and transitions them to `retryable`.
- After rescue, the maintenance service signals cancellation (`ctx.is_cancelled() == true`) for any rescued jobs still running on this worker instance.
- **Catches:** Worker process crash, OOM kill, network partition, pod eviction.

### 2. Hard Deadline (Runaway Protection)

- At claim time, `deadline_at = now() + deadline_duration` is set (default: 5 minutes).
- The maintenance leader periodically scans for running jobs where `deadline_at < now()` and transitions them to `retryable`.
- After rescue, the maintenance service signals cancellation to the in-flight handler via `ctx.is_cancelled()`, so long-running handlers can observe the deadline and exit gracefully.
- **Catches:** Infinite loops, hung I/O, deadlocks, GIL-blocked Python handlers that prevent heartbeat updates.

### Leader Election

Maintenance tasks (heartbeat rescue, deadline rescue, scheduled promotion, cleanup, cron evaluation) run on a single leader instance elected via Postgres advisory lock (`pg_try_advisory_lock(0x4157415f4d41494e)`). The lock is session-scoped -- it auto-releases if the leader's connection drops. Non-leaders retry every 10 seconds. The leader verifies its connection is still alive every 30 seconds; if the ping fails, it re-enters the election loop.

Scheduled and retryable promotion runs every 250ms by default, in bounded
batches, and emits queue notifications after promotion. Cron evaluation remains
on a 1-second tick.

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

client.periodic(                 ───►   PeriodicJob registered
    name="report", cron_expr="0 9 * * *",
    args_type=Report, args=Report(...),
    timezone="Pacific/Auckland",
)

client.start([("email", 10)])    ───►   awa_worker::Client::start()
# or dict form:                          ├── Dispatcher (`SKIP LOCKED` + LISTEN/NOTIFY)
# client.start(                          ├── HeartbeatService
#   [{"name":"email",                    ├── MaintenanceService
#     "min_workers":5,                   └── PythonWorker bridge
#     "weight":2,                            └── into_future_with_locals(...)
#     "rate_limit":(10.0, 20)}],
#   global_max_workers=30)

await client.shutdown()          ───►   phased drain + cancellation
```

Key design decisions:

- **Single runtime:** Python workers do not run a separate poller. They register callbacks, then delegate polling, heartbeats, maintenance, and shutdown to `awa-worker`.
- **Async bridge:** `pyo3_async_runtimes::tokio::future_into_py` converts Rust futures to Python awaitables. Registered Python handlers are driven from Rust via `into_future_with_locals`, using task-local event loop state captured when the handler is registered.
- **Sync support:** Every async database method has a `_sync` counterpart using `py.detach(|| block_on(...))` for Django/Flask handlers (see ADR-009). `SyncTransaction` provides `__enter__`/`__exit__` context manager support.
- **Heartbeats survive GIL blocks:** Heartbeat writes run on a dedicated Rust tokio task that never acquires the GIL. Even if a Python handler blocks the GIL (e.g., CPU-bound work in a sync call), heartbeats continue uninterrupted.
- **Type bridging:** Python dataclasses and pydantic BaseModels are serialized to `serde_json::Value` via `model_dump(mode="json")` or `dataclasses.asdict()`. On dispatch, the bridge reconstructs the typed args object before invoking the handler and exposes `job.is_cancelled()` from the shared Rust cancellation flag.

## Observability

### Tracing Spans

All components emit structured tracing spans via the `tracing` crate with `#[instrument]` and manual `info_span!`:

| Span | Location | Key Fields |
|---|---|---|
| `job.execute` | executor.rs | `job.id`, `job.kind`, `job.queue`, `job.attempt`, `otel.status_code` |
| `insert_with` | insert.rs | `job.kind`, `job.queue` |
| `insert_many` | insert.rs | `job.count` |
| `run` (dispatcher) | dispatcher.rs | `queue` |
| `poll_once` | dispatcher.rs | `queue` |
| `run` (heartbeat) | heartbeat.rs | `interval_ms` |
| `heartbeat_once` | heartbeat.rs | — |
| `maintenance.rescue_stale` | maintenance.rs | — |
| `maintenance.rescue_deadline` | maintenance.rs | — |
| `maintenance.promote` | maintenance.rs | — |
| `maintenance.cleanup` | maintenance.rs | — |
| `maintenance.cron_sync` | maintenance.rs | — |
| `maintenance.cron_eval` | maintenance.rs | — |

The `job.execute` span records `otel.status_code = "OK"` on success or `"ERROR"` on terminal failure, compatible with OpenTelemetry trace semantics.

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
- **Graceful shutdown:** `Client::shutdown(timeout)` uses a phased approach: stops dispatchers first (no new claims), then drains in-flight jobs while heartbeat and maintenance remain alive to prevent false rescue, then shuts down background services. In Kubernetes, set `terminationGracePeriodSeconds` to match the drain timeout.
- **No sticky state:** Workers can be rescheduled to any node. The Postgres connection pool is the only external dependency.
- **Queue assignment:** Different deployments can handle different queues by configuring `ClientBuilder::queue()`, enabling workload isolation (e.g., CPU-heavy jobs on dedicated node pools).
