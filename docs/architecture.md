# Awa Architecture Overview

## System Overview

Awa (Māori: river) is a Postgres-native background job queue providing durable, transactional job processing for Rust and Python. Postgres is the sole infrastructure dependency -- there is no Redis, RabbitMQ, or other broker. All queue state lives in Postgres, and all coordination uses Postgres primitives: `FOR UPDATE SKIP LOCKED` for dispatch, advisory locks for leader election, `LISTEN/NOTIFY` for wakeup, and transactions for atomic enqueue.

The Rust runtime owns all queue machinery -- polling, heartbeating, crash recovery, and dispatch. Python workers are callbacks invoked by this runtime via PyO3, inheriting Rust-grade reliability without reimplementing queue internals.

## Storage Engine

Queue storage is Awa's worker engine. It is organised into three planes — queue
data, execution state, and control metadata — with append-only tables on the
queue plane and rotating segments for lease churn.

```text
              Producers (Rust, Python)
                       │
                       ▼
 ┌─────────────────────────────────────────────────────────┐
 │                    Queue plane (append-only)            │
 │   ready_entries ──claim──► active_leases ──complete──►  │
 │                               │                         │
 │                               ├──retry──► deferred_jobs │
 │                               │              │          │
 │                               │              └─promote──┤
 │                               ├──fail────► dlq_entries  │
 │                               └──complete─► terminal_entries
 └─────────────────────────┬───────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
     Execution plane   Control plane   Maintenance leader
     active_leases     lane_state      promote / rescue /
     attempt_state     *_segments      rotate / prune /
     (optional)        *_cursor        dlq retention
```

Each queue-plane family (`ready_entries`, `deferred_jobs`, `terminal_entries`,
`dlq_entries`) rotates through segmented partitions so long-lived history does
not sit in one mutable heap. `active_leases` is its own rotating family with a
fast reclaim cycle. `attempt_state` is created lazily and only for jobs that
need mutable per-attempt data (progress, callback state).

ADR-019 is the source of truth for storage internals:

- [ADR-019: Queue Storage Engine](adr/019-queue-storage-redesign.md)

This overview focuses on the current runtime boundaries and subsystems. The
migration and compatibility surfaces for older SQL entry points are documented
in [migrations.md](migrations.md). ADR-019 is the storage-design source of
truth when this overview needs more detail.

### Queue storage at a glance

- queue plane: append-only `ready_entries`, `deferred_jobs`,
  `terminal_entries`, and `dlq_entries`
- execution plane: narrow `active_leases` plus optional per-attempt
  `attempt_state`
- control plane: `lane_state` plus ready/lease segment cursor tables
- maintenance leader: promotion, rescue, rotation, prune, DLQ retention, and
  queue-health publication

## Control-plane descriptors

Awa keeps two operator-facing descriptor catalogs, distinct from per-job payload metadata:

- `awa.queue_descriptors` — labels and ownership for queues
- `awa.job_kind_descriptors` — labels and ownership for job kinds

Descriptors are **code-declared by whichever runtime is hosting the workers** — either the Rust `ClientBuilder` or the Python `AsyncClient`. Both use the same catalog tables and the same hashing, so a mixed Rust + Python fleet produces consistent descriptors. See [Configuration → Queue and job-kind descriptors](configuration.md#queue-and-job-kind-descriptors) for the declaration surface in each language.

At startup and on every runtime snapshot tick, each worker upserts the descriptors it declares and refreshes a `last_seen_at` plus a BLAKE3 `descriptor_hash` over canonicalized (sorted-key) JSON of the descriptor fields. The admin API and UI render friendly names, descriptions, tags, docs links, and owner fields from the catalog, and derive two health signals from the snapshot stream:

- **stale** — no live runtime has refreshed the descriptor within its expected snapshot window, so whatever is in the catalog is out-of-date with production
- **drift** — two or more live runtimes are reporting different descriptor hashes for the same queue or kind (typical during a rolling deploy where old and new code disagree on ownership or docs URL)

The source-of-truth split matters because the three concerns have different lifecycles and writers:

- **descriptor payloads** — owned by application code; live in the dedicated catalog tables
- **descriptor liveness and drift** — derived at read time from per-runtime hash snapshots in `awa.runtime_instances`, so they don't need their own writer path
- **mutable queue control state** (pause/resume, paused_by, …) — owned by operators; stays in `awa.queue_meta`, which is also on the dispatcher hot path and therefore kept narrow

Declared-but-empty queues and kinds still appear in the admin surfaces because the catalog is authoritative; before descriptors existed, listings were driven by `queue_state_counts`, so an idle-but-declared queue would disappear from the UI.

### Catalog retention

The maintenance leader garbage-collects the catalog on the normal cleanup
cycle: descriptor rows whose `last_seen_at` is older than the configured
`descriptor_retention` (default 30 days) are deleted. This keeps long-running
fleets from accumulating descriptors for retired queues and kinds — a worker
rollout that stops declaring `legacy_thing` drops that row within 30 days
instead of showing it as permanently stale forever.

Retention is tunable via `ClientBuilder::descriptor_retention` (Rust) or
`AsyncClient.start(..., descriptor_retention_days=...)` (Python); passing
`Duration::ZERO` / `0` disables cleanup for operators who manage the catalog
externally.

Runtime liveness rows in `awa.runtime_instances` are unrelated and already
garbage-collected at a shorter 24h horizon. A stale k8s pod name can only
contribute to drift detection for ~30s after the pod dies, and drops out of
the table entirely within a day.

### Performance profile

The descriptor surface is deliberately off the hot path:

- **Dispatcher, claim query, completion batcher, heartbeat, maintenance rescue** — none of these touch `awa.queue_descriptors`, `awa.job_kind_descriptors`, or the descriptor-hash columns on `awa.runtime_instances`. The hot path now hits queue-storage lane / lease / attempt-state tables plus `awa.queue_meta`, so descriptor sync still stays off the execution path.
- **Startup and steady-state sync** — `ClientBuilder::build()` / `AsyncClient.start()` and every `runtime_snapshot_interval` tick (default 10 s) call `sync_queue_descriptors` / `sync_job_kind_descriptors`. Both are batched: all declared descriptors go into a single multi-row `INSERT ... ON CONFLICT` statement (chunked at 5000 rows to stay well under Postgres' 65k-parameter limit). Measured end-to-end against a local Postgres: ~2 ms / 10 descriptors, ~4.5 ms / 100, ~8 ms / 500, ~24 ms / 2000. That's a single round-trip per call at realistic fleet sizes and the per-descriptor cost drops sharply with batch size (from ~200 µs at n=10 to ~12 µs at n=2000 as the fixed round-trip overhead amortises). Sync runs on a separate pool connection from the dispatcher, so it cannot starve job processing.
- **BLAKE3 hash cost** — hashes are computed per descriptor on each tick from the canonicalized JSON body. For a ~200 byte descriptor this is well under 1 µs; the total hash work per tick stays in the low-microsecond range even for hundreds of descriptors.
- **Read side** — `admin::queue_overviews` and `admin::job_kind_overviews` grew a CTE that scans `runtime_instances` and `CROSS JOIN LATERAL jsonb_each_text(...)` on the per-runtime hash columns. Measured at 0.2 ms against 100 queues + 34 live-runtime rows (buffer-cache resident). The computation is O(live_runtimes × declared_descriptors_per_runtime), so very large fleets (≥1000 runtimes × ≥500 descriptors) will want a materialised view here, but the read path already sits behind the `/api/queues` cache layer so this is bounded by TTL rather than polling frequency.
- **Storage** — each descriptor row is ~200 bytes; 100 queues + 500 kinds = ~120 KB. Per runtime row, the two new JSONB hash columns are ~100 bytes per declared descriptor (64-char hex + key), so a runtime declaring 600 descriptors carries ~60 KB of hash snapshot. A 100-worker fleet publishing 600 descriptors each is ~6 MB of `runtime_instances` payload.
- **Migration cost** — `v009_descriptors` creates two tables (with `CHECK` constraints: non-empty names, 200-char name limits, 2000-char description limit, 2048-char docs URL, ≤20 tags, positive `sync_interval_ms`, ≤128-char descriptor hash) and adds two JSONB columns (`NOT NULL DEFAULT '{}'::jsonb`) to `awa.runtime_instances`. On Postgres 11+ the `ADD COLUMN` with a constant default is metadata-only — no table rewrite — so it's instant even on large `runtime_instances` tables.

## Crate Structure

```text
awa (workspace)
├── awa-macros        proc-macro crate: #[derive(JobArgs)] and CamelCase→snake_case
├── awa-model         Core types, SQL, migrations, insert/admin/cron APIs
├── awa-worker        Runtime: Client, Dispatcher, Executor, Heartbeat, Maintenance, Metrics
├── awa               Facade crate re-exporting awa-model + awa-worker
├── awa-testing       Test utilities (TestClient, WorkResult)
├── awa-ui            Web UI: axum REST API + embedded React/TypeScript frontend
├── awa-cli           CLI binary: migrations, job/queue/cron admin, web UI server
└── awa-python        PyO3 cdylib: Python bindings (separate Cargo workspace)
```

`awa-model` is the foundation — everything depends on it. `awa-worker` adds the runtime (dispatch, heartbeat, maintenance). `awa` is a facade re-exporting both. `awa-ui` and `awa-cli` are leaf crates for the web dashboard and CLI respectively. `awa-python` lives in a separate Cargo workspace with its own `pyproject.toml` and maturin build toolchain.

## Job Lifecycle State Machine

Jobs follow this state machine:

```text
INSERT ──► scheduled ──► available ──► running ──► completed
               │              ▲           │
               │              │           ├──► retryable ──► available (via promotion)
               │              │           │
               │              │           ├──► waiting_external ──► running ──► ...
               │              │           │            │
               │              │           │            ├──► completed/failed/retryable/cancelled
               │              │           │            └──► running (resume_external)
               │              │           │         (external callback / sequential wait)
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
| `waiting_external` | Parked for external callback completion or sequential resume |

Terminal states (`completed`, `failed`, `cancelled`) do not transition within
the normal dispatch lifecycle. The maintenance service eventually deletes them
based on configurable retention periods (default: 24h for completed, 72h for
failed/cancelled). Admin tooling can additionally move `failed` rows to the
DLQ (via `MoveFailedToDlq`) and retry DLQ rows back into `ready_entries` with
`attempt = 0` and `run_lease = 0` — see the Dead Letter Queue section below.

The Dead Letter Queue is not a dispatchable `job_state`. On DLQ-enabled queues,
terminal failures are materialized as separate rows in queue-storage
`dlq_entries`, preserving the failed snapshot plus DLQ metadata while keeping
that history off the runnable path. See [ADR-020](adr/020-dead-letter-queue.md).

Jobs carry an optional `progress` JSONB column that handlers can write during execution. Progress is cleared to NULL on completion but preserved across all other transitions (retry, snooze, cancel, fail, rescue), enabling checkpoint-based resumption on retry.

## Dead Letter Queue

Queue storage keeps DLQ rows in a dedicated append-only table,
`{schema}.dlq_entries`.

- Runtime routing moves terminal failures and exhausted callback-timeout
  attempts into `dlq_entries` on DLQ-enabled queues.
- Admin moves can backfill already-failed terminal rows into the DLQ after a
  queue opts in.
- Retry deletes the DLQ row and reinserts a fresh ready or deferred entry with
  `attempt = 0` and `run_lease = 0`.
- Purge deletes the DLQ row permanently.

This separation lets Awa keep forensic failure history out of the hot claim and
lease paths while giving operators an explicit retry/purge surface in the CLI,
REST API, Python bindings, and Web UI.

## Data Flow

### Insert (Producer)

```text
Application code
    │
    ▼
awa_model::insert() / insert_with() / insert_many()
    │
    ▼
enqueue into queue storage
    │
    ├── immediate rows append into `{schema}.ready_entries`
    ├── future `scheduled` / `retryable` rows append into `{schema}.deferred_jobs`
    ├── uniqueness claims live in `awa.job_unique_claims`
    └── `pg_notify('awa:<queue>', '')` wakes dispatchers for newly-ready work
```

Immediate jobs become immutable ready entries; deferred jobs become immutable
deferred rows; only the lease/control plane remains mutable. Producer-facing
compatibility SQL is part of the upgrade surface, not the storage model, and
is covered in [migrations.md](migrations.md).

Insert accepts a `PgExecutor`, so it works inside an existing transaction — the
job becomes visible only when the outer transaction commits. This is the
transactional enqueue pattern.

### Batch Insert via COPY

For high-throughput ingestion (10K+ jobs), `insert_many_copy` uses PostgreSQL's
COPY protocol via a staging table approach (see ADR-008):

```text
insert_many_copy(conn, jobs)
    │
    ├── CREATE TEMP TABLE IF NOT EXISTS pg_temp.awa_copy_staging (...) ON COMMIT DELETE ROWS
    ├── TRUNCATE pg_temp.awa_copy_staging
    ├── COPY pg_temp.awa_copy_staging FROM STDIN (CSV)
    ├── route staged rows through `awa.insert_job_compat(...)`
    │     into the active queue-storage schema
    └── unique rows use per-row savepoints to skip duplicates without aborting the batch
        RETURNING *
```

The staging table is session-local and reused across transactions so repeated
COPY calls avoid temp-table catalog churn. Accepts `&mut PgConnection`, so it
works within caller-managed transactions. `insert_many_copy_from_pool` is a
convenience wrapper that manages its own transaction.

### Poll and Claim (Dispatcher)

Each queue has a `Dispatcher` that runs a poll loop:

```text
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
            Claim query:
              lock lane state for queue + priority
              lock the lease segment cursor FOR SHARE
              read the next runnable entry from the current ready segment
              INSERT an active lease in the current lease segment
              hydrate the runtime job from the immutable ready entry + lease
            │
            ├── Release excess permits (if DB returned fewer jobs)
            ├── Consume rate limit tokens
            ▼
            For each claimed job + permit → executor.execute(job)
```

Permits are pre-acquired before the DB claim to guarantee every `running` job
has a reserved execution slot. The claim path is cursor-based rather than a
scan over a mutable heap: `claim_seq` on each lane chooses the next lane-local
ready entry, while the lease insert records the exact `(job_id, run_lease,
ready_slot, ready_generation, lease_slot, lease_generation)` snapshot used by
completion and rescue.

Dispatch still uses strict priority ordering by `(queue, priority, lane_seq)`.
Cross-priority fairness is handled separately by the maintenance leader's
priority-aging task, which lowers priority values for long-waiting ready work.

Ready segments stay append-only, so MVCC churn is concentrated in rotating
lease segments and the optional `attempt_state` table. That moves Awa off the
old “one hot heap row per lifecycle transition” failure mode, but Postgres
discipline still matters: long-lived readers can pin old lease or terminal
segments and delay prune, so keep analytical reads short and prefer replicas
for long-running read-only work.

### Execute (Executor)

```text
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
    complete_job(pool, job, &result)    ◄── lease-guarded finalize using the claim snapshot
    │
    ├── Ok(true):  state transitioned → record metrics
    ├── Ok(false): already rescued/cancelled → skip metrics
    └── Err:       DB error → log error
    │
    ├── Ok(Completed)    → append `terminal_entries`, delete the active lease
    ├── Ok(RetryAfter)   → append `deferred_jobs`, delete the active lease
    ├── Ok(Snooze)       → append `deferred_jobs` without advancing attempt
    ├── Ok(Cancel)       → append terminal `cancelled`
    ├── Err(Terminal)    → append terminal `failed` or route to `dlq_entries`
    └── Err(Retryable)   → append deferred retry or terminal failure, then release lease
    │
    ▼
    in_flight.remove(job_id)
})
```

Backoff uses a database-side function `awa.backoff_duration(attempt,
max_attempts)` implementing exponential backoff with jitter, capped at 24
hours. See [ADR-003](adr/003-heartbeat-deadline-hybrid.md) for the crash
recovery design that drives retry timing.

### Progress Tracking

Handlers can report structured progress during execution via an in-memory
buffer that is flushed to Postgres on each heartbeat cycle and atomically with
state transitions.

Three flush paths:

1. **Heartbeat piggyback** — on every heartbeat cycle, jobs with pending
   progress updates get a combined lease-heartbeat + `attempt_state` upsert.
   Jobs without changes still use the heartbeat-only path. At most two queries
   per cycle regardless of job count.

2. **State-transition atomic** — when `complete_job()` runs, the latest
   progress snapshot is carried into the same queue-storage transaction that
   appends terminal/deferred rows and removes the lease.

3. **Explicit flush** — `ctx.flush_progress()` upserts
   `{schema}.attempt_state(job_id, run_lease, progress)` guarded by the active
   lease. This is the reliable path for critical checkpoints.

```text
ctx.set_progress(50, "halfway")       ──► ProgressState.latest updated, generation bumped
ctx.update_metadata({"cursor": N})    ──► metadata shallow-merged, generation bumped

heartbeat_once()                      ──► if generation > acked_generation:
                                            heartbeat active lease
                                            upsert attempt_state.progress (batched)
                                            ack generation on success

ctx.flush_progress()                  ──► direct attempt_state upsert, ack generation on success

complete_job(result, progress_snapshot) ──► progress included in the terminal/deferred append
```

**Storage:** While a job is actively running, progress lives in
`{schema}.attempt_state.progress` keyed by `(job_id, run_lease)`. When the
attempt leaves the execution path, the latest snapshot is copied into the
payload on the deferred / terminal row. On successful completion that durable
payload snapshot is the retained record, while the live `attempt_state` row is
deleted, which is why the lifecycle table below shows `Completed` as `NULL`.
Short jobs can therefore complete without ever allocating `attempt_state`,
while long-running or callback-heavy jobs still have durable checkpoints.

**Buffer design:** Each in-flight job has an `Arc<Mutex<ProgressState>>` shared
between the handler and the heartbeat service. The buffer tracks a
`generation` counter (bumped on mutation) and an `acked_generation` (advanced
when Postgres confirms the write). The heartbeat service snapshots pending
progress into an `in_flight` field before flushing, preventing double-snapshots
and enabling retry on failure without data loss. `std::sync::Mutex` is used
(not tokio) because the critical section is pure in-memory JSON assembly with
no async work.

**Lifecycle semantics:**

| Transition | Progress value |
|---|---|
| Completed | `NULL` (ephemeral — job succeeded) |
| RetryAfter / Retryable error | Preserved (checkpoint for next attempt) |
| Snooze | Preserved |
| Cancel | Preserved (operator inspection) |
| WaitForCallback | Preserved |
| Failed (terminal or exhausted) | Preserved (operator inspection) |
| Rescue (stale heartbeat / deadline / callback timeout) | Preserved |

On retry, the handler can read the previous attempt's checkpoint from
`ctx.job.progress` and resume work from where it left off.

### State Guard on Completion

Every running attempt carries a durable `run_lease` token that is incremented
at claim time. Heartbeats, callback registration, and finalization all match
on `job_id` and `run_lease`, so a stale worker cannot mutate a newer running
attempt of the same job ID. If the guarded update affects zero rows, the job
was already rescued, reclaimed, or cancelled — the stale result is silently
discarded. Metrics are only recorded when the guarded transition succeeds.

Successful `Completed` outcomes are flushed through a small batched finalizer.
The worker does not release local in-flight tracking or capacity until the
batch flush acknowledges success or stale rejection, so shutdown drain and
heartbeat semantics still match the correctness model. Locally, in-flight
attempts are tracked in a sharded registry keyed by `(job_id, run_lease)`
rather than a single global lock, which preserves the lease model while
reducing executor/heartbeat contention.

### External Callbacks and Sequential Waits

External callback support has two related execution patterns:

1. `JobResult::WaitForCallback` parks the job in `waiting_external` and
   releases the handler task.
2. `ctx.wait_for_callback(token)` / `job.wait_for_callback(token)` parks the
   job in `waiting_external` but keeps the same handler task alive so it can
   resume in-process and continue with later steps.

Sequential waits work like this:

```text
register_callback(callback_id)
  -> wait_for_callback(callback_id)
  -> state = waiting_external

resume_external(callback_id, payload)
  -> state = running
  -> callback_id cleared
  -> payload stored in metadata._awa_callback_result

wait_for_callback(...)
  -> consumes metadata._awa_callback_result
  -> continues handler execution
```

Two details matter for correctness:

- `wait_for_callback` is token-specific. It only waits on the callback ID it
  registered and rejects stale tokens once a new callback is registered.
- `resume_external` is accepted while the job is still `running` as well as
  `waiting_external`, so an early callback can win the race before the handler
  finishes its transition into `waiting_external`.

This is the behavior captured by the callback TLA+ model.

### HTTP Callback Receiver

`awa-ui` can expose callback receiver endpoints for `HttpWorker` and other
external systems:

- `POST /api/callbacks/:callback_id/complete`
- `POST /api/callbacks/:callback_id/fail`
- `POST /api/callbacks/:callback_id/heartbeat`

When `AWA_CALLBACK_HMAC_SECRET` (or `--callback-hmac-secret`) is configured on
`awa serve`, these endpoints require a valid `X-Awa-Signature` header derived
from the callback ID using the shared 32-byte BLAKE3 key.

### Promotion (Scheduled → Available)

Future-dated and retryable jobs live in `{schema}.deferred_jobs` until their
`run_at` time arrives. The maintenance leader promotes due rows in bounded
batches by materializing fresh ready entries in the current ready segment:

```sql
WITH due AS (
    DELETE FROM {schema}.deferred_jobs
    WHERE ctid IN (
        SELECT ctid
        FROM {schema}.deferred_jobs
        WHERE state IN ('scheduled', 'retryable') AND run_at <= now()
        ORDER BY run_at ASC, job_id ASC
        LIMIT ...
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
)
INSERT INTO {schema}.ready_entries (...)
SELECT ..., 'available', ...
FROM due
```

### Uniqueness

Jobs can declare uniqueness constraints via `UniqueOpts`. The unique key is a
BLAKE3 hash of the job kind plus optional queue, args, and time-period
components. A separate `awa.job_unique_claims` table holds one row per active
claim, enforced by a unique index.

Each job carries a `unique_states` bitmask (BIT(8)) specifying which states
count as "active" for uniqueness purposes (default: scheduled, available,
running, completed, retryable). Queue storage acquires or releases
`awa.job_unique_claims` as it appends ready/deferred/terminal/DLQ rows, so the
uniqueness boundary survives retries and storage rotation without one giant
mutable jobs heap.

## Queue Concurrency Modes

Awa supports two concurrency modes, selected at build time. See [ADR-011](adr/011-weighted-concurrency.md) for design rationale.

### Hard-Reserved (Default)

Each queue owns an independent semaphore with `max_workers` permits. Simple and predictable — queues cannot interfere with each other.

### Weighted (Global Pool)

Enabled by `ClientBuilder::global_max_workers(N)`. Each queue gets a guaranteed `min_workers` local semaphore plus access to a shared `OverflowPool` for additional capacity. Overflow is allocated proportionally to per-queue `weight` values using a work-conserving weighted fair-share algorithm.

The dispatcher uses a **permit-before-claim** flow: permits are pre-acquired (non-blocking) before claiming jobs from the database, ensuring every job marked `running` has a reserved execution slot.

### Per-Queue Rate Limiting

An optional token bucket rate limiter can be configured per queue. See [ADR-010](adr/010-rate-limiting.md). When set, the dispatcher gates the batch size by available tokens, preventing downstream systems from being overwhelmed. Rate limiting composes with both concurrency modes.

## Graceful Shutdown

Shutdown uses a phased lifecycle with two cancellation domains (`dispatch_cancel` and `service_cancel`):

1. **Cancel dispatchers** (`dispatch_cancel`) — stop claiming new jobs
2. **Signal in-flight cancellation flags** — handlers see `ctx.is_cancelled() == true`
3. **Wait for dispatchers to exit** — each dispatcher returns its in-flight `JoinSet`
4. **Drain all returned JoinSets with timeout** — heartbeat and maintenance remain alive during drain to prevent false rescue
5. **Stop background services** (`service_cancel`) — heartbeat and maintenance shut down

This ensures in-flight jobs complete (or timeout) with heartbeats still running, preventing other workers from rescuing jobs that are still actively executing.

## Periodic/Cron Jobs

Awa supports periodic job scheduling via the `PeriodicJob` API. Schedules are defined in application code, synced to an `awa.cron_jobs` table, and evaluated by the maintenance leader. See [ADR-007](adr/007-periodic-cron-jobs.md) for design rationale.

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

### Lifecycle Hooks

Builder-side lifecycle hooks let applications react to committed job outcomes
without growing the `Worker` trait surface. See
[ADR-015](adr/015-post-commit-lifecycle-hooks.md) for the design rationale:

```rust
let client = Client::builder(pool)
    .queue("default", QueueConfig::default())
    .register::<SendEmail, _, _>(handle_email)
    .on_event::<SendEmail, _, _>(|event| async move {
        if let awa::JobEvent::Exhausted { args, error, .. } = event {
            tracing::error!(to = %args.to, error = %error, "email job exhausted retries");
        }
    })
    .build()?;
```

Hooks are best-effort, post-commit notifications. They run in detached tasks
after the job's in-flight permit has been released — a slow or panicking hook
cannot block queue capacity or delay other jobs. `shutdown()` does not wait
for hook tasks to complete; in-flight hooks may be dropped during shutdown.
If the side effect must be durable or retried, enqueue another job instead.

### Scheduler Flow (Leader-Only)

```text
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

Awa uses a hybrid approach with two independent crash recovery mechanisms, each catching a different failure mode. See [ADR-003](adr/003-heartbeat-deadline-hybrid.md) for rationale.

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
- **Catches:** Infinite loops, hung I/O, deadlocks, and other runaway handlers even when the worker process is still alive and heartbeating.

### Leader Election

Maintenance tasks (heartbeat rescue, deadline rescue, scheduled promotion, cleanup, cron evaluation, priority aging) run on a single leader instance elected via Postgres advisory lock (`pg_try_advisory_lock(0x4157415f4d41494e)`). The lock is session-scoped -- it auto-releases if the leader's connection drops. Non-leaders retry every 10 seconds. The leader verifies its connection is still alive every 30 seconds; if the ping fails, it re-enters the election loop.

Scheduled and retryable promotion runs every 250ms by default, in bounded
batches, and emits queue notifications after promotion. Cron evaluation remains
on a 1-second tick.

## Python Integration

The `awa-python` crate provides a native Python module built with PyO3 and maturin. Python workers are callbacks invoked by the Rust runtime — they don't run a separate poller, heartbeat, or maintenance service. All queue machinery is delegated to `awa-worker`.

Key properties:

- **Async + sync** — every async method has a `_sync` counterpart for Django/Flask (see [ADR-009](adr/009-python-sync-support.md))
- **Heartbeats survive GIL blocks** — heartbeat writes run on a dedicated Rust tokio task that never acquires the GIL
- **Type bridging** — Python dataclasses and pydantic BaseModels round-trip through `serde_json::Value`
- **Full feature parity** — progress tracking, callbacks, cron, weighted concurrency, rate limiting all available from Python

See [ADR-004](adr/004-pyo3-async-bridge.md) for the async bridge design.

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
| `maintenance.rescue_callback_timeout` | maintenance.rs | — |
| `maintenance.cron_sync` | maintenance.rs | — |
| `maintenance.cron_eval` | maintenance.rs | — |

The `job.execute` span records `otel.status_code = "OK"` on success or `"ERROR"` on terminal failure, compatible with OpenTelemetry trace semantics.

### OpenTelemetry Metrics

The `AwaMetrics` struct (in `awa-worker/src/metrics.rs`) publishes OTel metrics via the global meter provider. Callers configure their exporter (Prometheus, OTLP, etc.) before starting the client.

| Metric | Type | Unit | Description |
|---|---|---|---|
| `awa.job.inserted` | Counter | `{job}` | Number of jobs inserted |
| `awa.job.completed` | Counter | `{job}` | Number of jobs completed successfully |
| `awa.job.failed` | Counter | `{job}` | Number of jobs that failed terminally |
| `awa.job.retried` | Counter | `{job}` | Number of jobs marked retryable |
| `awa.job.cancelled` | Counter | `{job}` | Number of jobs cancelled |
| `awa.job.claimed` | Counter | `{job}` | Number of jobs claimed for execution |
| `awa.job.waiting_external` | Counter | `{job}` | Number of jobs parked for external callback |
| `awa.job.duration` | Histogram | `s` | Job execution duration |
| `awa.job.in_flight` | UpDownCounter | `{job}` | Current in-flight jobs |
| `awa.dispatch.claim_batches` | Counter | `{batch}` | Number of dispatcher claim queries |
| `awa.dispatch.claim_batch_size` | Histogram | `{job}` | Dispatcher claim batch size |
| `awa.dispatch.claim_duration` | Histogram | `s` | Dispatcher claim query duration |
| `awa.completion.flushes` | Counter | `{batch}` | Number of completion batch flushes |
| `awa.completion.flush_batch_size` | Histogram | `{job}` | Completion flush batch size |
| `awa.completion.flush_duration` | Histogram | `s` | Completion flush duration |
| `awa.maintenance.promote_batches` | Counter | `{batch}` | Number of promotion batches |
| `awa.maintenance.promote_batch_size` | Histogram | `{job}` | Promotion batch size |
| `awa.maintenance.promote_duration` | Histogram | `s` | Promotion batch duration |
| `awa.heartbeat.batches` | Counter | `{batch}` | Number of heartbeat batch updates |
| `awa.maintenance.rescues` | Counter | `{job}` | Number of jobs rescued by maintenance |

Job-level metrics carry `awa.job.kind` and `awa.job.queue` attributes. Dispatch metrics carry `awa.job.queue`. Completion metrics carry `awa.completion.shard`. Promotion metrics carry `awa.job.state`.

### Correctness (TLA+)

Core safety invariants of the queue storage engine and worker runtime are
checked with TLA+ models under [`correctness/`](../correctness/README.md).
The segmented-storage family has four complementary specs:

| Spec | Checks | Artifact |
|---|---|---|
| `AwaSegmentedStorage` | lifecycle transitions across ready / deferred / waiting / active_leases / attempt_state / terminal / dlq families and their rotate/prune safety; DLQ round-trip with `run_lease` reset; short-job rescue via lease-level heartbeat freshness | [spec](../correctness/storage/AwaSegmentedStorage.tla) |
| `AwaSegmentedStorageRaces` | the claim-vs-rotate-and-prune race that would exist if the Rust `FOR SHARE` on `lease_ring_state` were removed; paired with a checked-commit variant that closes it | [spec](../correctness/storage/AwaSegmentedStorageRaces.tla) |
| `AwaStorageLockOrder` | Postgres lock-acquisition order across claim / rotate-leases / prune-leases / rotate-ready / prune-ready; waits-for cycle detector; paired with a cycle-creating demo config to prove the checker works | [spec](../correctness/storage/AwaStorageLockOrder.tla) |
| `AwaSegmentedStorageTrace` | trace-replay harness that feeds hand-transcribed event sequences from real queue-storage runtime tests through the base spec; single-threaded validation that every observed transition is a legal spec action | [spec](../correctness/storage/AwaSegmentedStorageTrace.tla) |

Invariants covered include: no duplicate claim after rescue, stale completions
rejected via `run_lease` guard, pruned segments empty, `attempt_state`
existence implies active lease or waiting entry, DLQ and terminal families
disjoint, lock-order deadlock freedom. The TLA+ action → Rust function
mapping lives at [`correctness/storage/MAPPING.md`](../correctness/storage/MAPPING.md).

The worker-lifecycle specs `AwaCore`, `AwaExtended`, `AwaBatcher`, `AwaCbk`,
`AwaDispatchClaim`, `AwaViewTrigger`, `AwaCron` cover rescue, admin cancel,
stale completion protection, batcher flush, three-way callback races,
dispatcher claim, INSTEAD OF trigger concurrency, and cron double-fire
prevention respectively. See [`correctness/README.md`](../correctness/README.md)
for the full list and run commands.

### Queue Statistics (SQL)

The `admin::queue_overviews()` / `queue stats` read path is hybrid. Under the
queue-storage engine, the function first resolves the active schema from
`awa.runtime_storage_backends`, then reads live queue state across ready,
deferred, lease, terminal, and DLQ tables. The old `queue_state_counts` cache
table still exists for compatibility, but queue-storage workers no longer
depend on trigger-maintained counters on `jobs_hot` / `scheduled_jobs` to keep
operator views fresh.

## Web UI

The `awa-ui` crate provides a built-in dashboard, job inspector, queue management, and cron controls via `awa serve`. The frontend is React/TypeScript with IntentUI components, embedded into the binary via `rust-embed`. The backend is an axum REST API backed by `awa-model` admin functions.

```text
awa --database-url $DATABASE_URL serve
# → http://127.0.0.1:3000
```

Distributed via `pip install awa-cli` (no Rust toolchain needed) or `cargo install awa-cli`. See [Web UI design](ui-design.md) for API endpoints, page layouts, and component details.

## Deployment Model

Awa workers are stateless processes. All state lives in Postgres. The only external dependency is a Postgres connection.

- **Horizontal scaling:** Add more worker processes. `SKIP LOCKED` ensures no double dispatch.
- **Leader election:** Only one instance runs maintenance tasks at a time via `pg_try_advisory_lock`. If the leader dies, another instance acquires the lock within 10 seconds.
- **No sticky state:** Workers can be restarted or moved freely. There is no local disk state.
- **Queue assignment:** Different deployments can handle different queues by configuring `ClientBuilder::queue()`, enabling workload isolation.
