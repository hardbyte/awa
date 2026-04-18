# AWA — Product Requirements Document

*Version: 1.0 — March 2026*

---

## 1. Summary

Awa (Māori: river) is a Postgres-native background job queue for Rust and Python. It provides durable, transactional job enqueueing, typed handlers in both languages, reliable execution via `SKIP LOCKED` with heartbeat-based crash recovery, and a polyglot insert-only producer contract.

The Rust runtime handles all queue machinery — polling, heartbeating, crash recovery, dispatch. Python workers run as callbacks invoked by this runtime via PyO3, getting Rust-grade queue reliability with Python-native ergonomics.

**Positioning:** Postgres-native job execution for Rust and Python. One queue engine, two first-class languages.

**Inspirations:** River (Go), Oban (Elixir), GoodJob (Ruby). All three converged on the same core design — Postgres as the only dependency, `SKIP LOCKED` for dispatch, transactional enqueue as the killer feature. None of them provide first-class multi-language *worker execution* — River's Python/Ruby clients are insert-only, Oban has a Python insert library but no workers. Awa's Python workers are full participants backed by a Rust runtime.

---

## 2. Problem

### 2.1 Rust

Existing Rust Postgres job queues are fragmented and incomplete:

| Library | Status | Weakness |
|---|---|---|
| **sqlxmq** | Low activity (sqlx 0.8 update 2024, no new features since) | Best API design but stagnant |
| **fang** | Active | Multi-backend dilutes PG depth, `typetag` dynamic dispatch |
| **graphile_worker_rs** | Active | Port of Node.js system, inherits non-Rust conventions |
| **PGMQ** | Active | Message queue primitive, not a job framework — no workers, retries, cron |
| **apalis** | Active | Framework-heavy, multi-backend (Redis/Postgres/SQLite) dilutes PG depth |

### 2.2 Python

Python's most popular job queues (Celery, RQ, Dramatiq, ARQ) require Redis or RabbitMQ. Postgres-native alternatives exist but are pure Python — their queue engine performance is bounded by CPython:

| Library | Status | Weakness |
|---|---|---|
| **Procrastinate** | Active, mature (v3.7) | Pure Python queue engine. Heartbeats compete with handler for event loop. No cross-language interop. |
| **PgQueuer** | Active | Newer, less battle-tested. Pure Python. LISTEN/NOTIFY + SKIP LOCKED. |
| **django-postgres-queue** | Active | Django-only. |
| **pq** | Low activity | Minimal feature set. psycopg2 only. |

The common limitations of pure-Python Postgres queues:

- **Heartbeats die when the GIL blocks.** If a handler does CPU work or calls a blocking C extension, the Python event loop stalls and heartbeats stop — other workers think the job is dead and reclaim it.
- **Poll loops compete with handler code for CPU.** The queue engine and handlers share the same interpreter, so a busy handler degrades queue throughput.
- **Throughput is fundamentally limited by the interpreter.** JSON serialization, SQL parameter binding, and job dispatch all run in Python.
- **No cross-language worker interop.** A Rust service cannot process jobs enqueued by a Python service (or vice versa) through the same queue library.

**Awa's pitch:** keep your Postgres, skip Redis, and let a Rust runtime handle the queue plumbing. Heartbeats run on tokio (not the Python event loop), so they survive GIL-blocked handlers. Rust and Python workers are first-class participants on the same queues. Transactional enqueue works in both languages.

---

## 3. Goals & Non-Goals

### 3.1 Primary (v0.1 — must ship)

- Postgres-native correctness (`SKIP LOCKED`, transactional enqueue)
- Typed Rust worker API with compile-time kind validation
- Python client with full insert + work capabilities via PyO3
- Reliable execution with heartbeat + deadline crash recovery
- Graceful shutdown with drain timeout
- Admin operations (retry, cancel, pause, inspect) via CLI, Python, and raw SQL
- Migration tooling (CLI + programmatic SQL access)
- Health check endpoint

### 3.2 Secondary (v0.2–v0.3)

- Periodic/cron jobs with leader election
- Batch ingestion via `COPY` (staging table pattern)
- Rate limiting per queue
- Experimental ORM/driver transaction bridging (psycopg3, asyncpg)
- Weighted/opportunistic queue concurrency mode
- Structured progress tracking (percent + message + checkpoint metadata, persisted across retries)

### 3.3 Tertiary (v0.4+)

- Insert-only thin clients for TypeScript/Ruby

### 3.4 Delivered ahead of schedule

- Web UI (dashboard, job inspector, queue management, cron controls) — delivered v0.3
- Webhook completion for external systems (`waiting_external` state) — delivered v0.2

### 3.5 Non-Goals

- **Exactly-once semantics.** At-least-once with idempotent handlers.
- **Multi-backend support.** No SQLite, no MySQL, no Redis. Postgres-only.
- **Workflow engine.** No DAGs, no fan-out/fan-in, no saga orchestration. If you need Temporal, use Temporal.
- **ORM coupling.** The Python client accepts a connection string or pool config directly. No SQLAlchemy dependency. No Django dependency.
- **General-purpose Python DB layer.** `AwaTransaction` is a narrow escape hatch for atomic enqueue, not a replacement for your ORM.
- **ORM integration (Rust).** sqlx is the driver. One driver, done well.

---

## 4. Build Order & Critical Path

### Phase 0: PyO3 Async Spike (Gate for Python Workers)

Before committing to async Python workers as a headline feature, prove these four things:

1. **Rust tokio can call a Python `async def` and await its result** without deadlocking.
2. **A Rust background task can heartbeat** (write to Postgres) while a Python handler runs.
3. **Python exceptions propagate to Rust as structured errors** — not panics, not swallowed. Type, message, and traceback must be capturable.
4. **`ctx.is_cancelled()` works from Python** when Rust signals shutdown or deadline.

**Scope:** ~200 lines of Rust + ~50 lines of Python. One day.

**If the spike fails,** the fallback is sync Python handlers on a thread pool. The Rust runtime still owns the queue and heartbeats still survive, but the async story is weaker. The rest of this PRD assumes the spike succeeds.

### Phase 1: Rust Core

`awa-model`, `awa-macros`, `awa-worker`. Schema, migrations, insert, dispatch, heartbeat, crash recovery, shutdown, admin operations. Rust workers end-to-end.

### Phase 2: Python Client

`awa-python`: insert, transaction, worker registration. Dataclass/pydantic serialization bridge. Migration access from Python.

### Phase 3: Testing & Polish

`awa-testing` (Rust), `awa.testing` (Python). Cross-language integration tests. CLI, packaging, wheels.

---

## 5. Workspace Structure

```
awa/
├── Cargo.toml                  # [workspace]
├── awa-model/                  # Schema types, queries, migrations, admin ops
│   ├── src/
│   │   ├── lib.rs
│   │   ├── job.rs              # JobRow, JobState, InsertOpts, UniqueOpts
│   │   ├── error.rs            # AwaError enum (thiserror)
│   │   ├── insert.rs           # insert(), insert_many() — generic over Executor
│   │   ├── admin.rs            # retry, cancel, pause, drain, stats
│   │   ├── unique.rs           # BLAKE3 unique_key computation
│   │   ├── kind.rs             # Kind derivation + golden test suite
│   │   └── migrations.rs       # Embedded SQL migrations + version tracking
│   ├── queries/
│   │   ├── claim.sql           # SKIP LOCKED dequeue CTE with priority aging
│   │   ├── heartbeat.sql       # Batch heartbeat UPDATE
│   │   ├── rescue_stale.sql    # Heartbeat crash recovery
│   │   ├── rescue_deadline.sql # Hard deadline sweep
│   │   ├── promote.sql         # scheduled → available
│   │   ├── cleanup.sql         # Retention cleanup
│   │   └── stats.sql           # Queue lag/depth query
│   └── .sqlx/                  # Compile-time query metadata
├── awa-macros/                 # Derive macros (JobArgs)
│   └── src/lib.rs
├── awa-worker/                 # Runtime: poll loop, heartbeat, shutdown
│   └── src/
│       ├── lib.rs
│       ├── client.rs           # Client builder, start/shutdown, health_check
│       ├── dispatcher.rs       # Poll loop + LISTEN/NOTIFY wakeup
│       ├── heartbeat.rs        # Background heartbeat task
│       ├── executor.rs         # Spawns worker futures, tracks in-flight
│       ├── maintenance.rs      # Leader election + maintenance tasks
│       └── context.rs          # JobContext passed to workers
├── awa-python/                 # PyO3 extension module (maturin)
│   ├── Cargo.toml              # [lib] crate-type = ["cdylib"]
│   ├── pyproject.toml
│   ├── src/
│   │   ├── lib.rs              # #[pymodule] awa
│   │   ├── client.rs           # PyClient
│   │   ├── transaction.rs      # AwaTransaction — narrow SQL surface
│   │   ├── insert.rs
│   │   ├── job.rs              # PyJobRow, PyJobState
│   │   ├── worker.rs           # Worker registration + callback dispatch
│   │   ├── args.rs             # dataclass/pydantic → serde_json bridge
│   │   ├── kind.rs             # Snake-case derivation (must match Rust)
│   │   └── migrations.rs
│   └── python/
│       └── awa/
│           ├── __init__.py
│           ├── _awa.pyi        # Type stubs
│           └── py.typed        # PEP 561 marker
├── awa-testing/                # Rust test helpers
│   └── src/lib.rs
├── awa-cli/                    # CLI binary (migrations, admin, diagnostics)
│   └── src/main.rs
└── awa/                        # Facade crate — re-exports for Rust users
    └── src/lib.rs
```

### Which Crate/Package Do I Depend On?

| I want to… | Language | Depend on |
|---|---|---|
| Insert jobs from a Rust service (no worker) | Rust | `awa-model` |
| Insert AND process jobs | Rust | `awa` (or `awa-model` + `awa-worker`) |
| Insert and/or process jobs | Python | `pip install awa` |
| Write tests for job insertion | Rust | `awa-testing` (dev-dependency) |
| Run migrations or admin ops | Any | `awa migrate` / `awa job` / `awa queue` (CLI via cargo or pip) |

---

## 6. Core Concepts

### 6.1 Job Lifecycle (State Machine)

```
             insert with
             future run_at
                  │
                  ▼
scheduled ──(time passes)──▶ available ──(worker claims)──▶ running
                                 ▲                            │
                                 │                       ┌────┴────┐
                                 │                       │         │
                            retryable              completed   failed
                            (backoff)                       (max attempts
                                 ▲                           OR terminal
                                 │                            error)
                                 └──────(attempt < max)───────┘
                                                              │
                                                     cancelled (explicit)
```

| State | Meaning |
|---|---|
| `scheduled` | Enqueued with a future `run_at`. Not yet eligible. |
| `available` | Ready to be claimed by a worker. |
| `running` | Claimed. Being heartbeated. Has a hard `deadline_at`. |
| `completed` | Done. Subject to retention cleanup. |
| `retryable` | Failed but has attempts remaining. Becomes `available` after backoff. |
| `failed` | Exhausted all attempts, or hit a terminal error. Retained for inspection. |
| `cancelled` | Cancelled via API or from within the handler. Terminal. |

**Semantics:** At-least-once delivery. Handlers MUST be idempotent.

### 6.2 Handler Failure Classification

Five handler outcomes, identical semantics across Rust and Python:

| Category | Rust | Python | Effect |
|---|---|---|---|
| **Completed** | `Ok(Completed)` | return `None` | → `completed` |
| **Retryable error** | `Err(e)` | raise `Exception` | → `retryable` if attempts remain, else `failed`. Increments `attempt`. |
| **Retry after** | `Ok(RetryAfter(d))` | return `awa.RetryAfter(seconds=N)` | → `retryable` with explicit backoff. Increments `attempt`. |
| **Snooze** | `Ok(Snooze(d))` | return `awa.Snooze(seconds=N)` | → `available` with new `run_at`. Does NOT increment `attempt`. |
| **Cancel** | `Ok(Cancel)` | return `awa.Cancel(reason="...")` | → `cancelled`. Terminal. |

**Terminal errors (automatic):**

| Failure | Cause | Behaviour |
|---|---|---|
| Args don't match schema | serde failure (Rust), `TypeError`/`ValidationError` (Python) | → `failed` immediately. No retry. Attempt incremented to record the error. |
| Unknown `kind` | No handler registered | → `failed` immediately or skipped (configurable). |
| Explicit terminal | `raise awa.TerminalError(...)` (Python) | → `failed` immediately regardless of remaining attempts. |

Deserialization failures are terminal by default. Retrying malformed args is pointless. If you deploy a breaking schema change to job args, in-flight jobs with the old schema fail terminally. Drain or cancel old jobs before deploying breaking arg changes.

---

## 7. Data Model

### 7.1 Schema

All Awa objects live in the `awa` schema.

```sql
CREATE SCHEMA IF NOT EXISTS awa;

CREATE TYPE awa.job_state AS ENUM (
    'scheduled', 'available', 'running',
    'completed', 'retryable', 'failed', 'cancelled'
);

CREATE TABLE awa.jobs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind            TEXT        NOT NULL,
    queue           TEXT        NOT NULL DEFAULT 'default',
    args            JSONB       NOT NULL DEFAULT '{}',
    state           awa.job_state NOT NULL DEFAULT 'available',
    priority        SMALLINT    NOT NULL DEFAULT 2,
    attempt         SMALLINT    NOT NULL DEFAULT 0,
    max_attempts    SMALLINT    NOT NULL DEFAULT 25,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at    TIMESTAMPTZ,
    deadline_at     TIMESTAMPTZ,
    attempted_at    TIMESTAMPTZ,
    finalized_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    errors          JSONB[]     DEFAULT '{}',
    metadata        JSONB       NOT NULL DEFAULT '{}',
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    unique_key      BYTEA,
    unique_states   BIT(8),

    CONSTRAINT priority_in_range CHECK (priority BETWEEN 1 AND 4),
    CONSTRAINT max_attempts_range CHECK (max_attempts BETWEEN 1 AND 1000),
    CONSTRAINT queue_name_length CHECK (length(queue) <= 200),
    CONSTRAINT kind_length CHECK (length(kind) <= 200),
    CONSTRAINT tags_count CHECK (cardinality(tags) <= 20)
);

CREATE TABLE awa.queue_meta (
    queue       TEXT PRIMARY KEY,
    paused      BOOLEAN NOT NULL DEFAULT FALSE,
    paused_at   TIMESTAMPTZ,
    paused_by   TEXT
);

CREATE TABLE awa.schema_version (
    version     INT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 7.2 Key Design Decisions

- **`args` not `payload`.** Reinforces that producers and consumers share a contract on the JSON shape — especially important for cross-language interop.
- **`BIGINT GENERATED ALWAYS AS IDENTITY`** not `BIGSERIAL`. Modern PG standard. Prevents accidental manual ID assignment.
- **`errors JSONB[]`.** Array of error objects, one per failed attempt. Full retry history for debugging.
- **`heartbeat_at` + `deadline_at` (hybrid model).** `heartbeat_at` proves liveness (30s interval). `deadline_at` is a hard ceiling for runaway protection. See §9.5.
- **`unique_key BYTEA` (BLAKE3, 16 bytes).** See §9.7.
- **Priority 1–4.** 1 = highest (critical, high, default, low).
- **`awa.queue_meta` for pause/resume.** State lives in the database, not worker memory. Pausing from CLI affects all workers immediately.

### 7.3 Indexes

```sql
-- Dequeue hot path
CREATE INDEX idx_awa_jobs_dequeue
    ON awa.jobs (queue, priority, run_at, id)
    WHERE state = 'available';

-- Heartbeat staleness (crash detection)
CREATE INDEX idx_awa_jobs_heartbeat
    ON awa.jobs (heartbeat_at)
    WHERE state = 'running';

-- Hard deadline (runaway protection)
CREATE INDEX idx_awa_jobs_deadline
    ON awa.jobs (deadline_at)
    WHERE state = 'running' AND deadline_at IS NOT NULL;

-- Uniqueness enforcement
CREATE UNIQUE INDEX idx_awa_jobs_unique
    ON awa.jobs (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND awa.job_state_in_bitmask(unique_states, state);

-- Kind-based lookups (admin, monitoring)
CREATE INDEX idx_awa_jobs_kind_state
    ON awa.jobs (kind, state);
```

### 7.4 Functions & Triggers

```sql
CREATE FUNCTION awa.job_state_in_bitmask(bitmask BIT(8), state awa.job_state)
RETURNS BOOLEAN AS $$
    SELECT CASE state
        WHEN 'scheduled'  THEN get_bit(bitmask, 0) = 1
        WHEN 'available'  THEN get_bit(bitmask, 1) = 1
        WHEN 'running'    THEN get_bit(bitmask, 2) = 1
        WHEN 'completed'  THEN get_bit(bitmask, 3) = 1
        WHEN 'retryable'  THEN get_bit(bitmask, 4) = 1
        WHEN 'failed'     THEN get_bit(bitmask, 5) = 1
        WHEN 'cancelled'  THEN get_bit(bitmask, 6) = 1
        ELSE FALSE
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION awa.backoff_duration(attempt SMALLINT, max_attempts SMALLINT)
RETURNS interval AS $$
    SELECT LEAST(
        (power(2, attempt)::int || ' seconds')::interval
            + (random() * power(2, attempt) * 0.25 || ' seconds')::interval,
        interval '24 hours'
    );
$$ LANGUAGE sql VOLATILE;

CREATE FUNCTION awa.notify_new_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('awa:' || NEW.queue, NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_awa_notify
    AFTER INSERT ON awa.jobs
    FOR EACH ROW
    WHEN (NEW.state = 'available' AND NEW.run_at <= now())
    EXECUTE FUNCTION awa.notify_new_job();
```

---

## 8. Operational Limits

| Parameter | Limit | Enforcement | Rationale |
|---|---|---|---|
| `args` size | 500 KB | Advisory | Dequeue CTE reads full rows. Large args = slow claims. Store large data externally, pass a reference. |
| `tags` count | 20 | CHECK constraint | Tags are for filtering, not free-form metadata. Use `metadata` JSONB for unbounded data. |
| `tags` element length | 200 chars | CHECK constraint | Tags should be short labels. |
| `errors` history | `max_attempts` entries | Application code | Array appended on each failure. At 25 default max attempts, worst case is 25 JSONB objects. |
| `queue` name length | 200 chars | CHECK constraint | Queue names are identifiers, not data. |
| `kind` length | 200 chars | CHECK constraint | Same. |
| `max_attempts` | 1–1000 | CHECK constraint | Upper bound prevents accidental infinite retry. |
| `priority` | 1–4 | CHECK constraint | Four levels. 1 = critical, 2 = high (default), 3 = default, 4 = low. |
| Max in-flight per process | Sum of all `max_workers` | User-configured | Practically bounded by memory and connection pool. 500 concurrent is a reasonable upper bound. |
| Heartbeat batch size | 500 job IDs per UPDATE | Application limit | `WHERE id = ANY($1)` with very large arrays causes planner issues. >500 splits into multiple batches. |

---

## 9. Key Features

### 9.1 Transactional Enqueue

#### Rust (via `awa-model`, generic executor)

All `awa-model` functions accept `impl sqlx::Executor<'_, Database = Postgres>`. The caller decides pool vs transaction. No `insert` vs `insert_tx` split.

```rust
pub async fn insert<'e, E>(executor: E, args: impl JobArgs) -> Result<JobRow, AwaError>
where E: sqlx::Executor<'e, Database = sqlx::Postgres>;

pub async fn insert_with<'e, E>(executor: E, args: impl JobArgs, opts: InsertOpts) -> Result<JobRow, AwaError>
where E: sqlx::Executor<'e, Database = sqlx::Postgres>;

pub async fn insert_many<'e, E>(executor: E, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
where E: sqlx::Executor<'e, Database = sqlx::Postgres>;
```

```rust
// Pool — auto-commit
awa_model::insert(&pool, SendEmail { to: "a@b.com".into() }).await?;

// Transaction — atomic with business logic
let mut tx = pool.begin().await?;
billing_model::invoices::create(&mut *tx, &invoice).await?;
awa_model::insert(&mut *tx, SendInvoiceEmail { invoice_id: invoice.id }).await?;
tx.commit().await?;
```

#### Python — Transaction Tiers

**Tier 1: Guaranteed (`client.transaction()`).** The only fully guaranteed atomic path.

```python
async with client.transaction() as tx:
    await tx.execute(
        "INSERT INTO orders (id, customer_id, total) VALUES ($1, $2, $3)",
        order_id, customer_id, total
    )
    await tx.insert(SendConfirmationEmail(order_id=order_id, recipient=email))
```

**Tier 2: Experimental driver bridging (v0.2).** psycopg3/asyncpg connection bridging. Driver-specific, version-pinned. No ORM guarantees. We will not pretend both paths are equally solid.

#### `AwaTransaction` API (v0.1 — intentionally narrow)

```python
class AwaTransaction:
    """Thin SQL transaction wrapper for atomic enqueue. Not a general-purpose DB layer."""
    async def execute(self, query: str, *args: Any) -> int
    async def fetch_one(self, query: str, *args: Any) -> dict[str, Any]
    async def fetch_optional(self, query: str, *args: Any) -> dict[str, Any] | None
    async def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]
    async def insert(self, args: Any, **opts) -> Job
    async def insert_many(self, jobs: list[Any], **opts) -> list[Job]
```

**Explicit v0.1 limitations:** No savepoints. No nested transactions. No ORM integration. Raw SQL only (`$1`, `$2` placeholders). `dict[str, Any]` results. Schema-qualified table names required. Shared connection pool.

**Non-atomic insert (most common path):**

```python
await client.insert(SendEmail(to="a@b.com", subject="Welcome", body="..."))
await client.insert(args, queue="email", priority=1, run_at=future_time)
```

### 9.2 Typed Job Args & Cross-Language Kind Derivation

#### Kind String Specification

The `kind` string is the cross-language contract. Algorithm: CamelCase → snake_case.

1. Insert `_` before each uppercase letter following a lowercase letter or digit.
2. Insert `_` before an uppercase letter followed by a lowercase letter, if preceded by uppercase.
3. Lowercase everything.

**Golden test cases (must pass in both Rust and Python CI):**

| Input | Kind |
|---|---|
| `SendEmail` | `send_email` |
| `SendConfirmationEmail` | `send_confirmation_email` |
| `SMTPEmail` | `smtp_email` |
| `OAuthRefresh` | `o_auth_refresh` |
| `PDFRenderJob` | `pdf_render_job` |
| `ProcessV2Import` | `process_v2_import` |
| `ReconcileQ3Revenue` | `reconcile_q3_revenue` |
| `HTMLToPDF` | `html_to_pdf` |
| `IOError` | `io_error` |

Override always available: `#[awa(kind = "custom")]` (Rust), `@client.worker(T, kind="custom")` (Python).

#### Rust

```rust
use awa_model::JobArgs;

#[derive(Debug, Serialize, Deserialize, JobArgs)]
#[awa(kind = "send_confirmation_email")]
pub struct SendConfirmationEmail {
    pub order_id: i64,
    pub recipient: String,
}
```

#### Python

```python
@dataclass
class SendConfirmationEmail:
    order_id: int
    recipient: str
# kind auto-derived as "send_confirmation_email"

# Pydantic works identically:
class SendConfirmationEmail(BaseModel):
    order_id: int
    recipient: str
```

Detection is automatic: Pydantic `BaseModel` (has `model_validate`/`model_dump`) or dataclass (`__dataclass_fields__`).

### 9.3 Workers

#### Rust

```rust
#[async_trait]
impl Worker for SendConfirmationEmail {
    async fn perform(&self, ctx: &JobContext) -> JobResult {
        let mailer = ctx.extract::<Mailer>()?;
        mailer.send(&self.recipient, "Order confirmed", ...).await?;
        Ok(JobResult::Completed)
    }
}

let client = awa_worker::Client::builder()
    .pool(pool)
    .queue("default", QueueConfig { max_workers: 50 })
    .queue("billing", QueueConfig { max_workers: 10 })
    .maintenance_pool_size(2)
    .register::<SendConfirmationEmail>()
    .build().await?;
client.start().await?;
```

#### Python

```python
@client.worker(SendConfirmationEmail, queue="email")
async def handle_send_email(job: awa.Job[SendConfirmationEmail]) -> None:
    await email_service.send(to=job.args.to, subject=job.args.subject)
```

**`awa.Job[T]` wrapper:**

```python
class Job(Generic[T]):
    args: T                     # deserialized dataclass/pydantic instance
    id: int
    attempt: int                # 1-indexed
    max_attempts: int
    queue: str
    priority: int
    deadline: datetime | None
    metadata: dict[str, Any]
    tags: list[str]
    def is_cancelled(self) -> bool: ...
```

**Return model:** `None` (completed), `awa.RetryAfter(seconds=N)`, `awa.Snooze(seconds=N)`, `awa.Cancel(reason="...")`. Raise any `Exception` for retryable error. Raise `awa.TerminalError(...)` for immediate failure.

### 9.4 Job Dispatch (SKIP LOCKED) with Priority Aging

```sql
-- queries/claim.sql
WITH claimed AS (
    SELECT id
    FROM awa.jobs
    WHERE state = 'available'
      AND queue = $1
      AND run_at <= now()
      AND NOT EXISTS (
          SELECT 1 FROM awa.queue_meta
          WHERE queue = $1 AND paused = TRUE
      )
    ORDER BY
      -- Priority aging: boost by 1 level per aging_interval of wait time
      GREATEST(1, priority - FLOOR(EXTRACT(EPOCH FROM (now() - run_at)) / $4)::int) ASC,
      run_at ASC,
      id ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE awa.jobs
SET state = 'running',
    attempt = attempt + 1,
    attempted_at = now(),
    heartbeat_at = now(),
    deadline_at = now() + $3::interval
FROM claimed
WHERE awa.jobs.id = claimed.id
RETURNING awa.jobs.*;
```

**Priority aging** prevents starvation. A priority-4 job waiting 3× the aging interval becomes effectively priority-1. Default aging interval: 60s. Configurable per queue. Set to `MAX` to disable (strict priority, starvation possible).

| Job priority | Wait time (60s interval) | Effective priority |
|---|---|---|
| 4 (low) | 0s | 4 |
| 4 (low) | 60s | 3 |
| 4 (low) | 120s | 2 |
| 4 (low) | 180s+ | 1 (highest) |

**Poll interval:** Default 200ms. LISTEN/NOTIFY wakeup reduces latency to <10ms. LISTEN requires a dedicated connection (one per worker process).

### 9.5 Crash Recovery (Heartbeat + Deadline Hybrid)

**Signal 1: Heartbeat staleness (crash detection).** Worker runtime heartbeats every 30s — a single batch UPDATE for all in-flight jobs via `WHERE id = ANY($1)`. Stale >90s = presumed dead.

**Signal 2: Hard deadline (runaway protection).** Even healthy-heartbeating jobs are killed at `deadline_at`. Default: 5 minutes from claim. Override at insert time.

| Failure mode | Heartbeat catches it? | Deadline catches it? |
|---|---|---|
| Worker killed (OOM, SIGKILL) | Yes | Eventually |
| Network partition | Yes | Eventually |
| Infinite loop in handler | No | **Yes** |
| Deadlocked external call | No | **Yes** |

**Python advantage (assuming async workers — see Phase 0):** Heartbeat runs on the Rust tokio runtime, not the Python event loop. If Python handler code blocks the event loop, heartbeats continue. Awa preserves queue liveness even when Python handlers misbehave. This does NOT solve bad handler throughput or CPU-bound Python code.

**Heartbeat write amplification:** At 50 concurrent jobs, the batch heartbeat is one UPDATE touching 50 rows every 30 seconds (~1.7 writes/sec). Negligible. At 500 concurrent, it's still one query per 30s. Batches are capped at 500 IDs; larger in-flight counts split across multiple UPDATEs.

### 9.6 Queue Concurrency Model

#### Hard-Reserved Capacity (v0.1)

Each queue has a fixed `max_workers` that is hard-reserved. `billing: 10, email: 50` means 10 and 50, regardless of load distribution.

- Provides isolation: a flood of email jobs cannot starve billing.
- Wasted-capacity cost is real but acceptable for v0.1.
- Total concurrency = sum of all `max_workers`.

**Noisy queue isolation:** Maintenance runs independently via its own reserved connections (`maintenance_pool_size`). A noisy queue cannot block heartbeat rescue, scheduled promotion, or cleanup.

```rust
Client::builder()
    .pool(pool)
    .maintenance_pool_size(2)   // reserved for maintenance, not workers
    .queue("billing", QueueConfig {
        max_workers: 10,
        priority_aging_interval: Duration::from_secs(120),
    })
    .queue("email", QueueConfig { max_workers: 50 })
```

#### Weighted/Opportunistic Mode (v0.3)

Future mode with `global_max_workers`, `min_workers`, and `weight` per queue. Idle capacity redistributed proportionally.

### 9.7 Uniqueness (BLAKE3)

**Hash:** BLAKE3 truncated to 16 bytes (128 bits). Birthday bound: ~18 quintillion — effectively zero collision risk. BLAKE3 is faster than SHA-256, SIMD-accelerated, used by Cargo, with official Rust, C, and Python implementations.

**Why not FNV-1a:** River uses FNV-1a (64 bits, birthday bound ~4 billion). A hash collision in a uniqueness contract means a job silently fails to insert when it should have. That's a correctness bug. The 8 extra bytes per job are negligible.

```rust
// awa-model/src/unique.rs
pub fn compute_unique_key(
    kind: &str,
    queue: Option<&str>,
    args: Option<&serde_json::Value>,
    period_bucket: Option<i64>,
) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(kind.as_bytes());
    if let Some(q) = queue { hasher.update(b"\x00"); hasher.update(q.as_bytes()); }
    if let Some(a) = args {
        hasher.update(b"\x00");
        hasher.update(&serde_json::to_vec(a).unwrap()); // keys sorted by serde_json
    }
    if let Some(p) = period_bucket { hasher.update(b"\x00"); hasher.update(&p.to_le_bytes()); }
    hasher.finalize().as_bytes()[..16].to_vec()
}
```

```python
def compute_unique_key(kind, queue, args, period_bucket):
    h = blake3.blake3()
    h.update(kind.encode())
    if queue is not None: h.update(b"\x00"); h.update(queue.encode())
    if args is not None: h.update(b"\x00"); h.update(json.dumps(args, sort_keys=True).encode())
    if period_bucket is not None: h.update(b"\x00"); h.update(period_bucket.to_bytes(8, "little"))
    return h.digest(length=16)
```

Cross-language golden tests required. JSON canonicalization: UTF-8, sorted keys, no whitespace.

**Unique states bitmask** (bits 0–4 set by default = scheduled, available, running, completed, retryable):

| Bit | State |
|-----|-------|
| 0 | scheduled |
| 1 | available |
| 2 | running |
| 3 | completed |
| 4 | retryable |
| 5 | failed |
| 6 | cancelled |

Insert uses `ON CONFLICT (unique_key) WHERE ... DO NOTHING`.

### 9.8 Graceful Shutdown

1. Stop heartbeating and polling. Final heartbeat written.
2. Wait for in-flight jobs (up to timeout).
3. Signal cancellation via `is_cancelled()`.
4. Force exit after grace period (2s). Stale jobs recovered by heartbeat sweep.

### 9.9 Retry with Backoff

Exponential backoff + jitter: `min(2^attempt + jitter, 24h)`. Produces: ~1s, ~2s, ~4s, ~8s, ... capping at 24h. Workers can override via `RetryAfter(duration)`. Errors recorded in the `errors JSONB[]` array per attempt.

---

## 10. Admin Operations

Operators get day-one tools through CLI, Python, Rust, and documented SQL.

| Operation | CLI | Python | Rust |
|---|---|---|---|
| Retry one job | `awa job retry <id>` | `await client.retry(id)` | `awa_model::admin::retry(exec, id)` |
| Cancel one job | `awa job cancel <id>` | `await client.cancel(id)` | `awa_model::admin::cancel(exec, id)` |
| Retry failed by kind | `awa job retry-failed --kind X` | `await client.retry_failed(kind="X")` | `awa_model::admin::retry_failed_by_kind(...)` |
| Retry failed by queue | `awa job retry-failed --queue X` | `await client.retry_failed(queue="X")` | `awa_model::admin::retry_failed_by_queue(...)` |
| Discard failed by kind | `awa job discard --kind X` | `await client.discard_failed(kind="X")` | `awa_model::admin::discard_failed(...)` |
| Pause queue | `awa queue pause X` | `await client.pause_queue("X")` | `awa_model::admin::pause_queue(...)` |
| Resume queue | `awa queue resume X` | `await client.resume_queue("X")` | `awa_model::admin::resume_queue(...)` |
| Drain queue | `awa queue drain X` | `await client.drain_queue("X")` | `awa_model::admin::drain_queue(...)` |
| List jobs | `awa job list --state running` | `await client.list_jobs(state=...)` | `awa_model::admin::list_jobs(...)` |
| Queue stats / lag | `awa queue stats` | `await client.queue_stats()` | `awa_model::admin::queue_overviews(...)` |

**Pause/resume** is stored in `awa.queue_meta`. Pausing immediately stops new claims across all workers. In-flight jobs continue to completion.

### SQL Reference

CLI and client methods are thin wrappers around these queries. Operators can always drop to raw SQL.

```sql
-- Retry one job
UPDATE awa.jobs
SET state = 'available', attempt = 0, run_at = now(),
    finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
WHERE id = $1 AND state IN ('failed', 'cancelled') RETURNING id;

-- Cancel one job
UPDATE awa.jobs
SET state = 'cancelled', finalized_at = now()
WHERE id = $1 AND state NOT IN ('completed', 'failed', 'cancelled') RETURNING id;

-- Retry all failed by kind
UPDATE awa.jobs
SET state = 'available', attempt = 0, run_at = now(),
    finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
WHERE kind = $1 AND state = 'failed' RETURNING id;

-- Pause a queue
INSERT INTO awa.queue_meta (queue, paused, paused_at, paused_by)
VALUES ($1, TRUE, now(), $2)
ON CONFLICT (queue) DO UPDATE SET paused = TRUE, paused_at = now(), paused_by = $2;

-- Resume a queue
UPDATE awa.queue_meta SET paused = FALSE WHERE queue = $1;

-- Drain a queue
UPDATE awa.jobs SET state = 'cancelled', finalized_at = now()
WHERE queue = $1 AND state IN ('available', 'scheduled', 'retryable');

-- Queue lag
SELECT queue,
    count(*) FILTER (WHERE state = 'available') AS available,
    count(*) FILTER (WHERE state = 'running') AS running,
    count(*) FILTER (WHERE state = 'failed') AS failed,
    count(*) FILTER (WHERE state = 'completed'
        AND finalized_at > now() - interval '1 hour') AS completed_last_hour,
    EXTRACT(EPOCH FROM (now() - min(run_at) FILTER (WHERE state = 'available'))) AS lag_seconds
FROM awa.jobs GROUP BY queue;

-- Inspect running jobs
SELECT id, kind, queue, attempt, heartbeat_at, deadline_at,
    EXTRACT(EPOCH FROM (now() - heartbeat_at)) AS heartbeat_age_s,
    EXTRACT(EPOCH FROM (deadline_at - now())) AS deadline_remaining_s
FROM awa.jobs WHERE state = 'running' ORDER BY heartbeat_at ASC;
```

---

## 11. Health Check & Deployment

### Health Check

Awa provides the check; the application provides the HTTP server.

```rust
pub struct HealthCheck {
    pub healthy: bool,
    pub postgres_connected: bool,
    pub poll_loop_alive: bool,
    pub heartbeat_alive: bool,
    pub shutting_down: bool,
    pub leader: bool,
    pub queues: HashMap<String, QueueHealth>,
}
pub struct QueueHealth {
    pub in_flight: u32,
    pub max_workers: u32,
    pub available: u64,
}
```

```python
health = await client.health_check()
```

### Kubernetes

Awa workers are stateless. Deploy as a `Deployment` with `replicas: N`. All coordination happens through Postgres.

```yaml
terminationGracePeriodSeconds: 35  # slightly > drain timeout (30s)
livenessProbe:
  httpGet: { path: /healthz, port: 8080 }
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /readyz, port: 8080 }
  periodSeconds: 5
```

**Liveness:** `postgres_connected && poll_loop_alive && heartbeat_alive`. **Readiness:** `!shutting_down`.

No operator, CRD, or Helm chart planned. For KEDA autoscaling, point at `SELECT count(*) FROM awa.jobs WHERE state = 'available'`.

---

## 12. Serialization Bridge (Python)

**Insert (Python → Postgres):** Pydantic `model_dump(mode="json")` or `dataclasses.asdict()` → Python dict → `serde_json::Value` via PyO3 → JSONB.

**Dispatch (Postgres → Python):** JSONB → `serde_json::Value` → Python dict → Pydantic `model_validate(dict)` or `Cls(**dict)`. Reconstruction failure → terminal error.

---

## 13. Error Handling

### Rust

```rust
#[derive(Debug, Error)]
pub enum AwaError {
    #[error("job not found: {id}")] JobNotFound { id: i64 },
    #[error("unique conflict")] UniqueConflict { existing_id: i64 },
    #[error("schema not migrated")] SchemaNotMigrated { expected: i32, found: i32 },
    #[error("unknown job kind: {kind}")] UnknownJobKind { kind: String },
    #[error("serialization error")] Serialization(#[from] serde_json::Error),
    #[error("database error")] Database(#[source] sqlx::Error),
}
```

### Python

```python
class AwaError(Exception): ...
class UniqueConflict(AwaError): ...
class SchemaNotMigrated(AwaError): ...
class UnknownJobKind(AwaError): ...
class SerializationError(AwaError): ...
class ValidationError(AwaError): ...
class TerminalError(AwaError): ...
class DatabaseError(AwaError): ...
```

---

## 14. Migration Strategy

Four paths:

1. **CLI:** `awa migrate --database-url $DATABASE_URL` (via cargo or pip install)
2. **Rust programmatic:** `awa_model::migrations::run(&pool).await?`
3. **Python programmatic:** `await awa.migrate(dsn)` or `awa.migrations()` for raw SQL
4. **Raw extraction:** `awa migrate --extract-to ./migrations/awa/`

Versions tracked in `awa.schema_version`. Wheel/binary distribution via `cibuildwheel` + `maturin` for Linux, macOS (arm64 + x86_64), Windows.

---

## 15. Observability

**Rust:** `tracing` spans per job execution and insert. **Python:** Structured `logging` bridged from Rust:

```
INFO awa: job.completed kind=send_email queue=default job_id=12345 attempt=1 duration_ms=42
ERROR awa: job.failed kind=send_email job_id=12347 error="validation_error: field 'to' required" terminal=true
```

---

## 16. Maintenance Services

Run on a single elected leader via `pg_try_advisory_lock`. Maintenance uses reserved connections (`maintenance_pool_size`) to avoid contention with workers.

| Task | Frequency | Description |
|---|---|---|
| Heartbeat rescue | 30s | `running` + stale `heartbeat_at` (>90s) → `retryable` |
| Deadline rescue | 30s | `running` + past `deadline_at` → `retryable` |
| Scheduled promotion | 5s | `scheduled` past `run_at` → `available` |
| Completed cleanup | 60s | Delete `completed` older than retention (default: 24h) |
| Failed cleanup | 60s | Delete `failed`/`cancelled` older than retention (default: 72h) |
| Queue metrics | 10s | Emit stats via tracing / Python logging |

---

## 17. Testing

### Rust (`awa-testing`)

```rust
let tc = TestClient::from_pool(pool).await;
awa_model::insert(&tc.pool(), SendEmail { to: "a@b.com".into() }).await?;
let result = tc.work_one::<SendEmail>().await?;
assert!(result.is_completed());
```

### Python (`awa.testing`)

```python
async def test_send_email(awa_client):
    await awa_client.insert(SendEmail(to="a@b.com", subject="Hi", body="..."))
    result = await awa_client.work_one(SendEmail, handler=handle_send_email)
    assert result.is_completed()
```

### Cross-language

Insert from Python, verify JSON shape matches Rust serde schema. Insert from Rust, work from Python. The `kind` + JSON args are the contract.

Both test against real Postgres. No in-memory fakes.

---

## 18. Differentiators (Post-MVP)

- **Batch Ingestion via COPY (v0.2):** Staging table pattern for unique jobs. Direct COPY for non-unique.
- **Web UI (v0.4):** Separate `awa-ui` crate. Deferred — SQL + Grafana covers 80%.
- **Webhook Completion (v0.3):** `waiting_external` state, HMAC-signed callbacks.
- **Experimental ORM Bridging (v0.2):** psycopg3/asyncpg transaction bridging. Explicitly experimental.
- **Weighted Queue Concurrency (v0.3):** `global_max_workers` + `min_workers` + `weight`.

---

## 19. Architecture

```
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Rust Service     │  │  Python Service   │  │  Any Language     │
│  (awa-model)      │  │  (pip install awa)│  │  (raw SQL INSERT) │
└────────┬─────────┘  └────────┬──────────┘  └────────┬──────────┘
         │                     │                       │
         ▼                     ▼                       ▼
   ┌─────────────────────────────────────────────────────────┐
   │                      PostgreSQL                          │
   │                        awa.jobs                          │
   └─────────────────────────────┬───────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                   │
        ┌─────▼──────┐   ┌──────▼──────┐   ┌───────▼──────┐
        │ Rust Worker  │   │ Python Worker│   │ Python Worker │
        │ (awa-worker) │   │ (pip awa)    │   │ (pip awa)     │
        │ Instance 1   │   │ Instance 2   │   │ Instance 3    │
        │ (leader)     │   │              │   │               │
        └──────────────┘   └──────────────┘   └───────────────┘
```

Mixed Rust/Python fleets work correctly on the same queues.

---

## 20. MVP Scope

### In (v0.1)

**awa-model:** `insert()`, `insert_with()`, `insert_many()` (generic executor). `JobRow`, `JobState`, `InsertOpts`, `UniqueOpts`, `AwaError`. Admin operations (retry, cancel, pause, resume, drain, stats). BLAKE3 uniqueness. Priority 1–4 with aging. Embedded migrations + `schema_version`. Compile-time query checking (`.sqlx/`). Kind derivation + golden tests.

**awa-macros:** `#[derive(JobArgs)]`.

**awa-worker:** Client builder, `start()`, `shutdown()`. Worker trait, JobContext, JobResult (5 variants + terminal). SKIP LOCKED dispatch + LISTEN/NOTIFY. Automatic heartbeat + crash recovery (heartbeat + deadline). Retry with backoff + jitter. Graceful shutdown with drain. Multiple queues with hard-reserved concurrency. `health_check()`. Maintenance with reserved pool. `tracing` spans / Python logging bridge.

**awa-python:** Full client — insert, work, manage, transaction. dataclass + pydantic (auto-detected). `AwaTransaction` (narrow surface). `awa.testing` pytest fixtures. Migration access. CLI entry point.

**awa-testing:** Rust TestClient. **awa-cli:** Migrations + admin commands. **awa:** Facade crate.

### Out (v0.1)

Web UI. COPY ingestion. Webhook completion. Periodic/cron jobs. Rate limiting. ORM bridging. TypeScript/Ruby clients. Encrypted args. Savepoints. Sync Python workers. Weighted queue concurrency.

---

## 21. Success Metrics

- **<30 min** from `cargo add awa` or `pip install awa` to first job executing
- **<50ms** median pickup latency (LISTEN/NOTIFY enabled)
- **Zero job loss** on worker crash (`kill -9` chaos test)
- **>5,000 jobs/sec** sustained (Rust workers, single queue, no uniqueness)
- **>1,000 jobs/sec** sustained (Python workers, single queue, no uniqueness)

---

## 22. Risks

| Risk | Severity | Mitigation |
|---|---|---|
| `pyo3-asyncio` spike fails | **Critical** | Phase 0 gate. Fallback: sync workers on thread pool. |
| `AwaTransaction` adoption friction | Medium | Honest framing: atomic path, not a DB toolkit. ORM bridging in v0.2. |
| Maturin/wheel distribution | Medium | `cibuildwheel` + `maturin` in CI. Treat as real workstream. |
| Two-language testing surface | Medium | Test the cross-language JSON contract, not every permutation. |
| Python GIL | Medium | GIL only during handler execution. Queue machinery on tokio. |
| Kind derivation drift | Medium | Single spec, golden tests, CI enforcement. |
| Scope creep → workflow engine | High | Non-goal documented. |
| UI before engine stable | High | UI is v0.4. |

---

## 23. Open Questions

1. **Partitioning.** At what scale does a single table fall over? Does the dequeue index survive range partitioning on `created_at`?
2. **Multi-tenant isolation.** Per-tenant schemas vs queue-based isolation? Per-tenant conflicts with `query_as!` compile-time checking.
3. **Python sync support.** Ship `client.insert_sync()` for Django/Flask? Likely yes for insert, harder for workers. v0.2 candidate.
4. **`InsertOpts` defaults via type.** Should job args types carry default queue/priority/deadline?
5. **Python type stubs.** Automate `.pyi` via `pyo3-stub-gen` or hand-maintain?
6. **Pool isolation.** Should `AwaTransaction` queries use a separate pool from workers?
7. **Wheel size.** ~5-10MB bundling tokio runtime. Consider `awa-lite` (insert-only, no runtime) if blocker.
8. **JSON canonicalization.** `sort_keys=True` and accept edge cases, or RFC 8785 JCS?
