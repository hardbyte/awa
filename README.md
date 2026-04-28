# Awa

**Postgres-native job queue for Rust and Python.**

Awa (Māori: river) fills the gap between Postgres event queues that are too
narrow for real job-queue behavior and language-specific job frameworks (River,
Oban, Sidekiq) that couple you to one ecosystem. If you run Rust or Python (or
both) on Postgres and want priorities, cron, DLQ, and transactional enqueue
without Redis or RabbitMQ, Awa is built for you.

![AWA Web UI — Jobs (dark mode)](https://raw.githubusercontent.com/hardbyte/awa/main/docs/images/awa-ui-dark.png)

## Features

### Core queue
- **Transactional enqueue** — insert jobs inside your business transaction. Commit = visible. Rollback = gone.
- **Unique jobs** — declare uniqueness by kind/queue/args; cancel by unique key without storing job IDs.
- **Priorities, retries, snoozes** — exponential backoff with jitter; priority aging for fairness.
- **Dead Letter Queue** — first-class DLQ with per-queue opt-in, retention, and operator retry/purge.
- **Periodic/cron jobs** — leader-elected scheduler with timezone support and atomic enqueue.
- **Sequential callbacks** — `wait_for_callback()` / `resume_external()` for multi-step orchestration within a single handler.
- **Webhook callbacks** — park jobs for external completion with optional CEL-expression filtering.

### Runtime
- **Rust and Python workers** — same queues, same storage engine, mixed deployments.
- **Crash recovery** — heartbeat + hard deadline rescue. Stale jobs recovered automatically.
- **Runtime-owned maintenance** — dispatch, rescue, segment rotation, and pruning run in the worker fleet; no `pg_cron` ticker required.
- **Segmented queue storage** — append-only ready and terminal entries with rotating lease segments; queue history and execution churn stay off the dispatch path.
- **LISTEN/NOTIFY wakeup** — millisecond-scale pickup latency.
- **HTTP Worker** — feature-gated worker that dispatches jobs to serverless functions (Lambda, Cloud Run) via HTTP with HMAC-BLAKE3 callback auth.
- **Weighted concurrency + rate limiting** — global worker pool with per-queue guarantees; per-queue token bucket.

### Operations
- **Web UI** — dashboard, job inspector, queue management, cron controls, DLQ retry/purge.
- **Structured progress** — handlers report percent, message, and checkpoint metadata; persisted across retries.
- **OpenTelemetry metrics** — 20+ built-in counters, histograms, and gauges for Prometheus/Grafana. Python workers enable export with `awa.init_telemetry(endpoint, service)`; Rust workers install their own provider.
- **Operator descriptors** — code-declared queue and job-kind names/descriptions with stale/drift visibility in the UI.
- **Postgres-only** — one dependency you already have; no Redis, no RabbitMQ, no separate scheduler.

![AWA Web UI — Queue detail (dark mode)](https://raw.githubusercontent.com/hardbyte/awa/main/docs/images/awa-ui-queue-detail-dark.png)

## Correctness

Core concurrency invariants — no duplicate processing after rescue, stale
completions rejected, no claim/rotate/prune deadlock, DLQ round-trip safety,
prune-segment emptiness, heartbeat-driven short-job rescue — are checked by
[TLA+ models](https://github.com/hardbyte/awa/blob/main/correctness/README.md)
covering the segmented storage engine, the lock-ordering protocol, and the
single/multi-instance worker runtime. The storage model has a trace-replay
harness that verifies concrete runtime-test event sequences against the spec.

## Delivery Contract

- **Transactional enqueue** is a core Postgres-native feature: enqueue inside
  the same transaction as application data, and the job commits or rolls back
  with that data.
- **At-least-once delivery** is the contract. Awa rejects stale completions
  and rescues stuck work, but it does not promise “exactly once”.
- **Idempotency is recommended** for handlers, because retries and recovery are
  part of the honest failure model.
- **No lost work under failure** takes priority over clever fast paths. If a
  design weakens crash/restart safety, it loses even if the benchmark looks
  better.

## Benchmarks

Local queue-storage soak, 5k-job runtime run: **9.5k jobs/s**, **22 ms p95
pickup**, **417 exact final dead tuples**. Enqueue: ~30k/s single-producer,
~100k/s multi-producer.

The repo also carries a phase-driven portable benchmark harness that compares
Awa against PgQue, PGMQ, and pg-boss on a shared Postgres instance and records
producer, subscriber, and end-to-end delivery latency alongside throughput,
queue depth, and dead tuples over time. Current engineering runs show Awa
ahead of PgQue on sustained throughput and subscriber/end-to-end latency in
the event-delivery scenarios, while PgQue still keeps a lower dead-tuple
profile.

Methodology and caveats live in
[benchmarking notes](docs/benchmarking.md). Validation artifacts:
[ADR-019 (queue storage)](docs/adr/bench/019-queue-storage-validation-2026-04-19.md)
and [ADR-023 (receipt-plane ring partitioning)](docs/adr/bench/023-receipt-ring-validation-2026-04-26.md).

## Where Awa Fits

Awa is for teams that already trust Postgres and want a real job queue, not
just a stream or a framework tied to one host language.

- Choose Awa when you want priorities, unique jobs, retries, cron, callbacks,
  DLQ, and operator tooling on one Postgres-backed runtime.
- Choose PgQue-style systems when you want an event queue with independent
  consumer cursors and event-log semantics first.
- Choose River or Oban Pro when you want a job framework tightly shaped around
  one surrounding language ecosystem.

See [docs/positioning.md](docs/positioning.md) for the category map and messaging guidance.

## Getting Started

```bash
# 1. Install
pip install awa-pg awa-cli     # Python
# or: cargo add awa             # Rust

# 2. Start Postgres and run migrations
awa --database-url $DATABASE_URL migrate

# 3. Write a worker and start processing (see examples below)

# 4. Monitor
awa --database-url $DATABASE_URL serve   # → http://127.0.0.1:3000
awa --database-url $DATABASE_URL storage status
awa --database-url $DATABASE_URL job dump 123
awa --database-url $DATABASE_URL job dump-run 123
```

The Awa mental model: your app inserts durable queue entries inside Postgres,
often in the same transaction as business data; workers claim runnable entries
through short-lived execution leases and rescue stale work after crashes;
long-running attempts touch `attempt_state` only when they need mutable data
like progress or callback state; operators inspect live, terminal, and DLQ
state through the CLI or the built-in UI.

Language-specific guides:

- [Rust getting started](docs/getting-started-rust.md)
- [Python getting started](docs/getting-started-python.md)

Configuring real workloads:

- [Worker scope: which queues and which kinds](docs/configuration.md#worker-scope-which-queues-and-which-kinds) — running a worker against one queue, or splitting kinds across queues
- [Job priority and aging](docs/configuration.md#job-priority-and-aging) — priority scale, escalation, the per-queue `priority_aging_interval`
- [Reliability timings](docs/configuration.md#reliability-timings-heartbeat-deadline-rescue) — heartbeat / deadline / callback rescue, retention, the 3× heartbeat-staleness rule
- [Dead Letter Queue](docs/configuration.md#dead-letter-queue) — when to enable, per-queue overrides, operator workflow

Already running 0.5? Read the [0.5 → 0.6 upgrade guide](docs/upgrade-0.5-to-0.6.md)
before you bump — 0.6 introduces a staged storage transition (canonical →
prepared → mixed_transition → active) with a refused-by-default gate that
expects the operator to roll out queue-storage-capable workers first.

## Python Example

<!-- Tested in CI via awa-python/examples/quickstart.py -->

```python
import awa
import asyncio
from dataclasses import dataclass

@dataclass
class SendEmail:
    to: str
    subject: str

async def main():
    client = awa.AsyncClient("postgres://localhost/mydb")
    await client.migrate()

    @client.task(SendEmail, queue="email")
    async def handle_email(job):
        print(f"Sending to {job.args.to}: {job.args.subject}")

    await client.insert(
        SendEmail(to="alice@example.com", subject="Welcome"),
        queue="email",
    )

    client.start([("email", 2)])
    await asyncio.sleep(1)
    await client.shutdown()

asyncio.run(main())
```

**Progress tracking** — checkpoint and resume on retry:

```python
@client.task(BatchImport, queue="etl")
async def handle_import(job):
    last_id = (job.progress or {}).get("metadata", {}).get("last_id", 0)
    for item in fetch_items(after=last_id):
        process(item)
        job.set_progress(50, "halfway")
        job.update_metadata({"last_id": item.id})
    await job.flush_progress()
```

**Transactional enqueue** — atomic with your business logic:

```python
async with await client.transaction() as tx:
    await tx.execute("INSERT INTO orders (id) VALUES ($1)", order_id)
    await tx.insert(SendEmail(to="alice@example.com", subject="Order confirmed"))
```

**Sync API** for Django/Flask — use `awa.Client` for sync frameworks; all methods are plain (no suffix):

```python
client = awa.Client("postgres://localhost/mydb")
client.migrate()
job = client.insert(SendEmail(to="bob@example.com", subject="Hello"))
```

**Sequential callbacks** — suspend a handler, wait for an external system, then resume:

```python
@client.task(ProcessPayment, queue="payments")
async def handle_payment(job):
    token = await job.register_callback(timeout_seconds=3600)
    send_to_payment_gateway(token.id, job.args.amount)
    result = await job.wait_for_callback(token)
    # result contains the payload from resume_external()
    await record_payment(job.args.order_id, result)
```

The external system calls `await client.resume_external(callback_id, {"status": "paid"})` to wake the handler.

**Periodic jobs** — leader-elected cron scheduling with timezone support:

```python
client.periodic(
    "daily_report", "0 9 * * *",
    GenerateReport, GenerateReport(format="pdf"),
    timezone="Pacific/Auckland",
)
```

6-field expressions with seconds precision are also supported: `"*/15 * * * * *"` fires every 15 seconds.

See [`examples/python/`](https://github.com/hardbyte/awa/tree/main/examples/python) for complete runnable scripts tested in CI.

## Rust Example

```rust
use awa::{Client, QueueConfig, JobArgs, JobResult, JobError, JobContext, Worker};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

struct SendEmailWorker;

#[async_trait::async_trait]
impl Worker for SendEmailWorker {
    fn kind(&self) -> &'static str { "send_email" }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: SendEmail = serde_json::from_value(ctx.job.args.clone())
            .map_err(|e| JobError::terminal(e.to_string()))?;
        send_email(&args.to, &args.subject).await
            .map_err(JobError::retryable)?;
        Ok(JobResult::Completed)
    }
}

// Insert a job (with uniqueness)
awa::insert_with(&pool, &SendEmail { to: "alice@example.com".into(), subject: "Welcome".into() },
    awa::InsertOpts { unique: Some(awa::UniqueOpts { by_args: true, ..Default::default() }), ..Default::default() },
).await?;

// Cancel by unique key (e.g., when the triggering condition is resolved)
awa::admin::cancel_by_unique_key(&pool, "send_email", None, Some(&serde_json::json!({"to": "alice@example.com", "subject": "Welcome"})), None).await?;

// Start workers with a typed lifecycle hook
let client = Client::builder(pool)
    .queue("default", QueueConfig::default())
    .register_worker(SendEmailWorker)
    .on_event::<SendEmail, _, _>(|event| async move {
        if let awa::JobEvent::Exhausted { args, error, .. } = event {
            tracing::error!(to = %args.to, error = %error, "email job exhausted retries");
        }
    })
    .build()?;
client.start().await?;
```

Cancellation is cooperative for running handlers:

- Rust handlers can poll `ctx.is_cancelled()`.
- Python handlers can poll `job.is_cancelled()`.
- Shutdown and runtime rescue paths flip that flag.
- Admin cancel (`awa::admin::cancel`, `client.cancel`) always updates job state
  in storage, but a running handler is not currently guaranteed to observe an
  in-memory cancellation signal from admin cancel alone.

## Installation

### Python

```bash
pip install awa-pg       # SDK: insert, worker, admin, progress
pip install awa-cli      # CLI: migrations, queue admin, web UI
```

### Rust

```toml
[dependencies]
awa = "0.5"
```

### CLI

Available via pip (no Rust toolchain needed) or cargo:

```bash
pip install awa-cli
# or: cargo install awa-cli

awa --database-url $DATABASE_URL migrate
awa --database-url $DATABASE_URL serve
awa --database-url $DATABASE_URL queue stats
awa --database-url $DATABASE_URL job list --state failed
awa --database-url $DATABASE_URL job dump 123
awa --database-url $DATABASE_URL job dump-run 123
```

## Architecture

```
 ┌────────────────┐  ┌────────────────┐
 │ Rust producer  │  │  Python (pip)  │
 └───────┬────────┘  └────────┬───────┘
         └────────┬───────────┘
                  ▼
       ┌──────────────────────────────┐
       │          PostgreSQL          │
       │ ready / deferred entries     │
       │ active leases / attempt_state│
       │ terminal / dlq entries       │
       └──────────────┬───────────────┘
                 │
       ┌─────────┼─────────┐
       ▼         ▼         ▼
   ┌────────┐┌────────┐┌────────┐
   │ Worker ││ Worker ││ Worker │
   │ (Rust) ││ (PyO3) ││ (PyO3) │
   └────────┘└────────┘└────────┘
```

All coordination through Postgres. The Rust runtime owns dispatch, leases,
heartbeats, rescue, rotation, prune, and shutdown for both languages. Mixed
Rust and Python workers coexist on the same queues. See
[architecture overview](docs/architecture.md) for full details.

## Workspace

| Crate | Purpose |
|---|---|
| `awa` | Main crate — re-exports `awa-model` + `awa-worker` |
| `awa-model` | Types, queries, migrations, admin ops |
| `awa-macros` | `#[derive(JobArgs)]` proc macro |
| `awa-worker` | Runtime: dispatch, heartbeat, maintenance |
| `awa-ui` | Web UI (axum API + embedded React frontend) |
| `awa-cli` | CLI binary (migrations, admin, serve) |
| `awa-python` | PyO3 extension module (`pip install awa-pg`) |
| `awa-testing` | Test helpers (`TestClient`) |

## Documentation

| Doc | Description |
|---|---|
| [Rust getting started](docs/getting-started-rust.md) | From `cargo add` to a job reaching `completed` |
| [Python getting started](docs/getting-started-python.md) | From `pip install` to a job reaching `completed` |
| [Deployment guide](docs/deployment.md) | Docker, Kubernetes, pool sizing, graceful shutdown |
| [Migration guide](docs/migrations.md) | Fresh installs, upgrades, extracted SQL, rollback strategy |
| [0.5 → 0.6 upgrade](docs/upgrade-0.5-to-0.6.md) | Step-by-step operator checklist for the staged storage transition |
| [Configuration reference](docs/configuration.md) | `QueueConfig`, `ClientBuilder`, Python `start()`, env vars |
| [Security & Postgres roles](docs/security.md) | Minimum-privilege roles, callback auth, operational guidance |
| [Troubleshooting](docs/troubleshooting.md) | Stuck `running` jobs, leader delays, heartbeat timeouts |
| [Architecture overview](docs/architecture.md) | System design, data flow, state machine, crash recovery |
| [Web UI design](docs/ui-design.md) | API endpoints, pages, component library |
| [Benchmarking notes](docs/benchmarking.md) | Methodology, headline numbers, how to run |
| [Validation test plan](docs/test-plan.md) | Full test matrix with 100+ test cases |
| [TLA+ correctness models](correctness/README.md) | Formal verification of core invariants |
| [Grafana dashboards](docs/grafana/README.md) | Pre-built Prometheus dashboards for monitoring |

<details>
<summary>Architecture Decision Records (ADRs)</summary>

- [001: Postgres-only](docs/adr/001-postgres-only.md)
- [002: BLAKE3 uniqueness](docs/adr/002-blake3-uniqueness.md)
- [003: Heartbeat + deadline hybrid](docs/adr/003-heartbeat-deadline-hybrid.md)
- [004: PyO3 async bridge](docs/adr/004-pyo3-async-bridge.md)
- [005: Priority aging](docs/adr/005-priority-aging.md)
- [006: AwaTransaction as narrow SQL surface](docs/adr/006-awa-transaction.md)
- [007: Periodic cron jobs](docs/adr/007-periodic-cron-jobs.md)
- [008: COPY batch ingestion](docs/adr/008-copy-batch-ingestion.md)
- [009: Python sync support](docs/adr/009-python-sync-support.md)
- [010: Per-queue rate limiting](docs/adr/010-rate-limiting.md)
- [011: Weighted concurrency](docs/adr/011-weighted-concurrency.md)
- [012: Split hot and deferred job storage](docs/adr/012-hot-deferred-job-storage.md)
- [013: Durable run leases and guarded finalization](docs/adr/013-run-lease-and-guarded-finalization.md)
- [014: Structured progress and metadata](docs/adr/014-structured-progress.md)
- [015: Builder-side post-commit lifecycle hooks](docs/adr/015-post-commit-lifecycle-hooks.md)
- [016: Shared insert preparation and tokio-postgres adapter](docs/adr/016-bridge-adapters.md)
- [017: Python insert-only transaction bridging](docs/adr/017-python-transaction-bridging.md)
- [018: HTTP Worker for serverless job dispatch](docs/adr/018-http-worker.md)
- [019: Queue Storage Engine](docs/adr/019-queue-storage-redesign.md)
- [020: Dead Letter Queue](docs/adr/020-dead-letter-queue.md)
- [021: Sequential callbacks and callback heartbeats](docs/adr/021-enhanced-external-wait.md)
- [022: Descriptor catalog](docs/adr/022-descriptor-catalog.md)
- [023: Receipt plane ring partitioning](docs/adr/023-receipt-plane-ring-partitioning.md)

See [docs/adr/README.md](docs/adr/README.md) for the index with status and supersession.

</details>

## License

MIT OR Apache-2.0
