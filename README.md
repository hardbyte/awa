# Awa

**Postgres-native background job queue for Rust and Python.**

Awa (Māori: river) provides durable, transactional job enqueueing with typed handlers in both Rust and Python. All queue state lives in Postgres — no Redis, no RabbitMQ. The Rust runtime handles polling, heartbeating, crash recovery, and dispatch. Python workers run on that same runtime via PyO3, getting Rust-grade reliability with Python-native ergonomics.

![AWA Web UI — Dashboard (dark mode)](https://raw.githubusercontent.com/hardbyte/awa/main/docs/images/awa-ui-dark.png)

## Features

- **Postgres-only** — one dependency you already have.
- **Transactional enqueue** — insert jobs inside your business transaction. Commit = visible. Rollback = gone.
- **Cancel by unique key** — cancel scheduled jobs by their insert-time components (kind + args) without storing job IDs.
- **Rust and Python workers** — same queues, identical semantics, mixed deployments.
- **Crash recovery** — heartbeat + hard deadline rescue. Stale jobs recovered automatically.
- **Web UI** — dashboard, job inspector, queue management, cron controls.
- **Structured progress** — handlers report percent, message, and checkpoint metadata; persisted across retries.
- **Periodic/cron jobs** — leader-elected scheduler with timezone support and atomic enqueue.
- **Webhook callbacks** — park jobs for external completion with optional CEL expression filtering.
- **LISTEN/NOTIFY wakeup** — sub-10ms pickup latency.
- **OpenTelemetry** — 20 built-in metrics (counters, histograms, gauges) for Prometheus/Grafana.
- **Hot/cold storage** — runnable work in a hot table, deferred work in a cold table.
- **Rate limiting** — per-queue token bucket. **Weighted concurrency** — global worker pool with per-queue guarantees.

Local benchmarks show ~8k jobs/sec sustained throughput (Rust workers), ~5k jobs/sec (Python workers), and sub-10ms p50 pickup latency. See [benchmarking notes](https://github.com/hardbyte/awa/blob/main/docs/benchmarking.md) for methodology and caveats.

Core concurrency invariants (no duplicate processing after rescue, stale completions rejected, shutdown drain ordering) are checked with [TLA+ models](https://github.com/hardbyte/awa/blob/main/correctness/README.md) covering single and multi-instance deployments.

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
```

Language-specific guides:

- [Rust getting started](https://github.com/hardbyte/awa/blob/main/docs/getting-started-rust.md)
- [Python getting started](https://github.com/hardbyte/awa/blob/main/docs/getting-started-python.md)

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

## Installation

### Python

```bash
pip install awa-pg       # SDK: insert, worker, admin, progress
pip install awa-cli      # CLI: migrations, queue admin, web UI
```

### Rust

```toml
[dependencies]
awa = "0.4"
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
```

## Architecture

```
 ┌────────────────┐  ┌────────────────┐
 │ Rust producer  │  │  Python (pip)  │
 └───────┬────────┘  └────────┬───────┘
         └────────┬───────────┘
                  ▼
       ┌────────────────────┐
       │     PostgreSQL     │
       │     jobs_hot       │
       │     scheduled_jobs │
       └─────────┬──────────┘
                 │
       ┌─────────┼─────────┐
       ▼         ▼         ▼
   ┌────────┐┌────────┐┌────────┐
   │ Worker ││ Worker ││ Worker │
   │ (Rust) ││ (PyO3) ││ (PyO3) │
   └────────┘└────────┘└────────┘
```

All coordination through Postgres. The Rust runtime owns polling, heartbeats, shutdown, and crash recovery for both languages. Mixed Rust and Python workers coexist on the same queues. See [architecture overview](https://github.com/hardbyte/awa/blob/main/docs/architecture.md) for full details.

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
| [Rust getting started](https://github.com/hardbyte/awa/blob/main/docs/getting-started-rust.md) | From `cargo add` to a job reaching `completed` |
| [Python getting started](https://github.com/hardbyte/awa/blob/main/docs/getting-started-python.md) | From `pip install` to a job reaching `completed` |
| [Deployment guide](https://github.com/hardbyte/awa/blob/main/docs/deployment.md) | Docker, Kubernetes, pool sizing, graceful shutdown |
| [Migration guide](https://github.com/hardbyte/awa/blob/main/docs/migrations.md) | Fresh installs, upgrades, extracted SQL, rollback strategy |
| [Configuration reference](https://github.com/hardbyte/awa/blob/main/docs/configuration.md) | `QueueConfig`, `ClientBuilder`, Python `start()`, env vars |
| [Troubleshooting](https://github.com/hardbyte/awa/blob/main/docs/troubleshooting.md) | Stuck `running` jobs, leader delays, heartbeat timeouts |
| [Architecture overview](https://github.com/hardbyte/awa/blob/main/docs/architecture.md) | System design, data flow, state machine, crash recovery |
| [Web UI design](https://github.com/hardbyte/awa/blob/main/docs/ui-design.md) | API endpoints, pages, component library |
| [Benchmarking notes](https://github.com/hardbyte/awa/blob/main/docs/benchmarking.md) | Methodology, headline numbers, how to run |
| [Validation test plan](https://github.com/hardbyte/awa/blob/main/docs/test-plan.md) | Full test matrix with 100+ test cases |
| [TLA+ correctness models](https://github.com/hardbyte/awa/blob/main/correctness/README.md) | Formal verification of core invariants |
| [Grafana dashboards](https://github.com/hardbyte/awa/blob/main/docs/grafana/README.md) | Pre-built Prometheus dashboards for monitoring |

<details>
<summary>Architecture Decision Records (ADRs)</summary>

- [001: Postgres-only](https://github.com/hardbyte/awa/blob/main/docs/adr/001-postgres-only.md)
- [002: BLAKE3 uniqueness](https://github.com/hardbyte/awa/blob/main/docs/adr/002-blake3-uniqueness.md)
- [003: Heartbeat + deadline hybrid](https://github.com/hardbyte/awa/blob/main/docs/adr/003-heartbeat-deadline-hybrid.md)
- [004: PyO3 async bridge](https://github.com/hardbyte/awa/blob/main/docs/adr/004-pyo3-async-bridge.md)
- [005: Priority aging](https://github.com/hardbyte/awa/blob/main/docs/adr/005-priority-aging.md)
- [006: AwaTransaction as narrow SQL surface](https://github.com/hardbyte/awa/blob/main/docs/adr/006-awa-transaction.md)
- [007: Periodic cron jobs](https://github.com/hardbyte/awa/blob/main/docs/adr/007-periodic-cron-jobs.md)
- [008: COPY batch ingestion](https://github.com/hardbyte/awa/blob/main/docs/adr/008-copy-batch-ingestion.md)
- [009: Python sync support](https://github.com/hardbyte/awa/blob/main/docs/adr/009-python-sync-support.md)
- [010: Per-queue rate limiting](https://github.com/hardbyte/awa/blob/main/docs/adr/010-rate-limiting.md)
- [011: Weighted concurrency](https://github.com/hardbyte/awa/blob/main/docs/adr/011-weighted-concurrency.md)
- [012: Split hot and deferred job storage](https://github.com/hardbyte/awa/blob/main/docs/adr/012-hot-deferred-job-storage.md)
- [013: Durable run leases and guarded finalization](https://github.com/hardbyte/awa/blob/main/docs/adr/013-run-lease-and-guarded-finalization.md)
- [014: Structured progress and metadata](https://github.com/hardbyte/awa/blob/main/docs/adr/014-structured-progress.md)
- [015: Builder-side post-commit lifecycle hooks](https://github.com/hardbyte/awa/blob/main/docs/adr/015-post-commit-lifecycle-hooks.md)

</details>

## License

MIT OR Apache-2.0
