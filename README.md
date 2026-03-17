# Awa

**Postgres-native background job queue for Rust and Python.**

Awa (Māori: river) provides durable, transactional job enqueueing with typed handlers in both Rust and Python. The Rust runtime handles all queue machinery — polling, LISTEN/NOTIFY wakeups, heartbeating, crash recovery, dispatch — while Python workers run as callbacks via PyO3 on that same runtime, getting Rust-grade queue reliability with Python-native ergonomics.

## Features

- **Postgres-only** — no Redis, no RabbitMQ. One dependency you already have.
- **Transactional enqueue** — insert jobs inside your business transaction. Commit = job exists. Rollback = it doesn't.
- **Two first-class languages** — Rust and Python workers on the same queues with identical semantics.
- **SKIP LOCKED dispatch** — efficient, contention-free job claiming.
- **Heartbeat + deadline crash recovery** — stale jobs rescued automatically.
- **Priority aging** — low-priority jobs won't starve.
- **LISTEN/NOTIFY wakeup** — sub-10ms pickup latency.
- **OpenTelemetry metrics** — built-in counters, histograms, and gauges.

## Quick Start (Rust)

```rust
use awa::{Client, QueueConfig, JobArgs, JobResult, JobError, JobContext, JobRow, Worker};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

#[async_trait::async_trait]
impl Worker for SendEmail {
    fn kind(&self) -> &'static str { "send_email" }

    async fn perform(&self, job: &JobRow, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: SendEmail = serde_json::from_value(job.args.clone())
            .map_err(|e| JobError::terminal(e.to_string()))?;
        send_email(&args.to, &args.subject).await
            .map_err(JobError::retryable)?;
        Ok(JobResult::Completed)
    }
}

// Insert a job
awa::insert(&pool, &SendEmail {
    to: "alice@example.com".into(),
    subject: "Welcome".into(),
}).await?;

// Transactional insert — atomic with your business logic
let mut tx = pool.begin().await?;
create_order(&mut *tx, &order).await?;
awa::insert(&mut *tx, &SendOrderEmail { order_id: order.id }).await?;
tx.commit().await?;

// Start workers
let client = Client::builder(pool)
    .queue("default", QueueConfig::default())
    .register_worker(SendEmail { to: String::new(), subject: String::new() })
    .build()?;
client.start().await?;
```

## Quick Start (Python)

```python
import awa
import asyncio
from dataclasses import dataclass

@dataclass
class SendEmail:
    to: str
    subject: str

client = awa.Client("postgres://localhost/mydb")

# Insert
await client.insert(SendEmail(to="alice@example.com", subject="Welcome"))

# Transactional insert — atomic with your business logic
async with await client.transaction() as tx:
    await tx.execute("INSERT INTO orders (id, total) VALUES ($1, $2)", [order_id, total])
    await tx.insert(SendEmail(to="alice@example.com", subject="Order confirmed"))
    # Commits on success, rolls back on exception

# Worker
@client.worker(SendEmail, queue="email")
async def handle(job):
    await send_email(job.args.to, job.args.subject)

client.start([("email", 10)])
health = await client.health_check()
assert health.heartbeat_alive

await asyncio.sleep(10)
await client.shutdown()
```

> **Note:** `async with await client.transaction()` uses a double-await because
> `transaction()` is an async method that returns a context manager. This is
> inherent to the PyO3 async bridge pattern.

## Setup

```bash
# Run migrations once (not on every app startup)
awa --database-url $DATABASE_URL migrate
# Or from Python:
# await awa.migrate("postgres://...")
```

## Installation

### Rust

```toml
[dependencies]
awa = "0.1"
```

### Python

```bash
pip install awa
```

### CLI

```bash
cargo install awa-cli

awa --database-url $DATABASE_URL migrate
awa --database-url $DATABASE_URL queue stats
awa --database-url $DATABASE_URL job list --state failed
```

## Architecture

```
┌────────────────────┐      ┌────────────────────┐
│ Rust producers     │      │ Python producers   │
│ `awa-model` / `awa`│      │ `pip install awa`  │
└─────────┬──────────┘      └─────────┬──────────┘
          │                           │
          └──────────────┬────────────┘
                         ▼
              ┌──────────────────────┐
              │ PostgreSQL `awa.jobs`│
              └──────────┬───────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│ Rust runtime  │ │ Rust runtime  │ │ Rust runtime  │
│ + Rust worker │ │ + Python cb   │ │ + Python cb   │
│ `awa-worker`  │ │ via PyO3      │ │ via PyO3      │
└───────────────┘ └───────────────┘ └───────────────┘
```

All coordination happens through Postgres, and the Rust runtime owns polling, heartbeats, shutdown, and crash recovery for both Rust and Python handlers. Mixed Rust and Python workers coexist on the same queues — jobs inserted from any language are workable by any language.

## Workspace

| Crate | Purpose |
|---|---|
| `awa` | Facade — re-exports everything |
| `awa-model` | Types, queries, migrations, admin ops |
| `awa-macros` | `#[derive(JobArgs)]` proc macro |
| `awa-worker` | Runtime: dispatch, heartbeat, maintenance |
| `awa-python` | PyO3 extension module |
| `awa-testing` | Test helpers (`TestClient`) |
| `awa-cli` | CLI binary |

## Documentation

- [Architecture](docs/architecture.md)
- [ADR-001: Postgres-only](docs/adr/001-postgres-only.md)
- [ADR-002: BLAKE3 uniqueness](docs/adr/002-blake3-uniqueness.md)
- [ADR-003: Heartbeat + deadline hybrid](docs/adr/003-heartbeat-deadline-hybrid.md)
- [ADR-004: PyO3 async bridge](docs/adr/004-pyo3-async-bridge.md)
- [ADR-005: Priority aging](docs/adr/005-priority-aging.md)
- [ADR-006: AwaTransaction as narrow SQL surface](docs/adr/006-awa-transaction.md)
- [Validation test plan](docs/test-plan.md)

## License

MIT OR Apache-2.0
