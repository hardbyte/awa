# awa

Postgres-native background job queue. Transactional enqueue, heartbeat crash recovery, priority aging, retries with backoff, cron, callbacks, unique jobs, dead-letter queue, and a vacuum-aware storage engine designed to keep dead-tuple pressure bounded under sustained load.

This crate is the user-facing facade. It re-exports the worker (`awa-worker`) and model (`awa-model`) crates and is what most Rust applications depend on directly.

```toml
[dependencies]
awa = "0.6"
```

## Quick start

```rust
use awa::{insert_with, Client, InsertOpts, JobArgs, JobResult, QueueConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let client = Client::builder(pool.clone())
        .queue("email", QueueConfig::default())
        .register::<SendEmail, _, _>(|args, _ctx| async move {
            println!("sending to {}: {}", args.to, args.subject);
            Ok(JobResult::Completed)
        })
        .build()?;

    client.start().await?;

    insert_with(
        &pool,
        &SendEmail {
            to: "ada@example.com".into(),
            subject: "hello".into(),
        },
        InsertOpts {
            queue: "email".into(),
            ..Default::default()
        },
    )
    .await?;

    Ok(())
}
```

## What you get

- **Transactional enqueue** — enqueueing a job is a normal `INSERT` you can commit alongside your application's writes.
- **Vacuum-aware storage** — append-only ready/terminal partitions plus rotating lease and receipt rings keep the hot queue tables' dead-tuple footprint bounded under sustained load. See [ADR-019](../docs/adr/019-queue-storage-redesign.md) and [ADR-023](../docs/adr/023-receipt-plane-ring-partitioning.md).
- **Crash-safe execution** — heartbeat-based lease tracking; jobs whose workers vanish are rescued automatically.
- **Per-queue policy** — priorities, priority aging, weighted concurrency, rate limits, deadlines, retry/backoff, cron, dead-letter queue.
- **Unique jobs** — content-keyed deduplication windowed across pending / running / completed.
- **Callbacks and external waits** — wait for an external event without burning a worker slot.
- **First-class Python bindings** — same engine, same SQL, same defaults; see [awa-pg on PyPI](https://pypi.org/project/awa-pg/).

## Partitioned FIFO and ordering keys

Queues default to strict FIFO per `(queue, priority)`. Operators can raise `awa.queue_meta.enqueue_shards` on a contended queue to trade strict FIFO for throughput; the contract then becomes **partitioned FIFO** — strict order within each shard, no ordering promised across shards. Producers can pin related jobs to the same shard with `InsertOpts::ordering_key`:

```rust
let opts = InsertOpts {
    queue: "customer-updates".into(),
    ordering_key: Some(format!("customer-{customer_id}").into_bytes()),
    ..Default::default()
};
awa::insert_with(&pool, &UpdateCustomer { ... }, opts).await?;
```

Jobs sharing an `ordering_key` always pick the same shard, so the shard's strict FIFO carries over to per-key FIFO. At `enqueue_shards = 1` the key is ignored. See [ADR-025](../docs/adr/025-sharded-enqueue-heads.md) for the full contract.

## Logical Queue Fanout

For very hot workloads, `QueueFanout` gives Rust producers and workers the same deterministic physical queue set for one logical queue:

```rust
use awa::{InsertOpts, QueueConfig, QueueFanout};

let updates = QueueFanout::new("customer-updates", 4)?;

let client = awa::Client::builder(pool.clone())
    .queue_fanout(&updates, QueueConfig {
        max_workers: 32, // per physical queue
        ..Default::default()
    })
    .register::<UpdateCustomer, _, _>(handle_update)
    .build()?;

let opts = updates.route_opts_by_key(
    InsertOpts::default(),
    format!("customer-{customer_id}").into_bytes(),
);
awa::insert_with(&pool, &UpdateCustomer { ... }, opts).await?;
```

This is ordinary Awa queueing under the hood: each physical queue keeps its durable claim, lease, retry, DLQ, and completion behavior. The helper only standardizes naming and routing so applications do not hand-roll fanout.

## Documentation

- [Getting started (Rust)](../docs/getting-started-rust.md)
- [Configuration](../docs/configuration.md)
- [Architecture](../docs/architecture.md)
- [Migrations](../docs/migrations.md)
- [Upgrading 0.5.x → 0.6](../docs/upgrade-0.5-to-0.6.md)
- [Dead Letter Queue](../docs/dead-letter-queue.md)
- [Deployment](../docs/deployment.md)
- [Cross-system benchmark comparison](https://github.com/hardbyte/postgresql-job-queue-benchmarking)

## License

Dual-licensed under MIT or Apache-2.0, at your option.
