# awa-model

Schema, types, and admin surface for the [Awa](https://crates.io/crates/awa)
Postgres-native job queue. This is the foundation crate: every other
`awa-*` crate depends on it, and it owns the SQL migrations, the row
types, and the admin queries that back both the CLI and the web UI.

Most Rust applications should depend on the [`awa`](https://crates.io/crates/awa)
facade rather than `awa-model` directly. Reach for this crate when you
need a smaller dependency footprint (e.g. an enqueue-only producer that
does not link the worker runtime), or when you are building tooling
against the admin API.

## What's in here

- **Job model** — `JobRow`, `JobState`, `InsertOpts`, `UniqueOpts`,
  `InsertParams`.
- **Insertion** — `insert`, `insert_with`, `insert_many`,
  `insert_many_copy` (COPY-batched for large fan-outs).
- **Migrations** — `migrations::run` applies the schema; `migrations`,
  `migrations_range`, and `current_migration_version` expose the
  catalog for tooling.
- **Admin** (`admin`) — retry, cancel (single, by unique key, bulk),
  pause/resume/drain queues, queue and job-kind overviews, runtime
  instance snapshots, dirty-key recompute, and descriptor sync
  (`sync_queue_descriptors`, `sync_job_kind_descriptors`,
  `cleanup_stale_descriptors`).
- **Dead Letter Queue** (`dlq`) — `DlqRow`, `DlqMetadata`,
  `ListDlqFilter`, `RetryFromDlqOpts`, list / retry / move / purge
  helpers backing the `awa dlq` CLI and the DLQ admin UI tab.
- **Cron** (`cron`) — `PeriodicJob`, `PeriodicJobBuilder`, `CronJobRow`.
- **Queue storage** (`queue_storage`) — `QueueStorage`,
  `QueueStorageConfig`, `ClaimedRuntimeJob`, `RotateOutcome`,
  `PruneOutcome`. The vacuum-aware engine introduced in 0.6.
- **Storage status** (`storage`) — `StorageStatus` and the
  transition-state primitives the `awa storage` CLI surfaces.
- **Bridge adapters** (`bridge`) — `PreparedRow` and friends used by
  the Python and tokio-postgres enqueue bridges.
- **Errors** — `AwaError`, the single error type shared across the
  workspace.

`#[derive(JobArgs)]` is re-exported from [`awa-macros`](https://crates.io/crates/awa-macros)
so applications get the macro automatically.

## Example: enqueue-only producer

```rust
use awa_model::{insert, InsertOpts, JobArgs};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    awa_model::migrations::run(&pool).await?;

    let job = insert(&pool, &SendEmail {
        to: "ada@example.com".into(),
        subject: "hi".into(),
    }, InsertOpts::default()).await?;

    println!("enqueued {}", job.id);
    Ok(())
}
```

## Versioning

`awa-model` is versioned in lockstep with the rest of the workspace.
Pin to the same minor version as `awa` and `awa-worker` if you depend
on multiple crates directly.

## See also

- [Architecture overview](../docs/architecture.md)
- [Migrations reference](../docs/migrations.md)
- [Configuration](../docs/configuration.md)
- [Dead Letter Queue](../docs/dead-letter-queue.md)
- [ADR-019: queue storage engine](../docs/adr/019-queue-storage-redesign.md)
- [ADR-022: descriptor catalog](../docs/adr/022-descriptor-catalog.md)

## License

MIT OR Apache-2.0
