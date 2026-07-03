# awa-seaorm

SeaORM integration helpers for the [Awa](https://github.com/hardbyte/awa) job queue.

This crate is intentionally small. Awa's core is SQLx/Postgres-native, and a SeaORM `DatabaseConnection` already wraps a `sqlx::PgPool`, so most integration needs no glue at all:

- building a client, running migrations, or reading job state — reach the pool with `awa_pool()` / `pool()` and use Awa's existing APIs unchanged.

The one thing the pool **can't** do is enqueue a job on the same connection as an in-flight SeaORM transaction — `get_postgres_connection_pool()` hands back a separate pooled connection, so a job inserted through it commits independently of your ORM writes. The `insert` / `insert_with` / `insert_raw` functions run Awa's canonical insert SQL through SeaORM's `ConnectionTrait`, so they bind to a `DatabaseConnection` _or_ a `DatabaseTransaction` and let you enqueue a job atomically with the rest of a transaction.

It does **not** replace Awa's core `sqlx` API, reimplement Awa's admin/lifecycle operations, or introduce a new storage engine.

## Usage

```toml
[dependencies]
awa = "0.6"
awa-seaorm = "0.6"
sea-orm = { version = "=2.0.0-rc.38", default-features = false, features = [
    "sqlx-postgres",
    "runtime-tokio-rustls",
] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Enqueue atomically inside a SeaORM transaction

```rust
use awa::JobArgs;
use awa_seaorm::{insert, migrate};
use sea_orm::{Database, TransactionTrait};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendWelcomeEmail {
    user_id: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect(&std::env::var("DATABASE_URL")?).await?;
    migrate(&db).await?;

    // The user row and its welcome-email job commit together, or not at all.
    let txn = db.begin().await?;
    // ... insert your application rows via SeaORM on `txn` ...
    insert(&txn, &SendWelcomeEmail { user_id: 42 }).await?;
    txn.commit().await?;

    Ok(())
}
```

### Build a client / enqueue without a transaction

```rust
use awa::{JobArgs, QueueConfig};
use awa_seaorm::{client_builder, insert, migrate};
use sea_orm::Database;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect(&std::env::var("DATABASE_URL")?).await?;
    migrate(&db).await?;

    let _client = client_builder(&db)
        .queue("email", QueueConfig::default())
        .build()?;

    // Passing the connection (not a transaction) enqueues immediately.
    let job = insert(
        &db,
        &SendEmail {
            to: "ada@example.com".into(),
            subject: "hello".into(),
        },
    )
    .await?;

    println!("enqueued job {}", job.id);
    Ok(())
}
```
