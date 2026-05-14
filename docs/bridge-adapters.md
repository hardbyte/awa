# Bridge Adapters

Insert Awa jobs within existing transactions from non-sqlx Postgres libraries.

The core `awa::insert` and `awa::insert_with` functions require sqlx's `PgExecutor` trait. Bridge adapters let users of other libraries enqueue jobs without depending on sqlx directly. All adapters share the same preparation logic (validation, state determination, unique key computation) as the sqlx path — no semantic drift between drivers.

## Rust: external adapter API

External Rust integration crates can reuse Awa's canonical insert preparation
without depending on sqlx executors. The stable surface is
`awa::adapter::postgres`:

```rust
use awa::adapter::postgres::{
    prepare_job_insert, INSERT_JOB_SQL, UNIQUE_VIOLATION_SQLSTATE,
};
use awa::InsertOpts;

let prepared = prepare_job_insert(&args, InsertOpts::default())?;

// Execute INSERT_JOB_SQL with your driver's transaction or connection type.
// Bind values in this order:
// 1. prepared.kind()
// 2. prepared.queue()
// 3. prepared.args()
// 4. prepared.state_db_str()
// 5. prepared.priority()
// 6. prepared.max_attempts()
// 7. prepared.run_at()
// 8. prepared.metadata()
// 9. prepared.tags()
// 10. prepared.unique_key()
// 11. prepared.unique_states_bit_string()
// 12. prepared.ordering_key()
```

`prepare_job_insert` and `prepare_raw_job_insert` apply the same validation,
scheduled-state selection, unique-key computation, unique-state bitmask
formatting, and sharded enqueue ordering-key propagation as Awa's built-in
sqlx insert path. Adapters should map Postgres SQLSTATE
`UNIQUE_VIOLATION_SQLSTATE` to `AwaError::UniqueConflict`.

This API is intentionally single-row. High-throughput bulk ingestion remains
on Awa's native SQLx-backed APIs (`insert_many_copy_from_pool`) and the
queue-storage-native COPY path (`QueueStorage::enqueue_params_copy`), because
COPY support, row return semantics, and uniqueness handling are
driver-specific.

Worker polling, heartbeating, claiming, and completion remain on the Awa
runtime. That does not prevent applications from using their existing
database stack inside job handlers: pass a SeaORM connection, Diesel pool, or
other app dependency through `Client::builder(...).state(...)` and extract it
from `JobContext`. Integration crates can provide ergonomic helpers for this
handler dependency wiring, but should not reimplement Awa's lease/runtime
storage engine.

## Rust: tokio-postgres

### Dependencies

```toml
[dependencies]
awa = { version = "0.6.0-beta.1", features = ["tokio-postgres"] }
tokio-postgres = { version = "0.7", features = ["with-chrono-0_4", "with-serde_json-1", "with-uuid-1"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Basic insert

```rust
use awa::bridge::tokio_pg;
use awa::JobArgs;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) =
        tokio_postgres::connect("postgres://localhost/mydb", NoTls).await?;
    tokio::spawn(connection);

    let job = tokio_pg::insert_job(
        &client,
        &SendEmail { to: "alice@example.com".into(), subject: "Welcome".into() },
    ).await?;

    println!("inserted job {} (kind={}, state={:?})", job.id, job.kind, job.state);
    Ok(())
}
```

### Transactional enqueue

The primary use case: insert app data and an Awa job atomically in the same transaction. If the transaction rolls back, both the app row and the job disappear.

```rust
use awa::bridge::tokio_pg;
use awa::InsertOpts;

let mut client = client; // from connect()
let txn = client.transaction().await?;

// App logic
txn.execute(
    "INSERT INTO orders (id, total) VALUES ($1, $2)",
    &[&order_id, &total],
).await?;

// Awa job in the same transaction
let job = tokio_pg::insert_job_with(
    &txn,
    &SendEmail { to: "alice@example.com".into(), subject: "Order confirmed".into() },
    InsertOpts {
        queue: "email".into(),
        priority: 1,
        ..Default::default()
    },
).await?;

txn.commit().await?;
// Both the order and the job are now visible. Rollback would discard both.
```

### Supported types

`insert_job` and `insert_job_with` accept any `C: tokio_postgres::GenericClient`. This trait is implemented for:

- `tokio_postgres::Client` — direct connection
- `tokio_postgres::Transaction<'_>` — active transaction

Pool wrappers like `deadpool_postgres::Client` or `bb8::PooledConnection` typically `Deref` to `tokio_postgres::Client` but do **not** implement `GenericClient` directly. To use them, call `.transaction()` on the wrapper and pass the resulting `tokio_postgres::Transaction`:

```rust
// deadpool-postgres
let mut pool_client = pool.get().await?;
let txn = pool_client.transaction().await?;
tokio_pg::insert_job(&txn, &args).await?;
txn.commit().await?;
```

### Raw insert

When you don't have a `JobArgs` impl (e.g. forwarding from a dynamic source):

```rust
let job = tokio_pg::insert_job_raw(
    &txn,
    "send_email".into(),
    serde_json::json!({"to": "alice@example.com", "subject": "Welcome"}),
    InsertOpts::default(),
).await?;
```

### Return value

All functions return `awa::JobRow` with the full row from `RETURNING *` — same type as `awa::insert_with`. The only field not populated is `unique_states` (BIT(8), no direct tokio-postgres mapping). All other fields, including `errors`, are decoded from the database row.

## Rust: SeaORM

SeaORM already sits on top of SQLx, so this adapter keeps the integration
thin: it surfaces the underlying `sqlx::PgPool` from
`sea_orm::DatabaseConnection`, then reuses Awa's existing insert and
migration helpers on that pool.

The adapter lives in the optional `awa-seaorm` crate:

```toml
[dependencies]
awa = "0.6.0-alpha.9"
awa-seaorm = "0.6.0-alpha.9"
sea-orm = { version = "=2.0.0-rc.38", default-features = false, features = [
    "sqlx-postgres",
    "runtime-tokio-rustls",
] }
```

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

    insert(
        &db,
        &SendEmail {
            to: "ada@example.com".into(),
            subject: "hello".into(),
        },
    )
    .await?;

    Ok(())
}
```

## Python: psycopg3, asyncpg, SQLAlchemy, Django

See [Python getting started — ORM Transaction Bridging](getting-started-python.md#orm-transaction-bridging).

## Rust feature flags

| Feature | Crate | What it enables |
|---------|-------|-----------------|
| `tokio-postgres` | `awa` or `awa-model` | `awa::bridge::tokio_pg` adapter |
| `cel` | `awa` or `awa-model` | CEL expression evaluation for callback filtering |
| `anyhow` | `awa` | `From<anyhow::Error>` for `JobError` |
