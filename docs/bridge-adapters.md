# Bridge Adapters

Insert Awa jobs within existing transactions from non-sqlx Postgres libraries.

The core `awa::insert` and `awa::insert_with` functions require sqlx's `PgExecutor` trait. Bridge adapters let users of other libraries enqueue jobs without depending on sqlx directly. All adapters share the same preparation logic (validation, state determination, unique key computation) as the sqlx path — no semantic drift between drivers.

## Rust: tokio-postgres

### Dependencies

```toml
[dependencies]
awa = { version = "0.4", features = ["tokio-postgres"] }
tokio-postgres = { version = "0.7", features = ["with-chrono-0_4", "with-serde_json-1"] }
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
let pool_client = pool.get().await?;
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

All functions return `awa::JobRow` with the full row from `RETURNING *` — same type as `awa::insert_with`. The only fields not populated are `errors` (Postgres JSONB array, not auto-deserializable by tokio-postgres) and `unique_states` (BIT(8), not directly mappable).

## Python: psycopg3, asyncpg, SQLAlchemy, Django

See [Python getting started — ORM Transaction Bridging](getting-started-python.md#orm-transaction-bridging).

## Rust feature flags

| Feature | Crate | What it enables |
|---------|-------|-----------------|
| `tokio-postgres` | `awa` or `awa-model` | `awa::bridge::tokio_pg` adapter |
| `cel` | `awa` or `awa-model` | CEL expression evaluation for callback filtering |
| `anyhow` | `awa` | `From<anyhow::Error>` for `JobError` |
