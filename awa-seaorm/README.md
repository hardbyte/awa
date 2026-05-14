# awa-seaorm

SeaORM integration helpers for the [Awa](https://github.com/hardbyte/awa)
job queue.

This crate is intentionally small:

- it surfaces the underlying `sqlx::PgPool` from `sea_orm::DatabaseConnection`
- it reuses Awa's existing migration and insertion helpers
- it gives SeaORM-first applications a convenient Awa client builder

It does **not** replace Awa's core `sqlx` API or introduce a new storage
engine. It is just a thin adapter for SeaORM applications that already
own a PostgreSQL connection.

## Usage

```toml
[dependencies]
awa = "0.6.0-alpha.9"
awa-seaorm = "0.6.0-alpha.9"
sea-orm = { version = "=2.0.0-rc.38", default-features = false, features = [
    "sqlx-postgres",
    "runtime-tokio-rustls",
] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use awa::{JobArgs, QueueConfig};
use awa_seaorm::{client_builder, insert, migrate, SeaOrmAwaExt};
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

    let job = insert(
        &db,
        &SendEmail {
            to: "ada@example.com".into(),
            subject: "hello".into(),
        },
    )
    .await?;

    println!("enqueued job {}", job.id);
    let _pool = db.awa_pool();
    Ok(())
}
```
