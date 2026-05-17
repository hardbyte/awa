# awa-seaorm

SeaORM 2.0 integration for the Awa job queue.

This crate exposes a `JobRepository` that works with SeaORM connections and
transactions. It keeps Awa's canonical insert preparation logic, uses SeaORM
entities for the schema-facing model layer, and uses SeaORM raw statements for
PostgreSQL-specific job lifecycle transitions.

```rust
use awa::JobArgs;
use awa_seaorm::{migrate, JobRepository};
use sea_orm::{Database, TransactionTrait};
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

    let txn = db.begin().await?;
    let jobs = JobRepository::new(&txn);
    let job = jobs
        .insert(&SendEmail {
            to: "ada@example.com".into(),
            subject: "hello".into(),
        })
        .await?;
    txn.commit().await?;

    println!("enqueued job {}", job.id);
    Ok(())
}
```
