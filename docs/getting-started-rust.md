# Rust Getting Started

This guide takes you from `cargo add` to a job reaching `completed`.

## Mental Model

Before writing code, it helps to know what Awa is doing for you:

- enqueueing inserts a durable row into Postgres; if your transaction rolls back, the job disappears too
- workers claim runnable rows, increment the attempt, and keep the claim alive with heartbeats
- retries, callback waits, and progress updates all land back on that same job row
- inspection is row-centric: when something looks wrong, dump the job and inspect its current state, progress, callback config, and recorded errors

The important habit is to treat Postgres as the system of record for job execution, not worker memory.

## Prerequisites

- PostgreSQL running locally or remotely
- Rust toolchain installed
- A database URL exported as `DATABASE_URL`

Example local URL:

```bash
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test
```

## 1. Create a Project

```bash
cargo new awa-rust-quickstart
cd awa-rust-quickstart

cargo add awa
cargo add sqlx --features runtime-tokio-rustls,postgres
cargo add tokio --features macros,rt-multi-thread,time
cargo add serde --features derive
```

## 2. Add a Worker

Put this in `src/main.rs`:

```rust
use awa::{admin, insert_with, migrations, Client, InsertOpts, JobArgs, JobResult, QueueConfig};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::{env, time::Duration};

#[derive(Debug, Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

impl JobArgs for SendEmail {
    fn kind() -> &'static str {
        "send_email"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    migrations::run(&pool).await?;

    let client = Client::builder(pool.clone())
        .queue(
            "email",
            QueueConfig {
                max_workers: 2,
                ..Default::default()
            },
        )
        .register::<SendEmail, _, _>(|args, _ctx| async move {
            println!("sending email to {}: {}", args.to, args.subject);
            Ok(JobResult::Completed)
        })
        .build()?;

    client.start().await?;

    let job = insert_with(
        &pool,
        &SendEmail {
            to: "alice@example.com".into(),
            subject: "Welcome".into(),
        },
        InsertOpts {
            queue: "email".into(),
            ..Default::default()
        },
    )
    .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let job = admin::get_job(&pool, job.id).await?;
    println!("job {} state = {:?}", job.id, job.state);

    client.shutdown(Duration::from_secs(5)).await;
    Ok(())
}
```

## 3. Run It

```bash
cargo run
```

Expected output is similar to:

```text
sending email to alice@example.com: Welcome
job 1 state = Completed
```

## 4. Inspect the Queue

Install the CLI if you want migration/admin/UI commands:

```bash
pip install awa-cli
```

Then inspect what happened:

```bash
awa --database-url "$DATABASE_URL" job list --queue email
awa --database-url "$DATABASE_URL" job dump 1
awa --database-url "$DATABASE_URL" job dump-run 1
awa --database-url "$DATABASE_URL" queue stats
awa --database-url "$DATABASE_URL" serve
```

`job dump` prints the full job snapshot as JSON. `job dump-run` prints one attempt-oriented view: the current attempt comes from the live job row, while older attempts are reconstructed from the recorded error history.

The UI starts on `http://127.0.0.1:3000` by default.

## Production Notes

- This quickstart implements `JobArgs` manually to keep the dependency set minimal. If you want `#[derive(JobArgs)]`, add a direct dependency on `awa-model` and derive `awa_model::JobArgs`.
- `Client::start()` spawns background tasks and returns immediately. Your service should usually stay alive until it receives a shutdown signal.
- `Client::shutdown(Duration)` is the graceful drain path. Set your container or process shutdown timeout slightly above that duration.
- If you only need to enqueue jobs from Rust, depend on `awa-model` instead of `awa`.

## Next

- [Configuration reference](configuration.md)
- [Deployment guide](deployment.md)
- [Migration guide](migrations.md)
- [Troubleshooting](troubleshooting.md)
- [Advanced Rust example](../awa/examples/etl_pipeline.rs)
