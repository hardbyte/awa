# awa-testing

Test utilities for the [Awa](https://crates.io/crates/awa) job queue.

`awa-testing` lets you exercise job handlers and admin code paths in unit and integration tests without spinning up the full worker runtime. Use it for in-tree tests of the workspace, in your own crate's tests, or anywhere you want to drive a single job through a real Postgres without configuring queues, dispatchers, and maintenance leaders.

The crate is `dev-dependencies`-shaped: there is no embedded Postgres, you point it at a real test database (typically a local container on port `15432`).

## What's in here

- `TestClient` — synchronous-feeling wrapper around a `PgPool`:
  - `migrate()` runs the schema and resets the runtime backend so tests start from a known state.
  - `clean()` resets the runtime backend and deletes through the `awa.jobs` compatibility surface plus `awa.queue_meta` for cross-test isolation.
  - `insert(&args)` enqueues one job.
  - `work_one(&worker)` / `work_one_in_queue(&worker, queue)` claim and execute exactly one job through the supplied `Worker`, returning a `WorkResult` (`Completed`, `Failed`, `Snoozed`, `Retryable`, `Cancelled`, `WaitingExternal`, `NoJob`).
  - `get_job(id)` returns the current `JobRow`.
- `WorkResult` — enum with `is_completed()`, `is_failed()`, `is_waiting_external()`, `is_no_job()` predicates.
- `setup` module — `database_url()`, `database_url_with_app_name()`, `pool()`, `pool_with_url()` helpers and `reset_runtime_backend()` for explicit test cleanup.

`TestClient` is intentionally a lightweight compatibility-surface harness. It does not exercise the full dispatcher, queue-storage receipt plane, or maintenance leader; use the worker runtime or integration benchmarks when a test needs production storage-path fidelity.

## Usage

```rust
use awa::JobArgs;
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

struct SendEmailWorker;

#[async_trait::async_trait]
impl awa::Worker for SendEmailWorker {
    fn kind(&self) -> &'static str { "send_email" }
    async fn perform(&self, ctx: &awa::JobContext) -> Result<awa::JobResult, awa::JobError> {
        let _args: SendEmail = serde_json::from_value(ctx.job.args.clone())
            .map_err(|e| awa::JobError::terminal(e.to_string()))?;
        // ... run the side-effect under test ...
        Ok(awa::JobResult::Completed)
    }
}

#[tokio::test]
async fn send_email_completes() {
    let pool = awa_testing::setup::pool(4).await;
    let client = TestClient::from_pool(pool).await;
    client.migrate().await.unwrap();

    client.insert(&SendEmail {
        to: "test@example.com".into(),
        subject: "Test".into(),
    }).await.unwrap();

    let result = client.work_one_in_queue(&SendEmailWorker, Some("default")).await.unwrap();
    assert!(result.is_completed());
}
```

## See also

- [Test plan](../docs/test-plan.md)
- [Development](../docs/development.md)
- For Python projects, the equivalent helpers live in [`awa.testing`](../awa-python/python/awa/testing.py).

## License

MIT OR Apache-2.0
