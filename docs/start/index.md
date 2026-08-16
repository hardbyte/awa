# Choose a client

All AWA clients use the same PostgreSQL schema and job model. Choose by where the code runs, not by a separate server protocol.

| You are building… | Start with | Why |
| --- | --- | --- |
| A Rust producer and worker | [`awa`](../getting-started-rust.md) | Typed arguments, worker runtime, admin API, and migrations in one facade crate |
| A Rust producer only | [`awa-model`](../reference/rust.md#producers-without-workers) | Enqueue and inspect jobs without the worker runtime |
| A Python producer and worker | [`awa-pg`](../getting-started-python.md) | Async and sync clients plus the compiled worker runtime |
| A Python web request with an open transaction | [`awa.bridge`](../bridge-adapters.md) | Enqueue on the application's asyncpg, psycopg, SQLAlchemy, or Django transaction |
| An operator or migration job | [`awa-cli`](cli.md) | Migrations, health checks, queue/job inspection, DLQ administration, and the web UI |

## What every deployment needs

1. A supported PostgreSQL database and credentials.
2. A migration owner that can create or upgrade the `awa` schema.
3. One or more producers that insert jobs.
4. One or more workers registered for the queues and kinds they process.

The migration owner and runtime role can be separate. See [Security](../security.md) for the privilege model and [Deployment](../deployment.md) for rollout and shutdown guidance.

!!! tip "Start locally, keep the production boundary explicit"
    The quickstarts use one database role for clarity. Production deployments should use the least-privilege role split described in the security guide.
