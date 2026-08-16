# Python API

Install `awa-pg` for the Python clients and worker runtime:

```bash
pip install awa-pg
```

The package ships type information for its public surface. This page is a map; use Python help and your editor against the installed version for exact signatures.

## Clients

`awa.AsyncClient`
: Async migrations, workers, enqueue, admin queries, callbacks, and graceful shutdown.

`awa.Client`
: Synchronous counterpart for scripts, workers, and producers.

`awa.RawClient`
: Lower-level access when an application needs untyped job operations.

`awa.PartitionedQueue`
: Deterministic routing across physical queues for a partitioned logical queue.

## Common values

- `Job` and `JobState` describe hydrated job state.
- `HealthCheck`, `QueueHealth`, and `QueueStat` expose operational status.
- `CallbackToken`, `WaitForCallback`, and `ResolveResult` model callback waits.
- `RetryAfter`, `Snooze`, and `Cancel` are handler outcomes.
- `DlqEntry` and `RetryFailedResult` support failure administration.

## Transaction bridges

`awa.bridge` inserts jobs through application-owned asyncpg, psycopg 3, SQLAlchemy, and Django transactions. See [Bridge adapters](../bridge-adapters.md) before using a framework session: the application, not AWA, remains responsible for commit and rollback.

For a complete runnable program, follow the [Python getting started guide](../getting-started-python.md).
