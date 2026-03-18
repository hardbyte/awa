# ADR 009: Python Synchronous API Support

**Status:** Accepted
**Date:** 2026-03-18

## Context

Django and Flask web handlers are synchronous. The existing Python API requires `await` for all database operations, forcing sync frameworks to use `asyncio.run()` or adapter libraries. The PRD (section 23, Q3) calls for synchronous methods.

## Decision

Add `_sync` counterparts for every async method on `Client` that touches the database. Use the `block_on()` + `detach()` pattern already proven in `PyClient::new()`:

```rust
fn insert_sync(&self, py: Python<'_>, ...) -> PyResult<PyJob> {
    let pool = self.pool.clone();
    // ... extract Python objects ...
    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async { /* async logic */ })
    })
}
```

`py.detach()` (pyo3 0.28, formerly `allow_threads`) releases the GIL during the blocking call, preventing deadlocks with other Python threads.

### Sync methods added

| Async method | Sync counterpart |
|---|---|
| `insert()` | `insert_sync()` |
| `migrate()` | `migrate_sync()` |
| `transaction()` | `transaction_sync()` -> `SyncTransaction` |
| `retry(job_id)` | `retry_sync(job_id)` |
| `cancel(job_id)` | `cancel_sync(job_id)` |
| `retry_failed(...)` | `retry_failed_sync(...)` |
| `discard_failed(kind)` | `discard_failed_sync(kind)` |
| `pause_queue(...)` | `pause_queue_sync(...)` |
| `resume_queue(queue)` | `resume_queue_sync(queue)` |
| `drain_queue(queue)` | `drain_queue_sync(queue)` |
| `queue_stats()` | `queue_stats_sync()` |
| `list_jobs(...)` | `list_jobs_sync(...)` |
| `health_check()` | `health_check_sync()` |
| `insert_many_copy()` | `insert_many_copy_sync()` |

**NOT synced:** `worker()`, `periodic()`, `start()`, `shutdown()` — these are worker lifecycle methods that are inherently async.

### `SyncTransaction` class

New `PySyncTransaction` wrapping `Arc<Mutex<Option<Transaction>>>`:
- Sync methods: `execute()`, `fetch_one()`, `fetch_optional()`, `fetch_all()`, `insert()`, `insert_many()`, `commit()`, `rollback()`
- Sync context manager: `__enter__`/`__exit__` (not `__aenter__`/`__aexit__`)
- Usage: `with client.transaction_sync() as tx:` — simpler than async's `async with await client.transaction() as tx:`

## Consequences

- Django/Flask handlers can use awa directly without async adapters
- Full type stubs in `_awa.pyi` for IDE support
- `SyncTransaction` registered as a separate Python class alongside `Transaction`
- GIL released during all blocking calls via `py.detach()`
