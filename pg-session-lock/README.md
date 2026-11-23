# pg-session-lock

PostgreSQL session advisory locks on pooled [sqlx](https://crates.io/crates/sqlx) connections, and SQLSTATE checks for lock and retry handling.

A session advisory lock (`pg_advisory_lock`, `pg_try_advisory_lock`) outlives the transaction that took it. When a pooled connection goes back to the pool still holding one, the lock stays held until the pool closes that connection, and every other session waiting for the key waits with it. That happens when a future is dropped while the lock request is in flight, when a task panics, and when the unlock fails or is cancelled.

`SessionAdvisoryLock` returns its connection to the pool only after `pg_advisory_unlock` succeeds and `pg_locks` shows the session no longer holds the key. Every other way out closes the connection, and the server releases a closed session's locks.

```rust
use pg_session_lock::SessionAdvisoryLock;

async fn run_if_leader(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    if let Some(mut lock) = SessionAdvisoryLock::try_acquire(pool, 42).await? {
        sqlx::query("SELECT 1").execute(lock.connection()).await?;
        lock.release().await?;
    }
    Ok(())
}
```

For a leader that holds its lock for as long as its connection lives, `into_connection_closed_on_drop` hands over the connection, marked to close when dropped.

Where the work fits in one transaction, prefer `pg_advisory_xact_lock`: it ends with the transaction and needs no guard.

## Also included

- `PG_LOCKS_KEY`: the SQL expression that rebuilds a bigint advisory key from `pg_locks.classid` and `objid`, and `session_holds`, which uses it.
- `is_serialization_failure` (`40001`), `is_deadlock` (`40P01`), `is_lock_not_available` (`55P03`) and `is_query_canceled` (`57014`) for `sqlx::Error`.

## sqlx versions

| pg-session-lock | sqlx |
|---|---|
| 0.1 | 0.9.0 and later 0.9.x |

Each release is tested against sqlx 0.9.0 and the newest 0.9.x.

It enables only sqlx's `postgres` feature. The runtime and TLS features come from your own sqlx dependency.

## License

MIT or Apache-2.0, at your option.
