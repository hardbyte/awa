//! Session advisory locks held on a pooled connection.
//!
//! A session advisory lock outlives the transaction that took it. A pooled
//! connection that goes back to the pool still holding one keeps the lock
//! until the pool closes that connection, which with a long `idle_timeout` or
//! `max_lifetime` can be never. [`SessionAdvisoryLock`] returns the connection
//! to the pool only after `pg_advisory_unlock` succeeds and `pg_locks` shows
//! the session no longer holds the key; every other way out, including a dropped future, a panic or a failed
//! unlock, closes the connection, and closing the session releases the lock.
//!
//! Prefer a transaction-scoped lock (`pg_advisory_xact_lock`) where the work
//! fits in one transaction. `scripts/check-advisory-locks.py` rejects session
//! locks taken from Rust anywhere but here.

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

/// A session advisory lock held on a connection checked out of a pool.
///
/// The connection goes back to the pool only through [`release`] after the
/// unlock succeeds. Dropping the guard closes the connection instead.
///
/// [`release`]: SessionAdvisoryLock::release
#[derive(Debug)]
pub struct SessionAdvisoryLock {
    conn: Option<PoolConnection<Postgres>>,
    key: i64,
}

impl SessionAdvisoryLock {
    /// Takes `pg_try_advisory_lock(key)` on a connection from `pool`. Returns
    /// `None`, with the connection back in the pool, when another session
    /// holds the lock.
    pub async fn try_acquire(pool: &PgPool, key: i64) -> Result<Option<Self>, sqlx::Error> {
        // The guard owns the connection before the lock request goes out: if
        // this future is dropped while the request is in flight, the lock may
        // already be granted, so the connection must be closed, not pooled.
        let mut lock = Self {
            conn: Some(pool.acquire().await?),
            key,
        };
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(lock.connection())
            .await?;
        if locked {
            Ok(Some(lock))
        } else {
            // Not held, so the connection can go back to the pool.
            drop(lock.conn.take());
            Ok(None)
        }
    }

    /// The connection holding the lock.
    pub fn connection(&mut self) -> &mut PgConnection {
        self.conn
            .as_mut()
            .expect("the connection is held until release or drop")
    }

    /// The lock key.
    pub fn key(&self) -> i64 {
        self.key
    }

    /// Releases the lock. The connection goes back to the pool when the unlock
    /// succeeds and the session holds the key no more (session locks stack, so
    /// a key taken again through [`connection`](Self::connection) stays held),
    /// and is closed otherwise, so the lock is released either way;
    /// the error only reports that the explicit unlock failed.
    pub async fn release(mut self) -> Result<(), sqlx::Error> {
        // The unlock runs on the connection still owned by the guard, so a
        // drop while it is in flight closes the connection.
        let key = self.key;
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .fetch_one(self.connection())
            .await?;
        if !released {
            return Ok(());
        }
        // Session advisory locks stack: one unlock releases one hold. The
        // connection is pooled only once this session holds the key no more.
        let still_held: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks \
             WHERE locktype = 'advisory' AND pid = pg_backend_pid() \
               AND objsubid = 1 \
               AND classid = (($1 >> 32) & 4294967295)::oid \
               AND objid = ($1 & 4294967295)::oid)",
        )
        .bind(key)
        .fetch_one(self.connection())
        .await?;
        if !still_held {
            drop(self.conn.take());
        }
        Ok(())
    }
}

impl Drop for SessionAdvisoryLock {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.as_mut() {
            conn.close_on_drop();
        }
    }
}
