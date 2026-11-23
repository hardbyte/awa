//! PostgreSQL session advisory locks on pooled [sqlx] connections, and the
//! SQLSTATE checks that go with locking.
//!
//! A session advisory lock outlives the transaction that took it. A pooled
//! connection that goes back to the pool still holding one keeps the lock
//! until the pool closes that connection, which with a long `idle_timeout` or
//! `max_lifetime` can be never, and every other session waiting for the key
//! waits with it. [`SessionAdvisoryLock`] returns its connection to the pool
//! only after `pg_advisory_unlock` succeeds and `pg_locks` shows the session no
//! longer holds the key. Every other way out (a dropped future, a panic, a
//! failed or cancelled unlock) closes the connection, and the server releases
//! a closed session's locks.
//!
//! Prefer a transaction-scoped lock (`pg_advisory_xact_lock`) where the work
//! fits in one transaction: it ends with the transaction and needs no guard.
//!
//! Closing is asynchronous: sqlx closes a dropped connection in a background
//! task, so another session can take the key only once the server has seen
//! the connection end. A waiting [`SessionAdvisoryLock::acquire`] holds one
//! of the pool's connections for as long as it waits.
//!
//! # Leaders
//!
//! A leader that keeps its lock for as long as its connection lives
//! ([`SessionAdvisoryLock::into_connection_closed_on_drop`]) must notice when
//! it has lost it. If the network drops the connection, the server releases
//! the lock only once it detects the dead connection, which with default TCP
//! keepalive settings can take hours; meanwhile the old leader may still
//! believe it leads, and once the server notices, another session can lead
//! too. So:
//!
//! - Check [`session_holds`] on the leader's connection periodically, and
//!   step down when it returns `false` or an error.
//! - Set TCP keepalives on the server or the connection (for example
//!   `tcp_keepalives_idle`, `tcp_keepalives_interval` and
//!   `tcp_keepalives_count`) so a dead connection is detected in seconds.
//! - Session locks need a real session: they do not work through a
//!   transaction-mode connection pooler such as PgBouncer in `transaction`
//!   mode, which hands each transaction to any server connection.
//!
//! ```no_run
//! # async fn run(pool: sqlx::PgPool) -> Result<(), sqlx::Error> {
//! use pg_session_lock::SessionAdvisoryLock;
//!
//! if let Some(mut lock) = SessionAdvisoryLock::try_acquire(&pool, 42).await? {
//!     sqlx::query("SELECT 1").execute(lock.connection()).await?;
//!     lock.release().await?;
//! }
//! # Ok(())
//! # }
//! ```

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

/// The bigint advisory key of a `pg_locks` row, as an SQL expression literal
/// for `concat!`, so the query stays a `&'static str` as sqlx requires.
///
/// PostgreSQL shows a bigint key split in two, its high 32 bits in `classid`
/// and its low 32 bits in `objid`, with `objsubid = 1`. This puts the halves
/// back together for comparison with the key: `int8 << 32` does not check for
/// overflow, so a key with its sign bit set comes back negative, as it was
/// taken. The columns are unqualified, so don't join `pg_locks` to a relation
/// that also has them.
///
/// Advisory keys are per database, but `pg_locks` lists the whole cluster's
/// locks, so filter by database too, unless you filter by `pid`:
///
/// ```
/// let sql: &'static str = concat!(
///     "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 \
///        AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
///        AND ",
///     pg_session_lock::pg_locks_key!(),
///     " = $1"
/// );
/// let _ = sqlx::query_scalar::<sqlx::Postgres, i32>(sql);
/// ```
#[macro_export]
macro_rules! pg_locks_key {
    () => {
        "((classid::int8 << 32) | objid::int8)"
    };
}

/// [`pg_locks_key!`] as a constant.
pub const PG_LOCKS_KEY: &str = pg_locks_key!();

/// Whether the session on `conn` holds the bigint advisory key `key`, in
/// either mode.
pub async fn session_holds(conn: &mut PgConnection, key: i64) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(concat!(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks \
         WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted \
           AND objsubid = 1 AND ",
        pg_locks_key!(),
        " = $1)"
    ))
    .bind(key)
    .fetch_one(conn)
    .await
}

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
        let locked: bool = match sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(lock.connection())
            .await
        {
            Ok(locked) => locked,
            Err(err) => return Err(lock.refused(err)),
        };
        if locked {
            Ok(Some(lock))
        } else {
            // Not held, so the connection can go back to the pool.
            drop(lock.conn.take());
            Ok(None)
        }
    }

    /// Takes `pg_advisory_lock(key)` on a connection from `pool`, waiting until
    /// the lock is free. Bound the wait with the session's `lock_timeout`, which
    /// fails with SQLSTATE `55P03` and returns the connection to the pool, or
    /// with a timeout on the future, which closes the connection because the
    /// lock may have been granted as the future was dropped. The wait holds one
    /// of the pool's connections.
    pub async fn acquire(pool: &PgPool, key: i64) -> Result<Self, sqlx::Error> {
        let mut lock = Self {
            conn: Some(pool.acquire().await?),
            key,
        };
        if let Err(err) = sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(key)
            .execute(lock.connection())
            .await
        {
            return Err(lock.refused(err));
        }
        Ok(lock)
    }

    /// The lock request failed with `err`. An error the server returned (a
    /// lock or statement timeout, a cancel) means the lock was not granted,
    /// so the connection goes back to the pool; any other error leaves that
    /// unknown, and dropping the guard closes the connection.
    fn refused(mut self, err: sqlx::Error) -> sqlx::Error {
        if matches!(err, sqlx::Error::Database(_)) {
            drop(self.conn.take());
        }
        err
    }

    /// Hands over the connection holding the lock, marked to close rather than
    /// return to the pool when it is dropped, for a holder that keeps the lock
    /// until its connection ends.
    pub fn into_connection_closed_on_drop(mut self) -> PoolConnection<Postgres> {
        let mut conn = self
            .conn
            .take()
            .expect("the connection is held until release or drop");
        conn.close_on_drop();
        conn
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
    /// and is closed otherwise, so the lock is released either way; the error
    /// only reports that the explicit unlock failed.
    pub async fn release(mut self) -> Result<(), sqlx::Error> {
        // The unlock runs on the connection still owned by the guard, so a
        // drop while it is in flight closes the connection.
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(self.key)
            .fetch_one(self.connection())
            .await?;
        if !released {
            return Ok(());
        }
        // One unlock releases one hold. The connection is pooled only once
        // this session holds the key no more.
        let key = self.key;
        if !session_holds(self.connection(), key).await? {
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

/// The SQLSTATE of a database error, or `None` for any other error.
pub fn sqlstate(err: &sqlx::Error) -> Option<std::borrow::Cow<'_, str>> {
    match err {
        sqlx::Error::Database(database) => database.code(),
        _ => None,
    }
}

fn has_sqlstate(err: &sqlx::Error, code: &str) -> bool {
    sqlstate(err).as_deref() == Some(code)
}

/// The server could not serialize a `REPEATABLE READ` or `SERIALIZABLE`
/// transaction against a concurrent one (SQLSTATE `40001`). Nothing the
/// transaction did was applied; run the whole transaction again.
pub fn is_serialization_failure(err: &sqlx::Error) -> bool {
    has_sqlstate(err, "40001")
}

/// The server aborted the transaction to break a lock cycle (SQLSTATE
/// `40P01`). Nothing the transaction did was applied.
pub fn is_deadlock(err: &sqlx::Error) -> bool {
    has_sqlstate(err, "40P01")
}

/// A lock was not available: `lock_timeout` expired, or a `NOWAIT` lock found
/// it taken (SQLSTATE `55P03`). `SKIP LOCKED` skips such rows without an
/// error.
pub fn is_lock_not_available(err: &sqlx::Error) -> bool {
    has_sqlstate(err, "55P03")
}

/// The statement was cancelled, by `statement_timeout` or by a cancel request
/// such as `pg_cancel_backend` (SQLSTATE `57014`, `query_canceled`). In
/// PL/pgSQL, `WHEN OTHERS` does not catch it: a handler that must run on a
/// cancel names `query_canceled` as well.
pub fn is_query_canceled(err: &sqlx::Error) -> bool {
    has_sqlstate(err, "57014")
}
