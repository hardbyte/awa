//! The isolation level of the transactions awa opens for its own work.
//!
//! Queue storage takes a lock and then reads what earlier holders of that
//! lock committed: a producer reads the ring cursor after its lane's
//! reservation lock, and a claim reads ready rows after its lane lock. At READ
//! COMMITTED each statement sees every row committed before it started,
//! including rows committed while an earlier statement waited for the lock.
//! At REPEATABLE READ or SERIALIZABLE the transaction keeps its first snapshot
//! and those rows stay hidden (#847). A role or database
//! `default_transaction_isolation` would otherwise apply to awa's own
//! transactions, so they name their isolation level instead of inheriting it:
//! READ COMMITTED for every transaction that writes, and REPEATABLE READ,
//! READ ONLY for the few reads that want one consistent snapshot.
//! A transaction a caller passes in, or one awa opens and hands to the
//! application (the Python client's `transaction()`), keeps the caller's level.
//!
//! `scripts/check-transaction-isolation.py` rejects `begin()` on a pool in
//! library code.

use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};
use std::future::Future;

/// The statement awa's own transactions start with.
pub const BEGIN_READ_COMMITTED: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";

/// The prefix awa's own multi-statement `BEGIN`s start with, followed by their
/// `SET LOCAL` settings.
macro_rules! begin_read_committed_with {
    ($settings:literal) => {
        concat!("BEGIN ISOLATION LEVEL READ COMMITTED; ", $settings)
    };
}
pub(crate) use begin_read_committed_with;

/// Opens a READ COMMITTED transaction on a connection awa holds for its own
/// work. Inside a transaction already open on `conn`, it opens a savepoint
/// instead, which runs at the outer transaction's level.
pub async fn begin_read_committed_on(
    conn: &mut PgConnection,
) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    if conn.is_in_transaction() {
        // isolation: a savepoint, at the enclosing transaction's level
        conn.begin().await
    } else {
        Connection::begin_with(conn, BEGIN_READ_COMMITTED).await
    }
}

/// Opens a READ COMMITTED transaction whatever the session's default
/// isolation level. The level is part of the `BEGIN`, so it costs no extra
/// round trip.
pub trait BeginReadCommitted {
    /// Like [`PgPool::begin`], but at READ COMMITTED.
    fn begin_read_committed(
        &self,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, sqlx::Error>> + Send;
}

impl BeginReadCommitted for PgPool {
    fn begin_read_committed(
        &self,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, sqlx::Error>> + Send {
        self.begin_with(BEGIN_READ_COMMITTED)
    }
}
