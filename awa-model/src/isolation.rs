//! Transactions awa opens on its own pool connections run at READ COMMITTED.
//!
//! Queue storage takes a lock and then reads what earlier holders of that
//! lock committed: a producer reads the ring cursor after its lane's
//! reservation lock, and a claim reads ready rows after its lane lock. At READ
//! COMMITTED each statement sees every row committed before it started,
//! including rows committed while an earlier statement waited for the lock.
//! At REPEATABLE READ or SERIALIZABLE the transaction keeps its first snapshot
//! and those rows stay hidden (#847). A role or database
//! `default_transaction_isolation` would otherwise apply to awa's own
//! transactions, so they name their isolation level instead of inheriting it.
//! A transaction a caller passes in keeps the caller's level.
//!
//! `scripts/check-transaction-isolation.py` rejects `begin()` on a pool in
//! library code.

use sqlx::{PgPool, Postgres, Transaction};
use std::future::Future;

/// The statement awa's own transactions start with.
pub const BEGIN_READ_COMMITTED: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";

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
