//! Protect owned read connections while SQLx is still acknowledging BEGIN.
//!
//! SQLx records transaction depth after BEGIN completes. Dropping that future
//! earlier can leave a transaction that its normal rollback guard cannot see.

use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};

pub(crate) struct BeginGuard {
    // Callers borrow this field directly so the separate flag can be cleared
    // while the resulting transaction retains its connection borrow.
    pub(crate) connection: PoolConnection<Postgres>,
    pub(crate) begin_pending: bool,
}

impl BeginGuard {
    pub(crate) async fn acquire(pool: &PgPool) -> Result<Self, sqlx::Error> {
        Ok(Self {
            connection: pool.acquire().await?,
            begin_pending: true,
        })
    }
}

impl Drop for BeginGuard {
    fn drop(&mut self) {
        if self.begin_pending {
            self.connection.close_on_drop();
        }
    }
}
