use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;

use crate::cache::DashboardCache;
use crate::error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub read_only: bool,
    pub cache: DashboardCache,
    pub callback_hmac_secret: Option<[u8; 32]>,
    /// Suggested frontend poll interval — at least as long as the cache TTL
    /// so clients don't poll faster than the cache can refresh.
    pub poll_interval_ms: u64,
}

impl AppState {
    pub fn new(
        pool: PgPool,
        read_only: bool,
        cache_ttl: Duration,
        callback_hmac_secret: Option<[u8; 32]>,
    ) -> Self {
        let poll_interval_ms = cache_ttl.as_millis().max(5_000) as u64;
        Self {
            pool,
            read_only,
            cache: DashboardCache::new(cache_ttl),
            callback_hmac_secret,
            poll_interval_ms,
        }
    }

    pub fn require_writable(&self) -> Result<(), ApiError> {
        if self.read_only {
            return Err(ApiError::read_only());
        }
        Ok(())
    }
}

pub async fn detect_read_only(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let read_only =
        sqlx::query_scalar::<_, bool>("SELECT current_setting('transaction_read_only') = 'on'")
            .fetch_one(pool)
            .await?;
    Ok(read_only)
}

/// How the API server decides whether to accept mutation requests.
///
/// `Auto` (the default) probes the Postgres connection — useful for running
/// the UI against a read replica or a read-only role without extra config.
/// Forcing `ReadOnly` is the operator escape hatch when the DB is writable
/// but mutations should still be disabled (e.g. an incident read-out, a
/// shared debugging instance, a less-trusted public UI session).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadOnlyMode {
    /// Probe the DB connection — read-only if Postgres reports
    /// `transaction_read_only = on`, writable otherwise.
    #[default]
    Auto,
    /// Force read-only regardless of DB privilege. Mutation endpoints return
    /// 503 (the same status the auto-detect path uses) and
    /// `/api/capabilities` reports `read_only: true`.
    ReadOnly,
    /// Force writable. Mutations are always permitted at the HTTP layer; the
    /// DB still has the final say at query time.
    Writable,
}

impl ReadOnlyMode {
    /// Resolve to a concrete boolean, probing the pool only when `Auto`.
    pub async fn resolve(self, pool: &PgPool) -> Result<bool, sqlx::Error> {
        match self {
            Self::Auto => detect_read_only(pool).await,
            Self::ReadOnly => Ok(true),
            Self::Writable => Ok(false),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Capabilities {
    pub read_only: bool,
    /// Suggested polling interval in milliseconds. The server sets this
    /// higher for read-only replicas where data is inherently stale.
    pub poll_interval_ms: u64,
}
