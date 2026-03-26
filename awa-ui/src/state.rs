use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;

use crate::cache::ResponseCache;
use crate::error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub read_only: bool,
    pub cache: Arc<ResponseCache>,
    /// Suggested frontend poll interval — at least as long as the cache TTL
    /// so clients don't poll faster than the cache can refresh.
    pub poll_interval_ms: u64,
}

impl AppState {
    pub fn new(pool: PgPool, read_only: bool, cache_ttl: Duration) -> Self {
        let poll_interval_ms = cache_ttl.as_millis().max(5_000) as u64;
        Self {
            pool,
            read_only,
            cache: Arc::new(ResponseCache::new(cache_ttl)),
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

#[derive(Debug, Serialize)]
pub struct Capabilities {
    pub read_only: bool,
    /// Suggested polling interval in milliseconds. The server sets this
    /// higher for read-only replicas where data is inherently stale.
    pub poll_interval_ms: u64,
}
