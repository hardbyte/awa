use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use awa_model::admin;
use awa_model::job::JobState;
use awa_model::AwaError;
use moka::future::Cache;

/// Typed, TTL-based caches for the dashboard's hot endpoints.
///
/// Each endpoint gets its own `moka::future::Cache` so values are
/// fully typed. Moka handles expiry, eviction, and request coalescing
/// (`try_get_with` ensures only one in-flight fetch per key while
/// concurrent callers wait).
///
/// All caches use a single `()` key since each stores one aggregated
/// response value.
#[derive(Clone)]
pub struct DashboardCache {
    pub stats: Cache<(), HashMap<JobState, i64>>,
    pub queues: Cache<(), Vec<admin::QueueOverview>>,
    pub runtime: Cache<(), admin::RuntimeOverview>,
    pub queue_runtime: Cache<(), Vec<admin::QueueRuntimeSummary>>,
}

fn build_cache<V: Clone + Send + Sync + 'static>(ttl: Duration) -> Cache<(), V> {
    Cache::builder().time_to_live(ttl).max_capacity(1).build()
}

impl DashboardCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            stats: build_cache(ttl),
            queues: build_cache(ttl),
            runtime: build_cache(ttl),
            queue_runtime: build_cache(ttl),
        }
    }
}

/// Shared error wrapper so moka's `try_get_with` can propagate errors.
///
/// `AwaError` is not `Clone`, but moka requires the error from
/// `try_get_with` to be `Clone + Send + Sync`. We wrap it in an `Arc`.
#[derive(Debug, Clone)]
pub struct CacheError(pub Arc<AwaError>);

impl From<AwaError> for CacheError {
    fn from(err: AwaError) -> Self {
        Self(Arc::new(err))
    }
}
