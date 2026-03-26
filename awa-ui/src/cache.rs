use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

struct CacheEntry {
    value: Arc<dyn Any + Send + Sync>,
    expires_at: Instant,
}

/// A simple in-memory cache with a fixed TTL.
///
/// Each key stores a type-erased value that is downcast on retrieval.
/// Concurrent misses may duplicate work (no request coalescing) which
/// is acceptable for a dashboard polling use-case.
pub struct ResponseCache {
    ttl: Duration,
    entries: RwLock<HashMap<String, CacheEntry>>,
}

impl ResponseCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Return a cached value if fresh, otherwise call `fetch` and cache the
    /// result. `T` must match the type previously stored under `key`.
    pub async fn get_or_fetch<T, E, F, Fut>(&self, key: &str, fetch: F) -> Result<T, E>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        // Fast path — read lock only.
        {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(key) {
                if entry.expires_at > Instant::now() {
                    if let Some(value) = entry.value.downcast_ref::<T>() {
                        return Ok(value.clone());
                    }
                }
            }
        }

        // Cache miss — execute the query.
        let value = fetch().await?;

        // Store result under write lock.
        {
            let mut entries = self.entries.write().await;
            entries.insert(
                key.to_string(),
                CacheEntry {
                    value: Arc::new(value.clone()),
                    expires_at: Instant::now() + self.ttl,
                },
            );
        }

        Ok(value)
    }
}
