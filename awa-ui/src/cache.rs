use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock};

struct CacheEntry {
    value: Arc<dyn Any + Send + Sync>,
    expires_at: Instant,
}

/// A simple in-memory cache with a fixed TTL and request coalescing.
///
/// Each key stores a type-erased value that is downcast on retrieval.
/// Concurrent misses for the same key are coalesced — only one fetch
/// runs while others wait for it to complete, then read from the cache.
pub struct ResponseCache {
    ttl: Duration,
    entries: RwLock<HashMap<String, CacheEntry>>,
    /// Tracks in-flight fetches so concurrent misses coalesce.
    in_flight: Mutex<HashMap<String, Arc<tokio::sync::Notify>>>,
}

impl ResponseCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: RwLock::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Return a cached value if fresh, otherwise call `fetch` and cache the
    /// result. Concurrent callers for the same key wait for one fetch rather
    /// than all hitting the database.
    ///
    /// `T` must match the type previously stored under `key`.
    pub async fn get_or_fetch<T, E, F, Fut>(&self, key: &str, fetch: F) -> Result<T, E>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        // Fast path — read lock only.
        if let Some(value) = self.try_get::<T>(key) {
            return Ok(value);
        }

        // Check if another task is already fetching this key.
        {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(notify) = in_flight.get(key) {
                // Another task is fetching — wait for it.
                let notify = Arc::clone(notify);
                drop(in_flight);
                notify.notified().await;
                // The fetch should have populated the cache. If it failed
                // (rare), we fall through and do our own fetch below.
                if let Some(value) = self.try_get::<T>(key) {
                    return Ok(value);
                }
            } else {
                // We are the first — register ourselves as in-flight.
                let notify = Arc::new(tokio::sync::Notify::new());
                in_flight.insert(key.to_string(), Arc::clone(&notify));
            }
        }

        // Execute the fetch.
        let result = fetch().await;

        // Store on success.
        if let Ok(ref value) = result {
            let mut entries = self.entries.write().await;
            entries.insert(
                key.to_string(),
                CacheEntry {
                    value: Arc::new(value.clone()),
                    expires_at: Instant::now() + self.ttl,
                },
            );
        }

        // Clean up in-flight tracking and notify waiters.
        {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(notify) = in_flight.remove(key) {
                notify.notify_waiters();
            }
        }

        result
    }

    /// Try to read a fresh value from the cache without fetching.
    fn try_get<T: Clone + 'static>(&self, key: &str) -> Option<T> {
        let entries = self.entries.try_read().ok()?;
        let entry = entries.get(key)?;
        if entry.expires_at > Instant::now() {
            entry.value.downcast_ref::<T>().cloned()
        } else {
            None
        }
    }
}
