use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
/// Each aggregate is generation-keyed to fence obsolete in-flight fills.
#[derive(Clone)]
pub struct DashboardCache {
    pub stats: FencedCache<HashMap<JobState, i64>>,
    pub queues: FencedCache<Vec<admin::QueueOverview>>,
    pub runtime: FencedCache<admin::RuntimeOverview>,
    pub queue_runtime: FencedCache<Vec<admin::QueueRuntimeSummary>>,
    pub storage: FencedCache<awa_model::storage::StorageStatusReport>,
}

/// A fill is keyed by the generation it observed before starting. Old fills
/// can finish, but can neither satisfy a new-generation request nor be returned
/// as current. Moka targets two retained generation entries; a shared fill
/// budget independently bounds obsolete work.
#[derive(Clone)]
pub struct FencedCache<V: Clone + Send + Sync + 'static> {
    values: Cache<u64, Observed<V>>,
    generation: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct Observed<V> {
    pub data: V,
    pub observed_at: DateTime<Utc>,
    observed_since: Instant,
}

impl<V> Observed<V> {
    pub fn age_ms(&self) -> u128 {
        self.observed_since.elapsed().as_millis()
    }
}

impl<V: serde::Serialize> IntoResponse for Observed<V> {
    fn into_response(self) -> Response {
        let headers = [
            ("x-awa-observed-at", self.observed_at.to_rfc3339()),
            ("x-awa-served-at", Utc::now().to_rfc3339()),
            ("x-awa-cache-age-ms", self.age_ms().to_string()),
            ("cache-control", "private, no-store".to_string()),
        ];
        (headers, axum::Json(self.data)).into_response()
    }
}

/// Bounds distinct aggregate fills (including obsolete generations), not the
/// separately owned whole-request/SQL deadline layer.
struct FillBudget {
    active: Arc<Semaphore>,
    waiters: Semaphore,
}
impl FillBudget {
    fn shared() -> &'static Self {
        static BUDGET: OnceLock<FillBudget> = OnceLock::new();
        BUDGET.get_or_init(|| Self {
            active: Arc::new(Semaphore::new(4)),
            waiters: Semaphore::new(16),
        })
    }
    async fn admit(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<OwnedSemaphorePermit, CacheError> {
        if let Ok(permit) = self.active.clone().try_acquire_owned() {
            return Ok(permit);
        }
        let _waiter = self.waiters.try_acquire().map_err(|_| CacheError::Busy)?;
        tokio::time::timeout_at(deadline, self.active.clone().acquire_owned())
            .await
            .map_err(|_| CacheError::Busy)?
            .map_err(|_| CacheError::Busy)
    }
}

impl<V: Clone + Send + Sync + 'static> FencedCache<V> {
    fn new(ttl: Duration) -> Self {
        Self {
            values: Cache::builder().time_to_live(ttl).max_capacity(2).build(),
            generation: Arc::new(AtomicU64::new(0)),
        }
    }
    pub fn invalidate_all(&self) {
        // Saturation is fail-closed: the practically unreachable terminal
        // generation refuses all fills, rather than wrapping to an old key.
        let _ = self
            .generation
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |g| g.checked_add(1));
        self.values.invalidate_all();
    }
    pub async fn get<F, Fut>(&self, fetch: F) -> Result<Observed<V>, Arc<CacheError>>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: Future<Output = Result<V, AwaError>> + Send,
    {
        let admission_deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        for _ in 0..2 {
            let generation = self.generation.load(Ordering::SeqCst);
            if generation == u64::MAX {
                break;
            }
            let observed = self
                .values
                .try_get_with(generation, async {
                    let _permit = FillBudget::shared().admit(admission_deadline).await?;
                    if self.generation.load(Ordering::SeqCst) != generation {
                        return Err(CacheError::Changed);
                    }
                    let observed_at = Utc::now();
                    let observed_since = Instant::now();
                    fetch()
                        .await
                        .map(|data| Observed {
                            data,
                            observed_at,
                            observed_since,
                        })
                        .map_err(CacheError::from)
                })
                .await;
            match observed {
                Ok(sample) if self.generation.load(Ordering::SeqCst) == generation => {
                    return Ok(sample)
                }
                Ok(_) => {}
                Err(error) if matches!(error.as_ref(), CacheError::Changed) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Arc::new(CacheError::Changed))
    }
}

fn build_cache<V: Clone + Send + Sync + 'static>(ttl: Duration) -> FencedCache<V> {
    FencedCache::new(ttl)
}

impl DashboardCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            stats: build_cache(ttl),
            queues: build_cache(ttl),
            runtime: build_cache(ttl),
            queue_runtime: build_cache(ttl),
            storage: build_cache(ttl),
        }
    }
}

/// Shared error wrapper so moka's `try_get_with` can propagate errors.
///
/// `AwaError` is not `Clone`, but moka requires the error from
/// `try_get_with` to be `Clone + Send + Sync`. We wrap it in an `Arc`.
#[derive(Debug, Clone)]
pub enum CacheError {
    Database(Arc<AwaError>),
    Busy,
    Changed,
}

impl From<AwaError> for CacheError {
    fn from(err: AwaError) -> Self {
        Self::Database(Arc::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn an_old_fill_cannot_repopulate_or_satisfy_the_new_generation() {
        let cache = FencedCache::new(Duration::from_secs(60));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let old_cache = cache.clone();
        let calls = Arc::new(AtomicU64::new(0));
        let old_calls = calls.clone();
        let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let old = tokio::spawn(async move {
            old_cache
                .get(|| {
                    let release = release.clone();
                    let started = started.clone();
                    let calls = old_calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        if let Some(sender) = started.lock().unwrap().take() {
                            let _ = sender.send(());
                        }
                        if let Some(receiver) = release.lock().await.take() {
                            let _ = receiver.await;
                        }
                        Ok(1)
                    }
                })
                .await
                .unwrap()
        });
        started_rx.await.unwrap();
        cache.invalidate_all();
        // Does not join the old blocked fill.
        let current = cache.get(|| async { Ok(2) }).await.unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(old.await.unwrap().data, 2);
        assert_eq!(current.data, 2);
        let hit = cache
            .get(|| async { panic!("must be a cache hit") })
            .await
            .unwrap();
        assert_eq!(hit.data, 2);
        assert_eq!(hit.observed_at, current.observed_at);
        assert_eq!(hit.observed_since, current.observed_since);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn cache_age_does_not_become_fresh_when_wall_time_moves_backwards() {
        let sample = Observed {
            data: (),
            observed_at: Utc::now() + chrono::Duration::hours(1),
            observed_since: Instant::now() - Duration::from_secs(2),
        };
        assert!(sample.age_ms() >= 2000);
    }
    #[tokio::test]
    async fn admission_bounds_waiters_and_cancellation_releases_capacity() {
        let budget = FillBudget {
            active: Arc::new(Semaphore::new(4)),
            waiters: Semaphore::new(16),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut permits = Vec::new();
        for _ in 0..4 {
            permits.push(budget.admit(deadline).await.unwrap());
        }
        let mut waiting = (0..16)
            .map(|_| Box::pin(budget.admit(deadline)))
            .collect::<Vec<_>>();
        std::future::poll_fn(|cx| {
            for future in &mut waiting {
                assert!(future.as_mut().poll(cx).is_pending());
            }
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(budget.waiters.available_permits(), 0);
        assert!(matches!(
            budget.admit(deadline).await,
            Err(CacheError::Busy)
        ));
        drop(waiting);
        assert_eq!(budget.waiters.available_permits(), 16);
        assert!(matches!(
            budget.admit(tokio::time::Instant::now()).await,
            Err(CacheError::Busy)
        ));
        assert_eq!(budget.waiters.available_permits(), 16);
        drop(permits);
        assert_eq!(budget.active.available_permits(), 4);
        assert!(budget.admit(deadline).await.is_ok());
    }

    #[tokio::test]
    async fn continuous_invalidations_have_a_finite_retry_bound() {
        let cache = FencedCache::new(Duration::from_secs(60));
        let calls = AtomicU64::new(0);
        let result = cache
            .get(|| async {
                calls.fetch_add(1, Ordering::SeqCst);
                cache.invalidate_all();
                Ok(1)
            })
            .await;
        assert!(matches!(result, Err(error) if matches!(error.as_ref(), CacheError::Changed)));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn exhausted_generation_fails_closed() {
        let cache = FencedCache::<u8>::new(Duration::from_secs(60));
        cache.generation.store(u64::MAX, Ordering::SeqCst);
        cache.invalidate_all();
        let result = cache
            .get(|| async { panic!("terminal generation cannot fetch") })
            .await;
        assert!(matches!(result, Err(error) if matches!(error.as_ref(), CacheError::Changed)));
    }
}
