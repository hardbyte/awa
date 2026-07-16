use std::time::Duration;

use awa_metrics::AwaMetrics;
use serde::Serialize;
use sqlx::PgPool;

use crate::cache::DashboardCache;
use crate::error::ApiError;

/// Operator-assigned identity for this UI instance (#437).
///
/// The UI is single-backend by design — one `awa serve` per database. This
/// identity is what lets an operator with several instances open tell the
/// tabs (and mutation targets) apart: it is exposed via `/api/capabilities`
/// and rendered in the header badge, browser tab title, and favicon tint.
#[derive(Clone, Debug, Default)]
pub struct InstanceIdentity {
    /// Human-readable name, e.g. "cloudsql-prod".
    pub name: Option<String>,
    /// Accent color as a CSS hex value, e.g. "#0ea5e9". Validated by the
    /// CLI before it gets here.
    pub color: Option<String>,
    /// Static links to peer Awa UIs — a zero-data-plane "switcher".
    pub peers: Vec<PeerLink>,
}

/// A named link to a peer Awa UI serving a different database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PeerLink {
    pub name: String,
    pub url: String,
}

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub read_only: bool,
    pub cache: DashboardCache,
    pub callback_hmac_secret: Option<[u8; 32]>,
    /// Suggested frontend poll interval — at least as long as the cache TTL
    /// so clients don't poll faster than the cache can refresh.
    pub poll_interval_ms: u64,
    /// Cached `AwaMetrics` handle. `AwaMetrics::from_global()` rebuilds the
    /// full instrument set on every call, so we construct it once here and
    /// share it across request handlers.
    pub metrics: AwaMetrics,
    /// Identity shown by the frontend; `Arc` keeps per-request state clones
    /// cheap.
    pub instance: std::sync::Arc<InstanceIdentity>,
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
            metrics: AwaMetrics::from_global(),
            instance: std::sync::Arc::new(InstanceIdentity::default()),
        }
    }

    /// Attach an operator-assigned instance identity (#437).
    pub fn with_instance(mut self, instance: InstanceIdentity) -> Self {
        self.instance = std::sync::Arc::new(instance);
        self
    }

    pub fn require_writable(&self) -> Result<(), ApiError> {
        if self.read_only {
            return Err(ApiError::read_only());
        }
        Ok(())
    }

    /// UI mutations change multiple dashboard surfaces, and the server caches
    /// those aggregate endpoints independently. Invalidate them as a unit so
    /// follow-up reads do not serve stale queue/job state after a mutation.
    pub fn invalidate_dashboard_caches(&self) {
        self.cache.stats.invalidate_all();
        self.cache.queues.invalidate_all();
        self.cache.runtime.invalidate_all();
        self.cache.queue_runtime.invalidate_all();
        self.cache.storage.invalidate_all();
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
    /// Operator-assigned name identifying this instance/database (#437).
    pub instance_name: Option<String>,
    /// Accent color (CSS hex) tinting the header badge and favicon.
    pub instance_color: Option<String>,
    /// Static links to peer Awa UIs, rendered as plain links.
    pub peers: Vec<PeerLink>,
}
