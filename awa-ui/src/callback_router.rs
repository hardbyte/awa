//! Callback-only receiver router.
//!
//! Builds an axum router that exposes only the three callback ingress routes
//! (`complete`, `fail`, `heartbeat`) — no admin REST, no static UI assets, no
//! permissive CORS. Pair with the CLI subcommand `awa callbacks serve` to
//! deploy callback ingress on its own host/port, separate from the admin UI.
//!
//! See ADR-027 for design rationale and the deployable-role split.
//!
//! ```no_run
//! # use awa_ui::callback_router::{callback_router, CallbackReceiverConfig, CallbackAuth};
//! # async fn build(pool: sqlx::PgPool) -> Result<axum::Router, Box<dyn std::error::Error>> {
//! let secret = [7u8; 32];
//! let router = callback_router(
//!     pool,
//!     CallbackReceiverConfig::new(CallbackAuth::Signed(secret)),
//! )
//! .await?;
//! # Ok(router)
//! # }
//! ```

use awa_model::callback_contract::DEFAULT_CALLBACK_PATH_PREFIX;
use axum::routing::post;
use axum::Router;
use sqlx::PgPool;

use crate::handlers;
use crate::state::AppState;

/// How an incoming callback request is authenticated.
#[derive(Debug, Clone)]
pub enum CallbackAuth {
    /// BLAKE3 keyed-hash signature verification. Every request must carry a
    /// valid `X-Awa-Signature` header. This is the only sensible mode for a
    /// publicly-reachable callback receiver.
    Signed([u8; 32]),
    /// No signature verification. Callers must protect the receiver some
    /// other way (private network, mTLS at the load balancer, IP allow-list,
    /// etc.). The name is intentionally awkward — choose `Signed` unless you
    /// have a clear story for the alternative.
    Unsigned,
}

/// Configuration for the callback-only receiver router.
#[derive(Debug, Clone)]
pub struct CallbackReceiverConfig {
    /// How inbound requests are authenticated.
    pub auth: CallbackAuth,
    /// Path prefix the three callback routes are mounted under. Defaults to
    /// [`DEFAULT_CALLBACK_PATH_PREFIX`] (`/api/callbacks`), matching the
    /// built-in `awa serve` layout so callback URLs produced by the worker
    /// continue to work unchanged. Override when the receiver lives at a
    /// different path (e.g. `/awa-cb`).
    pub path_prefix: String,
}

impl CallbackReceiverConfig {
    /// New config with the default `/api/callbacks` path prefix.
    pub fn new(auth: CallbackAuth) -> Self {
        Self {
            auth,
            path_prefix: DEFAULT_CALLBACK_PATH_PREFIX.to_string(),
        }
    }

    pub fn with_path_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.path_prefix = prefix.into();
        self
    }
}

/// Build a router exposing only the callback ingress routes.
///
/// The returned router:
/// - Mounts `POST {prefix}/{callback_id}/{complete,fail,heartbeat}`.
/// - Does NOT serve static UI assets.
/// - Does NOT expose admin REST routes.
/// - Does NOT apply permissive CORS.
/// - Requires writable database access — all three routes mutate job state.
///   The pool is probed once at build time and the call fails if the pool
///   reports `transaction_read_only = on`.
pub async fn callback_router(
    pool: PgPool,
    config: CallbackReceiverConfig,
) -> Result<Router, CallbackRouterBuildError> {
    let read_only = crate::state::detect_read_only(&pool).await?;
    if read_only {
        return Err(CallbackRouterBuildError::ReadOnlyDatabase);
    }

    let secret = match config.auth {
        CallbackAuth::Signed(secret) => Some(secret),
        CallbackAuth::Unsigned => None,
    };

    // Cache TTL is unused on the callback path — pass any value; the handler
    // does not touch the dashboard caches.
    let state = AppState::new(pool, false, std::time::Duration::ZERO, secret);

    let prefix = normalize_prefix(&config.path_prefix);
    let routes = Router::new()
        .route(
            &format!("{prefix}/{{callback_id}}/complete"),
            post(handlers::callbacks::complete_callback),
        )
        .route(
            &format!("{prefix}/{{callback_id}}/fail"),
            post(handlers::callbacks::fail_callback),
        )
        .route(
            &format!("{prefix}/{{callback_id}}/heartbeat"),
            post(handlers::callbacks::heartbeat_callback),
        )
        .with_state(state);

    Ok(routes)
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// Reasons `callback_router` can refuse to build.
#[derive(Debug, thiserror::Error)]
pub enum CallbackRouterBuildError {
    /// The database pool resolves to a read-only connection. All three
    /// callback routes mutate job state, so a read-only DB would surface as
    /// runtime 503s on every request. Fail at build time instead.
    #[error(
        "callback receiver requires a writable database (pool reports transaction_read_only=on)"
    )]
    ReadOnlyDatabase,
    /// Pool probe failure when reading `transaction_read_only`.
    #[error("failed to probe database read-only state: {0}")]
    Probe(#[from] sqlx::Error),
}
