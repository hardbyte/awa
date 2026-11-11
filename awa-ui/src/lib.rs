pub mod cache;
pub mod callback_router;
pub mod error;
mod frontend;
pub mod handlers;
pub mod pool;
pub mod state;

pub use callback_router::{
    callback_router, CallbackAuth, CallbackReceiverConfig, CallbackRouterBuildError,
};
pub use pool::{
    admin_connect_options, admin_pool_options, configure_admin_connection, recover_admin_connection,
};

use std::time::Duration;

use axum::routing::{any, delete, get, patch, post};
use axum::Router;
use sqlx::PgPool;
use tower_http::cors::CorsLayer;

use crate::state::{AppState, ReadOnlyMode};

/// Create the awa-ui router with all API routes and static file serving.
///
/// Read-only mode is auto-detected by probing the database connection — see
/// [`router_with`] to force it explicitly.
pub async fn router(pool: PgPool, cache_ttl: Duration) -> Result<Router, sqlx::Error> {
    router_with(pool, cache_ttl, None, ReadOnlyMode::Auto).await
}

/// Create the awa-ui router with optional callback signature verification.
///
/// Read-only mode is auto-detected. Prefer [`router_with`] when you need to
/// force read-only or writable behaviour regardless of DB privilege.
pub async fn router_with_callback_secret(
    pool: PgPool,
    cache_ttl: Duration,
    callback_hmac_secret: Option<[u8; 32]>,
) -> Result<Router, sqlx::Error> {
    router_with(pool, cache_ttl, callback_hmac_secret, ReadOnlyMode::Auto).await
}

/// Create the awa-ui router with full control over callback signing and
/// read-only behaviour.
///
/// `read_only_mode`:
/// - [`ReadOnlyMode::Auto`] — probe the DB (matches legacy behaviour)
/// - [`ReadOnlyMode::ReadOnly`] — force mutation endpoints off even if the
///   DB can write
/// - [`ReadOnlyMode::Writable`] — force mutation endpoints on; the DB still
///   has the final say at query time
///
/// The router can be mounted with [`Router::nest`] at any path. The UI's
/// assets, API requests and navigation automatically use that mount path.
pub async fn router_with(
    pool: PgPool,
    cache_ttl: Duration,
    callback_hmac_secret: Option<[u8; 32]>,
    read_only_mode: ReadOnlyMode,
) -> Result<Router, sqlx::Error> {
    let read_only = read_only_mode.resolve(&pool).await?;
    let state = AppState::new(pool, read_only, cache_ttl, callback_hmac_secret);
    let api = Router::new()
        // Jobs
        .route("/jobs", get(handlers::jobs::list_jobs))
        .route("/jobs/{id}", get(handlers::jobs::get_job))
        .route("/jobs/{id}/retry", post(handlers::jobs::retry_job))
        .route("/jobs/{id}/cancel", post(handlers::jobs::cancel_job))
        .route("/jobs/bulk-retry", post(handlers::jobs::bulk_retry))
        .route("/jobs/bulk-cancel", post(handlers::jobs::bulk_cancel))
        // Batch operations
        .route(
            "/batch-ops",
            get(handlers::batch_ops::list_batch_operations),
        )
        .route(
            "/batch-ops",
            post(handlers::batch_ops::submit_batch_operation),
        )
        .route(
            "/batch-ops/preview",
            post(handlers::batch_ops::preview_batch_operation),
        )
        .route(
            "/batch-ops/{id}",
            get(handlers::batch_ops::get_batch_operation),
        )
        .route(
            "/batch-ops/{id}",
            patch(handlers::batch_ops::patch_batch_operation),
        )
        // Queues
        .route("/queues", get(handlers::queues::list_queues))
        .route(
            "/queues/runtime",
            get(handlers::runtime::list_queue_runtime),
        )
        .route("/queues/{queue}", get(handlers::queues::get_queue))
        .route("/queues/{queue}/pause", post(handlers::queues::pause_queue))
        .route(
            "/queues/{queue}/resume",
            post(handlers::queues::resume_queue),
        )
        .route("/queues/{queue}/drain", post(handlers::queues::drain_queue))
        // Kinds
        .route("/kinds", get(handlers::kinds::list_kinds))
        // Cron
        .route("/cron", get(handlers::cron::list_cron_jobs))
        .route(
            "/cron/{name}/trigger",
            post(handlers::cron::trigger_cron_job),
        )
        .route("/cron/{name}/pause", post(handlers::cron::pause_cron_job))
        .route("/cron/{name}/resume", post(handlers::cron::resume_cron_job))
        // Stats
        .route("/stats", get(handlers::stats::get_stats))
        .route("/stats/timeseries", get(handlers::stats::get_timeseries))
        .route("/stats/kinds", get(handlers::stats::get_distinct_kinds))
        .route("/stats/queues", get(handlers::stats::get_distinct_queues))
        .route("/capabilities", get(handlers::stats::get_capabilities))
        // DLQ
        .route("/dlq", get(handlers::dlq::list_dlq))
        .route("/dlq/depth", get(handlers::dlq::dlq_depth))
        .route("/dlq/{id}", get(handlers::dlq::get_dlq_job))
        .route("/dlq/{id}", delete(handlers::dlq::purge_dlq_job))
        .route("/dlq/{id}/retry", post(handlers::dlq::retry_dlq_job))
        .route("/dlq/bulk-retry", post(handlers::dlq::bulk_retry_dlq))
        .route("/dlq/bulk-purge", post(handlers::dlq::bulk_purge_dlq))
        .route("/dlq/bulk-move", post(handlers::dlq::bulk_move_failed))
        // Runtime
        .route("/runtime", get(handlers::runtime::get_runtime))
        .route("/storage", get(handlers::runtime::get_storage))
        // Callbacks (for HTTP workers and external systems)
        .route(
            "/callbacks/{callback_id}/complete",
            post(handlers::callbacks::complete_callback),
        )
        .route(
            "/callbacks/{callback_id}/fail",
            post(handlers::callbacks::fail_callback),
        )
        .route(
            "/callbacks/{callback_id}/heartbeat",
            post(handlers::callbacks::heartbeat_callback),
        );

    Ok(Router::new()
        .nest("/api", api)
        // Explicit SPA routes survive multiple outer Router::nest calls;
        // axum does not propagate an inherited fallback through every nest.
        .route("/", any(frontend::static_handler))
        .route("/{*path}", any(frontend::static_handler))
        .fallback(frontend::static_handler)
        .layer(CorsLayer::permissive())
        .with_state(state))
}
