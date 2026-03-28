pub mod cache;
pub mod error;
pub mod handlers;
pub mod state;

use std::time::Duration;

use axum::http::header;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use rust_embed::Embed;
use sqlx::PgPool;
use tower_http::cors::CorsLayer;

use crate::state::{detect_read_only, AppState};

#[derive(Embed)]
#[folder = "static/"]
struct StaticAssets;

/// Create the awa-ui router with all API routes and static file serving.
pub async fn router(pool: PgPool, cache_ttl: Duration) -> Result<Router, sqlx::Error> {
    let read_only = detect_read_only(&pool).await?;
    let state = AppState::new(pool, read_only, cache_ttl);
    let api = Router::new()
        // Jobs
        .route("/jobs", get(handlers::jobs::list_jobs))
        .route("/jobs/{id}", get(handlers::jobs::get_job))
        .route("/jobs/{id}/retry", post(handlers::jobs::retry_job))
        .route("/jobs/{id}/cancel", post(handlers::jobs::cancel_job))
        .route("/jobs/bulk-retry", post(handlers::jobs::bulk_retry))
        .route("/jobs/bulk-cancel", post(handlers::jobs::bulk_cancel))
        // Queues
        .route("/queues", get(handlers::queues::list_queues))
        .route(
            "/queues/runtime",
            get(handlers::runtime::list_queue_runtime),
        )
        .route("/queues/{queue}/pause", post(handlers::queues::pause_queue))
        .route(
            "/queues/{queue}/resume",
            post(handlers::queues::resume_queue),
        )
        .route("/queues/{queue}/drain", post(handlers::queues::drain_queue))
        // Cron
        .route("/cron", get(handlers::cron::list_cron_jobs))
        .route(
            "/cron/{name}/trigger",
            post(handlers::cron::trigger_cron_job),
        )
        // Stats
        .route("/stats", get(handlers::stats::get_stats))
        .route("/stats/timeseries", get(handlers::stats::get_timeseries))
        .route("/stats/kinds", get(handlers::stats::get_distinct_kinds))
        .route("/stats/queues", get(handlers::stats::get_distinct_queues))
        .route("/capabilities", get(handlers::stats::get_capabilities))
        // Runtime
        .route("/runtime", get(handlers::runtime::get_runtime));

    Ok(Router::new()
        .nest("/api", api)
        .fallback(static_handler)
        .layer(CorsLayer::permissive())
        .with_state(state))
}

/// Serve embedded static files, falling back to index.html for SPA routing.
async fn static_handler(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    // Try to serve the exact file
    if let Some(file) = StaticAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], file.data.to_vec()).into_response();
    }

    // SPA fallback: serve index.html for non-API routes
    if let Some(index) = StaticAssets::get("index.html") {
        return Html(index.data.to_vec()).into_response();
    }

    // No frontend built — serve placeholder
    Html(PLACEHOLDER_HTML).into_response()
}

const PLACEHOLDER_HTML: &str = r#"<!DOCTYPE html>
<html>
<head><title>AWA</title></head>
<body>
  <h1>AWA Job Queue</h1>
  <p>API available at <a href="/api/stats">/api/stats</a></p>
  <p>Frontend not built. Run <code>cd awa-ui/frontend && npm install && npm run build</code> to build the UI.</p>
</body>
</html>"#;
