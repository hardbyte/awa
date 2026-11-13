//! Mount behavior must also work without a frontend build (the placeholder).
use std::time::Duration;

use awa_ui::state::ReadOnlyMode;
use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

async fn ui() -> Router {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@localhost/unused")
        .unwrap();
    // Explicit mode keeps these routing tests independent of a database.
    awa_ui::router_with(pool, Duration::from_secs(5), None, ReadOnlyMode::ReadOnly)
        .await
        .unwrap()
}

async fn request(app: &Router, path: &str) -> (StatusCode, axum::http::HeaderMap, String) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, headers, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn entry_points_infer_the_actual_mount() {
    let ui = ui().await;
    let apps = [
        (ui.clone(), "", "/"),
        (Router::new().nest("/awa", ui.clone()), "/awa", "/awa"),
        (
            Router::new().nest("/admin", Router::new().nest("/awa", ui.clone())),
            "/admin/awa",
            "/admin/awa",
        ),
        (
            Router::new().nest("/jobs", Router::new().nest("/jobs", ui.clone())),
            "/jobs/jobs",
            "/jobs/jobs",
        ),
        (
            Router::new().nest("/teams/{team}", Router::new().nest("/awa", ui.clone())),
            "/teams/ops/awa",
            "/teams/ops/awa",
        ),
        (Router::new().nest("/awa/", ui), "/awa", "/awa/"),
    ];
    for (app, mount, root) in apps {
        for path in [
            root.to_owned(),
            format!("{mount}/jobs"),
            format!("{mount}/jobs/123?tab=errors"),
            format!("{mount}/index.html"),
        ] {
            let (status, headers, html) = request(&app, &path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(headers[header::CACHE_CONTROL], "no-cache");
            assert!(
                html.contains(&format!(
                    "<head><base href=\"{mount}/\" data-awa-root=\"{root}\">"
                )),
                "incorrect base for {path}: {html}"
            );
        }
    }
}

#[tokio::test]
async fn nest_service_accepts_both_root_forms() {
    // Axum's Router::nest distinguishes /awa from /awa/; nest_service
    // explicitly registers both. The UI base remains the same for either.
    let app = Router::new().nest_service("/awa", ui().await);
    let (status, _, without_slash) = request(&app, "/awa").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, with_slash) = request(&app, "/awa/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(without_slash, with_slash);
    let (status, _, deep) = request(&app, "/awa/jobs/123").await;
    assert_eq!(status, StatusCode::OK);
    assert!(deep.contains("data-awa-root=\"/awa\""));
    let (status, _, json) = request(&app, "/awa/api/capabilities").await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.contains("\"read_only\":true"));
}

#[tokio::test]
async fn nested_api_and_assets_do_not_use_host_routes() {
    let ui = ui().await;
    let app = Router::new()
        .route("/api/capabilities", get(|| async { "host api" }))
        .route("/assets/{*path}", get(|| async { "host asset" }))
        .nest("/awa", ui.clone())
        .nest("/admin", Router::new().nest("/awa", ui));
    assert_eq!(request(&app, "/api/capabilities").await.2, "host api");
    assert_eq!(request(&app, "/assets/test.js").await.2, "host asset");

    for mount in ["/awa", "/admin/awa"] {
        let (status, headers, json) = request(&app, &format!("{mount}/api/capabilities")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        assert!(json.contains("\"read_only\":true"));

        // Build-independent routing coverage above always runs. When the
        // bundle exists, also verify the actual emitted asset is embedded.
        let (_, _, html) = request(&app, &format!("{mount}/jobs/123")).await;
        if let Some(asset) = html.split("src=\"./assets/").nth(1) {
            let name = asset.split('"').next().unwrap();
            let (status, headers, body) = request(&app, &format!("{mount}/assets/{name}")).await;
            assert_eq!(status, StatusCode::OK);
            assert!(headers[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .contains("javascript"));
            assert_ne!(body, "host asset");
            assert!(!body.starts_with("<!doctype html>"));
        }
    }
}
