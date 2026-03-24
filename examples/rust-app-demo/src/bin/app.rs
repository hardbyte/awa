use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use sqlx::PgPool;
use tracing_subscriber::EnvFilter;

#[path = "../shared.rs"]
mod shared;

use shared::{create_checkout, create_pool, list_recent_orders, prepare_schema, CheckoutRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let pool = create_pool().await?;
    prepare_schema(&pool).await?;

    let app = Router::new()
        .route("/health", get(health))
        .route("/orders", get(orders))
        .route("/orders/checkout", post(checkout))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000").await?;
    tracing::info!("Rust demo app listening on http://127.0.0.1:8000");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn orders(State(pool): State<PgPool>) -> Result<Json<Vec<shared::OrderSummary>>, String> {
    list_recent_orders(&pool)
        .await
        .map_err(|err| format!("failed to list orders: {err}"))
}

async fn checkout(
    State(pool): State<PgPool>,
    Json(request): Json<CheckoutRequest>,
) -> Result<Json<shared::CheckoutResponse>, String> {
    create_checkout(
        &pool,
        &request.customer_email,
        request.total_cents,
        request.checkout_id,
    )
    .await
    .map(Json)
    .map_err(|err| format!("failed to create checkout: {err}"))
}
