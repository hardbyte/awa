#![allow(dead_code)]

use awa::{
    admin, insert_with, migrations, Client, InsertOpts, JobArgs, JobContext, JobError,
    JobResult, JobState, PeriodicJob, QueueConfig, Worker,
};
use axum::Json;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const DATABASE_URL_FALLBACK: &str = "postgres://postgres:test@localhost:15432/awa_test";
pub const ORDERS_TABLE: &str = "demo_rust_orders";

pub const EMAIL_QUEUE: &str = "rust_store_email";
pub const PAYMENTS_QUEUE: &str = "rust_store_payments";
pub const OPS_QUEUE: &str = "rust_store_ops";
pub const CACHE_QUEUE: &str = "rust_store_cache";
pub const REPORTS_QUEUE: &str = "rust_store_reports";
pub const CRON_NAME: &str = "rust_store_daily_revenue_digest";

#[derive(Clone, Copy)]
pub struct SeedScale {
    pub completed_orders: usize,
    pub failed_syncs: usize,
    pub waiting_payments: usize,
    pub available_cache_jobs: usize,
    pub scheduled_reports: usize,
}

pub fn seed_scale(name: &str) -> SeedScale {
    match name {
        "small" => SeedScale {
            completed_orders: 4,
            failed_syncs: 2,
            waiting_payments: 2,
            available_cache_jobs: 24,
            scheduled_reports: 12,
        },
        "large" => SeedScale {
            completed_orders: 28,
            failed_syncs: 8,
            waiting_payments: 6,
            available_cache_jobs: 1200,
            scheduled_reports: 600,
        },
        // Lean seed for video recording: just enough to show each state,
        // few enough that the interesting jobs aren't buried.
        "video" => SeedScale {
            completed_orders: 3,
            failed_syncs: 1,
            waiting_payments: 1,
            available_cache_jobs: 3,
            scheduled_reports: 2,
        },
        _ => SeedScale {
            completed_orders: 14,
            failed_syncs: 4,
            waiting_payments: 3,
            available_cache_jobs: 180,
            scheduled_reports: 90,
        },
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, awa_model::JobArgs)]
pub struct SendOrderConfirmationEmail {
    pub order_id: String,
    pub customer_email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, awa_model::JobArgs)]
pub struct CapturePayment {
    pub order_id: String,
    pub payment_ref: String,
    pub total_cents: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, awa_model::JobArgs)]
pub struct SyncInventoryBatch {
    pub supplier: String,
    pub total_items: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, awa_model::JobArgs)]
pub struct WarmProductCache {
    pub slug: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, awa_model::JobArgs)]
pub struct GenerateRevenueReport {
    pub report_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CheckoutRequest {
    pub customer_email: String,
    pub total_cents: i32,
    pub checkout_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CheckoutResponse {
    pub order_id: String,
    pub confirmation_job_id: Option<i64>,
    pub duplicate: bool,
}

#[derive(Debug, Serialize)]
pub struct OrderSummary {
    pub order_id: String,
    pub customer_email: String,
    pub total_cents: i32,
    pub status: String,
    pub created_at: chrono::DateTime<Utc>,
}

struct SendEmailWorker;

struct InventorySyncWorker;

struct CapturePaymentWorker {
    callback_ids: Option<Arc<Mutex<Vec<Uuid>>>>,
}

struct CacheWarmWorker;

struct ReportWorker;

fn decode_args<T: for<'de> Deserialize<'de>>(ctx: &JobContext) -> Result<T, JobError> {
    serde_json::from_value(ctx.job.args.clone())
        .map_err(|err| JobError::terminal(err.to_string()))
}

// ── Email: completes in ~5s with progress ─────────────────────────

#[async_trait::async_trait]
impl Worker for SendEmailWorker {
    fn kind(&self) -> &'static str {
        SendOrderConfirmationEmail::kind()
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: SendOrderConfirmationEmail = decode_args(ctx)?;

        ctx.set_progress(10, "Rendering email template");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        ctx.set_progress(50, "Connecting to SMTP");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        ctx.set_progress(90, "Sending email");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        tokio::time::sleep(Duration::from_secs(1)).await;

        ctx.set_progress(100, "Delivered");
        ctx.update_metadata(json!({
            "to": args.customer_email,
            "order_id": args.order_id,
            "status": "delivered",
        }))
        .map_err(|err| JobError::terminal(err.to_string()))?;
        ctx.flush_progress().await.map_err(JobError::retryable)?;

        tracing::info!(
            order_id = %args.order_id,
            to = %args.customer_email,
            "Sent order confirmation"
        );
        Ok(JobResult::Completed)
    }
}

// ── Inventory sync: ~60s of batch processing, fails partway ───────

#[async_trait::async_trait]
impl Worker for InventorySyncWorker {
    fn kind(&self) -> &'static str {
        SyncInventoryBatch::kind()
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: SyncInventoryBatch = decode_args(ctx)?;
        // Cap at 20 items so the job takes ~40s max (2s per item)
        let total_items = (args.total_items as usize).clamp(5, 20);
        let fail_at = total_items * 4 / 10; // fail at ~40%

        for i in 0..total_items {
            let pct = ((i + 1) as f64 / total_items as f64 * 100.0) as u8;
            let sku = format!("SKU-{:04}", i + 1);

            ctx.set_progress(pct.min(100), &format!("Validating {sku} ({}/{total_items})", i + 1));
            ctx.update_metadata(json!({
                "supplier": args.supplier,
                "last_sku": sku,
                "items_processed": i + 1,
                "items_total": total_items,
            }))
            .map_err(|err| JobError::terminal(err.to_string()))?;
            ctx.flush_progress().await.map_err(JobError::retryable)?;

            // ~2s per item → 20 items ≈ 40s, fails at ~16s (40% of 20 items)
            tokio::time::sleep(Duration::from_secs(2)).await;

            if i == fail_at {
                return Err(JobError::terminal(format!(
                    "supplier feed missing wholesale_price for {sku}"
                )));
            }
        }

        ctx.set_progress(100, "Sync complete");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        Ok(JobResult::Completed)
    }
}

// ── Payment capture: progress then callback park ──────────────────

#[async_trait::async_trait]
impl Worker for CapturePaymentWorker {
    fn kind(&self) -> &'static str {
        CapturePayment::kind()
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: CapturePayment = decode_args(ctx)?;

        ctx.set_progress(10, "Validating payment details");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        ctx.set_progress(30, "Creating charge with provider");
        ctx.update_metadata(json!({
            "order_id": args.order_id,
            "payment_ref": args.payment_ref,
            "provider": "stripe",
        }))
        .map_err(|err| JobError::terminal(err.to_string()))?;
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        tokio::time::sleep(Duration::from_secs(3)).await;

        ctx.set_progress(60, "Charge created, waiting for provider callback");
        ctx.flush_progress().await.map_err(JobError::retryable)?;

        let token = ctx
            .register_callback(Duration::from_secs(3600))
            .await
            .map_err(JobError::retryable)?;

        if let Some(ids) = &self.callback_ids {
            ids.lock().expect("callback ids lock").push(token.id());
        }

        Ok(JobResult::WaitForCallback(token))
    }
}

// ── Cache warm: ~30s of simulated work ────────────────────────────

#[async_trait::async_trait]
impl Worker for CacheWarmWorker {
    fn kind(&self) -> &'static str {
        WarmProductCache::kind()
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: WarmProductCache = decode_args(ctx)?;

        let steps = [
            (15, "Fetching product data"),
            (35, "Resolving variants and pricing"),
            (55, "Rendering templates"),
            (75, "Building search index entry"),
            (90, "Writing to cache"),
            (100, "Cache warm complete"),
        ];

        for (pct, msg) in &steps {
            ctx.set_progress(*pct, msg);
            ctx.update_metadata(json!({
                "slug": args.slug,
                "step": msg,
            }))
            .map_err(|err| JobError::terminal(err.to_string()))?;
            ctx.flush_progress().await.map_err(JobError::retryable)?;
            // ~5s per step → ~30s total
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        tracing::info!(slug = %args.slug, "Cache warmed");
        Ok(JobResult::Completed)
    }
}

// ── Revenue report: ~45s of simulated generation ──────────────────

#[async_trait::async_trait]
impl Worker for ReportWorker {
    fn kind(&self) -> &'static str {
        GenerateRevenueReport::kind()
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: GenerateRevenueReport = decode_args(ctx)?;

        let steps = [
            (10, "Querying order data"),
            (25, "Aggregating revenue by region"),
            (45, "Computing margins"),
            (60, "Building charts"),
            (75, "Formatting PDF"),
            (85, "Uploading to storage"),
            (95, "Sending notification"),
            (100, "Report complete"),
        ];

        for (pct, msg) in &steps {
            ctx.set_progress(*pct, msg);
            ctx.update_metadata(json!({
                "report_name": args.report_name,
                "step": msg,
            }))
            .map_err(|err| JobError::terminal(err.to_string()))?;
            ctx.flush_progress().await.map_err(JobError::retryable)?;
            // ~5-6s per step → ~45s total
            tokio::time::sleep(Duration::from_millis(5500)).await;
        }

        tracing::info!(report = %args.report_name, "Report generated");
        Ok(JobResult::Completed)
    }
}

pub fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DATABASE_URL_FALLBACK.to_string())
}

pub async fn create_pool() -> Result<PgPool, Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url())
        .await?;
    Ok(pool)
}

pub async fn prepare_schema(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    migrations::run(pool).await?;
    ensure_app_schema(pool).await?;
    Ok(())
}

pub async fn ensure_app_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {ORDERS_TABLE} (
            order_id TEXT PRIMARY KEY,
            customer_email TEXT NOT NULL,
            total_cents INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#
    ))
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn clear_demo_data(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("DELETE FROM {ORDERS_TABLE}"))
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM awa.jobs WHERE queue LIKE 'rust_store_%'")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(CRON_NAME)
        .execute(pool)
        .await?;
    Ok(())
}

pub fn build_demo_client(
    pool: PgPool,
    callback_ids: Option<Arc<Mutex<Vec<Uuid>>>>,
) -> Result<Client, Box<dyn std::error::Error>> {
    let callback_ids_for_payments = callback_ids.clone();

    let client = Client::builder(pool)
        .queue(
            EMAIL_QUEUE,
            QueueConfig {
                max_workers: 2,
                ..Default::default()
            },
        )
        .queue(
            PAYMENTS_QUEUE,
            QueueConfig {
                max_workers: 1,
                ..Default::default()
            },
        )
        .queue(
            OPS_QUEUE,
            QueueConfig {
                max_workers: 1,
                ..Default::default()
            },
        )
        .queue(
            CACHE_QUEUE,
            QueueConfig {
                max_workers: 2,
                ..Default::default()
            },
        )
        .queue(
            REPORTS_QUEUE,
            QueueConfig {
                max_workers: 1,
                ..Default::default()
            },
        )
        .register_worker(SendEmailWorker)
        .register_worker(InventorySyncWorker)
        .register_worker(CapturePaymentWorker {
            callback_ids: callback_ids_for_payments,
        })
        .register_worker(CacheWarmWorker)
        .register_worker(ReportWorker)
        .periodic(
            PeriodicJob::builder(CRON_NAME, "0 9 * * *")
                .queue(REPORTS_QUEUE)
                .timezone("Pacific/Auckland")
                .build(&GenerateRevenueReport {
                    report_name: "daily-revenue-digest".into(),
                })?,
        )
        .leader_election_interval(Duration::from_millis(1000))
        .heartbeat_interval(Duration::from_millis(1000))
        .promote_interval(Duration::from_millis(1000))
        .build()?;

    Ok(client)
}

pub async fn create_checkout(
    pool: &PgPool,
    customer_email: &str,
    total_cents: i32,
    order_id: Option<String>,
) -> Result<CheckoutResponse, Box<dyn std::error::Error>> {
    let resolved_order_id = order_id.unwrap_or_else(|| format!("ord_{}", Uuid::new_v4().simple()));

    let mut tx = pool.begin().await?;
    let inserted = sqlx::query(&format!(
        r#"
        INSERT INTO {ORDERS_TABLE} (order_id, customer_email, total_cents, status)
        VALUES ($1, $2, $3, 'submitted')
        ON CONFLICT (order_id) DO NOTHING
        RETURNING order_id
        "#
    ))
    .bind(&resolved_order_id)
    .bind(customer_email)
    .bind(total_cents)
    .fetch_optional(&mut *tx)
    .await?;

    if inserted.is_none() {
        tx.commit().await?;
        return Ok(CheckoutResponse {
            order_id: resolved_order_id,
            confirmation_job_id: None,
            duplicate: true,
        });
    }

    let confirmation_job = insert_with(
        &mut *tx,
        &SendOrderConfirmationEmail {
            order_id: resolved_order_id.clone(),
            customer_email: customer_email.to_string(),
        },
        InsertOpts {
            queue: EMAIL_QUEUE.to_string(),
            tags: vec!["demo".into(), "order-confirmation".into()],
            metadata: json!({"app": "rust-demo-shop", "order_id": resolved_order_id}),
            ..Default::default()
        },
    )
    .await?;

    tx.commit().await?;

    Ok(CheckoutResponse {
        order_id: resolved_order_id,
        confirmation_job_id: Some(confirmation_job.id),
        duplicate: false,
    })
}

pub async fn list_recent_orders(pool: &PgPool) -> Result<Json<Vec<OrderSummary>>, sqlx::Error> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT order_id, customer_email, total_cents, status, created_at
        FROM {ORDERS_TABLE}
        ORDER BY created_at DESC
        LIMIT 20
        "#
    ))
    .fetch_all(pool)
    .await?;

    let orders = rows
        .into_iter()
        .map(|row| OrderSummary {
            order_id: row.get("order_id"),
            customer_email: row.get("customer_email"),
            total_cents: row.get("total_cents"),
            status: row.get("status"),
            created_at: row.get("created_at"),
        })
        .collect();

    Ok(Json(orders))
}

pub async fn seed_failed_syncs(
    pool: &PgPool,
    count: usize,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let job = insert_with(
            pool,
            &SyncInventoryBatch {
                supplier: format!("supplier-{}", i + 1),
                total_items: (100 + (i * 20)) as i32,
            },
            InsertOpts {
                queue: OPS_QUEUE.to_string(),
                tags: vec!["demo".into(), "inventory".into()],
                metadata: json!({"app": "rust-demo-shop", "team": "ops"}),
                ..Default::default()
            },
        )
        .await?;
        ids.push(job.id);
    }
    Ok(ids)
}

pub async fn seed_pending_payments(
    pool: &PgPool,
    count: usize,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let job = insert_with(
            pool,
            &CapturePayment {
                order_id: format!("pay_{:03}", i + 1),
                payment_ref: format!("pi_demo_{:03}", i + 1),
                total_cents: (1999 + (i * 250)) as i32,
            },
            InsertOpts {
                queue: PAYMENTS_QUEUE.to_string(),
                tags: vec!["demo".into(), "payments".into()],
                metadata: json!({"app": "rust-demo-shop"}),
                ..Default::default()
            },
        )
        .await?;
        ids.push(job.id);
    }
    Ok(ids)
}

pub async fn seed_available_cache_jobs(
    pool: &PgPool,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..count {
        insert_with(
            pool,
            &WarmProductCache {
                slug: format!("/products/demo-{}", i + 1),
            },
            InsertOpts {
                queue: CACHE_QUEUE.to_string(),
                tags: vec!["demo".into(), "cache".into()],
                metadata: json!({"app": "rust-demo-shop"}),
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

pub async fn seed_scheduled_reports(
    pool: &PgPool,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..count {
        insert_with(
            pool,
            &GenerateRevenueReport {
                report_name: format!("scheduled-revenue-report-{}", i + 1),
            },
            InsertOpts {
                queue: REPORTS_QUEUE.to_string(),
                run_at: Some(Utc::now() + chrono::Duration::days(7 + i as i64)),
                tags: vec!["demo".into(), "reports".into()],
                metadata: json!({"app": "rust-demo-shop"}),
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

pub async fn wait_for_state(
    pool: &PgPool,
    job_id: i64,
    expected_state: JobState,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let job = admin::get_job(pool, job_id).await?;
        if job.state == expected_state {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "job {job_id} never reached {expected_state:?}; last state was {:?}",
                job.state
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_for_many(
    pool: &PgPool,
    job_ids: &[i64],
    expected_state: JobState,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    for &job_id in job_ids {
        wait_for_state(pool, job_id, expected_state, timeout).await?;
    }
    Ok(())
}

pub async fn wait_for_cron_sync(
    pool: &PgPool,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let row = sqlx::query("SELECT 1 FROM awa.cron_jobs WHERE name = $1")
            .bind(CRON_NAME)
            .fetch_optional(pool)
            .await?;
        if row.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("cron job {CRON_NAME} was not synced").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn hero_scheduled_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0)
        .single()
        .expect("valid future timestamp")
}
