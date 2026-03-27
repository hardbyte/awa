use awa::{insert_with, InsertOpts, JobState};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[path = "../shared.rs"]
mod shared;

use shared::{
    build_demo_client, clear_demo_data, create_checkout, create_pool, hero_scheduled_time,
    prepare_schema, seed_available_cache_jobs, seed_failed_syncs, seed_pending_payments,
    seed_scale, seed_scheduled_reports, wait_for_cron_sync, wait_for_many, CACHE_QUEUE, CRON_NAME,
    EMAIL_QUEUE, GenerateRevenueReport, OPS_QUEUE, PAYMENTS_QUEUE, REPORTS_QUEUE,
    WarmProductCache,
};

fn parse_scale() -> &'static str {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--scale" {
            return match args.next().as_deref() {
                Some("small") => "small",
                Some("large") => "large",
                Some("video") => "video",
                _ => "medium",
            };
        }
    }
    "medium"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let scale_name = parse_scale();
    let preset = seed_scale(scale_name);

    let pool = create_pool().await?;
    prepare_schema(&pool).await?;
    clear_demo_data(&pool).await?;

    let callback_ids = Arc::new(Mutex::new(Vec::<Uuid>::new()));
    let client = build_demo_client(pool.clone(), Some(callback_ids.clone()))?;

    let mut receipt_job_ids = Vec::with_capacity(preset.completed_orders);
    for i in 0..preset.completed_orders {
        let checkout = create_checkout(
            &pool,
            &format!("customer{}@example.com", i + 1),
            (1499 + (i * 200)) as i32,
            Some(format!("ord_demo_{:03}", i + 1)),
        )
        .await?;
        if let Some(job_id) = checkout.confirmation_job_id {
            receipt_job_ids.push(job_id);
        }
    }

    let failed_ids = seed_failed_syncs(&pool, preset.failed_syncs).await?;
    let waiting_ids = seed_pending_payments(&pool, preset.waiting_payments).await?;
    seed_available_cache_jobs(&pool, preset.available_cache_jobs).await?;
    seed_scheduled_reports(&pool, preset.scheduled_reports).await?;

    let hero_available = insert_with(
        &pool,
        &WarmProductCache { slug: "/".into() },
        InsertOpts {
            queue: CACHE_QUEUE.to_string(),
            tags: vec!["demo".into(), "hero".into(), "cache".into()],
            metadata: json!({"app": "rust-demo-shop", "hero": true}),
            ..Default::default()
        },
    )
    .await?;

    let hero_scheduled = insert_with(
        &pool,
        &GenerateRevenueReport {
            report_name: "weekly-gross-margin".into(),
        },
        InsertOpts {
            queue: REPORTS_QUEUE.to_string(),
            run_at: Some(hero_scheduled_time()),
            tags: vec!["demo".into(), "hero".into(), "reports".into()],
            metadata: json!({"app": "rust-demo-shop", "hero": true}),
            ..Default::default()
        },
    )
    .await?;

    // For "video" scale, don't start workers or wait for state transitions.
    // Jobs stay in their initial states (available, scheduled) so the user
    // can start workers separately and watch progress live in the UI.
    let callback_id = if scale_name == "video" {
        // Still need to briefly start the client to sync the cron schedule,
        // but don't process any jobs.
        client.start().await?;
        wait_for_cron_sync(&pool, Duration::from_secs(10)).await?;
        client.shutdown(Duration::from_secs(2)).await;
        None
    } else {
        client.start().await?;

        wait_for_many(
            &pool,
            &receipt_job_ids,
            JobState::Completed,
            Duration::from_secs(12),
        )
        .await?;
        wait_for_many(
            &pool,
            &failed_ids,
            JobState::Failed,
            Duration::from_secs(60),
        )
        .await?;
        wait_for_many(
            &pool,
            &waiting_ids,
            JobState::WaitingExternal,
            Duration::from_secs(30),
        )
        .await?;
        wait_for_cron_sync(&pool, Duration::from_secs(10)).await?;

        client.shutdown(Duration::from_secs(5)).await;

        callback_ids
            .lock()
            .expect("callback id lock")
            .first()
            .copied()
    };

    println!("\nRust demo data ready");
    println!("====================");
    println!("scale preset:       {scale_name}");
    println!(
        "queues:             {EMAIL_QUEUE}, {PAYMENTS_QUEUE}, {OPS_QUEUE}, {CACHE_QUEUE}, {REPORTS_QUEUE}"
    );
    println!(
        "counts:             completed={} failed={} waiting={} available={} scheduled={}",
        receipt_job_ids.len(),
        failed_ids.len(),
        waiting_ids.len(),
        preset.available_cache_jobs + 1,
        preset.scheduled_reports + 1
    );
    println!(
        "hero available:     id={} state={:?}",
        hero_available.id, hero_available.state
    );
    println!(
        "hero scheduled:     id={} state={:?}",
        hero_scheduled.id, hero_scheduled.state
    );
    println!(
        "callback id:        {}",
        callback_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "missing".into())
    );
    println!("cron job:           {CRON_NAME}");
    println!("\nSuggested next steps:");
    println!("  cargo run -p awa-cli -- --database-url $DATABASE_URL serve");
    println!("  cargo run --bin app");
    println!("  cargo run --bin workers");

    Ok(())
}
