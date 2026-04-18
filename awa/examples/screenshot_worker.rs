//! Runs a live demo workload against awa_screenshot_demo so the two Grafana
//! dashboards have real data while screenshots are being taken. Unlike a
//! one-shot version, this keeps producing and draining jobs until killed,
//! so the runtime stays "Healthy" and descriptor liveness stays fresh.
//!
//! Run via:
//!   DATABASE_URL=... OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
//!     cargo run --release --example screenshot_worker -p awa

use async_trait::async_trait;
use awa::{
    insert_with, migrations, Client, InsertOpts, JobArgs, JobContext, JobError, JobKindDescriptor,
    JobResult, QueueConfig, QueueDescriptor, Worker,
};
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
}
#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ResizeImage {
    url: String,
}
#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct RunAnalytics {
    report: String,
}

struct EmailWorker;
#[async_trait]
impl Worker for EmailWorker {
    fn kind(&self) -> &'static str {
        "send_email"
    }
    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        tokio::time::sleep(Duration::from_millis(40)).await;
        Ok(JobResult::Completed)
    }
}

struct ImageWorker;
#[async_trait]
impl Worker for ImageWorker {
    fn kind(&self) -> &'static str {
        "resize_image"
    }
    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        tokio::time::sleep(Duration::from_millis(120)).await;
        Ok(JobResult::Completed)
    }
}

struct AnalyticsWorker;
#[async_trait]
impl Worker for AnalyticsWorker {
    fn kind(&self) -> &'static str {
        "run_analytics"
    }
    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        tokio::time::sleep(Duration::from_millis(80)).await;
        Ok(JobResult::Completed)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_screenshot_demo".into());
    let otlp = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".into());

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp)
        .build()?;
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(1))
        .build();
    let resource = Resource::builder()
        .with_service_name("awa-screenshot-demo")
        .build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();
    global::set_meter_provider(meter_provider.clone());

    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect(&db_url)
        .await?;
    migrations::run(&pool).await?;

    // Pre-seed one retired descriptor so the Postgres dashboard's "Descriptor
    // Health" panel has a stale row to show while the live ones stay fresh.
    sqlx::query(
        "INSERT INTO awa.queue_descriptors \
         (queue, display_name, owner, tags, extra, descriptor_hash, sync_interval_ms, \
          created_at, updated_at, last_seen_at) \
         VALUES ('retired_payments', 'Retired payments queue', 'payments@example.com', \
                 ARRAY['legacy']::text[], '{}'::jsonb, 'stale_hash_demo', 10000, \
                 now() - interval '1 day', now() - interval '1 day', now() - interval '10 minutes') \
         ON CONFLICT (queue) DO UPDATE SET last_seen_at = now() - interval '10 minutes'",
    )
    .execute(&pool)
    .await?;

    let client = Client::builder(pool.clone())
        .queue(
            "email",
            QueueConfig {
                max_workers: 6,
                poll_interval: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .queue_descriptor(
            "email",
            QueueDescriptor::new()
                .display_name("Outbound email")
                .description("Transactional email delivery via SES.")
                .owner("growth@example.com")
                .docs_url("https://runbook.example.com/email")
                .tags(vec!["user-facing", "critical"]),
        )
        .queue(
            "media",
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .queue_descriptor(
            "media",
            QueueDescriptor::new()
                .display_name("Media processing")
                .description("Image resize and transcode pipeline.")
                .owner("media@example.com")
                .tags(vec!["batch"]),
        )
        .queue(
            "analytics",
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .queue_descriptor(
            "analytics",
            QueueDescriptor::new()
                .display_name("Scheduled analytics")
                .description("Nightly report generation and export.")
                .owner("data@example.com")
                .tags(vec!["batch", "scheduled"]),
        )
        .register::<SendEmail, _, _>(|_, _| async { Ok(JobResult::Completed) })
        .register::<ResizeImage, _, _>(|_, _| async { Ok(JobResult::Completed) })
        .register::<RunAnalytics, _, _>(|_, _| async { Ok(JobResult::Completed) })
        .register_worker(EmailWorker)
        .register_worker(ImageWorker)
        .register_worker(AnalyticsWorker)
        .job_kind_descriptor::<SendEmail>(
            JobKindDescriptor::new()
                .display_name("Send user email")
                .description("Renders template + hands off to SES.")
                .owner("growth@example.com")
                .docs_url("https://runbook.example.com/send-email")
                .tag("idempotent"),
        )
        .job_kind_descriptor::<ResizeImage>(
            JobKindDescriptor::new()
                .display_name("Resize uploaded image")
                .owner("media@example.com")
                .tag("idempotent"),
        )
        .job_kind_descriptor::<RunAnalytics>(
            JobKindDescriptor::new()
                .display_name("Run analytics report")
                .owner("data@example.com")
                .tag("scheduled"),
        )
        .runtime_snapshot_interval(Duration::from_secs(2))
        .queue_stats_interval(Duration::from_secs(1))
        .build()?;
    client.start().await?;

    // Continuous producer — keeps queues non-empty and throughput steady
    // for however long the screenshot window needs to be.
    let producer_pool = pool.clone();
    let producer = tokio::spawn(async move {
        let mut i: u64 = 0;
        let mut interval = tokio::time::interval(Duration::from_millis(150));
        loop {
            interval.tick().await;
            i += 1;
            for _ in 0..6 {
                let _ = insert_with(
                    &producer_pool,
                    &SendEmail {
                        to: format!("user{i}@example.com"),
                    },
                    InsertOpts {
                        queue: "email".into(),
                        ..Default::default()
                    },
                )
                .await;
            }
            for _ in 0..3 {
                let _ = insert_with(
                    &producer_pool,
                    &ResizeImage {
                        url: format!("s3://bucket/img-{i}.png"),
                    },
                    InsertOpts {
                        queue: "media".into(),
                        ..Default::default()
                    },
                )
                .await;
            }
            let _ = insert_with(
                &producer_pool,
                &RunAnalytics {
                    report: format!("daily-{i}"),
                },
                InsertOpts {
                    queue: "analytics".into(),
                    ..Default::default()
                },
            )
            .await;
        }
    });

    eprintln!("Running demo workload. Ctrl-C to stop.");
    tokio::signal::ctrl_c().await?;
    producer.abort();
    client.shutdown(Duration::from_secs(5)).await;
    meter_provider.force_flush().ok();
    meter_provider.shutdown().ok();
    Ok(())
}
