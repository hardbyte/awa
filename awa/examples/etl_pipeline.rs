//! ETL Pipeline with Progress Tracking and Checkpointing
//!
//! Demonstrates:
//! - Typed job arguments with `#[derive(JobArgs)]`
//! - Transactional enqueue (jobs + business logic atomically)
//! - Structured progress: percent, message, checkpoint metadata
//! - Checkpoint/resume: a crashed job resumes from last offset
//! - Multiple workers on one queue
//!
//! In production you'd split this into:
//! - An API/scheduler service that enqueues ImportTable jobs
//!   (the transactional insert section below)
//! - A worker process: `client.start().await` as a separate
//!   deployment (e.g. k8s Deployment with replicas)
//!
//! Run:
//!   DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo run -p awa --example etl_pipeline

use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobRow, QueueConfig, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

// ── Job types (shared between producer and worker) ──────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ImportTable {
    source_table: String,
    batch_size: usize,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct AggregateMetrics {
    date: String,
    tables: Vec<String>,
}

// ── Worker implementations ──────────────────────────────────────────

struct ImportWorker;

#[async_trait::async_trait]
impl Worker for ImportWorker {
    fn kind(&self) -> &'static str {
        "import_table"
    }

    async fn perform(&self, job: &JobRow, ctx: &JobContext) -> Result<JobResult, JobError> {
        let table: &str = job
            .args
            .get("source_table")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let batch_size = job
            .args
            .get("batch_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000) as usize;

        let total = match table {
            "users" => 5000,
            "orders" => 12000,
            "events" => 50000,
            _ => 1000,
        };

        // Resume from checkpoint — progress metadata survives retries
        let checkpoint = job
            .progress
            .as_ref()
            .and_then(|p| p.get("metadata").cloned());
        let mut offset = checkpoint
            .as_ref()
            .and_then(|m| m.get("last_offset"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        let mut rows_imported = checkpoint
            .as_ref()
            .and_then(|m| m.get("rows_imported"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        if offset > 0 {
            tracing::info!(table, offset, "Resuming import from checkpoint");
        }

        while offset < total {
            let batch_end = (offset + batch_size).min(total);
            rows_imported += batch_end - offset;
            offset = batch_end;

            let pct = (100 * rows_imported / total).min(100) as u8;
            ctx.set_progress(
                pct,
                Some(&format!("Importing {table}: {rows_imported}/{total}")),
            );
            ctx.update_metadata(serde_json::json!({
                "last_offset": offset,
                "rows_imported": rows_imported,
                "source_table": table,
            }))
            .map_err(|e| JobError::terminal(e.to_string()))?;

            // Flush periodically so progress is visible in the UI
            if offset % (batch_size * 5) == 0 {
                ctx.flush_progress().await.map_err(JobError::retryable)?;
            }

            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        tracing::info!(table, rows_imported, "Import complete");
        Ok(JobResult::Completed)
    }
}

struct AggregateWorker;

#[async_trait::async_trait]
impl Worker for AggregateWorker {
    fn kind(&self) -> &'static str {
        "aggregate_metrics"
    }

    async fn perform(&self, job: &JobRow, ctx: &JobContext) -> Result<JobResult, JobError> {
        let tables: Vec<String> = job
            .args
            .get("tables")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        for (i, table) in tables.iter().enumerate() {
            let pct = (100 * (i + 1) / tables.len()).min(100) as u8;
            ctx.set_progress(pct, Some(&format!("Aggregating {table}")));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        tracing::info!(count = tables.len(), "Aggregation complete");
        Ok(JobResult::Completed)
    }
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".into());

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    awa::model::migrations::run(&pool).await?;
    tracing::info!("AWA ETL Pipeline Example");

    // ── Enqueue jobs transactionally (PRODUCER in production) ────

    let mut tx = pool.begin().await?;
    for table in ["users", "orders", "events"] {
        awa::insert_with(
            &mut *tx,
            &ImportTable {
                source_table: table.into(),
                batch_size: 500,
            },
            InsertOpts {
                queue: "etl_example".into(),
                tags: vec!["etl".into(), table.into()],
                metadata: serde_json::json!({"pipeline_run": "2026-03-22"}),
                ..Default::default()
            },
        )
        .await?;
    }
    awa::insert_with(
        &mut *tx,
        &AggregateMetrics {
            date: "2026-03-22".into(),
            tables: vec!["users".into(), "orders".into(), "events".into()],
        },
        InsertOpts {
            queue: "etl_example".into(),
            priority: 3, // runs after imports
            tags: vec!["etl".into(), "aggregate".into()],
            ..Default::default()
        },
    )
    .await?;
    tx.commit().await?;
    tracing::info!("Enqueued 4 ETL jobs (3 imports + 1 aggregation)");

    // ── Start workers (WORKER PROCESS in production) ────────────

    let client = Client::builder(pool.clone())
        .queue(
            "etl_example",
            QueueConfig {
                max_workers: 3,
                ..Default::default()
            },
        )
        .register_worker(ImportWorker)
        .register_worker(AggregateWorker)
        .leader_election_interval(Duration::from_millis(500))
        .build()?;

    client.start().await?;

    // ── Monitor until complete ──────────────────────────────────

    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let stats = awa::model::admin::queue_stats(&pool).await?;
        if let Some(etl) = stats.iter().find(|s| s.queue == "etl_example") {
            if etl.available == 0 && etl.running == 0 {
                tracing::info!("All ETL jobs completed");
                break;
            }
            tracing::info!(
                available = etl.available,
                running = etl.running,
                "ETL queue status"
            );
        }
    }

    client.shutdown(Duration::from_secs(5)).await;
    tracing::info!("Done");
    Ok(())
}
