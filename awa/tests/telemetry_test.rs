//! OTLP integration test — validates that AWA metrics reach an external
//! Prometheus-compatible collector via OTLP gRPC export.
//!
//! Requires:
//! - A running Postgres instance (DATABASE_URL)
//! - A running OTLP collector with Prometheus query API (e.g. grafana/otel-lgtm)
//!
//! Marked `#[ignore]` — only runs when explicitly requested:
//!   cargo test -p awa --test telemetry_test -- --ignored --nocapture
//!
//! See docs/test-plan.md for local setup instructions.

use async_trait::async_trait;
use awa::model::{insert_with, migrations, InsertOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

// ── Helpers ──────────────────────────────────────────────────────────

fn base_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn replace_database_name(url: &str, database_name: &str) -> String {
    let (without_query, query_suffix) = match url.split_once('?') {
        Some((prefix, query)) => (prefix, Some(query)),
        None => (url, None),
    };
    let (base, _) = without_query
        .rsplit_once('/')
        .expect("database URL should include a database name");
    let mut out = format!("{base}/{database_name}");
    if let Some(query) = query_suffix {
        out.push('?');
        out.push_str(query);
    }
    out
}

fn validate_database_name(database_name: &str) {
    assert!(
        !database_name.is_empty()
            && database_name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "telemetry test database names must use only [A-Za-z0-9_]"
    );
}

fn database_url(test_database_name: &str) -> String {
    validate_database_name(test_database_name);
    replace_database_name(&base_database_url(), test_database_name)
}

async fn ensure_database_exists(url: &str) {
    let admin_url = replace_database_name(url, "postgres");
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("Failed to connect to admin database for telemetry tests");
    let database_name = url
        .split_once('?')
        .map(|(prefix, _)| prefix)
        .unwrap_or(url)
        .rsplit_once('/')
        .map(|(_, name)| name)
        .expect("database URL should include a database name");
    let create_sql = format!("CREATE DATABASE {database_name}");
    match sqlx::query(&create_sql).execute(&admin_pool).await {
        Ok(_) => {}
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => {}
        Err(err) => panic!("Failed to create telemetry test database {database_name}: {err}"),
    }
}

fn otlp_endpoint() -> String {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string())
}

fn prometheus_url() -> String {
    std::env::var("PROMETHEUS_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

fn ignored_test_gate() -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    GATE.get_or_init(|| Arc::new(Semaphore::new(1))).clone()
}

async fn setup_pool(test_database_name: &str) -> sqlx::PgPool {
    let url = database_url(test_database_name);
    ensure_database_exists(&url).await;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool).await.expect("Failed to migrate");
    reset_runtime_backend(&pool).await;
    pool
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
    clean_queue_storage_claims(pool, queue).await;
}

/// Purge the queue-storage receipt-claim evidence for one queue.
/// `clean_queue` above only deletes from the `awa.jobs` compat view; when
/// queue-storage is the active backend the open-claim evidence that the
/// running count reads lives on `{schema}.lease_claim_batches` /
/// `lease_claims` / `leases`. A test that panics mid-flight can leave open
/// claims there that a later re-run (on a persistent test DB) would still
/// count as running. Clearing the claim planes keeps per-queue running
/// assertions idempotent across runs. The closure tables carry no `queue`
/// column (keyed by claim slot + receipt identity), so they are cleared
/// wholesale; the claim/lease tables filter by queue.
async fn clean_queue_storage_claims(pool: &sqlx::PgPool, queue: &str) {
    // Clean by physical table presence, not the active-backend flag:
    // `setup_pool` resets the runtime backend to canonical, so at
    // `clean_queue` time `active_queue_storage_schema()` reports None even
    // though the `awa` receipt-plane tables (and any leftover open claims
    // from a prior run) still exist and become live once the client
    // re-activates queue-storage. The default queue-storage schema is `awa`.
    let schema = "awa";
    let installed: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("{schema}.lease_claim_batches"))
        .fetch_one(pool)
        .await
        .expect("probe lease_claim_batches presence");
    if !installed {
        return;
    }
    for table in ["lease_claim_batches", "lease_claims", "leases"] {
        sqlx::query(&format!("DELETE FROM {schema}.{table} WHERE queue = $1"))
            .bind(queue)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("Failed to clean {schema}.{table}: {err}"));
    }
    for table in ["lease_claim_closures", "lease_claim_closure_batches"] {
        sqlx::query(&format!("DELETE FROM {schema}.{table}"))
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("Failed to clean {schema}.{table}: {err}"));
    }
}

async fn reset_runtime_backend(pool: &sqlx::PgPool) {
    let mut tx = pool
        .begin()
        .await
        .expect("Failed to start runtime backend reset transaction");

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET current_engine = 'canonical',
            prepared_engine = NULL,
            state = 'canonical',
            transition_epoch = transition_epoch + 1,
            details = '{}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("Failed to reset storage transition state");
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset active runtime backend");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset runtime instances");
    tx.commit()
        .await
        .expect("Failed to commit runtime backend reset");
}

async fn active_queue_storage_schema(pool: &sqlx::PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT awa.active_queue_storage_schema()")
        .fetch_one(pool)
        .await
        .expect("Failed to resolve active queue storage schema")
}

async fn queue_storage_schema_for_counts(pool: &sqlx::PgPool) -> Option<String> {
    active_queue_storage_schema(pool).await
}

/// Per-queue `(state)` rows across every queue-storage plane, used by the
/// count and breakdown helpers below. `$1` is the queue name.
///
/// Open receipt claims count as `running`. Since #246 (compact deadline
/// claims) the claim path writes receipt claims — zero-deadline AND
/// deadline-backed — as compact `{schema}.lease_claim_batches` rows rather
/// than one `lease_claims` row per job; only claims later materialised into
/// mutable `leases` (or legacy in-flight rows written before the upgrade)
/// remain in `lease_claims`. Reading `lease_claims` alone therefore sees 0
/// running jobs while several are in flight, so both tables feed a single
/// `claim_items` CTE that unnests the batch member arrays. This mirrors the
/// authoritative `queue_counts` running derivation in queue_storage.rs and
/// the `open_receipt_claim_count` helper in queue_storage_runtime_test.rs.
fn queue_storage_state_sources_sql(schema: &str) -> String {
    format!(
        "WITH claim_items AS ( \
             SELECT claims.claim_slot, claims.job_id, claims.run_lease, \
                    claims.receipt_id, claims.queue, claims.closed_at \
             FROM {schema}.lease_claims AS claims \
             UNION ALL \
             SELECT batches.claim_slot, items.job_id, items.run_lease, \
                    items.receipt_id, batches.queue, NULL::timestamptz AS closed_at \
             FROM {schema}.lease_claim_batches AS batches \
             CROSS JOIN LATERAL unnest( \
                 batches.job_ids, batches.run_leases, batches.receipt_ids \
             ) AS items(job_id, run_lease, receipt_id) \
         ) \
         SELECT 'available'::awa.job_state AS state \
         FROM {schema}.ready_entries AS ready \
         JOIN {schema}.queue_claim_heads AS claims \
           ON claims.queue = ready.queue \
          AND claims.priority = ready.priority \
          AND claims.enqueue_shard = ready.enqueue_shard \
         WHERE ready.queue = $1 \
           AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name) \
         UNION ALL \
         SELECT state FROM {schema}.deferred_jobs WHERE queue = $1 \
         UNION ALL \
         SELECT state FROM {schema}.leases WHERE queue = $1 \
         UNION ALL \
         SELECT 'running'::awa.job_state AS state \
         FROM claim_items AS lc \
         WHERE lc.queue = $1 \
           AND lc.closed_at IS NULL \
           AND NOT EXISTS ( \
             SELECT 1 FROM {schema}.lease_claim_closures AS cx \
             WHERE cx.claim_slot = lc.claim_slot \
               AND cx.job_id = lc.job_id \
               AND cx.run_lease = lc.run_lease \
           ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM {schema}.lease_claim_closure_batches AS cb \
             WHERE cb.claim_slot = lc.claim_slot \
               AND cb.receipt_ranges @> lc.receipt_id \
           ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM {schema}.leases AS lease \
             WHERE lease.job_id = lc.job_id \
               AND lease.run_lease = lc.run_lease \
           ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM {schema}.deferred_jobs AS deferred \
             WHERE deferred.job_id = lc.job_id \
               AND deferred.run_lease = lc.run_lease \
           ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM {schema}.done_entries AS done \
             WHERE done.job_id = lc.job_id \
               AND done.run_lease = lc.run_lease \
           ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM {schema}.dlq_entries AS dlq \
             WHERE dlq.job_id = lc.job_id \
               AND dlq.run_lease = lc.run_lease \
           ) \
         UNION ALL \
         SELECT state FROM {schema}.terminal_jobs WHERE queue = $1 \
         UNION ALL \
         SELECT state FROM {schema}.dlq_entries WHERE queue = $1"
    )
}

async fn queue_job_count(pool: &sqlx::PgPool, queue: &str, state: &str) -> i64 {
    if let Some(schema) = queue_storage_schema_for_counts(pool).await {
        let sources = queue_storage_state_sources_sql(&schema);
        let sql = format!(
            "SELECT COUNT(*)::bigint FROM ({sources}) AS jobs \
             WHERE state = $2::awa.job_state"
        );
        return sqlx::query_scalar(&sql)
            .bind(queue)
            .bind(state)
            .fetch_one(pool)
            .await
            .expect("Failed to query queue-storage job count");
    }

    sqlx::query_scalar(
        "SELECT COUNT(*) FROM awa.jobs WHERE queue = $1 AND state = $2::awa.job_state",
    )
    .bind(queue)
    .bind(state)
    .fetch_one(pool)
    .await
    .expect("Failed to query canonical job count")
}

async fn queue_state_breakdown(pool: &sqlx::PgPool, queue: &str) -> Vec<(String, i64)> {
    if let Some(schema) = queue_storage_schema_for_counts(pool).await {
        // Shares `queue_storage_state_sources_sql` with `queue_job_count`
        // above so the breakdown and the count agree — including the
        // compact `lease_claim_batches` running expansion added for #246.
        let sources = queue_storage_state_sources_sql(&schema);
        let sql = format!(
            "SELECT state::text, COUNT(*)::bigint FROM ({sources}) AS jobs \
             GROUP BY state ORDER BY state"
        );
        return sqlx::query_as(&sql)
            .bind(queue)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    }

    sqlx::query_as(
        "SELECT state::text, COUNT(*)::bigint \
         FROM awa.jobs WHERE queue = $1 GROUP BY state ORDER BY state",
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

async fn backdate_callback_timeouts_for_queue(pool: &sqlx::PgPool, queue: &str) {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        sqlx::query(&format!(
            "UPDATE {schema}.leases \
             SET callback_timeout_at = now() - interval '1 second' \
             WHERE queue = $1 AND state = 'waiting_external'"
        ))
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage callback_timeout_at");
        return;
    }

    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' \
         WHERE queue = $1 AND state = 'waiting_external'",
    )
    .bind(queue)
    .execute(pool)
    .await
    .expect("Failed to backdate callback_timeout_at");
}

async fn backdate_callback_timeouts_by_ids(pool: &sqlx::PgPool, job_ids: &[i64]) {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        sqlx::query(&format!(
            "UPDATE {schema}.leases \
             SET callback_timeout_at = now() - interval '1 second' \
             WHERE job_id = ANY($1) AND state = 'waiting_external'"
        ))
        .bind(job_ids)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage callback timeouts");
        return;
    }

    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' WHERE id = ANY($1)",
    )
    .bind(job_ids)
    .execute(pool)
    .await
    .expect("Failed to backdate callback timeouts");
}

async fn backdate_retryable_run_at_for_queue(pool: &sqlx::PgPool, queue: &str) {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        sqlx::query(&format!(
            "UPDATE {schema}.deferred_jobs \
             SET run_at = now() - interval '1 second' \
             WHERE queue = $1 AND state = 'retryable'"
        ))
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage retryable jobs");
        return;
    }

    sqlx::query(
        "UPDATE awa.jobs SET run_at = now() - interval '1 second' \
         WHERE queue = $1 AND state = 'retryable'",
    )
    .bind(queue)
    .execute(pool)
    .await
    .expect("Failed to backdate retryable run_at");
}

async fn backdate_scheduled_run_at_by_ids(pool: &sqlx::PgPool, job_ids: &[i64]) {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        sqlx::query(&format!(
            "UPDATE {schema}.deferred_jobs \
             SET run_at = now() - interval '1 second' \
             WHERE job_id = ANY($1) AND state = 'scheduled'"
        ))
        .bind(job_ids)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage scheduled jobs");
        return;
    }

    sqlx::query("UPDATE awa.jobs SET run_at = now() - interval '1 second' WHERE id = ANY($1)")
        .bind(job_ids)
        .execute(pool)
        .await
        .expect("Failed to backdate scheduled jobs");
}

async fn backdate_running_deadline(pool: &sqlx::PgPool, job_id: i64) {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        // An in-flight receipt claim can live on any of three planes: a
        // `leases` row once it materializes (heartbeat / progress flush /
        // callback wait), a compact `lease_claim_batches` row otherwise
        // (the #246 default for both zero-deadline and deadline-backed
        // claims), or a legacy row-local `lease_claims` row for claims
        // written before the upgrade. Backdate the deadline on all three so
        // the rescue setup works regardless of which plane holds the attempt.
        sqlx::query(&format!(
            "UPDATE {schema}.leases \
             SET deadline_at = now() - interval '1 second' \
             WHERE job_id = $1 AND state = 'running'"
        ))
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage lease deadline rescue job");
        sqlx::query(&format!(
            "UPDATE {schema}.lease_claim_batches \
             SET deadline_at = now() - interval '1 second' \
             WHERE $1 = ANY(job_ids)"
        ))
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage compact batch deadline rescue job");
        sqlx::query(&format!(
            "UPDATE {schema}.lease_claims \
             SET deadline_at = now() - interval '1 second' \
             WHERE job_id = $1"
        ))
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage receipt deadline rescue job");
        return;
    }

    sqlx::query("UPDATE awa.jobs SET deadline_at = now() - interval '1 second' WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate deadline rescue job");
}

async fn backdate_running_heartbeat(pool: &sqlx::PgPool, job_id: i64) {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        // Heartbeat-based rescue only applies once a claim has
        // materialised into a `leases` row (heartbeat_at lives there, not
        // on the claim planes). For an un-materialized receipt claim the
        // deadline-based rescue is the analogue; backdate the receipt's
        // deadline_at on both the compact `lease_claim_batches` plane (the
        // #246 default) and legacy row-local `lease_claims` so
        // `rescue_expired_receipt_deadlines_tx` closes it on the next tick.
        sqlx::query(&format!(
            "UPDATE {schema}.leases \
             SET heartbeat_at = now() - interval '5 minutes' \
             WHERE job_id = $1 AND state = 'running'"
        ))
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage lease heartbeat rescue job");
        sqlx::query(&format!(
            "UPDATE {schema}.lease_claim_batches \
             SET deadline_at = now() - interval '1 second' \
             WHERE $1 = ANY(job_ids)"
        ))
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage compact batch rescue job");
        sqlx::query(&format!(
            "UPDATE {schema}.lease_claims \
             SET deadline_at = now() - interval '1 second' \
             WHERE job_id = $1"
        ))
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage receipt rescue job");
        return;
    }

    sqlx::query("UPDATE awa.jobs SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate heartbeat rescue job");
}

// ── Job type ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TelemetryJob {
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct FailureModeTelemetryJob {
    mode: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct DashboardTelemetryJob {
    mode: String,
    sleep_ms: u64,
}

struct FailureModeWorker;

struct DashboardWorker;

#[async_trait]
impl Worker for FailureModeWorker {
    fn kind(&self) -> &'static str {
        "failure_mode_telemetry_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: FailureModeTelemetryJob =
            serde_json::from_value(ctx.job.args.clone()).map_err(JobError::retryable)?;

        match args.mode.as_str() {
            "complete" => Ok(JobResult::Completed),
            "terminal_fail" => Err(JobError::terminal("intentional telemetry test failure")),
            "retry_once" => {
                if ctx.job.attempt == 1 {
                    // The test backdates run_at after the rows enter retryable,
                    // so retry timing never depends on CI scheduling.
                    Ok(JobResult::RetryAfter(Duration::from_secs(3600)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "callback_timeout" => {
                if ctx.job.attempt == 1 {
                    // Keep the callback parked until the test backdates
                    // callback_timeout_at after verifying waiting_external rows.
                    let callback = ctx
                        .register_callback(Duration::from_secs(3600))
                        .await
                        .map_err(JobError::retryable)?;
                    Ok(JobResult::WaitForCallback(callback))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            other => Err(JobError::terminal(format!(
                "unknown telemetry test mode: {other}"
            ))),
        }
    }
}

#[async_trait]
impl Worker for DashboardWorker {
    fn kind(&self) -> &'static str {
        "dashboard_telemetry_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: DashboardTelemetryJob =
            serde_json::from_value(ctx.job.args.clone()).map_err(JobError::retryable)?;

        match args.mode.as_str() {
            "complete" => {
                if args.sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(args.sleep_ms)).await;
                }
                Ok(JobResult::Completed)
            }
            "cancel" => Ok(JobResult::Cancel(
                "intentional dashboard telemetry cancellation".to_string(),
            )),
            "terminal_fail" => Err(JobError::terminal(
                "intentional dashboard telemetry failure",
            )),
            "retry_once" => {
                if ctx.job.attempt == 1 {
                    Ok(JobResult::RetryAfter(Duration::from_secs(3600)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "callback_timeout" => {
                if ctx.job.attempt == 1 {
                    let callback = ctx
                        .register_callback(Duration::from_secs(3600))
                        .await
                        .map_err(JobError::retryable)?;
                    Ok(JobResult::WaitForCallback(callback))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "deadline_rescue" | "heartbeat_rescue" => {
                if ctx.job.attempt == 1 {
                    let deadline = std::time::Instant::now() + Duration::from_secs(10);
                    loop {
                        if ctx.is_cancelled() {
                            return Ok(JobResult::Completed);
                        }
                        if std::time::Instant::now() >= deadline {
                            return Ok(JobResult::Completed);
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                } else {
                    Ok(JobResult::Completed)
                }
            }
            other => Err(JobError::terminal(format!(
                "unknown dashboard telemetry mode: {other}"
            ))),
        }
    }
}

// ── OTLP + Prometheus helpers ───────────────────────────────────────

fn build_otlp_meter_provider(endpoint: &str, service_name: &str) -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("Failed to build OTLP metric exporter");

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(1))
        .build();

    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .build();

    SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build()
}

async fn wait_for_job_count(pool: &sqlx::PgPool, queue: &str, state: &str, min: i64) {
    let start = std::time::Instant::now();
    // 60s is tight when a rescue + retry path depends on the promote timer,
    // dispatcher claim, worker sleep, and completion flush all completing
    // for 2–4 jobs in sequence. 120s keeps the test deterministic on loaded
    // CI runners without masking a genuine regression (steady-state the
    // wait resolves in single-digit seconds).
    let timeout = Duration::from_secs(120);
    loop {
        let count = queue_job_count(pool, queue, state).await;

        if count >= min {
            return;
        }

        if start.elapsed() > timeout {
            let breakdown = queue_state_breakdown(pool, queue).await;
            panic!(
                "Timed out waiting for {min} {state} jobs in queue {queue}; \
                 only {count} found. Full state breakdown: {breakdown:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_job_state(pool: &sqlx::PgPool, job_id: i64, state: &str) {
    let start = std::time::Instant::now();
    loop {
        match awa::model::admin::get_job(pool, job_id).await {
            Ok(job) if job.state.to_string() == state => return,
            Ok(job) => {
                if start.elapsed() > Duration::from_secs(60) {
                    panic!(
                        "Timed out waiting for job {job_id} to reach state {state}; current state: {}",
                        job.state
                    );
                }
            }
            Err(awa::model::AwaError::JobNotFound { .. }) => {
                if start.elapsed() > Duration::from_secs(60) {
                    panic!("Timed out waiting for job {job_id} to reach state {state}; job disappeared");
                }
            }
            Err(err) => panic!("Failed to query job state: {err}"),
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_no_live_jobs(pool: &sqlx::PgPool, queue: &str, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        let breakdown = queue_state_breakdown(pool, queue).await;
        let live_jobs: i64 = breakdown
            .iter()
            .filter(|(state, _)| {
                matches!(
                    state.as_str(),
                    "available" | "running" | "scheduled" | "retryable" | "waiting_external"
                )
            })
            .map(|(_, count)| *count)
            .sum();

        if live_jobs == 0 {
            return;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for queue {queue} to drain; live_jobs={live_jobs}, breakdown={breakdown:?}"
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_leader(client: &Client, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        if client.health_check().await.leader {
            return;
        }
        if start.elapsed() > timeout {
            panic!("Timed out waiting for single telemetry client to become leader");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Start a TCP proxy that forwards traffic to target_addr.
/// Aborting the returned handle kills the proxy, severing all connections.
async fn start_tcp_proxy(target_addr: &str) -> (u16, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind TCP proxy listener");
    let port = listener.local_addr().unwrap().port();
    let target = target_addr.to_string();
    let accepted_connections = Arc::new(AtomicUsize::new(0));
    let accepted_connections_task = accepted_connections.clone();
    let handle = tokio::spawn(async move {
        while let Ok((mut client_stream, _)) = listener.accept().await {
            let target = target.clone();
            accepted_connections_task.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if let Ok(mut server_stream) = TcpStream::connect(&target).await {
                    let _ =
                        tokio::io::copy_bidirectional(&mut client_stream, &mut server_stream).await;
                }
            });
        }
    });
    (port, accepted_connections, handle)
}

// ── Prometheus query helpers ─────────────────────────────────────────

/// Response shape for Prometheus instant query API.
#[derive(Debug, Deserialize)]
struct PromResponse {
    status: String,
    data: PromData,
}

#[derive(Debug, Deserialize)]
struct PromData {
    result: Vec<PromResult>,
}

#[derive(Debug, Deserialize)]
struct PromResult {
    #[serde(default)]
    metric: std::collections::BTreeMap<String, String>,
    value: (f64, String),
}

/// Query Prometheus and return the sum across all returned series, or None.
async fn prom_query(client: &reqwest::Client, metric: &str) -> Option<f64> {
    let url = format!("{}/api/v1/query", prometheus_url());
    let resp = client
        .get(&url)
        .query(&[("query", metric)])
        .send()
        .await
        .ok()?;

    let body: PromResponse = resp.json().await.ok()?;
    if body.status != "success" {
        return None;
    }
    let mut total = 0.0;
    let mut found = false;
    for result in body.data.result {
        if let Ok(value) = result.value.1.parse::<f64>() {
            total += value;
            found = true;
        }
    }
    found.then_some(total)
}

async fn prom_query_series(client: &reqwest::Client, metric: &str) -> Vec<(String, f64)> {
    let url = format!("{}/api/v1/query", prometheus_url());
    let resp = match client.get(&url).query(&[("query", metric)]).send().await {
        Ok(resp) => resp,
        Err(_) => return Vec::new(),
    };

    let body: PromResponse = match resp.json().await {
        Ok(body) => body,
        Err(_) => return Vec::new(),
    };

    if body.status != "success" {
        return Vec::new();
    }

    body.data
        .result
        .into_iter()
        .filter_map(|result| {
            let value = result.value.1.parse::<f64>().ok()?;
            let labels = if result.metric.is_empty() {
                "value".to_string()
            } else {
                result
                    .metric
                    .into_iter()
                    .filter(|(key, _)| key != "__name__")
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            Some((labels, value))
        })
        .collect()
}

async fn wait_for_series(
    client: &reqwest::Client,
    metric: &str,
    min_series: usize,
    timeout: Duration,
) -> Vec<(String, f64)> {
    let start = std::time::Instant::now();
    loop {
        let series = prom_query_series(client, metric).await;
        if series.len() >= min_series {
            return series;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for {metric} to return at least {min_series} series after {timeout:?}"
            );
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn print_panel_report(title: &str, series: &[(String, f64)]) {
    let observed = series
        .iter()
        .map(|(labels, value)| format!("{labels}={value:.4}"))
        .collect::<Vec<_>>()
        .join("; ");
    eprintln!("panel: {title} -> {observed}");
}

fn named_series(name: &str, series: Vec<(String, f64)>) -> Vec<(String, f64)> {
    series
        .into_iter()
        .map(|(labels, value)| (format!("{name} {labels}"), value))
        .collect()
}

/// Retry a Prometheus query until it returns a value >= threshold or timeout.
async fn wait_for_metric(
    client: &reqwest::Client,
    metric: &str,
    min_value: f64,
    timeout: Duration,
) -> f64 {
    let start = std::time::Instant::now();
    loop {
        if let Some(value) = prom_query(client, metric).await {
            if value >= min_value {
                return value;
            }
            eprintln!(
                "  {metric} = {value} (waiting for >= {min_value}), elapsed {:?}",
                start.elapsed()
            );
        } else {
            eprintln!("  {metric} not found yet, elapsed {:?}", start.elapsed());
        }

        if start.elapsed() > timeout {
            panic!("Timed out waiting for {metric} >= {min_value} after {timeout:?}");
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_for_atomic_count(
    counter: &Arc<AtomicUsize>,
    min: usize,
    label: &str,
    timeout: Duration,
) -> usize {
    let start = std::time::Instant::now();
    loop {
        let value = counter.load(Ordering::SeqCst);
        if value >= min {
            return value;
        }

        if start.elapsed() > timeout {
            panic!("Timed out waiting for {label} to reach {min}; current value={value}");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── Test ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_otlp_metrics_reach_prometheus() {
    let _permit = ignored_test_gate()
        .acquire_owned()
        .await
        .expect("ignored test gate should be available");
    let pool = setup_pool("awa_test_telemetry_otlp").await;
    let queue = "telemetry_otlp_test";
    clean_queue(&pool, queue).await;

    // 1. Configure OTLP metric exporter targeting the collector's gRPC endpoint.
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint())
        .build()
        .expect("Failed to build OTLP metric exporter");

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(1))
        .build();

    let resource = Resource::builder()
        .with_service_name("awa-telemetry-test")
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    // 2. Set as global meter provider so AwaMetrics::from_global() uses it.
    global::set_meter_provider(meter_provider.clone());

    // Exercise the prune instruments directly so this collector-backed test
    // validates their Prometheus names and labels independently of leader and
    // ring timing. Database-backed prune behavior is covered in
    // queue_storage_runtime_test.
    let prune_metrics = awa::worker::AwaMetrics::from_global();
    prune_metrics.record_prune_outcome(
        "queue",
        &awa::model::PruneOutcome::Pruned {
            slot: 3,
            generation: 19,
            carried_failed_rows: 0,
            durations: awa::model::PruneDurations {
                lock: Duration::from_millis(7),
                truncate: Duration::from_millis(647),
                commit: Duration::from_millis(581),
            },
        },
    );
    prune_metrics.record_prune_already_pruned("queue");

    // 3. Build + start Client with a worker. Declare a queue and kind
    // descriptor so the awa.queue.info / awa.job_kind.info gauges fire.
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .queue_descriptor(
            queue,
            awa::QueueDescriptor::new()
                .display_name("Telemetry OTLP queue")
                .owner("otlp-test")
                .tag("telemetry-test"),
        )
        .register::<TelemetryJob, _, _>(|_args, _ctx| async { Ok(JobResult::Completed) })
        .job_kind_descriptor::<TelemetryJob>(
            awa::JobKindDescriptor::new()
                .display_name("Telemetry job")
                .owner("otlp-test"),
        )
        .queue_stats_interval(Duration::from_secs(2))
        .runtime_snapshot_interval(Duration::from_secs(1))
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // 4. Insert jobs and wait for completion.
    let num_jobs = 3;
    for i in 0..num_jobs {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("otlp-test-{i}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert job");
    }

    // Wait for all jobs to complete by polling the DB.
    let start = std::time::Instant::now();
    loop {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .expect("Failed to query completed count");

        if count >= num_jobs {
            eprintln!("All {num_jobs} jobs completed");
            break;
        }

        if start.elapsed() > Duration::from_secs(60) {
            panic!("Timed out waiting for jobs to complete; only {count}/{num_jobs} completed");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 5. Shutdown client + flush meter provider so metrics are exported.
    client.shutdown(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("Failed to flush meter provider");

    // Give the collector a moment to process + scrape.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 6. Query Prometheus HTTP API for AWA metrics.
    let http = reqwest::Client::new();
    let timeout = Duration::from_secs(60);

    eprintln!("Querying Prometheus for awa metrics...");

    // OTel metric names (e.g. awa.job.completed) are translated by the
    // Prometheus exporter: dots → underscores, counter → _total suffix,
    // unit "s" → _seconds suffix. Annotation units like {job} are dropped.
    let completed = wait_for_metric(&http, "awa_job_completed_total", 1.0, timeout).await;
    eprintln!("  awa.job.completed = {completed}");

    let claimed = wait_for_metric(&http, "awa_job_claimed_total", 1.0, timeout).await;
    eprintln!("  awa.job.claimed = {claimed}");

    // awa.dispatch.claim_batches — reliably fires during job execution
    // (heartbeat has a 30s default interval so may not fire in a fast test)
    let claim_batches =
        wait_for_metric(&http, "awa_dispatch_claim_batches_total", 1.0, timeout).await;
    eprintln!("  awa.dispatch.claim_batches = {claim_batches}");

    // Histogram awa.job.duration (unit: s) → awa_job_duration_seconds_count
    let duration_count =
        wait_for_metric(&http, "awa_job_duration_seconds_count", 1.0, timeout).await;
    eprintln!("  awa.job.duration count = {duration_count}");

    // Queue health metrics (new)
    // awa.job.wait_duration (unit: s) → awa_job_wait_duration_seconds_count
    let wait_duration_count =
        wait_for_metric(&http, "awa_job_wait_duration_seconds_count", 1.0, timeout).await;
    eprintln!("  awa.job.wait_duration count = {wait_duration_count}");

    for phase in ["lock", "truncate", "commit"] {
        let query = format!(
            "awa_maintenance_prune_duration_seconds_count{{awa_ring=\"queue\",awa_maintenance_phase=\"{phase}\"}}"
        );
        let count = wait_for_metric(&http, &query, 1.0, timeout).await;
        eprintln!("  awa.maintenance.prune.duration {phase} count = {count}");
    }
    let already_pruned = wait_for_metric(
        &http,
        "awa_maintenance_prune_attempts_total{awa_ring=\"queue\",awa_ring_outcome=\"already_pruned\"}",
        1.0,
        timeout,
    )
    .await;
    eprintln!("  awa.maintenance.prune.attempts already_pruned = {already_pruned}");

    // Note: awa.queue.depth and awa.queue.lag are leader-only gauges published
    // by the maintenance loop. They require leader election + queue_stats_interval
    // timing alignment, so they're validated in the in-memory observability tests
    // rather than the OTLP integration test.

    // 7. Assertions (wait_for_metric already panics on timeout, but
    //    let's be explicit about what we expected).
    assert!(
        completed >= 1.0,
        "Expected awa.job.completed >= 1, got {completed}"
    );
    assert!(
        claimed >= 1.0,
        "Expected awa.job.claimed >= 1, got {claimed}"
    );
    assert!(
        claim_batches >= 1.0,
        "Expected awa.dispatch.claim_batches >= 1, got {claim_batches}"
    );
    assert!(
        duration_count >= 1.0,
        "Expected awa.job.duration count >= 1, got {duration_count}"
    );
    assert!(
        wait_duration_count >= 1.0,
        "Expected awa.job.wait_duration count >= 1, got {wait_duration_count}"
    );

    // Descriptor info gauges. These are the label-join targets for any
    // panel that wants to surface display_name / owner alongside raw queue
    // and kind names. Each gauge should be 1.
    let queue_info = wait_for_metric(&http, "awa_queue_info", 1.0, timeout).await;
    eprintln!("  awa.queue.info = {queue_info}");
    assert!(
        queue_info >= 1.0,
        "Expected awa.queue.info >= 1, got {queue_info}"
    );

    let kind_info = wait_for_metric(&http, "awa_job_kind_info", 1.0, timeout).await;
    eprintln!("  awa.job_kind.info = {kind_info}");
    assert!(
        kind_info >= 1.0,
        "Expected awa.job_kind.info >= 1, got {kind_info}"
    );

    // The info gauges carry descriptor attributes; verify by querying with
    // a label filter so we know the owner made it through the exporter.
    let queue_info_labeled = wait_for_metric(
        &http,
        "awa_queue_info{awa_queue_owner=\"otlp-test\"}",
        1.0,
        timeout,
    )
    .await;
    assert!(
        queue_info_labeled >= 1.0,
        "Expected awa_queue_info{{awa_queue_owner=\"otlp-test\"}} >= 1, got {queue_info_labeled}"
    );

    // Clean up.
    let _ = meter_provider.shutdown();
    eprintln!("Telemetry OTLP integration test passed!");
}

/// Validates that failure-path metrics (failed, retried, rescues) reach Prometheus
/// via the full OTLP gRPC export pipeline.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_failure_path_metrics_reach_prometheus() {
    let _permit = ignored_test_gate()
        .acquire_owned()
        .await
        .expect("ignored test gate should be available");
    let pool = setup_pool("awa_test_telemetry_failure").await;
    let queue = "telemetry_failure_path";
    clean_queue(&pool, queue).await;

    // 1. Configure OTLP exporter and set as global.
    let meter_provider = build_otlp_meter_provider(&otlp_endpoint(), "awa-failure-path-test");
    global::set_meter_provider(meter_provider.clone());

    // 2. Build client with fast maintenance intervals, but keep retry/callback
    // transitions under explicit DB control to avoid CI timing flakes (#67).
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .callback_rescue_interval(Duration::from_millis(150))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(FailureModeWorker)
        .build()
        .expect("Failed to build failure-path client");

    client
        .start()
        .await
        .expect("Failed to start failure-path client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    // 3. Insert jobs across failure modes.
    let modes = [
        ("complete", 3, 3),         // 3 jobs, max_attempts 3
        ("terminal_fail", 2, 1),    // 2 jobs, max_attempts 1 → immediate terminal
        ("callback_timeout", 2, 3), // 2 jobs, max_attempts 3 → callback rescue then complete
        ("retry_once", 2, 3),       // 2 jobs, max_attempts 3 → retry then complete
    ];

    for (mode, count, max_attempts) in modes {
        for i in 0..count {
            insert_with(
                &pool,
                &FailureModeTelemetryJob {
                    mode: mode.to_string(),
                },
                InsertOpts {
                    queue: queue.into(),
                    max_attempts,
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to insert {mode} job {i}"));
        }
    }

    // 4. Wait for the stable intermediate states, then force the timed
    // transitions from the database so the test doesn't rely on wall clock.
    wait_for_job_count(&pool, queue, "completed", 3).await;
    wait_for_job_count(&pool, queue, "failed", 2).await;
    wait_for_job_count(&pool, queue, "waiting_external", 2).await;
    wait_for_job_count(&pool, queue, "retryable", 2).await;

    // Backdate callback_timeout_at so the next callback-rescue tick
    // transitions these jobs into retryable. The rescue itself sets
    // run_at = now() + backoff_duration(attempt, max_attempts) — which can
    // be several seconds — so we can't backdate run_at before the rescue
    // fires (the new retryable rows would inherit future run_at). Wait for
    // waiting_external to drain first, then backdate run_at for every
    // retryable row (both the original retry_once jobs and the callback
    // rescues).
    backdate_callback_timeouts_for_queue(&pool, queue).await;

    // Retryable jumps from 2 to 4 once both callback rescues land. No row
    // leaves retryable until run_at is backdated (below), so the count is
    // monotonic and waiting for >= 4 is the right signal that the rescue
    // pass is done.
    wait_for_job_count(&pool, queue, "retryable", 4).await;

    backdate_retryable_run_at_for_queue(&pool, queue).await;

    // 5. Wait for terminal states: 7 completed (3 + 2 callback + 2 retry) + 2 failed.
    let expected_completed = 7_i64;
    let expected_failed = 2_i64;

    wait_for_job_count(&pool, queue, "completed", expected_completed).await;
    wait_for_job_count(&pool, queue, "failed", expected_failed).await;

    // 6. Shutdown + flush to push metrics to the collector.
    client.shutdown(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("Failed to flush meter provider");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 7. Query Prometheus for failure-path metrics.
    let http = reqwest::Client::new();
    let timeout = Duration::from_secs(60);

    eprintln!("Querying Prometheus for failure-path metrics...");

    let completed = wait_for_metric(
        &http,
        "awa_job_completed_total",
        expected_completed as f64,
        timeout,
    )
    .await;
    eprintln!("  awa_job_completed_total = {completed}");

    let failed = wait_for_metric(
        &http,
        "awa_job_failed_total",
        expected_failed as f64,
        timeout,
    )
    .await;
    eprintln!("  awa_job_failed_total = {failed}");

    let retried = wait_for_metric(&http, "awa_job_retried_total", 2.0, timeout).await;
    eprintln!("  awa_job_retried_total = {retried}");

    let rescues = wait_for_metric(&http, "awa_maintenance_rescues_total", 2.0, timeout).await;
    eprintln!("  awa_maintenance_rescues_total = {rescues}");

    let claimed = wait_for_metric(&http, "awa_job_claimed_total", 9.0, timeout).await;
    eprintln!("  awa_job_claimed_total = {claimed}");

    let duration_count =
        wait_for_metric(&http, "awa_job_duration_seconds_count", 5.0, timeout).await;
    eprintln!("  awa_job_duration_seconds_count = {duration_count}");

    assert!(completed >= expected_completed as f64);
    assert!(failed >= expected_failed as f64);
    assert!(retried >= 2.0, "Expected retried >= 2, got {retried}");
    assert!(rescues >= 2.0, "Expected rescues >= 2, got {rescues}");
    assert!(claimed >= 9.0, "Expected claimed >= 9, got {claimed}");
    assert!(
        duration_count >= 5.0,
        "Expected duration count >= 5, got {duration_count}"
    );

    let _ = meter_provider.shutdown();
    eprintln!("Failure-path OTLP telemetry test passed!");
}

/// Validates that job processing is unaffected when the OTLP collector dies mid-flight.
///
/// Uses an in-process TCP proxy to the real collector. Phase 1 verifies metrics
/// flow through the proxy. Phase 2 kills the proxy (simulating collector death)
/// and asserts jobs still complete and health checks pass.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_collector_death_does_not_block_job_processing() {
    let _permit = ignored_test_gate()
        .acquire_owned()
        .await
        .expect("ignored test gate should be available");
    let pool = setup_pool("awa_test_telemetry_collector").await;
    let queue = "telemetry_collector_death";
    clean_queue(&pool, queue).await;

    // 1. Start TCP proxy forwarding to the real OTLP collector.
    let otlp_target = otlp_endpoint()
        .strip_prefix("http://")
        .unwrap_or("localhost:4317")
        .to_string();
    let (proxy_port, proxy_connections, proxy_handle) = start_tcp_proxy(&otlp_target).await;
    let proxy_endpoint = format!("http://127.0.0.1:{proxy_port}");
    eprintln!("TCP proxy listening on {proxy_endpoint} → {otlp_target}");

    // 2. Configure OTLP exporter through the proxy.
    let meter_provider = build_otlp_meter_provider(&proxy_endpoint, "awa-collector-death-test");
    global::set_meter_provider(meter_provider.clone());

    // 3. Build + start client.
    let executed_jobs = Arc::new(AtomicUsize::new(0));
    let executed_jobs_worker = executed_jobs.clone();

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register::<TelemetryJob, _, _>(move |_args, _ctx| {
            let executed_jobs_worker = executed_jobs_worker.clone();
            async move {
                executed_jobs_worker.fetch_add(1, Ordering::SeqCst);
                Ok(JobResult::Completed)
            }
        })
        .build()
        .expect("Failed to build collector-death client");

    client
        .start()
        .await
        .expect("Failed to start collector-death client");

    // ── Phase 1: collector alive ──
    let phase1_jobs = 5_i64;
    for i in 0..phase1_jobs {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("alive-{i}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert phase-1 job");
    }

    wait_for_atomic_count(
        &executed_jobs,
        phase1_jobs as usize,
        "phase-1 executed jobs",
        Duration::from_secs(30),
    )
    .await;
    wait_for_no_live_jobs(&pool, queue, Duration::from_secs(30)).await;
    eprintln!("Phase 1: {phase1_jobs} jobs executed with live collector");

    // Flush to ensure at least one export went through the proxy.
    meter_provider
        .force_flush()
        .expect("Failed to flush meter provider");
    wait_for_atomic_count(
        &proxy_connections,
        1,
        "proxy connections",
        Duration::from_secs(10),
    )
    .await;
    eprintln!(
        "Phase 1: exporter established {} proxy connection(s)",
        proxy_connections.load(Ordering::SeqCst)
    );

    // ── Phase 2: kill the collector proxy ──
    proxy_handle.abort();
    eprintln!("Phase 2: TCP proxy killed — OTLP collector is now unreachable");

    // Insert more jobs while the collector is dead.
    let phase2_jobs = 5_i64;
    for i in 0..phase2_jobs {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("dead-{i}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert phase-2 job");
    }

    // Jobs must still complete — the dead collector must not block processing.
    wait_for_atomic_count(
        &executed_jobs,
        (phase1_jobs + phase2_jobs) as usize,
        "phase-2 executed jobs",
        Duration::from_secs(30),
    )
    .await;
    wait_for_no_live_jobs(&pool, queue, Duration::from_secs(30)).await;
    eprintln!(
        "Phase 2: all {} jobs executed with dead collector",
        phase1_jobs + phase2_jobs
    );

    // Health check: the runtime loops must still be alive.
    let health = client.health_check().await;
    assert!(
        health.poll_loop_alive,
        "Dispatch loop should still be alive after collector death"
    );
    assert!(
        health.heartbeat_alive,
        "Heartbeat loop should still be alive after collector death"
    );

    client.shutdown(Duration::from_secs(5)).await;
    let _ = meter_provider.shutdown();
    eprintln!("Collector-death resilience test passed!");
}

/// Regression guard for the `queue_job_count` / `queue_state_breakdown`
/// helpers used by `dashboard_panels_have_observed_data`. Since #246 the
/// claim path writes receipt claims as compact `lease_claim_batches` rows
/// rather than one `lease_claims` row per job, so a helper that enumerates
/// running work from `lease_claims` alone reports 0 running for a saturated
/// queue (the exact failure the dashboard test hit). This test holds every
/// worker slot in-flight and asserts the batch-expanding helpers see them —
/// it needs Postgres but no OTLP collector, so it is not `#[ignore]`d and
/// runs on every CI leg.
#[tokio::test(flavor = "multi_thread")]
async fn running_count_helpers_see_compact_batch_claims() {
    let pool = setup_pool("awa_test_running_count_batches").await;
    let queue = "running_count_batches";
    clean_queue(&pool, queue).await;

    let in_flight: i64 = 4;
    let entered = Arc::new(AtomicUsize::new(0));
    // Zero permits: every handler blocks on acquire until the test releases
    // it, pinning `in_flight` jobs in the claimed-but-not-complete state.
    let release = Arc::new(Semaphore::new(0));
    let entered_worker = entered.clone();
    let release_worker = release.clone();

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: in_flight as u32,
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<TelemetryJob, _, _>(move |_args, _ctx| {
            let entered_worker = entered_worker.clone();
            let release_worker = release_worker.clone();
            async move {
                entered_worker.fetch_add(1, Ordering::SeqCst);
                let permit = release_worker
                    .acquire()
                    .await
                    .expect("release semaphore stays open");
                permit.forget();
                Ok(JobResult::Completed)
            }
        })
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .build()
        .expect("Failed to build running-count client");

    client.start().await.expect("Failed to start client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    for index in 0..in_flight {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("held-{index}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert held demo job");
    }

    // Wait until all workers are parked inside the handler.
    let start = std::time::Instant::now();
    while entered.load(Ordering::SeqCst) < in_flight as usize {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "timed out waiting for {in_flight} jobs to enter the handler; \
             only {} entered",
            entered.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The claims live in `lease_claim_batches`; the helpers must expand them.
    let running = queue_job_count(&pool, queue, "running").await;
    assert_eq!(
        running,
        in_flight,
        "queue_job_count must count compact-batch claims as running; \
         breakdown: {:?}",
        queue_state_breakdown(&pool, queue).await
    );
    let breakdown = queue_state_breakdown(&pool, queue).await;
    let running_in_breakdown = breakdown
        .iter()
        .find(|(state, _)| state == "running")
        .map(|(_, count)| *count)
        .unwrap_or(0);
    assert_eq!(
        running_in_breakdown, in_flight,
        "queue_state_breakdown must agree with queue_job_count; got {breakdown:?}"
    );

    // Release the workers and drain so the DB is clean for reuse.
    release.add_permits(in_flight as usize);
    let drain_start = std::time::Instant::now();
    loop {
        if queue_job_count(&pool, queue, "running").await == 0 {
            break;
        }
        assert!(
            drain_start.elapsed() < Duration::from_secs(30),
            "held jobs never drained after release"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.shutdown(Duration::from_secs(5)).await;
}

/// Exercises the Grafana dashboard queries against a live LGTM stack and prints
/// a per-panel report so we can verify every panel has observed data.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn dashboard_panels_have_observed_data() {
    let _permit = ignored_test_gate()
        .acquire_owned()
        .await
        .expect("ignored test gate should be available");
    let pool = setup_pool("awa_test_telemetry_dashboard").await;
    let queue = "grafana_demo";
    clean_queue(&pool, queue).await;

    let meter_provider = build_otlp_meter_provider(&otlp_endpoint(), "awa-grafana-demo");
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register::<TelemetryJob, _, _>(|args: TelemetryJob, _ctx| async move {
            let sleep_ms = if args.value.starts_with("slow") {
                4_000
            } else {
                20
            };
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            Ok(JobResult::Completed)
        })
        .register_worker(DashboardWorker)
        .queue_stats_interval(Duration::from_secs(1))
        .heartbeat_interval(Duration::from_millis(100))
        .heartbeat_rescue_interval(Duration::from_millis(150))
        .deadline_rescue_interval(Duration::from_millis(150))
        .callback_rescue_interval(Duration::from_millis(150))
        .promote_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    // Phase 1: generate non-backlog metrics (failures, retries, callbacks,
    // promotions, rescues, cancellations).
    let cancelled_job = insert_with(
        &pool,
        &DashboardTelemetryJob {
            mode: "cancel".into(),
            sleep_ms: 0,
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert cancelled demo job");

    for _ in 0..2 {
        insert_with(
            &pool,
            &DashboardTelemetryJob {
                mode: "terminal_fail".into(),
                sleep_ms: 0,
            },
            InsertOpts {
                queue: queue.into(),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert terminal failure demo job");
    }

    let retry_job_ids = {
        let mut job_ids = Vec::new();
        for _ in 0..2 {
            let job = insert_with(
                &pool,
                &DashboardTelemetryJob {
                    mode: "retry_once".into(),
                    sleep_ms: 0,
                },
                InsertOpts {
                    queue: queue.into(),
                    max_attempts: 3,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to insert retry demo job");
            job_ids.push(job.id);
        }
        job_ids
    };

    let callback_job_ids = {
        let mut job_ids = Vec::new();
        for _ in 0..2 {
            let job = insert_with(
                &pool,
                &DashboardTelemetryJob {
                    mode: "callback_timeout".into(),
                    sleep_ms: 0,
                },
                InsertOpts {
                    queue: queue.into(),
                    max_attempts: 3,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to insert callback demo job");
            job_ids.push(job.id);
        }
        job_ids
    };

    let deadline_job = insert_with(
        &pool,
        &DashboardTelemetryJob {
            mode: "deadline_rescue".into(),
            sleep_ms: 0,
        },
        InsertOpts {
            queue: queue.into(),
            max_attempts: 3,
            deadline_duration: Some(chrono::Duration::hours(1)),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert deadline rescue demo job");

    let heartbeat_job = insert_with(
        &pool,
        &DashboardTelemetryJob {
            mode: "heartbeat_rescue".into(),
            sleep_ms: 0,
        },
        InsertOpts {
            queue: queue.into(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert heartbeat rescue demo job");

    let scheduled_job_ids = {
        let mut job_ids = Vec::new();
        for index in 0..2 {
            let job = insert_with(
                &pool,
                &TelemetryJob {
                    value: format!("scheduled-{index}"),
                },
                InsertOpts {
                    queue: queue.into(),
                    run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to insert scheduled demo job");
            job_ids.push(job.id);
        }
        job_ids
    };

    wait_for_job_state(&pool, cancelled_job.id, "cancelled").await;
    wait_for_job_count(&pool, queue, "failed", 2).await;
    wait_for_job_count(&pool, queue, "retryable", 2).await;
    wait_for_job_count(&pool, queue, "waiting_external", 2).await;
    wait_for_job_state(&pool, deadline_job.id, "running").await;
    wait_for_job_state(&pool, heartbeat_job.id, "running").await;
    wait_for_job_count(&pool, queue, "scheduled", 2).await;

    // Let the queue depth gauge capture retryable/waiting_external/scheduled.
    tokio::time::sleep(Duration::from_secs(2)).await;

    backdate_callback_timeouts_by_ids(&pool, &callback_job_ids).await;

    // Wait for the callback rescue to land — waiting_external drains and
    // retryable count jumps from 2 to 4. Then backdate run_at for ALL
    // retryable rows (both the original retry_once jobs and the freshly
    // rescued callback_timeout jobs). Doing this before the rescue lets
    // the newly-rescued rows inherit `run_at = now() + backoff_duration`
    // and stall the test (see maintenance.rs:560-561).
    wait_for_job_count(&pool, queue, "retryable", 4).await;

    backdate_retryable_run_at_for_queue(&pool, queue).await;
    // Silence the unused-binding warning — IDs are only needed for the
    // callback backdate above; the run_at backdate intentionally targets
    // the queue-wide set so it covers the rescued rows.
    let _ = &retry_job_ids;

    backdate_scheduled_run_at_by_ids(&pool, &scheduled_job_ids).await;

    backdate_running_deadline(&pool, deadline_job.id).await;

    backdate_running_heartbeat(&pool, heartbeat_job.id).await;

    wait_for_job_count(&pool, queue, "completed", 6).await;
    wait_for_job_count(&pool, queue, "failed", 2).await;

    // Phase 2: create current backlog so gauge/stat panels are populated now.
    for index in 0..10 {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("slow-{index}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert slow demo job");
    }

    for index in 0..6 {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("fast-{index}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert fast demo job");
    }

    wait_for_job_count(&pool, queue, "running", 4).await;
    wait_for_job_count(&pool, queue, "available", 2).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Flush twice with a gap so Prometheus has ≥2 scrape points for rate()
    // calculations. A single flush produces one data point; rate() over a 5m
    // window needs at least two to return non-zero results.
    meter_provider
        .force_flush()
        .expect("first flush before panel queries");
    tokio::time::sleep(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("second flush for rate() calculations");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let http = reqwest::Client::new();
    let timeout = Duration::from_secs(90);
    let queue_match = queue;

    let queue_lag = wait_for_series(
        &http,
        &format!("awa_queue_lag_seconds{{awa_job_queue=\"{queue_match}\"}}"),
        1,
        timeout,
    )
    .await;
    print_panel_report("Queue Lag", &queue_lag);

    let queue_depth = wait_for_series(
        &http,
        &format!(
            "awa_queue_depth{{awa_job_queue=\"{queue_match}\", awa_job_state=~\"available|running|failed|scheduled|retryable|waiting_external\"}}"
        ),
        6,
        timeout,
    )
    .await;
    print_panel_report("Queue Depth", &queue_depth);

    let mut job_wait = Vec::new();
    job_wait.extend(named_series(
        "p50",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.50, sum by (le, awa_job_queue) (rate(awa_job_wait_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    job_wait.extend(named_series(
        "p95",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.95, sum by (le, awa_job_queue) (rate(awa_job_wait_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    job_wait.extend(named_series(
        "p99",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.99, sum by (le, awa_job_queue) (rate(awa_job_wait_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    print_panel_report("Job Wait Time", &job_wait);

    let mut job_throughput = Vec::new();
    job_throughput.extend(named_series(
        "completed",
        wait_for_series(
            &http,
            &format!("sum(rate(awa_job_completed_total{{awa_job_queue=\"{queue_match}\"}}[5m]))"),
            1,
            timeout,
        )
        .await,
    ));
    job_throughput.extend(named_series(
        "failed",
        wait_for_series(
            &http,
            &format!("sum(rate(awa_job_failed_total{{awa_job_queue=\"{queue_match}\"}}[5m]))"),
            1,
            timeout,
        )
        .await,
    ));
    job_throughput.extend(named_series(
        "retried",
        wait_for_series(
            &http,
            &format!("sum(rate(awa_job_retried_total{{awa_job_queue=\"{queue_match}\"}}[5m]))"),
            1,
            timeout,
        )
        .await,
    ));
    job_throughput.extend(named_series(
        "cancelled",
        wait_for_series(
            &http,
            &format!("sum(rate(awa_job_cancelled_total{{awa_job_queue=\"{queue_match}\"}}[5m]))"),
            1,
            timeout,
        )
        .await,
    ));
    print_panel_report("Job Throughput", &job_throughput);

    let in_flight = wait_for_series(
        &http,
        &format!("sum by (awa_job_queue) (awa_job_in_flight{{awa_job_queue=\"{queue_match}\"}})"),
        1,
        timeout,
    )
    .await;
    print_panel_report("In-Flight Jobs", &in_flight);

    let mut job_duration = Vec::new();
    job_duration.extend(named_series(
        "p50",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.50, sum by (le, awa_job_queue) (rate(awa_job_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    job_duration.extend(named_series(
        "p95",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.95, sum by (le, awa_job_queue) (rate(awa_job_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    job_duration.extend(named_series(
        "p99",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.99, sum by (le, awa_job_queue) (rate(awa_job_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    print_panel_report("Job Duration", &job_duration);

    let throughput_by_kind = wait_for_series(
        &http,
        &format!(
            "topk(10, sum by (awa_job_kind) (rate(awa_job_completed_total{{awa_job_queue=\"{queue_match}\"}}[5m])))"
        ),
        1,
        timeout,
    )
    .await;
    print_panel_report("Throughput by Kind", &throughput_by_kind);

    let mut claim_latency = Vec::new();
    claim_latency.extend(named_series(
        "p50",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.50, sum by (le, awa_job_queue) (rate(awa_dispatch_claim_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    claim_latency.extend(named_series(
        "p95",
        wait_for_series(
            &http,
            &format!(
                "histogram_quantile(0.95, sum by (le, awa_job_queue) (rate(awa_dispatch_claim_duration_seconds_bucket{{awa_job_queue=\"{queue_match}\"}}[5m])))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    print_panel_report("Claim Latency", &claim_latency);

    let claim_batch_size = wait_for_series(
        &http,
        &format!(
            "sum by (awa_job_queue) (rate(awa_dispatch_claim_batch_size_sum{{awa_job_queue=\"{queue_match}\"}}[5m])) / sum by (awa_job_queue) (rate(awa_dispatch_claim_batch_size_count{{awa_job_queue=\"{queue_match}\"}}[5m]))"
        ),
        1,
        timeout,
    )
    .await;
    print_panel_report("Claim Batch Size", &claim_batch_size);

    // Only assert callback_timeout rescue kind — heartbeat and deadline
    // rescues race with the heartbeat service which refreshes running jobs,
    // preventing the stale-heartbeat/expired-deadline from persisting.
    let rescues = wait_for_series(
        &http,
        "sum by (awa_rescue_kind) (rate(awa_maintenance_rescues_total[5m]))",
        1,
        timeout,
    )
    .await;
    print_panel_report("Maintenance Rescues", &rescues);

    let completion_flush = wait_for_series(
        &http,
        "histogram_quantile(0.95, sum by (le) (rate(awa_completion_flush_duration_seconds_bucket[5m])))",
        1,
        timeout,
    )
    .await;
    print_panel_report("Completion Flush Performance", &completion_flush);

    let promotion = wait_for_series(
        &http,
        "sum by (awa_job_state) (rate(awa_maintenance_promote_batches_total[5m]))",
        1,
        timeout,
    )
    .await;
    print_panel_report("Promotion Throughput", &promotion);

    let mut claims_waiting_external = Vec::new();
    claims_waiting_external.extend(named_series(
        "claimed",
        wait_for_series(
            &http,
            &format!("sum(rate(awa_job_claimed_total{{awa_job_queue=\"{queue_match}\"}}[5m]))"),
            1,
            timeout,
        )
        .await,
    ));
    claims_waiting_external.extend(named_series(
        "waiting_external",
        wait_for_series(
            &http,
            &format!(
                "sum(rate(awa_job_waiting_external_total{{awa_job_queue=\"{queue_match}\"}}[5m]))"
            ),
            1,
            timeout,
        )
        .await,
    ));
    print_panel_report("Claims / Waiting External", &claims_waiting_external);

    let error_rate = wait_for_series(
        &http,
        &format!(
            "sum(rate(awa_job_failed_total{{awa_job_queue=\"{queue_match}\"}}[5m])) / (sum(rate(awa_job_completed_total{{awa_job_queue=\"{queue_match}\"}}[5m])) + sum(rate(awa_job_failed_total{{awa_job_queue=\"{queue_match}\"}}[5m])) + 1e-10)"
        ),
        1,
        timeout,
    )
    .await;
    print_panel_report("Error Rate", &error_rate);

    let jobs_in_flight = wait_for_series(
        &http,
        &format!("sum(awa_job_in_flight{{awa_job_queue=\"{queue_match}\"}})"),
        1,
        timeout,
    )
    .await;
    print_panel_report("Jobs In Flight", &jobs_in_flight);

    let throughput = wait_for_series(
        &http,
        &format!("sum(rate(awa_job_completed_total{{awa_job_queue=\"{queue_match}\"}}[5m]))"),
        1,
        timeout,
    )
    .await;
    print_panel_report("Throughput", &throughput);

    let rescues_5m = wait_for_series(
        &http,
        "sum(increase(awa_maintenance_rescues_total[5m]))",
        1,
        timeout,
    )
    .await;
    print_panel_report("Rescues (5m)", &rescues_5m);

    wait_for_no_live_jobs(&pool, queue, Duration::from_secs(30)).await;
    client.shutdown(Duration::from_secs(30)).await;
    meter_provider.force_flush().expect("flush");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = meter_provider.shutdown();
    eprintln!("Dashboard validated at http://localhost:3200/d/awa-job-queue/awa-job-queue");
}

// ── Trace pipeline e2e (ADR-039) ─────────────────────────────────────
//
// Asserts the FULL span path: producer span → traceparent captured at
// enqueue → worker execution span exported over OTLP → queryable in Tempo
// with the topology ADR-039 promises (first attempt parented to the
// enqueue site; retries as fresh roots carrying a span link back).

fn tempo_url() -> String {
    std::env::var("TEMPO_URL").unwrap_or_else(|_| "http://localhost:3200".to_string())
}

/// Install a process-global OTLP trace pipeline (tracer provider + tracing
/// subscriber). Global because the worker's spans are created on spawned
/// runtime tasks; once-only because a process can only have one subscriber
/// — the ignored-test gate serializes the tests that rely on it.
///
/// The tonic channel is connected EAGERLY on the test's tokio runtime: the
/// batch span processor drives exports from its own plain thread, and a
/// lazily-connected channel would try to start its connection driver there
/// (no reactor → every export times out).
async fn install_trace_pipeline(
    endpoint: &str,
    service_name: &str,
) -> opentelemetry_sdk::trace::SdkTracerProvider {
    static PIPELINE: OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> = OnceLock::new();
    if let Some(provider) = PIPELINE.get() {
        return provider.clone();
    }
    let channel = tonic::transport::Channel::from_shared(endpoint.to_string())
        .expect("valid OTLP endpoint")
        .connect()
        .await
        .expect("connect to OTLP collector");
    use opentelemetry_otlp::WithTonicConfig;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_channel(channel)
        .build()
        .expect("Failed to build OTLP span exporter");
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.to_owned())
                .build(),
        )
        .build();
    use opentelemetry::trace::TracerProvider as _;
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("awa-trace-e2e"));
    use tracing_subscriber::layer::SubscriberExt;
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
        .expect("set global tracing subscriber");
    PIPELINE.set(provider.clone()).ok();
    provider
}

/// Standard base64 (with padding) of a hex string — OTLP protojson encodes
/// trace/span ids as base64 bytes, while awa's traceparent carries hex.
fn hex_to_base64(hex: &str) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
        .collect();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Fetch all spans of one trace from Tempo, or None until it is queryable.
async fn tempo_trace_spans(
    client: &reqwest::Client,
    trace_id_hex: &str,
) -> Option<Vec<serde_json::Value>> {
    let url = format!("{}/api/traces/{trace_id_hex}", tempo_url());
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = resp.json().await.ok()?;
    let mut spans = Vec::new();
    for batch in body["batches"].as_array()? {
        for scope in batch["scopeSpans"].as_array().cloned().unwrap_or_default() {
            for span in scope["spans"].as_array().cloned().unwrap_or_default() {
                spans.push(span);
            }
        }
    }
    (!spans.is_empty()).then_some(spans)
}

/// Poll Tempo until the trace is queryable (ingest is asynchronous).
async fn wait_for_tempo_trace(
    client: &reqwest::Client,
    trace_id_hex: &str,
    minimum_spans: usize,
) -> Vec<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(spans) = tempo_trace_spans(client, trace_id_hex).await {
            if spans.len() >= minimum_spans {
                return spans;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "trace {trace_id_hex} did not become queryable in Tempo within 60s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn span_attr<'a>(span: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    span["attributes"].as_array()?.iter().find_map(|attribute| {
        (attribute["key"].as_str() == Some(key))
            .then(|| attribute["value"]["stringValue"].as_str())
            .flatten()
    })
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TraceE2eJob {
    pub mode: String,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_trace_pipeline_reaches_tempo() {
    let _permit = ignored_test_gate()
        .acquire_owned()
        .await
        .expect("ignored test gate should be available");
    let pool = setup_pool("awa_test_telemetry_traces").await;
    let queue = "trace_demo";
    clean_queue(&pool, queue).await;

    let provider = install_trace_pipeline(&otlp_endpoint(), "awa-trace-e2e").await;

    // Enqueue two jobs inside one producer span: one that completes first
    // try, one that retries once (to witness the fresh-root-plus-link
    // topology). Inserted BEFORE the worker starts so claims are immediate.
    let producer_span = tracing::info_span!("e2e.enqueue");
    let producer_context = {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        producer_span.context().span().span_context().clone()
    };
    assert!(producer_context.is_valid(), "producer span context invalid");
    let producer_trace_hex = producer_context.trace_id().to_string();

    use tracing::Instrument;
    let complete_job = insert_with(
        &pool,
        &TraceE2eJob {
            mode: "complete".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .instrument(producer_span.clone())
    .await
    .expect("insert complete job");
    let retry_job = insert_with(
        &pool,
        &TraceE2eJob {
            mode: "retry".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .instrument(producer_span.clone())
    .await
    .expect("insert retry job");
    drop(producer_span);

    let enqueue_site = |job: &awa::model::JobRow| -> opentelemetry::trace::SpanContext {
        awa_model::trace::traceparent_from_metadata(&job.metadata)
            .and_then(awa_model::trace::parse_traceparent)
            .expect("traceparent captured at enqueue")
    };
    let complete_site = enqueue_site(&complete_job);
    let retry_site = enqueue_site(&retry_job);
    assert_eq!(complete_site.trace_id().to_string(), producer_trace_hex);

    let attempts = Arc::new(AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register::<TraceE2eJob, _, _>(move |args: TraceE2eJob, ctx| {
            let attempts = handler_attempts.clone();
            let attempt = ctx.job.attempt;
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                if args.mode == "retry" && attempt <= 1 {
                    Ok(JobResult::RetryAfter(Duration::ZERO))
                } else {
                    Ok(JobResult::Completed)
                }
            }
        })
        .promote_interval(Duration::from_millis(100))
        .heartbeat_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .build()
        .expect("Failed to build client");
    client.start().await.expect("Failed to start client");

    wait_for_job_state(&pool, complete_job.id, "completed").await;
    wait_for_job_state(&pool, retry_job.id, "completed").await;
    client.shutdown(Duration::from_secs(5)).await;
    provider.force_flush().expect("flush spans");

    // ── Producer trace in Tempo: enqueue → send → execute, one trace ──
    let http = reqwest::Client::new();
    // Expect at least: e2e.enqueue + 2× send + first-attempt executes.
    let spans = wait_for_tempo_trace(&http, &producer_trace_hex, 4).await;

    let send_b64 = hex_to_base64(&complete_site.span_id().to_string());
    let send_span = spans
        .iter()
        .find(|s| s["spanId"].as_str() == Some(send_b64.as_str()))
        .unwrap_or_else(|| panic!("enqueue-site span missing from Tempo trace: {spans:?}"));
    assert_eq!(
        send_span["name"].as_str(),
        Some(format!("send {queue}").as_str())
    );

    let execute_span = spans
        .iter()
        .find(|s| {
            s["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("job.execute"))
                && s["parentSpanId"].as_str() == Some(send_b64.as_str())
        })
        .unwrap_or_else(|| {
            panic!("job.execute span parented to the enqueue site missing: {spans:?}")
        });
    assert_eq!(execute_span["kind"].as_str(), Some("SPAN_KIND_CONSUMER"));
    assert_eq!(span_attr(execute_span, "messaging.system"), Some("awa"));
    assert_eq!(
        span_attr(execute_span, "messaging.destination.name"),
        Some(queue)
    );

    // ── Retry attempt: fresh root trace, span link back ──
    // Find the retry's second-attempt trace via TraceQL on the job id.
    let retry_site_trace_hex = retry_site.trace_id().to_string();
    let retry_site_span_b64 = hex_to_base64(&retry_site.span_id().to_string());
    let query = format!("{{ span.messaging.message.id = \"{}\" }}", retry_job.id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let retry_trace_hex = loop {
        let resp: serde_json::Value = http
            .get(format!("{}/api/search", tempo_url()))
            .query(&[("q", query.as_str()), ("limit", "20")])
            .send()
            .await
            .expect("tempo search request")
            .json()
            .await
            .unwrap_or_default();
        let other = resp["traces"].as_array().and_then(|traces| {
            traces.iter().find_map(|t| {
                let id = t["traceID"].as_str()?;
                // Tempo may render the id without leading zeros; compare
                // against the tail of the padded producer id.
                (!retry_site_trace_hex.ends_with(id)).then(|| id.to_string())
            })
        });
        if let Some(id) = other {
            break id;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retry attempt trace not searchable in Tempo within 60s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    let retry_spans = wait_for_tempo_trace(&http, &retry_trace_hex, 1).await;
    let retry_execute = retry_spans
        .iter()
        .find(|s| {
            s["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("job.execute"))
        })
        .unwrap_or_else(|| panic!("retry execute span missing: {retry_spans:?}"));
    assert!(
        retry_execute["parentSpanId"]
            .as_str()
            .unwrap_or("")
            .is_empty(),
        "retry execute span should be a root span"
    );
    let links = retry_execute["links"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        links
            .iter()
            .any(|l| l["spanId"].as_str() == Some(retry_site_span_b64.as_str())),
        "retry execute span should link back to the enqueue site; links: {links:?}"
    );

    let _ = attempts;
    eprintln!(
        "trace e2e OK: producer trace {producer_trace_hex} and retry trace {retry_trace_hex} both queryable in Tempo"
    );
}
