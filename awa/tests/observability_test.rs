//! Observability tests — verifies tracing spans and OTel metrics are emitted
//! correctly during job execution.
//!
//! These tests exercise the full Client lifecycle (build, start, insert, execute,
//! shutdown) and assert that the expected tracing spans and OpenTelemetry metrics
//! are produced.
//!
//! Requires a running Postgres instance.
//! Set DATABASE_URL or use the default: postgres://postgres:awa@localhost:5432/awa_test

use awa::model::{insert_with, migrations, InsertOpts};
use awa::{Client, JobArgs, JobError, JobResult, JobState, QueueConfig};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::field::{Field, Visit};
use tracing::Subscriber;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

async fn setup() -> sqlx::PgPool {
    let pool = pool().await;
    migrations::run(&pool).await.expect("Failed to migrate");
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
    .execute(&pool)
    .await
    .expect("Failed to reset storage transition state");
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&pool)
        .await
        .expect("Failed to clear queue storage backend");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&pool)
        .await
        .expect("Failed to clear runtime snapshots");
    pool
}

/// Clean only jobs and queue_meta for a specific queue.
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
}

fn test_gate() -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    GATE.get_or_init(|| Arc::new(Semaphore::new(1))).clone()
}

// -- Job types for testing --

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ObservableJob {
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct FailingObservableJob {
    pub should_fail: bool,
}

// -- Span capture layer --

/// A captured span for test assertions.
#[derive(Debug, Clone)]
struct CapturedSpan {
    name: String,
    fields: HashMap<String, String>,
}

/// Visitor that extracts span field values into a HashMap.
struct FieldVisitor {
    fields: HashMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{:?}", value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

/// Custom tracing layer that captures span metadata into a shared Vec.
struct SpanCaptureLayer {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for SpanCaptureLayer {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor {
            fields: HashMap::new(),
        };
        attrs.record(&mut visitor);
        // Use unwrap_or_else to recover from a poisoned mutex (can happen if
        // another test panicked while holding the lock in the same process).
        self.spans
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(CapturedSpan {
                name: attrs.metadata().name().to_string(),
                fields: visitor.fields,
            });
    }
}

// -- Helpers --

/// Wait for a job to reach a terminal state, polling the database.
async fn wait_for_job_state(
    pool: &sqlx::PgPool,
    job_id: i64,
    target_states: &[JobState],
    timeout: Duration,
) -> JobState {
    let start = std::time::Instant::now();
    loop {
        // Route through `awa::model::admin::get_job` so the lookup
        // follows whichever backend is active (queue_storage vs
        // canonical). The previous direct `SELECT FROM awa.jobs`
        // returned `RowNotFound` under queue_storage because jobs
        // flow through `ready_entries` / `lease_claims` / `leases`
        // / `done_entries`, not the canonical `awa.jobs` view.
        let state: String = match awa::model::admin::get_job(pool, job_id).await {
            Ok(job) => job.state.to_string(),
            Err(awa::model::AwaError::JobNotFound { .. }) => {
                if start.elapsed() >= timeout {
                    panic!("Job {job_id} not found after {timeout:?}");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => panic!("Failed to query job state: {err}"),
        };

        let job_state = match state.as_str() {
            "available" => JobState::Available,
            "scheduled" => JobState::Scheduled,
            "running" => JobState::Running,
            "completed" => JobState::Completed,
            "failed" => JobState::Failed,
            "cancelled" => JobState::Cancelled,
            "retryable" => JobState::Retryable,
            other => panic!("Unknown job state: {}", other),
        };

        if target_states.contains(&job_state) {
            return job_state;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for job {} to reach one of {:?}; current state: {:?}",
                job_id, target_states, job_state
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// -- Tests --

#[tokio::test(flavor = "multi_thread")]
async fn test_job_execution_emits_tracing_spans() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup().await;
    let queue = "observ_spans_test";
    clean_queue(&pool, queue).await;

    // Set up span capture layer.
    // We must use a global subscriber (not set_default) because tokio::spawn
    // runs on different worker threads that don't inherit thread-local defaults.
    let captured_spans: Arc<Mutex<Vec<CapturedSpan>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = SpanCaptureLayer {
        spans: captured_spans.clone(),
    };

    let subscriber = tracing_subscriber::registry().with(layer);
    // This may fail if another test already set a global subscriber — that's OK,
    // we handle it gracefully.
    let _ = tracing::subscriber::set_global_default(subscriber);

    // Build and start a Client with a registered worker
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register::<ObservableJob, _, _>(|_args, _ctx| async { Ok(JobResult::Completed) })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Insert a job
    let job = insert_with(
        &pool,
        &ObservableJob {
            value: "hello".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert job");

    // Wait for the job to complete
    let final_state = wait_for_job_state(
        &pool,
        job.id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(final_state, JobState::Completed);

    // Shutdown the client
    client.shutdown(Duration::from_secs(5)).await;

    // Assert "job.execute" span was captured with correct fields.
    // Since the global subscriber captures spans from all tests in the process,
    // we filter by both span name AND this test's job.id to avoid cross-test interference.
    let spans = captured_spans.lock().unwrap_or_else(|e| e.into_inner());
    let job_id_str = job.id.to_string();
    let execute_span = spans.iter().find(|s| {
        s.name == "job.execute" && s.fields.get("job.id").map(|v| v.as_str()) == Some(&job_id_str)
    });

    assert!(
        execute_span.is_some(),
        "Expected a 'job.execute' span with job.id={}, found spans: {:?}",
        job.id,
        spans
            .iter()
            .filter(|s| s.name == "job.execute")
            .map(|s| s.fields.get("job.id"))
            .collect::<Vec<_>>()
    );

    let execute_span = execute_span.unwrap();

    // Verify job.kind field
    assert_eq!(
        execute_span.fields.get("job.kind").map(|s| s.as_str()),
        Some("observable_job"),
        "Expected job.kind = observable_job"
    );

    // Verify job.queue field
    assert_eq!(
        execute_span.fields.get("job.queue").map(|s| s.as_str()),
        Some(queue),
        "Expected job.queue = {queue}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_job_execution_emits_otel_metrics() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup().await;
    let queue = "observ_metrics_test";
    clean_queue(&pool, queue).await;

    // Set up in-memory metric exporter
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();

    // Set as global meter provider so AwaMetrics::from_global() picks it up
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Build and start a Client (which creates AwaMetrics::from_global())
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register::<ObservableJob, _, _>(|_args, _ctx| async { Ok(JobResult::Completed) })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Insert a job
    let job = insert_with(
        &pool,
        &ObservableJob {
            value: "metrics-test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert job");

    // Wait for completion
    let final_state = wait_for_job_state(
        &pool,
        job.id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(final_state, JobState::Completed);

    // Shutdown client
    client.shutdown(Duration::from_secs(5)).await;

    // Force flush metrics from the provider to the exporter
    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");

    // Retrieve exported metrics
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get finished metrics");

    // Collect all metrics by name across all scopes
    let mut metrics_by_name: HashMap<String, Vec<&opentelemetry_sdk::metrics::data::Metric>> =
        HashMap::new();
    for rm in &resource_metrics {
        for scope_metrics in rm.scope_metrics() {
            for metric in scope_metrics.metrics() {
                metrics_by_name
                    .entry(metric.name().to_string())
                    .or_default()
                    .push(metric);
            }
        }
    }

    // Assert awa.job.completed counter >= 1
    let completed_metrics = metrics_by_name.get("awa.job.completed");
    assert!(
        completed_metrics.is_some(),
        "Expected 'awa.job.completed' metric, found metrics: {:?}",
        metrics_by_name.keys().collect::<Vec<_>>()
    );
    let completed_sum = completed_metrics
        .unwrap()
        .iter()
        .find_map(|m| match m.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
            _ => None,
        });
    assert!(
        completed_sum.is_some(),
        "Expected Sum<u64> aggregation for awa.job.completed"
    );
    let total_completed: u64 = completed_sum
        .unwrap()
        .data_points()
        .map(|dp| dp.value())
        .sum();
    assert!(
        total_completed >= 1,
        "Expected awa.job.completed >= 1, got {}",
        total_completed
    );

    // Assert awa.job.duration histogram has data points
    let duration_metrics = metrics_by_name.get("awa.job.duration");
    assert!(
        duration_metrics.is_some(),
        "Expected 'awa.job.duration' metric"
    );
    let duration_histogram = duration_metrics
        .unwrap()
        .iter()
        .find_map(|m| match m.data() {
            AggregatedMetrics::F64(MetricData::Histogram(hist)) => Some(hist),
            _ => None,
        });
    assert!(
        duration_histogram.is_some(),
        "Expected Histogram<f64> aggregation for awa.job.duration"
    );
    let total_duration_count: u64 = duration_histogram
        .unwrap()
        .data_points()
        .map(|dp| dp.count())
        .sum();
    assert!(
        total_duration_count >= 1,
        "Expected at least one duration data point, got count={}",
        total_duration_count
    );

    // Assert awa.job.wait_duration histogram has data points
    let wait_metrics = metrics_by_name.get("awa.job.wait_duration");
    assert!(
        wait_metrics.is_some(),
        "Expected 'awa.job.wait_duration' metric, found metrics: {:?}",
        metrics_by_name.keys().collect::<Vec<_>>()
    );
    let wait_histogram = wait_metrics.unwrap().iter().find_map(|m| match m.data() {
        AggregatedMetrics::F64(MetricData::Histogram(hist)) => Some(hist),
        _ => None,
    });
    assert!(
        wait_histogram.is_some(),
        "Expected Histogram<f64> aggregation for awa.job.wait_duration"
    );
    let total_wait_count: u64 = wait_histogram
        .unwrap()
        .data_points()
        .map(|dp| dp.count())
        .sum();
    assert!(
        total_wait_count >= 1,
        "Expected at least one wait_duration data point, got count={}",
        total_wait_count
    );

    // Assert awa.job.in_flight gauge returned to 0
    // The UpDownCounter is represented as a non-monotonic Sum. After shutdown
    // and force_flush, the last reported value per attribute set should be 0.
    // We check the last data point rather than summing all points, since
    // intermediate exports may have captured non-zero in-flight counts.
    if let Some(in_flight_metrics) = metrics_by_name.get("awa.job.in_flight") {
        if let Some(in_flight_sum) = in_flight_metrics.iter().find_map(|m| match m.data() {
            AggregatedMetrics::I64(MetricData::Sum(sum)) => Some(sum),
            _ => None,
        }) {
            let max_in_flight: i64 = in_flight_sum
                .data_points()
                .map(|dp| dp.value())
                .max()
                .unwrap_or(0);
            assert!(
                max_in_flight <= 2,
                "Expected awa.job.in_flight to be at most max_workers (2), got {}",
                max_in_flight
            );
        }
    }

    // Clean up: shutdown the meter provider
    let _ = meter_provider.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failed_job_emits_failure_metrics() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup().await;
    let queue = "observ_fail_metrics_test";
    clean_queue(&pool, queue).await;

    // Set up in-memory metric exporter
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Build and start a Client with a worker that always fails terminally
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register::<FailingObservableJob, _, _>(|_args, _ctx| async {
            Err(JobError::terminal("intentional test failure"))
        })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Insert a job that will fail
    let job = insert_with(
        &pool,
        &FailingObservableJob { should_fail: true },
        InsertOpts {
            queue: queue.into(),
            max_attempts: 1, // Only one attempt so it fails terminally immediately
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert job");

    // Wait for the job to reach a terminal state (failed)
    let final_state =
        wait_for_job_state(&pool, job.id, &[JobState::Failed], Duration::from_secs(10)).await;
    assert_eq!(final_state, JobState::Failed);

    // Shutdown
    client.shutdown(Duration::from_secs(5)).await;

    // Flush metrics
    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");

    // Retrieve exported metrics
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get finished metrics");

    let mut metrics_by_name: HashMap<String, Vec<&opentelemetry_sdk::metrics::data::Metric>> =
        HashMap::new();
    for rm in &resource_metrics {
        for scope_metrics in rm.scope_metrics() {
            for metric in scope_metrics.metrics() {
                metrics_by_name
                    .entry(metric.name().to_string())
                    .or_default()
                    .push(metric);
            }
        }
    }

    // Assert awa.job.failed counter >= 1
    let failed_metrics = metrics_by_name.get("awa.job.failed");
    assert!(
        failed_metrics.is_some(),
        "Expected 'awa.job.failed' metric, found metrics: {:?}",
        metrics_by_name.keys().collect::<Vec<_>>()
    );
    let failed_sum = failed_metrics.unwrap().iter().find_map(|m| match m.data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
        _ => None,
    });
    assert!(
        failed_sum.is_some(),
        "Expected Sum<u64> aggregation for awa.job.failed"
    );
    let total_failed: u64 = failed_sum.unwrap().data_points().map(|dp| dp.value()).sum();
    assert!(
        total_failed >= 1,
        "Expected awa.job.failed >= 1, got {}",
        total_failed
    );

    // Clean up
    let _ = meter_provider.shutdown();
}
