//! OpenTelemetry metrics for the Awa worker runtime.
//!
//! Emits standard OTel metrics for job processing observability.
//! Metrics are published via the global OTel meter provider — callers
//! configure their exporter (Prometheus, OTLP, etc.) before starting the client.
//!
//! All metrics use the `awa` meter name and follow OTel semantic conventions.

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use std::time::Duration;

/// Awa worker metrics backed by OpenTelemetry.
#[derive(Clone)]
pub struct AwaMetrics {
    /// Total jobs inserted.
    pub jobs_inserted: Counter<u64>,
    /// Total jobs completed successfully.
    pub jobs_completed: Counter<u64>,
    /// Total jobs that failed (terminal).
    pub jobs_failed: Counter<u64>,
    /// Total jobs marked retryable.
    pub jobs_retried: Counter<u64>,
    /// Total jobs cancelled.
    pub jobs_cancelled: Counter<u64>,
    /// Total jobs claimed (dequeued) for execution.
    pub jobs_claimed: Counter<u64>,
    /// Job execution duration in seconds.
    pub job_duration_seconds: Histogram<f64>,
    /// Current in-flight jobs (gauge — can go up and down).
    pub jobs_in_flight: UpDownCounter<i64>,
    /// Total heartbeat batches sent.
    pub heartbeat_batches: Counter<u64>,
    /// Total maintenance rescue operations.
    pub maintenance_rescues: Counter<u64>,
    /// Total jobs parked for external callback.
    pub jobs_waiting_external: Counter<u64>,
}

impl AwaMetrics {
    /// Create metrics from an OpenTelemetry meter.
    pub fn new(meter: &Meter) -> Self {
        Self {
            jobs_inserted: meter
                .u64_counter("awa.jobs.inserted")
                .with_description("Total jobs inserted")
                .build(),
            jobs_completed: meter
                .u64_counter("awa.jobs.completed")
                .with_description("Total jobs completed successfully")
                .build(),
            jobs_failed: meter
                .u64_counter("awa.jobs.failed")
                .with_description("Total jobs that failed terminally")
                .build(),
            jobs_retried: meter
                .u64_counter("awa.jobs.retried")
                .with_description("Total jobs marked retryable")
                .build(),
            jobs_cancelled: meter
                .u64_counter("awa.jobs.cancelled")
                .with_description("Total jobs cancelled")
                .build(),
            jobs_claimed: meter
                .u64_counter("awa.jobs.claimed")
                .with_description("Total jobs claimed for execution")
                .build(),
            job_duration_seconds: meter
                .f64_histogram("awa.jobs.duration")
                .with_description("Job execution duration in seconds")
                .with_unit("s")
                .build(),
            jobs_in_flight: meter
                .i64_up_down_counter("awa.jobs.in_flight")
                .with_description("Current number of in-flight jobs")
                .build(),
            heartbeat_batches: meter
                .u64_counter("awa.heartbeat.batches")
                .with_description("Total heartbeat batch updates sent")
                .build(),
            maintenance_rescues: meter
                .u64_counter("awa.maintenance.rescues")
                .with_description("Total jobs rescued by maintenance (stale heartbeat + deadline)")
                .build(),
            jobs_waiting_external: meter
                .u64_counter("awa.jobs.waiting_external")
                .with_description("Total jobs parked for external callback")
                .build(),
        }
    }

    /// Create metrics using the global OTel meter provider with meter name "awa".
    pub fn from_global() -> Self {
        let meter = opentelemetry::global::meter("awa");
        Self::new(&meter)
    }

    /// Record a job completion with duration and attributes.
    pub fn record_job_completed(&self, kind: &str, queue: &str, duration: Duration) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.kind", kind.to_string()),
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
        ];
        self.jobs_completed.add(1, &attrs);
        self.job_duration_seconds
            .record(duration.as_secs_f64(), &attrs);
    }

    /// Record a job failure.
    pub fn record_job_failed(&self, kind: &str, queue: &str, terminal: bool) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.kind", kind.to_string()),
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.job.terminal", terminal),
        ];
        self.jobs_failed.add(1, &attrs);
    }

    /// Record a job retry.
    pub fn record_job_retried(&self, kind: &str, queue: &str) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.kind", kind.to_string()),
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
        ];
        self.jobs_retried.add(1, &attrs);
    }

    /// Record a job claimed from queue.
    pub fn record_job_claimed(&self, queue: &str, batch_size: u64) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        self.jobs_claimed.add(batch_size, &attrs);
    }

    /// Record in-flight change.
    pub fn record_in_flight_change(&self, queue: &str, delta: i64) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        self.jobs_in_flight.add(delta, &attrs);
    }
}

/// No-op metrics for when OTel is not configured.
impl Default for AwaMetrics {
    fn default() -> Self {
        Self::from_global()
    }
}
