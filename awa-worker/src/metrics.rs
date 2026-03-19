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
    /// Number of dispatcher claim queries executed.
    pub claim_batches: Counter<u64>,
    /// Claim batch size in jobs.
    pub claim_batch_size: Histogram<u64>,
    /// Claim query latency in seconds.
    pub claim_duration_seconds: Histogram<f64>,
    /// Job execution duration in seconds.
    pub job_duration_seconds: Histogram<f64>,
    /// Number of completion batch flushes executed.
    pub completion_flushes: Counter<u64>,
    /// Completion flush batch size in jobs.
    pub completion_flush_batch_size: Histogram<u64>,
    /// Completion flush latency in seconds.
    pub completion_flush_duration_seconds: Histogram<f64>,
    /// Number of scheduled/retryable promotion batches executed.
    pub promotion_batches: Counter<u64>,
    /// Promotion batch size in jobs.
    pub promotion_batch_size: Histogram<u64>,
    /// Promotion query latency in seconds.
    pub promotion_duration_seconds: Histogram<f64>,
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
            claim_batches: meter
                .u64_counter("awa.dispatch.claim_batches")
                .with_description("Total dispatcher claim queries executed")
                .build(),
            claim_batch_size: meter
                .u64_histogram("awa.dispatch.claim_batch_size")
                .with_description("Dispatcher claim batch size in jobs")
                .build(),
            claim_duration_seconds: meter
                .f64_histogram("awa.dispatch.claim_duration")
                .with_description("Dispatcher claim query duration in seconds")
                .with_unit("s")
                .build(),
            job_duration_seconds: meter
                .f64_histogram("awa.jobs.duration")
                .with_description("Job execution duration in seconds")
                .with_unit("s")
                .build(),
            completion_flushes: meter
                .u64_counter("awa.completion.flushes")
                .with_description("Total completion batch flushes")
                .build(),
            completion_flush_batch_size: meter
                .u64_histogram("awa.completion.flush_batch_size")
                .with_description("Completion batch flush size in jobs")
                .build(),
            completion_flush_duration_seconds: meter
                .f64_histogram("awa.completion.flush_duration")
                .with_description("Completion batch flush duration in seconds")
                .with_unit("s")
                .build(),
            promotion_batches: meter
                .u64_counter("awa.maintenance.promote_batches")
                .with_description("Total scheduled/retryable promotion batches")
                .build(),
            promotion_batch_size: meter
                .u64_histogram("awa.maintenance.promote_batch_size")
                .with_description("Promotion batch size in jobs")
                .build(),
            promotion_duration_seconds: meter
                .f64_histogram("awa.maintenance.promote_duration")
                .with_description("Promotion batch duration in seconds")
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

    /// Record a dispatcher claim query batch and its latency.
    pub fn record_claim_batch(&self, queue: &str, batch_size: u64, duration: Duration) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        self.claim_batches.add(1, &attrs);
        self.claim_batch_size.record(batch_size, &attrs);
        self.claim_duration_seconds
            .record(duration.as_secs_f64(), &attrs);
    }

    /// Record a completion batch flush.
    pub fn record_completion_flush(&self, shard: usize, batch_size: u64, duration: Duration) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.completion.shard",
            shard as i64,
        )];
        self.completion_flushes.add(1, &attrs);
        self.completion_flush_batch_size.record(batch_size, &attrs);
        self.completion_flush_duration_seconds
            .record(duration.as_secs_f64(), &attrs);
    }

    /// Record a scheduled/retryable promotion batch.
    pub fn record_promotion_batch(&self, state: &str, batch_size: u64, duration: Duration) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.state",
            state.to_string(),
        )];
        self.promotion_batches.add(1, &attrs);
        self.promotion_batch_size.record(batch_size, &attrs);
        self.promotion_duration_seconds
            .record(duration.as_secs_f64(), &attrs);
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
