//! OpenTelemetry metrics for the Awa worker runtime.
//!
//! Emits standard OTel metrics for job processing observability.
//! Metrics are published via the global OTel meter provider — callers
//! configure their exporter (Prometheus, OTLP, etc.) before starting the client.
//!
//! All metrics use the `awa` meter name and follow OTel semantic conventions:
//! - Dot-separated hierarchical namespaces (`awa.job.*`, `awa.dispatch.*`)
//! - Singular nouns for namespaces (not pluralized)
//! - Units declared via `.with_unit()` using UCUM notation
//! - No unit suffix in metric names (exporters append automatically)

use awa_model::storage::StorageStatus;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
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
    /// Number of dispatcher wake-ups by reason.
    pub dispatch_wakeups: Counter<u64>,
    /// Time from wake-up to the first claim attempt.
    pub dispatch_wake_to_claim_seconds: Histogram<f64>,
    /// Number of permits available when a dispatcher wake is processed.
    pub dispatch_capacity_available: Histogram<u64>,
    /// Number of wakes that found no jobs despite available capacity.
    pub dispatch_empty_claims: Counter<u64>,
    /// Number of pre-acquired permits released unused after a claim round.
    pub dispatch_unused_permits: Counter<u64>,
    /// Number of wakes that were blocked by rate limiting.
    pub dispatch_rate_limited: Counter<u64>,
    /// Claim batch size distribution.
    pub claim_batch_size: Histogram<u64>,
    /// Claim query duration.
    pub claim_duration_seconds: Histogram<f64>,
    /// Job execution duration.
    pub job_duration_seconds: Histogram<f64>,
    /// Number of completion batch flushes executed.
    pub completion_flushes: Counter<u64>,
    /// Completion flush batch size distribution.
    pub completion_flush_batch_size: Histogram<u64>,
    /// Completion flush duration.
    pub completion_flush_duration_seconds: Histogram<f64>,
    /// Number of scheduled/retryable promotion batches executed.
    pub promotion_batches: Counter<u64>,
    /// Promotion batch size distribution.
    pub promotion_batch_size: Histogram<u64>,
    /// Promotion query duration.
    pub promotion_duration_seconds: Histogram<f64>,
    /// Current in-flight jobs (can go up and down).
    pub jobs_in_flight: UpDownCounter<i64>,
    /// Total heartbeat batches sent.
    pub heartbeat_batches: Counter<u64>,
    /// Total maintenance rescue operations.
    pub maintenance_rescues: Counter<u64>,
    /// Total jobs parked for external callback.
    pub jobs_waiting_external: Counter<u64>,
    /// Current queue depth per state — how many jobs are in each state per queue.
    pub queue_depth: Gauge<i64>,
    /// Queue lag — age of the oldest available job per queue.
    pub queue_lag_seconds: Gauge<f64>,
    /// Time from job creation to claim — the user-visible queuing latency.
    pub wait_duration_seconds: Histogram<f64>,
    /// Total jobs moved into the Dead Letter Queue.
    pub dlq_moved: Counter<u64>,
    /// Total jobs retried out of the Dead Letter Queue.
    pub dlq_retried: Counter<u64>,
    /// Total DLQ rows purged.
    pub dlq_purged: Counter<u64>,
    /// Current DLQ depth per queue.
    pub dlq_depth: Gauge<i64>,
    /// Info gauge for declared queue descriptors — value is always 1, the
    /// useful payload is the attribute set (display_name, owner, tags).
    /// Dashboards join it into throughput / latency panels with a
    /// `* on(awa_job_queue) group_left(awa_queue_display_name, awa_queue_owner)`
    /// Prometheus expression, which keeps descriptor fields out of the
    /// high-cardinality per-metric label set.
    pub queue_info: Gauge<i64>,
    /// Info gauge for declared job-kind descriptors. Same pattern as
    /// [`queue_info`][Self::queue_info].
    pub job_kind_info: Gauge<i64>,
    /// Readiness gauge for storage transition actions such as
    /// `enter_mixed_transition` and `finalize` (1 = ready, 0 = blocked).
    pub storage_transition_ready: Gauge<i64>,
    /// Current canonical live backlog observed by queue-storage-capable runtimes.
    pub storage_canonical_live_backlog: Gauge<i64>,
    /// Current live runtime count per reported storage capability.
    pub storage_live_runtime_capability: Gauge<i64>,
    /// One-hot info gauge for the current storage transition state and engines.
    pub storage_state: Gauge<i64>,
}

impl AwaMetrics {
    /// Create metrics from an OpenTelemetry meter.
    pub fn new(meter: &Meter) -> Self {
        Self {
            jobs_inserted: meter
                .u64_counter("awa.job.inserted")
                .with_description("Number of jobs inserted")
                .with_unit("{job}")
                .build(),
            jobs_completed: meter
                .u64_counter("awa.job.completed")
                .with_description("Number of jobs completed successfully")
                .with_unit("{job}")
                .build(),
            jobs_failed: meter
                .u64_counter("awa.job.failed")
                .with_description("Number of jobs that failed terminally")
                .with_unit("{job}")
                .build(),
            jobs_retried: meter
                .u64_counter("awa.job.retried")
                .with_description("Number of jobs marked retryable")
                .with_unit("{job}")
                .build(),
            jobs_cancelled: meter
                .u64_counter("awa.job.cancelled")
                .with_description("Number of jobs cancelled")
                .with_unit("{job}")
                .build(),
            jobs_claimed: meter
                .u64_counter("awa.job.claimed")
                .with_description("Number of jobs claimed for execution")
                .with_unit("{job}")
                .build(),
            claim_batches: meter
                .u64_counter("awa.dispatch.claim_batches")
                .with_description("Number of dispatcher claim queries executed")
                .with_unit("{batch}")
                .build(),
            dispatch_wakeups: meter
                .u64_counter("awa.dispatch.wakeups")
                .with_description("Number of dispatcher wake-ups by reason")
                .with_unit("{wake}")
                .build(),
            dispatch_wake_to_claim_seconds: meter
                .f64_histogram("awa.dispatch.wake_to_claim_duration")
                .with_description("Time from dispatcher wake-up to first claim attempt")
                .with_unit("s")
                .build(),
            dispatch_capacity_available: meter
                .u64_histogram("awa.dispatch.capacity_available")
                .with_description("Number of permits available when a dispatcher wake is processed")
                .with_unit("{permit}")
                .build(),
            dispatch_empty_claims: meter
                .u64_counter("awa.dispatch.empty_claims")
                .with_description("Number of dispatcher wakes that found no jobs despite available capacity")
                .with_unit("{wake}")
                .build(),
            dispatch_unused_permits: meter
                .u64_counter("awa.dispatch.unused_permits")
                .with_description("Number of pre-acquired permits released unused after claiming fewer jobs than capacity")
                .with_unit("{permit}")
                .build(),
            dispatch_rate_limited: meter
                .u64_counter("awa.dispatch.rate_limited")
                .with_description("Number of dispatcher wakes that could not claim because of rate limiting")
                .with_unit("{wake}")
                .build(),
            claim_batch_size: meter
                .u64_histogram("awa.dispatch.claim_batch_size")
                .with_description("Dispatcher claim batch size")
                .with_unit("{job}")
                .build(),
            claim_duration_seconds: meter
                .f64_histogram("awa.dispatch.claim_duration")
                .with_description("Dispatcher claim query duration")
                .with_unit("s")
                .build(),
            job_duration_seconds: meter
                .f64_histogram("awa.job.duration")
                .with_description("Job execution duration")
                .with_unit("s")
                .build(),
            completion_flushes: meter
                .u64_counter("awa.completion.flushes")
                .with_description("Number of completion batch flushes")
                .with_unit("{batch}")
                .build(),
            completion_flush_batch_size: meter
                .u64_histogram("awa.completion.flush_batch_size")
                .with_description("Completion batch flush size")
                .with_unit("{job}")
                .build(),
            completion_flush_duration_seconds: meter
                .f64_histogram("awa.completion.flush_duration")
                .with_description("Completion batch flush duration")
                .with_unit("s")
                .build(),
            promotion_batches: meter
                .u64_counter("awa.maintenance.promote_batches")
                .with_description("Number of scheduled/retryable promotion batches")
                .with_unit("{batch}")
                .build(),
            promotion_batch_size: meter
                .u64_histogram("awa.maintenance.promote_batch_size")
                .with_description("Promotion batch size")
                .with_unit("{job}")
                .build(),
            promotion_duration_seconds: meter
                .f64_histogram("awa.maintenance.promote_duration")
                .with_description("Promotion batch duration")
                .with_unit("s")
                .build(),
            jobs_in_flight: meter
                .i64_up_down_counter("awa.job.in_flight")
                .with_description("Current number of in-flight jobs")
                .with_unit("{job}")
                .build(),
            heartbeat_batches: meter
                .u64_counter("awa.heartbeat.batches")
                .with_description("Number of heartbeat batch updates sent")
                .with_unit("{batch}")
                .build(),
            maintenance_rescues: meter
                .u64_counter("awa.maintenance.rescues")
                .with_description("Number of jobs rescued by maintenance")
                .with_unit("{job}")
                .build(),
            jobs_waiting_external: meter
                .u64_counter("awa.job.waiting_external")
                .with_description("Number of jobs parked for external callback")
                .with_unit("{job}")
                .build(),
            queue_depth: meter
                .i64_gauge("awa.queue.depth")
                .with_description("Current number of jobs per queue and state")
                .with_unit("{job}")
                .build(),
            queue_lag_seconds: meter
                .f64_gauge("awa.queue.lag")
                .with_description("Age of the oldest available job per queue")
                .with_unit("s")
                .build(),
            wait_duration_seconds: meter
                .f64_histogram("awa.job.wait_duration")
                .with_description("Time from job creation to claim")
                .with_unit("s")
                .build(),
            dlq_moved: meter
                .u64_counter("awa.job.dlq_moved")
                .with_description("Number of jobs moved into the Dead Letter Queue")
                .with_unit("{job}")
                .build(),
            dlq_retried: meter
                .u64_counter("awa.job.dlq_retried")
                .with_description("Number of jobs retried out of the Dead Letter Queue")
                .with_unit("{job}")
                .build(),
            dlq_purged: meter
                .u64_counter("awa.job.dlq_purged")
                .with_description("Number of DLQ rows deleted")
                .with_unit("{job}")
                .build(),
            dlq_depth: meter
                .i64_gauge("awa.job.dlq_depth")
                .with_description("Current Dead Letter Queue depth per queue")
                .with_unit("{job}")
                .build(),
            queue_info: meter
                .i64_gauge("awa.queue.info")
                .with_description(
                    "Declared queue descriptors (always 1; use as a label-join target)",
                )
                .with_unit("{queue}")
                .build(),
            job_kind_info: meter
                .i64_gauge("awa.job_kind.info")
                .with_description(
                    "Declared job-kind descriptors (always 1; use as a label-join target)",
                )
                .with_unit("{kind}")
                .build(),
            storage_transition_ready: meter
                .i64_gauge("awa.storage.transition_ready")
                .with_description("Storage transition readiness by action (1 = ready, 0 = blocked)")
                .with_unit("{state}")
                .build(),
            storage_canonical_live_backlog: meter
                .i64_gauge("awa.storage.canonical_live_backlog")
                .with_description("Current canonical live backlog during a storage transition")
                .with_unit("{job}")
                .build(),
            storage_live_runtime_capability: meter
                .i64_gauge("awa.storage.live_runtime_capability")
                .with_description("Current live runtime count by reported storage capability")
                .with_unit("{runtime}")
                .build(),
            storage_state: meter
                .i64_gauge("awa.storage.state")
                .with_description(
                    "Current storage transition state and engine combination (always 1)",
                )
                .with_unit("{state}")
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

    /// Record a dispatcher wake-up reason.
    pub fn record_dispatch_wake(&self, queue: &str, reason: &str) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.dispatch.reason", reason.to_string()),
        ];
        self.dispatch_wakeups.add(1, &attrs);
    }

    /// Record time from wake-up to the first claim attempt.
    pub fn record_dispatch_wake_to_claim(&self, queue: &str, reason: &str, duration: Duration) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.dispatch.reason", reason.to_string()),
        ];
        self.dispatch_wake_to_claim_seconds
            .record(duration.as_secs_f64(), &attrs);
    }

    /// Record how many permits were available on a dispatcher wake.
    pub fn record_dispatch_capacity_available(&self, queue: &str, reason: &str, permits: u64) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.dispatch.reason", reason.to_string()),
        ];
        self.dispatch_capacity_available.record(permits, &attrs);
    }

    /// Record a dispatcher wake that found no jobs.
    pub fn record_dispatch_empty_claim(&self, queue: &str, reason: &str) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.dispatch.reason", reason.to_string()),
        ];
        self.dispatch_empty_claims.add(1, &attrs);
    }

    /// Record permits released unused after a claim round.
    pub fn record_dispatch_unused_permits(&self, queue: &str, count: u64) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        self.dispatch_unused_permits.add(count, &attrs);
    }

    /// Record a wake that could not claim because of rate limiting.
    pub fn record_dispatch_rate_limited(&self, queue: &str, reason: &str) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.dispatch.reason", reason.to_string()),
        ];
        self.dispatch_rate_limited.add(1, &attrs);
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

    /// Record queue depth for a specific state.
    pub fn record_queue_depth(&self, queue: &str, state: &str, count: i64) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.job.state", state.to_string()),
        ];
        self.queue_depth.record(count, &attrs);
    }

    /// Record queue lag (age of oldest available job).
    pub fn record_queue_lag(&self, queue: &str, lag_seconds: f64) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        self.queue_lag_seconds.record(lag_seconds, &attrs);
    }

    /// Record job wait duration (time from creation to claim).
    pub fn record_wait_duration(&self, queue: &str, seconds: f64) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        self.wait_duration_seconds.record(seconds, &attrs);
    }

    /// Record a job moved into the DLQ.
    pub fn record_dlq_moved(&self, kind: &str, queue: &str, reason: &str) {
        let attrs = [
            opentelemetry::KeyValue::new("awa.job.kind", kind.to_string()),
            opentelemetry::KeyValue::new("awa.job.queue", queue.to_string()),
            opentelemetry::KeyValue::new("awa.dlq.reason", reason.to_string()),
        ];
        self.dlq_moved.add(1, &attrs);
    }

    /// Record a bulk admin move into the DLQ.
    pub fn record_dlq_moved_bulk(
        &self,
        kind: Option<&str>,
        queue: Option<&str>,
        reason: &str,
        count: u64,
    ) {
        if count == 0 {
            return;
        }

        let mut attrs = vec![opentelemetry::KeyValue::new(
            "awa.dlq.reason",
            reason.to_string(),
        )];
        if let Some(kind) = kind {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job.kind",
                kind.to_string(),
            ));
        }
        if let Some(queue) = queue {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job.queue",
                queue.to_string(),
            ));
        }
        self.dlq_moved.add(count, &attrs);
    }

    /// Record jobs retried out of the DLQ.
    pub fn record_dlq_retried(&self, queue: Option<&str>, count: u64) {
        let attrs: Vec<opentelemetry::KeyValue> = queue
            .map(|q| vec![opentelemetry::KeyValue::new("awa.job.queue", q.to_string())])
            .unwrap_or_default();
        self.dlq_retried.add(count, &attrs);
    }

    /// Record DLQ rows purged.
    pub fn record_dlq_purged(&self, queue: Option<&str>, count: u64) {
        let attrs: Vec<opentelemetry::KeyValue> = queue
            .map(|q| vec![opentelemetry::KeyValue::new("awa.job.queue", q.to_string())])
            .unwrap_or_default();
        self.dlq_purged.add(count, &attrs);
    }

    /// Record current DLQ depth for a queue.
    pub fn record_dlq_depth(&self, queue: &str, count: i64) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        self.dlq_depth.record(count, &attrs);
    }

    /// Emit the info gauge for a declared queue descriptor. Called once per
    /// descriptor on every runtime snapshot tick — constant value of 1 with
    /// the descriptor fields as attributes. Optional fields that are `None`
    /// are elided so we don't produce `display_name=""` series.
    pub fn record_queue_info(
        &self,
        queue: &str,
        display_name: Option<&str>,
        description: Option<&str>,
        owner: Option<&str>,
        docs_url: Option<&str>,
        tags: &[String],
    ) {
        let mut attrs = vec![opentelemetry::KeyValue::new(
            "awa.job.queue",
            queue.to_string(),
        )];
        if let Some(v) = display_name {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.queue.display_name",
                v.to_string(),
            ));
        }
        if let Some(v) = description {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.queue.description",
                v.to_string(),
            ));
        }
        if let Some(v) = owner {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.queue.owner",
                v.to_string(),
            ));
        }
        if let Some(v) = docs_url {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.queue.docs_url",
                v.to_string(),
            ));
        }
        if !tags.is_empty() {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.queue.tags",
                tags.join(","),
            ));
        }
        self.queue_info.record(1, &attrs);
    }

    /// Emit the info gauge for a declared job-kind descriptor. Same shape
    /// as [`record_queue_info`][Self::record_queue_info].
    pub fn record_job_kind_info(
        &self,
        kind: &str,
        display_name: Option<&str>,
        description: Option<&str>,
        owner: Option<&str>,
        docs_url: Option<&str>,
        tags: &[String],
    ) {
        let mut attrs = vec![opentelemetry::KeyValue::new(
            "awa.job.kind",
            kind.to_string(),
        )];
        if let Some(v) = display_name {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job_kind.display_name",
                v.to_string(),
            ));
        }
        if let Some(v) = description {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job_kind.description",
                v.to_string(),
            ));
        }
        if let Some(v) = owner {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job_kind.owner",
                v.to_string(),
            ));
        }
        if let Some(v) = docs_url {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job_kind.docs_url",
                v.to_string(),
            ));
        }
        if !tags.is_empty() {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job_kind.tags",
                tags.join(","),
            ));
        }
        self.job_kind_info.record(1, &attrs);
    }

    /// Record whether a storage transition action is currently ready.
    pub fn record_storage_transition_ready(&self, action: &str, ready: bool) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.storage.action",
            action.to_string(),
        )];
        self.storage_transition_ready
            .record(if ready { 1 } else { 0 }, &attrs);
    }

    /// Record canonical live backlog for the storage transition.
    pub fn record_storage_canonical_live_backlog(&self, count: i64) {
        self.storage_canonical_live_backlog.record(count, &[]);
    }

    /// Record the number of live runtimes reporting a given storage capability.
    pub fn record_storage_live_runtime_capability(&self, capability: &str, count: i64) {
        let attrs = [opentelemetry::KeyValue::new(
            "awa.storage.capability",
            capability.to_string(),
        )];
        self.storage_live_runtime_capability.record(count, &attrs);
    }

    /// Emit the current storage transition state as a one-hot info gauge.
    pub fn record_storage_state(&self, status: &StorageStatus) {
        let mut attrs = vec![
            opentelemetry::KeyValue::new("awa.storage.state", status.state.clone()),
            opentelemetry::KeyValue::new(
                "awa.storage.current_engine",
                status.current_engine.clone(),
            ),
            opentelemetry::KeyValue::new("awa.storage.active_engine", status.active_engine.clone()),
        ];
        if let Some(prepared_engine) = &status.prepared_engine {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.storage.prepared_engine",
                prepared_engine.clone(),
            ));
        }
        self.storage_state.record(1, &attrs);
    }
}

/// No-op metrics for when OTel is not configured.
impl Default for AwaMetrics {
    fn default() -> Self {
        Self::from_global()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The info gauges are no-op under a default (global) meter provider —
    /// this just confirms the method signatures build and don't panic when
    /// called with a realistic attribute mix. End-to-end OTLP export is
    /// covered by the telemetry integration test.
    #[test]
    fn record_queue_info_does_not_panic_on_mixed_attrs() {
        let metrics = AwaMetrics::from_global();
        metrics.record_queue_info(
            "emails",
            Some("Outbound email"),
            Some("Transactional mail"),
            Some("growth@example.com"),
            Some("https://runbook/emails"),
            &["user-facing".to_string(), "critical".to_string()],
        );
        // With every optional field absent only the queue label is emitted.
        metrics.record_queue_info("minimal", None, None, None, None, &[]);
    }

    #[test]
    fn record_job_kind_info_does_not_panic_on_mixed_attrs() {
        let metrics = AwaMetrics::from_global();
        metrics.record_job_kind_info(
            "send_email",
            Some("Send user email"),
            None,
            Some("growth@example.com"),
            None,
            &["outbound".to_string()],
        );
        metrics.record_job_kind_info("minimal", None, None, None, None, &[]);
    }
}
