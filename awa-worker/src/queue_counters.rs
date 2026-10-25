//! Per-queue outcome counters published with each runtime snapshot (#543).
//!
//! These complement the OpenTelemetry instruments rather than replace them:
//! OTel goes to whatever exporter the operator configured, while these reach
//! the admin UI through `awa.runtime_instances` with no extra infrastructure.
//! The maintenance leader differences consecutive readings into per-minute
//! rates, so the values only ever increase until the process restarts.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use awa_model::admin::QueueOutcomeCounters;

#[derive(Debug, Default)]
pub(crate) struct QueueCounters {
    claimed: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    rate_limited: AtomicU64,
    claim_wait_ms: AtomicU64,
    parked: AtomicU64,
}

/// Shared by the dispatchers, the executor and the snapshot reporter. Built
/// once with every configured queue, so lookups never allocate.
pub(crate) type QueueCounterMap = Arc<HashMap<String, Arc<QueueCounters>>>;

pub(crate) fn counter_map<'a>(queues: impl IntoIterator<Item = &'a String>) -> QueueCounterMap {
    Arc::new(
        queues
            .into_iter()
            .map(|queue| (queue.clone(), Arc::new(QueueCounters::default())))
            .collect(),
    )
}

// Relaxed throughout: each counter is independent and only read for
// reporting, so no ordering with other memory is needed.
impl QueueCounters {
    pub(crate) fn record_claimed(&self, jobs: u64, wait_ms: u64) {
        self.claimed.fetch_add(jobs, Ordering::Relaxed);
        self.claim_wait_ms.fetch_add(wait_ms, Ordering::Relaxed);
    }

    pub(crate) fn record_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rate_limited(&self) {
        self.rate_limited.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_parked(&self) {
        self.parked.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> QueueOutcomeCounters {
        QueueOutcomeCounters {
            claimed: self.claimed.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            claim_wait_ms: self.claim_wait_ms.load(Ordering::Relaxed),
            parked: self.parked.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_recorded_outcomes() {
        let map = counter_map(&["email".to_string()]);
        let counters = &map["email"];
        counters.record_claimed(3, 450);
        counters.record_completed();
        counters.record_completed();
        counters.record_failed();
        counters.record_retried();
        counters.record_rate_limited();
        counters.record_parked();
        assert_eq!(
            counters.snapshot(),
            QueueOutcomeCounters {
                claimed: 3,
                completed: 2,
                failed: 1,
                retried: 1,
                rate_limited: 1,
                claim_wait_ms: 450,
                parked: 1,
            }
        );
    }
}
