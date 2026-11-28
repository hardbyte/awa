//! Escalated priority aging while one of a queue's lanes pins the queue ring.
//!
//! Queue prune can only reclaim the oldest sealed ring slot once no lane has
//! pending ready rows in it. A lane that strict priority (or a long aging
//! interval) never reaches therefore keeps storage from being reclaimed for
//! every queue. While such a lane of a queue appears in
//! `QueueStorage::queue_ring_pin`, that queue's dispatchers claim with an
//! aging interval short enough for the lane's head row to reach priority 1.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use awa_model::QueueRingPin;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::storage::QueueStorageRuntime;

/// How often a runtime reads the lanes pinning the queue ring.
pub(crate) const RING_PIN_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// An observation older than this is ignored, so a runtime that cannot read
/// the pin claims with the configured aging interval.
const RING_PIN_STALE_AFTER: Duration = Duration::from_secs(45);

/// The shortest aging interval escalation uses.
const MIN_ESCALATED_AGING_INTERVAL: Duration = Duration::from_secs(1);

/// A ready lane pinning the queue ring.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PinnedLane {
    pub queue: String,
    pub priority: i16,
    pub head_age: Duration,
}

/// The aging interval that brings the head of every lane in `lanes`, given
/// as `(priority, head age)`, to priority 1, when that is shorter than
/// `configured` or `configured` is zero (strict priority). `None` when
/// escalation changes nothing: no lane above priority 1, or the configured
/// interval is already at least as short.
///
/// The claim orders lanes by `GREATEST(1, priority - floor(head_age /
/// interval))`, so a head `age` old reaches priority 1 at
/// `age / (priority - 1)`. The interval is rounded down to a millisecond so
/// that floating point error cannot leave the head one level short, and is
/// never below one second.
pub(crate) fn escalated_aging_interval(
    configured: Duration,
    lanes: impl IntoIterator<Item = (i16, Duration)>,
) -> Option<Duration> {
    let shortest = lanes
        .into_iter()
        .filter(|(priority, _)| *priority > 1)
        .map(|(priority, age)| {
            let millis = (age.as_secs_f64() * 1000.0 / f64::from(priority - 1)).floor();
            Duration::from_millis(millis as u64).max(MIN_ESCALATED_AGING_INTERVAL)
        })
        .min()?;
    (configured.is_zero() || shortest < configured).then_some(shortest)
}

#[derive(Default)]
struct Observation {
    observed_at: Option<Instant>,
    lanes: Vec<PinnedLane>,
}

/// The lanes pinning the queue ring as last read by this runtime, shared by
/// its dispatchers and fed by [`run_refresher`] and the maintenance leader.
#[derive(Default)]
pub(crate) struct RingPinWatch {
    observation: Mutex<Observation>,
}

impl RingPinWatch {
    pub(crate) fn publish_pin(&self, pin: Option<&QueueRingPin>, now: Instant) {
        let lanes = pin
            .map(|pin| {
                pin.lanes
                    .iter()
                    .map(|lane| PinnedLane {
                        queue: lane.queue.clone(),
                        priority: lane.priority,
                        head_age: lane.head_age,
                    })
                    .collect()
            })
            .unwrap_or_default();
        self.publish(lanes, now);
    }

    fn publish(&self, lanes: Vec<PinnedLane>, now: Instant) {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observation.observed_at = Some(now);
        observation.lanes = lanes;
    }

    fn observed_within(&self, age: Duration, now: Instant) -> bool {
        self.observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observed_at
            .is_some_and(|at| now.saturating_duration_since(at) < age)
    }

    /// The aging interval to claim a queue with while its lanes pin the
    /// ring; see [`escalated_aging_interval`]. `claims_lane` says whether a
    /// pinned lane's queue is read by that queue's claims.
    pub(crate) fn escalated_interval(
        &self,
        configured: Duration,
        claims_lane: impl Fn(&str) -> bool,
        now: Instant,
    ) -> Option<Duration> {
        let observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let observed_at = observation.observed_at?;
        if observation.lanes.is_empty()
            || now.saturating_duration_since(observed_at) >= RING_PIN_STALE_AFTER
        {
            return None;
        }
        escalated_aging_interval(
            configured,
            observation
                .lanes
                .iter()
                .filter(|lane| claims_lane(&lane.queue))
                .map(|lane| (lane.priority, lane.head_age)),
        )
    }
}

/// Reads the lanes pinning the queue ring every [`RING_PIN_REFRESH_INTERVAL`]
/// until `cancel` fires, one query per runtime. A read the maintenance leader
/// published within the interval stands in for it. A failed read is logged
/// and leaves the last observation to go stale.
pub(crate) async fn run_refresher(
    watch: Arc<RingPinWatch>,
    runtime: QueueStorageRuntime,
    pool: PgPool,
    cancel: CancellationToken,
    start_offset: Duration,
) {
    tokio::select! {
        _ = cancel.cancelled() => return,
        _ = tokio::time::sleep(start_offset) => {}
    }
    let mut ticker = tokio::time::interval(RING_PIN_REFRESH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        if watch.observed_within(RING_PIN_REFRESH_INTERVAL, Instant::now()) {
            continue;
        }
        match runtime.store.queue_ring_pin(&pool).await {
            Ok(pin) => watch.publish_pin(pin.as_ref(), Instant::now()),
            Err(err) => {
                debug!(error = %err, "Failed to read the lanes pinning the queue ring for aging escalation");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(value: u64) -> Duration {
        Duration::from_secs(value)
    }

    #[test]
    fn escalates_a_strict_priority_lane_to_priority_one() {
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, [(4, secs(3600))]),
            Some(secs(1200))
        );
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, [(2, secs(90))]),
            Some(secs(90))
        );
    }

    #[test]
    fn a_head_reaches_priority_one_at_the_escalated_interval() {
        for (priority, age_secs) in [(4_i16, 3600.123_f64), (3, 7.0), (4, 11.9), (9, 1234.5678)] {
            let interval = escalated_aging_interval(
                Duration::ZERO,
                [(priority, Duration::from_secs_f64(age_secs))],
            )
            .expect("escalates");
            let aged = (age_secs / interval.as_secs_f64()).floor() as i64;
            assert_eq!(
                (i64::from(priority) - aged).max(1),
                1,
                "priority {priority} age {age_secs}"
            );
        }
    }

    #[test]
    fn takes_the_shortest_interval_across_pinning_lanes() {
        assert_eq!(
            escalated_aging_interval(
                Duration::ZERO,
                [(4, secs(3600)), (2, secs(300)), (3, secs(2000))]
            ),
            Some(secs(300))
        );
    }

    #[test]
    fn ignores_priority_one_lanes() {
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, [(1, secs(3600))]),
            None
        );
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, [(1, secs(3600)), (3, secs(600))]),
            Some(secs(300))
        );
        assert_eq!(escalated_aging_interval(Duration::ZERO, []), None);
    }

    #[test]
    fn only_replaces_a_longer_configured_interval() {
        assert_eq!(
            escalated_aging_interval(secs(60), [(4, secs(3600))]),
            None,
            "the configured interval already ages faster"
        );
        assert_eq!(
            escalated_aging_interval(secs(1200), [(4, secs(3600))]),
            None,
            "an equal interval changes nothing"
        );
        assert_eq!(
            escalated_aging_interval(secs(3600), [(4, secs(3600))]),
            Some(secs(1200))
        );
    }

    #[test]
    fn clamps_to_one_second_and_survives_extreme_ages() {
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, [(4, Duration::ZERO)]),
            Some(secs(1))
        );
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, [(4, Duration::from_millis(1))]),
            Some(secs(1))
        );
        assert_eq!(
            escalated_aging_interval(secs(60), [(5, secs(2))]),
            Some(secs(1))
        );
        assert!(escalated_aging_interval(Duration::ZERO, [(2, Duration::MAX)]).is_some());
        assert_eq!(
            escalated_aging_interval(secs(60), [(2, Duration::MAX)]),
            None
        );
    }

    #[test]
    fn the_watch_serves_only_its_queue_and_fresh_observations() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        let is_emails = |queue: &str| queue == "emails";
        assert_eq!(
            watch.escalated_interval(Duration::ZERO, is_emails, start),
            None,
            "nothing observed yet"
        );

        watch.publish(
            vec![
                PinnedLane {
                    queue: "emails".to_string(),
                    priority: 4,
                    head_age: secs(600),
                },
                PinnedLane {
                    queue: "reports".to_string(),
                    priority: 2,
                    head_age: secs(30),
                },
            ],
            start,
        );
        assert_eq!(
            watch.escalated_interval(Duration::ZERO, is_emails, start + secs(1)),
            Some(secs(200))
        );
        assert_eq!(
            watch.escalated_interval(Duration::ZERO, |queue| queue == "other", start),
            None
        );
        assert_eq!(
            watch.escalated_interval(Duration::ZERO, is_emails, start + RING_PIN_STALE_AFTER),
            None,
            "a stale observation falls back to the configured interval"
        );

        watch.publish(Vec::new(), start + secs(2));
        assert_eq!(
            watch.escalated_interval(Duration::ZERO, is_emails, start + secs(3)),
            None,
            "a lane that left the pin reverts"
        );
        assert!(watch.observed_within(RING_PIN_REFRESH_INTERVAL, start + secs(3)));
        assert!(!watch.observed_within(RING_PIN_REFRESH_INTERVAL, start + secs(12)));
    }
}
