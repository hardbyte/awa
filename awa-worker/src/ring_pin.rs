//! Escalated priority aging while one of a queue's lanes pins the queue ring.
//!
//! Queue prune can only reclaim the oldest sealed ring slot once no lane has
//! pending ready rows in it. A lane that strict priority (or a long aging
//! interval) never reaches therefore keeps storage from being reclaimed for
//! every queue. A lane that stays in `QueueStorage::queue_ring_pin` with its
//! claim cursor still, for a queue a runtime serves, is `starved`; while it is,
//! that queue's dispatchers claim with an aging interval short enough for every
//! pending row of the lane in the pinned slot to reach priority 1.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use awa_model::{QueueRingPin, QueueStorage};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::storage::QueueStorageRuntime;

/// How often a runtime reads the lanes pinning the queue ring.
pub(crate) const RING_PIN_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// An observation older than this is ignored, so a runtime that cannot read
/// the pin claims with the configured aging interval.
const RING_PIN_STALE_AFTER: Duration = Duration::from_secs(45);

/// Two reads of a lane's claim cursor closer together than this do not show
/// whether the lane is being claimed.
const MIN_CURSOR_COMPARE_INTERVAL: Duration = Duration::from_secs(5);

/// The shortest aging interval escalation uses.
const MIN_ESCALATED_AGING_INTERVAL: Duration = Duration::from_secs(1);

/// The claim computes `priority - floor(age / interval)` as a `smallint` for
/// every lane of the queue, which fails every claim of the queue once the
/// quotient passes 32,767. Escalation keeps it at half that.
const MAX_AGING_STEPS: f64 = 16_384.0;

/// A lane of the queue ring: queue, priority and enqueue shard.
pub(crate) type LaneKey = (String, i16, i16);

/// Why a lane keeps queue prune from reclaiming the oldest sealed ring slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PinCause {
    /// The lane's queue is paused.
    Paused,
    /// No live runtime on queue storage serves the lane's queue.
    NoRuntime,
    /// A runtime serves the queue and the lane's claim cursor did not move
    /// between two reads in the same slot.
    Starved,
    /// The lane's claim cursor moved between two reads in the same slot: it
    /// is being claimed, more slowly than rows arrive or than the ring rotates.
    Advancing,
    /// Not known yet: fewer than two reads in the slot, or the queue's state
    /// could not be read.
    Unknown,
}

impl PinCause {
    /// `serving` is whether the lane's queue is paused and whether a live
    /// runtime serves it, when that could be read. `moved` is whether the
    /// lane's cursor moved since an earlier read in the same slot, when there
    /// is one to compare with.
    fn classify(serving: Option<(bool, bool)>, moved: Option<bool>) -> Self {
        match (serving, moved) {
            (None, _) => Self::Unknown,
            (Some((true, _)), _) => Self::Paused,
            (Some((false, false)), _) => Self::NoRuntime,
            (Some((false, true)), Some(true)) => Self::Advancing,
            (Some((false, true)), Some(false)) => Self::Starved,
            (Some((false, true)), None) => Self::Unknown,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::NoRuntime => "no_runtime",
            Self::Starved => "starved",
            Self::Advancing => "advancing",
            Self::Unknown => "unknown",
        }
    }
}

/// A ready lane pinning the queue ring, as read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LaneReading {
    pub key: LaneKey,
    /// The queue whose claims read the lane: the lane's queue without its
    /// stripe suffix.
    pub logical_queue: String,
    /// The lane's first pending row; it moves when the lane is claimed.
    pub head_lane_seq: i64,
    /// How long the lane's newest row in the pinned slot has been ready.
    pub newest_age: Duration,
}

/// What one read of the queue ring pin found.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PinReading {
    pub slot: i32,
    pub lanes: Vec<LaneReading>,
    /// Whether each lane's queue is paused and whether a live runtime serves
    /// it, by lane queue. `None` when it could not be read.
    pub serving: Option<HashMap<String, (bool, bool)>>,
    /// How long the oldest ready segment still pending in any lane of a queue
    /// has been ready, by queue without stripe suffixes.
    pub oldest_pending: HashMap<String, Duration>,
}

/// Reads what [`RingPinWatch::publish`] needs beyond `pin`: whether the
/// lanes' queues are served, and how old each queue's oldest pending row is.
/// Two queries; a failed one leaves its part empty, so the lanes are not
/// escalated.
pub(crate) async fn read_pin(
    store: &QueueStorage,
    pool: &PgPool,
    pin: &QueueRingPin,
) -> PinReading {
    let lanes: Vec<LaneReading> = pin
        .lanes
        .iter()
        .map(|lane| LaneReading {
            key: (lane.queue.clone(), lane.priority, lane.enqueue_shard),
            logical_queue: store.logical_queue_name(&lane.queue).to_string(),
            head_lane_seq: lane.head_lane_seq,
            newest_age: lane.newest_age,
        })
        .collect();

    let mut lane_queues: Vec<String> = lanes.iter().map(|lane| lane.key.0.clone()).collect();
    lane_queues.sort_unstable();
    lane_queues.dedup();
    let serving = match store.queue_serving_states(pool, &lane_queues).await {
        Ok(states) => Some(
            states
                .into_iter()
                .map(|(queue, state)| (queue, (state.paused, state.served)))
                .collect(),
        ),
        Err(err) => {
            debug!(error = %err, "Failed to read whether the lanes pinning the queue ring are served");
            None
        }
    };

    let mut escalatable: Vec<String> = lanes
        .iter()
        .filter(|lane| lane.key.1 > 1)
        .map(|lane| lane.key.0.clone())
        .collect();
    escalatable.sort_unstable();
    escalatable.dedup();
    let oldest_pending = if escalatable.is_empty() {
        HashMap::new()
    } else {
        match store.oldest_pending_ready_ages(pool, &escalatable).await {
            Ok(ages) => ages,
            Err(err) => {
                debug!(error = %err, "Failed to read the age of a queue's oldest pending ready row");
                HashMap::new()
            }
        }
    };

    PinReading {
        slot: pin.slot,
        lanes,
        serving,
        oldest_pending,
    }
}

/// The aging interval that brings every pending row of a lane of priority
/// `priority` in the pinned slot to priority 1, given the age of the lane's
/// newest row there. `None` for priority 1.
///
/// The claim orders lanes by `GREATEST(1, priority - floor(age / interval))`,
/// so a row `age` old reaches priority 1 at `age / (priority - 1)`. The
/// interval is rounded down to a millisecond so that floating point error
/// cannot leave a row one level short, and is never below one second.
fn lane_wanted_interval(priority: i16, newest_age: Duration) -> Option<Duration> {
    (priority > 1).then(|| {
        let millis = (newest_age.as_secs_f64() * 1000.0 / f64::from(priority - 1)).floor();
        Duration::from_millis(millis as u64).max(MIN_ESCALATED_AGING_INTERVAL)
    })
}

/// The shortest interval, in whole seconds and at least one, that keeps
/// `oldest_pending / interval` within [`MAX_AGING_STEPS`].
fn overflow_safe_interval(oldest_pending: Duration) -> Duration {
    let seconds = (oldest_pending.as_secs_f64() / MAX_AGING_STEPS).ceil() as u64;
    Duration::from_secs(seconds).max(MIN_ESCALATED_AGING_INTERVAL)
}

/// The aging interval to claim a queue with while its lanes are escalated:
/// `wanted`, raised to what keeps the age of the queue's oldest pending row
/// within what the claim can compute. `None` when that is not shorter than
/// `configured`, unless `configured` is zero (strict priority).
fn escalated_aging_interval(
    configured: Duration,
    wanted: Duration,
    oldest_pending: Duration,
) -> Option<Duration> {
    let interval = wanted.max(overflow_safe_interval(oldest_pending));
    (configured.is_zero() || interval < configured).then_some(interval)
}

/// What a queue's escalation needs to know, as of the last read.
#[derive(Debug, Clone, Copy)]
struct QueueEscalation {
    /// The shortest interval any escalated lane of the queue asks for.
    wanted: Duration,
    oldest_pending: Duration,
}

#[derive(Default)]
struct Observation {
    observed_at: Option<Instant>,
    slot: Option<i32>,
    /// Each lane's first pending row at the first read in the slot where it
    /// stood there, and when.
    baselines: HashMap<LaneKey, (i64, Instant)>,
    /// Lanes escalated since they were first read as starved, until they no
    /// longer pin the slot.
    escalating: HashSet<LaneKey>,
    /// Escalated queues, by queue without stripe suffixes.
    queues: HashMap<String, QueueEscalation>,
}

impl Observation {
    /// Whether the lane's cursor moved since an earlier read in this slot,
    /// when a read at least [`MIN_CURSOR_COMPARE_INTERVAL`] old can say.
    fn cursor_moved(&mut self, lane: &LaneKey, head_lane_seq: i64, now: Instant) -> Option<bool> {
        match self.baselines.get(lane).copied() {
            None => {
                self.baselines.insert(lane.clone(), (head_lane_seq, now));
                None
            }
            Some((seq, _)) if head_lane_seq > seq => {
                self.baselines.insert(lane.clone(), (head_lane_seq, now));
                Some(true)
            }
            Some((_, at)) if now.saturating_duration_since(at) >= MIN_CURSOR_COMPARE_INTERVAL => {
                Some(false)
            }
            Some(_) => None,
        }
    }
}

/// The lanes pinning the queue ring as last read by this runtime, shared by
/// its dispatchers and fed by [`run_refresher`] and the maintenance leader.
///
/// A lane is escalated from the first read that finds it `starved` until it
/// no longer pins the slot, unless its queue is paused or unserved.
#[derive(Default)]
pub(crate) struct RingPinWatch {
    observation: Mutex<Observation>,
}

impl RingPinWatch {
    fn lock(&self) -> std::sync::MutexGuard<'_, Observation> {
        self.observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records `reading` and returns each lane's cause.
    pub(crate) fn publish(&self, reading: PinReading, now: Instant) -> HashMap<LaneKey, PinCause> {
        let mut guard = self.lock();
        let observation = &mut *guard;
        observation.observed_at = Some(now);
        if observation.slot != Some(reading.slot) {
            observation.slot = Some(reading.slot);
            observation.baselines.clear();
            observation.escalating.clear();
        }
        let pinning: HashSet<&LaneKey> = reading.lanes.iter().map(|lane| &lane.key).collect();
        observation
            .baselines
            .retain(|lane, _| pinning.contains(lane));
        observation.escalating.retain(|lane| pinning.contains(lane));

        let mut causes = HashMap::with_capacity(reading.lanes.len());
        for lane in &reading.lanes {
            let moved = observation.cursor_moved(&lane.key, lane.head_lane_seq, now);
            let serving = reading
                .serving
                .as_ref()
                .and_then(|states| states.get(&lane.key.0))
                .copied();
            let cause = PinCause::classify(serving, moved);
            match cause {
                PinCause::Starved => {
                    observation.escalating.insert(lane.key.clone());
                }
                PinCause::Paused | PinCause::NoRuntime => {
                    observation.escalating.remove(&lane.key);
                }
                PinCause::Advancing | PinCause::Unknown => {}
            }
            causes.insert(lane.key.clone(), cause);
        }

        let mut queues: HashMap<String, QueueEscalation> = HashMap::new();
        for lane in &reading.lanes {
            if !observation.escalating.contains(&lane.key) {
                continue;
            }
            let (Some(wanted), Some(oldest_pending)) = (
                lane_wanted_interval(lane.key.1, lane.newest_age),
                reading.oldest_pending.get(&lane.logical_queue).copied(),
            ) else {
                continue;
            };
            queues
                .entry(lane.logical_queue.clone())
                .and_modify(|queue| queue.wanted = queue.wanted.min(wanted))
                .or_insert(QueueEscalation {
                    wanted,
                    oldest_pending,
                });
        }
        observation.queues = queues;
        causes
    }

    /// Records that no lane pins the queue ring.
    pub(crate) fn clear(&self, now: Instant) {
        *self.lock() = Observation {
            observed_at: Some(now),
            ..Observation::default()
        };
    }

    fn observed_within(&self, age: Duration, now: Instant) -> bool {
        self.lock()
            .observed_at
            .is_some_and(|at| now.saturating_duration_since(at) < age)
    }

    /// The aging interval to claim `queue` with while its lanes are
    /// escalated, if that is shorter than `configured` or `configured` is
    /// zero; see [`escalated_aging_interval`]. One map lookup under the lock.
    pub(crate) fn escalated_interval(
        &self,
        queue: &str,
        configured: Duration,
        now: Instant,
    ) -> Option<Duration> {
        let escalation = {
            let observation = self.lock();
            let observed_at = observation.observed_at?;
            if now.saturating_duration_since(observed_at) >= RING_PIN_STALE_AFTER {
                return None;
            }
            *observation.queues.get(queue)?
        };
        escalated_aging_interval(configured, escalation.wanted, escalation.oldest_pending)
    }
}

/// Reads the lanes pinning the queue ring every [`RING_PIN_REFRESH_INTERVAL`]
/// until `cancel` fires, one read per runtime. A read the maintenance leader
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
            Ok(Some(pin)) => {
                let reading = read_pin(&runtime.store, &pool, &pin).await;
                watch.publish(reading, Instant::now());
            }
            Ok(None) => watch.clear(Instant::now()),
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

    fn lane_key(queue: &str, priority: i16) -> LaneKey {
        (queue.to_string(), priority, 0)
    }

    fn lane(queue: &str, priority: i16, head_lane_seq: i64, newest_age: u64) -> LaneReading {
        LaneReading {
            key: lane_key(queue, priority),
            logical_queue: queue.to_string(),
            head_lane_seq,
            newest_age: secs(newest_age),
        }
    }

    /// A reading of `lanes` in `slot` for queues that are served and not
    /// paused, with the oldest pending row of every queue `oldest` old.
    fn reading(slot: i32, lanes: Vec<LaneReading>, oldest: u64) -> PinReading {
        let serving = lanes
            .iter()
            .map(|lane| (lane.key.0.clone(), (false, true)))
            .collect();
        let oldest_pending = lanes
            .iter()
            .map(|lane| (lane.logical_queue.clone(), secs(oldest)))
            .collect();
        PinReading {
            slot,
            lanes,
            serving: Some(serving),
            oldest_pending,
        }
    }

    fn interval(watch: &RingPinWatch, queue: &str, at: Instant) -> Option<Duration> {
        watch.escalated_interval(queue, Duration::ZERO, at)
    }

    /// Publishes the same lanes at `start` and ten seconds later, which reads
    /// them as starved.
    fn publish_starved(watch: &RingPinWatch, lanes: impl Fn() -> Vec<LaneReading>, start: Instant) {
        watch.publish(reading(0, lanes(), 600), start);
        watch.publish(reading(0, lanes(), 600), start + secs(10));
    }

    #[test]
    fn a_lane_reaches_priority_one_at_the_wanted_interval() {
        assert_eq!(lane_wanted_interval(4, secs(3600)), Some(secs(1200)));
        assert_eq!(lane_wanted_interval(2, secs(90)), Some(secs(90)));
        assert_eq!(lane_wanted_interval(1, secs(3600)), None);
        for (priority, age_secs) in [(4_i16, 3600.123_f64), (3, 7.0), (4, 11.9), (9, 1234.5678)] {
            let wanted = lane_wanted_interval(priority, Duration::from_secs_f64(age_secs))
                .expect("a lane above priority 1");
            let aged = (age_secs / wanted.as_secs_f64()).floor() as i64;
            assert_eq!(
                (i64::from(priority) - aged).max(1),
                1,
                "priority {priority} age {age_secs}"
            );
        }
    }

    #[test]
    fn every_row_of_a_lane_in_the_slot_reaches_priority_one() {
        let row_ages = [3600_u64, 3570, 3540, 3500];
        let newest = *row_ages.last().expect("rows");
        let wanted = lane_wanted_interval(4, secs(newest)).expect("priority 4");
        for age in row_ages {
            let aged = (age as f64 / wanted.as_secs_f64()).floor() as i64;
            assert_eq!((4 - aged).max(1), 1, "a row {age}s old");
        }
        let head_only = lane_wanted_interval(4, secs(row_ages[0])).expect("priority 4");
        let aged = (newest as f64 / head_only.as_secs_f64()).floor() as i64;
        assert!(
            4 - aged > 1,
            "an interval from the head's age leaves the newer rows behind"
        );
    }

    #[test]
    fn the_wanted_interval_is_at_least_one_second_and_survives_extreme_ages() {
        assert_eq!(lane_wanted_interval(4, Duration::ZERO), Some(secs(1)));
        assert_eq!(
            lane_wanted_interval(4, Duration::from_millis(1)),
            Some(secs(1))
        );
        assert!(lane_wanted_interval(2, Duration::MAX).is_some());
    }

    #[test]
    fn the_interval_keeps_the_oldest_pending_row_within_the_claims_range() {
        assert_eq!(overflow_safe_interval(Duration::ZERO), secs(1));
        assert_eq!(overflow_safe_interval(secs(10 * 3600)), secs(3));
        assert_eq!(overflow_safe_interval(secs(100 * 3600)), secs(22));
        for age in [1_u64, 16_384, 16_385, 36_000, 360_000, 86_400 * 365] {
            let interval = overflow_safe_interval(secs(age));
            assert!(
                secs(age).as_secs_f64() / interval.as_secs_f64() <= MAX_AGING_STEPS,
                "age {age}"
            );
        }
        assert!(overflow_safe_interval(Duration::MAX) >= secs(1));
    }

    #[test]
    fn escalation_raises_the_interval_to_what_the_oldest_row_allows() {
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, secs(1), secs(36_000)),
            Some(secs(3))
        );
        assert_eq!(
            escalated_aging_interval(Duration::ZERO, secs(30), secs(36_000)),
            Some(secs(30))
        );
    }

    #[test]
    fn escalation_only_replaces_a_longer_configured_interval() {
        assert_eq!(
            escalated_aging_interval(secs(60), secs(1200), secs(0)),
            None,
            "the configured interval already ages faster"
        );
        assert_eq!(
            escalated_aging_interval(secs(1200), secs(1200), secs(0)),
            None,
            "an equal interval changes nothing"
        );
        assert_eq!(
            escalated_aging_interval(secs(3600), secs(1200), secs(0)),
            Some(secs(1200))
        );
        assert_eq!(
            escalated_aging_interval(secs(3), secs(1), secs(36_000)),
            None,
            "the safe interval is not below the configured one"
        );
        assert_eq!(
            escalated_aging_interval(secs(4), secs(1), secs(36_000)),
            Some(secs(3))
        );
    }

    #[test]
    fn classifies_a_lane_by_its_queue_then_by_its_cursor() {
        let state = |paused, served| Some((paused, served));
        assert_eq!(
            PinCause::classify(state(true, true), Some(true)),
            PinCause::Paused
        );
        assert_eq!(
            PinCause::classify(state(true, false), None),
            PinCause::Paused
        );
        assert_eq!(
            PinCause::classify(state(false, false), Some(false)),
            PinCause::NoRuntime
        );
        assert_eq!(
            PinCause::classify(state(false, false), None),
            PinCause::NoRuntime
        );
        assert_eq!(
            PinCause::classify(state(false, true), Some(true)),
            PinCause::Advancing
        );
        assert_eq!(
            PinCause::classify(state(false, true), Some(false)),
            PinCause::Starved
        );
        assert_eq!(
            PinCause::classify(state(false, true), None),
            PinCause::Unknown,
            "a single read says nothing about the cursor"
        );
        assert_eq!(PinCause::classify(None, Some(false)), PinCause::Unknown);
        assert_eq!(PinCause::NoRuntime.as_str(), "no_runtime");
    }

    #[test]
    fn a_lane_is_starved_once_its_cursor_stays_still_between_two_reads() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        let lanes = || vec![lane("emails", 4, 7, 600)];

        let causes = watch.publish(reading(0, lanes(), 600), start);
        assert_eq!(causes[&lane_key("emails", 4)], PinCause::Unknown);
        assert_eq!(interval(&watch, "emails", start), None);

        let causes = watch.publish(reading(0, lanes(), 600), start + secs(1));
        assert_eq!(
            causes[&lane_key("emails", 4)],
            PinCause::Unknown,
            "reads a second apart do not show whether the lane is claimed"
        );
        assert_eq!(interval(&watch, "emails", start + secs(1)), None);

        let causes = watch.publish(reading(0, lanes(), 600), start + secs(10));
        assert_eq!(causes[&lane_key("emails", 4)], PinCause::Starved);
        assert_eq!(
            interval(&watch, "emails", start + secs(10)),
            Some(secs(200)),
            "the newest row's age over priority minus one"
        );
        assert_eq!(interval(&watch, "reports", start + secs(10)), None);
    }

    #[test]
    fn a_lane_whose_cursor_moves_is_never_escalated() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        for (n, head) in [(0_u64, 7_i64), (10, 9), (20, 15), (30, 40)] {
            let at = start + secs(n);
            let causes = watch.publish(reading(0, vec![lane("emails", 4, head, 600)], 600), at);
            assert_ne!(
                causes[&lane_key("emails", 4)],
                PinCause::Starved,
                "read {n}"
            );
            assert_eq!(interval(&watch, "emails", at), None, "read {n}");
        }
    }

    #[test]
    fn a_lane_keeps_escalating_while_it_pins_the_slot_whatever_its_cursor_does() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        let at = |n: u64| start + secs(n);
        publish_starved(&watch, || vec![lane("emails", 4, 7, 600)], start);
        assert!(interval(&watch, "emails", at(10)).is_some());

        let causes = watch.publish(reading(0, vec![lane("emails", 4, 9, 610)], 610), at(20));
        assert_eq!(causes[&lane_key("emails", 4)], PinCause::Advancing);
        assert_eq!(
            interval(&watch, "emails", at(20)),
            Some(Duration::from_millis(203_333)),
            "claims moved the cursor, and the lane still escalates"
        );

        watch.publish(reading(0, vec![lane("emails", 4, 11, 620)], 620), at(30));
        assert!(interval(&watch, "emails", at(30)).is_some());

        watch.publish(reading(0, vec![lane("other", 4, 3, 620)], 620), at(40));
        assert_eq!(
            interval(&watch, "emails", at(40)),
            None,
            "a lane that no longer pins the slot reverts"
        );

        watch.publish(reading(0, vec![lane("emails", 4, 12, 630)], 630), at(50));
        assert_eq!(
            interval(&watch, "emails", at(50)),
            None,
            "back in the pin, it has to be read as starved again"
        );
    }

    #[test]
    fn escalation_ends_when_the_pin_moves_to_another_slot_or_clears() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        let at = |n: u64| start + secs(n);
        publish_starved(&watch, || vec![lane("emails", 4, 7, 600)], start);
        assert!(interval(&watch, "emails", at(10)).is_some());

        let causes = watch.publish(reading(1, vec![lane("emails", 4, 7, 600)], 600), at(20));
        assert_eq!(causes[&lane_key("emails", 4)], PinCause::Unknown);
        assert_eq!(
            interval(&watch, "emails", at(20)),
            None,
            "reads in different slots do not show a still cursor"
        );

        watch.publish(reading(1, vec![lane("emails", 4, 7, 600)], 600), at(30));
        assert!(interval(&watch, "emails", at(30)).is_some());
        watch.clear(at(31));
        assert_eq!(interval(&watch, "emails", at(31)), None);
    }

    #[test]
    fn a_paused_or_unserved_queue_does_not_escalate_and_stops_when_it_becomes_so() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        let at = |n: u64| start + secs(n);
        let with_state = |paused, served| {
            let mut read = reading(0, vec![lane("emails", 4, 7, 600)], 600);
            read.serving = Some(HashMap::from([("emails".to_string(), (paused, served))]));
            read
        };
        watch.publish(with_state(false, true), at(0));
        watch.publish(with_state(false, true), at(10));
        assert!(interval(&watch, "emails", at(10)).is_some());

        let causes = watch.publish(with_state(true, true), at(20));
        assert_eq!(causes[&lane_key("emails", 4)], PinCause::Paused);
        assert_eq!(interval(&watch, "emails", at(20)), None);

        let causes = watch.publish(with_state(false, true), at(30));
        assert_eq!(causes[&lane_key("emails", 4)], PinCause::Starved);
        assert!(interval(&watch, "emails", at(30)).is_some());

        let causes = watch.publish(with_state(false, false), at(40));
        assert_eq!(causes[&lane_key("emails", 4)], PinCause::NoRuntime);
        assert_eq!(interval(&watch, "emails", at(40)), None);
    }

    #[test]
    fn a_lane_is_not_escalated_without_what_the_interval_is_computed_from() {
        let start = Instant::now();
        let starved = |tweak: fn(&mut PinReading)| {
            let watch = RingPinWatch::default();
            for at in [start, start + secs(10)] {
                let mut read = reading(0, vec![lane("emails", 4, 7, 600)], 600);
                tweak(&mut read);
                watch.publish(read, at);
            }
            interval(&watch, "emails", start + secs(10))
        };
        assert!(starved(|_| {}).is_some());
        assert_eq!(
            starved(|read| read.serving = None),
            None,
            "whether the queue is served could not be read"
        );
        assert_eq!(
            starved(|read| read.oldest_pending.clear()),
            None,
            "the age of the oldest pending row could not be read"
        );

        let watch = RingPinWatch::default();
        publish_starved(&watch, || vec![lane("emails", 1, 7, 600)], start);
        assert_eq!(
            interval(&watch, "emails", start + secs(10)),
            None,
            "priority 1 has nothing to gain"
        );
    }

    #[test]
    fn the_interval_serves_the_shortest_lane_and_the_oldest_pending_row_of_its_queue() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        let lanes = || {
            vec![
                lane("emails", 4, 7, 3600),
                lane("emails", 2, 7, 300),
                lane("reports", 3, 7, 2000),
            ]
        };
        publish_starved(&watch, lanes, start);
        let at = start + secs(10);
        assert_eq!(interval(&watch, "emails", at), Some(secs(300)));
        assert_eq!(interval(&watch, "reports", at), Some(secs(1000)));
        assert_eq!(
            watch.escalated_interval("emails", secs(300), at),
            None,
            "not shorter than the configured interval"
        );
    }

    #[test]
    fn an_observation_goes_stale() {
        let watch = RingPinWatch::default();
        let start = Instant::now();
        assert_eq!(interval(&watch, "emails", start), None);
        publish_starved(&watch, || vec![lane("emails", 4, 7, 600)], start);
        assert!(interval(&watch, "emails", start + secs(11)).is_some());
        assert_eq!(
            interval(&watch, "emails", start + secs(10) + RING_PIN_STALE_AFTER),
            None,
            "a stale observation falls back to the configured interval"
        );
        assert!(watch.observed_within(RING_PIN_REFRESH_INTERVAL, start + secs(15)));
        assert!(!watch.observed_within(RING_PIN_REFRESH_INTERVAL, start + secs(25)));
    }
}
