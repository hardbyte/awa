//! Maintenance progress, judged from the captured maintenance metrics
//! (`maintenance-metrics.jsonl`) against the declared intents:
//!
//! - every ring's `awa.ring.generation` keeps advancing, unless rotation reports the ring
//!   idle or filling, or the ready lanes pinning it are explained;
//! - prune `blocked` does not rise while `pruned` stays flat (#721);
//! - a ready lane pinning the queue ring for longer than a bound is explained by declared
//!   intent or by claims still moving through its queue;
//! - after an operator operation on a pinning lane, the pin clears within a bound.
//!
//! The capture covers the runtimes in this process. While a runtime in another process
//! leads, its metrics are missing, and the in-process gauges repeat their last values, so
//! a lane is judged only at exports where this process's leader was pruning.

use super::intent::{logical, Intent, IntentSpan};
use super::shared::Shared;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::sync::Mutex;

/// When the oracle saw each queue's claim cursor move, by physical queue name.
#[derive(Default)]
pub struct ProgressLog {
    moves: Mutex<HashMap<String, Vec<i64>>>,
}

impl ProgressLog {
    pub fn moved(&self, queue: &str, at_ms: i64) {
        self.moves
            .lock()
            .expect("progress log")
            .entry(queue.to_string())
            .or_default()
            .push(at_ms);
    }

    pub fn snapshot(&self) -> HashMap<String, Vec<i64>> {
        self.moves.lock().expect("progress log").clone()
    }
}

const RING_GENERATION: &str = "awa.ring.generation";
const PRUNE_ATTEMPTS: &str = "awa.maintenance.prune.attempts";
const ROTATE_ATTEMPTS: &str = "awa.maintenance.rotate.attempts";
const PINNED_READY_AGE: &str = "awa.maintenance.prune.pinned_ready_age";
/// The in-process leader counts as pruning at an export when it recorded a queue-ring
/// prune attempt within this long before it.
const LEADING_WITHIN_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Lane {
    pub queue: String,
    pub priority: String,
    pub shard: String,
}

/// (metric, ring, outcome).
type CounterKey = (String, String, String);

/// The captured series, in run time.
#[derive(Default, Debug)]
pub struct Series {
    /// ring -> (at_ms, generation)
    generation: BTreeMap<String, Vec<(i64, i64)>>,
    /// (metric, ring, outcome) -> attribute set -> (at_ms, cumulative)
    counters: BTreeMap<CounterKey, BTreeMap<String, Vec<(i64, f64)>>>,
    /// lane -> (at_ms, age seconds)
    pins: BTreeMap<Lane, Vec<(i64, f64)>>,
}

fn attribute(row: &Value, key: &str) -> String {
    match &row["attributes"][key] {
        Value::String(text) => text.clone(),
        Value::Null => "none".into(),
        other => other.to_string(),
    }
}

impl Series {
    /// `epoch_offset_ms` is the epoch time of the run's start.
    pub fn from_rows(rows: &[Value], epoch_offset_ms: i64) -> Self {
        let mut series = Series::default();
        for row in rows {
            let at_ms = row["at_ms"].as_i64().unwrap_or_default() - epoch_offset_ms;
            let name = row["name"].as_str().unwrap_or_default();
            let value = row["value"].as_f64().unwrap_or_default();
            match name {
                RING_GENERATION => series
                    .generation
                    .entry(attribute(row, "awa.ring"))
                    .or_default()
                    .push((at_ms, value as i64)),
                PRUNE_ATTEMPTS | ROTATE_ATTEMPTS => series
                    .counters
                    .entry((
                        name.to_string(),
                        attribute(row, "awa.ring"),
                        attribute(row, "awa.ring.outcome"),
                    ))
                    .or_default()
                    .entry(row["attributes"].to_string())
                    .or_default()
                    .push((at_ms, value)),
                PINNED_READY_AGE => series
                    .pins
                    .entry(Lane {
                        queue: attribute(row, "awa.job.queue"),
                        priority: attribute(row, "awa.job.priority"),
                        shard: attribute(row, "awa.queue.enqueue_shard"),
                    })
                    .or_default()
                    .push((at_ms, value)),
                _ => {}
            }
        }
        for points in series.generation.values_mut() {
            points.sort_unstable();
        }
        for points in series.pins.values_mut() {
            points.sort_by_key(|point| point.0);
        }
        for sets in series.counters.values_mut() {
            for points in sets.values_mut() {
                points.sort_by_key(|point| point.0);
            }
        }
        series
    }

    /// The cumulative `metric` count for `ring` and `outcome` as of `at_ms`.
    fn count(&self, metric: &str, ring: &str, outcome: &str, at_ms: i64) -> f64 {
        self.counters
            .get(&(metric.to_string(), ring.to_string(), outcome.to_string()))
            .map_or(0.0, |sets| {
                sets.values()
                    .filter_map(|points| {
                        points
                            .iter()
                            .take_while(|point| point.0 <= at_ms)
                            .last()
                            .map(|point| point.1)
                    })
                    .sum()
            })
    }

    fn prune_total(&self, ring: &str, at_ms: i64) -> f64 {
        self.counters
            .iter()
            .filter(|((metric, counter_ring, _), _)| {
                metric == PRUNE_ATTEMPTS && counter_ring == ring
            })
            .map(|((_, _, outcome), _)| self.count(PRUNE_ATTEMPTS, ring, outcome, at_ms))
            .sum()
    }

    /// Whether this process's leader was pruning the queue ring shortly before `at_ms`.
    fn leading(&self, at_ms: i64) -> bool {
        self.prune_total("queue", at_ms) > self.prune_total("queue", at_ms - LEADING_WITHIN_MS)
    }

    /// Lanes reported pinning at the export nearest before `at_ms`.
    fn pins_at(&self, at_ms: i64) -> Vec<(&Lane, f64)> {
        self.pins
            .iter()
            .filter_map(|(lane, points)| {
                let point = points.iter().take_while(|point| point.0 <= at_ms).last()?;
                (point.1 > 0.0 && at_ms - point.0 <= LEADING_WITHIN_MS).then_some((lane, point.1))
            })
            .collect()
    }

    fn export_times(&self, ring: &str) -> Vec<i64> {
        let mut times: BTreeSet<i64> = BTreeSet::new();
        for ((_, counter_ring, _), sets) in &self.counters {
            if counter_ring == ring {
                for points in sets.values() {
                    times.extend(points.iter().map(|point| point.0));
                }
            }
        }
        times.into_iter().collect()
    }
}

/// What the checks may treat as explained.
pub struct Context<'a> {
    pub spans: &'a [IntentSpan],
    /// Physical queue -> times its claim cursor was seen moving.
    pub progress: &'a HashMap<String, Vec<i64>>,
    pub grace_ms: i64,
    /// A queue counts as progressing when its cursor moved within this long.
    pub progress_window_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum Explanation {
    Intent { kind: String, detail: String },
    Progress { last_move_s: f64 },
    HoldsLocks { detail: String },
}

impl Context<'_> {
    fn holds_locks(&self, at_ms: i64) -> Option<Explanation> {
        super::intent::covering(self.spans, None, at_ms, self.grace_ms)
            .find(|span| span.intent == Intent::HoldsLocks)
            .map(|span| Explanation::HoldsLocks {
                detail: span.detail.clone(),
            })
    }

    /// Why `queue` (physical) may hold a pinning ready row at `at_ms`, if anything explains it.
    pub fn explain_pin(&self, queue: &str, at_ms: i64) -> Option<Explanation> {
        if let Some(span) = self.spans.iter().find(|span| {
            span.queue.is_some() && span.applies_to(queue) && span.covers(at_ms, self.grace_ms)
        }) {
            return Some(Explanation::Intent {
                kind: match span.intent {
                    Intent::OperatorOp(op) => format!("operator_op:{op}"),
                    other => format!("{other:?}").to_lowercase(),
                },
                detail: span.detail.clone(),
            });
        }
        if let Some(last) = self.progress.get(queue).and_then(|moves| {
            moves
                .iter()
                .copied()
                .filter(|moved| {
                    *moved <= at_ms + 10_000 && *moved > at_ms - self.progress_window_ms
                })
                .max()
        }) {
            return Some(Explanation::Progress {
                last_move_s: last as f64 / 1000.0,
            });
        }
        None
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RingStall {
    pub ring: String,
    pub generation: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub rotate_skipped_busy: f64,
    pub pins: Vec<String>,
}

/// Stretches of at least `window_ms` in which a ring's generation stood still while its
/// rotation was skipped busy, and nothing explained the ring being held.
pub fn ring_stalls(series: &Series, context: &Context, window_ms: i64) -> Vec<RingStall> {
    let mut stalls = Vec::new();
    for (ring, points) in &series.generation {
        let mut index = 0;
        while index < points.len() {
            let generation = points[index].1;
            let mut end = index;
            while end + 1 < points.len() && points[end + 1].1 == generation {
                end += 1;
            }
            // The longest run of unexplained exports inside the stretch.
            let mut run_start: Option<i64> = None;
            for point in &points[index..=end] {
                let at_ms = point.0;
                let busy = series.count(ROTATE_ATTEMPTS, ring, "skipped_busy", at_ms)
                    > series.count(
                        ROTATE_ATTEMPTS,
                        ring,
                        "skipped_busy",
                        at_ms - window_ms.min(60_000),
                    );
                let pins = series.pins_at(at_ms);
                let explained = !busy
                    || context.holds_locks(at_ms).is_some()
                    || (ring == "queue"
                        && !pins.is_empty()
                        && pins
                            .iter()
                            .all(|(lane, _)| context.explain_pin(&lane.queue, at_ms).is_some()));
                match (explained, run_start) {
                    (true, _) => run_start = None,
                    (false, None) => run_start = Some(at_ms),
                    (false, Some(start)) if at_ms - start >= window_ms => {
                        stalls.push(RingStall {
                            ring: ring.clone(),
                            generation,
                            from_ms: start,
                            to_ms: at_ms,
                            rotate_skipped_busy: series.count(
                                ROTATE_ATTEMPTS,
                                ring,
                                "skipped_busy",
                                at_ms,
                            ) - series.count(
                                ROTATE_ATTEMPTS,
                                ring,
                                "skipped_busy",
                                start,
                            ),
                            pins: pins
                                .iter()
                                .map(|(lane, age)| {
                                    format!(
                                        "{}/{}/{} {age:.0}s",
                                        lane.queue, lane.priority, lane.shard
                                    )
                                })
                                .collect(),
                        });
                        break;
                    }
                    (false, Some(_)) => {}
                }
            }
            index = end + 1;
        }
    }
    stalls
}

#[derive(Debug, Clone, Serialize)]
pub struct PruneStarvation {
    pub ring: String,
    pub from_ms: i64,
    pub to_ms: i64,
    pub blocked: f64,
    pub pruned: f64,
}

/// Windows of `window_ms` in which prune was `blocked` at least `min_blocked` times and
/// never `pruned` (#721), unless a session held locks on purpose; consecutive windows
/// merge into one episode.
pub fn prune_starvation(
    series: &Series,
    context: &Context,
    window_ms: i64,
    min_blocked: u64,
) -> Vec<PruneStarvation> {
    let rings: BTreeSet<String> = series
        .counters
        .keys()
        .filter(|(metric, _, _)| metric == PRUNE_ATTEMPTS)
        .map(|(_, ring, _)| ring.clone())
        .collect();
    let mut episodes = Vec::new();
    for ring in rings {
        let times = series.export_times(&ring);
        let mut current: Option<PruneStarvation> = None;
        for &at_ms in &times {
            let Some(&from_ms) = times.iter().rev().find(|time| **time <= at_ms - window_ms) else {
                continue;
            };
            let blocked = series.count(PRUNE_ATTEMPTS, &ring, "blocked", at_ms)
                - series.count(PRUNE_ATTEMPTS, &ring, "blocked", from_ms);
            let pruned = series.count(PRUNE_ATTEMPTS, &ring, "pruned", at_ms)
                - series.count(PRUNE_ATTEMPTS, &ring, "pruned", from_ms);
            let locked = context.spans.iter().any(|span| {
                span.intent == Intent::HoldsLocks
                    && span.queue.is_none()
                    && span.overlaps(from_ms, at_ms, context.grace_ms)
            });
            if blocked >= min_blocked as f64 && pruned == 0.0 && !locked {
                match &mut current {
                    Some(episode) => {
                        episode.to_ms = at_ms;
                        episode.blocked = series.count(PRUNE_ATTEMPTS, &ring, "blocked", at_ms)
                            - series.count(PRUNE_ATTEMPTS, &ring, "blocked", episode.from_ms);
                    }
                    None => {
                        current = Some(PruneStarvation {
                            ring: ring.clone(),
                            from_ms,
                            to_ms: at_ms,
                            blocked,
                            pruned,
                        })
                    }
                }
            } else if let Some(episode) = current.take() {
                episodes.push(episode);
            }
        }
        episodes.extend(current);
    }
    episodes
}

#[derive(Debug, Clone, Serialize)]
pub struct UnexplainedPin {
    pub lane: Lane,
    pub at_ms: i64,
    pub age_seconds: f64,
    /// `(at_s, age_s)` of the lane's exports around it.
    pub series: Vec<(f64, f64)>,
}

/// The first export per lane at which it had pinned the queue ring for more than
/// `max_age_ms` with nothing to explain it.
pub fn unexplained_pins(
    series: &Series,
    context: &Context,
    max_age_ms: i64,
) -> Vec<UnexplainedPin> {
    let mut found = Vec::new();
    for (lane, points) in &series.pins {
        if let Some(&(at_ms, age)) = points.iter().find(|(at_ms, age)| {
            *age * 1000.0 > max_age_ms as f64
                && series.leading(*at_ms)
                && context.explain_pin(&lane.queue, *at_ms).is_none()
        }) {
            found.push(UnexplainedPin {
                lane: lane.clone(),
                at_ms,
                age_seconds: age,
                series: points
                    .iter()
                    .filter(|point| (point.0 - at_ms).abs() <= 180_000)
                    .map(|point| (point.0 as f64 / 1000.0, point.1))
                    .collect(),
            });
        }
    }
    found
}

#[derive(Debug, Clone, Serialize)]
pub struct OpRelease {
    pub op: String,
    pub queue: String,
    pub detail: String,
    pub start_ms: i64,
    pub end_ms: i64,
    /// When the queue was free of every other declared hold after the operation.
    pub released_from_ms: i64,
    pub pinning_lanes: Vec<Lane>,
    /// Seconds from `released_from_ms` until every pinning lane reported zero; `None` when
    /// a lane still pinned at the last export.
    pub release_seconds: Option<f64>,
    pub over_bound: bool,
}

/// For each operator operation, whether its queue's lanes pinned the queue ring around it
/// and how long after it (and after any hold on the queue) the pins took to clear.
pub fn op_releases(series: &Series, spans: &[IntentSpan], bound_ms: i64) -> Vec<OpRelease> {
    let last_export = series
        .pins
        .values()
        .filter_map(|points| points.last().map(|point| point.0))
        .max()
        .unwrap_or(0);
    let mut releases = Vec::new();
    for span in spans {
        let (Intent::OperatorOp(op), Some(queue), Some(end_ms)) =
            (span.intent, &span.queue, span.end_ms)
        else {
            continue;
        };
        let released_from_ms = spans
            .iter()
            .filter(|other| {
                other.id != span.id
                    && other.queue.as_deref() == Some(queue.as_str())
                    && matches!(other.intent, Intent::Paused | Intent::Unserved)
                    && other.overlaps(span.start_ms, end_ms, 0)
            })
            .filter_map(|other| other.end_ms)
            .fold(end_ms, i64::max);
        let pinning: Vec<(&Lane, &Vec<(i64, f64)>)> = series
            .pins
            .iter()
            .filter(|(lane, points)| {
                logical(&lane.queue) == queue
                    && points.iter().any(|(at_ms, age)| {
                        *age > 0.0 && *at_ms >= span.start_ms - 15_000 && *at_ms <= end_ms + 15_000
                    })
            })
            .collect();
        let mut release_seconds = Some(0.0f64);
        for (_, points) in &pinning {
            let cleared = points
                .iter()
                .find(|(at_ms, age)| *at_ms > released_from_ms && *age == 0.0)
                .map(|point| (point.0 - released_from_ms) as f64 / 1000.0);
            release_seconds = match (release_seconds, cleared) {
                (Some(longest), Some(cleared)) => Some(longest.max(cleared)),
                _ => None,
            };
        }
        let over_bound = !pinning.is_empty()
            && match release_seconds {
                Some(seconds) => seconds * 1000.0 > bound_ms as f64,
                None => last_export - released_from_ms > bound_ms,
            };
        releases.push(OpRelease {
            op: op.to_string(),
            queue: queue.clone(),
            detail: span.detail.clone(),
            start_ms: span.start_ms,
            end_ms,
            released_from_ms,
            pinning_lanes: pinning.iter().map(|(lane, _)| (*lane).clone()).collect(),
            release_seconds: if pinning.is_empty() {
                None
            } else {
                release_seconds
            },
            over_bound,
        });
    }
    releases
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProgressVerdict {
    pub not_evaluated: Option<String>,
    pub ring_stalls: Vec<RingStall>,
    pub prune_starvation: Vec<PruneStarvation>,
    pub unexplained_pins: Vec<UnexplainedPin>,
    pub op_releases: Vec<OpRelease>,
}

/// Runs the maintenance-progress checks over the captured rows and raises violations.
pub fn evaluate(shared: &Shared, rows: Result<&[Value], &str>) -> ProgressVerdict {
    let rows = match rows {
        Ok(rows) => rows,
        Err(reason) => {
            return ProgressVerdict {
                not_evaluated: Some(reason.to_string()),
                ..Default::default()
            }
        }
    };
    let checks = &shared.config.checks;
    let series = Series::from_rows(rows, shared.started_epoch_ms);
    let spans = shared.intents.spans();
    let progress = shared.progress.snapshot();
    let context = Context {
        spans: &spans,
        progress: &progress,
        grace_ms: checks.pin_release.as_millis() as i64,
        progress_window_ms: checks.pinned_age_max.as_millis() as i64,
    };
    let verdict = ProgressVerdict {
        not_evaluated: None,
        ring_stalls: ring_stalls(&series, &context, checks.ring_stall.as_millis() as i64),
        prune_starvation: prune_starvation(
            &series,
            &context,
            checks.prune_blocked_window.as_millis() as i64,
            checks.prune_blocked_min,
        ),
        unexplained_pins: unexplained_pins(
            &series,
            &context,
            checks.pinned_age_max.as_millis() as i64,
        ),
        op_releases: op_releases(&series, &spans, checks.pin_release.as_millis() as i64),
    };
    for stall in &verdict.ring_stalls {
        shared.violation_at(
            "ring_generation_stalled",
            stall.from_ms,
            format!(
                "the {} ring stayed at generation {} from {:.0}s to {:.0}s with rotation skipped busy {} time(s), and no declared intent, held locks or claim progress explains it",
                stall.ring, stall.generation, stall.from_ms as f64 / 1000.0, stall.to_ms as f64 / 1000.0, stall.rotate_skipped_busy
            ),
            vec![serde_json::to_string(stall).unwrap_or_default()],
        );
    }
    for episode in &verdict.prune_starvation {
        shared.violation_at(
            "prune_starved",
            episode.from_ms,
            format!(
                "{} ring prune was blocked {} time(s) from {:.0}s to {:.0}s without pruning a slot (the #721 signature)",
                episode.ring, episode.blocked, episode.from_ms as f64 / 1000.0, episode.to_ms as f64 / 1000.0
            ),
            vec![serde_json::to_string(episode).unwrap_or_default()],
        );
    }
    for pin in &verdict.unexplained_pins {
        let intents: Vec<String> = spans
            .iter()
            .filter(|span| span.queue.is_some() && span.applies_to(&pin.lane.queue))
            .map(|span| serde_json::to_string(span).unwrap_or_default())
            .take(5)
            .collect();
        let mut rows = vec![serde_json::to_string(pin).unwrap_or_default()];
        rows.extend(intents);
        shared.violation_at(
            "unexplained_ring_pin",
            pin.at_ms,
            format!(
                "ready lane {}/{}/{} pinned the queue ring with a {:.0} s old row at {:.0}s; no declared intent covers its queue and its claim cursor had not moved for {} s",
                pin.lane.queue,
                pin.lane.priority,
                pin.lane.shard,
                pin.age_seconds,
                pin.at_ms as f64 / 1000.0,
                checks.pinned_age_max.as_secs()
            ),
            rows,
        );
    }
    for release in verdict
        .op_releases
        .iter()
        .filter(|release| release.over_bound)
    {
        shared.violation_at(
            "pin_not_released",
            release.released_from_ms,
            format!(
                "after {} on {} ({}), its lanes kept pinning the queue ring for {} (bound {} s)",
                release.op,
                release.queue,
                release.detail,
                release
                    .release_seconds
                    .map_or("the rest of the run".to_string(), |seconds| format!(
                        "{seconds:.0} s"
                    )),
                checks.pin_release.as_secs()
            ),
            vec![serde_json::to_string(release).unwrap_or_default()],
        );
    }
    verdict
}

pub fn render(out: &mut String, verdict: &ProgressVerdict, shared: &Shared) {
    let checks = &shared.config.checks;
    let _ = writeln!(out, "## Maintenance progress");
    if let Some(reason) = &verdict.not_evaluated {
        let _ = writeln!(out, "- not evaluated: {reason}\n");
        return;
    }
    let _ = writeln!(
        out,
        "- ring generation standing still while rotation is skipped busy, unexplained for {} s: {}",
        checks.ring_stall.as_secs(),
        if verdict.ring_stalls.is_empty() {
            "none".to_string()
        } else {
            format!("{} episode(s)", verdict.ring_stalls.len())
        }
    );
    let _ = writeln!(
        out,
        "- prune blocked at least {} times with nothing pruned over {} s (#721): {}",
        checks.prune_blocked_min,
        checks.prune_blocked_window.as_secs(),
        if verdict.prune_starvation.is_empty() {
            "none".to_string()
        } else {
            format!("{} episode(s)", verdict.prune_starvation.len())
        }
    );
    let _ = writeln!(
        out,
        "- ready lanes pinning the queue ring past {} s with no intent or claim progress: {}",
        checks.pinned_age_max.as_secs(),
        if verdict.unexplained_pins.is_empty() {
            "none".to_string()
        } else {
            format!("{} lane(s)", verdict.unexplained_pins.len())
        }
    );
    let pinned: Vec<&OpRelease> = verdict
        .op_releases
        .iter()
        .filter(|release| !release.pinning_lanes.is_empty())
        .collect();
    let _ = writeln!(
        out,
        "- operator operations: {} recorded, {} on a lane pinning the queue ring (release bound {} s)",
        verdict.op_releases.len(),
        pinned.len(),
        checks.pin_release.as_secs()
    );
    for release in pinned.iter().take(20) {
        let _ = writeln!(
            out,
            "    - [{:.0}s] {} on {} ({}): took {:.1} s, {} lane(s) pinning, released {}",
            release.start_ms as f64 / 1000.0,
            release.op,
            release.queue,
            release.detail,
            (release.end_ms - release.start_ms) as f64 / 1000.0,
            release.pinning_lanes.len(),
            release
                .release_seconds
                .map_or("not by the last export".to_string(), |seconds| format!(
                    "{seconds:.0} s after the queue was free"
                ))
        );
    }
    let _ = writeln!(out);
}

#[cfg(test)]
mod tests {
    use super::super::intent::IntentLog;
    use super::*;
    use serde_json::json;

    fn row(at_s: i64, name: &str, attributes: Value, value: f64) -> Value {
        json!({"at_ms": at_s * 1000, "name": name, "attributes": attributes, "value": value})
    }

    fn rotate(at_s: i64, ring: &str, outcome: &str, total: f64) -> Value {
        row(
            at_s,
            ROTATE_ATTEMPTS,
            json!({"awa.ring": ring, "awa.ring.outcome": outcome, "awa.ring.blocker": "none"}),
            total,
        )
    }

    fn prune(at_s: i64, ring: &str, outcome: &str, total: f64) -> Value {
        row(
            at_s,
            PRUNE_ATTEMPTS,
            json!({"awa.ring": ring, "awa.ring.outcome": outcome, "awa.ring.reason": "none"}),
            total,
        )
    }

    fn generation(at_s: i64, ring: &str, value: f64) -> Value {
        row(at_s, RING_GENERATION, json!({"awa.ring": ring}), value)
    }

    fn pin(at_s: i64, queue: &str, age: f64) -> Value {
        row(
            at_s,
            PINNED_READY_AGE,
            json!({"awa.ring": "queue", "awa.job.queue": queue, "awa.job.priority": 2, "awa.queue.enqueue_shard": 0}),
            age,
        )
    }

    /// A queue ring stuck at one generation for `seconds`, skipped busy on every tick,
    /// with `queue` pinning it from the start.
    fn stuck_ring(seconds: i64, queue: Option<&str>) -> Vec<Value> {
        let mut rows = Vec::new();
        for (step, at_s) in (0..=seconds).step_by(10).enumerate() {
            let step = step as f64;
            rows.push(generation(at_s, "queue", 40.0));
            rows.push(rotate(at_s, "queue", "skipped_busy", step * 10.0));
            rows.push(prune(at_s, "queue", "skipped_active", step * 10.0));
            if let Some(queue) = queue {
                rows.push(pin(at_s, queue, 30.0 + at_s as f64));
            }
        }
        rows
    }

    fn context<'a>(
        spans: &'a [IntentSpan],
        progress: &'a HashMap<String, Vec<i64>>,
    ) -> Context<'a> {
        Context {
            spans,
            progress,
            grace_ms: 60_000,
            progress_window_ms: 120_000,
        }
    }

    #[test]
    fn an_unexplained_stuck_ring_fails_and_an_idle_one_passes() {
        let series = Series::from_rows(&stuck_ring(300, Some("tom_7")), 0);
        let progress = HashMap::new();
        let stalls = ring_stalls(&series, &context(&[], &progress), 180_000);
        assert_eq!(stalls.len(), 1);
        assert_eq!(stalls[0].ring, "queue");
        // The first export has no earlier busy skip to compare against.
        assert_eq!(stalls[0].from_ms, 10_000);

        let idle: Vec<Value> = (0..=30)
            .flat_map(|step| {
                [
                    generation(step * 10, "claim", 9.0),
                    rotate(step * 10, "claim", "skipped_idle", step as f64 * 10.0),
                ]
            })
            .collect();
        let series = Series::from_rows(&idle, 0);
        assert!(ring_stalls(&series, &context(&[], &progress), 180_000).is_empty());
    }

    #[test]
    fn a_ring_held_by_a_paused_queue_or_a_progressing_backlog_passes() {
        let series = Series::from_rows(&stuck_ring(300, Some("alex_a")), 0);
        let log = IntentLog::default();
        log.window(Some("alex_a"), Intent::Paused, "pause", 0, 290_000);
        let spans = log.spans();
        let progress = HashMap::new();
        assert!(ring_stalls(&series, &context(&spans, &progress), 180_000).is_empty());
        assert!(unexplained_pins(&series, &context(&spans, &progress), 120_000).is_empty());

        let moving = HashMap::from([(
            "alex_a".to_string(),
            (0..=30).map(|step| step * 10_000).collect::<Vec<i64>>(),
        )]);
        assert!(ring_stalls(&series, &context(&[], &moving), 180_000).is_empty());
        assert!(unexplained_pins(&series, &context(&[], &moving), 120_000).is_empty());
    }

    #[test]
    fn an_old_pin_without_intent_or_progress_fails_once_per_lane() {
        let series = Series::from_rows(&stuck_ring(300, Some("tom_7")), 0);
        let log = IntentLog::default();
        log.window(Some("other"), Intent::Paused, "unrelated", 0, 300_000);
        let spans = log.spans();
        let progress = HashMap::from([("tom_7".to_string(), vec![5_000])]);
        let pins = unexplained_pins(&series, &context(&spans, &progress), 120_000);
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].lane.queue, "tom_7");
        // Age passes 120 s at t=100 s, but the cursor moved at 5 s, within the window until t=120 s.
        assert_eq!(pins[0].at_ms, 130_000);
    }

    #[test]
    fn pins_are_not_judged_while_another_process_leads() {
        let mut rows = stuck_ring(300, Some("tom_7"));
        rows.retain(|row| row["name"] != PRUNE_ATTEMPTS);
        let series = Series::from_rows(&rows, 0);
        let progress = HashMap::new();
        assert!(unexplained_pins(&series, &context(&[], &progress), 120_000).is_empty());
    }

    #[test]
    fn blocked_prunes_without_a_prune_are_the_721_signature() {
        let mut rows = Vec::new();
        for step in 0..=40 {
            let at_s = step * 10;
            let blocked = if at_s < 100 { 0.0 } else { (at_s - 100) as f64 };
            let pruned = if at_s < 100 { step as f64 } else { 10.0 };
            rows.push(prune(at_s, "queue", "blocked", blocked));
            rows.push(prune(at_s, "queue", "pruned", pruned));
        }
        let series = Series::from_rows(&rows, 0);
        let progress = HashMap::new();
        let episodes = prune_starvation(&series, &context(&[], &progress), 120_000, 10);
        assert_eq!(episodes.len(), 1);
        assert_eq!(episodes[0].ring, "queue");
        assert!(episodes[0].from_ms >= 100_000 && episodes[0].to_ms == 400_000);

        let log = IntentLog::default();
        log.window(None, Intent::Disruption, "chaos window", 0, 400_000);
        let spans = log.spans();
        assert_eq!(
            prune_starvation(&series, &context(&spans, &progress), 120_000, 10).len(),
            1,
            "a chaos window does not excuse a starved prune"
        );
        log.window(None, Intent::HoldsLocks, "completion stall", 0, 400_000);
        let spans = log.spans();
        assert!(prune_starvation(&series, &context(&spans, &progress), 120_000, 10).is_empty());
    }

    #[test]
    fn blocked_prunes_that_still_prune_pass() {
        let rows: Vec<Value> = (0..=40)
            .flat_map(|step| {
                [
                    prune(step * 10, "claim", "blocked", step as f64 * 3.0),
                    prune(step * 10, "claim", "pruned", (step / 3) as f64),
                ]
            })
            .collect();
        let series = Series::from_rows(&rows, 0);
        let progress = HashMap::new();
        assert!(prune_starvation(&series, &context(&[], &progress), 120_000, 10).is_empty());
    }

    #[test]
    fn operator_op_release_is_measured_from_the_end_of_the_last_hold() {
        let log = IntentLog::default();
        log.window(Some("alex_a"), Intent::Paused, "hold", 0, 70_000);
        log.window(
            Some("alex_a"),
            Intent::OperatorOp("move"),
            "move 900 rows",
            50_000,
            65_000,
        );
        log.window(
            Some("tom_3"),
            Intent::OperatorOp("cancel"),
            "cancel 8",
            100_000,
            101_000,
        );
        let spans = log.spans();
        let mut rows = Vec::new();
        for at_s in (0..=200).step_by(10) {
            let age = if at_s <= 90 { 30.0 + at_s as f64 } else { 0.0 };
            rows.push(pin(at_s, "alex_a", age));
        }
        let series = Series::from_rows(&rows, 0);
        let releases = op_releases(&series, &spans, 60_000);
        assert_eq!(releases.len(), 2);
        let moved = &releases[0];
        assert_eq!(moved.released_from_ms, 70_000);
        assert_eq!(moved.pinning_lanes.len(), 1);
        assert_eq!(moved.release_seconds, Some(30.0));
        assert!(!moved.over_bound);
        assert!(releases[1].pinning_lanes.is_empty());

        let stuck: Vec<Value> = (0..=200)
            .step_by(10)
            .map(|at_s| pin(at_s, "alex_a", 30.0 + at_s as f64))
            .collect();
        let series = Series::from_rows(&stuck, 0);
        let releases = op_releases(&series, &spans, 60_000);
        assert_eq!(releases[0].release_seconds, None);
        assert!(releases[0].over_bound);
    }
}
