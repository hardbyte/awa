//! Rolling, in-memory history of queue depth for the admin overview.
//!
//! Job state counts are point-in-time: nothing in the schema records how a
//! queue's backlog moved, and retained `created_at` cannot stand in for an
//! arrival rate because compacted terminal rows fall back to `finalized_at`
//! once their ready entry rotates away. Rather than add a migration, the UI
//! server samples the same `queue_overviews` read the queues endpoint already
//! caches and keeps the last window in memory.
//!
//! Consequences the frontend has to be honest about: history starts when this
//! process starts, a failed sample leaves a gap, and each `awa serve` replica
//! keeps (and pays for) its own copy.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use awa_model::admin::{self, QueueOverview};
use chrono::{DateTime, Utc};
use moka::future::Cache;
use serde::Serialize;
use sqlx::PgPool;

use crate::cache::CacheError;

/// How much queue history the UI server keeps, and how often it samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueHistoryConfig {
    /// Retained window. `Duration::ZERO` disables sampling entirely.
    pub window: Duration,
    /// Minimum spacing between retained samples. The background sampler
    /// ticks at this interval; viewer-driven cache fills in between are
    /// dropped so a fast-polling tab cannot inflate memory.
    pub sample_interval: Duration,
}

impl Default for QueueHistoryConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(60 * 60),
            sample_interval: Duration::from_secs(15),
        }
    }
}

impl QueueHistoryConfig {
    pub fn disabled() -> Self {
        Self {
            window: Duration::ZERO,
            ..Self::default()
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.window.is_zero() && !self.sample_interval.is_zero()
    }

    /// Width of each bucket in the API response: one minute, or the sample
    /// interval when sampling is coarser than that.
    fn bucket(&self) -> Duration {
        self.sample_interval.max(Duration::from_secs(60))
    }
}

#[derive(Clone, Copy, Debug)]
struct QueuePoint {
    available: i64,
    running: i64,
    lag_seconds: Option<f64>,
}

#[derive(Debug)]
struct Sample {
    at: DateTime<Utc>,
    /// Sorted by queue name so the next sample can reuse the interned names.
    queues: Vec<(Arc<str>, QueuePoint)>,
}

#[derive(Debug)]
pub struct QueueHistory {
    config: QueueHistoryConfig,
    samples: Mutex<VecDeque<Sample>>,
}

/// Bucketed queue depth over the retained window, oldest bucket first.
#[derive(Debug, Serialize)]
pub struct QueueHistorySnapshot {
    pub enabled: bool,
    pub window_secs: u64,
    pub sample_interval_secs: u64,
    pub bucket_secs: u64,
    /// Oldest retained sample. History never reaches back past the start of
    /// this server process.
    pub observed_since: Option<DateTime<Utc>>,
    /// End time of each bucket. Every series below has the same length; a
    /// bucket with no sample for that queue is `null`.
    pub buckets: Vec<DateTime<Utc>>,
    pub queues: BTreeMap<String, QueueSeries>,
}

/// Last observed value per bucket.
#[derive(Debug, Default, Serialize)]
pub struct QueueSeries {
    pub available: Vec<Option<i64>>,
    pub running: Vec<Option<i64>>,
    pub lag_seconds: Vec<Option<f64>>,
}

impl QueueHistory {
    pub fn new(config: QueueHistoryConfig) -> Self {
        Self {
            config,
            samples: Mutex::new(VecDeque::new()),
        }
    }

    pub fn config(&self) -> QueueHistoryConfig {
        self.config
    }

    pub fn record(&self, at: DateTime<Utc>, queues: &[QueueOverview]) {
        if !self.config.is_enabled() {
            return;
        }
        let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(last) = samples.back() {
            // 90% of the interval so a sampler tick that lands a little early
            // is still kept.
            let min_gap = self.config.sample_interval.mul_f64(0.9);
            if (at - last.at).to_std().map_or(true, |gap| gap < min_gap) {
                return;
            }
        }

        let previous = samples.back();
        let mut points: Vec<(Arc<str>, QueuePoint)> = queues
            .iter()
            .map(|queue| {
                let name = previous
                    .and_then(|sample| {
                        sample
                            .queues
                            .binary_search_by(|(name, _)| name.as_ref().cmp(queue.queue.as_str()))
                            .ok()
                            .map(|index| Arc::clone(&sample.queues[index].0))
                    })
                    .unwrap_or_else(|| Arc::from(queue.queue.as_str()));
                (
                    name,
                    QueuePoint {
                        available: queue.available,
                        running: queue.running,
                        lag_seconds: queue.lag_seconds,
                    },
                )
            })
            .collect();
        points.sort_by(|left, right| left.0.cmp(&right.0));
        samples.push_back(Sample { at, queues: points });

        while samples.front().is_some_and(|oldest| {
            (at - oldest.at).to_std().unwrap_or_default() > self.config.window
        }) {
            samples.pop_front();
        }
    }

    pub fn snapshot(&self) -> QueueHistorySnapshot {
        let bucket = self.config.bucket();
        let mut snapshot = QueueHistorySnapshot {
            enabled: self.config.is_enabled(),
            window_secs: self.config.window.as_secs(),
            sample_interval_secs: self.config.sample_interval.as_secs(),
            bucket_secs: bucket.as_secs(),
            observed_since: None,
            buckets: Vec::new(),
            queues: BTreeMap::new(),
        };
        let samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        let (Some(oldest), Some(latest)) = (samples.front(), samples.back()) else {
            return snapshot;
        };
        snapshot.observed_since = Some(oldest.at);

        let count = self
            .config
            .window
            .as_millis()
            .div_ceil(bucket.as_millis().max(1))
            .max(1) as usize;
        let bucket_ms = bucket.as_millis().max(1) as i64;
        let anchor = latest.at;
        snapshot.buckets = (0..count)
            .map(|index| {
                anchor - chrono::Duration::milliseconds(bucket_ms * (count - 1 - index) as i64)
            })
            .collect();

        for sample in samples.iter() {
            let age_ms = (anchor - sample.at).num_milliseconds().max(0);
            let Some(index) = count.checked_sub(1 + (age_ms / bucket_ms) as usize) else {
                continue;
            };
            for (name, point) in &sample.queues {
                let series =
                    snapshot
                        .queues
                        .entry(name.to_string())
                        .or_insert_with(|| QueueSeries {
                            available: vec![None; count],
                            running: vec![None; count],
                            lag_seconds: vec![None; count],
                        });
                // Samples are oldest-first, so the last write wins: each
                // bucket reports the depth at the end of its interval.
                series.available[index] = Some(point.available);
                series.running[index] = Some(point.running);
                series.lag_seconds[index] = point.lag_seconds;
            }
        }
        snapshot
    }
}

/// Read queue overviews through the shared cache, recording a history sample
/// whenever the read actually reaches the database.
pub(crate) async fn cached_queue_overviews(
    pool: &PgPool,
    cache: &Cache<(), Vec<QueueOverview>>,
    history: &QueueHistory,
) -> Result<Vec<QueueOverview>, Arc<CacheError>> {
    cache
        .try_get_with((), async {
            let queues = admin::queue_overviews(pool)
                .await
                .map_err(CacheError::from)?;
            history.record(Utc::now(), &queues);
            Ok(queues)
        })
        .await
}

/// Sample queue depth in the background so history exists before anyone opens
/// the overview. The task holds only a weak reference to the history and exits
/// once the router that owns it is dropped.
pub(crate) fn spawn_sampler(
    history: &Arc<QueueHistory>,
    pool: PgPool,
    cache: Cache<(), Vec<QueueOverview>>,
) {
    let config = history.config();
    if !config.is_enabled() {
        return;
    }
    let history = Arc::downgrade(history);
    tokio::spawn(async move {
        // The first tick waits a full interval: startup already issues enough
        // queries, and short-lived routers (tests, embedders probing the API)
        // should not pay for a sample they will never read.
        let start = tokio::time::Instant::now() + config.sample_interval;
        let mut ticker = tokio::time::interval_at(start, config.sample_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let Some(history) = history.upgrade() else {
                break;
            };
            if let Err(err) = cached_queue_overviews(&pool, &cache, &history).await {
                tracing::debug!(error = %err.0, "queue history sample failed; leaving a gap");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(name: &str, available: i64) -> QueueOverview {
        QueueOverview {
            queue: name.to_string(),
            display_name: None,
            description: None,
            owner: None,
            docs_url: None,
            tags: Vec::new(),
            extra: serde_json::Value::Null,
            descriptor_last_seen_at: None,
            descriptor_stale: false,
            descriptor_mismatch: false,
            total_queued: available,
            scheduled: 0,
            available,
            retryable: 0,
            running: 1,
            failed: 0,
            waiting_external: 0,
            completed_last_hour: 0,
            lag_seconds: Some(available as f64),
            paused: false,
        }
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
    }

    fn history(window_secs: u64, interval_secs: u64) -> QueueHistory {
        QueueHistory::new(QueueHistoryConfig {
            window: Duration::from_secs(window_secs),
            sample_interval: Duration::from_secs(interval_secs),
        })
    }

    #[test]
    fn disabled_history_records_nothing() {
        let history = QueueHistory::new(QueueHistoryConfig::disabled());
        history.record(at(0), &[queue("email", 5)]);
        let snapshot = history.snapshot();
        assert!(!snapshot.enabled);
        assert!(snapshot.buckets.is_empty());
        assert!(snapshot.queues.is_empty());
    }

    #[test]
    fn samples_closer_than_the_interval_are_dropped() {
        let history = history(600, 15);
        history.record(at(0), &[queue("email", 1)]);
        history.record(at(5), &[queue("email", 2)]);
        history.record(at(12), &[queue("email", 3)]);
        history.record(at(30), &[queue("email", 4)]);
        assert_eq!(history.samples.lock().unwrap().len(), 2);
    }

    #[test]
    fn samples_older_than_the_window_are_pruned() {
        let history = history(120, 15);
        for minute in 0..5 {
            history.record(at(minute * 60), &[queue("email", minute)]);
        }
        let snapshot = history.snapshot();
        assert_eq!(snapshot.observed_since, Some(at(120)));
        assert_eq!(snapshot.buckets.len(), 2);
        assert_eq!(
            snapshot.queues["email"].available,
            vec![Some(3), Some(4)],
            "the 120s-old sample sits on the window edge and falls outside both buckets"
        );
    }

    #[test]
    fn buckets_keep_the_latest_sample_and_mark_gaps() {
        let history = history(300, 15);
        // Buckets end at the latest sample (240s), so they cover
        // (-60,0], (0,60], (60,120], (120,180] and (180,240].
        history.record(at(10), &[queue("email", 10)]);
        history.record(at(30), &[queue("email", 12)]);
        // No samples for 90s: a failed sampler or a restarted DB.
        history.record(at(150), &[queue("email", 30), queue("reports", 2)]);
        history.record(at(240), &[queue("email", 31), queue("reports", 3)]);

        let snapshot = history.snapshot();
        assert_eq!(snapshot.bucket_secs, 60);
        assert_eq!(snapshot.buckets.len(), 5);
        assert_eq!(snapshot.buckets.last(), Some(&at(240)));
        assert_eq!(
            snapshot.queues["email"].available,
            vec![None, Some(12), None, Some(30), Some(31)]
        );
        assert_eq!(
            snapshot.queues["reports"].available,
            vec![None, None, None, Some(2), Some(3)]
        );
        assert_eq!(snapshot.queues["email"].lag_seconds[4], Some(31.0));
    }

    #[test]
    fn queue_names_are_shared_across_samples() {
        let history = history(600, 15);
        history.record(at(0), &[queue("email", 1)]);
        history.record(at(15), &[queue("email", 2)]);
        let samples = history.samples.lock().unwrap();
        assert!(Arc::ptr_eq(
            &samples[0].queues[0].0,
            &samples[1].queues[0].0
        ));
    }

    #[test]
    fn coarse_sampling_widens_the_bucket() {
        let history = history(600, 120);
        history.record(at(0), &[queue("email", 1)]);
        let snapshot = history.snapshot();
        assert_eq!(snapshot.queues["email"].available[4], Some(1));
        assert_eq!(snapshot.bucket_secs, 120);
        assert_eq!(snapshot.buckets.len(), 5);
    }
}
