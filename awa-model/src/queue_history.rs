//! Per-minute queue and fleet history (v053, #543).
//!
//! The elected maintenance leader samples queue depth and differences the
//! cumulative per-queue counters runtimes publish in their snapshots
//! ([`crate::admin::QueueOutcomeCounters`]) into per-minute deltas. The admin
//! UI reads the result to judge whether each queue is keeping up.
//!
//! Counter deltas are `None` ("unknown") rather than a guess whenever the
//! leader cannot account for every live runtime serving a queue: on its first
//! sample, for a runtime it has not seen before that started earlier, and for
//! runtimes that predate the counters. A runtime that dies between samples
//! loses at most its last snapshot interval of counts.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::admin::{QueueOutcomeCounters, QueueOverview, QueueRuntimeMode, RuntimeInstance};
use crate::error::AwaError;

/// Counter deltas for one queue over one sample interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct QueueFlow {
    pub claimed: i64,
    pub completed: i64,
    pub failed_attempts: i64,
    pub retried: i64,
    pub rate_limited: i64,
    pub claim_wait_ms: i64,
}

impl QueueFlow {
    fn between(previous: Option<&QueueOutcomeCounters>, current: &QueueOutcomeCounters) -> Self {
        let delta = |now: u64, before: u64| i64::try_from(now - before).unwrap_or(i64::MAX);
        match previous {
            // Any counter going backwards means the runtime restarted (or its
            // counters did); everything it reports now is new since then.
            Some(prev)
                if current.claimed >= prev.claimed
                    && current.completed >= prev.completed
                    && current.failed >= prev.failed
                    && current.retried >= prev.retried
                    && current.rate_limited >= prev.rate_limited
                    && current.claim_wait_ms >= prev.claim_wait_ms =>
            {
                Self {
                    claimed: delta(current.claimed, prev.claimed),
                    completed: delta(current.completed, prev.completed),
                    failed_attempts: delta(current.failed, prev.failed),
                    retried: delta(current.retried, prev.retried),
                    rate_limited: delta(current.rate_limited, prev.rate_limited),
                    claim_wait_ms: delta(current.claim_wait_ms, prev.claim_wait_ms),
                }
            }
            _ => Self::between(Some(&QueueOutcomeCounters::default()), current),
        }
    }

    fn add(&mut self, other: Self) {
        self.claimed = self.claimed.saturating_add(other.claimed);
        self.completed = self.completed.saturating_add(other.completed);
        self.failed_attempts = self.failed_attempts.saturating_add(other.failed_attempts);
        self.retried = self.retried.saturating_add(other.retried);
        self.rate_limited = self.rate_limited.saturating_add(other.rate_limited);
        self.claim_wait_ms = self.claim_wait_ms.saturating_add(other.claim_wait_ms);
    }
}

/// What the leader remembers between samples. Lost on leader change, which
/// makes the next sample's counter deltas unknown.
#[derive(Debug, Default)]
pub struct CounterBaseline {
    sampled_at: Option<DateTime<Utc>>,
    readings: HashMap<(Uuid, String), QueueOutcomeCounters>,
}

/// Per-queue runtime coverage for one sample.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueCoverage {
    pub flow: Option<QueueFlow>,
    pub live_workers: i32,
    /// Summed `max_workers` across live hard-reserved runtimes. `None` when
    /// any live runtime runs the queue in weighted mode, where capacity is
    /// shared and cannot be attributed to one queue.
    pub capacity: Option<i64>,
}

impl CounterBaseline {
    /// Difference the live runtimes' counters against the previous sample and
    /// advance the baseline to `now`.
    pub fn advance(
        &mut self,
        now: DateTime<Utc>,
        instances: &[RuntimeInstance],
    ) -> HashMap<String, QueueCoverage> {
        let mut coverage: HashMap<String, QueueCoverage> = HashMap::new();
        let mut unknown: BTreeSet<String> = BTreeSet::new();
        let mut readings = HashMap::new();

        for instance in instances.iter().filter(|instance| !instance.stale) {
            for snapshot in &instance.queues {
                let entry =
                    coverage
                        .entry(snapshot.queue.clone())
                        .or_insert_with(|| QueueCoverage {
                            flow: Some(QueueFlow::default()),
                            live_workers: 0,
                            capacity: Some(0),
                        });
                entry.live_workers += 1;
                entry.capacity = match (entry.capacity, snapshot.config.mode) {
                    (Some(total), QueueRuntimeMode::HardReserved) => {
                        Some(total + i64::from(snapshot.config.max_workers.unwrap_or(0)))
                    }
                    _ => None,
                };

                let Some(current) = snapshot.counters else {
                    unknown.insert(snapshot.queue.clone());
                    continue;
                };
                let key = (instance.instance_id, snapshot.queue.clone());
                let previous = self.readings.get(&key);
                let started_since_last_sample = self
                    .sampled_at
                    .is_some_and(|sampled_at| instance.started_at >= sampled_at);
                if previous.is_none() && !started_since_last_sample {
                    // Counts from before our baseline: we cannot tell how many
                    // fall in this interval.
                    unknown.insert(snapshot.queue.clone());
                } else if let Some(flow) = entry.flow.as_mut() {
                    flow.add(QueueFlow::between(previous, &current));
                }
                readings.insert(key, current);
            }
        }
        for queue in unknown {
            if let Some(entry) = coverage.get_mut(&queue) {
                entry.flow = None;
            }
        }
        self.sampled_at = Some(now);
        self.readings = readings;
        coverage
    }
}

/// One row of `awa.queue_history`.
#[derive(Debug, Clone, PartialEq, Serialize, sqlx::FromRow)]
pub struct QueueHistoryRow {
    pub queue: String,
    pub bucket: DateTime<Utc>,
    pub available: i64,
    pub running: i64,
    pub scheduled: i64,
    pub retryable: i64,
    pub failed: i64,
    pub waiting_external: i64,
    pub lag_seconds: Option<f64>,
    pub paused: bool,
    pub claimed: Option<i64>,
    pub completed: Option<i64>,
    pub failed_attempts: Option<i64>,
    pub retried: Option<i64>,
    pub rate_limited: Option<i64>,
    pub claim_wait_ms: Option<i64>,
    pub live_workers: i32,
    pub capacity: Option<i64>,
}

/// One row of `awa.fleet_history`.
#[derive(Debug, Clone, PartialEq, Serialize, sqlx::FromRow)]
pub struct FleetHistoryRow {
    pub bucket: DateTime<Utc>,
    pub live_instances: i32,
    pub healthy_instances: i32,
    pub stale_instances: i32,
    pub leader_instance_id: Option<Uuid>,
    pub versions: Vec<String>,
}

/// Build the rows for one sample from current queue depth and runtime state.
pub fn build_sample(
    bucket: DateTime<Utc>,
    queues: &[QueueOverview],
    coverage: &HashMap<String, QueueCoverage>,
    instances: &[RuntimeInstance],
) -> (Vec<QueueHistoryRow>, FleetHistoryRow) {
    let rows = queues
        .iter()
        .map(|queue| {
            let covered = coverage.get(&queue.queue);
            let flow = covered.and_then(|c| c.flow);
            QueueHistoryRow {
                queue: queue.queue.clone(),
                bucket,
                available: queue.available,
                running: queue.running,
                scheduled: queue.scheduled,
                retryable: queue.retryable,
                failed: queue.failed,
                waiting_external: queue.waiting_external,
                lag_seconds: queue.lag_seconds,
                paused: queue.paused,
                claimed: flow.map(|f| f.claimed),
                completed: flow.map(|f| f.completed),
                failed_attempts: flow.map(|f| f.failed_attempts),
                retried: flow.map(|f| f.retried),
                rate_limited: flow.map(|f| f.rate_limited),
                claim_wait_ms: flow.map(|f| f.claim_wait_ms),
                // A queue nobody serves has no unknown counts: nothing ran.
                live_workers: covered.map_or(0, |c| c.live_workers),
                capacity: covered.and_then(|c| c.capacity),
            }
        })
        .map(|mut row| {
            if row.live_workers == 0 {
                row.claimed = Some(0);
                row.completed = Some(0);
                row.failed_attempts = Some(0);
                row.retried = Some(0);
                row.rate_limited = Some(0);
                row.claim_wait_ms = Some(0);
            }
            row
        })
        .collect();

    let live: Vec<&RuntimeInstance> = instances.iter().filter(|i| !i.stale).collect();
    let versions: BTreeSet<String> = live.iter().map(|i| i.version.clone()).collect();
    let fleet = FleetHistoryRow {
        bucket,
        live_instances: live.len() as i32,
        healthy_instances: live.iter().filter(|i| i.healthy).count() as i32,
        stale_instances: (instances.len() - live.len()) as i32,
        leader_instance_id: live.iter().find(|i| i.leader).map(|i| i.instance_id),
        versions: versions.into_iter().collect(),
    };
    (rows, fleet)
}

/// Whether v053 is applied. Lets callers report "no history" on an older
/// schema instead of failing.
pub async fn history_installed(pool: &PgPool) -> Result<bool, AwaError> {
    let installed: bool = sqlx::query_scalar(
        "SELECT to_regclass('awa.queue_history') IS NOT NULL \
         AND to_regclass('awa.fleet_history') IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    Ok(installed)
}

/// Write one sample. A bucket that already has rows (a second leader in the
/// same minute after failover) keeps the first write.
pub async fn record_sample(
    pool: &PgPool,
    rows: &[QueueHistoryRow],
    fleet: &FleetHistoryRow,
) -> Result<(), AwaError> {
    let mut tx = pool.begin().await?;
    if !rows.is_empty() {
        let column =
            |f: fn(&QueueHistoryRow) -> Option<i64>| rows.iter().map(f).collect::<Vec<_>>();
        sqlx::query(
            r#"
            INSERT INTO awa.queue_history (
                queue, bucket, available, running, scheduled, retryable, failed,
                waiting_external, lag_seconds, paused, claimed, completed,
                failed_attempts, retried, rate_limited, claim_wait_ms,
                live_workers, capacity
            )
            SELECT * FROM UNNEST(
                $1::text[], $2::timestamptz[], $3::bigint[], $4::bigint[], $5::bigint[],
                $6::bigint[], $7::bigint[], $8::bigint[], $9::float8[], $10::bool[],
                $11::bigint[], $12::bigint[], $13::bigint[], $14::bigint[], $15::bigint[],
                $16::bigint[], $17::int[], $18::bigint[]
            )
            ON CONFLICT (queue, bucket) DO NOTHING
            "#,
        )
        .bind(rows.iter().map(|r| r.queue.clone()).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.bucket).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.available).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.running).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.scheduled).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.retryable).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.failed).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.waiting_external).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.lag_seconds).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.paused).collect::<Vec<_>>())
        .bind(column(|r| r.claimed))
        .bind(column(|r| r.completed))
        .bind(column(|r| r.failed_attempts))
        .bind(column(|r| r.retried))
        .bind(column(|r| r.rate_limited))
        .bind(column(|r| r.claim_wait_ms))
        .bind(rows.iter().map(|r| r.live_workers).collect::<Vec<_>>())
        .bind(column(|r| r.capacity))
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        r#"
        INSERT INTO awa.fleet_history (
            bucket, live_instances, healthy_instances, stale_instances,
            leader_instance_id, versions
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (bucket) DO NOTHING
        "#,
    )
    .bind(fleet.bucket)
    .bind(fleet.live_instances)
    .bind(fleet.healthy_instances)
    .bind(fleet.stale_instances)
    .bind(fleet.leader_instance_id)
    .bind(&fleet.versions)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Delete history older than `retention`. Returns rows removed.
pub async fn prune_history(pool: &PgPool, retention: std::time::Duration) -> Result<u64, AwaError> {
    let secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
    let queues = sqlx::query(
        "DELETE FROM awa.queue_history WHERE bucket < now() - make_interval(secs => $1)",
    )
    .bind(secs as f64)
    .execute(pool)
    .await?
    .rows_affected();
    let fleet = sqlx::query(
        "DELETE FROM awa.fleet_history WHERE bucket < now() - make_interval(secs => $1)",
    )
    .bind(secs as f64)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(queues + fleet)
}

/// Queue rows since `since`, oldest first, grouped by queue.
pub async fn queue_history_since(
    pool: &PgPool,
    since: DateTime<Utc>,
) -> Result<BTreeMap<String, Vec<QueueHistoryRow>>, AwaError> {
    let rows: Vec<QueueHistoryRow> = sqlx::query_as(
        r#"
        SELECT queue, bucket, available, running, scheduled, retryable, failed,
               waiting_external, lag_seconds, paused, claimed, completed,
               failed_attempts, retried, rate_limited, claim_wait_ms,
               live_workers, capacity
        FROM awa.queue_history
        WHERE bucket >= $1
        ORDER BY queue, bucket
        "#,
    )
    .bind(since)
    .fetch_all(pool)
    .await?;
    let mut grouped: BTreeMap<String, Vec<QueueHistoryRow>> = BTreeMap::new();
    for row in rows {
        grouped.entry(row.queue.clone()).or_default().push(row);
    }
    Ok(grouped)
}

/// Fleet rows since `since`, oldest first.
pub async fn fleet_history_since(
    pool: &PgPool,
    since: DateTime<Utc>,
) -> Result<Vec<FleetHistoryRow>, AwaError> {
    Ok(sqlx::query_as(
        r#"
        SELECT bucket, live_instances, healthy_instances, stale_instances,
               leader_instance_id, versions
        FROM awa.fleet_history
        WHERE bucket >= $1
        ORDER BY bucket
        "#,
    )
    .bind(since)
    .fetch_all(pool)
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{QueueRuntimeConfigSnapshot, QueueRuntimeSnapshot, StorageCapability};

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
    }

    fn counters(claimed: u64, completed: u64) -> QueueOutcomeCounters {
        QueueOutcomeCounters {
            claimed,
            completed,
            claim_wait_ms: claimed * 10,
            ..Default::default()
        }
    }

    fn instance(
        id: u128,
        started_at: DateTime<Utc>,
        queues: Vec<(&str, Option<QueueOutcomeCounters>, QueueRuntimeMode)>,
    ) -> RuntimeInstance {
        RuntimeInstance {
            instance_id: Uuid::from_u128(id),
            hostname: None,
            pid: 1,
            version: "0.7.0".into(),
            storage_capability: StorageCapability::QueueStorage,
            transition_role: Default::default(),
            started_at,
            last_seen_at: started_at,
            snapshot_interval_ms: 10_000,
            stale: false,
            healthy: true,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: true,
            maintenance_alive: true,
            shutting_down: false,
            leader: id == 1,
            global_max_workers: None,
            queues: queues
                .into_iter()
                .map(|(queue, counters, mode)| QueueRuntimeSnapshot {
                    queue: queue.into(),
                    in_flight: 0,
                    overflow_held: None,
                    config: QueueRuntimeConfigSnapshot {
                        mode,
                        max_workers: Some(8),
                        min_workers: None,
                        weight: None,
                        global_max_workers: None,
                        poll_interval_ms: 200,
                        deadline_duration_secs: 300,
                        priority_aging_interval_secs: 60,
                        claimers: None,
                        claim_batch_size: None,
                        dlq_enabled: None,
                        rate_limit: None,
                    },
                    counters,
                })
                .collect(),
        }
    }

    const HARD: QueueRuntimeMode = QueueRuntimeMode::HardReserved;

    #[test]
    fn first_sample_after_leader_start_is_unknown() {
        let mut baseline = CounterBaseline::default();
        let coverage = baseline.advance(
            at(60),
            &[instance(
                1,
                at(0),
                vec![("email", Some(counters(5, 5)), HARD)],
            )],
        );
        assert_eq!(coverage["email"].flow, None);
        assert_eq!(coverage["email"].live_workers, 1);
        assert_eq!(coverage["email"].capacity, Some(8));
    }

    #[test]
    fn later_samples_difference_and_sum_across_instances() {
        let mut baseline = CounterBaseline::default();
        baseline.advance(
            at(60),
            &[
                instance(1, at(0), vec![("email", Some(counters(5, 5)), HARD)]),
                instance(2, at(0), vec![("email", Some(counters(1, 0)), HARD)]),
            ],
        );
        let coverage = baseline.advance(
            at(120),
            &[
                instance(1, at(0), vec![("email", Some(counters(15, 14)), HARD)]),
                instance(2, at(0), vec![("email", Some(counters(4, 4)), HARD)]),
            ],
        );
        let flow = coverage["email"].flow.unwrap();
        assert_eq!(flow.claimed, 13);
        assert_eq!(flow.completed, 13);
        assert_eq!(flow.claim_wait_ms, 130);
        assert_eq!(coverage["email"].capacity, Some(16));
    }

    #[test]
    fn a_restart_counts_from_zero() {
        let mut baseline = CounterBaseline::default();
        baseline.advance(
            at(60),
            &[instance(
                1,
                at(0),
                vec![("email", Some(counters(50, 50)), HARD)],
            )],
        );
        let coverage = baseline.advance(
            at(120),
            &[instance(
                1,
                at(0),
                vec![("email", Some(counters(3, 2)), HARD)],
            )],
        );
        assert_eq!(coverage["email"].flow.unwrap().claimed, 3);
    }

    #[test]
    fn new_instances_count_only_if_they_started_after_the_baseline() {
        let mut baseline = CounterBaseline::default();
        baseline.advance(
            at(60),
            &[instance(
                1,
                at(0),
                vec![("email", Some(counters(1, 1)), HARD)],
            )],
        );
        let fresh = baseline.advance(
            at(120),
            &[
                instance(1, at(0), vec![("email", Some(counters(2, 2)), HARD)]),
                instance(2, at(90), vec![("email", Some(counters(7, 7)), HARD)]),
            ],
        );
        assert_eq!(fresh["email"].flow.unwrap().completed, 8);

        // An instance the leader first sees, but which predates the baseline,
        // makes the minute unknown rather than double counting.
        let unseen = baseline.advance(
            at(180),
            &[
                instance(1, at(0), vec![("email", Some(counters(3, 3)), HARD)]),
                instance(2, at(90), vec![("email", Some(counters(7, 7)), HARD)]),
                instance(3, at(10), vec![("email", Some(counters(900, 900)), HARD)]),
            ],
        );
        assert_eq!(unseen["email"].flow, None);
    }

    #[test]
    fn runtimes_without_counters_make_the_queue_unknown() {
        let mut baseline = CounterBaseline::default();
        baseline.advance(
            at(60),
            &[instance(
                1,
                at(0),
                vec![("email", Some(counters(1, 1)), HARD)],
            )],
        );
        let coverage = baseline.advance(
            at(120),
            &[
                instance(1, at(0), vec![("email", Some(counters(2, 2)), HARD)]),
                instance(2, at(0), vec![("email", None, HARD)]),
            ],
        );
        assert_eq!(coverage["email"].flow, None);
        assert_eq!(coverage["email"].live_workers, 2);
    }

    #[test]
    fn weighted_queues_have_no_attributable_capacity() {
        let mut baseline = CounterBaseline::default();
        let coverage = baseline.advance(
            at(60),
            &[instance(
                1,
                at(0),
                vec![("email", None, QueueRuntimeMode::Weighted)],
            )],
        );
        assert_eq!(coverage["email"].capacity, None);
    }

    #[test]
    fn unserved_queues_record_zero_flow() {
        let queue = QueueOverview {
            queue: "orphan".into(),
            display_name: None,
            description: None,
            owner: None,
            docs_url: None,
            tags: vec![],
            extra: serde_json::Value::Null,
            descriptor_last_seen_at: None,
            descriptor_stale: false,
            descriptor_mismatch: false,
            total_queued: 4,
            scheduled: 0,
            available: 4,
            retryable: 0,
            running: 0,
            failed: 0,
            waiting_external: 0,
            completed_last_hour: 0,
            lag_seconds: Some(120.0),
            paused: false,
        };
        let (rows, fleet) = build_sample(at(60), &[queue], &HashMap::new(), &[]);
        assert_eq!(rows[0].live_workers, 0);
        assert_eq!(rows[0].claimed, Some(0));
        assert_eq!(fleet.live_instances, 0);
        assert!(fleet.versions.is_empty());
    }
}
