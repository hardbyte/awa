//! Admin reads on queue storage are written so that Postgres can't pick a
//! plan that visits every retained row. They must still return what the
//! definition they replaced returned. Each test builds one database holding
//! every state a job can be in, in several lanes, kinds and queues, then runs
//! the reference query kept here beside the function under test.
//!
//! The reference reads `current_jobs`, the expanded view of every job
//! (`admin::queue_storage_current_jobs_cte`), so it follows that definition
//! rather than a copy of it.

use awa::model::admin::{self, JobKindOverview, ListJobsFilter};
use awa::model::{insert_many, migrations, InsertOpts, InsertParams, QueueStorage};
use awa::{JobRow, JobState, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    admin: PgPool,
    database: String,
    pool: PgPool,
    store: QueueStorage,
    /// Jobs inserted, by kind.
    inserted: BTreeMap<String, i64>,
}

impl Fixture {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let url = awa_testing::setup::database_url();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let database = format!("awa_admin_equiv_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
            .execute(&admin)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.database(&database);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        migrations::run(&pool).await?;
        awa_testing::setup::activate_queue_storage(&pool).await;
        let store = QueueStorage::new(QueueStorageConfig::default())?;
        Ok(Self {
            admin,
            database,
            pool,
            store,
            inserted: BTreeMap::new(),
        })
    }

    async fn close(self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.database
        )))
        .execute(&self.admin)
        .await?;
        self.admin.close().await;
        Ok(())
    }

    /// Insert `count` jobs of `kind` on `queue` at `priority`, with `tags`
    /// and optionally scheduled `run_in` from now.
    async fn enqueue(
        &mut self,
        kind: &str,
        queue: &str,
        priority: i16,
        count: usize,
        tags: &[&str],
        run_in: Option<chrono::Duration>,
    ) -> Result<Vec<JobRow>, Box<dyn std::error::Error>> {
        let params: Vec<InsertParams> = (0..count)
            .map(|seq| InsertParams {
                kind: kind.to_string(),
                args: serde_json::json!({ "seq": seq }),
                opts: InsertOpts {
                    queue: queue.to_string(),
                    priority,
                    tags: tags.iter().map(|tag| tag.to_string()).collect(),
                    run_at: run_in.map(|delay| chrono::Utc::now() + delay),
                    ..Default::default()
                },
            })
            .collect();
        let rows = insert_many(&self.pool, &params).await?;
        *self.inserted.entry(kind.to_string()).or_default() += count as i64;
        Ok(rows)
    }

    async fn claim(
        &self,
        queue: &str,
        count: i64,
    ) -> Result<Vec<awa::model::queue_storage::ClaimedRuntimeJob>, Box<dyn std::error::Error>> {
        // A claim takes from one lane at a time.
        let mut claimed = Vec::new();
        while (claimed.len() as i64) < count {
            let batch = self
                .store
                .claim_runtime_batch(
                    &self.pool,
                    queue,
                    count - claimed.len() as i64,
                    Duration::ZERO,
                )
                .await?;
            assert!(
                !batch.is_empty(),
                "claimed {} of {count} from {queue}",
                claimed.len()
            );
            claimed.extend(batch);
        }
        Ok(claimed)
    }

    /// Put jobs in every state, in several lanes, kinds and queues.
    async fn build_mix(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Available, across lanes: three queues, four priorities, several kinds.
        for (queue, priority, kind, count) in [
            ("eq_avail_a", 1, "alpha", 9),
            ("eq_avail_a", 2, "alpha", 7),
            ("eq_avail_a", 3, "beta", 5),
            ("eq_avail_a", 4, "gamma", 3),
            ("eq_avail_b", 2, "alpha", 6),
            ("eq_avail_b", 2, "delta", 4),
            ("eq_avail_c", 1, "delta", 8),
            ("eq_avail_c", 3, "epsilon", 2),
        ] {
            self.enqueue(kind, queue, priority, count, &[], None)
                .await?;
        }
        // Tagged jobs; a tagged completion is written as a done row.
        self.enqueue("alpha", "eq_avail_b", 2, 4, &["tenant:a", "bulk"], None)
            .await?;
        self.enqueue("beta", "eq_avail_c", 3, 3, &["tenant:b"], None)
            .await?;

        // Scheduled, in a queue that also holds other states.
        self.enqueue(
            "zeta",
            "eq_avail_a",
            2,
            3,
            &[],
            Some(chrono::Duration::hours(2)),
        )
        .await?;
        self.enqueue(
            "zeta",
            "eq_running",
            2,
            2,
            &["tenant:a"],
            Some(chrono::Duration::hours(3)),
        )
        .await?;

        // Completed through compact receipt batches: two batches, then some
        // members deleted, so their batches carry tombstones.
        let done = self
            .enqueue("alpha", "eq_receipts", 2, 12, &[], None)
            .await?;
        let more = self.enqueue("beta", "eq_receipts", 3, 6, &[], None).await?;
        let claimed = self.claim("eq_receipts", 18).await?;
        let (first, second) = claimed.split_at(10);
        self.store.complete_runtime_batch(&self.pool, first).await?;
        self.store
            .complete_runtime_batch(&self.pool, second)
            .await?;
        let deleted = [done[0].id, done[5].id, more[1].id];
        sqlx::query(
            "INSERT INTO awa.receipt_completion_tombstones \
                 (ready_slot, ready_generation, job_id, run_lease, queue, priority, \
                  enqueue_shard, lane_seq, deleted_at) \
             SELECT batch.ready_slot, batch.ready_generation, member.job_id, member.run_lease, \
                    batch.queue, batch.priority, batch.enqueue_shard, member.lane_seq, now() \
             FROM awa.receipt_completion_batches AS batch \
             CROSS JOIN LATERAL unnest(batch.job_ids, batch.run_leases, batch.lane_seqs) \
                 AS member(job_id, run_lease, lane_seq) \
             WHERE member.job_id = ANY($1)",
        )
        .bind(&deleted[..])
        .execute(&self.pool)
        .await?;
        *self.inserted.get_mut("alpha").expect("alpha was inserted") -= 2;
        *self.inserted.get_mut("beta").expect("beta was inserted") -= 1;

        // Completed as done rows (tagged jobs take the slow path), failed
        // and cancelled while running, through the dead-letter queue, and
        // left for retry.
        self.enqueue("gamma", "eq_terminal", 2, 5, &["tenant:b"], None)
            .await?;
        self.enqueue("gamma", "eq_terminal", 3, 11, &[], None)
            .await?;
        self.enqueue("delta", "eq_terminal", 3, 4, &[], None)
            .await?;
        let terminal = self.claim("eq_terminal", 20).await?;
        let mut iter = terminal.into_iter();
        let slow: Vec<_> = iter.by_ref().take(5).collect();
        self.store.complete_runtime_batch(&self.pool, &slow).await?;
        for claim in iter.by_ref().take(3) {
            self.store
                .fail_terminal(&self.pool, claim.job.id, claim.job.run_lease, "boom", None)
                .await?;
        }
        for claim in iter.by_ref().take(3) {
            self.store
                .fail_to_dlq(
                    &self.pool,
                    claim.job.id,
                    claim.job.run_lease,
                    "poison",
                    "boom",
                    None,
                )
                .await?;
        }
        for claim in iter.by_ref().take(2) {
            self.store
                .cancel_running(&self.pool, claim.job.id, claim.job.run_lease, "stop", None)
                .await?;
        }
        for claim in iter.by_ref().take(2) {
            self.store
                .retry_after(
                    &self.pool,
                    claim.job.id,
                    claim.job.run_lease,
                    Duration::from_secs(3600),
                    None,
                )
                .await?;
        }
        for claim in iter.by_ref().take(2) {
            self.store
                .snooze(
                    &self.pool,
                    claim.job.id,
                    claim.job.run_lease,
                    Duration::from_secs(1800),
                    None,
                )
                .await?;
        }
        // The rest stay running on open receipt claims.
        drop(iter);

        // Running on open receipt claims, with a callback registered on some
        // (which materialises a lease), and one waiting on its callback.
        self.enqueue("beta", "eq_running", 2, 6, &[], None).await?;
        self.enqueue("gamma", "eq_running", 3, 5, &[], None).await?;
        let running = self.claim("eq_running", 11).await?;
        for claim in &running[..2] {
            self.store
                .register_callback(
                    &self.pool,
                    claim.job.id,
                    claim.job.run_lease,
                    Duration::from_secs(3600),
                )
                .await?;
        }
        let waiting = &running[2];
        let callback = self
            .store
            .register_callback(
                &self.pool,
                waiting.job.id,
                waiting.job.run_lease,
                Duration::from_secs(3600),
            )
            .await?;
        self.store
            .enter_callback_wait(&self.pool, waiting.job.id, waiting.job.run_lease, callback)
            .await?;

        // Cancelled while available (a ready tombstone), after a lane's claim
        // cursor has moved past earlier rows.
        let cancelled = self
            .enqueue("epsilon", "eq_cancel", 2, 9, &[], None)
            .await?;
        self.claim("eq_cancel", 2).await?;
        for job in &cancelled[4..7] {
            admin::cancel(&self.pool, job.id).await?;
        }

        // Some completions are older than the last hour.
        sqlx::raw_sql(
            "UPDATE awa.receipt_completion_batches \
                 SET finalized_at = finalized_at - interval '3 hours' \
                 WHERE job_ids[1] % 2 = 0; \
             UPDATE awa.done_entries \
                 SET finalized_at = finalized_at - interval '3 hours' \
                 WHERE state = 'completed' AND job_id % 2 = 0",
        )
        .execute(&self.pool)
        .await?;

        // A kind that has a descriptor but no jobs, and a runtime that reports
        // another and a mismatched hash.
        sqlx::raw_sql(
            "INSERT INTO awa.job_kind_descriptors (kind, display_name, descriptor_hash) \
                 VALUES ('alpha', 'Alpha', 'h1'), ('idle_kind', 'Idle', 'h2'); \
             INSERT INTO awa.runtime_instances \
                 (instance_id, hostname, pid, version, started_at, last_seen_at, \
                  snapshot_interval_ms, healthy, postgres_connected, poll_loop_alive, \
                  heartbeat_alive, shutting_down, leader, job_kind_descriptor_hashes) \
                 VALUES (gen_random_uuid(), 'h', 1, 'test', now(), now(), 10000, true, true, true, true, false, false, \
                         '{\"alpha\": \"x\", \"runtime_only\": \"y\"}'::jsonb), \
                        (gen_random_uuid(), 'h', 2, 'test', now(), now(), 10000, true, true, true, true, false, false, \
                         '{\"alpha\": \"z\"}'::jsonb)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// `job_kind_overviews` as it was defined before it stopped reading
/// `current_jobs`.
async fn reference_job_kind_overviews(
    pool: &PgPool,
    schema: &str,
) -> Result<Vec<JobKindOverview>, sqlx::Error> {
    let sql = format!(
        r#"
        {current_jobs_cte},
        all_kinds AS (
            SELECT kind FROM current_jobs
            UNION
            SELECT kind FROM awa.job_kind_descriptors
            UNION
            SELECT DISTINCT descriptor.key AS kind
            FROM awa.runtime_instances runtime
            CROSS JOIN LATERAL jsonb_each_text(runtime.job_kind_descriptor_hashes) AS descriptor(key, value)
        ),
        live_kind_descriptor_variants AS (
            SELECT
                descriptor.key AS kind,
                count(DISTINCT descriptor.value)::bigint AS descriptor_variant_count
            FROM awa.runtime_instances runtime
            CROSS JOIN LATERAL jsonb_each_text(runtime.job_kind_descriptor_hashes) AS descriptor(key, value)
            WHERE runtime.last_seen_at + make_interval(
                secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
            ) >= now()
            GROUP BY descriptor.key
        ),
        kind_counts AS (
            SELECT
                kind,
                count(*)::bigint AS job_count,
                count(DISTINCT queue)::bigint AS queue_count
            FROM current_jobs
            GROUP BY kind
        ),
        completed_recent AS (
            SELECT
                kind,
                count(*)::bigint AS completed_last_hour
            FROM current_jobs
            WHERE state = 'completed'
              AND finalized_at > clock_timestamp() - interval '1 hour'
            GROUP BY kind
        )
        SELECT
            k.kind,
            kd.display_name,
            kd.description,
            kd.owner,
            kd.docs_url,
            COALESCE(kd.tags, ARRAY[]::text[]) AS tags,
            COALESCE(kd.extra, '{{}}'::jsonb) AS extra,
            to_jsonb(kd) -> 'output' AS output,
            kd.last_seen_at AS descriptor_last_seen_at,
            CASE
                WHEN kd.last_seen_at IS NULL THEN FALSE
                ELSE kd.last_seen_at + make_interval(
                    secs => GREATEST(((COALESCE(kd.sync_interval_ms, 10000) / 1000) * 3)::int, 30)
                ) < now()
            END AS descriptor_stale,
            COALESCE(kdv.descriptor_variant_count, 0) > 1 AS descriptor_mismatch,
            COALESCE(kc.job_count, 0) AS job_count,
            COALESCE(kc.queue_count, 0) AS queue_count,
            COALESCE(cr.completed_last_hour, 0) AS completed_last_hour
        FROM all_kinds k
        LEFT JOIN kind_counts kc ON kc.kind = k.kind
        LEFT JOIN awa.job_kind_descriptors kd ON kd.kind = k.kind
        LEFT JOIN live_kind_descriptor_variants kdv ON kdv.kind = k.kind
        LEFT JOIN completed_recent cr ON cr.kind = k.kind
        ORDER BY k.kind
        "#,
        current_jobs_cte = admin::queue_storage_current_jobs_cte(schema)
    );
    sqlx::query_as::<_, JobKindOverview>(sqlx::AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn job_kind_overviews_match_the_current_jobs_definition() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let result = async {
        fixture.build_mix().await?;

        let states = admin::state_counts(&fixture.pool).await?;
        for state in [
            JobState::Available,
            JobState::Scheduled,
            JobState::Running,
            JobState::WaitingExternal,
            JobState::Retryable,
            JobState::Completed,
            JobState::Failed,
            JobState::Cancelled,
        ] {
            assert!(
                states.get(&state).copied().unwrap_or(0) > 0,
                "the fixture holds no {state:?} jobs: {states:?}"
            );
        }

        let expected = reference_job_kind_overviews(&fixture.pool, fixture.store.schema()).await?;
        let actual = admin::job_kind_overviews(&fixture.pool).await?;
        assert_eq!(
            serde_json::to_value(&actual)?,
            serde_json::to_value(&expected)?,
            "job_kind_overviews differs from the current_jobs definition"
        );

        // The reference and the function could both be wrong in the same
        // way: every job inserted is in exactly one row, and the fixture
        // gives the kinds that exercise each leg.
        for (kind, inserted) in &fixture.inserted {
            let overview = actual
                .iter()
                .find(|overview| &overview.kind == kind)
                .unwrap_or_else(|| panic!("no overview for {kind}"));
            assert_eq!(
                overview.job_count, *inserted,
                "job_count for {kind}: every inserted job is counted once"
            );
        }
        let by_kind: BTreeMap<_, _> = actual
            .iter()
            .map(|overview| (overview.kind.as_str(), overview))
            .collect();
        assert!(by_kind["alpha"].queue_count >= 3, "alpha spans queues");
        assert!(
            by_kind["alpha"].completed_last_hour > 0
                && by_kind["alpha"].completed_last_hour < by_kind["alpha"].job_count,
            "some completions are inside the last hour and some are not"
        );
        assert_eq!(by_kind["idle_kind"].job_count, 0);
        assert_eq!(by_kind["runtime_only"].job_count, 0);
        assert!(by_kind["alpha"].descriptor_mismatch);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// The ids `list_jobs` selected before it stopped reading `current_jobs`,
/// newest first, for a filter's state, kind, queue and `before_id`.
async fn reference_job_ids(
    pool: &PgPool,
    schema: &str,
    filter: &ListJobsFilter,
    limit: i64,
) -> Result<Vec<i64>, sqlx::Error> {
    let sql = format!(
        "{} \
         SELECT job_id \
         FROM current_jobs \
         WHERE ($1::awa.job_state IS NULL OR state = $1) \
           AND ($2::text IS NULL OR kind = $2) \
           AND ($3::text IS NULL OR queue = $3) \
           AND ($4::bigint IS NULL OR job_id < $4) \
         ORDER BY job_id DESC \
         LIMIT $5",
        admin::queue_storage_current_jobs_cte(schema)
    );
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(filter.state)
        .bind(&filter.kind)
        .bind(&filter.queue)
        .bind(filter.before_id)
        .bind(limit)
        .fetch_all(pool)
        .await
}

const ALL_STATES: [JobState; 8] = [
    JobState::Available,
    JobState::Scheduled,
    JobState::Running,
    JobState::WaitingExternal,
    JobState::Retryable,
    JobState::Completed,
    JobState::Failed,
    JobState::Cancelled,
];

// Keep exercising the deprecated compatibility wrapper against its reference.
#[allow(deprecated)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_jobs_matches_the_current_jobs_definition() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let result = async {
        fixture.build_mix().await?;
        let schema = fixture.store.schema().to_string();

        let states: Vec<Option<JobState>> = std::iter::once(None)
            .chain(ALL_STATES.into_iter().map(Some))
            .collect();
        let kinds = [None, Some("alpha"), Some("gamma"), Some("no_such_kind")];
        let queues = [None, Some("eq_terminal"), Some("eq_receipts")];
        let mut non_empty = 0;
        for state in &states {
            for kind in kinds {
                for queue in queues {
                    for limit in [4, 100] {
                        let filter = ListJobsFilter {
                            state: *state,
                            kind: kind.map(str::to_string),
                            queue: queue.map(str::to_string),
                            limit: Some(limit),
                            ..Default::default()
                        };
                        let expected =
                            reference_job_ids(&fixture.pool, &schema, &filter, limit).await?;
                        let actual: Vec<i64> = admin::list_jobs(&fixture.pool, &filter)
                            .await?
                            .into_iter()
                            .map(|job| job.id)
                            .collect();
                        assert_eq!(actual, expected, "list_jobs for {filter:?}");
                        non_empty += usize::from(!actual.is_empty());
                    }
                }
            }
        }
        assert!(non_empty > 40, "the filters match jobs: {non_empty}");

        // Paging by `before_id` walks the same ids in the same order.
        for state in [None, Some(JobState::Completed), Some(JobState::Running)] {
            let everything = reference_job_ids(
                &fixture.pool,
                &schema,
                &ListJobsFilter {
                    state,
                    ..Default::default()
                },
                1000,
            )
            .await?;
            let mut paged = Vec::new();
            let mut before_id = None;
            loop {
                let page = admin::list_jobs(
                    &fixture.pool,
                    &ListJobsFilter {
                        state,
                        before_id,
                        limit: Some(5),
                        ..Default::default()
                    },
                )
                .await?;
                let Some(last) = page.last() else { break };
                before_id = Some(last.id);
                paged.extend(page.iter().map(|job| job.id));
            }
            assert_eq!(paged, everything, "paging through {state:?}");
        }

        // Tags are applied to the loaded jobs.
        for tag in ["tenant:a", "tenant:b", "bulk", "absent"] {
            let filter = ListJobsFilter {
                tag: Some(tag.to_string()),
                limit: Some(100),
                ..Default::default()
            };
            let mut expected = Vec::new();
            for id in
                reference_job_ids(&fixture.pool, &schema, &ListJobsFilter::default(), 1000).await?
            {
                if admin::get_job(&fixture.pool, id)
                    .await?
                    .tags
                    .iter()
                    .any(|job_tag| job_tag == tag)
                {
                    expected.push(id);
                }
            }
            let actual: Vec<i64> = admin::list_jobs(&fixture.pool, &filter)
                .await?
                .into_iter()
                .map(|job| job.id)
                .collect();
            assert_eq!(actual, expected, "list_jobs tagged {tag}");
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    fixture.close().await?;
    result
}
