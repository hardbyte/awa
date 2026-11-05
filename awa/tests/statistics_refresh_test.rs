//! The maintenance leader refreshes planner statistics for the queue-storage
//! tables autovacuum never analyses (the head tables and the partitioned ring
//! parents) on election and every interval, without delaying ring prune.

use awa::model::{migrations, PruneOutcome, QueueStorage, QueueStorageConfig, RotateOutcome};
use awa::{audited_sql, Client, JobContext, JobError, JobResult, QueueConfig, Worker};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

const ROLE_PASSWORD: &str = "statistics_refresh_test";
const RING_PARENTS: [&str; 13] = [
    "done_entries",
    "lease_claim_batches",
    "lease_claim_closure_batches",
    "lease_claim_closures",
    "lease_claims",
    "leases",
    "queue_terminal_count_deltas",
    "ready_claim_attempt_batches",
    "ready_entries",
    "ready_segments",
    "ready_tombstones",
    "receipt_completion_batches",
    "receipt_completion_tombstones",
];

static DATABASE_COUNTER: AtomicU32 = AtomicU32::new(0);

fn base_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

struct StatisticsProbeWorker;

#[async_trait::async_trait]
impl Worker for StatisticsProbeWorker {
    fn kind(&self) -> &'static str {
        "statistics_probe_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

/// A throwaway database with the awa schema migrated, optionally owned by a
/// non-superuser role the way a production schema owner is.
struct TestDatabase {
    admin: PgPool,
    name: String,
    roles: Vec<String>,
    options: PgConnectOptions,
    /// Connects as the schema owner (or the superuser when no owner role).
    owner_pool: PgPool,
}

impl TestDatabase {
    async fn create(owner_role: Option<&str>, extra_roles: &[&str]) -> Self {
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&base_database_url())
            .await
            .expect("connect admin pool");
        let name = format!(
            "awa_stats_{}_{}",
            std::process::id(),
            DATABASE_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let mut roles = Vec::new();
        for role in owner_role.iter().chain(extra_roles.iter()) {
            sqlx::raw_sql(audited_sql(format!(
                "DROP ROLE IF EXISTS {role}; CREATE ROLE {role} LOGIN PASSWORD '{ROLE_PASSWORD}'"
            )))
            .execute(&admin)
            .await
            .expect("create role");
            roles.push((*role).to_string());
        }
        let owner_clause = owner_role
            .map(|role| format!(" OWNER {role}"))
            .unwrap_or_default();
        sqlx::raw_sql(audited_sql(format!("CREATE DATABASE {name}{owner_clause}")))
            .execute(&admin)
            .await
            .expect("create database");
        let options = PgConnectOptions::from_str(&base_database_url())
            .expect("parse DATABASE_URL")
            .database(&name);
        let owner_options = match owner_role {
            Some(role) => options.clone().username(role).password(ROLE_PASSWORD),
            None => options.clone(),
        };
        let owner_pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(owner_options)
            .await
            .expect("connect owner pool");
        migrations::run(&owner_pool).await.expect("run migrations");
        Self {
            admin,
            name,
            roles,
            options,
            owner_pool,
        }
    }

    /// A pool for a role other than the schema owner, with the runtime grants
    /// from the database-roles guide.
    async fn runtime_pool(&self, role: &str) -> PgPool {
        sqlx::raw_sql(audited_sql(format!(
            r#"
            GRANT USAGE ON SCHEMA awa TO {role};
            GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA awa TO {role};
            GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO {role};
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {role};
            REVOKE EXECUTE ON FUNCTION
              awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN) FROM {role};
            "#
        )))
        .execute(&self.owner_pool)
        .await
        .expect("grant runtime privileges");
        PgPoolOptions::new()
            .max_connections(8)
            .connect_with(self.options.clone().username(role).password(ROLE_PASSWORD))
            .await
            .expect("connect runtime pool")
    }

    async fn drop(self) {
        self.owner_pool.close().await;
        let _ = sqlx::raw_sql(audited_sql(format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        )))
        .execute(&self.admin)
        .await;
        for role in &self.roles {
            let _ = sqlx::raw_sql(audited_sql(format!("DROP ROLE IF EXISTS {role}")))
                .execute(&self.admin)
                .await;
        }
        self.admin.close().await;
    }
}

async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn stats_rows(pool: &PgPool, schema: &str, table: &str, inherited_only: bool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stats
         WHERE schemaname = $1 AND tablename = $2 AND (inherited OR NOT $3)",
    )
    .bind(schema)
    .bind(table)
    .bind(inherited_only)
    .fetch_one(pool)
    .await
    .expect("read pg_stats")
}

async fn parent_reltuples(pool: &PgPool, schema: &str, table: &str) -> f32 {
    sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE oid = to_regclass($1)")
        .bind(format!("{schema}.{table}"))
        .fetch_one(pool)
        .await
        .expect("read reltuples")
}

async fn analyze_count(pool: &PgPool, schema: &str, table: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(analyze_count, 0)::bigint FROM pg_stat_user_tables
         WHERE schemaname = $1 AND relname = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await
    .expect("read analyze_count")
}

fn probe_client(
    pool: PgPool,
    statistics_interval: Duration,
    storage: Option<QueueStorageConfig>,
) -> Client {
    let mut builder = Client::builder(pool)
        .queue(
            "statistics_probe",
            QueueConfig {
                max_workers: 1,
                poll_interval: Duration::from_millis(100),
                ..QueueConfig::default()
            },
        )
        .register_worker(StatisticsProbeWorker)
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .statistics_refresh_interval(statistics_interval);
    if let Some(config) = storage {
        builder = builder
            .queue_storage(config, Duration::from_secs(600), Duration::from_secs(600))
            .claim_rotate_interval(Duration::from_secs(600));
    }
    builder.build().expect("build client")
}

async fn seed_ready_rows(store: &QueueStorage, pool: &PgPool, rows_per_queue: i64) {
    for queue in ["stats_a", "stats_b", "stats_c"] {
        store
            .enqueue_batch(pool, queue, 2, rows_per_queue)
            .await
            .expect("enqueue rows");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_refreshes_statistics_on_election_and_every_interval() {
    let database = TestDatabase::create(None, &[]).await;
    let pool = &database.owner_pool;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage config");
    seed_ready_rows(&store, pool, 200).await;

    assert_eq!(stats_rows(pool, "awa", "queue_claim_heads", false).await, 0);
    assert_eq!(
        stats_rows(pool, "awa", "queue_enqueue_heads", false).await,
        0
    );
    assert_eq!(stats_rows(pool, "awa", "ready_entries", true).await, 0);
    assert!(parent_reltuples(pool, "awa", "ready_entries").await < 0.0);

    let client = probe_client(pool.clone(), Duration::from_secs(1), None);
    client.start().await.expect("start client");

    wait_for(
        "first statistics pass on election",
        Duration::from_secs(30),
        || async {
            stats_rows(pool, "awa", "queue_claim_heads", false).await > 0
                && stats_rows(pool, "awa", "queue_enqueue_heads", false).await > 0
                && stats_rows(pool, "awa", "ready_entries", true).await > 0
        },
    )
    .await;
    let partitioned: Vec<String> = sqlx::query_scalar(
        "SELECT relname::text FROM pg_class
         WHERE relnamespace = 'awa'::regnamespace AND relkind = 'p' ORDER BY relname",
    )
    .fetch_all(pool)
    .await
    .expect("list partitioned tables");
    assert_eq!(
        partitioned, RING_PARENTS,
        "the refresh covers every ring parent"
    );
    wait_for(
        "every ring parent analysed",
        Duration::from_secs(30),
        || async {
            for parent in RING_PARENTS {
                if analyze_count(pool, "awa", parent).await < 1 {
                    return false;
                }
            }
            true
        },
    )
    .await;
    let first_tuples = parent_reltuples(pool, "awa", "ready_entries").await;
    assert!(
        (550.0..=650.0).contains(&first_tuples),
        "parent reltuples after the first pass: {first_tuples}"
    );

    seed_ready_rows(&store, pool, 2_000).await;
    wait_for(
        "a later pass to see the new rows",
        Duration::from_secs(30),
        || async { parent_reltuples(pool, "awa", "ready_entries").await > 5_000.0 },
    )
    .await;
    assert!(analyze_count(pool, "awa", "ready_entries").await >= 2);
    assert!(analyze_count(pool, "awa", "queue_claim_heads").await >= 2);

    client.shutdown(Duration::from_secs(5)).await;
    database.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_refreshes_statistics_on_election_before_the_first_interval() {
    let database = TestDatabase::create(None, &[]).await;
    let pool = &database.owner_pool;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage config");
    seed_ready_rows(&store, pool, 200).await;

    let client = probe_client(pool.clone(), Duration::from_secs(300), None);
    client.start().await.expect("start client");
    wait_for(
        "a statistics pass on election",
        Duration::from_secs(30),
        || async { stats_rows(pool, "awa", "ready_entries", true).await > 0 },
    )
    .await;

    client.shutdown(Duration::from_secs(5)).await;
    database.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_interval_disables_the_refresh() {
    let database = TestDatabase::create(None, &[]).await;
    let pool = &database.owner_pool;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage config");
    seed_ready_rows(&store, pool, 100).await;

    let client = probe_client(pool.clone(), Duration::ZERO, None);
    client.start().await.expect("start client");
    wait_for("leadership", Duration::from_secs(10), || async {
        client.health_check().await.maintenance_alive
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(stats_rows(pool, "awa", "queue_claim_heads", false).await, 0);
    assert_eq!(stats_rows(pool, "awa", "ready_entries", true).await, 0);

    client.shutdown(Duration::from_secs(5)).await;
    database.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_owner_runtime_role_refreshes_statistics() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("stats_owner_{suffix}");
    let runtime = format!("stats_runtime_{suffix}");
    let database = TestDatabase::create(Some(&owner), &[&runtime]).await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage config");
    seed_ready_rows(&store, &database.owner_pool, 200).await;
    let runtime_pool = database.runtime_pool(&runtime).await;

    let is_superuser_or_owner: bool = sqlx::query_scalar(
        "SELECT rolsuper OR pg_has_role(current_user, 'pg_maintain', 'USAGE')
              OR has_table_privilege('awa.queue_claim_heads', 'MAINTAIN')
              OR (SELECT relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)
                  FROM pg_class WHERE oid = 'awa.queue_claim_heads'::regclass)
         FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(&runtime_pool)
    .await
    .expect("check runtime role privileges");
    assert!(
        !is_superuser_or_owner,
        "the runtime role must not be able to analyse the tables itself"
    );

    for (schema, relation, message) in [
        (
            "public",
            "queue_claim_heads",
            "is not a queue-storage schema",
        ),
        ("awa", "jobs", "is not a partitioned or head table"),
    ] {
        let error =
            sqlx::query_scalar::<_, String>("SELECT awa.analyze_queue_storage_relation($1, $2)")
                .bind(schema)
                .bind(relation)
                .fetch_one(&runtime_pool)
                .await
                .expect_err("the helper rejects what it may not analyse");
        let database_error = error.as_database_error().expect("database error");
        assert_eq!(database_error.code().as_deref(), Some("22023"));
        assert!(
            database_error.message().contains(message),
            "{schema}.{relation}: {}",
            database_error.message()
        );
    }

    let client = probe_client(runtime_pool.clone(), Duration::from_secs(1), None);
    client
        .start()
        .await
        .expect("start client as the runtime role");
    wait_for(
        "statistics written through a non-owner role",
        Duration::from_secs(30),
        || async {
            stats_rows(&database.owner_pool, "awa", "queue_claim_heads", false).await > 0
                && stats_rows(&database.owner_pool, "awa", "ready_entries", true).await > 0
        },
    )
    .await;

    client.shutdown(Duration::from_secs(5)).await;
    runtime_pool.close().await;
    database.drop().await;
}

/// A conflicting lock on a ring parent fails the call with `lock_not_available`
/// rather than letting `ANALYZE (SKIP_LOCKED)` skip every partition and report
/// success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locked_ring_parent_fails_the_call_instead_of_reporting_analyzed() {
    let database = TestDatabase::create(None, &[]).await;
    let pool = &database.owner_pool;

    let mut blocker = pool.begin().await.expect("begin blocker");
    sqlx::query("LOCK TABLE ONLY awa.ready_entries IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(blocker.as_mut())
        .await
        .expect("lock the parent");

    let mut call = pool.begin().await.expect("begin call");
    sqlx::query("SELECT set_config('lock_timeout', '200ms', true)")
        .execute(call.as_mut())
        .await
        .expect("set lock_timeout");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT awa.analyze_queue_storage_relation('awa', 'ready_entries')",
    )
    .fetch_one(call.as_mut())
    .await
    .expect_err("the locked parent must not report analyzed");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    call.rollback().await.expect("rollback call");

    blocker.rollback().await.expect("release the parent");
    let status: String =
        sqlx::query_scalar("SELECT awa.analyze_queue_storage_relation('awa', 'ready_entries')")
            .fetch_one(pool)
            .await
            .expect("analyse once the lock is gone");
    assert_eq!(status, "analyzed");
    database.drop().await;
}

fn sum_by_outcome(
    resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    outcome: &str,
) -> u64 {
    let mut total = 0;
    for resource in resource_metrics {
        for scope in resource.scope_metrics() {
            for metric in scope.metrics() {
                if metric.name() != "awa.maintenance.analyze.attempts" {
                    continue;
                }
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                    total += sum
                        .data_points()
                        .filter(|point| {
                            point.attributes().any(|KeyValue { key, value, .. }| {
                                key.as_str() == "awa.analyze.outcome" && value.as_str() == outcome
                            })
                        })
                        .map(|point| point.value())
                        .sum::<u64>();
                }
            }
        }
    }
    total
}

/// A refresh that cannot run is recorded and leaves the runtime healthy: the
/// helper's owner may not analyse the tables, or the schema predates the
/// helper.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrunnable_refresh_is_recorded_and_does_not_fail_the_runtime() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("stats_owner_{suffix}");
    let helper = format!("stats_helper_{suffix}");
    let database = TestDatabase::create(Some(&owner), &[&helper]).await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage config");
    seed_ready_rows(&store, &database.owner_pool, 50).await;

    // The function now runs as a role that owns nothing and holds no MAINTAIN.
    sqlx::raw_sql(audited_sql(format!("GRANT {helper} TO {owner}")))
        .execute(&database.admin)
        .await
        .expect("let the schema owner hand over the function");
    sqlx::raw_sql(audited_sql(format!(
        "GRANT USAGE, CREATE ON SCHEMA awa TO {helper};
         ALTER FUNCTION awa.analyze_queue_storage_relation(text, text) OWNER TO {helper}"
    )))
    .execute(&database.owner_pool)
    .await
    .expect("hand the helper to an unprivileged role");

    let client = probe_client(
        database.owner_pool.clone(),
        Duration::from_millis(200),
        None,
    );
    client.start().await.expect("start client");
    wait_for("a skipped pass", Duration::from_secs(30), || async {
        provider.force_flush().expect("flush metrics");
        let metrics = exporter.get_finished_metrics().expect("read metrics");
        sum_by_outcome(&metrics, "skipped_privilege") > 0
    })
    .await;
    assert_eq!(
        stats_rows(&database.owner_pool, "awa", "queue_claim_heads", false).await,
        0,
        "a skipped pass writes no statistics"
    );
    assert!(client.health_check().await.healthy);
    client.shutdown(Duration::from_secs(5)).await;

    // A schema that predates the helper skips the same way.
    sqlx::raw_sql("DROP FUNCTION awa.analyze_queue_storage_relation(text, text)")
        .execute(&database.owner_pool)
        .await
        .expect("drop the helper");
    let client = probe_client(
        database.owner_pool.clone(),
        Duration::from_millis(200),
        None,
    );
    client
        .start()
        .await
        .expect("start client without the helper");
    wait_for("an unavailable pass", Duration::from_secs(30), || async {
        provider.force_flush().expect("flush metrics");
        let metrics = exporter.get_finished_metrics().expect("read metrics");
        sum_by_outcome(&metrics, "unavailable") > 0
    })
    .await;
    assert!(client.health_check().await.healthy);
    client.shutdown(Duration::from_secs(5)).await;

    provider.force_flush().expect("flush metrics");
    let metrics = exporter.get_finished_metrics().expect("read metrics");
    assert_eq!(sum_by_outcome(&metrics, "failed"), 0);
    database.drop().await;
}

/// Back-to-back statistics passes (a far higher duty cycle than the default
/// five-minute interval) run while rotation and prune cycle the ring: prune
/// keeps reclaiming slots and only defers an attempt that lands while ANALYZE
/// is sampling the slot's tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statistics_refresh_does_not_stop_prune_reclaiming_slots() {
    let database = TestDatabase::create(None, &[]).await;
    let pool = &database.owner_pool;
    let schema = "awa_stats_ring";
    let config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        lease_claim_receipts: false,
        ..Default::default()
    };
    let store = QueueStorage::new(config.clone()).expect("queue storage config");
    store.prepare_schema(pool).await.expect("prepare schema");

    let queue = "statistics_ring";
    let analyze_before = analyze_count(pool, schema, "ready_entries").await;
    let client = probe_client(pool.clone(), Duration::from_millis(1), Some(config));
    client.start().await.expect("start client");

    let (mut pruned, mut deferred) = (0_u32, 0_u32);
    let started = Instant::now();
    for _ in 0..60 {
        store
            .enqueue_batch(pool, queue, 1, 600)
            .await
            .expect("enqueue");
        loop {
            let claims = store.claim_batch(pool, queue, 600).await.expect("claim");
            if claims.is_empty() {
                break;
            }
            store.complete_batch(pool, &claims).await.expect("complete");
        }
        match store.rotate(pool).await.expect("rotate") {
            RotateOutcome::Rotated { .. } | RotateOutcome::SkippedBusy { .. } => {}
            other => panic!("unexpected rotate outcome {other:?}"),
        }
        match store
            .prune_oldest(pool, Duration::ZERO)
            .await
            .expect("prune never errors under ANALYZE")
        {
            PruneOutcome::Pruned { .. } => pruned += 1,
            PruneOutcome::Blocked { .. } => deferred += 1,
            PruneOutcome::Noop | PruneOutcome::SkippedActive { .. } => {}
        }
        if started.elapsed() > Duration::from_secs(60) {
            break;
        }
    }
    let passes = analyze_count(pool, schema, "ready_entries").await - analyze_before;
    eprintln!("prune under ANALYZE: pruned={pruned} deferred={deferred} analyze_passes={passes}");

    client.shutdown(Duration::from_secs(5)).await;
    assert!(
        passes >= 3,
        "the refresh ran {passes} times during the loop"
    );
    assert!(pruned >= 20, "prune reclaimed {pruned} slots");

    // Shutdown can leave one ANALYZE statement finishing server-side; once it
    // is gone the oldest sealed slot reclaims without being blocked.
    wait_for(
        "the last ANALYZE to finish",
        Duration::from_secs(30),
        || async {
            let running: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity
             WHERE state = 'active' AND query LIKE '%analyze_queue_storage_relation%'
               AND pid <> pg_backend_pid()",
            )
            .fetch_one(pool)
            .await
            .expect("read pg_stat_activity");
            running == 0
        },
    )
    .await;
    store.rotate(pool).await.expect("rotate");
    assert!(!matches!(
        store
            .prune_oldest(pool, Duration::ZERO)
            .await
            .expect("prune"),
        PruneOutcome::Blocked { .. }
    ));
    database.drop().await;
}
