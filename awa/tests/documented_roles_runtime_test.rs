//! Runs the worker runtime as the least-privilege role that `docs/security.md`
//! documents, and fails on any permission error from a runtime write path.
//!
//! The role setup is the guide's own SQL, extracted from its "Create roles",
//! "Transfer ownership" and "Grant runtime privileges" blocks at compile time
//! with only the role, database and password names substituted. A table,
//! function or statement that needs a privilege the guide does not grant
//! fails here, as does a guide change that drops one the runtime needs.
//!
//! Many runtime paths are best-effort and only log a failure, so the test
//! captures every warning and error the runtime logs and fails on any that
//! reports `permission denied` (SQLSTATE 42501). Paths that return the error
//! fail the test directly.
//!
//! Exercised as the runtime role: producer inserts (immediate, scheduled,
//! unique); dispatch, claim and completion; progress writes; retry, DLQ and
//! terminal failure; promotion of scheduled and retryable jobs; deadline
//! rescue; cron sync and evaluation; admin cancel; runtime snapshots,
//! descriptors and runtime-control observations; leader election, the
//! maintenance loop's rotation, prune, cleanup and metadata branches; and a
//! runtime that opts out of leadership. A schema patch applied after the
//! grants, as every upgrade applies one, is covered by recreating the newest
//! patch table through `awa migrate` and relying on the guide's default
//! privileges.

mod ci_timing;

use awa::model::{admin, migrations, runtime_control, QueueStorage};
use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobState, PeriodicJob,
    QueueConfig, QueueStorageConfig, Worker,
};
use ci_timing::scaled_timeout;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::io::Write;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SECURITY_GUIDE: &str = include_str!("../../docs/security.md");
const QUEUE: &str = "documented_roles";
const KIND: &str = "documented_roles_probe";
const PASSWORD: &str = "documented-roles";

/// The first ```sql block after `heading` in the security guide.
fn guide_sql(heading: &str) -> &'static str {
    let section = SECURITY_GUIDE
        .split_once(heading)
        .unwrap_or_else(|| panic!("docs/security.md has no {heading:?} heading"))
        .1;
    let block = section
        .split_once("```sql\n")
        .unwrap_or_else(|| panic!("no sql block after {heading:?}"))
        .1;
    block
        .split_once("```")
        .unwrap_or_else(|| panic!("unterminated sql block after {heading:?}"))
        .0
}

/// Role and database names unique to one run, so concurrent runs and other
/// tests never share the cluster-wide roles.
struct Names {
    owner: String,
    migrator: String,
    runtime: String,
    database: String,
}

impl Names {
    fn new() -> Self {
        let suffix = uuid::Uuid::new_v4().simple().to_string()[..12].to_string();
        Self {
            owner: format!("awa_owner_{suffix}"),
            migrator: format!("awa_migrator_{suffix}"),
            runtime: format!("awa_runtime_{suffix}"),
            database: format!("awa_roles_{suffix}"),
        }
    }

    /// The guide's SQL with its example names replaced by this run's.
    fn substitute(&self, sql: &str) -> String {
        sql.replace("awa_owner", &self.owner)
            .replace("awa_migrator", &self.migrator)
            .replace("awa_runtime", &self.runtime)
            .replace("mydb", &self.database)
            .replace("'strong-password-here'", &format!("'{PASSWORD}'"))
    }
}

/// Collects everything the runtime logs at WARN and above.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl CapturedLogs {
    fn install() -> Self {
        let logs = Self::default();
        let writer = logs.clone();
        tracing_subscriber::fmt()
            // sqlx's slow-statement warnings quote whole migrations, whose
            // comments discuss privileges; they are not runtime errors.
            .with_env_filter("warn,sqlx::query=error")
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .init();
        logs
    }

    fn permission_errors(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .filter(|line| {
                // PostgreSQL's wording, e.g. "permission denied for table x".
                line.contains("permission denied for") || line.contains("42501")
            })
            .map(str::to_string)
            .collect()
    }

    /// Fail now, naming the denied statement, rather than later as a stalled
    /// wait.
    fn assert_no_permission_errors(&self) {
        let denied = self.permission_errors();
        assert!(
            denied.is_empty(),
            "the runtime hit permission errors as the documented role:\n{}",
            denied.join("\n")
        );
    }
}

/// Arguments for [`Probe`]; the derived kind is `documented_roles_probe`.
#[derive(Serialize, Deserialize, JobArgs)]
struct DocumentedRolesProbe {
    mode: String,
}

/// One job kind whose behaviour comes from its `mode` argument, so a few
/// inserts drive completion, retry, DLQ, terminal failure, progress and
/// deadline rescue.
struct Probe;

#[async_trait::async_trait]
impl Worker for Probe {
    fn kind(&self) -> &'static str {
        KIND
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let first_attempt = ctx.job.attempt <= 1;
        match ctx.job.args["mode"].as_str().unwrap_or("complete") {
            "progress" => {
                ctx.set_progress(50, "half way");
                ctx.flush_progress().await.map_err(JobError::retryable)?;
                Ok(JobResult::Completed)
            }
            "retry_once" if first_attempt => Ok(JobResult::RetryAfter(Duration::from_millis(100))),
            "exhaust" => Err(JobError::retryable_msg("exhausts its only attempt")),
            "terminal" => Err(JobError::Terminal("terminal by design".into())),
            // Outlives its deadline on the first attempt, so deadline rescue
            // takes it back and a second attempt completes it.
            "overrun_once" if first_attempt => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(JobResult::Completed)
            }
            _ => Ok(JobResult::Completed),
        }
    }
}

fn runtime_client(pool: PgPool, maintenance_leader: bool) -> Client {
    Client::builder(pool)
        .queue(
            QUEUE,
            QueueConfig {
                max_workers: 8,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .queue_dlq_enabled(QUEUE, true)
        .register_worker(Probe)
        .periodic(
            PeriodicJob::builder("documented_roles_every_second", "* * * * * *")
                .queue(QUEUE)
                .build(&DocumentedRolesProbe {
                    mode: "complete".into(),
                })
                .unwrap(),
        )
        .queue_storage(
            QueueStorageConfig::default(),
            Duration::from_millis(250),
            Duration::from_millis(250),
        )
        .claim_rotate_interval(Duration::from_millis(200))
        .leader_election_interval(Duration::from_millis(50))
        .leader_check_interval(Duration::from_millis(100))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .heartbeat_interval(Duration::from_millis(100))
        .heartbeat_rescue_interval(Duration::from_millis(200))
        .deadline_rescue_interval(Duration::from_millis(200))
        .callback_rescue_interval(Duration::from_millis(200))
        .cleanup_interval(Duration::from_millis(200))
        .completed_retention(Duration::from_millis(1))
        .queue_stats_interval(Duration::from_millis(200))
        .terminal_count_rollup_interval(Duration::from_millis(200))
        .maintenance_leader(maintenance_leader)
        .build()
        .unwrap()
}

async fn insert(pool: &PgPool, mode: &str, opts: InsertOpts) -> i64 {
    awa::insert_with(
        pool,
        &DocumentedRolesProbe { mode: mode.into() },
        InsertOpts {
            queue: QUEUE.into(),
            ..opts
        },
    )
    .await
    .unwrap_or_else(|err| panic!("runtime role could not insert a {mode} job: {err}"))
    .id
}

async fn wait_for_state(
    logs: &CapturedLogs,
    store: &QueueStorage,
    pool: &PgPool,
    id: i64,
    want: JobState,
) {
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(30));
    loop {
        logs.assert_no_permission_errors();
        let state = store.load_job(pool, id).await.unwrap().map(|job| job.state);
        if state == Some(want) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {id} never reached {want:?}; last state {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn scalar_i64(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn runtime_runs_as_the_documented_least_privilege_role() {
    let logs = CapturedLogs::install();
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".into());
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let names = Names::new();
    sqlx::raw_sql(&format!("CREATE DATABASE {}", names.database))
        .execute(&admin)
        .await
        .unwrap();
    // On its own runtime, so a failed assertion still reaches the cleanup.
    let (body_url, body_names, body_logs) = (url.clone(), names_clone(&names), logs.clone());
    let result = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(exercise(body_url, body_names, body_logs))
    })
    .await;

    // One statement each: DROP DATABASE cannot run in the implicit
    // transaction of a multi-statement query.
    for statement in [
        format!("DROP DATABASE {} WITH (FORCE)", names.database),
        format!("DROP ROLE IF EXISTS {}", names.runtime),
        format!("DROP ROLE IF EXISTS {}", names.migrator),
        format!("DROP ROLE IF EXISTS {}", names.owner),
    ] {
        sqlx::raw_sql(&statement).execute(&admin).await.unwrap();
    }
    admin.close().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic.into_panic());
    }
    // Shutdown and anything logged after the last wait.
    logs.assert_no_permission_errors();
}

fn names_clone(names: &Names) -> Names {
    Names {
        owner: names.owner.clone(),
        migrator: names.migrator.clone(),
        runtime: names.runtime.clone(),
        database: names.database.clone(),
    }
}

async fn exercise(url: String, names: Names, logs: CapturedLogs) {
    let base = PgConnectOptions::from_str(&url)
        .unwrap()
        .database(&names.database);
    let superuser = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(base.clone())
        .await
        .unwrap();
    let connect_as = |role: &str| {
        PgPoolOptions::new()
            .max_connections(16)
            .connect_with(base.clone().username(role).password(PASSWORD))
    };

    // 1. Create roles, as a superuser.
    sqlx::raw_sql(&names.substitute(guide_sql("### 1. Create roles")))
        .execute(&superuser)
        .await
        .expect("the guide's role setup must run");
    // 2. Migrate, 3. transfer ownership and 4. grant, as the migrator.
    let migrator = connect_as(&names.migrator).await.unwrap();
    migrations::run(&migrator)
        .await
        .expect("awa migrate as the documented migrator");
    for heading in [
        "### 3. Transfer ownership",
        "### 4. Grant runtime privileges",
    ] {
        sqlx::raw_sql(&names.substitute(guide_sql(heading)))
            .execute(&migrator)
            .await
            .unwrap_or_else(|err| panic!("the guide's {heading:?} SQL must run: {err}"));
    }
    // An upgrade applies new schema patches after the grants, so their
    // objects get only the default privileges. Recreate the newest patch
    // table that way.
    sqlx::raw_sql("DROP TABLE awa.runtime_maintenance_eligibility")
        .execute(&migrator)
        .await
        .unwrap();
    migrations::run(&migrator)
        .await
        .expect("awa migrate re-applies the patch");
    migrator.close().await;

    let runtime_pool = connect_as(&names.runtime).await.unwrap();
    let opted_out_pool = connect_as(&names.runtime).await.unwrap();
    let leader = runtime_client(runtime_pool.clone(), true);
    leader
        .start()
        .await
        .expect("the runtime role must start a leader");
    let opted_out = runtime_client(opted_out_pool.clone(), false);
    opted_out
        .start()
        .await
        .expect("the runtime role must start an opted-out runtime");

    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();
    let pool = &runtime_pool;
    let completed = insert(pool, "complete", InsertOpts::default()).await;
    let progress = insert(pool, "progress", InsertOpts::default()).await;
    let retried = insert(pool, "retry_once", InsertOpts::default()).await;
    let exhausted = insert(
        pool,
        "exhaust",
        InsertOpts {
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await;
    let terminal = insert(pool, "terminal", InsertOpts::default()).await;
    let scheduled = insert(
        pool,
        "complete",
        InsertOpts {
            run_at: Some(chrono::Utc::now() + chrono::Duration::milliseconds(300)),
            ..Default::default()
        },
    )
    .await;
    let rescued = insert(
        pool,
        "overrun_once",
        InsertOpts {
            deadline_duration: Some(chrono::Duration::milliseconds(300)),
            ..Default::default()
        },
    )
    .await;
    let cancelled = insert(
        pool,
        "complete",
        InsertOpts {
            run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            ..Default::default()
        },
    )
    .await;
    admin::cancel(pool, cancelled)
        .await
        .expect("the runtime role can cancel a job");

    for (id, state) in [
        (completed, JobState::Completed),
        (progress, JobState::Completed),
        (retried, JobState::Completed),
        (exhausted, JobState::Failed),
        (terminal, JobState::Failed),
        (scheduled, JobState::Completed),
        (rescued, JobState::Completed),
        (cancelled, JobState::Cancelled),
    ] {
        wait_for_state(&logs, &store, pool, id, state).await;
    }

    // Let the maintenance loop run its slower branches: ring rotation wraps
    // and prunes, cleanup deletes completed rows, cron fires.
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(30));
    loop {
        logs.assert_no_permission_errors();
        let claim_generation =
            scalar_i64(&superuser, "SELECT generation FROM awa.claim_ring_state").await;
        let queue_generation =
            scalar_i64(&superuser, "SELECT generation FROM awa.queue_ring_state").await;
        let cron_fired = scalar_i64(
            &superuser,
            "SELECT count(*) FROM awa.cron_jobs \
             WHERE name = 'documented_roles_every_second' AND last_enqueued_at IS NOT NULL",
        )
        .await;
        // Claim ring: 8 slots. Queue ring: 16. Past a full wrap each ring has
        // rotated onto, and pruned, a slot it used before.
        if claim_generation > 8 && queue_generation > 16 && cron_fired == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "maintenance did not progress: claim generation {claim_generation}, queue \
             generation {queue_generation}, cron fired {cron_fired}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let overview = admin::runtime_overview(pool)
        .await
        .expect("the runtime role can read the runtime overview");
    assert_eq!(overview.live_instances, 2);
    assert_eq!(overview.leader_instances, 1);
    assert_eq!(
        overview.leader_eligible_instances, 1,
        "the opted-out runtime's opt-out must be recorded as the runtime role"
    );
    let status = runtime_control::status(pool)
        .await
        .expect("the runtime role can read runtime control status");
    assert_eq!(status.leader_eligible_runtimes, 1);
    assert_eq!(
        scalar_i64(
            &superuser,
            "SELECT count(*) FROM awa.runtime_control_observations"
        )
        .await,
        2,
        "both runtimes report control observations"
    );
    assert!(
        scalar_i64(&superuser, "SELECT count(*) FROM awa.dlq_entries").await >= 1,
        "the exhausted job moves to the DLQ"
    );

    opted_out.shutdown(Duration::from_secs(5)).await;
    leader.shutdown(Duration::from_secs(5)).await;
    runtime_pool.close().await;
    opted_out_pool.close().await;
    superuser.close().await;
}
