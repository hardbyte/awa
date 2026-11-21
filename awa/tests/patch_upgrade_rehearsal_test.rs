//! Released-artifact rehearsal of a released -> candidate -> released patch upgrade.
//!
//! The journey an operator follows, as a release gate:
//!
//! 1. Install with the released CLI as the migration owner, activate
//!    queue storage in a custom schema, and run released workers and
//!    producers as a separate runtime role with the grants a working released
//!    deployment needs (schema `CREATE` included), banking a backlog.
//! 2. Run the candidate `awa migrate` as the owner while that traffic
//!    continues. No pre-existing awa or queue-storage relation may be locked.
//!    Lane positions and routing stay unchanged.
//! 3. Run candidate and released workers together, with retries, scheduled
//!    jobs, slow jobs in flight and a hard-killed released worker.
//! 4. Roll back to released binaries only, including a re-run of the released
//!    `awa migrate`; the patch stays installed and work continues, including
//!    a queue created after the rollback.
//! 5. Tighten privileges (provision queues, grant sequence `UPDATE`, revoke
//!    runtime `CREATE`) and restart the released workers again.
//!
//! Every job enqueued is accounted for: each completes exactly once (a second
//! completion only for a job whose first worker was killed), retries complete
//! on a later attempt, and per-queue counts match what was enqueued.
//!
//! Needs a superuser `DATABASE_URL` (it creates roles and a database) and:
//! - `AWA_RELEASED_VERSION` (optional): a version in `RELEASED`;
//!   defaults to `0.6.9`
//! - `AWA_RELEASED_CLI`: the matching released `awa` binary (`awa-cli` wheel)
//! - `AWA_RELEASED_PYTHON`: a Python with the matching released `awa-pg` wheel
//! - `AWA_CANDIDATE_CLI` (optional): the candidate `awa`; defaults to
//!   `target/debug/awa`, so build it with `cargo build -p awa-cli` first.
//!
//! Every worker runs with a one-hour ready-ring rotation so completed jobs are
//! still readable when the final accounting runs.

mod ci_timing;

use async_trait::async_trait;
use awa::model::{admin, migrations, runtime_control, JobState, QueueStorage, QueueStorageConfig};
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

const SCHEMA: &str = "rehearsal_qs";
const MAIN: &str = "rh_main";
const BANKED: &str = "rh_banked";
const NEW_AFTER_PATCH: &str = "rh_new";
const NEW_AFTER_ROLLBACK: &str = "rh_rollback_new";
const PROVISIONED: &str = "rh_provisioned";
const UNPROVISIONED: &str = "rh_unprovisioned";
const PASSWORD: &str = "rehearsal";
const RING_ROTATION: Duration = Duration::from_secs(3600);
/// Long enough for jobs scheduled under released binaries to come due after the patch.
const SCHEDULE_DELAY: Duration = Duration::from_secs(45);

// ── environment ────────────────────────────────────────────────────────

fn superuser_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn env_path(var: &str) -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os(var).unwrap_or_else(|| panic!("{var} must name an artifact path")),
    );
    if path.is_absolute() {
        path
    } else {
        workspace_root().join(path)
    }
}

/// A released version the rehearsal runs against.
struct Released {
    version: &'static str,
    /// The schema patches this version shipped. A database it installed has
    /// these and lacks every later one.
    shipped_patches: &'static [&'static str],
    /// Whether its runtimes report runtime-control observations.
    observes_runtime_controls: bool,
}

impl Released {
    /// The schema patches the candidate's `awa migrate` applies to a database
    /// this version installed, in `migrations::SCHEMA_PATCHES` order.
    fn pending_patches(&self) -> Vec<&'static str> {
        let missing: Vec<&str> = migrations::SCHEMA_PATCHES
            .iter()
            .map(|patch| patch.name)
            .filter(|name| !self.shipped_patches.contains(name))
            .collect();
        migrations::patches_to_apply(&missing)
    }
}

const RELEASED: &[Released] = &[
    Released {
        version: "0.6.9",
        shipped_patches: &["wait_free_dirty_marks"],
        observes_runtime_controls: false,
    },
    Released {
        version: "0.6.15",
        shipped_patches: &[
            "wait_free_dirty_marks",
            "prepared_lane_sequences",
            "definer_lane_provisioning",
        ],
        observes_runtime_controls: false,
    },
    Released {
        version: "0.6.16",
        shipped_patches: &[
            "wait_free_dirty_marks",
            "prepared_lane_sequences",
            "definer_lane_provisioning",
            "claim_batch_job_index",
            "insert_jobs_compat_batch",
            "prune_controls",
        ],
        observes_runtime_controls: true,
    },
    Released {
        version: "0.6.17",
        shipped_patches: &[
            "wait_free_dirty_marks",
            "prepared_lane_sequences",
            "definer_lane_provisioning",
            "claim_batch_job_index",
            "insert_jobs_compat_batch",
            "prune_controls",
            "runtime_maintenance_eligibility",
            "correlated_claim_tombstones",
        ],
        observes_runtime_controls: true,
    },
];

fn released() -> &'static Released {
    let version = std::env::var("AWA_RELEASED_VERSION").unwrap_or_else(|_| "0.6.9".to_string());
    RELEASED
        .iter()
        .find(|released| released.version == version)
        .unwrap_or_else(|| {
            panic!(
                "AWA_RELEASED_VERSION must be one of {:?}",
                RELEASED
                    .iter()
                    .map(|released| released.version)
                    .collect::<Vec<_>>()
            )
        })
}

fn released_version() -> &'static str {
    released().version
}

fn candidate_cli() -> PathBuf {
    if std::env::var_os("AWA_CANDIDATE_CLI").is_some() {
        return env_path("AWA_CANDIDATE_CLI");
    }
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    let target = if target.is_absolute() {
        target
    } else {
        workspace_root().join(target)
    };
    target.join("debug").join("awa")
}

// ── events ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum EventKind {
    Enqueued,
    Start,
    Retry,
    Complete,
}

#[derive(Debug, Clone)]
struct Event {
    at: Instant,
    source: String,
    kind: EventKind,
    job_id: i64,
    attempt: i64,
    seq: i64,
    mode: String,
    queue: String,
}

#[derive(Clone, Default)]
struct EventLog(Arc<Mutex<Vec<Event>>>);

impl EventLog {
    fn push(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }

    fn snapshot(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }

    fn count(&self, kind: EventKind) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.kind == kind)
            .count()
    }

    /// Parse one line from the released helper.
    fn record_line(&self, source: &str, line: &str) {
        let kind = match line.split_whitespace().next() {
            Some("ENQUEUED") => EventKind::Enqueued,
            Some("START") => EventKind::Start,
            Some("RETRY") => EventKind::Retry,
            Some("COMPLETE") => EventKind::Complete,
            _ => return,
        };
        let fields: HashMap<&str, &str> = line
            .split_whitespace()
            .filter_map(|field| field.split_once('='))
            .collect();
        let number = |key: &str| fields.get(key).and_then(|v| v.parse().ok()).unwrap_or(0);
        self.push(Event {
            at: Instant::now(),
            source: source.to_string(),
            kind,
            job_id: number("job_id"),
            attempt: number("attempt"),
            seq: number("seq"),
            mode: fields.get("mode").unwrap_or(&"").to_string(),
            queue: fields.get("queue").unwrap_or(&"").to_string(),
        });
    }
}

// ── candidate (source) worker ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct PatchRehearsalJob {
    seq: i64,
    mode: String,
}

struct CandidateWorker {
    source: String,
    log: EventLog,
}

#[async_trait]
impl Worker for CandidateWorker {
    fn kind(&self) -> &'static str {
        "patch_rehearsal_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: PatchRehearsalJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|error| JobError::terminal(error.to_string()))?;
        let event = |kind| Event {
            at: Instant::now(),
            source: self.source.clone(),
            kind,
            job_id: ctx.job.id,
            attempt: ctx.job.attempt as i64,
            seq: args.seq,
            mode: args.mode.clone(),
            queue: ctx.job.queue.clone(),
        };
        self.log.push(event(EventKind::Start));
        if args.mode == "fail_once" && ctx.job.attempt == 1 {
            self.log.push(event(EventKind::Retry));
            return Ok(JobResult::RetryAfter(Duration::from_millis(500)));
        }
        let pause = if args.mode == "slow" { 1500 } else { 5 };
        tokio::time::sleep(Duration::from_millis(pause)).await;
        self.log.push(event(EventKind::Complete));
        Ok(JobResult::Completed)
    }
}

fn candidate_client(runtime: &PgPool, log: &EventLog, source: &str, queues: &[&str]) -> Client {
    let mut builder = Client::builder(runtime.clone())
        .queue_storage(
            QueueStorageConfig {
                schema: SCHEMA.to_string(),
                ..Default::default()
            },
            RING_ROTATION,
            Duration::from_millis(250),
        )
        .heartbeat_interval(Duration::from_millis(100))
        .heartbeat_rescue_interval(Duration::from_millis(250))
        .heartbeat_staleness(rehearsal_heartbeat_staleness())
        .promote_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(200))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .register_worker(CandidateWorker {
            source: source.to_string(),
            log: log.clone(),
        });
    for queue in queues {
        builder = builder.queue(
            *queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        );
    }
    builder.build().expect("build candidate client")
}

async fn candidate_produce(
    runtime: &PgPool,
    log: &EventLog,
    queue: &str,
    seq_start: i64,
    count: i64,
    modes: &[&str],
) {
    for index in 0..count {
        let mode = modes[index as usize % modes.len()];
        let job = awa::insert_with(
            runtime,
            &PatchRehearsalJob {
                seq: seq_start + index,
                mode: mode.to_string(),
            },
            InsertOpts {
                queue: queue.to_string(),
                max_attempts: 5,
                ..Default::default()
            },
        )
        .await
        .expect("candidate enqueue");
        log.push(Event {
            at: Instant::now(),
            source: "candidate-producer".into(),
            kind: EventKind::Enqueued,
            job_id: job.id,
            attempt: 0,
            seq: seq_start + index,
            mode: mode.to_string(),
            queue: queue.to_string(),
        });
    }
}

// ── released-wheel helpers ─────────────────────────────────────

struct ReleasedWorker {
    child: Child,
    source: String,
    _reader: tokio::task::JoinHandle<()>,
}

impl ReleasedWorker {
    async fn kill(mut self) -> String {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        self.source
    }
}

/// Heartbeat staleness for every runtime in the rehearsal, candidate and
/// released alike: stale-heartbeat rescue runs on whichever runtime leads
/// maintenance, with that runtime's own staleness.
fn rehearsal_heartbeat_staleness() -> Duration {
    ci_timing::scaled_staleness(Duration::from_secs(2))
}

fn released_command(runtime_url: &str) -> Command {
    let mut command = Command::new(env_path("AWA_RELEASED_PYTHON"));
    command
        .arg(workspace_root().join("awa-python/tests/patch_rehearsal_worker.py"))
        .env("DATABASE_URL", runtime_url)
        .env("REHEARSAL_SCHEMA", SCHEMA)
        .env(
            "REHEARSAL_HEARTBEAT_STALENESS_MS",
            rehearsal_heartbeat_staleness().as_millis().to_string(),
        )
        .env("PYTHONUNBUFFERED", "1")
        .kill_on_drop(true);
    command
}

async fn spawn_released_worker(
    runtime_url: &str,
    log: &EventLog,
    queues: &[&str],
) -> ReleasedWorker {
    let mut command = released_command(runtime_url);
    command
        .env("REHEARSAL_MODE", "worker")
        .env("REHEARSAL_QUEUES", queues.join(","))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = command.spawn().expect("spawn released worker");
    let source = format!("released-{}", child.id().unwrap_or_default());
    let stdout = child.stdout.take().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let reader_log = log.clone();
    let reader_source = source.clone();
    let expected_version = format!("version={}", released_version());
    let reader = tokio::spawn(async move {
        let mut ready_tx = Some(ready_tx);
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.starts_with("READY") {
                assert!(
                    line.split_whitespace()
                        .any(|field| field == expected_version),
                    "the released worker must report {expected_version}: {line}"
                );
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(());
                }
            }
            reader_log.record_line(&reader_source, &line);
        }
    });
    tokio::time::timeout(Duration::from_secs(60), ready_rx)
        .await
        .expect("released worker ready")
        .expect("released worker ready");
    ReleasedWorker {
        child,
        source,
        _reader: reader,
    }
}

struct ProduceSpec<'a> {
    queue: &'a str,
    seq_start: i64,
    count: i64,
    modes: &'a str,
    delay_s: f64,
    pause_ms: u64,
}

/// Run the released producer to completion, logging each enqueue as it
/// happens. Returns stderr on failure.
async fn released_produce(
    runtime_url: &str,
    log: &EventLog,
    spec: ProduceSpec<'_>,
) -> Result<(), String> {
    let mut command = released_command(runtime_url);
    command
        .env("REHEARSAL_MODE", "produce")
        .env("REHEARSAL_QUEUE", spec.queue)
        .env("REHEARSAL_SEQ_START", spec.seq_start.to_string())
        .env("REHEARSAL_COUNT", spec.count.to_string())
        .env("REHEARSAL_JOB_MODES", spec.modes)
        .env("REHEARSAL_DELAY_S", spec.delay_s.to_string())
        .env("REHEARSAL_PAUSE_MS", spec.pause_ms.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn released producer");
    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let stderr_text = tokio::spawn(async move {
        let mut text = String::new();
        let _ = tokio::io::AsyncReadExt::read_to_string(&mut stderr, &mut text).await;
        text
    });
    let mut produced = false;
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        produced |= line.starts_with("PRODUCED");
        log.record_line("released-producer", &line);
    }
    let status = child.wait().await.expect("wait for released producer");
    let stderr_text = stderr_text.await.unwrap();
    if status.success() && produced {
        Ok(())
    } else {
        Err(stderr_text)
    }
}

async fn run_cli(binary: &PathBuf, database_url: &str, args: &[&str]) -> (bool, String, String) {
    let output = Command::new(binary)
        .arg("--database-url")
        .arg(database_url)
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .await
        .unwrap_or_else(|error| panic!("run {}: {error}", binary.display()));
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

async fn cli_ok(binary: &PathBuf, database_url: &str, args: &[&str]) -> String {
    let (ok, stdout, stderr) = run_cli(binary, database_url, args).await;
    assert!(
        ok,
        "{} {args:?} failed:\n{stdout}\n{stderr}",
        binary.display()
    );
    stdout
}

const MIGRATE_ATTEMPTS: usize = 5;
const MIGRATE_RETRY_PAUSE: Duration = Duration::from_millis(500);

/// `awa migrate` under live traffic, re-run while it fails only because the
/// `replica_identity` patch could not lock a table within its lock_timeout.
/// Returns the successful run's stdout, its start and duration, and the number
/// of attempts.
async fn migrate_under_traffic(
    binary: &PathBuf,
    database_url: &str,
) -> (String, Instant, Duration, usize) {
    for attempt in 1..=MIGRATE_ATTEMPTS {
        let started = Instant::now();
        let (ok, stdout, stderr) = run_cli(binary, database_url, &["migrate"]).await;
        if ok {
            return (stdout, started, started.elapsed(), attempt);
        }
        let lock_timeout = stderr.contains("code: \"55P03\"")
            && stderr.contains("replica_identity: could not lock ")
            && stderr.contains("This patch changed nothing and is safe to run again.")
            && stdout.contains("Schema patch failed and changed nothing");
        assert!(
            lock_timeout && attempt < MIGRATE_ATTEMPTS,
            "{} [\"migrate\"] failed on attempt {attempt} of {MIGRATE_ATTEMPTS}:\n{stdout}\n{stderr}",
            binary.display()
        );
        eprintln!(
            "[patch-rehearsal] awa migrate attempt {attempt} of {MIGRATE_ATTEMPTS} hit the \
             replica_identity lock timeout; retrying in {MIGRATE_RETRY_PAUSE:?}:\n{}",
            stderr.trim()
        );
        tokio::time::sleep(MIGRATE_RETRY_PAUSE).await;
    }
    unreachable!()
}

// ── database fixture ───────────────────────────────────────────────────

struct Fixture {
    database: String,
    owner: String,
    runtime: String,
    owner_url: String,
    runtime_url: String,
    admin: PgPool,
}

impl Fixture {
    async fn create() -> Self {
        let suffix = uuid::Uuid::new_v4().simple().to_string()[..12].to_string();
        let owner = format!("rh_owner_{suffix}");
        let runtime = format!("rh_runtime_{suffix}");
        let database = format!("rh_patch_{suffix}");
        let server = PgPoolOptions::new()
            .max_connections(1)
            .connect(&superuser_url())
            .await
            .unwrap();
        sqlx::raw_sql(&format!(
            "CREATE ROLE {owner} LOGIN PASSWORD '{PASSWORD}'; \
             CREATE ROLE {runtime} LOGIN PASSWORD '{PASSWORD}';"
        ))
        .execute(&server)
        .await
        .unwrap();
        sqlx::raw_sql(&format!("CREATE DATABASE {database} OWNER {owner}"))
            .execute(&server)
            .await
            .unwrap();
        server.close().await;
        let base = PgConnectOptions::from_str(&superuser_url()).unwrap();
        let url_for = |role: &str| {
            format!(
                "postgres://{role}:{PASSWORD}@{}:{}/{database}",
                base.get_host(),
                base.get_port()
            )
        };
        let admin = PgPoolOptions::new()
            .max_connections(6)
            .connect_with(base.clone().database(&database))
            .await
            .unwrap();
        Self {
            owner_url: url_for(&owner),
            runtime_url: url_for(&runtime),
            database,
            owner,
            runtime,
            admin,
        }
    }

    async fn runtime_pool(&self) -> PgPool {
        PgPoolOptions::new()
            .max_connections(12)
            .connect(&self.runtime_url)
            .await
            .unwrap()
    }

    async fn sql(&self, sql: &str) {
        sqlx::raw_sql(sql)
            .execute(&self.admin)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    async fn drop(self) {
        self.admin.close().await;
        let server = PgPoolOptions::new()
            .max_connections(1)
            .connect(&superuser_url())
            .await
            .unwrap();
        for statement in [
            format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.database),
            format!("DROP ROLE IF EXISTS {}", self.runtime),
            format!("DROP ROLE IF EXISTS {}", self.owner),
        ] {
            sqlx::raw_sql(&statement).execute(&server).await.unwrap();
        }
        server.close().await;
    }
}

/// The grants a working released deployment gives its runtime role: the
/// documented DML/sequence/function grants, plus schema `CREATE`, which the
/// 0.6.9 lane helpers need to create lane sequences lazily.
async fn grant_released_runtime(fixture: &Fixture) {
    let runtime = &fixture.runtime;
    let owner = &fixture.owner;
    for schema in ["awa", SCHEMA] {
        fixture
            .sql(&format!(
                "GRANT USAGE, CREATE ON SCHEMA {schema} TO {runtime}; \
                 GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {schema} TO {runtime}; \
                 GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {runtime}; \
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {runtime}; \
                 ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} \
                   GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO {runtime}; \
                 ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} \
                   GRANT USAGE, SELECT ON SEQUENCES TO {runtime}; \
                 ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} \
                   GRANT EXECUTE ON FUNCTIONS TO {runtime};"
            ))
            .await;
    }
    fixture
        .sql(&format!(
            "REVOKE EXECUTE ON FUNCTION awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN) \
             FROM {runtime}"
        ))
        .await;
}

/// The optional tightening step from the upgrade guide, run by the owner after
/// provisioning every queue: grant sequence `UPDATE` on the owner's sequences
/// and revoke runtime `CREATE`.
fn tighten_runtime_sql(schemas: &[&str], runtime: &str) -> String {
    let schema_list = schemas
        .iter()
        .map(|schema| format!("'{schema}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        r#"
        DO $$
        DECLARE v_seq TEXT;
        BEGIN
            FOR v_seq IN
                SELECT format('%I.%I', schemaname, sequencename) FROM pg_sequences
                WHERE schemaname IN ({schema_list}) AND sequenceowner = current_user
            LOOP
                EXECUTE format('GRANT USAGE, SELECT, UPDATE ON SEQUENCE %s TO {runtime}', v_seq);
            END LOOP;
        END
        $$;
        "#
    );
    for schema in schemas {
        sql.push_str(&format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} \
               GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {runtime};\n\
             REVOKE CREATE ON SCHEMA {schema} FROM {runtime};\n"
        ));
    }
    sql
}

/// `(queue, priority, shard) -> (next enqueue seq, next claim seq)` for every
/// lane in the rehearsal schema.
async fn lane_positions(pool: &PgPool) -> BTreeMap<(String, i16, i16), (i64, i64)> {
    let rows: Vec<(String, i16, i16, i64, i64)> = sqlx::query_as(&format!(
        "SELECT e.queue, e.priority, e.enqueue_shard, \
                {SCHEMA}.sequence_next_value(e.seq_name), \
                {SCHEMA}.sequence_next_value(c.seq_name) \
         FROM {SCHEMA}.queue_enqueue_heads AS e \
         JOIN {SCHEMA}.queue_claim_heads AS c USING (queue, priority, enqueue_shard)"
    ))
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(queue, priority, shard, enqueue, claim)| {
            ((queue, priority, shard), (enqueue, claim))
        })
        .collect()
}

async fn existing_relation_ids(pool: &PgPool) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT c.oid::bigint FROM pg_class AS c \
         JOIN pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE n.nspname IN ('awa', $1) ORDER BY c.oid",
    )
    .bind(SCHEMA)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Relation locks stronger than `RowExclusiveLock` held by `role` on the
/// pre-existing `awa` or queue-storage relations.
async fn strong_locks_held_by(pool: &PgPool, role: &str, relation_ids: &[i64]) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT format('%s.%s %s', n.nspname, c.relname, l.mode) \
         FROM pg_locks AS l \
         JOIN pg_stat_activity AS a ON a.pid = l.pid \
         JOIN pg_class AS c ON c.oid = l.relation \
         JOIN pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE a.usename = $1 AND l.locktype = 'relation' \
           AND n.nspname IN ('awa', $2) \
           AND l.relation::bigint = ANY($3) \
           AND l.mode IN ('ShareUpdateExclusiveLock', 'ShareLock', 'ShareRowExclusiveLock', \
                          'ExclusiveLock', 'AccessExclusiveLock')",
    )
    .bind(role)
    .bind(SCHEMA)
    .bind(relation_ids)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// The locks the `replica_identity` patch takes on the tables it alters, as
/// formatted by `strong_locks_held_by`.
fn replica_identity_lock(lock: &str) -> bool {
    let Some((relation, mode)) = lock.rsplit_once(' ') else {
        return false;
    };
    if relation == "awa.idx_awa_jobs_unique" {
        return mode == "ShareLock";
    }
    let Some((schema, table)) = relation.split_once('.') else {
        return false;
    };
    let delta = table
        .strip_prefix("queue_terminal_count_deltas")
        .is_some_and(|suffix| {
            suffix.is_empty()
                || suffix.strip_prefix('_').is_some_and(|slot| {
                    !slot.is_empty() && slot.bytes().all(|b| b.is_ascii_digit())
                })
        });
    let awa_table = schema == "awa"
        && matches!(
            table,
            "admin_dirty_queue_marks" | "admin_dirty_kind_marks" | "job_unique_claims"
        );
    mode == "AccessExclusiveLock" && (delta || awa_table)
}

async fn lane_patch_applied(pool: &PgPool) -> migrations::SchemaPatchStatus {
    migrations::schema_patch_status(pool)
        .await
        .unwrap()
        .into_iter()
        .find(|status| status.name == "prepared_lane_sequences")
        .unwrap()
}

async fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until every enqueued job has completed.
async fn wait_for_drain(log: &EventLog, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let events = log.snapshot();
        let enqueued: HashSet<i64> = events
            .iter()
            .filter(|e| e.kind == EventKind::Enqueued)
            .map(|e| e.job_id)
            .collect();
        let completed: HashSet<i64> = events
            .iter()
            .filter(|e| e.kind == EventKind::Complete)
            .map(|e| e.job_id)
            .collect();
        let missing: Vec<i64> = enqueued.difference(&completed).copied().collect();
        if missing.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} jobs never completed, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(10)]
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until the database records every logged job as completed. A worker
/// logs a completion before writing it, so a worker killed straight after
/// `wait_for_drain` can leave its last jobs running.
async fn wait_for_durable_completions(admin: &PgPool, log: &EventLog, timeout: Duration) {
    let mut pending: Vec<i64> = log
        .snapshot()
        .iter()
        .filter(|e| e.kind == EventKind::Enqueued)
        .map(|e| e.job_id)
        .collect();
    pending.sort_unstable();
    pending.dedup();
    let deadline = Instant::now() + timeout;
    loop {
        let mut running = Vec::new();
        for job_id in pending {
            let row = admin::get_job(admin, job_id)
                .await
                .unwrap_or_else(|error| panic!("job {job_id}: {error}"));
            if row.state != JobState::Completed {
                running.push(job_id);
            }
        }
        pending = running;
        if pending.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} completions never became durable, e.g. {:?}",
            pending.len(),
            &pending[..pending.len().min(10)]
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release gate: needs matching AWA_RELEASED_CLI and AWA_RELEASED_PYTHON artifacts"]
async fn released_patch_upgrade_and_rollback() {
    let released_version = released_version();
    let released_observes_controls = released().observes_runtime_controls;
    let released_cli = env_path("AWA_RELEASED_CLI");
    let candidate_cli = candidate_cli();
    assert!(
        candidate_cli.exists(),
        "candidate CLI not found at {}; run cargo build -p awa-cli or set AWA_CANDIDATE_CLI",
        candidate_cli.display()
    );
    let (_, version, _) = run_cli(&released_cli, "postgres://unused", &["--version"]).await;
    assert_eq!(
        version.trim(),
        format!("awa {released_version}"),
        "AWA_RELEASED_CLI must match AWA_RELEASED_VERSION"
    );

    let fixture = Fixture::create().await;
    let log = EventLog::default();
    let mut killed_sources = HashSet::new();

    // ── 1. released install and traffic ───────────────────────────────────
    cli_ok(&released_cli, &fixture.owner_url, &["migrate"]).await;
    for args in [
        &[
            "storage",
            "prepare-queue-storage-schema",
            "--schema",
            SCHEMA,
        ][..],
        &[
            "storage",
            "prepare",
            "--engine",
            "queue_storage",
            "--details",
            &format!(r#"{{"schema":"{SCHEMA}"}}"#),
        ][..],
        &["storage", "enter-mixed-transition", "--quiesced"][..],
        &["storage", "finalize"][..],
    ] {
        cli_ok(&released_cli, &fixture.owner_url, args).await;
    }
    grant_released_runtime(&fixture).await;
    let expected_pending = released().pending_patches();
    let lane_patch_pending = expected_pending.contains(&"prepared_lane_sequences");
    let before = lane_patch_applied(&fixture.admin).await;
    assert_eq!(
        before.applied, !lane_patch_pending,
        "lane patch on a {released_version} install"
    );
    if lane_patch_pending {
        assert!(before
            .needs_repair
            .contains(&format!("lane helpers in schema {SCHEMA}")));
    } else {
        assert!(before.needs_repair.is_empty());
    }
    let pending = migrations::pending_schema_patches(&fixture.admin)
        .await
        .unwrap();
    assert_eq!(
        pending.iter().map(|patch| patch.name).collect::<Vec<_>>(),
        expected_pending,
        "schema patches a released {released_version} deployment needs"
    );

    let worker_a = spawn_released_worker(&fixture.runtime_url, &log, &[MAIN]).await;
    let worker_b = spawn_released_worker(&fixture.runtime_url, &log, &[MAIN]).await;
    released_produce(
        &fixture.runtime_url,
        &log,
        ProduceSpec {
            queue: BANKED,
            seq_start: 0,
            count: 200,
            modes: "ok",
            delay_s: 0.0,
            pause_ms: 0,
        },
    )
    .await
    .expect("bank a backlog");
    released_produce(
        &fixture.runtime_url,
        &log,
        ProduceSpec {
            queue: MAIN,
            seq_start: 1_000,
            count: 320,
            // Enough slow work to keep every released worker busy through
            // the patch.
            modes: "ok,slow,fail_once,slow",
            delay_s: 0.0,
            pause_ms: 0,
        },
    )
    .await
    .expect("main backlog");
    released_produce(
        &fixture.runtime_url,
        &log,
        ProduceSpec {
            queue: MAIN,
            seq_start: 2_000,
            count: 40,
            modes: "ok,fail_once",
            delay_s: SCHEDULE_DELAY.as_secs_f64(),
            pause_ms: 0,
        },
    )
    .await
    .expect("scheduled jobs");
    wait_until(
        "released workers to make progress",
        Duration::from_secs(60),
        || log.count(EventKind::Complete) >= 40 && log.count(EventKind::Retry) >= 5,
    )
    .await;

    // ── 2. candidate patch under traffic ───────────────────────────────
    // Audit the complete exported patch in one owner transaction.
    let patch_sql = cli_ok(
        &candidate_cli,
        &fixture.owner_url,
        &["migrate", "--sql", "--pending", "--no-transaction"],
    )
    .await;
    assert_eq!(
        patch_sql.matches("-- Schema patch R__").count(),
        expected_pending.len()
    );
    for patch in migrations::SCHEMA_PATCHES {
        assert_eq!(
            patch_sql.contains(&format!("-- Schema patch R__{}", patch.name)),
            expected_pending.contains(&patch.name),
            "R__{} in the patch exported for {released_version}",
            patch.name
        );
    }
    assert!(
        !patch_sql.contains("-- Migration V"),
        "a v40 database needs only the patch"
    );
    let patch_relation_ids = existing_relation_ids(&fixture.admin).await;
    assert!(!patch_relation_ids.is_empty());
    let patch_relation_locks = {
        let owner = PgPoolOptions::new()
            .max_connections(1)
            .connect(&fixture.owner_url)
            .await
            .unwrap();
        let mut attempt = 0;
        let locks: Vec<String> = loop {
            attempt += 1;
            let mut tx = owner.begin().await.unwrap();
            let patch_backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
            if let Err(error) = sqlx::raw_sql(&patch_sql).execute(&mut *tx).await {
                let lock_timeout = matches!(
                    &error,
                    sqlx::Error::Database(db)
                        if db.code().as_deref() == Some("55P03")
                            && db.message().starts_with("replica_identity: could not lock ")
                );
                assert!(
                    lock_timeout && attempt < MIGRATE_ATTEMPTS,
                    "exported patch failed on attempt {attempt} of {MIGRATE_ATTEMPTS}: {error:?}"
                );
                eprintln!(
                    "[patch-rehearsal] exported patch attempt {attempt} of {MIGRATE_ATTEMPTS} hit \
                     the replica_identity lock timeout; retrying in {MIGRATE_RETRY_PAUSE:?}: {error}"
                );
                tx.rollback().await.unwrap();
                tokio::time::sleep(MIGRATE_RETRY_PAUSE).await;
                continue;
            }
            let locks: Vec<String> = sqlx::query_scalar(
                "SELECT format('%s.%s %s', n.nspname, c.relname, l.mode) \
                 FROM pg_locks AS l \
                 JOIN pg_class AS c ON c.oid = l.relation \
                 JOIN pg_namespace AS n ON n.oid = c.relnamespace \
                 WHERE l.pid = $1 AND l.locktype = 'relation' \
                   AND n.nspname IN ('awa', $2) \
                   AND l.relation::bigint = ANY($3) \
                 ORDER BY n.nspname, c.relname, l.mode",
            )
            .bind(patch_backend_pid)
            .bind(SCHEMA)
            .bind(&patch_relation_ids)
            .fetch_all(&fixture.admin)
            .await
            .unwrap();
            tx.rollback().await.unwrap();
            break locks;
        };
        owner.close().await;
        let (identity_locks, other_locks): (Vec<String>, Vec<String>) = locks
            .into_iter()
            .partition(|lock| replica_identity_lock(lock));
        assert!(
            other_locks.is_empty(),
            "the patch locked pre-existing awa or queue-storage relations beyond the \
             replica identity targets: {other_locks:?}"
        );
        assert!(
            identity_locks.contains(&"awa.job_unique_claims AccessExclusiveLock".to_string()),
            "the replica_identity patch alters awa.job_unique_claims: {identity_locks:?}"
        );
        identity_locks
    };

    let positions_before = lane_positions(&fixture.admin).await;
    let banked_before: Vec<_> = positions_before
        .iter()
        .filter(|((queue, _, _), _)| queue.starts_with(BANKED))
        .map(|(lane, position)| (lane.clone(), *position))
        .collect();
    assert!(!banked_before.is_empty());

    // Released traffic keeps flowing while the candidate migrates.
    let producer_url = fixture.runtime_url.clone();
    let producer_log = log.clone();
    let producer = tokio::spawn(async move {
        released_produce(
            &producer_url,
            &producer_log,
            ProduceSpec {
                queue: MAIN,
                seq_start: 3_000,
                count: 120,
                modes: "ok,ok,fail_once",
                delay_s: 0.0,
                pause_ms: 15,
            },
        )
        .await
    });
    let concurrent = |e: &Event| e.kind == EventKind::Enqueued && (3_000..3_120).contains(&e.seq);
    wait_until(
        "the concurrent producer to start",
        Duration::from_secs(30),
        || log.snapshot().iter().any(concurrent),
    )
    .await;
    let migrate_relation_ids = existing_relation_ids(&fixture.admin).await;
    assert!(!migrate_relation_ids.is_empty());
    let sampler_relation_ids = migrate_relation_ids.clone();
    let sampler_pool = fixture.admin.clone();
    let owner_role = fixture.owner.clone();
    let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let sampler_flag = sampling.clone();
    let sampler = tokio::spawn(async move {
        let mut seen = Vec::new();
        while sampler_flag.load(std::sync::atomic::Ordering::Relaxed) {
            seen.extend(
                strong_locks_held_by(&sampler_pool, &owner_role, &sampler_relation_ids).await,
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        seen
    });
    let completes_before_migrate = log.count(EventKind::Complete);
    let (migrate_out, migrate_started, migrate_elapsed, migrate_attempts) =
        migrate_under_traffic(&candidate_cli, &fixture.owner_url).await;
    sampling.store(false, std::sync::atomic::Ordering::Relaxed);
    let sampled_locks = sampler.await.unwrap();
    let migrate_ended = migrate_started + migrate_elapsed;
    let during = |e: &Event| e.at >= migrate_started && e.at <= migrate_ended;
    let events = log.snapshot();
    let enqueued_during = events.iter().filter(|e| concurrent(e) && during(e)).count();
    let completed_during = events
        .iter()
        .filter(|e| e.kind == EventKind::Complete && during(e))
        .count();
    // Jobs that were running when the patch started; the final accounting
    // checks each of them completed.
    let started_before: HashSet<i64> = events
        .iter()
        .filter(|e| e.kind == EventKind::Start && e.at < migrate_started)
        .map(|e| e.job_id)
        .collect();
    let finished_before: HashSet<i64> = events
        .iter()
        .filter(|e| e.kind == EventKind::Complete && e.at <= migrate_started)
        .map(|e| e.job_id)
        .collect();
    let in_flight_count = started_before
        .iter()
        .filter(|job| !finished_before.contains(job))
        .count();
    assert!(
        enqueued_during > 0 && in_flight_count > 0,
        "the patch must run under traffic: {enqueued_during} enqueued while awa migrate ran \
         and {in_flight_count} jobs in flight when it started"
    );
    let unexpected_locks: Vec<&String> = sampled_locks
        .iter()
        .filter(|lock| !replica_identity_lock(lock))
        .collect();
    assert!(
        unexpected_locks.is_empty(),
        "awa migrate strongly locked pre-existing relations beyond the replica identity \
         targets: {unexpected_locks:?}"
    );
    let unexpected_lock_count = unexpected_locks.len();
    let mut expected_lines = vec!["Schema: v40 (unchanged)".to_string()];
    for patch in migrations::SCHEMA_PATCHES {
        let outcome = if expected_pending.contains(&patch.name)
            && (migrate_attempts == 1
                || patch.name == "replica_identity"
                || patch.name == "spent_claim_prefix_gaps")
        {
            "applied"
        } else {
            "already present"
        };
        expected_lines.push(format!("Schema patch {}: {outcome}", patch.name));
    }
    expected_lines.push(format!(
        "Storage routing: unchanged (state active, engine queue_storage, schema {SCHEMA}"
    ));
    for expected in expected_lines {
        assert!(
            migrate_out.contains(&expected),
            "missing {expected:?} in:\n{migrate_out}"
        );
    }
    if lane_patch_pending && migrate_attempts == 1 {
        assert!(migrate_out.contains("lane helpers in schema awa"));
        assert!(migrate_out.contains(&format!("lane helpers in schema {SCHEMA}")));
    }
    assert!(lane_patch_applied(&fixture.admin).await.applied);
    assert!(migrations::schema_patch_status(&fixture.admin)
        .await
        .unwrap()
        .iter()
        .all(|patch| patch.applied && patch.needs_repair.is_empty()));
    producer
        .await
        .unwrap()
        .expect("released producer during the patch");

    // Lanes nobody touched keep their exact positions; the rest only advance.
    let positions_after = lane_positions(&fixture.admin).await;
    for (lane, position) in &banked_before {
        assert_eq!(
            positions_after.get(lane),
            Some(position),
            "banked lane {lane:?} moved"
        );
    }
    for (lane, (enqueue, claim)) in &positions_before {
        let (enqueue_after, claim_after) = positions_after[lane];
        assert!(
            enqueue_after >= *enqueue && claim_after >= *claim,
            "lane {lane:?} went backwards"
        );
    }
    wait_until(
        "released workers to keep completing after the patch",
        Duration::from_secs(30),
        || log.count(EventKind::Complete) > completes_before_migrate + 20,
    )
    .await;

    // ── 3. mixed fleet, retries, scheduled work, a killed worker ───────
    let runtime = fixture.runtime_pool().await;
    let candidate_a = candidate_client(
        &runtime,
        &log,
        "candidate-a",
        &[MAIN, BANKED, NEW_AFTER_PATCH],
    );
    candidate_a.start().await.expect("candidate workers start");
    let released_pids = [
        worker_a.child.id().unwrap() as i32,
        worker_b.child.id().unwrap() as i32,
    ];
    let halt = runtime_control::halt(
        &fixture.admin,
        runtime_control::PRUNE_SCOPE,
        "mixed released-fleet rehearsal",
        "drain",
    )
    .await
    .unwrap();
    let halt_id = halt.halt_id.unwrap();
    let control_deadline = Instant::now() + Duration::from_secs(30);
    let mixed_fleet_control_status = loop {
        let status = runtime_control::status(&fixture.admin).await.unwrap();
        let released: Vec<_> = status
            .runtimes
            .iter()
            .filter(|runtime| released_pids.contains(&runtime.pid) && runtime.live)
            .collect();
        let candidate_observes = status.runtimes.iter().any(|runtime| {
            runtime.live
                && !released_pids.contains(&runtime.pid)
                && runtime.observes_current_request
        });
        let fleet_visible = released.len() == 2
            && released.iter().any(|runtime| runtime.leader)
            && released.iter().any(|runtime| !runtime.leader)
            && candidate_observes;
        let released_observe = released
            .iter()
            .all(|runtime| runtime.observes_current_request);
        if fleet_visible && (!released_observes_controls || released_observe) {
            assert_eq!(status.requested.halt_id, Some(halt_id));
            for runtime in &released {
                assert_eq!(runtime.version, released_version);
            }
            if released_observes_controls {
                assert!(
                    matches!(
                        status.state,
                        runtime_control::RuntimeControlState::Draining
                            | runtime_control::RuntimeControlState::Halted
                    ),
                    "a fleet that observes the halt drains or halts: {status:?}"
                );
                assert!(status.unobserving_runtimes.is_empty(), "{status:?}");
                for runtime in &released {
                    assert!(runtime.observation.is_some());
                }
            } else {
                assert_eq!(
                    status.state,
                    runtime_control::RuntimeControlState::Unconfirmed
                );
                for runtime in &released {
                    assert!(runtime.observation.is_none());
                    assert!(status
                        .unobserving_runtimes
                        .iter()
                        .any(|unobserving| unobserving.instance_id == runtime.instance_id));
                }
            }
            break serde_json::to_value(&status).unwrap();
        }
        assert!(
            Instant::now() < control_deadline,
            "released leader and nonleader must remain visible during the halt: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let resumed = runtime_control::resume(&fixture.admin, runtime_control::PRUNE_SCOPE, halt_id)
        .await
        .unwrap();
    assert!(resumed.halt_id.is_none());
    let resumed_status = runtime_control::status(&fixture.admin).await.unwrap();
    assert!(resumed_status.requested.halt_id.is_none());
    if released_observes_controls {
        assert_eq!(
            resumed_status.state,
            runtime_control::RuntimeControlState::Running
        );
        assert!(resumed_status.unobserving_runtimes.is_empty());
    } else {
        assert_eq!(
            resumed_status.state,
            runtime_control::RuntimeControlState::Degraded
        );
        assert!(released_pids.iter().all(|pid| resumed_status
            .unobserving_runtimes
            .iter()
            .any(|runtime| runtime.pid == *pid && runtime.live)));
    }
    candidate_produce(
        &runtime,
        &log,
        MAIN,
        4_000,
        80,
        &["ok", "slow", "fail_once"],
    )
    .await;
    released_produce(
        &fixture.runtime_url,
        &log,
        ProduceSpec {
            queue: NEW_AFTER_PATCH,
            seq_start: 5_000,
            count: 60,
            modes: "ok,fail_once",
            delay_s: 0.0,
            pause_ms: 0,
        },
    )
    .await
    .expect("released enqueue on a lane created after the patch");

    // Kill a released worker with a slow job in flight; its jobs are rescued.
    wait_until(
        "a slow job in flight on worker a",
        Duration::from_secs(60),
        || {
            let events = log.snapshot();
            let done: HashSet<i64> = events
                .iter()
                .filter(|e| e.kind == EventKind::Complete)
                .map(|e| e.job_id)
                .collect();
            events.iter().any(|e| {
                e.kind == EventKind::Start
                    && e.source == worker_a.source
                    && e.mode == "slow"
                    && !done.contains(&e.job_id)
            })
        },
    )
    .await;
    killed_sources.insert(worker_a.kill().await);
    let worker_c = spawn_released_worker(&fixture.runtime_url, &log, &[MAIN, BANKED]).await;

    // Restart the candidate client too.
    candidate_a.shutdown(Duration::from_secs(5)).await;
    let candidate_b = candidate_client(
        &runtime,
        &log,
        "candidate-b",
        &[MAIN, BANKED, NEW_AFTER_PATCH],
    );
    candidate_b.start().await.expect("candidate restart");
    wait_for_drain(&log, ci_timing::scaled_timeout(Duration::from_secs(120))).await;

    // ── 4. roll back to released binaries ──────────────────────────────
    candidate_b.shutdown(Duration::from_secs(5)).await;
    let rollback_migrate = cli_ok(&released_cli, &fixture.owner_url, &["migrate"]).await;
    assert!(rollback_migrate.contains("Migrations applied successfully"));
    assert!(
        migrations::schema_patch_status(&fixture.admin)
            .await
            .unwrap()
            .iter()
            .all(|patch| patch.applied && patch.needs_repair.is_empty()),
        "the released binaries must leave every candidate patch installed"
    );
    killed_sources.insert(worker_b.kill().await);
    let worker_d = spawn_released_worker(
        &fixture.runtime_url,
        &log,
        &[MAIN, BANKED, NEW_AFTER_PATCH, NEW_AFTER_ROLLBACK],
    )
    .await;
    for (queue, seq_start) in [
        (MAIN, 6_000),
        (NEW_AFTER_ROLLBACK, 7_000),
        (NEW_AFTER_PATCH, 8_000),
    ] {
        released_produce(
            &fixture.runtime_url,
            &log,
            ProduceSpec {
                queue,
                seq_start,
                count: 50,
                modes: "ok,fail_once,slow",
                delay_s: 0.0,
                pause_ms: 0,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("released enqueue on {queue} after rollback: {error}"));
    }
    wait_for_drain(&log, ci_timing::scaled_timeout(Duration::from_secs(120))).await;
    // A released admin process reads the patched schema as the runtime role.
    let stats = cli_ok(&released_cli, &fixture.runtime_url, &["queue", "stats"]).await;
    assert!(stats.contains(NEW_AFTER_ROLLBACK), "{stats}");

    // ── 5. optional privilege tightening, released workers restart ─────
    let queues = [
        MAIN,
        BANKED,
        NEW_AFTER_PATCH,
        NEW_AFTER_ROLLBACK,
        PROVISIONED,
    ];
    for queue in queues {
        cli_ok(
            &candidate_cli,
            &fixture.owner_url,
            &[
                "storage",
                "prepare-queue",
                "--schema",
                SCHEMA,
                "--queue",
                queue,
            ],
        )
        .await;
    }
    // Run as the owner, as an operator would, with the SQL from
    // docs/upgrade-0.6.9-to-0.6.10.md. Sequences the 0.6.9 runtime created
    // lazily belong to the runtime already, and GRANT ... ON ALL SEQUENCES
    // fails on them, so the grant covers the owner's sequences only.
    let runtime_role = &fixture.runtime;
    let owner_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&fixture.owner_url)
        .await
        .unwrap();
    sqlx::raw_sql(&tighten_runtime_sql(&["awa", SCHEMA], runtime_role))
        .execute(&owner_pool)
        .await
        .expect("tighten runtime privileges as the owner");
    owner_pool.close().await;
    let ungranted: Vec<String> = sqlx::query_scalar(
        "SELECT format('%I.%I', schemaname, sequencename) FROM pg_sequences \
         WHERE schemaname IN ('awa', $1) \
           AND NOT has_sequence_privilege($2, format('%I.%I', schemaname, sequencename), 'USAGE, SELECT, UPDATE')",
    )
    .bind(SCHEMA)
    .bind(runtime_role)
    .fetch_all(&fixture.admin)
    .await
    .unwrap();
    assert_eq!(ungranted, Vec::<String>::new());
    let can_create: bool = sqlx::query_scalar(
        "SELECT has_schema_privilege($1, $2, 'CREATE') OR has_schema_privilege($1, 'awa', 'CREATE')",
    )
    .bind(runtime_role)
    .bind(SCHEMA)
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert!(!can_create);

    killed_sources.insert(worker_c.kill().await);
    killed_sources.insert(worker_d.kill().await);
    let restricted_queues = [
        MAIN,
        BANKED,
        NEW_AFTER_PATCH,
        NEW_AFTER_ROLLBACK,
        PROVISIONED,
        UNPROVISIONED,
    ];
    let worker_e = spawn_released_worker(&fixture.runtime_url, &log, &restricted_queues).await;
    let worker_f = spawn_released_worker(&fixture.runtime_url, &log, &restricted_queues).await;
    // A released producer without CREATE creates an unprepared queue's lane
    // through the schema owner's provisioning function.
    for (queue, seq_start) in [
        (MAIN, 9_000),
        (PROVISIONED, 10_000),
        (NEW_AFTER_ROLLBACK, 11_000),
        (UNPROVISIONED, 12_000),
    ] {
        released_produce(
            &fixture.runtime_url,
            &log,
            ProduceSpec {
                queue,
                seq_start,
                count: 50,
                modes: "ok,fail_once,slow",
                delay_s: 0.0,
                pause_ms: 0,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("restricted released enqueue on {queue}: {error}"));
    }
    let new_lane_owners: Vec<bool> = sqlx::query_scalar(&format!(
        "SELECT sequence.relowner = namespace.nspowner \
         FROM {SCHEMA}.queue_enqueue_heads AS heads \
         JOIN pg_namespace AS namespace ON namespace.nspname = '{SCHEMA}' \
         JOIN pg_class AS sequence \
           ON sequence.relnamespace = namespace.oid AND sequence.relname = heads.seq_name \
         WHERE heads.queue = $1"
    ))
    .bind(UNPROVISIONED)
    .fetch_all(&fixture.admin)
    .await
    .unwrap();
    assert!(
        !new_lane_owners.is_empty() && new_lane_owners.iter().all(|owned| *owned),
        "the new lane's sequences belong to the schema owner: {new_lane_owners:?}"
    );
    wait_for_drain(&log, ci_timing::scaled_timeout(Duration::from_secs(120))).await;
    wait_for_durable_completions(
        &fixture.admin,
        &log,
        ci_timing::scaled_timeout(Duration::from_secs(120)),
    )
    .await;
    let stats = cli_ok(&released_cli, &fixture.runtime_url, &["queue", "stats"]).await;
    assert!(stats.contains(PROVISIONED), "{stats}");
    for worker in [worker_e, worker_f] {
        worker.kill().await;
    }
    runtime.close().await;

    // ── accounting ─────────────────────────────────────────────────────
    let events = log.snapshot();
    let enqueued: BTreeMap<i64, &Event> = events
        .iter()
        .filter(|e| e.kind == EventKind::Enqueued)
        .map(|e| (e.job_id, e))
        .collect();
    let mut completions: HashMap<i64, Vec<&Event>> = HashMap::new();
    let mut retries: HashSet<i64> = HashSet::new();
    for event in &events {
        match event.kind {
            EventKind::Complete => completions.entry(event.job_id).or_default().push(event),
            // Handlers retry fail_once jobs on their first attempt only.
            EventKind::Retry => {
                assert_eq!(
                    event.attempt, 1,
                    "job {} retried on a later attempt",
                    event.job_id
                );
                retries.insert(event.job_id);
            }
            _ => {}
        }
    }
    let mut per_queue: BTreeMap<String, i64> = BTreeMap::new();
    for (job_id, enqueue) in &enqueued {
        *per_queue.entry(enqueue.queue.clone()).or_default() += 1;
        let done = completions
            .get(job_id)
            .unwrap_or_else(|| panic!("job {job_id} never completed"));
        if done.len() > 1 {
            assert!(
                done[..done.len() - 1]
                    .iter()
                    .all(|event| killed_sources.contains(&event.source)),
                "job {job_id} completed more than once outside a killed worker: {done:?}"
            );
        }
        let row = admin::get_job(&fixture.admin, *job_id)
            .await
            .unwrap_or_else(|error| panic!("job {job_id}: {error}"));
        assert_eq!(row.state, JobState::Completed, "job {job_id}");
        if enqueue.mode == "fail_once" {
            // The first attempt either ran and asked for a retry, or was
            // claimed by a worker killed before the handler returned, so a
            // rescue started attempt 2 without a Retry event. A surviving
            // worker's first attempt always ends in a Retry.
            let first_attempt_starts: Vec<&Event> = events
                .iter()
                .filter(|e| e.kind == EventKind::Start && e.job_id == *job_id && e.attempt == 1)
                .collect();
            assert!(
                retries.contains(job_id)
                    || first_attempt_starts
                        .iter()
                        .all(|e| killed_sources.contains(&e.source)),
                "fail_once job {job_id} ran attempt 1 on a surviving worker without retrying: \
                 {first_attempt_starts:?}"
            );
            assert!(
                row.attempt >= 2,
                "fail_once job {job_id} completed on attempt {}",
                row.attempt
            );
        }
    }
    let store = QueueStorage::new(QueueStorageConfig {
        schema: SCHEMA.to_string(),
        ..Default::default()
    })
    .unwrap();
    for (queue, count) in &per_queue {
        let counts = store.queue_counts(&fixture.admin, queue).await.unwrap();
        assert_eq!(
            (counts.available, counts.running, counts.terminal),
            (0, 0, *count),
            "queue {queue}"
        );
    }
    // Jobs scheduled under released binaries ran once due, after the patch.
    let patched_at = migrate_started + migrate_elapsed;
    let scheduled: Vec<&Event> = enqueued
        .values()
        .filter(|e| (2_000..2_040).contains(&e.seq))
        .copied()
        .collect();
    assert_eq!(scheduled.len(), 40);
    for job in scheduled {
        let first_start = events
            .iter()
            .filter(|e| e.kind == EventKind::Start && e.job_id == job.job_id)
            .map(|e| e.at)
            .min()
            .expect("scheduled job started");
        assert!(
            first_start >= job.at + SCHEDULE_DELAY - Duration::from_secs(1),
            "scheduled job {} ran before it was due",
            job.job_id
        );
        assert!(
            first_start > patched_at,
            "scheduled job {} ran before the patch",
            job.job_id
        );
    }
    let server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
    let report = serde_json::json!({
        "cell": "released_patch_upgrade_and_rollback",
        "result": "pass",
        "candidate_version": env!("CARGO_PKG_VERSION"),
        "released_version": released_version,
        "postgres": server_version,
        "schema": SCHEMA,
        "jobs_enqueued": enqueued.len(),
        "jobs_completed": completions.len(),
        "jobs_retried": retries.len(),
        "jobs_per_queue": per_queue,
        "workers_killed": killed_sources.len(),
        "migrate_ms": migrate_elapsed.as_millis() as u64,
        "migrate_attempts": migrate_attempts,
        "migrate_output": migrate_out,
        "mixed_fleet_control_status": mixed_fleet_control_status,
        "strong_locks_sampled": sampled_locks.len(),
        "patch_relation_ids": patch_relation_ids,
        "patch_relation_locks": patch_relation_locks,
        "migrate_relation_ids": migrate_relation_ids,
        "sampled_relation_locks": sampled_locks,
        "unexpected_relation_locks_sampled": unexpected_lock_count,
        "enqueued_during_migrate": enqueued_during,
        "completed_during_migrate": completed_during,
        "in_flight_at_migrate": in_flight_count,
    });
    eprintln!("[patch-rehearsal] {report}");
    if let Some(dir) = std::env::var_os("AWA_REHEARSAL_ARTIFACT_DIR") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(format!("patch-upgrade-{released_version}.json")),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }
    fixture.drop().await;
}

const ADVANCE: &str = "rh_advance";
const ADVANCE_DEST: &str = "rh_advance_dest";

/// #779: released workers keep claiming a lane while the candidate moves some
/// of its jobs away and advances the lane's claim cursor over the tombstones,
/// as the candidate's maintenance leader does for a lane pinning the queue
/// ring. Every job completes exactly once, in the queue it was in when
/// claimed, and a moved job never runs from its old queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release gate: needs matching AWA_RELEASED_CLI and AWA_RELEASED_PYTHON artifacts"]
async fn released_claimers_with_candidate_claim_cursor_advance() {
    const JOBS: usize = 600;
    const MOVED_PREFIX: usize = 30;
    let released_cli = env_path("AWA_RELEASED_CLI");
    let candidate_cli = candidate_cli();
    assert!(
        candidate_cli.exists(),
        "candidate CLI not found at {}; run cargo build -p awa-cli or set AWA_CANDIDATE_CLI",
        candidate_cli.display()
    );
    let fixture = Fixture::create().await;
    let log = EventLog::default();

    cli_ok(&released_cli, &fixture.owner_url, &["migrate"]).await;
    for args in [
        &[
            "storage",
            "prepare-queue-storage-schema",
            "--schema",
            SCHEMA,
        ][..],
        &[
            "storage",
            "prepare",
            "--engine",
            "queue_storage",
            "--details",
            &format!(r#"{{"schema":"{SCHEMA}"}}"#),
        ][..],
        &["storage", "enter-mixed-transition", "--quiesced"][..],
        &["storage", "finalize"][..],
    ] {
        cli_ok(&released_cli, &fixture.owner_url, args).await;
    }
    grant_released_runtime(&fixture).await;
    cli_ok(&candidate_cli, &fixture.owner_url, &["migrate"]).await;

    released_produce(
        &fixture.runtime_url,
        &log,
        ProduceSpec {
            queue: ADVANCE,
            seq_start: 0,
            count: JOBS as i64,
            modes: "ok",
            delay_s: 0.0,
            pause_ms: 0,
        },
    )
    .await
    .expect("released backlog");
    let mut enqueued: Vec<(i64, i64)> = log
        .snapshot()
        .iter()
        .filter(|event| event.kind == EventKind::Enqueued && event.queue == ADVANCE)
        .map(|event| (event.seq, event.job_id))
        .collect();
    enqueued.sort_unstable();
    let jobs: Vec<i64> = enqueued.into_iter().map(|(_, job_id)| job_id).collect();
    assert_eq!(jobs.len(), JOBS);

    let store = Arc::new(
        QueueStorage::new(QueueStorageConfig {
            schema: SCHEMA.to_string(),
            ..Default::default()
        })
        .unwrap(),
    );
    let pool = fixture.admin.clone();
    store.rotate(&pool).await.expect("seal the backlog's slot");
    let mut moved = Vec::new();
    for job_id in jobs[..MOVED_PREFIX].iter().rev() {
        assert!(store
            .move_queue(&pool, *job_id, ADVANCE_DEST, None)
            .await
            .expect("move queue"));
        moved.push(*job_id);
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let advances = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let advancer = {
        let (store, pool, stop, advances) =
            (store.clone(), pool.clone(), stop.clone(), advances.clone());
        tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            while !stop.load(Ordering::SeqCst) {
                if let Some(pin) = store.queue_ring_pin(&pool).await.expect("read pin") {
                    let advanced = store
                        .advance_spent_claim_heads(&pool, &pin.lanes, 4)
                        .await
                        .expect("advance spent claim heads");
                    assert!(advanced.failed.is_empty(), "{advanced:?}");
                    advances.fetch_add(advanced.advanced.len(), Ordering::SeqCst);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    wait_until(
        "the candidate advance to pass the moved prefix",
        Duration::from_secs(30),
        || advances.load(std::sync::atomic::Ordering::SeqCst) > 0,
    )
    .await;

    let worker = spawn_released_worker(&fixture.runtime_url, &log, &[ADVANCE, ADVANCE_DEST]).await;
    for job_id in jobs[MOVED_PREFIX..].iter().rev().step_by(4) {
        if store
            .move_queue(&pool, *job_id, ADVANCE_DEST, None)
            .await
            .expect("move queue")
        {
            moved.push(*job_id);
        }
    }
    wait_for_drain(&log, Duration::from_secs(180)).await;
    // The worker reports a completion before writing it; wait for the writes.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs WHERE id = ANY($1) AND state = 'completed'",
        )
        .bind(&jobs)
        .fetch_one(&pool)
        .await
        .expect("count completed jobs");
        if completed == JOBS as i64 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {completed} of {JOBS} completions became durable"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    advancer.await.expect("advancer");
    worker.kill().await;

    let mut completions: HashMap<i64, usize> = HashMap::new();
    for event in log.snapshot() {
        if event.kind == EventKind::Complete {
            *completions.entry(event.job_id).or_default() += 1;
        }
    }
    let duplicated: Vec<_> = completions.iter().filter(|(_, n)| **n > 1).collect();
    assert!(
        duplicated.is_empty(),
        "jobs completed twice: {duplicated:?}"
    );
    assert_eq!(
        completions.keys().copied().collect::<HashSet<_>>(),
        jobs.iter().copied().collect::<HashSet<_>>(),
        "every job completes"
    );
    let rows: Vec<(i64, String, String)> =
        sqlx::query_as("SELECT id, queue, state::text FROM awa.jobs WHERE id = ANY($1)")
            .bind(&jobs)
            .fetch_all(&pool)
            .await
            .expect("read final job rows");
    assert_eq!(rows.len(), JOBS);
    let moved_set: HashSet<i64> = moved.iter().copied().collect();
    for (job_id, queue, state) in &rows {
        let expected = if moved_set.contains(job_id) {
            ADVANCE_DEST
        } else {
            ADVANCE
        };
        assert_eq!(
            (queue.as_str(), state.as_str()),
            (expected, "completed"),
            "job {job_id}"
        );
    }
    eprintln!(
        "[cursor-advance-rehearsal] released={} jobs={JOBS} moved={} advances={}",
        released_version(),
        moved.len(),
        advances.load(std::sync::atomic::Ordering::SeqCst)
    );
    fixture.drop().await;
}
