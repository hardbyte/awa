//! Released-artifact rehearsal of the 0.6.9 -> 0.6.10 -> 0.6.9 patch upgrade.
//!
//! The journey an operator follows, as a release gate:
//!
//! 1. Install with the released 0.6.9 CLI as the migration owner, activate
//!    queue storage in a custom schema, and run released 0.6.9 workers and
//!    producers as a separate runtime role with the grants a working 0.6.9
//!    deployment needs (schema `CREATE` included), banking a backlog.
//! 2. Run the candidate `awa migrate` as the owner while that traffic
//!    continues. The patch takes no lock on any queue-storage or `awa` table,
//!    keeps every lane sequence position, and leaves routing unchanged.
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
//! - `AWA_RELEASED_CLI`: the released 0.6.9 `awa` binary (the `awa-cli` wheel)
//! - `AWA_RELEASED_PYTHON`: a Python with the released `awa-pg` 0.6.9 wheel
//! - `AWA_CANDIDATE_CLI` (optional): the candidate `awa`; defaults to
//!   `target/debug/awa`, so build it with `cargo build -p awa-cli` first.
//!
//! Every worker runs with a one-hour ready-ring rotation so completed jobs are
//! still readable when the final accounting runs.

use async_trait::async_trait;
use awa::model::{admin, migrations, JobState, QueueStorage, QueueStorageConfig};
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
/// Long enough for jobs scheduled under 0.6.9 to come due after the patch.
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
        std::env::var_os(var).unwrap_or_else(|| panic!("{var} must name the released artifact")),
    );
    if path.is_absolute() {
        path
    } else {
        workspace_root().join(path)
    }
}

fn candidate_cli() -> PathBuf {
    match std::env::var_os("AWA_CANDIDATE_CLI") {
        Some(path) => PathBuf::from(path),
        None => {
            let target = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| workspace_root().join("target"));
            target.join("debug").join("awa")
        }
    }
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
        .heartbeat_staleness(Duration::from_secs(2))
        .promote_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(200))
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

// ── released (0.6.9 wheel) helpers ─────────────────────────────────────

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

fn released_command(runtime_url: &str) -> Command {
    let mut command = Command::new(env_path("AWA_RELEASED_PYTHON"));
    command
        .arg(workspace_root().join("awa-python/tests/patch_rehearsal_worker.py"))
        .env("DATABASE_URL", runtime_url)
        .env("REHEARSAL_SCHEMA", SCHEMA)
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
    let reader = tokio::spawn(async move {
        let mut ready_tx = Some(ready_tx);
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.starts_with("READY") {
                assert!(
                    line.contains("version=0.6.9"),
                    "the released worker must be the 0.6.9 wheel: {line}"
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

/// The grants a working 0.6.9 deployment gives its runtime role: the
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

/// Relation locks stronger than `RowExclusiveLock` held by `role` on `awa` or
/// queue-storage relations.
async fn strong_locks_held_by(pool: &PgPool, role: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT format('%s.%s %s', n.nspname, c.relname, l.mode) \
         FROM pg_locks AS l \
         JOIN pg_stat_activity AS a ON a.pid = l.pid \
         JOIN pg_class AS c ON c.oid = l.relation \
         JOIN pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE a.usename = $1 AND l.locktype = 'relation' \
           AND n.nspname IN ('awa', $2) \
           AND l.mode IN ('ShareUpdateExclusiveLock', 'ShareLock', 'ShareRowExclusiveLock', \
                          'ExclusiveLock', 'AccessExclusiveLock')",
    )
    .bind(role)
    .bind(SCHEMA)
    .fetch_all(pool)
    .await
    .unwrap()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release gate: needs AWA_RELEASED_CLI and AWA_RELEASED_PYTHON (0.6.9)"]
async fn released_0_6_9_patch_upgrade_and_rollback() {
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
        "awa 0.6.9",
        "AWA_RELEASED_CLI must be 0.6.9"
    );

    let fixture = Fixture::create().await;
    let log = EventLog::default();
    let mut killed_sources = HashSet::new();

    // ── 1. 0.6.9 install and traffic ───────────────────────────────────
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
    let before = lane_patch_applied(&fixture.admin).await;
    assert!(!before.applied, "a 0.6.9 install lacks the lane patch");
    assert!(before
        .needs_repair
        .contains(&format!("lane helpers in schema {SCHEMA}")));

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
    // Lock audit: the exported patch, in one owner transaction, holds no
    // lock above RowExclusive on any awa or queue-storage relation.
    let patch_sql = cli_ok(
        &candidate_cli,
        &fixture.owner_url,
        &["migrate", "--sql", "--pending", "--no-transaction"],
    )
    .await;
    assert!(patch_sql.contains("R__prepared_lane_sequences"));
    assert!(
        !patch_sql.contains("-- Migration V"),
        "a v40 database needs only the patch"
    );
    {
        let owner = PgPoolOptions::new()
            .max_connections(1)
            .connect(&fixture.owner_url)
            .await
            .unwrap();
        let mut tx = owner.begin().await.unwrap();
        sqlx::raw_sql(&patch_sql).execute(&mut *tx).await.unwrap();
        let locks: Vec<String> = sqlx::query_scalar(
            "SELECT format('%s.%s %s', n.nspname, c.relname, l.mode) \
             FROM pg_locks AS l \
             JOIN pg_class AS c ON c.oid = l.relation \
             JOIN pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation' \
               AND n.nspname IN ('awa', $1)",
        )
        .bind(SCHEMA)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        owner.close().await;
        assert_eq!(
            locks,
            Vec::<String>::new(),
            "the patch must not lock awa or queue-storage relations"
        );
    }

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
    let sampler_pool = fixture.admin.clone();
    let owner_role = fixture.owner.clone();
    let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let sampler_flag = sampling.clone();
    let sampler = tokio::spawn(async move {
        let mut seen = Vec::new();
        while sampler_flag.load(std::sync::atomic::Ordering::Relaxed) {
            seen.extend(strong_locks_held_by(&sampler_pool, &owner_role).await);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        seen
    });
    let completes_before_migrate = log.count(EventKind::Complete);
    let migrate_started = Instant::now();
    let migrate_out = cli_ok(&candidate_cli, &fixture.owner_url, &["migrate"]).await;
    let migrate_elapsed = migrate_started.elapsed();
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
    assert_eq!(
        sampled_locks,
        Vec::<String>::new(),
        "awa migrate took strong relation locks"
    );
    for expected in [
        "Schema: v40 (unchanged)".to_string(),
        "Schema patch prepared_lane_sequences: applied".to_string(),
        "lane helpers in schema awa".to_string(),
        format!("lane helpers in schema {SCHEMA}"),
        format!("Storage routing: unchanged (state active, engine queue_storage, schema {SCHEMA}"),
    ] {
        assert!(
            migrate_out.contains(&expected),
            "missing {expected:?} in:\n{migrate_out}"
        );
    }
    assert!(lane_patch_applied(&fixture.admin).await.applied);
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
    wait_for_drain(&log, Duration::from_secs(120)).await;

    // ── 4. roll back to released binaries ──────────────────────────────
    candidate_b.shutdown(Duration::from_secs(5)).await;
    let rollback_migrate = cli_ok(&released_cli, &fixture.owner_url, &["migrate"]).await;
    assert!(rollback_migrate.contains("Migrations applied successfully"));
    assert!(
        lane_patch_applied(&fixture.admin).await.applied,
        "the released binaries must leave the patch installed"
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
    wait_for_drain(&log, Duration::from_secs(120)).await;
    // A 0.6.9 admin process reads the patched schema as the runtime role.
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
    let worker_e = spawn_released_worker(&fixture.runtime_url, &log, &queues).await;
    let worker_f = spawn_released_worker(&fixture.runtime_url, &log, &queues).await;
    for (queue, seq_start) in [
        (MAIN, 9_000),
        (PROVISIONED, 10_000),
        (NEW_AFTER_ROLLBACK, 11_000),
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
    let refused = released_produce(
        &fixture.runtime_url,
        &log,
        ProduceSpec {
            queue: UNPROVISIONED,
            seq_start: 12_000,
            count: 1,
            modes: "ok",
            delay_s: 0.0,
            pause_ms: 0,
        },
    )
    .await
    .expect_err("an unprovisioned queue needs provisioning once CREATE is revoked");
    assert!(refused.contains("has not been provisioned"), "{refused}");
    wait_for_drain(&log, Duration::from_secs(120)).await;
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
            assert!(
                retries.contains(job_id),
                "fail_once job {job_id} never retried"
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
    // Jobs scheduled under 0.6.9 ran once due, after the patch.
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
        "cell": "released_0_6_9_patch_upgrade_and_rollback",
        "result": "pass",
        "candidate_version": env!("CARGO_PKG_VERSION"),
        "released_version": "0.6.9",
        "postgres": server_version,
        "schema": SCHEMA,
        "jobs_enqueued": enqueued.len(),
        "jobs_completed": completions.len(),
        "jobs_retried": retries.len(),
        "jobs_per_queue": per_queue,
        "workers_killed": killed_sources.len(),
        "migrate_ms": migrate_elapsed.as_millis() as u64,
        "migrate_output": migrate_out,
        "strong_locks_sampled": sampled_locks.len(),
        "enqueued_during_migrate": enqueued_during,
        "completed_during_migrate": completed_during,
        "in_flight_at_migrate": in_flight_count,
    });
    eprintln!("[patch-rehearsal] {report}");
    if let Some(dir) = std::env::var_os("AWA_REHEARSAL_ARTIFACT_DIR") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("patch-upgrade-0.6.9.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }
    fixture.drop().await;
}
