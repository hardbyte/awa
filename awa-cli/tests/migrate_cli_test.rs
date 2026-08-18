//! Integration tests for `awa migrate`: the applied run is atomic and
//! idempotent, and the SQL it emits for external tooling is too.
//!
//! The library-level guarantees live in `awa/tests/migration_test.rs`. These
//! tests exercise the *compiled binary*, which is what an operator actually
//! runs, against a dedicated database (`awa_migrate_cli_test`) so they never
//! contend with the shared `awa_test` schema. A process-local mutex plus an
//! advisory lock keeps the tests in this file from racing each other.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use assert_cmd::Command;
use awa_model::migrations;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use tokio::sync::Mutex;

const TEST_DB_NAME: &str = "awa_migrate_cli_test";
const MIGRATE_CLI_LOCK_KEY: i64 = 0x6177616d67636c69; // "awamgcli"

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn test_mutex() -> &'static Mutex<()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

struct TestGuard {
    _local: tokio::sync::MutexGuard<'static, ()>,
    _conn: PgConnection,
}

fn base_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn replace_database_name(url: &str, db_name: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let (prefix, _old_db) = base
        .rsplit_once('/')
        .expect("DATABASE_URL must include a database name");
    let mut out = format!("{prefix}/{db_name}");
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    out
}

fn test_database_url() -> String {
    replace_database_name(&base_database_url(), TEST_DB_NAME)
}

fn admin_database_url() -> String {
    replace_database_name(&base_database_url(), "postgres")
}

async fn ensure_test_database() {
    let mut admin = PgConnection::connect(&admin_database_url())
        .await
        .expect("connect admin db");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(TEST_DB_NAME)
            .fetch_one(&mut admin)
            .await
            .expect("check db existence");
    if !exists {
        // CREATE DATABASE takes no bind parameters. Tolerate the duplicate
        // race under per-test processes (cargo-nextest CI shards).
        if let Err(err) = sqlx::raw_sql(&format!("CREATE DATABASE {TEST_DB_NAME}"))
            .execute(&mut admin)
            .await
        {
            let duplicate = matches!(
                &err,
                sqlx::Error::Database(db) if db.code().as_deref() == Some("42P04")
            );
            assert!(duplicate, "create migrate cli test db: {err}");
        }
    }
}

async fn acquire_guard() -> TestGuard {
    let local = test_mutex().lock().await;
    ensure_test_database().await;
    let mut conn = PgConnection::connect(&test_database_url())
        .await
        .expect("lock conn");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATE_CLI_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("acquire advisory lock");
    TestGuard {
        _local: local,
        _conn: conn,
    }
}

async fn pool() -> PgPool {
    ensure_test_database().await;
    let opts = PgConnectOptions::from_str(&test_database_url()).expect("parse test db url");
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(opts)
        .await
        .expect("connect test db")
}

async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(pool)
        .await
        .expect("drop awa schema");
}

/// Count every relation the migrations own, so a partially-applied schema is
/// detectable even when the `awa` namespace itself survives.
async fn awa_relation_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'awa'",
    )
    .fetch_one(pool)
    .await
    .expect("count awa relations")
}

/// Fail any migration run the moment it creates `awa.runtime_instances` (the
/// first object of v002), so v001 has fully applied when the abort fires.
///
/// An event trigger injects the failure without touching the migration SQL, so
/// the abort is a genuine mid-transaction DDL error.
async fn arm_migration_abort(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION public.awa_migrate_cli_abort() \
           RETURNS event_trigger LANGUAGE plpgsql AS $fn$ \
         BEGIN \
           IF EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger_ddl_commands() \
                      WHERE object_identity = 'awa.runtime_instances') THEN \
             RAISE EXCEPTION 'injected migration failure'; \
           END IF; \
         END $fn$; \
         DROP EVENT TRIGGER IF EXISTS awa_migrate_cli_abort; \
         CREATE EVENT TRIGGER awa_migrate_cli_abort ON ddl_command_end \
           EXECUTE FUNCTION public.awa_migrate_cli_abort();",
    )
    .execute(pool)
    .await
    .expect("arming the abort event trigger requires superuser");
}

async fn disarm_migration_abort(pool: &PgPool) {
    sqlx::raw_sql(
        "DROP EVENT TRIGGER IF EXISTS awa_migrate_cli_abort; \
         DROP FUNCTION IF EXISTS public.awa_migrate_cli_abort();",
    )
    .execute(pool)
    .await
    .expect("disarm");
}

fn run_cli(args: &[&str]) -> Command {
    let mut command = Command::cargo_bin("awa").expect("awa binary should build");
    command
        .env("DATABASE_URL", test_database_url())
        .env("RUST_LOG", "warn")
        .args(args);
    command
}

// ── Applying ─────────────────────────────────────────────────────

/// Running the binary repeatedly converges: the version stays at
/// CURRENT_VERSION and no migration is recorded twice.
#[tokio::test]
async fn migrate_is_idempotent_across_repeated_runs() {
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    for run in 1..=3 {
        run_cli(&["migrate"])
            .assert()
            .try_success()
            .unwrap_or_else(|err| panic!("migrate run {run} failed: {err}"));
    }

    let version: i32 = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(&pool)
        .await
        .expect("read schema version");
    assert_eq!(version, migrations::CURRENT_VERSION);

    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.schema_version")
        .fetch_one(&pool)
        .await
        .expect("count recorded versions");
    assert_eq!(
        recorded,
        migrations::migration_sql().len() as i64,
        "repeated `awa migrate` runs must record each migration exactly once"
    );

    pool.close().await;
}

/// A failure part-way through `awa migrate` must leave nothing behind: the
/// command exits non-zero and the database is exactly as it was.
#[tokio::test]
async fn migrate_rolls_back_completely_when_a_step_fails() {
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    arm_migration_abort(&pool).await;
    let output = run_cli(&["migrate"]).output().expect("run awa migrate");
    disarm_migration_abort(&pool).await;

    assert!(
        !output.status.success(),
        "migrate should exit non-zero when a step fails"
    );
    assert_eq!(
        awa_relation_count(&pool).await,
        0,
        "a failed `awa migrate` must leave no partially-created relations"
    );

    // The database is still usable — a retry succeeds and reaches the top.
    run_cli(&["migrate"]).assert().success();
    let version: i32 = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(&pool)
        .await
        .expect("read schema version");
    assert_eq!(version, migrations::CURRENT_VERSION);

    pool.close().await;
}

/// `--from` / `--to` / `--version` select a range to *render*. On the apply
/// path they used to be accepted and silently ignored, so `--to 20` migrated
/// all the way to CURRENT_VERSION. Refuse instead.
#[tokio::test]
async fn migrate_refuses_range_selectors_on_the_apply_path() {
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    for selector in [
        vec!["migrate", "--to", "20"],
        vec!["migrate", "--version", "3"],
    ] {
        let output = run_cli(&selector).output().expect("run awa migrate");
        assert!(
            !output.status.success(),
            "{selector:?} should be refused on the apply path"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("select a range to render"),
            "{selector:?} should explain the refusal, got: {stderr}"
        );
        assert_eq!(
            awa_relation_count(&pool).await,
            0,
            "{selector:?} must not have applied anything"
        );
    }

    pool.close().await;
}

// ── Emitted SQL ──────────────────────────────────────────────────

/// The SQL handed to an operator is wrapped in one transaction that takes the
/// same advisory lock the built-in runner takes.
#[test]
fn sql_output_is_transaction_wrapped_by_default() {
    let output = Command::cargo_bin("awa")
        .expect("awa binary")
        .args(["migrate", "--sql"])
        .output()
        .expect("render sql");
    assert!(output.status.success());
    let rendered = String::from_utf8(output.stdout).expect("utf-8 sql");

    assert!(
        rendered.contains("\nBEGIN;\n"),
        "rendered SQL should open a transaction"
    );
    assert!(
        rendered.contains(&format!(
            "SELECT pg_advisory_xact_lock({});",
            migrations::MIGRATION_LOCK_KEY
        )),
        "rendered SQL should take the runner's advisory lock"
    );
    assert!(
        rendered.trim_end().ends_with("COMMIT;"),
        "rendered SQL should close the transaction"
    );
}

/// `--no-transaction` is the escape hatch for runners that open their own
/// transaction per migration.
#[test]
fn no_transaction_omits_the_wrapper() {
    let output = Command::cargo_bin("awa")
        .expect("awa binary")
        .args(["migrate", "--sql", "--no-transaction"])
        .output()
        .expect("render sql");
    assert!(output.status.success());
    let rendered = String::from_utf8(output.stdout).expect("utf-8 sql");

    assert!(!rendered.contains("\nBEGIN;\n"));
    // Migration bodies legitimately call `pg_advisory_xact_lock` themselves
    // (v006's admin-metadata serialization), so assert on the wrapper's own
    // statement rather than the bare function name.
    assert!(!rendered.contains(&format!(
        "SELECT pg_advisory_xact_lock({});",
        migrations::MIGRATION_LOCK_KEY
    )));
    assert!(!rendered.trim_end().ends_with("COMMIT;"));
}

/// The rendered SQL is a complete, correct install on its own — and applying
/// it twice is a no-op, so an external runner can safely retry.
#[tokio::test]
async fn rendered_sql_installs_the_current_schema_and_replays_cleanly() {
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    let rendered = String::from_utf8(
        Command::cargo_bin("awa")
            .expect("awa binary")
            .args(["migrate", "--sql"])
            .output()
            .expect("render sql")
            .stdout,
    )
    .expect("utf-8 sql");

    // Applied on a bare connection in autocommit — the same way `psql` runs it.
    let mut conn = PgConnection::connect(&test_database_url())
        .await
        .expect("connect");
    sqlx::raw_sql(&rendered)
        .execute(&mut conn)
        .await
        .expect("rendered SQL should install the schema");

    let version: i32 = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(&mut conn)
        .await
        .expect("read schema version");
    assert_eq!(version, migrations::CURRENT_VERSION);

    sqlx::raw_sql(&rendered)
        .execute(&mut conn)
        .await
        .expect("rendered SQL should be safe to re-apply");
    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.schema_version")
        .fetch_one(&mut conn)
        .await
        .expect("count recorded versions");
    assert_eq!(recorded, migrations::migration_sql().len() as i64);

    conn.close().await.expect("close");
    pool.close().await;
}

/// `--extract-to` must write the *whole* set or nothing usable is produced.
///
/// It used to build the path by string-substituting the description, so v017 —
/// whose description contains `/` — resolved to a nested directory that does
/// not exist. The command aborted there, leaving a partial 15-file extraction
/// that looked plausible but silently omitted two thirds of the schema.
#[test]
fn extract_to_writes_every_migration() {
    let dir = std::env::temp_dir().join(format!("awa-extract-all-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    Command::cargo_bin("awa")
        .expect("awa binary")
        .args(["migrate", "--extract-to", dir.to_str().expect("utf-8 path")])
        .assert()
        .success();

    let written: Vec<String> = std::fs::read_dir(&dir)
        .expect("read extracted dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert_eq!(
        written.len(),
        migrations::migration_sql().len(),
        "every migration should be extracted, got {written:?}"
    );
    for (version, _, sql) in migrations::migration_sql() {
        let prefix = format!("V{version}__");
        let name = written
            .iter()
            .find(|name| name.starts_with(&prefix))
            .unwrap_or_else(|| panic!("no file extracted for v{version}"));
        assert!(
            !name.contains('/') && !name.contains('\\'),
            "extracted filename must be a single path component: {name}"
        );
        let contents = std::fs::read_to_string(dir.join(name)).expect("read extracted file");
        assert_eq!(contents, sql, "v{version} extracted with different SQL");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// The wrapper is what makes `awa migrate --sql` atomic end-to-end.
///
/// The comparison is against the artifact an external runner consumes: the
/// per-version files from `--extract-to`, applied one at a time. Each file
/// then commits on its own, so a failure in v002 leaves v001 behind — which
/// is exactly why the runner must own the transaction boundary, and why the
/// default `--sql` output carries one.
#[tokio::test]
async fn rendered_sql_is_atomic_only_with_the_transaction_wrapper() {
    let _guard = acquire_guard().await;
    let pool = pool().await;

    let dir = std::env::temp_dir().join(format!("awa-migrate-cli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    run_cli(&["migrate", "--extract-to", dir.to_str().expect("utf-8 path")])
        .assert()
        .success();

    // `V<version>__<description>.sql` — order by the parsed version, not by
    // the lexicographic filename (V10 sorts before V2).
    let mut files: Vec<(i32, std::path::PathBuf)> = std::fs::read_dir(&dir)
        .expect("read extracted dir")
        .map(|entry| entry.expect("dir entry").path())
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?.to_string();
            let version = name.strip_prefix('V')?.split("__").next()?.parse().ok()?;
            Some((version, path))
        })
        .collect();
    files.sort();
    assert_eq!(
        files.len(),
        migrations::migration_sql().len(),
        "every migration should be extracted"
    );

    async fn connect() -> PgConnection {
        PgConnection::connect(&test_database_url())
            .await
            .expect("connect")
    }

    reset_schema(&pool).await;
    arm_migration_abort(&pool).await;

    // An external runner applying file by file, with no wrapping transaction.
    let mut conn = connect().await;
    for (_, path) in &files {
        let sql = std::fs::read_to_string(path).expect("read extracted sql");
        if sqlx::raw_sql(&sql).execute(&mut conn).await.is_err() {
            break;
        }
    }
    conn.close().await.expect("close");
    let leaked = awa_relation_count(&pool).await;

    // The same failure against the default, transaction-wrapped `--sql`.
    reset_schema(&pool).await;
    let wrapped = String::from_utf8(
        run_cli(&["migrate", "--sql"])
            .output()
            .expect("render sql")
            .stdout,
    )
    .expect("utf-8 sql");
    let mut conn = connect().await;
    let _ = sqlx::raw_sql(&wrapped).execute(&mut conn).await;
    conn.close().await.expect("close");
    let wrapped_leftovers = awa_relation_count(&pool).await;

    disarm_migration_abort(&pool).await;
    reset_schema(&pool).await;
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        leaked > 0,
        "sanity check: per-file application should half-apply, otherwise this \
         test is not exercising the wrapper"
    );
    assert_eq!(
        wrapped_leftovers, 0,
        "transaction-wrapped SQL must roll back completely on a mid-stream failure"
    );

    pool.close().await;
}
