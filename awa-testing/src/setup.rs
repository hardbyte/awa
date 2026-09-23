//! Common test setup utilities for Awa integration tests.

use awa_model::audited_sql;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgPool};
use std::collections::HashMap;
use std::time::Duration;

/// Default database URL for test runs.
pub fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

/// Database URL with a custom application_name parameter appended.
pub fn database_url_with_app_name(app_name: &str) -> String {
    let mut url = database_url();
    let sep = if url.contains('?') { '&' } else { '?' };
    url.push(sep);
    url.push_str("application_name=");
    url.push_str(app_name);
    url
}

/// Create a connection pool.
pub async fn pool(max_connections: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

/// Create a connection pool with a custom database URL.
pub async fn pool_with_url(url: &str, max_connections: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await
        .expect("Failed to connect to database")
}

/// Create a pool, run migrations, and return it.
pub async fn setup(max_connections: u32) -> PgPool {
    let pool = pool(max_connections).await;
    awa_model::migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    reset_runtime_backend(&pool).await;
    pool
}

/// Initialise the active storage engine to the one selected by the
/// `AWA_TEST_ENGINE` env var: `canonical` (default) or `queue_storage`. awa's
/// caller-facing contract is engine-invariant, so running the same suite under
/// both values exercises both backends. Tests that pin a specific engine call
/// [`reset_to_canonical`] / [`activate_queue_storage`] directly.
pub async fn reset_runtime_backend(pool: &PgPool) {
    match std::env::var("AWA_TEST_ENGINE").as_deref() {
        Ok("queue_storage") => activate_queue_storage(pool).await,
        _ => reset_to_canonical(pool).await,
    }
}

/// Force the canonical engine and clear any queue-storage runtime registration.
pub async fn reset_to_canonical(pool: &PgPool) {
    let mut tx = pool
        .begin()
        .await
        .expect("Failed to start runtime backend reset transaction");

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET current_engine = 'canonical',
            prepared_engine = NULL,
            state = 'canonical',
            transition_epoch = transition_epoch + 1,
            details = '{}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("Failed to reset storage transition state for test setup");
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset active runtime backend for test setup");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset runtime instances for test setup");
    tx.commit()
        .await
        .expect("Failed to commit runtime backend reset transaction");
}

/// Activate the queue-storage engine against the substrate installed in the
/// `awa` schema (mirrors a fresh-install auto-finalize).
pub async fn activate_queue_storage(pool: &PgPool) {
    let mut tx = pool
        .begin()
        .await
        .expect("Failed to start queue-storage activation transaction");

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET state = 'active',
            current_engine = 'queue_storage',
            prepared_engine = NULL,
            details = jsonb_build_object('schema', 'awa'),
            transition_epoch = transition_epoch + 1,
            updated_at = now(),
            finalized_at = now()
        WHERE singleton
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("Failed to activate queue-storage transition state for test setup");
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
        VALUES ('queue_storage', 'awa', now())
        ON CONFLICT (backend) DO UPDATE
        SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("Failed to register queue-storage backend for test setup");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset runtime instances for test setup");
    tx.commit()
        .await
        .expect("Failed to commit queue-storage activation transaction");
}

/// The storage engine the suite is running against, selected by `AWA_TEST_ENGINE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestEngine {
    Canonical,
    QueueStorage,
}

/// The engine selected for this test run (`canonical` unless `AWA_TEST_ENGINE=queue_storage`).
pub fn test_engine() -> TestEngine {
    match std::env::var("AWA_TEST_ENGINE").as_deref() {
        Ok("queue_storage") => TestEngine::QueueStorage,
        _ => TestEngine::Canonical,
    }
}

/// Guard for tests whose assertions are specific to the canonical engine —
/// e.g. admin-metadata caches maintained by canonical row triggers, or the
/// canonical running-cancel notification. Returns `true` (with a skip notice)
/// when the run is under another engine, so the test can early-return:
///
/// ```ignore
/// if awa_testing::setup::skip_unless_canonical("my_test") { return; }
/// ```
pub fn skip_unless_canonical(test: &str) -> bool {
    if test_engine() != TestEngine::Canonical {
        eprintln!(
            "[skip] {test}: canonical-only assertions; running under AWA_TEST_ENGINE=queue_storage"
        );
        true
    } else {
        false
    }
}

/// Delete all jobs, queue metadata, and admin caches for a specific queue.
///
/// Explicitly deletes the `queue_state_counts` row to prevent accumulated
/// cache drift from affecting assertions. The DELETE trigger normally handles
/// this, but concurrent test runs against a shared DB can cause small delta
/// errors that compound over time.
pub async fn clean_queue(pool: &PgPool, queue: &str) {
    match awa_model::queue_storage::QueueStorage::active_schema(pool)
        .await
        .expect("active_schema lookup for clean_queue")
    {
        Some(schema) => clean_queue_substrate(pool, &schema, queue).await,
        None => {
            sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
                .bind(queue)
                .execute(pool)
                .await
                .expect("Failed to clean queue jobs");
        }
    }
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
    sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue state counts");
}

/// Drain every job-bearing plane of the queue-storage substrate for one queue
/// and release its unique claims, so a queue name can be reused across tests
/// and re-runs against the same database. The canonical `awa.jobs` view only
/// surfaces a queue's head under queue storage, so a single view `DELETE`
/// leaves the rest of the lane (and its `job_unique_claims`) behind.
async fn clean_queue_substrate(pool: &PgPool, schema: &str, queue: &str) {
    // Release unique claims first, while the rows that carry the job ids exist.
    sqlx::query(audited_sql(format!(
        "DELETE FROM awa.job_unique_claims WHERE job_id IN (
             SELECT job_id FROM {schema}.ready_entries WHERE queue = $1
             UNION SELECT job_id FROM {schema}.deferred_jobs WHERE queue = $1
             UNION SELECT job_id FROM {schema}.leases WHERE queue = $1
             UNION SELECT job_id FROM {schema}.done_entries WHERE queue = $1
             UNION SELECT job_id FROM {schema}.dlq_entries WHERE queue = $1)"
    )))
    .bind(queue)
    .execute(pool)
    .await
    .expect("Failed to release queue unique claims");

    for plane in [
        "ready_entries",
        "deferred_jobs",
        "leases",
        "done_entries",
        "dlq_entries",
    ] {
        sqlx::query(audited_sql(format!(
            "DELETE FROM {schema}.{plane} WHERE queue = $1"
        )))
        .bind(queue)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("Failed to clean {plane} for queue {queue}: {err}"));
    }
}

/// Query job state counts for a queue, returning a map of state -> count.
pub async fn queue_state_counts(pool: &PgPool, queue: &str) -> HashMap<String, i64> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT state::text, count(*)::bigint
        FROM awa.jobs
        WHERE queue = $1
        GROUP BY state
        "#,
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("Failed to query state counts");

    rows.into_iter().collect()
}

/// Extract a count for a given state from a state-counts map.
pub fn state_count(counts: &HashMap<String, i64>, state: &str) -> i64 {
    counts.get(state).copied().unwrap_or(0)
}

/// Poll queue state counts until a predicate is satisfied, or panic on timeout.
pub async fn wait_for_counts(
    pool: &PgPool,
    queue: &str,
    predicate: impl Fn(&HashMap<String, i64>) -> bool,
    timeout: Duration,
) -> HashMap<String, i64> {
    let start = std::time::Instant::now();
    loop {
        let counts = queue_state_counts(pool, queue).await;
        if predicate(&counts) {
            return counts;
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for queue {queue} counts; last counts: {counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A throwaway database holding the fully migrated `awa` schema.
///
/// The first call in a process builds a template database by running the
/// migration chain once; every `TestDatabase` is then a `CREATE DATABASE ...
/// TEMPLATE` clone of it, which takes on the order of 100 ms instead of a
/// full replay. Each instance owns a distinct database, so tests holding one
/// can run in parallel without coordinating schema resets, and the database
/// is dropped when the value goes out of scope.
///
/// The template name carries a fingerprint of the migration SQL, so a changed
/// migration selects a new template. Other templates and abandoned clones
/// are retained; each instance only drops its own clone.
pub struct TestDatabase {
    name: String,
    admin_url: String,
    pool: PgPool,
}

const TEMPLATE_PREFIX: &str = "awa_tpl_";
const CLONE_PREFIX: &str = "awa_t_";
const TEMPLATE_BUILD_LOCK_KEY: i64 = 0x6177615f7470;

static CANONICAL_TEMPLATE: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static CLONE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl TestDatabase {
    /// A fresh database at the current schema version on canonical storage.
    pub async fn canonical() -> Self {
        Self::with_max_connections(5).await
    }

    /// A fresh database at the current schema version with the default
    /// queue-storage substrate installed and active.
    pub async fn queue_storage() -> Self {
        let db = Self::canonical().await;
        awa_model::queue_storage::QueueStorage::new(
            awa_model::queue_storage::QueueStorageConfig::default(),
        )
        .expect("default queue storage config should build")
        .install(&db.pool)
        .await
        .expect("installing queue storage on the test database should succeed");
        db
    }

    /// Like [`Self::canonical`], with a pool sized for the test's needs.
    pub async fn with_max_connections(max_connections: u32) -> Self {
        let template = CANONICAL_TEMPLATE
            .get_or_init(ensure_canonical_template)
            .await;
        let admin_url = admin_database_url();
        let mut admin = connect(&admin_url).await;
        let name = format!(
            "{CLONE_PREFIX}{}_{}_{}",
            unix_time_secs(),
            std::process::id(),
            CLONE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        sqlx::raw_sql(audited_sql(format!(
            "CREATE DATABASE {name} TEMPLATE {template}"
        )))
        .execute(&mut admin)
        .await
        .expect("cloning the test template database should succeed");
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&replace_database_name(&admin_url, &name))
            .await
            .expect("connecting to the cloned test database should succeed");
        Self {
            name,
            admin_url,
            pool,
        }
    }

    /// The pool connected to this database.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The database name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A connection URL for this database.
    pub fn url(&self) -> String {
        replace_database_name(&self.admin_url, &self.name)
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let admin_url = self.admin_url.clone();
        let name = self.name.clone();
        // Dropping needs a connection and there is no async Drop, so run it on
        // a throwaway runtime; joining keeps cleanup deterministic under panics.
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test database cleanup runtime");
            runtime.block_on(async {
                if let Ok(mut admin) = sqlx::postgres::PgConnection::connect(&admin_url).await {
                    let _ = sqlx::raw_sql(audited_sql(format!(
                        "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
                    )))
                    .execute(&mut admin)
                    .await;
                }
            });
        });
        let _ = cleanup.join();
    }
}

async fn ensure_canonical_template() -> String {
    ensure_template(format!("{TEMPLATE_PREFIX}{:016x}", schema_fingerprint())).await
}

async fn ensure_template(name: String) -> String {
    let admin_url = admin_database_url();
    let mut admin = connect(&admin_url).await;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(TEMPLATE_BUILD_LOCK_KEY)
        .execute(&mut admin)
        .await
        .expect("template build lock");
    if !database_exists(&mut admin, &name).await {
        let build_name = format!("{name}_build_{}", std::process::id());
        sqlx::raw_sql(audited_sql(format!(
            "DROP DATABASE IF EXISTS {build_name} WITH (FORCE)"
        )))
        .execute(&mut admin)
        .await
        .expect("clearing a stale template build database should succeed");
        sqlx::raw_sql(audited_sql(format!("CREATE DATABASE {build_name}")))
            .execute(&mut admin)
            .await
            .expect("creating the template build database should succeed");
        let pool = pool_with_url(&replace_database_name(&admin_url, &build_name), 2).await;
        awa_model::migrations::run(&pool)
            .await
            .expect("migrating the template database should succeed");
        pool.close().await;
        sqlx::raw_sql(audited_sql(format!(
            "ALTER DATABASE {build_name} RENAME TO {name}"
        )))
        .execute(&mut admin)
        .await
        .expect("publishing the template database should succeed");
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(TEMPLATE_BUILD_LOCK_KEY)
        .execute(&mut admin)
        .await
        .expect("template build unlock");
    name
}

fn schema_fingerprint() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    awa_model::migrations::CURRENT_VERSION.hash(&mut hasher);
    for (version, name, sql) in awa_model::migrations::migration_sql() {
        version.hash(&mut hasher);
        name.hash(&mut hasher);
        sql.hash(&mut hasher);
    }
    hasher.finish()
}

async fn database_exists(admin: &mut sqlx::postgres::PgConnection, name: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
        .bind(name)
        .fetch_one(admin)
        .await
        .expect("pg_database lookup")
}

async fn connect(url: &str) -> sqlx::postgres::PgConnection {
    sqlx::postgres::PgConnection::connect(url)
        .await
        .expect("Failed to connect to the admin database — is Postgres running?")
}

fn admin_database_url() -> String {
    replace_database_name(&database_url(), "postgres")
}

fn replace_database_name(url: &str, db_name: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let (prefix, _) = base
        .rsplit_once('/')
        .expect("DATABASE_URL must include a database name");
    match query {
        Some(query) => format!("{prefix}/{db_name}?{query}"),
        None => format!("{prefix}/{db_name}"),
    }
}

fn unix_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_guard(name: String) -> TestDatabase {
        let admin_url = admin_database_url();
        let pool = PgPoolOptions::new()
            .connect_lazy(&replace_database_name(&admin_url, &name))
            .expect("test database URL");
        TestDatabase {
            name,
            admin_url,
            pool,
        }
    }

    #[tokio::test]
    async fn building_a_template_preserves_other_templates_and_live_clones() {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let old_template = database_guard(format!("{TEMPLATE_PREFIX}{suffix}_old"));
        let active_clone = database_guard(format!("{CLONE_PREFIX}0_{suffix}"));
        let new_template = database_guard(format!("{TEMPLATE_PREFIX}{suffix}_new"));
        let next_clone = database_guard(format!("{CLONE_PREFIX}1_{suffix}"));
        let mut admin = connect(&admin_database_url()).await;
        sqlx::raw_sql(audited_sql(format!(
            "CREATE DATABASE {}",
            old_template.name()
        )))
        .execute(&mut admin)
        .await
        .unwrap();
        sqlx::raw_sql(audited_sql(format!(
            "CREATE DATABASE {} TEMPLATE {}",
            active_clone.name(),
            old_template.name()
        )))
        .execute(&mut admin)
        .await
        .unwrap();
        let mut active_connection = active_clone.pool().acquire().await.unwrap();

        ensure_template(new_template.name().to_owned()).await;

        let old_template_exists = database_exists(&mut admin, old_template.name()).await;
        let active_clone_exists = database_exists(&mut admin, active_clone.name()).await;
        assert!(
            old_template_exists && active_clone_exists,
            "template preserved: {old_template_exists}, live clone preserved: {active_clone_exists}"
        );
        let value: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&mut *active_connection)
            .await
            .unwrap();
        assert_eq!(value, 1);
        sqlx::raw_sql(audited_sql(format!(
            "CREATE DATABASE {} TEMPLATE {}",
            next_clone.name(),
            old_template.name()
        )))
        .execute(&mut admin)
        .await
        .unwrap();
    }
}
