//! Migration v062 (#847): the queue-ring generation floor.

use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Like `assert!`, but returns an error, so a test that created cluster-wide
/// roles still drops them before it reports the failure.
macro_rules! ensure {
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err(format!($($arg)+).into());
        }
    };
}

/// Like `assert_eq!`, but returns an error; see [`ensure!`].
macro_rules! ensure_eq {
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        let (left, right) = (&$left, &$right);
        if left != right {
            return Err(format!("{}: left {:?}, right {:?}", format!($($arg)+), left, right).into());
        }
    }};
}

const CUSTOM: &str = "floor_custom";
const COMPAT_FUNCTIONS: [&str; 2] = ["insert_job_compat", "insert_jobs_compat_batch"];

async fn compat_function(pool: &PgPool, name: &str) -> Result<(i64, String), sqlx::Error> {
    sqlx::query_as(
        "SELECT p.oid::bigint, pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'awa' AND p.proname = $1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
}

async fn compat_functions(pool: &PgPool) -> Result<Vec<(i64, String)>, sqlx::Error> {
    let mut functions = Vec::new();
    for name in COMPAT_FUNCTIONS {
        functions.push(compat_function(pool, name).await?);
    }
    Ok(functions)
}

async fn apply(pool: &PgPool, from: i32, to: i32) -> TestResult {
    for (_, _, sql) in migrations::migration_sql_range(from, to) {
        sqlx::raw_sql(audited_sql(sql)).execute(pool).await?;
    }
    Ok(())
}

/// The definitions of `set_sequence_next` and the enqueue-head trigger
/// function in `schema`.
async fn lane_sequence_helpers(pool: &PgPool, schema: &str) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1
           AND p.proname IN ('set_sequence_next', 'queue_enqueue_head_sequence_sync')
         ORDER BY p.proname",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
}

/// The floor's last value, or `None` before its first raise.
async fn floor(pool: &PgPool, schema: &str) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(audited_sql(format!(
        "SELECT pg_sequence_last_value('{schema}.queue_ring_generation_floor'::regclass)"
    )))
    .fetch_one(pool)
    .await
}

/// Removes the floor from `schema`, as before v062.
async fn drop_floor(pool: &PgPool, schema: &str) -> TestResult {
    sqlx::raw_sql(audited_sql(format!(
        "DROP FUNCTION {schema}.enqueue_ring_cursor();
         DROP SEQUENCE {schema}.queue_ring_generation_floor;"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Moves the queue ring of an empty schema to `generation` by appending
/// rotation-ledger rows.
async fn rotate_to(pool: &PgPool, schema: &str, generation: i64, slots: i64) -> TestResult {
    sqlx::query(audited_sql(format!(
        "INSERT INTO {schema}.queue_ring_rotations (generation, slot)
         SELECT g, g % $2 FROM generate_series(
             (SELECT max(generation) + 1 FROM {schema}.queue_ring_rotations), $1
         ) AS g"
    )))
    .bind(generation)
    .bind(slots)
    .execute(pool)
    .await?;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn v062_installs_the_floor_and_rewrites_the_compat_producers(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: CUSTOM.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?;
    store.install(&pool).await?;
    for schema in ["awa", CUSTOM] {
        assert_eq!(floor(&pool, schema).await?, Some(0), "{schema}: seeded");
    }

    let installed = compat_functions(&pool).await?;
    for (_, definition) in &installed {
        assert!(definition.contains("enqueue_ring_cursor()"));
    }
    let installed_lane_helpers = lane_sequence_helpers(&pool, CUSTOM).await?;

    // Back to v061: the released compat producers, and no floor.
    apply(&pool, 17, 18).await?;
    apply(&pool, 56, 57).await?;
    for schema in ["awa", CUSTOM] {
        drop_floor(&pool, schema).await?;
        // The lane-sequence helpers as released: their unlock handler does
        // not catch a cancel.
        for definition in lane_sequence_helpers(&pool, schema).await? {
            assert!(definition.contains("OR query_canceled"));
            sqlx::raw_sql(audited_sql(
                definition.replace("WHEN OTHERS OR query_canceled THEN", "WHEN OTHERS THEN"),
            ))
            .execute(&pool)
            .await?;
        }
    }
    let released = compat_functions(&pool).await?;
    for (_, definition) in &released {
        assert!(!definition.contains("enqueue_ring_cursor()"));
    }

    // A binary from this release on a schema before v062 reads the cursor as
    // before, through the compat producers and its own enqueue path alike.
    rotate_to(&pool, CUSTOM, 6, 4).await?;
    let fallback_store = QueueStorage::new(QueueStorageConfig {
        schema: CUSTOM.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?;
    fallback_store
        .enqueue_batch(&pool, "fallback", 2, 1)
        .await?;
    let written: (i32, i64) = sqlx::query_as(audited_sql(format!(
        "SELECT ready_slot, ready_generation FROM {CUSTOM}.ready_entries WHERE queue = 'fallback'"
    )))
    .fetch_one(&pool)
    .await?;
    assert_eq!(written, (2, 6));

    apply(&pool, 61, 62).await?;
    assert_eq!(
        lane_sequence_helpers(&pool, CUSTOM).await?,
        installed_lane_helpers,
        "v062 restores the cancel-safe lane-sequence helpers"
    );
    let migrated = compat_functions(&pool).await?;
    for ((released_oid, _), (migrated_oid, migrated_definition)) in released.iter().zip(&migrated) {
        assert_eq!(released_oid, migrated_oid, "replaced in place");
        assert!(migrated_definition.contains("enqueue_ring_cursor()"));
    }
    // A fresh install runs v018 and v057 and then v062, so the bodies match.
    assert_eq!(
        migrated.iter().map(|(_, d)| d).collect::<Vec<_>>(),
        installed.iter().map(|(_, d)| d).collect::<Vec<_>>(),
        "v062 rewrites the released producers to the fresh-install bodies"
    );
    assert_eq!(floor(&pool, "awa").await?, Some(0));
    assert_eq!(
        floor(&pool, CUSTOM).await?,
        Some(6),
        "seeded from the cursor"
    );

    for schema in ["awa", CUSTOM] {
        let (definer, owned_by_ring_owner, public_execute, sequence_owned): (
            bool,
            bool,
            bool,
            bool,
        ) = sqlx::query_as(audited_sql(format!(
            "SELECT p.prosecdef,
                        p.proowner = state.relowner,
                        has_function_privilege('public', p.oid, 'EXECUTE'),
                        seq.relowner = state.relowner
                 FROM pg_proc AS p,
                      pg_class AS state,
                      pg_class AS seq
                 WHERE p.oid = '{schema}.enqueue_ring_cursor()'::regprocedure
                   AND state.oid = '{schema}.queue_ring_state'::regclass
                   AND seq.oid = '{schema}.queue_ring_generation_floor'::regclass"
        )))
        .fetch_one(&pool)
        .await?;
        assert!(definer, "{schema}: SECURITY DEFINER");
        assert!(owned_by_ring_owner, "{schema}: owned by the ring's owner");
        assert!(public_execute, "{schema}: PUBLIC may execute it");
        assert!(sequence_owned, "{schema}: floor owned by the ring's owner");
    }

    apply(&pool, 61, 62).await?;
    assert_eq!(
        compat_functions(&pool).await?,
        migrated,
        "replaying v062 changes nothing"
    );
    assert_eq!(floor(&pool, CUSTOM).await?, Some(6));

    // A snapshot that still sees generation 6 gets the floor's generation and
    // its slot, and cannot lower the floor.
    let cursor_sql = format!("SELECT slot, generation FROM {CUSTOM}.enqueue_ring_cursor()");
    let mut stale = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *stale)
        .await?;
    sqlx::query("SELECT 1").execute(&mut *stale).await?;
    rotate_to(&pool, CUSTOM, 9, 4).await?;
    let fresh: (i32, i64) = sqlx::query_as(audited_sql(cursor_sql.clone()))
        .fetch_one(&pool)
        .await?;
    assert_eq!(fresh, (1, 9));
    let seen: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT generation FROM {CUSTOM}.ring_cursor('queue')"
    )))
    .fetch_one(&mut *stale)
    .await?;
    assert_eq!(seen, 6, "the snapshot predates the rotations");
    let stale_cursor: (i32, i64) = sqlx::query_as(audited_sql(cursor_sql))
        .fetch_one(&mut *stale)
        .await?;
    stale.rollback().await?;
    assert_eq!(stale_cursor, (1, 9));
    assert_eq!(floor(&pool, CUSTOM).await?, Some(9));
    Ok(())
}

/// A runtime role granted what it needed at v061, with no default privileges,
/// keeps enqueueing after v062 and raises the floor through
/// `enqueue_ring_cursor()`, without any privilege on the floor itself.
#[sqlx::test(migrations = false)]
async fn v062_needs_no_new_runtime_grants(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    drop_floor(&pool, "awa").await?;

    let runtime = format!("floor_runtime_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(audited_sql(format!(
        "CREATE ROLE {runtime} LOGIN PASSWORD 'floor_test';
         GRANT CONNECT ON DATABASE \"{database}\" TO {runtime};
         GRANT USAGE ON SCHEMA awa TO {runtime};
         GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA awa TO {runtime};
         GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO {runtime};
         GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {runtime};"
    )))
    .execute(&pool)
    .await?;
    let result = async {
        apply(&pool, 61, 62).await?;
        rotate_to(&pool, "awa", 3, 16).await?;

        let floor_privilege: bool = sqlx::query_scalar(
            "SELECT has_sequence_privilege($1, 'awa.queue_ring_generation_floor', 'SELECT')
                 OR has_sequence_privilege($1, 'awa.queue_ring_generation_floor', 'UPDATE')",
        )
        .bind(&runtime)
        .fetch_one(&pool)
        .await?;
        ensure!(!floor_privilege, "the runtime has no grant on the floor");

        let runtime_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                pool.connect_options()
                    .as_ref()
                    .clone()
                    .username(&runtime)
                    .password("floor_test"),
            )
            .await?;
        let store = QueueStorage::new(QueueStorageConfig::default())?;
        store
            .enqueue_batch(&runtime_pool, "runtime_queue", 2, 1)
            .await?;
        runtime_pool.close().await;

        let written: (i32, i64) = sqlx::query_as(
            "SELECT ready_slot, ready_generation FROM awa.ready_entries
             WHERE queue = 'runtime_queue'",
        )
        .fetch_one(&pool)
        .await?;
        ensure_eq!(written, (3, 3), "written into the floor's generation");
        ensure_eq!(floor(&pool, "awa").await?, Some(3), "the runtime raised it");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(audited_sql(format!(
        "DROP OWNED BY {runtime}; DROP ROLE {runtime};"
    )))
    .execute(&pool)
    .await?;
    result
}

/// A migration role that can neither hand `enqueue_ring_cursor()` to the
/// ring's owner nor read the ring itself leaves the function out, with a
/// warning, rather than install one that would fail every enqueue.
#[sqlx::test(migrations = false)]
async fn floor_helper_leaves_out_a_function_its_owner_could_not_run(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    drop_floor(&pool, "awa").await?;

    let migrator = format!("floor_migrator_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(audited_sql(format!(
        "CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'floor_test';
         GRANT CONNECT ON DATABASE \"{database}\" TO {migrator};
         GRANT USAGE, CREATE ON SCHEMA awa TO {migrator};
         GRANT EXECUTE ON FUNCTION awa.refresh_ring_generation_floor_helper(text) TO {migrator};"
    )))
    .execute(&pool)
    .await?;
    let result = async {
        let migrator_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                pool.connect_options()
                    .as_ref()
                    .clone()
                    .username(&migrator)
                    .password("floor_test"),
            )
            .await?;
        sqlx::query("SELECT awa.refresh_ring_generation_floor_helper('awa')")
            .execute(&migrator_pool)
            .await?;
        migrator_pool.close().await;

        let present: bool =
            sqlx::query_scalar("SELECT to_regprocedure('awa.enqueue_ring_cursor()') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        ensure!(!present, "the function is left out");

        let store = QueueStorage::new(QueueStorageConfig::default())?;
        store.enqueue_batch(&pool, "fallback_queue", 2, 1).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(audited_sql(format!(
        "DROP OWNED BY {migrator}; DROP ROLE {migrator};"
    )))
    .execute(&pool)
    .await?;
    result
}

/// A migration role that may SET ROLE to the ring's owner but does not
/// inherit its privileges (PostgreSQL 16+), and has no CREATE on the schema,
/// installs the floor as the owner and can replay it.
#[sqlx::test(migrations = false)]
async fn floor_helper_runs_as_the_owner_for_a_set_role_migrator(pool: PgPool) -> TestResult {
    let server_version: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await?;
    if server_version < 160000 {
        return Ok(());
    }
    migrations::run(&pool).await?;
    drop_floor(&pool, "awa").await?;

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("floor_owner_{suffix}");
    let migrator = format!("floor_set_migrator_{suffix}");
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    // The owner gets just what the floor needs: the ring tables, the cursor
    // reader and CREATE on the schema.
    sqlx::raw_sql(audited_sql(format!(
        "CREATE ROLE {owner} NOLOGIN NOSUPERUSER;
         CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'floor_test';
         GRANT {owner} TO {migrator} WITH INHERIT FALSE, SET TRUE;
         GRANT CONNECT ON DATABASE \"{database}\" TO {migrator};
         GRANT USAGE, CREATE ON SCHEMA awa TO {owner};
         GRANT USAGE ON SCHEMA awa TO {migrator};
         ALTER TABLE awa.queue_ring_state OWNER TO {owner};
         GRANT SELECT ON awa.queue_ring_rotations, awa.ring_cursor_authority TO {owner};
         GRANT EXECUTE ON FUNCTION awa.ring_cursor(text) TO {owner};
         GRANT EXECUTE ON FUNCTION awa.refresh_ring_generation_floor_helper(text) TO {migrator};"
    )))
    .execute(&pool)
    .await?;
    let result = async {
        let migrator_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                pool.connect_options()
                    .as_ref()
                    .clone()
                    .username(&migrator)
                    .password("floor_test"),
            )
            .await?;
        // One transaction, as awa migrate runs it: the role the helper sets
        // is transaction-local, so only a check inside the same transaction
        // shows the helper restored the caller's role.
        let mut tx = migrator_pool.begin().await?;
        for _ in 0..2 {
            sqlx::query("SELECT awa.refresh_ring_generation_floor_helper('awa')")
                .execute(&mut *tx)
                .await?;
        }
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        migrator_pool.close().await;
        ensure_eq!(role, migrator, "the helper restores the caller's role");

        let (function_owner, sequence_owner): (String, String) = sqlx::query_as(
            "SELECT pg_get_userbyid(p.proowner)::text, pg_get_userbyid(c.relowner)::text
             FROM pg_proc AS p, pg_class AS c
             WHERE p.oid = 'awa.enqueue_ring_cursor()'::regprocedure
               AND c.oid = 'awa.queue_ring_generation_floor'::regclass",
        )
        .fetch_one(&pool)
        .await?;
        ensure_eq!(
            function_owner,
            owner,
            "the function belongs to the ring's owner"
        );
        ensure_eq!(
            sequence_owner,
            owner,
            "the floor belongs to the ring's owner"
        );
        ensure_eq!(floor(&pool, "awa").await?, Some(0), "seeded");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(audited_sql(format!(
        "ALTER TABLE awa.queue_ring_state OWNER TO CURRENT_USER;
         REASSIGN OWNED BY {owner} TO CURRENT_USER;
         DROP OWNED BY {owner}; DROP OWNED BY {migrator};
         DROP ROLE {migrator}; DROP ROLE {owner};"
    )))
    .execute(&pool)
    .await?;
    result
}

/// Whether `pid` holds any advisory lock.
async fn holds_advisory_lock(pool: &PgPool, pid: i32) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks
         WHERE locktype = 'advisory' AND pid = $1 AND granted)",
    )
    .bind(pid)
    .fetch_one(pool)
    .await
}

/// Runs `statement` on a fresh connection while another holds the session
/// advisory lock `lock_key`, then releases the lock and cancels the waiting
/// statement in one statement, so it is granted the lock before it handles the
/// cancel: the error is raised with the lock held. Returns whether the
/// statement's session still holds an advisory lock afterwards, or `None` when
/// the cancel arrived only after the statement had finished, which proves
/// nothing and is worth another try.
async fn cancel_as_granted(
    pool: &PgPool,
    lock_key: i64,
    statement: &str,
) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    let mut gate = pool.acquire().await?;
    let mut victim = pool.acquire().await?;
    let victim_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *victim)
        .await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut *gate)
        .await?;
    let run = async {
        sqlx::query(audited_sql(statement.to_string()))
            .execute(&mut *victim)
            .await
    };
    let cancel = async {
        for _ in 0..500 {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE pid = $1 AND wait_event_type = 'Lock' AND wait_event = 'advisory')",
            )
            .bind(victim_pid)
            .fetch_one(pool)
            .await?;
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        sqlx::query("SELECT pg_advisory_unlock($1), pg_cancel_backend($2)")
            .bind(lock_key)
            .bind(victim_pid)
            .execute(&mut *gate)
            .await?;
        Ok::<(), sqlx::Error>(())
    };
    let (ran, cancelled) = tokio::join!(run, cancel);
    cancelled?;
    match ran {
        Ok(_) => Ok(None),
        Err(err) => {
            assert_eq!(
                err.as_database_error().and_then(|db| db.code()).as_deref(),
                Some("57014"),
                "{statement}: {err}"
            );
            Ok(Some(holds_advisory_lock(pool, victim_pid).await?))
        }
    }
}

/// Runs [`cancel_as_granted`] until the cancel lands while the statement runs,
/// calling `before` ahead of each try, and returns whether the lock was left
/// held.
async fn cancel_as_granted_until_cancelled<F, Fut>(
    pool: &PgPool,
    lock_key: i64,
    statement: &str,
    mut before: F,
) -> Result<bool, Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = TestResult>,
{
    for _ in 0..10 {
        before().await?;
        if let Some(held) = cancel_as_granted(pool, lock_key, statement).await? {
            return Ok(held);
        }
    }
    Err(format!("{statement}: no cancel landed while it ran in 10 tries").into())
}

/// A pooled connection keeps a session lock it leaks, so a raise of the floor
/// that is cancelled must leave the floor's lock free, whether the cancel lands
/// as the lock is granted or a statement timeout fires while it waits.
#[sqlx::test(migrations = false)]
async fn cancelled_floor_raise_leaves_no_session_lock(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let lock_key: i64 = sqlx::query_scalar(
        "SELECT hashtextextended('awa.queue_storage.ring_generation_floor:awa', 0)",
    )
    .fetch_one(&pool)
    .await?;

    // Each try needs a raise, so the ring moves one generation first.
    let generation = std::sync::atomic::AtomicI64::new(0);
    let held = cancel_as_granted_until_cancelled(
        &pool,
        lock_key,
        "SELECT FROM awa.enqueue_ring_cursor()",
        || {
            let next = generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            let pool = pool.clone();
            async move { rotate_to(&pool, "awa", next, 16).await }
        },
    )
    .await?;
    assert!(
        !held,
        "a cancel as the lock was granted kept the floor lock"
    );

    // A statement timeout that fires while the raise waits for the lock.
    let next = generation.load(std::sync::atomic::Ordering::Relaxed) + 1;
    rotate_to(&pool, "awa", next, 16).await?;
    let mut gate = pool.acquire().await?;
    let mut raiser = pool.acquire().await?;
    let raiser_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *raiser)
        .await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut *gate)
        .await?;
    sqlx::query("SET statement_timeout = '200ms'")
        .execute(&mut *raiser)
        .await?;
    let err = sqlx::query("SELECT FROM awa.enqueue_ring_cursor()")
        .execute(&mut *raiser)
        .await
        .expect_err("the statement timeout cancels the waiting raise");
    assert_eq!(
        err.as_database_error().and_then(|db| db.code()).as_deref(),
        Some("57014"),
        "{err}"
    );
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut *raiser)
        .await?;
    assert!(
        !holds_advisory_lock(&pool, raiser_pid).await?,
        "a timeout while waiting left a lock held"
    );
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *gate)
        .await?;

    let cursor: (i32, i64) =
        sqlx::query_as("SELECT slot, generation FROM awa.enqueue_ring_cursor()")
            .fetch_one(&mut *gate)
            .await?;
    assert_eq!(
        cursor,
        ((next % 16) as i32, next),
        "a later raise still succeeds"
    );
    Ok(())
}

/// The lane-sequence helpers take a session lock on the lane's sequence. A
/// cancel must not leave it held: a leaked lock on a claim sequence stalls
/// every claim cursor move on the lane, and one on an enqueue sequence stalls
/// every reservation, until the pooled connection closes.
#[sqlx::test(migrations = false)]
async fn cancelled_lane_sequence_moves_leave_no_session_lock(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let store = QueueStorage::new(QueueStorageConfig::default())?;
    store.enqueue_batch(&pool, "lock_q", 2, 1).await?;
    let lane_key = |table: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_as::<_, (String, i64)>(audited_sql(format!(
                "SELECT seq_name, hashtextextended(format('%I.%I', 'awa', seq_name), 0)
                 FROM awa.{table} WHERE queue = 'lock_q'"
            )))
            .fetch_one(&pool)
            .await
        }
    };

    let (claim_seq, claim_key) = lane_key("queue_claim_heads").await?;
    // Each try moves the cursor further, so a try that completes still
    // leaves the next one something to do.
    let target = std::sync::atomic::AtomicI64::new(5);
    let claim_sql = |next: i64| format!("SELECT awa.set_sequence_next('{claim_seq}', {next})");
    let mut held = None;
    for _ in 0..10 {
        let next = target.fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        if let Some(result) = cancel_as_granted(&pool, claim_key, &claim_sql(next)).await? {
            held = Some(result);
            break;
        }
    }
    assert_eq!(
        held,
        Some(false),
        "set_sequence_next kept the lane sequence lock, or no cancel landed in 10 tries"
    );

    let (_, enqueue_key) = lane_key("queue_enqueue_heads").await?;
    assert!(
        !cancel_as_granted_until_cancelled(
            &pool,
            enqueue_key,
            "UPDATE awa.queue_enqueue_heads SET next_seq = next_seq + 3 WHERE queue = 'lock_q'",
            || async { Ok(()) },
        )
        .await?,
        "the enqueue-head trigger kept the lane sequence lock"
    );

    // The lane still takes work.
    store.enqueue_batch(&pool, "lock_q", 2, 1).await?;
    Ok(())
}

/// The lane functions `awa.refresh_lane_sequence_helpers` replaces.
const LANE_FUNCTIONS: &str = "'ensure_lane_sequences', 'queue_enqueue_head_sequence_sync', \
     'queue_claim_head_sequence_sync', 'provision_lane_sequences', 'provision_lane', \
     'set_sequence_next'";

/// v062 refreshes the lane helpers through
/// `awa.refresh_lane_sequence_helpers_as_migrator`. A migration role that may
/// only SET ROLE to the owner of the schema and its lane functions refreshes
/// them as that owner, lending it EXECUTE on the refresh helper for the call,
/// and gets its own role back inside the same transaction.
#[sqlx::test(migrations = false)]
async fn lane_refresh_runs_as_the_owner_for_a_set_role_migrator(pool: PgPool) -> TestResult {
    let server_version: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await?;
    if server_version < 160000 {
        return Ok(());
    }
    migrations::run(&pool).await?;

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("lane_owner_{suffix}");
    let migrator = format!("lane_migrator_{suffix}");
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(audited_sql(format!(
        "CREATE ROLE {owner} NOLOGIN NOSUPERUSER;
         CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'floor_test';
         GRANT {owner} TO {migrator} WITH INHERIT FALSE, SET TRUE;
         GRANT CONNECT ON DATABASE \"{database}\" TO {migrator};
         GRANT USAGE ON SCHEMA awa TO {migrator};
         GRANT EXECUTE ON FUNCTION awa.refresh_lane_sequence_helpers_as_migrator(text) TO {migrator};
         GRANT EXECUTE ON FUNCTION awa.refresh_lane_sequence_helpers(text) TO {migrator} WITH GRANT OPTION;
         ALTER SCHEMA awa OWNER TO {owner};
         DO $$
         DECLARE f regprocedure;
         BEGIN
             FOR f IN SELECT p.oid::regprocedure FROM pg_proc p
                      WHERE p.pronamespace = 'awa'::regnamespace
                        AND p.proname IN ({LANE_FUNCTIONS})
             LOOP
                 EXECUTE format('ALTER FUNCTION %s OWNER TO {owner}', f);
             END LOOP;
         END $$;"
    )))
    .execute(&pool)
    .await?;
    // The released set_sequence_next, whose unlock handler misses a cancel.
    let current: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('awa.set_sequence_next(text,bigint)'::regprocedure)",
    )
    .fetch_one(&pool)
    .await?;
    sqlx::raw_sql(audited_sql(
        current.replace("WHEN OTHERS OR query_canceled THEN", "WHEN OTHERS THEN"),
    ))
    .execute(&pool)
    .await?;

    let result = async {
        let migrator_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                pool.connect_options()
                    .as_ref()
                    .clone()
                    .username(&migrator)
                    .password("floor_test"),
            )
            .await?;
        let mut tx = migrator_pool.begin().await?;
        let refreshed: bool =
            sqlx::query_scalar("SELECT awa.refresh_lane_sequence_helpers_as_migrator('awa')")
                .fetch_one(&mut *tx)
                .await?;
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        migrator_pool.close().await;
        ensure!(refreshed, "the refresh reported success");
        ensure_eq!(role, migrator, "the caller's role is restored");

        let definition: String = sqlx::query_scalar(
            "SELECT pg_get_functiondef('awa.set_sequence_next(text,bigint)'::regprocedure)",
        )
        .fetch_one(&pool)
        .await?;
        ensure!(
            definition.contains("OR query_canceled"),
            "set_sequence_next was refreshed"
        );
        let (owners, lent): (Vec<String>, bool) = sqlx::query_as(audited_sql(format!(
            "SELECT array_agg(DISTINCT pg_get_userbyid(p.proowner)::text),
                    has_function_privilege('{owner}', 'awa.refresh_lane_sequence_helpers(text)', 'EXECUTE')
             FROM pg_proc p
             WHERE p.pronamespace = 'awa'::regnamespace AND p.proname IN ({LANE_FUNCTIONS})"
        )))
        .fetch_one(&pool)
        .await?;
        ensure_eq!(owners, vec![owner.clone()], "the lane functions keep their owner");
        ensure!(!lent, "EXECUTE lent to the owner was taken back");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(audited_sql(format!(
        "ALTER SCHEMA awa OWNER TO CURRENT_USER;
         REASSIGN OWNED BY {owner} TO CURRENT_USER;
         DROP OWNED BY {owner}; DROP OWNED BY {migrator};
         DROP ROLE {migrator}; DROP ROLE {owner};"
    )))
    .execute(&pool)
    .await?;
    result
}
