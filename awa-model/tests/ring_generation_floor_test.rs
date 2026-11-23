//! Schema patch `ring_generation_floor` (#847): the queue-ring generation
//! floor, which keeps a lane's ring generations in order under REPEATABLE READ
//! and SERIALIZABLE producers.

use awa_model::advisory::PG_LOCKS_KEY;
use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATCH: &str = "ring_generation_floor";
const CUSTOM: &str = "floor_custom";
const COMPAT_FUNCTIONS: [&str; 2] = ["insert_job_compat", "insert_jobs_compat_batch"];

/// The 0.6.18 definitions of the objects the patch rewrites.
const RELEASED_INSERT_JOB_COMPAT: &str =
    include_str!("../migrations/v018_insert_job_compat_ordering_key.sql");
const RELEASED_BATCH: &str = include_str!("../migrations/patches/insert_jobs_compat_batch.sql");
const RELEASED_INSTALLER: &str =
    include_str!("../migrations/v023_install_queue_storage_substrate.sql");

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

fn patch_sql() -> &'static str {
    migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == PATCH)
        .expect("the patch is registered")
        .sql
}

fn custom_store(schema: &str) -> Result<QueueStorage, awa_model::AwaError> {
    QueueStorage::new(QueueStorageConfig {
        schema: schema.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })
}

async fn pending_targets(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(migrations::schema_patch_status(pool)
        .await?
        .into_iter()
        .find(|status| status.name == PATCH)
        .ok_or("the patch is registered")?
        .needs_repair)
}

/// `(oid, definition)` of a function in `awa`.
async fn awa_function(pool: &PgPool, name: &str) -> Result<(i64, String), sqlx::Error> {
    sqlx::query_as(
        "SELECT p.oid::bigint, pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'awa' AND p.proname = $1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
}

/// The compat producers and the installer, in that order.
async fn rewritten_functions(pool: &PgPool) -> Result<Vec<(i64, String)>, sqlx::Error> {
    let mut functions = Vec::new();
    for name in COMPAT_FUNCTIONS
        .into_iter()
        .chain(["install_queue_storage_substrate"])
    {
        functions.push(awa_function(pool, name).await?);
    }
    Ok(functions)
}

/// The floor's last value, or `None` before its first raise.
async fn floor(pool: &PgPool, schema: &str) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT pg_sequence_last_value('{schema}.queue_ring_generation_floor'::regclass)"
    ))
    .fetch_one(pool)
    .await
}

/// Removes the floor from `schema`, as before the patch.
async fn drop_floor(pool: &PgPool, schema: &str) -> TestResult {
    sqlx::raw_sql(&format!(
        "DROP FUNCTION {schema}.enqueue_ring_cursor();
         DROP SEQUENCE {schema}.queue_ring_generation_floor;"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Puts back what a 0.6.18 schema has where the patch changes it: the
/// released compat producers and installer, no floor helper, and no floor in
/// `schemas`.
async fn restore_released(pool: &PgPool, schemas: &[&str]) -> TestResult {
    for sql in [
        RELEASED_INSERT_JOB_COMPAT,
        RELEASED_BATCH,
        RELEASED_INSTALLER,
    ] {
        sqlx::raw_sql(sql).execute(pool).await?;
    }
    for schema in schemas {
        drop_floor(pool, schema).await?;
    }
    sqlx::raw_sql("DROP FUNCTION awa.refresh_ring_generation_floor_helper(text)")
        .execute(pool)
        .await?;
    Ok(())
}

/// Moves the queue ring of an empty schema to `generation` the way a rotator
/// without the patch does: without raising the floor.
async fn rotate_to(pool: &PgPool, schema: &str, generation: i64) -> TestResult {
    sqlx::raw_sql(&format!(
        "UPDATE {schema}.queue_ring_slots AS slots
         SET generation = latest.generation
         FROM (
             SELECT g % state.slot_count AS slot, max(g) AS generation
             FROM {schema}.queue_ring_state AS state,
                  generate_series(state.generation + 1, {generation}) AS g
             WHERE state.singleton
             GROUP BY 1
         ) AS latest
         WHERE slots.slot = latest.slot;
         UPDATE {schema}.queue_ring_state
         SET current_slot = ({generation} % slot_count)::int, generation = {generation}
         WHERE singleton;"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn written(pool: &PgPool, schema: &str, queue: &str) -> Result<(i32, i64), sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation FROM {schema}.ready_entries WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
}

/// From a 0.6.18 schema the patch installs the floor in every queue-storage
/// schema, seeded from the ring, and rewrites the compat producers and the
/// installer in place to the bodies a fresh install has. Replaying it changes
/// nothing. Before it, this release's enqueue path reads the ring as before.
#[sqlx::test(migrations = false)]
async fn the_patch_upgrades_a_released_schema(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    assert!(
        pending_targets(&pool).await?.is_empty(),
        "a fresh install has the patch"
    );
    assert_eq!(floor(&pool, "awa").await?, Some(0), "seeded");
    let installed = rewritten_functions(&pool).await?;
    for (_, definition) in &installed {
        assert!(
            definition.contains("enqueue_ring_cursor()")
                || definition.contains("refresh_ring_generation_floor_helper")
        );
    }

    restore_released(&pool, &["awa"]).await?;
    let released = rewritten_functions(&pool).await?;
    for (_, definition) in &released {
        assert!(!definition.contains("enqueue_ring_cursor()"));
        assert!(!definition.contains("refresh_ring_generation_floor_helper"));
    }
    // A schema prepared by the released installer has no floor.
    let store = custom_store(CUSTOM)?;
    store.prepare_schema(&pool).await?;
    let has_floor: bool = sqlx::query_scalar(&format!(
        "SELECT to_regclass('{CUSTOM}.queue_ring_generation_floor') IS NOT NULL"
    ))
    .fetch_one(&pool)
    .await?;
    assert!(!has_floor);
    let mut pending = pending_targets(&pool).await?;
    pending.sort();
    assert_eq!(
        pending,
        vec![
            "awa.insert_job_compat".to_string(),
            "awa.insert_jobs_compat_batch".to_string(),
            "awa.install_queue_storage_substrate".to_string(),
            "awa.refresh_ring_generation_floor_helper".to_string(),
            "generation floor in schema awa".to_string(),
            format!("generation floor in schema {CUSTOM}"),
        ]
    );

    // This release's enqueue path on a schema without the floor reads the
    // ring as before.
    rotate_to(&pool, CUSTOM, 6).await?;
    custom_store(CUSTOM)?
        .enqueue_batch(&pool, "fallback", 2, 1)
        .await?;
    assert_eq!(written(&pool, CUSTOM, "fallback").await?, (2, 6));

    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert!(pending_targets(&pool).await?.is_empty());
    let patched = rewritten_functions(&pool).await?;
    for ((released_oid, _), (patched_oid, _)) in released.iter().zip(&patched) {
        assert_eq!(released_oid, patched_oid, "replaced in place");
    }
    assert_eq!(
        patched[..2].iter().map(|(_, d)| d).collect::<Vec<_>>(),
        installed[..2].iter().map(|(_, d)| d).collect::<Vec<_>>(),
        "the patch rewrites the released producers to the fresh-install bodies"
    );
    assert!(patched[2]
        .1
        .contains("PERFORM awa.refresh_ring_generation_floor_helper(p_schema);"));
    assert_eq!(floor(&pool, "awa").await?, Some(0));
    assert_eq!(floor(&pool, CUSTOM).await?, Some(6), "seeded from the ring");

    for schema in ["awa", CUSTOM] {
        let (definer, owned_by_ring_owner, public_execute, sequence_owned): (
            bool,
            bool,
            bool,
            bool,
        ) = sqlx::query_as(&format!(
            "SELECT p.prosecdef,
                    p.proowner = state.relowner,
                    has_function_privilege('public', p.oid, 'EXECUTE'),
                    seq.relowner = state.relowner
             FROM pg_proc AS p, pg_class AS state, pg_class AS seq
             WHERE p.oid = '{schema}.enqueue_ring_cursor()'::regprocedure
               AND state.oid = '{schema}.queue_ring_state'::regclass
               AND seq.oid = '{schema}.queue_ring_generation_floor'::regclass"
        ))
        .fetch_one(&pool)
        .await?;
        assert!(definer, "{schema}: SECURITY DEFINER");
        assert!(owned_by_ring_owner, "{schema}: owned by the ring's owner");
        assert!(public_execute, "{schema}: PUBLIC may execute it");
        assert!(sequence_owned, "{schema}: floor owned by the ring's owner");
    }

    // awa migrate re-applies the other patches the released installer lacks
    // and leaves this one present.
    let report = migrations::run_with_report(&pool).await?;
    assert!(report.complete(), "{:?}", report.patches);
    let migrated = rewritten_functions(&pool).await?;
    assert_eq!(
        migrated, installed,
        "awa migrate ends at the fresh-install bodies"
    );

    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert_eq!(
        rewritten_functions(&pool).await?,
        migrated,
        "replaying the patch changes nothing"
    );
    assert_eq!(floor(&pool, CUSTOM).await?, Some(6));

    // A schema prepared after the patch gets the floor from the installer.
    custom_store("floor_later")?.prepare_schema(&pool).await?;
    assert_eq!(floor(&pool, "floor_later").await?, Some(0));

    // A snapshot that still sees generation 6 gets the floor's generation and
    // its slot, and cannot lower the floor.
    let cursor_sql = format!("SELECT slot, generation FROM {CUSTOM}.enqueue_ring_cursor()");
    let mut stale = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *stale)
        .await?;
    sqlx::query("SELECT 1").execute(&mut *stale).await?;
    rotate_to(&pool, CUSTOM, 9).await?;
    let fresh: (i32, i64) = sqlx::query_as(&cursor_sql).fetch_one(&pool).await?;
    assert_eq!(fresh, (1, 9));
    let seen: i64 = sqlx::query_scalar(&format!(
        "SELECT generation FROM {CUSTOM}.queue_ring_state WHERE singleton"
    ))
    .fetch_one(&mut *stale)
    .await?;
    assert_eq!(seen, 6, "the snapshot predates the rotations");
    let stale_cursor: (i32, i64) = sqlx::query_as(&cursor_sql).fetch_one(&mut *stale).await?;
    stale.rollback().await?;
    assert_eq!(stale_cursor, (1, 9));
    assert_eq!(floor(&pool, CUSTOM).await?, Some(9));

    // A store that read the ring directly before the patch uses the floor
    // with its next enqueue.
    store.enqueue_batch(&pool, "after_patch", 2, 1).await?;
    assert_eq!(written(&pool, CUSTOM, "after_patch").await?, (1, 9));
    Ok(())
}

/// A runtime role granted what it needed at 0.6.18, with no default
/// privileges, keeps enqueueing after the patch and raises the floor through
/// `enqueue_ring_cursor()`, without any privilege on the floor itself.
#[sqlx::test(migrations = false)]
async fn the_patch_needs_no_new_runtime_grants(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    restore_released(&pool, &["awa"]).await?;

    let runtime = format!("floor_runtime_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE ROLE {runtime} LOGIN PASSWORD 'floor_test';
         GRANT CONNECT ON DATABASE \"{database}\" TO {runtime};
         GRANT USAGE ON SCHEMA awa TO {runtime};
         GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA awa TO {runtime};
         GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO {runtime};
         GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {runtime};"
    ))
    .execute(&pool)
    .await?;
    let result = async {
        sqlx::raw_sql(patch_sql()).execute(&pool).await?;
        rotate_to(&pool, "awa", 3).await?;

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

        ensure_eq!(
            written(&pool, "awa", "runtime_queue").await?,
            (3, 3),
            "written into the ring's generation"
        );
        ensure_eq!(floor(&pool, "awa").await?, Some(3), "the runtime raised it");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(&format!("DROP OWNED BY {runtime}; DROP ROLE {runtime};"))
        .execute(&pool)
        .await?;
    result
}

/// A migration role that can neither hand `enqueue_ring_cursor()` to the
/// ring's owner nor read the ring itself leaves the function out, with a
/// warning, rather than install one that would fail every enqueue, and the
/// patch stays pending for that schema.
#[sqlx::test(migrations = false)]
async fn floor_helper_leaves_out_a_function_its_owner_could_not_run(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    drop_floor(&pool, "awa").await?;

    let migrator = format!("floor_migrator_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'floor_test';
         GRANT CONNECT ON DATABASE \"{database}\" TO {migrator};
         GRANT USAGE, CREATE ON SCHEMA awa TO {migrator};
         GRANT EXECUTE ON FUNCTION awa.refresh_ring_generation_floor_helper(text) TO {migrator};"
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
        sqlx::query("SELECT awa.refresh_ring_generation_floor_helper('awa')")
            .execute(&migrator_pool)
            .await?;
        migrator_pool.close().await;

        let present: bool =
            sqlx::query_scalar("SELECT to_regprocedure('awa.enqueue_ring_cursor()') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        ensure!(!present, "the function is left out");
        let floor_left: bool = sqlx::query_scalar(
            "SELECT to_regclass('awa.queue_ring_generation_floor') IS NOT NULL",
        )
        .fetch_one(&pool)
        .await?;
        ensure!(
            !floor_left,
            "the floor the migrator created is dropped with the function, so the owner can install both"
        );
        ensure_eq!(
            pending_targets(&pool).await?,
            vec!["generation floor in schema awa".to_string()],
            "the patch stays pending"
        );

        let store = QueueStorage::new(QueueStorageConfig::default())?;
        store.enqueue_batch(&pool, "fallback_queue", 2, 1).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(&format!("DROP OWNED BY {migrator}; DROP ROLE {migrator};"))
        .execute(&pool)
        .await?;
    result
}

/// A migration role that may SET ROLE to the ring's owner but does not
/// inherit its privileges (PostgreSQL 16+) installs the floor as the owner and
/// can replay it: in `awa`, where it may create, and in a custom schema on
/// which it has no privilege at all, not even USAGE, which the patch finds
/// from the catalogs.
#[sqlx::test(migrations = false)]
async fn the_patch_runs_as_the_owner_for_a_set_role_migrator(pool: PgPool) -> TestResult {
    let server_version: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await?;
    if server_version < 160000 {
        return Ok(());
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("floor_owner_{suffix}");
    let migrator = format!("floor_set_migrator_{suffix}");
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE ROLE {owner} NOLOGIN NOSUPERUSER;
         CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'floor_test';
         GRANT {owner} TO {migrator} WITH INHERIT FALSE, SET TRUE;
         GRANT CONNECT, CREATE ON DATABASE \"{database}\" TO {migrator};"
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
        // The migration role installs and owns awa; the ring belongs to an
        // owner it may only SET ROLE to.
        migrations::run(&migrator_pool).await?;
        sqlx::raw_sql(&format!(
            "GRANT USAGE, CREATE ON SCHEMA awa TO {owner};
             ALTER TABLE awa.queue_ring_state OWNER TO {owner};"
        ))
        .execute(&migrator_pool)
        .await?;
        drop_floor(&migrator_pool, "awa").await?;
        custom_store(CUSTOM)?.prepare_schema(&pool).await?;
        drop_floor(&pool, CUSTOM).await?;
        sqlx::raw_sql(&format!(
            "ALTER SCHEMA {CUSTOM} OWNER TO {owner};
             ALTER TABLE {CUSTOM}.queue_ring_state OWNER TO {owner};"
        ))
        .execute(&pool)
        .await?;
        rotate_to(&pool, CUSTOM, 5).await?;
        let migrator_usage: bool = sqlx::query_scalar(&format!(
            "SELECT has_schema_privilege('{migrator}', '{CUSTOM}', 'USAGE')"
        ))
        .fetch_one(&pool)
        .await?;
        ensure!(
            !migrator_usage,
            "the migration role has no USAGE on {CUSTOM}"
        );

        // One transaction, as awa migrate runs a patch: the role the helper
        // sets is transaction-local, so only a check inside the same
        // transaction shows the patch restored the caller's role.
        let mut tx = migrator_pool.begin().await?;
        for _ in 0..2 {
            sqlx::raw_sql(patch_sql()).execute(&mut *tx).await?;
        }
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        migrator_pool.close().await;
        ensure_eq!(role, migrator, "the patch restores the caller's role");

        for (schema, seeded) in [("awa", 0), (CUSTOM, 5)] {
            let (function_owner, sequence_owner): (String, String) = sqlx::query_as(&format!(
                "SELECT pg_get_userbyid(p.proowner)::text, pg_get_userbyid(c.relowner)::text
                 FROM pg_proc AS p, pg_class AS c
                 WHERE p.oid = '{schema}.enqueue_ring_cursor()'::regprocedure
                   AND c.oid = '{schema}.queue_ring_generation_floor'::regclass"
            ))
            .fetch_one(&pool)
            .await?;
            ensure_eq!(
                function_owner,
                owner,
                "{schema}: the function belongs to the ring's owner"
            );
            ensure_eq!(
                sequence_owner,
                owner,
                "{schema}: the floor belongs to the ring's owner"
            );
            ensure_eq!(
                floor(&pool, schema).await?,
                Some(seeded),
                "{schema}: seeded"
            );
        }
        ensure!(
            pending_targets(&pool).await?.is_empty(),
            "the patch is applied"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(&format!(
        "ALTER TABLE awa.queue_ring_state OWNER TO CURRENT_USER;
         REASSIGN OWNED BY {owner}, {migrator} TO CURRENT_USER;
         DROP OWNED BY {owner}; DROP OWNED BY {migrator};
         DROP ROLE {migrator}; DROP ROLE {owner};"
    ))
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
    // advisory-lock: test gate on a connection it releases below.
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut *gate)
        .await?;
    let run = async { sqlx::query(statement).execute(&mut *victim).await };
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

/// Ends the sessions in this database holding the advisory lock `key`, so a
/// lock a cancelled try leaked does not stall the next one.
async fn end_lock_holders(pool: &PgPool, key: i64) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_locks
         WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()
           AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND objsubid = 1 AND {PG_LOCKS_KEY} = $1"
    ))
    .bind(key)
    .execute(pool)
    .await?;
    Ok(())
}

/// Raises the floor of `awa` with a cancel landing as the floor's lock is
/// granted, up to ten times, moving the ring one generation before each try
/// so each needs a raise. Returns whether any try that a cancel interrupted
/// left the lock held.
async fn cancelled_raise_leaves_lock(
    pool: &PgPool,
    lock_key: i64,
    generation: &mut i64,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut landed = 0;
    for _ in 0..10 {
        *generation += 1;
        rotate_to(pool, "awa", *generation).await?;
        match cancel_as_granted(pool, lock_key, "SELECT FROM awa.enqueue_ring_cursor()").await? {
            Some(true) => {
                end_lock_holders(pool, lock_key).await?;
                return Ok(true);
            }
            Some(false) => landed += 1,
            None => {}
        }
    }
    if landed == 0 {
        return Err("no cancel landed while the raise ran in 10 tries".into());
    }
    Ok(false)
}

/// A pooled connection keeps a session lock it leaks, so a raise of the floor
/// that is cancelled must leave the floor's lock free, whether the cancel lands
/// as the lock is granted or a statement timeout fires while it waits. A
/// handler that releases the lock only on OTHERS leaks it.
#[sqlx::test(migrations = false)]
async fn cancelled_floor_raise_leaves_no_session_lock(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let lock_key: i64 = sqlx::query_scalar(
        "SELECT hashtextextended('awa.queue_storage.ring_generation_floor:awa', 0)",
    )
    .fetch_one(&pool)
    .await?;
    let mut generation = 0;

    let (_, definition) = awa_function(&pool, "enqueue_ring_cursor").await?;
    let others_only = definition.replace("WHEN OTHERS OR query_canceled THEN", "WHEN OTHERS THEN");
    assert_ne!(others_only, definition);
    sqlx::raw_sql(&others_only).execute(&pool).await?;
    assert!(
        cancelled_raise_leaves_lock(&pool, lock_key, &mut generation).await?,
        "a handler without query_canceled keeps the floor lock"
    );
    assert_eq!(
        pending_targets(&pool).await?,
        vec!["generation floor in schema awa".to_string()]
    );
    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert!(pending_targets(&pool).await?.is_empty());

    assert!(
        !cancelled_raise_leaves_lock(&pool, lock_key, &mut generation).await?,
        "a cancel as the lock was granted kept the floor lock"
    );

    // A statement timeout that fires while the raise waits for the lock.
    generation += 1;
    rotate_to(&pool, "awa", generation).await?;
    let mut gate = pool.acquire().await?;
    let mut raiser = pool.acquire().await?;
    let raiser_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *raiser)
        .await?;
    // advisory-lock: test gate on a connection it releases below.
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
        ((generation % 16) as i32, generation),
        "a later raise still succeeds"
    );
    Ok(())
}

/// The probe names each object that lacks the patch, including a batch
/// producer the `insert_jobs_compat_batch` patch has yet to create, and
/// ignores a schema with the substrate's object names under a name awa
/// cannot install a substrate under. `awa migrate` repairs them all.
#[sqlx::test(migrations = false)]
async fn the_probe_names_what_lacks_the_patch(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    sqlx::raw_sql(
        r#"CREATE SCHEMA "Lookalike";
           CREATE TABLE "Lookalike".queue_ring_state ();
           CREATE FUNCTION "Lookalike".claim_ready_runtime(text, bigint, double precision, double precision)
               RETURNS void LANGUAGE sql AS '';
           CREATE SCHEMA lookalike;
           CREATE TABLE lookalike.queue_ring_state (description text);
           CREATE FUNCTION lookalike.claim_ready_runtime(text, bigint, double precision, double precision)
               RETURNS void LANGUAGE sql AS '';"#,
    )
    .execute(&pool)
    .await?;
    assert!(pending_targets(&pool).await?.is_empty());
    // A lowercase schema with awa's claim function and a ring table of the
    // same name but not awa's columns is not a substrate: the patch replays
    // over it and leaves it alone.
    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    let floor_added: bool = sqlx::query_scalar(
        "SELECT to_regclass('lookalike.queue_ring_generation_floor') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await?;
    assert!(!floor_added, "the lookalike schema gets no floor");
    let refused = sqlx::query("SELECT awa.refresh_ring_generation_floor_helper('lookalike')")
        .execute(&pool)
        .await
        .expect_err("the helper refuses a table that is not awa's ring");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("22023")
    );

    drop_floor(&pool, "awa").await?;
    assert_eq!(
        pending_targets(&pool).await?,
        vec!["generation floor in schema awa".to_string()]
    );
    let pending: Vec<_> = migrations::pending_schema_patches(&pool)
        .await?
        .iter()
        .map(|patch| patch.name)
        .collect();
    assert_eq!(pending, migrations::patches_to_apply(&[PATCH]));

    sqlx::raw_sql(RELEASED_INSERT_JOB_COMPAT)
        .execute(&pool)
        .await?;
    sqlx::raw_sql(
        "DROP FUNCTION awa.insert_jobs_compat_batch(text[], text[], jsonb[], text[], \
         smallint[], smallint[], timestamptz[], jsonb[], jsonb[], bytea[], text[], text[], boolean)",
    )
    .execute(&pool)
    .await?;
    let (_, installer) = awa_function(&pool, "install_queue_storage_substrate").await?;
    // Cut the patch's floor block, from its marker to the installer's END.
    let without_floor = {
        let marker = "\n\n    -- #847: the queue-ring generation floor";
        let start = installer
            .find(marker)
            .ok_or("the installer calls the floor helper")?;
        let end = start
            + installer[start..]
                .find("\nEND;")
                .ok_or("the installer ends")?;
        format!("{}{}", &installer[..start], &installer[end..])
    };
    assert_ne!(without_floor, installer);
    sqlx::raw_sql(&without_floor).execute(&pool).await?;
    assert_eq!(
        pending_targets(&pool).await?,
        vec![
            "awa.insert_job_compat".to_string(),
            "awa.insert_jobs_compat_batch".to_string(),
            "awa.install_queue_storage_substrate".to_string(),
            "generation floor in schema awa".to_string(),
        ]
    );
    let pending: Vec<_> = migrations::pending_schema_patches(&pool)
        .await?
        .iter()
        .map(|patch| patch.name)
        .collect();
    assert_eq!(
        pending,
        migrations::patches_to_apply(&["insert_jobs_compat_batch", PATCH])
    );

    let report = migrations::run_with_report(&pool).await?;
    assert!(report.complete(), "{:?}", report.patches);
    assert!(pending_targets(&pool).await?.is_empty());
    let (_, batch) = awa_function(&pool, "insert_jobs_compat_batch").await?;
    assert!(batch.contains("enqueue_ring_cursor()"));
    assert_eq!(floor(&pool, "awa").await?, Some(0));
    Ok(())
}

/// The patch does not wait for a schema's install lock, which a concurrent
/// `prepare_schema()` holds: in an `awa migrate --sql` bundle the earlier
/// replica_identity patch can hold table locks that installer needs, so a wait
/// could deadlock. It fails with 55P03 instead, changes nothing, and succeeds
/// on a later run.
#[sqlx::test(migrations = false)]
async fn the_patch_does_not_wait_for_an_install_lock(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    drop_floor(&pool, "awa").await?;

    let mut installer = pool.begin().await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('awa.queue_storage.install:awa', 0))",
    )
    .execute(&mut *installer)
    .await?;
    // A patch that waited for the lock would otherwise hang here, since the
    // holder releases it only after this returns.
    let mut patcher = pool.acquire().await?;
    sqlx::query("SET statement_timeout = '10s'")
        .execute(&mut *patcher)
        .await?;
    let started = std::time::Instant::now();
    let error = sqlx::raw_sql(patch_sql())
        .execute(&mut *patcher)
        .await
        .expect_err("the install lock is held");
    sqlx::query("RESET statement_timeout")
        .execute(&mut *patcher)
        .await?;
    drop(patcher);
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(
        error
            .as_database_error()
            .and_then(|db| db.code())
            .as_deref(),
        Some("55P03"),
        "{error}"
    );
    assert_eq!(
        pending_targets(&pool).await?,
        vec!["generation floor in schema awa".to_string()],
        "a refused run changes nothing"
    );
    installer.rollback().await?;

    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert!(pending_targets(&pool).await?.is_empty());
    Ok(())
}

/// A role granted the installer may also run the floor helper the installer
/// now calls, so it can still prepare a schema after the patch.
#[sqlx::test(migrations = false)]
async fn installer_grantees_may_run_the_floor_helper(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let operator = format!("floor_operator_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {operator} NOLOGIN;
         GRANT EXECUTE ON FUNCTION awa.install_queue_storage_substrate(text, integer, integer, integer, boolean) TO {operator};"
    ))
    .execute(&pool)
    .await?;
    let result = async {
        let before: bool = sqlx::query_scalar(&format!(
            "SELECT has_function_privilege('{operator}', 'awa.refresh_ring_generation_floor_helper(text)', 'EXECUTE')"
        ))
        .fetch_one(&pool)
        .await?;
        ensure!(!before, "the helper is not executable by default");
        sqlx::raw_sql(patch_sql()).execute(&pool).await?;
        let after: bool = sqlx::query_scalar(&format!(
            "SELECT has_function_privilege('{operator}', 'awa.refresh_ring_generation_floor_helper(text)', 'EXECUTE')"
        ))
        .fetch_one(&pool)
        .await?;
        ensure!(after, "the patch grants the helper to the installer's grantees");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(&format!("DROP OWNED BY {operator}; DROP ROLE {operator};"))
        .execute(&pool)
        .await?;
    result
}

/// A role granted the installer and its 0.6.18 helpers gets the floor helper
/// from the patch and prepares schemas with the floor; once the helper is
/// revoked it still prepares them, without the floor and with a warning.
#[sqlx::test(migrations = false)]
async fn an_installer_grantee_prepares_a_schema_with_or_without_the_helper(
    pool: PgPool,
) -> TestResult {
    migrations::run(&pool).await?;
    let operator = format!("floor_installer_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE ROLE {operator} LOGIN NOSUPERUSER PASSWORD 'floor_test';
         GRANT CONNECT, CREATE ON DATABASE \"{database}\" TO {operator};
         GRANT USAGE ON SCHEMA awa TO {operator};
         GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA awa TO {operator};
         GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO {operator};
         GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {operator};"
    ))
    .execute(&pool)
    .await?;
    let result = async {
        let operator_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                pool.connect_options()
                    .as_ref()
                    .clone()
                    .username(&operator)
                    .password("floor_test"),
            )
            .await?;
        let has_floor = |schema: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, bool>(&format!(
                    "SELECT to_regprocedure('{schema}.enqueue_ring_cursor()') IS NOT NULL"
                ))
                .fetch_one(&pool)
                .await
            }
        };

        // The patch grants the helper to the installer's grantees.
        sqlx::raw_sql(patch_sql()).execute(&pool).await?;
        sqlx::query("SELECT awa.install_queue_storage_substrate('floor_with')")
            .execute(&operator_pool)
            .await?;
        ensure!(
            has_floor("floor_with").await?,
            "a granted caller installs the floor"
        );

        // Without the helper the schema is still prepared, without the floor.
        sqlx::raw_sql(&format!(
            "REVOKE EXECUTE ON FUNCTION awa.refresh_ring_generation_floor_helper(text) FROM {operator}"
        ))
        .execute(&pool)
        .await?;
        sqlx::query("SELECT awa.install_queue_storage_substrate('floor_without')")
            .execute(&operator_pool)
            .await?;
        ensure!(
            !has_floor("floor_without").await?,
            "a caller that may not run the helper installs without the floor"
        );
        operator_pool.close().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(&format!(
        "DROP SCHEMA IF EXISTS floor_without CASCADE; DROP SCHEMA IF EXISTS floor_with CASCADE;
         DROP OWNED BY {operator}; DROP ROLE {operator};"
    ))
    .execute(&pool)
    .await?;
    result
}
