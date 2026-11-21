//! Schema patch `sequence_lock_cancel_release`: the lane-sequence helpers release
//! their session advisory lock when a cancel or statement timeout interrupts
//! them.

use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATCH: &str = "sequence_lock_cancel_release";

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

/// Puts back the released handlers, which release the lock only on OTHERS.
async fn restore_released_handlers(pool: &PgPool, schema: &str) -> TestResult {
    for definition in lane_sequence_helpers(pool, schema).await? {
        let released = definition.replace("WHEN OTHERS OR query_canceled THEN", "WHEN OTHERS THEN");
        assert_ne!(released, definition);
        sqlx::raw_sql(&released).execute(pool).await?;
    }
    Ok(())
}

async fn pending_targets(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(migrations::schema_patch_status(pool)
        .await?
        .into_iter()
        .find(|status| status.name == PATCH)
        .ok_or("the patch is registered")?
        .needs_repair)
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

/// Runs `statement(n)` for n = 1, 2, … 10 and returns whether any try that a
/// cancel interrupted left the lock held. The victim can still handle the
/// cancel while it waits, before it is granted the lock, and then holds
/// nothing whatever its handler does, so one try that does not leak proves
/// nothing either way.
async fn cancel_until_it_lands(
    pool: &PgPool,
    lock_key: i64,
    statement: impl Fn(i64) -> String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut landed = 0;
    for attempt in 1..=10 {
        match cancel_as_granted(pool, lock_key, &statement(attempt)).await? {
            Some(true) => return Ok(true),
            Some(false) => landed += 1,
            None => {}
        }
    }
    if landed == 0 {
        return Err("no cancel landed while the statement ran in 10 tries".into());
    }
    Ok(false)
}

async fn lane_key(pool: &PgPool, table: &str) -> Result<(String, i64), sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT seq_name, hashtextextended(format('%I.%I', 'awa', seq_name), 0)
         FROM awa.{table} WHERE queue = 'lock_q'"
    ))
    .fetch_one(pool)
    .await
}

/// Ends the sessions in this database holding the lane lock `key`, so a lock a
/// cancelled try leaked does not stall the next check. Advisory locks are per
/// database, and `pg_locks` lists the whole cluster's.
async fn end_lane_lock_holders(pool: &PgPool, key: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_locks
         WHERE locktype = 'advisory' AND pid <> pg_backend_pid()
           AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND objsubid = 1
           AND classid = (($1 >> 32) & 4294967295)::oid
           AND objid = ($1 & 4294967295)::oid",
    )
    .bind(key)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether a cancel that lands as `set_sequence_next` or the enqueue-head
/// trigger is granted its lane lock leaves the lock held, for each in turn.
async fn cancelled_lane_moves_leave_lock(
    pool: &PgPool,
) -> Result<(bool, bool), Box<dyn std::error::Error>> {
    let (claim_seq, claim_key) = lane_key(pool, "queue_claim_heads").await?;
    // Each try moves the cursor further, so a try that completes still leaves
    // the next one something to do.
    let set_next = cancel_until_it_lands(pool, claim_key, |attempt| {
        format!(
            "SELECT awa.set_sequence_next('{claim_seq}', {})",
            100 * attempt
        )
    })
    .await?;
    // A leaked lock would stall the next try; release it for the trigger check.
    end_lane_lock_holders(pool, claim_key).await?;
    let (_, enqueue_key) = lane_key(pool, "queue_enqueue_heads").await?;
    let trigger = cancel_until_it_lands(pool, enqueue_key, |_| {
        "UPDATE awa.queue_enqueue_heads SET next_seq = next_seq + 3 WHERE queue = 'lock_q'"
            .to_string()
    })
    .await?;
    end_lane_lock_holders(pool, enqueue_key).await?;
    Ok((set_next, trigger))
}

/// A cancel that lands just as a lane helper is granted its session lock leaves
/// the lock held on the pooled connection with the released handlers, and
/// releases it once the patch has refreshed them.
#[sqlx::test(migrations = false)]
async fn the_patch_releases_lane_locks_on_cancel(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let store = QueueStorage::new(QueueStorageConfig::default())?;
    store.enqueue_batch(&pool, "lock_q", 2, 1).await?;
    assert!(
        pending_targets(&pool).await?.is_empty(),
        "a fresh install has the patch"
    );

    restore_released_handlers(&pool, "awa").await?;
    assert_eq!(
        pending_targets(&pool).await?,
        vec!["lane locks in schema awa".to_string()]
    );
    assert_eq!(
        cancelled_lane_moves_leave_lock(&pool).await?,
        (true, true),
        "the released handlers leak the lock"
    );

    let report = migrations::run_with_report(&pool).await?;
    assert!(report.complete(), "{:?}", report.patches);
    assert!(pending_targets(&pool).await?.is_empty());
    for definition in lane_sequence_helpers(&pool, "awa").await? {
        assert!(definition.contains("WHEN OTHERS OR query_canceled THEN"));
    }
    assert_eq!(
        cancelled_lane_moves_leave_lock(&pool).await?,
        (false, false),
        "the patched helpers release the lock"
    );

    // The lane still takes work.
    store.enqueue_batch(&pool, "lock_q", 2, 1).await?;
    Ok(())
}

/// The lane functions `awa.refresh_lane_sequence_helpers` replaces.
const LANE_FUNCTIONS: &str = "'ensure_lane_sequences', 'queue_enqueue_head_sequence_sync', \
     'queue_claim_head_sequence_sync', 'provision_lane_sequences', 'provision_lane', \
     'set_sequence_next'";

/// The patch refreshes the helpers through
/// `awa.refresh_lane_sequence_helpers_as_migrator`. Here the migration role
/// owns the `awa` functions, as after a migration run without `SET ROLE`, while
/// the `awa` schema and its lane functions belong to an owner it may only
/// `SET ROLE` to, and that owner lacks `USAGE` on `awa` and `EXECUTE` on the
/// refresh helper. The patch, run as the migration role, refreshes the helpers
/// as the owner, takes back what it lent, and leaves the migration role as the
/// current user inside the same transaction.
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
    let owner = format!("lane_owner_{suffix}");
    let migrator = format!("lane_migrator_{suffix}");
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE ROLE {owner} NOLOGIN NOSUPERUSER;
         CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'lane_test';
         GRANT {owner} TO {migrator} WITH INHERIT FALSE, SET TRUE;
         GRANT CONNECT, CREATE ON DATABASE \"{database}\" TO {migrator};
         GRANT CREATE ON DATABASE \"{database}\" TO {owner};"
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
                    .password("lane_test"),
            )
            .await?;
        migrations::run(&migrator_pool).await?;
        sqlx::raw_sql(&format!("ALTER SCHEMA awa OWNER TO {owner}"))
            .execute(&migrator_pool)
            .await?;
        // The migration role keeps the schema privileges it created the awa
        // functions with, and the grant option to lend USAGE, which the owner
        // has given up.
        sqlx::raw_sql(&format!(
            "GRANT USAGE, CREATE ON SCHEMA awa TO {migrator} WITH GRANT OPTION;
             REVOKE USAGE ON SCHEMA awa FROM {owner};"
        ))
        .execute(&pool)
        .await?;
        sqlx::raw_sql(&format!(
            "DO $$
             DECLARE f regprocedure;
             BEGIN
                 FOR f IN SELECT p.oid::regprocedure FROM pg_proc p
                          WHERE p.pronamespace = 'awa'::regnamespace
                            AND p.proname IN ({LANE_FUNCTIONS})
                 LOOP
                     EXECUTE format('ALTER FUNCTION %s OWNER TO {owner}', f);
                 END LOOP;
             END $$;"
        ))
        .execute(&migrator_pool)
        .await?;
        restore_released_handlers(&pool, "awa").await?;
        // A second substrate owned by the same role, on which the migration
        // role has no privileges at all, not even USAGE: the patch must find
        // it from the catalogs and refresh it as the owner.
        QueueStorage::new(QueueStorageConfig {
            schema: "lane_custom".into(),
            ..Default::default()
        })?
        .prepare_schema(&pool)
        .await?;
        sqlx::raw_sql(&format!(
            "ALTER SCHEMA lane_custom OWNER TO {owner};
             DO $$
             DECLARE f regprocedure;
             BEGIN
                 FOR f IN SELECT p.oid::regprocedure FROM pg_proc p
                          WHERE p.pronamespace = 'lane_custom'::regnamespace
                 LOOP
                     EXECUTE format('ALTER FUNCTION %s OWNER TO {owner}', f);
                 END LOOP;
             END $$;"
        ))
        .execute(&pool)
        .await?;
        restore_released_handlers(&pool, "lane_custom").await?;
        let migrator_usage: bool = sqlx::query_scalar(&format!(
            "SELECT has_schema_privilege('{migrator}', 'lane_custom', 'USAGE')"
        ))
        .fetch_one(&pool)
        .await?;
        ensure!(!migrator_usage, "the migration role has no USAGE on lane_custom");

        let patch = migrations::SCHEMA_PATCHES
            .iter()
            .find(|patch| patch.name == PATCH)
            .ok_or("the patch is registered")?;
        let mut tx = migrator_pool.begin().await?;
        sqlx::raw_sql(patch.sql).execute(&mut *tx).await?;
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        migrator_pool.close().await;
        ensure_eq!(role, migrator, "the migration role is restored");

        for schema in ["awa", "lane_custom"] {
            for definition in lane_sequence_helpers(&pool, schema).await? {
                ensure!(
                    definition.contains("WHEN OTHERS OR query_canceled THEN"),
                    "the lane helpers in {schema} were refreshed"
                );
            }
        }
        let (owners, lent_execute, lent_usage): (Vec<String>, bool, bool) =
            sqlx::query_as(&format!(
                "SELECT array_agg(DISTINCT pg_get_userbyid(p.proowner)::text),
                        has_function_privilege('{owner}', 'awa.refresh_lane_sequence_helpers(text)', 'EXECUTE'),
                        has_schema_privilege('{owner}', 'awa', 'USAGE')
                 FROM pg_proc p
                 WHERE p.pronamespace = 'awa'::regnamespace AND p.proname IN ({LANE_FUNCTIONS})"
            ))
            .fetch_one(&pool)
            .await?;
        ensure_eq!(owners, vec![owner.clone()], "the lane functions keep their owner");
        ensure!(!lent_execute, "EXECUTE lent to the owner was taken back");
        ensure!(!lent_usage, "USAGE lent to the owner was taken back");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(&format!(
        "REASSIGN OWNED BY {owner}, {migrator} TO CURRENT_USER;
         DROP OWNED BY {owner}; DROP OWNED BY {migrator};
         DROP ROLE {migrator}; DROP ROLE {owner};"
    ))
    .execute(&pool)
    .await?;
    result
}

/// A schema that lost `set_sequence_next`, as a partial restore can leave it,
/// still needs the patch, and applying it defines the helper again.
#[sqlx::test(migrations = false)]
async fn a_missing_lane_helper_keeps_the_patch_pending(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    sqlx::raw_sql("DROP FUNCTION awa.set_sequence_next(text, bigint)")
        .execute(&pool)
        .await?;
    assert_eq!(
        pending_targets(&pool).await?,
        vec!["lane locks in schema awa".to_string()]
    );

    let report = migrations::run_with_report(&pool).await?;
    assert!(report.complete(), "{:?}", report.patches);
    assert!(pending_targets(&pool).await?.is_empty());
    let definitions = lane_sequence_helpers(&pool, "awa").await?;
    assert_eq!(definitions.len(), 2, "both helpers are defined");
    QueueStorage::new(QueueStorageConfig::default())?
        .enqueue_batch(&pool, "lock_q", 2, 1)
        .await?;
    Ok(())
}
