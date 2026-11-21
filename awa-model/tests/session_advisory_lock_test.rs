//! `SessionAdvisoryLock` never returns a connection to the pool while it still
//! holds the lock.

use awa_model::advisory::SessionAdvisoryLock;
use sqlx::PgPool;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Whether any session in this database holds the session advisory lock
/// `key` (a small positive key, so it fits in `objid`). Advisory locks are
/// per database, and `pg_locks` lists the whole cluster's.
async fn held(pool: &PgPool, key: i64) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND granted AND objsubid = 1
               AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND classid = 0 AND objid = $1::bigint::oid
         )",
    )
    .bind(key)
    .fetch_one(pool)
    .await
}

/// Waits up to five seconds for the lock to be released; a closed connection
/// ends its session asynchronously.
async fn released_within_five_seconds(pool: &PgPool, key: i64) -> Result<bool, sqlx::Error> {
    let poll = async {
        while held(pool, key).await? {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    };
    match tokio::time::timeout(Duration::from_secs(5), poll).await {
        Ok(result) => result.map(|()| true),
        Err(_) => Ok(false),
    }
}

#[sqlx::test(migrations = false)]
async fn release_unlocks_and_returns_the_connection(pool: PgPool) -> TestResult {
    let key = 7_101;
    let lock = SessionAdvisoryLock::try_acquire(&pool, key)
        .await?
        .expect("the lock is free");
    assert!(held(&pool, key).await?);
    assert!(
        SessionAdvisoryLock::try_acquire(&pool, key)
            .await?
            .is_none(),
        "a second session does not get the lock"
    );
    let pid: i32 = {
        let mut lock = lock;
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(lock.connection())
            .await?;
        lock.release().await?;
        pid
    };
    assert!(!held(&pool, key).await?, "release unlocks at once");
    let alive: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
            .bind(pid)
            .fetch_one(&pool)
            .await?;
    assert!(alive, "the connection went back to the pool, not closed");
    Ok(())
}

/// Session locks stack, so a key taken again on the guard's connection is
/// still held after one unlock; release then closes the connection rather than
/// pool it.
#[sqlx::test(migrations = false)]
async fn release_closes_a_connection_that_still_holds_the_key(pool: PgPool) -> TestResult {
    let key = 7_105;
    let mut lock = SessionAdvisoryLock::try_acquire(&pool, key)
        .await?
        .expect("the lock is free");
    // advisory-lock: a second hold on the guard's own connection.
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(key)
        .execute(lock.connection())
        .await?;
    lock.release().await?;
    assert!(released_within_five_seconds(&pool, key).await?);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn a_dropped_guard_releases_the_lock(pool: PgPool) -> TestResult {
    let key = 7_102;
    let lock = SessionAdvisoryLock::try_acquire(&pool, key)
        .await?
        .expect("the lock is free");
    drop(lock);
    assert!(released_within_five_seconds(&pool, key).await?);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn a_cancelled_holder_releases_the_lock(pool: PgPool) -> TestResult {
    let key = 7_103;
    // Acquired before the timeout starts, so the timeout cancels a holder
    // that owns the lock rather than the acquisition.
    let lock = SessionAdvisoryLock::try_acquire(&pool, key)
        .await?
        .expect("the lock is free");
    let holder = async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        lock.release().await
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(500), holder)
            .await
            .is_err(),
        "the holder is cancelled while it holds the lock"
    );
    assert!(released_within_five_seconds(&pool, key).await?);
    Ok(())
}

/// The hazard the guard exists for: a plain pooled connection dropped while it
/// holds a session lock goes back to the pool with the lock still held.
#[sqlx::test(migrations = false)]
async fn a_dropped_pooled_connection_keeps_the_lock(pool: PgPool) -> TestResult {
    let key = 7_104;
    let mut conn = pool.acquire().await?;
    // advisory-lock: demonstrates the leak the guard prevents.
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(key)
        .execute(&mut *conn)
        .await?;
    drop(conn);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        held(&pool, key).await?,
        "the idle pooled connection still holds it"
    );
    Ok(())
}

/// A cancelled `awa migrate` whose statement is still waiting on the server,
/// here for a table lock another transaction holds, gives up the migration
/// lock within seconds rather than when that wait ends.
#[sqlx::test(migrations = false)]
async fn a_cancelled_migration_run_releases_the_migration_lock(pool: PgPool) -> TestResult {
    awa_model::migrations::run(&pool).await?;
    let mut blocker = pool.begin().await?;
    sqlx::query("LOCK TABLE awa.schema_version IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;

    let key = awa_model::migrations::MIGRATION_LOCK_KEY;
    let mut run = Box::pin(awa_model::migrations::run(&pool));
    let waiting = async {
        for _ in 0..200 {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_locks AS held
                     JOIN pg_stat_activity AS activity ON activity.pid = held.pid
                     WHERE held.locktype = 'advisory' AND held.granted AND held.objsubid = 1
                       AND held.classid = (($1 >> 32) & 4294967295)::oid
                       AND held.objid = ($1 & 4294967295)::oid
                       AND activity.wait_event_type = 'Lock'
                       AND activity.wait_event = 'relation'
                 )",
            )
            .bind(key)
            .fetch_one(&pool)
            .await?;
            if waiting {
                return Ok::<bool, sqlx::Error>(true);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(false)
    };
    let waiting = tokio::select! {
        result = &mut run => panic!("the run finished while the table was locked: {result:?}"),
        waiting = waiting => waiting?,
    };
    assert!(
        waiting,
        "the run holds the migration lock and waits for the table"
    );

    // Cancel the run: dropping its future closes the connection.
    drop(run);
    let held = |pool: PgPool| async move {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND objsubid = 1
               AND classid = (($1 >> 32) & 4294967295)::oid
               AND objid = ($1 & 4294967295)::oid)",
        )
        .bind(key)
        .fetch_one(&pool)
        .await
    };
    let released = tokio::time::timeout(Duration::from_secs(5), async {
        while held(pool.clone()).await? {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<(), sqlx::Error>(())
    })
    .await;
    blocker.rollback().await?;
    assert!(
        matches!(released, Ok(Ok(()))),
        "the migration lock outlived the cancelled run"
    );
    Ok(())
}
