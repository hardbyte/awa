//! `SessionAdvisoryLock` never returns a connection to the pool while it still
//! holds the lock.

use awa_model::advisory::SessionAdvisoryLock;
use sqlx::PgPool;
use std::time::{Duration, Instant};

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
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !held(pool, key).await? {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(false)
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
