//! `SessionAdvisoryLock` never returns a connection to the pool while it still
//! holds the lock.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pg_session_lock::pg_locks_key;
use pg_session_lock::{
    is_deadlock, is_lock_not_available, is_query_canceled, is_serialization_failure, session_holds,
    sqlstate, SessionAdvisoryLock, PG_LOCKS_KEY,
};
use sqlx::PgPool;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Whether any session in this database holds the session advisory lock
/// `key`. Advisory locks are per database, and `pg_locks` lists the whole
/// cluster's.
async fn held(pool: &PgPool, key: i64) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(concat!(
        "SELECT EXISTS (
             SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND granted AND objsubid = 1
               AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND ",
        pg_locks_key!(),
        " = $1)"
    ))
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

/// `acquire` waits for a holder to let go, and its guard releases like
/// `try_acquire`'s.
#[sqlx::test(migrations = false)]
async fn acquire_waits_for_the_holder(pool: PgPool) -> TestResult {
    let key = 7_106;
    let holder = SessionAdvisoryLock::try_acquire(&pool, key)
        .await?
        .expect("the lock is free");
    let waiter = tokio::spawn({
        let pool = pool.clone();
        async move { SessionAdvisoryLock::acquire(&pool, key).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiter.is_finished(),
        "acquire waits while the lock is held"
    );
    holder.release().await?;
    let lock = tokio::time::timeout(Duration::from_secs(5), waiter).await???;
    assert!(held(&pool, key).await?);
    lock.release().await?;
    assert!(!held(&pool, key).await?);
    Ok(())
}

/// A connection handed over for the life of the lock closes when dropped.
#[sqlx::test(migrations = false)]
async fn a_handed_over_connection_releases_the_lock_when_dropped(pool: PgPool) -> TestResult {
    let key = 7_107;
    let conn = SessionAdvisoryLock::try_acquire(&pool, key)
        .await?
        .expect("the lock is free")
        .into_connection_closed_on_drop();
    assert!(held(&pool, key).await?);
    drop(conn);
    assert!(released_within_five_seconds(&pool, key).await?);
    Ok(())
}

/// `PG_LOCKS_KEY` rebuilds every bigint key, including those with the sign
/// bit set and those either side of 2^32, and `session_holds` sees only the
/// keys its own session holds.
#[sqlx::test(migrations = false)]
async fn pg_locks_key_rebuilds_every_bigint_key(pool: PgPool) -> TestResult {
    let keys = [
        0,
        1,
        -1,
        i64::MIN,
        i64::MAX,
        (1 << 32) - 1,
        1 << 32,
        -(1 << 32),
        -(1 << 32) - 1,
        0x4157_415f_4d49_4752,
    ];
    let mut conn = pool.acquire().await?;
    for key in keys {
        // advisory-lock: held on this test's own connection, closed below.
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(key)
            .execute(&mut *conn)
            .await?;
    }
    for key in keys {
        assert!(session_holds(&mut conn, key).await?, "key {key}");
        assert!(held(&pool, key).await?, "key {key}");
    }
    assert!(!session_holds(&mut conn, 12_345).await?);
    assert_eq!(PG_LOCKS_KEY, pg_locks_key!());
    let mut other = pool.acquire().await?;
    assert!(
        !session_holds(&mut other, keys[0]).await?,
        "another session's hold is not this session's"
    );
    conn.close_on_drop();
    Ok(())
}

async fn raise(pool: &PgPool, code: &str) -> sqlx::Error {
    // The codes are this file's own constants.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DO $$ BEGIN RAISE EXCEPTION 'raised' USING ERRCODE = '{code}'; END $$"
    )))
    .execute(pool)
    .await
    .expect_err("the block raises")
}

#[sqlx::test(migrations = false)]
async fn sqlstate_checks_match_only_their_code(pool: PgPool) -> TestResult {
    type Check = fn(&sqlx::Error) -> bool;
    let checks: [(&str, Check); 4] = [
        ("40001", is_serialization_failure),
        ("40P01", is_deadlock),
        ("55P03", is_lock_not_available),
        ("57014", is_query_canceled),
    ];
    for (code, _) in checks {
        let err = raise(&pool, code).await;
        assert_eq!(sqlstate(&err).as_deref(), Some(code));
        for (other, check) in checks {
            assert_eq!(check(&err), other == code, "{other} check on {code}");
        }
    }
    let other = raise(&pool, "23505").await;
    assert!(checks.iter().all(|(_, check)| !check(&other)));
    assert_eq!(sqlstate(&sqlx::Error::RowNotFound), None);
    Ok(())
}

/// A real statement timeout and a real lock timeout are recognised.
#[sqlx::test(migrations = false)]
async fn timeouts_are_recognised(pool: PgPool) -> TestResult {
    let mut conn = pool.acquire().await?;
    sqlx::query("SET statement_timeout = '50ms'")
        .execute(&mut *conn)
        .await?;
    let err = sqlx::query("SELECT pg_sleep(5)")
        .execute(&mut *conn)
        .await
        .expect_err("the statement times out");
    assert!(is_query_canceled(&err), "{err}");

    let mut holder = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(7108)")
        .execute(&mut *holder)
        .await?;
    let mut waiter = pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '50ms'")
        .execute(&mut *waiter)
        .await?;
    let err = sqlx::query("SELECT pg_advisory_xact_lock(7108)")
        .execute(&mut *waiter)
        .await
        .expect_err("the lock wait times out");
    assert!(is_lock_not_available(&err), "{err}");
    Ok(())
}

/// A lock request the server refuses (here `lock_timeout`) was not granted, so
/// the connection goes back to the pool rather than being closed.
#[sqlx::test(migrations = false)]
async fn a_refused_acquire_returns_the_connection(pool: PgPool) -> TestResult {
    let key = 7_109;
    let holder = SessionAdvisoryLock::try_acquire(&pool, key)
        .await?
        .expect("the lock is free");
    let waiters = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout = '50ms'")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&waiters)
        .await?;

    let err = SessionAdvisoryLock::acquire(&waiters, key)
        .await
        .expect_err("the lock wait times out");
    assert!(is_lock_not_available(&err), "{err}");
    let same: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&waiters)
        .await?;
    assert_eq!(
        same, pid,
        "the connection went back to the pool, not closed"
    );

    holder.release().await?;
    waiters.close().await;
    Ok(())
}
