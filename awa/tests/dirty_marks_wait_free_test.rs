//! The canonical engine's admin dirty-key triggers must never make a job
//! transition wait on another transaction (v046).
//!
//! Requires a running Postgres instance:
//! `DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test`.

use awa::model::reschedule::{reschedule_canonical_attempt, Reschedule, RescheduleOutcome};
use awa_testing::setup;
use sqlx::PgPool;
use std::time::Duration;

const TABLES: [&str; 2] = ["awa.admin_dirty_queue_marks", "awa.admin_dirty_kind_marks"];
async fn drain_all_marks(pool: &PgPool) {
    sqlx::query("SELECT awa.refresh_admin_metadata()")
        .execute(pool)
        .await
        .expect("refresh admin metadata");
}

/// A transaction that enqueues first and then locks an application row must
/// not deadlock with one that locks the same row first and then enqueues into
/// the same queue. Under the v006 `ON CONFLICT DO NOTHING` marks the second
/// enqueue blocked on the first transaction's uncommitted dirty row, and the
/// first transaction's row lock then closed the cycle; Postgres aborted one of
/// them with SQLSTATE 40P01. With append-only marks neither enqueue waits.
#[tokio::test]
async fn enqueues_in_application_transactions_do_not_deadlock_through_dirty_marks() {
    if setup::skip_unless_canonical(
        "enqueues_in_application_transactions_do_not_deadlock_through_dirty_marks",
    ) {
        return;
    }
    let pool = setup::setup(4).await;
    let queue = format!("dirty_marks_app_{}", uuid::Uuid::new_v4().simple());

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS awa_test_dirty_marks_app_rows (id INT PRIMARY KEY, n INT NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO awa_test_dirty_marks_app_rows (id, n) VALUES (1, 0) ON CONFLICT (id) DO NOTHING",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Both marks must be absent so the first enqueue is the inserter.
    drain_all_marks(&pool).await;

    let mut first = pool.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO awa.jobs (kind, queue, args) VALUES ('dirty_marks_app', $1, '{}'::jsonb)",
    )
    .bind(&queue)
    .execute(&mut *first)
    .await
    .unwrap();

    let mut second = pool.begin().await.unwrap();
    sqlx::query("UPDATE awa_test_dirty_marks_app_rows SET n = n + 1 WHERE id = 1")
        .execute(&mut *second)
        .await
        .unwrap();
    // The enqueue that used to block: it must complete while `first` is
    // still open and holds its uncommitted mark for the same queue.
    tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query(
            "INSERT INTO awa.jobs (kind, queue, args) VALUES ('dirty_marks_app', $1, '{}'::jsonb)",
        )
        .bind(&queue)
        .execute(&mut *second),
    )
    .await
    .expect("second enqueue waited on the first transaction's dirty mark")
    .expect("second enqueue");

    // `first` now takes the application row, which `second` holds: a
    // legitimate application-level wait that resolves when `second` commits.
    let first_update = tokio::spawn(async move {
        sqlx::query("UPDATE awa_test_dirty_marks_app_rows SET n = n + 1 WHERE id = 1")
            .execute(&mut *first)
            .await?;
        first.commit().await?;
        Ok::<(), sqlx::Error>(())
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    second.commit().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), first_update)
        .await
        .expect("first transaction did not finish after second committed")
        .expect("join")
        .expect("first transaction must commit without a deadlock abort");

    let inserted: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = $1")
        .bind(&queue)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(inserted, 2);

    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(&queue)
        .execute(&pool)
        .await
        .unwrap();
}

/// Lock contract for every trigger on the canonical hot tables (ADR-045).
///
/// A trigger runs inside every job transition, so anything it does that can
/// wait on another transaction is a deadlock edge joining transactions that
/// share nothing but a queue or kind name. The contract:
///
/// * a trigger may write only the transitioning job's own rows in
///   `job_unique_claims` (uniqueness needs the unique index, and a conflict
///   there is a real duplicate) and the append-only mark tables;
/// * the mark tables carry no index and no constraint, so an INSERT into them
///   is a plain heap insert;
/// * no trigger body uses `ON CONFLICT`, `UPDATE`, `DELETE` of a shared row,
///   `LOCK`, or any row lock (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`,
///   `FOR KEY SHARE`), except `sync_job_unique_claims` deleting the job's own
///   claim row;
/// * the maintenance functions that read the marks never `TRUNCATE`, so they
///   never take ACCESS EXCLUSIVE against the hot path.
///
/// Adding a trigger that writes anywhere else, or a constraint to a mark
/// table, must fail here and be argued in a new ADR.
#[tokio::test]
async fn hot_path_triggers_cannot_wait_on_other_transactions() {
    if setup::skip_unless_canonical("hot_path_triggers_cannot_wait_on_other_transactions") {
        return;
    }
    let pool = setup::setup(2).await;

    for table in TABLES {
        let indexes: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_index WHERE indrelid = $1::regclass")
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(indexes, 0, "{table} must have no index");
        let constraints: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass AND contype <> 'n'",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(constraints, 0, "{table} must have no constraint");
    }

    // Every non-internal trigger on the canonical hot tables and its body.
    let triggers: Vec<(String, String, String)> = sqlx::query_as(
        r#"
        SELECT c.relname::text, p.proname::text, p.prosrc
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_proc p ON p.oid = t.tgfoid
        WHERE c.relnamespace = 'awa'::regnamespace
          AND c.relname IN ('jobs_hot', 'scheduled_jobs')
          AND NOT t.tgisinternal
        ORDER BY 1, 2
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        triggers.len() >= 7,
        "expected the dirty-mark, unique-claim and notify triggers, found {}",
        triggers.len()
    );

    for (table, function, body) in &triggers {
        let upper = body.to_uppercase();
        let allowed_writes: &[&str] = if function == "sync_job_unique_claims" {
            &["awa.job_unique_claims"]
        } else {
            &["awa.admin_dirty_queue_marks", "awa.admin_dirty_kind_marks"]
        };
        for target in write_targets(body) {
            assert!(
                allowed_writes.contains(&target.as_str()),
                "trigger {function} on {table} writes {target}, outside the ADR-045 allowlist {allowed_writes:?}"
            );
        }
        for forbidden in [
            "ON CONFLICT",
            "TRUNCATE",
            "LOCK ",
            "FOR UPDATE",
            "FOR NO KEY UPDATE",
            "FOR SHARE",
            "FOR KEY SHARE",
        ] {
            assert!(
                !upper.contains(forbidden),
                "trigger {function} on {table} contains `{forbidden}`, which can wait on another transaction"
            );
        }
        if function != "sync_job_unique_claims" {
            for forbidden in ["UPDATE ", "DELETE "] {
                assert!(
                    !upper.contains(forbidden),
                    "trigger {function} on {table} contains `{forbidden}` against a shared row"
                );
            }
        }
    }

    for function in ["refresh_admin_metadata", "recompute_dirty_admin_metadata"] {
        let body: String = sqlx::query_scalar(
            "SELECT prosrc FROM pg_proc WHERE proname = $1 AND pronamespace = 'awa'::regnamespace",
        )
        .bind(function)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            !body.to_uppercase().contains("TRUNCATE"),
            "{function} must not TRUNCATE tables the hot-path triggers write"
        );
    }
}

/// Tables a PL/pgSQL body writes: the identifier after each `INSERT INTO`,
/// `UPDATE`, or `DELETE FROM`, with comments and string literals removed and
/// whitespace normalised first. Unqualified names are returned as written so
/// the allowlist check fails closed; only the transition tables `new_rows`
/// and `old_rows`, which are read but never written, appear unqualified in a
/// compliant trigger.
fn write_targets(body: &str) -> Vec<String> {
    let mut stripped = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '-' if chars.peek() == Some(&'-') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
                stripped.push(' ');
            }
            '\'' => {
                for c in chars.by_ref() {
                    if c == '\'' {
                        break;
                    }
                }
                stripped.push(' ');
            }
            c if c.is_whitespace() => stripped.push(' '),
            c => stripped.extend(c.to_lowercase()),
        }
    }
    let tokens: Vec<&str> = stripped.split(' ').filter(|t| !t.is_empty()).collect();
    let ident = |t: &str| -> Option<String> {
        let name: String = t
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            .collect();
        (!name.is_empty()).then_some(name)
    };
    let mut targets = Vec::new();
    for (i, token) in tokens.iter().enumerate() {
        let target = match *token {
            "insert" if tokens.get(i + 1) == Some(&"into") => tokens.get(i + 2),
            "delete" if tokens.get(i + 1) == Some(&"from") => tokens.get(i + 2),
            "update" => tokens.get(i + 1),
            _ => None,
        };
        if let Some(target) = target.and_then(|t| ident(t)) {
            targets.push(target);
        }
    }
    targets
}

#[test]
fn write_targets_fails_closed_on_unqualified_names_and_ignores_comments() {
    let body = "-- heartbeat updates don't matter\n\
                INSERT INTO\n  awa.admin_dirty_queue_marks (queue) SELECT DISTINCT queue FROM new_rows;\n\
                IF TG_OP = 'UPDATE' THEN DELETE FROM other_table WHERE x = 'DELETE FROM literal'; END IF;\n\
                update awa.job_unique_claims set job_id = 1;";
    assert_eq!(
        write_targets(body),
        vec![
            "awa.admin_dirty_queue_marks".to_string(),
            "other_table".to_string(),
            "awa.job_unique_claims".to_string(),
        ]
    );
}

/// The drain deletes only marks it can see, so a transaction that commits
/// after the drain read its keys leaves its mark for the next drain and the
/// cached counts stay exact.
#[tokio::test]
async fn drain_leaves_marks_of_uncommitted_transitions_for_the_next_pass() {
    if setup::skip_unless_canonical(
        "drain_leaves_marks_of_uncommitted_transitions_for_the_next_pass",
    ) {
        return;
    }
    let pool = setup::setup(3).await;
    let queue = format!("dirty_marks_drain_{}", uuid::Uuid::new_v4().simple());

    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO awa.jobs (kind, queue, args) VALUES ('dirty_marks_drain', $1, '{}'::jsonb)",
    )
    .bind(&queue)
    .execute(&mut *tx)
    .await
    .unwrap();

    // Drain while the enqueue is uncommitted: its mark is invisible and the
    // drain must not block on it.
    tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query("SELECT awa.recompute_dirty_admin_metadata(1000)").execute(&pool),
    )
    .await
    .expect("drain blocked on an uncommitted transition")
    .unwrap();
    let cached_before = setup::queue_state_counts(&pool, &queue).await;
    assert_eq!(setup::state_count(&cached_before, "available"), 0);

    tx.commit().await.unwrap();
    let marks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.admin_dirty_queue_marks WHERE queue = $1")
            .bind(&queue)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        marks, 1,
        "the committed transition's mark must survive the earlier drain"
    );

    awa::model::admin::flush_dirty_admin_metadata(&pool)
        .await
        .unwrap();
    let cached_after = setup::queue_state_counts(&pool, &queue).await;
    assert_eq!(setup::state_count(&cached_after, "available"), 1);

    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(&queue)
        .execute(&pool)
        .await
        .unwrap();
}

/// Concurrent re-schedules across a few queues and kinds while the drain and
/// the full refresh run back-to-back: the incident shape, at test scale. No
/// completion may fail, and no statement may wait on another transaction
/// long enough to matter.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_reschedules_with_drain_and_refresh_never_deadlock() {
    if setup::skip_unless_canonical("concurrent_reschedules_with_drain_and_refresh_never_deadlock")
    {
        return;
    }
    let pool = setup::setup(12).await;
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let queues: Vec<String> = (0..3)
        .map(|i| format!("dirty_marks_load_{tag}_q{i}"))
        .collect();

    let mut job_ids = Vec::new();
    for i in 0..24 {
        let id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO awa.jobs_hot (kind, queue, args, state, attempt, run_at, attempted_at, heartbeat_at, run_lease)
            VALUES ($1, $2, '{}'::jsonb, 'running', 1, now(), now(), now(), 1)
            RETURNING id
            "#,
        )
        .bind(format!("dirty_marks_load_k{}", i % 3))
        .bind(&queues[i % 3])
        .fetch_one(&pool)
        .await
        .unwrap();
        job_ids.push(id);
    }

    let stop = std::sync::Arc::new(tokio::sync::Notify::new());
    let maintenance = {
        let pool = pool.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut ticks = 0u32;
            loop {
                tokio::select! {
                    _ = stop.notified() => break,
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
                sqlx::query("SELECT awa.recompute_dirty_admin_metadata(100)")
                    .execute(&pool)
                    .await
                    .expect("drain");
                ticks += 1;
                if ticks.is_multiple_of(20) {
                    sqlx::query("SELECT awa.refresh_admin_metadata()")
                        .execute(&pool)
                        .await
                        .expect("refresh");
                }
            }
        })
    };

    let mut workers = Vec::new();
    for id in job_ids.clone() {
        let pool = pool.clone();
        workers.push(tokio::spawn(async move {
            let mut lease = 1i64;
            for _ in 0..40 {
                let outcome = reschedule_canonical_attempt(
                    &pool,
                    id,
                    lease,
                    Reschedule::Snooze { delay_secs: 0.0 },
                    None,
                    None,
                )
                .await
                .expect("re-schedule must not fail (a deadlock abort surfaces here)");
                assert!(matches!(outcome, RescheduleOutcome::Rescheduled { .. }));
                // Stand-in for promote + claim: back into jobs_hot as running
                // under a fresh lease.
                lease = sqlx::query_scalar(
                    r#"
                    WITH deleted AS (
                        DELETE FROM awa.scheduled_jobs WHERE id = $1 RETURNING *
                    ), moved AS (
                        INSERT INTO awa.jobs_hot (
                            id, kind, queue, args, state, priority, attempt, max_attempts,
                            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
                            created_at, errors, metadata, tags, unique_key, unique_states,
                            callback_id, callback_timeout_at, callback_filter,
                            callback_on_complete, callback_on_fail, callback_transform,
                            run_lease, progress
                        )
                        SELECT id, kind, queue, args, 'running', priority, attempt + 1, max_attempts,
                               run_at, now(), NULL, now(), NULL,
                               created_at, errors, metadata, tags, unique_key, unique_states,
                               callback_id, callback_timeout_at, callback_filter,
                               callback_on_complete, callback_on_fail, callback_transform,
                               run_lease + 1, progress
                        FROM deleted
                        RETURNING run_lease
                    )
                    SELECT run_lease FROM moved
                    "#,
                )
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("re-arm must not fail");
            }
        }));
    }
    for worker in workers {
        worker.await.expect("worker task");
    }
    stop.notify_waiters();
    stop.notify_one();
    maintenance.await.expect("maintenance task");

    // Exactness after the churn: every job is back in jobs_hot as running.
    awa::model::admin::flush_dirty_admin_metadata(&pool)
        .await
        .unwrap();
    for queue in &queues {
        let cached = setup::queue_state_counts(&pool, queue).await;
        assert_eq!(setup::state_count(&cached, "running"), 8, "queue {queue}");
    }

    sqlx::query("DELETE FROM awa.jobs_hot WHERE id = ANY($1)")
        .bind(&job_ids)
        .execute(&pool)
        .await
        .unwrap();
}
