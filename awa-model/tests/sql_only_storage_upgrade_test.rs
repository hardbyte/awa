//! Proves an external operator can drive a canonical → queue-storage
//! transition using only SQL function calls — no Rust code path needs
//! to run any DDL, and no Rust wrapper needs to be involved beyond
//! `awa migrate` (which is itself just a SQL applier).
//!
//! This is the "external migration tooling" contract that
//! `docs/queue-storage-substrate.md` describes. The substrate DDL
//! ships in the migration set (v023 calls
//! `awa.install_queue_storage_substrate('awa')`), and the staged
//! transition is driven by the SQL functions defined in v010/v013/v014.
//! Tests below call those functions directly via raw SQL.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use uuid::Uuid;

// Storage-transition state is a singleton — serialise tests that
// mutate it so they don't stomp each other when run with `--test-threads`.
static TRANSITION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn migrated_pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url())
        .await
        .expect("connect");
    awa_model::migrations::run(&pool)
        .await
        .expect("run migrations");
    pool
}

/// Clear every canonical source the transition gates inspect:
///
/// - `awa.jobs_hot` and `awa.scheduled_jobs`, because `awa.jobs` is
///   `jobs_hot UNION ALL scheduled_jobs` and
///   `storage_auto_finalize_if_fresh` counts rows from the view.
/// - `awa.scheduled_jobs` additionally, because
///   `awa.canonical_live_backlog()` (the finalize gate) sums
///   non-terminal `jobs_hot` rows + the full count of `scheduled_jobs`.
///
/// A leftover scheduled row from another test run can make the
/// fresh-install test report `auto_finalize = false` or the upgrade
/// test report a non-zero backlog after the simulated drain.
async fn clear_canonical_work(pool: &PgPool) {
    sqlx::query("DELETE FROM awa.jobs_hot")
        .execute(pool)
        .await
        .expect("clear awa.jobs_hot");
    sqlx::query("DELETE FROM awa.scheduled_jobs")
        .execute(pool)
        .await
        .expect("clear awa.scheduled_jobs");
}

async fn reset_transition_state(pool: &PgPool) {
    let mut tx = pool.begin().await.expect("begin reset tx");
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
    .expect("reset transition state");
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&mut *tx)
        .await
        .expect("clear runtime_storage_backends");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&mut *tx)
        .await
        .expect("clear runtime_instances");
    tx.commit().await.expect("commit reset tx");
}

async fn read_status(pool: &PgPool) -> (String, String, Option<String>) {
    let row: (String, String, Option<String>) =
        sqlx::query_as("SELECT state, current_engine, prepared_engine FROM awa.storage_status()")
            .fetch_one(pool)
            .await
            .expect("read storage_status");
    row
}

/// Stamp a synthetic `queue_storage_target` runtime row so the
/// mixed-transition gate (`storage_enter_mixed_transition`) sees an
/// executor ready to take queue-storage work after the routing flip.
/// In a real upgrade the operator brings up a worker with
/// `transition_role=queue_storage_target`; for an SQL-only test we
/// synthesise the row directly.
async fn stamp_queue_storage_target_runtime(pool: &PgPool) -> Uuid {
    let instance_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id,
            hostname,
            pid,
            version,
            storage_capability,
            transition_role,
            started_at,
            last_seen_at,
            snapshot_interval_ms,
            healthy,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader,
            global_max_workers,
            queues,
            queue_descriptor_hashes,
            job_kind_descriptor_hashes
        )
        VALUES (
            $1, 'sql-only-upgrade-test', 0, '0.0.0-test',
            'queue_storage', 'queue_storage_target',
            now(), now(), 5000, TRUE, TRUE, TRUE, TRUE, TRUE,
            FALSE, FALSE, 1,
            '[]'::jsonb, '[]'::jsonb, '[]'::jsonb
        )
        "#,
    )
    .bind(instance_id)
    .execute(pool)
    .await
    .expect("stamp queue_storage_target runtime");
    instance_id
}

/// Upgrade-from-canonical path: existing deployment has canonical jobs,
/// so `storage_auto_finalize_if_fresh` returns FALSE and the operator
/// must drive the staged transition. This test calls the SQL functions
/// directly — no Rust wrappers — to prove external migration tooling can
/// orchestrate the upgrade without any worker DDL.
#[tokio::test]
async fn external_tooling_can_upgrade_canonical_to_queue_storage_via_sql() {
    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    reset_transition_state(&pool).await;
    clear_canonical_work(&pool).await;

    // Defeat auto-finalize: ensure there's at least one canonical row
    // so `storage_auto_finalize_if_fresh` would refuse to short-circuit.
    sqlx::query(
        "INSERT INTO awa.jobs_hot (kind, queue, args) \
         VALUES ('sql_only_upgrade_marker', 'sql_only_upgrade', '{}'::jsonb)",
    )
    .execute(&pool)
    .await
    .expect("insert canonical marker");

    let target_instance = stamp_queue_storage_target_runtime(&pool).await;

    // Drive the staged transition via raw SQL — no awa_model::storage
    // wrappers, no Rust DDL. This mirrors what an external migration
    // tool would do as a post-DDL hook.
    sqlx::query("SELECT awa.storage_prepare($1, $2)")
        .bind("queue_storage")
        .bind(serde_json::json!({"schema": "awa"}))
        .execute(&pool)
        .await
        .expect("storage_prepare via raw SQL");

    let (state_after_prepare, _, prepared_after_prepare) = read_status(&pool).await;
    assert_eq!(state_after_prepare, "prepared");
    assert_eq!(prepared_after_prepare.as_deref(), Some("queue_storage"));

    sqlx::query("SELECT awa.storage_enter_mixed_transition()")
        .execute(&pool)
        .await
        .expect("storage_enter_mixed_transition via raw SQL");

    let (state_after_enter, _, _) = read_status(&pool).await;
    assert_eq!(state_after_enter, "mixed_transition");

    // `storage_finalize` refuses to advance while canonical live work
    // remains. In a real upgrade the operator waits in mixed_transition
    // for workers to drain; the test clears all canonical sources
    // (jobs_hot + scheduled_jobs, since `canonical_live_backlog()`
    // sums both) to simulate that drain, then asserts the two
    // documented SQL gates explicitly.
    clear_canonical_work(&pool).await;

    let backlog: i64 = sqlx::query_scalar("SELECT awa.canonical_live_backlog()")
        .fetch_one(&pool)
        .await
        .expect("canonical_live_backlog");
    assert_eq!(
        backlog, 0,
        "documented SQL gate must report empty backlog before finalize"
    );

    // Second documented gate: no live canonical-only runtimes. Drain-only
    // runtimes have no supported source of new canonical work once the
    // backlog is empty, so v040 deliberately permits them to remain.
    let drain_instance = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version, storage_capability,
            transition_role, started_at, last_seen_at, snapshot_interval_ms,
            healthy, postgres_connected, poll_loop_alive, heartbeat_alive,
            maintenance_alive, shutting_down, leader, global_max_workers,
            queues, queue_descriptor_hashes, job_kind_descriptor_hashes
        )
        SELECT
            $1, 'sql-only-drain-test', 1, version, 'canonical_drain_only',
            'auto', now(), now(), snapshot_interval_ms,
            healthy, postgres_connected, poll_loop_alive, heartbeat_alive,
            maintenance_alive, shutting_down, FALSE, global_max_workers,
            queues, queue_descriptor_hashes, job_kind_descriptor_hashes
        FROM awa.runtime_instances
        WHERE instance_id = $2
        "#,
    )
    .bind(drain_instance)
    .bind(target_instance)
    .execute(&pool)
    .await
    .expect("stamp canonical_drain_only runtime");
    let live_canonical: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.runtime_instances \
         WHERE storage_capability = 'canonical' \
           AND last_seen_at + make_interval( \
                 secs => GREATEST(((GREATEST(snapshot_interval_ms, 1000) / 1000) * 3)::int, 30) \
               ) >= now()",
    )
    .fetch_one(&pool)
    .await
    .expect("count live canonical runtimes");
    assert_eq!(
        live_canonical, 0,
        "documented SQL gate must report no live canonical-only runtimes before finalize"
    );

    sqlx::query("SELECT awa.storage_finalize()")
        .execute(&pool)
        .await
        .expect("storage_finalize via raw SQL");

    let (final_state, final_engine, _) = read_status(&pool).await;
    assert_eq!(final_state, "active");
    assert_eq!(final_engine, "queue_storage");
    let live_drain: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.runtime_instances \
         WHERE storage_capability = 'canonical_drain_only'",
    )
    .fetch_one(&pool)
    .await
    .expect("count drain-only runtimes after finalize");
    assert_eq!(live_drain, 1);

    // Cleanup so other tests aren't poisoned.
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(target_instance)
        .execute(&pool)
        .await
        .expect("cleanup runtime row");
    reset_transition_state(&pool).await;
}

/// Fresh-install path: empty `awa.jobs` and no live runtimes means
/// `storage_auto_finalize_if_fresh` is allowed to jump straight from
/// canonical to active. The function carries `GRANT EXECUTE ... TO
/// PUBLIC` (v013), so the EXECUTE bit is open to any role; the
/// function is `SECURITY INVOKER` and still reads/writes
/// `storage_transition_state`, `jobs`, `runtime_instances`, and
/// `runtime_storage_backends`, so a non-owner caller also needs the
/// normal table privileges. This test calls it via raw SQL with no
/// Rust runtime involvement.
#[tokio::test]
async fn external_tooling_can_finalize_fresh_install_via_sql() {
    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    reset_transition_state(&pool).await;
    clear_canonical_work(&pool).await;

    let promoted: bool = sqlx::query_scalar("SELECT awa.storage_auto_finalize_if_fresh($1)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .expect("auto-finalize via raw SQL");
    assert!(promoted, "fresh install should auto-finalize to active");

    let (state, engine, _) = read_status(&pool).await;
    assert_eq!(state, "active");
    assert_eq!(engine, "queue_storage");

    reset_transition_state(&pool).await;
}

/// Seed a canonical `running` row directly in `awa.jobs_hot`, mimicking a
/// claimed attempt (the insert trigger creates its unique claim).
async fn seed_canonical_running_job(pool: &PgPool, unique_key: &[u8], run_lease: i64) -> i64 {
    sqlx::query_scalar(
        r#"
        INSERT INTO awa.jobs_hot (
            kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, attempted_at, created_at, errors, metadata,
            tags, unique_key, unique_states, run_lease
        )
        VALUES (
            'reschedule_test', 'reschedule_q', '{"n":1}'::jsonb, 'running', 2, 1, 25,
            now(), now(), now(), now(), ARRAY['{"error":"seed"}'::jsonb],
            '{"tenant":"t1"}'::jsonb, ARRAY['tagged'], $1, B'11111111', $2
        )
        RETURNING id
        "#,
    )
    .bind(unique_key.to_vec())
    .bind(run_lease)
    .fetch_one(pool)
    .await
    .expect("seed canonical running job")
}

async fn claim_holder(pool: &PgPool, unique_key: &[u8]) -> Option<i64> {
    sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(unique_key.to_vec())
        .fetch_optional(pool)
        .await
        .expect("read unique claim")
}

/// #456: `awa_model::reschedule` keeps snooze/retry re-schedules
/// canonical while the cluster is canonical, and moves them to the active
/// queue-storage schema's `deferred_jobs` once routing has flipped — so the
/// canonical drain converges even for handlers that snooze on every run.
#[tokio::test]
async fn reschedule_canonical_attempt_routes_by_transition_state() {
    use awa_model::reschedule::{reschedule_canonical_attempt, Reschedule, RescheduleOutcome};

    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    clear_canonical_work(&pool).await;
    sqlx::query("DELETE FROM awa.deferred_jobs")
        .execute(&pool)
        .await
        .expect("clear deferred_jobs");
    sqlx::query("DELETE FROM awa.job_unique_claims")
        .execute(&pool)
        .await
        .expect("clear unique claims");
    reset_transition_state(&pool).await;

    // ── Canonical routing: snooze re-schedules under the existing id ──
    let key1 = b"reschedule-key-1";
    let job1 = seed_canonical_running_job(&pool, key1, 3).await;

    // Wrong run_lease loses the guard.
    let stale = reschedule_canonical_attempt(
        &pool,
        job1,
        99,
        Reschedule::Snooze { delay_secs: 60.0 },
        None,
        None,
    )
    .await
    .expect("stale reschedule call");
    assert!(matches!(stale, RescheduleOutcome::Stale));

    let outcome = reschedule_canonical_attempt(
        &pool,
        job1,
        3,
        Reschedule::Snooze { delay_secs: 60.0 },
        None,
        None,
    )
    .await
    .expect("canonical snooze");
    match outcome {
        RescheduleOutcome::Rescheduled {
            job_id, attempt, ..
        } => {
            assert_eq!(job_id, job1, "canonical snooze keeps the job id");
            assert_eq!(attempt, 0, "snooze does not count the attempt");
        }
        other => panic!("expected Rescheduled, got {other:?}"),
    }
    let (state, count): (String, i64) = sqlx::query_as(
        "SELECT state::text, count(*) OVER () FROM awa.scheduled_jobs WHERE id = $1",
    )
    .bind(job1)
    .fetch_one(&pool)
    .await
    .expect("scheduled row after canonical snooze");
    assert_eq!((state.as_str(), count), ("scheduled", 1));
    assert_eq!(claim_holder(&pool, key1).await, Some(job1));

    // ── Flip to mixed transition ──
    sqlx::query("SELECT awa.storage_prepare('queue_storage', '{\"schema\": \"awa\"}'::jsonb)")
        .execute(&pool)
        .await
        .expect("storage_prepare");
    // `prepared` must still route canonical: migrating before the routing flip
    // would put rows in a schema that is not yet authoritative and would break
    // the `storage abort` rollback interlock, which requires the queue-storage
    // tables to be empty.
    let key_prepared = b"reschedule-prepared-key";
    let job_prepared = seed_canonical_running_job(&pool, key_prepared, 9).await;
    let outcome = reschedule_canonical_attempt(
        &pool,
        job_prepared,
        9,
        Reschedule::Snooze { delay_secs: 60.0 },
        None,
        None,
    )
    .await
    .expect("prepared-state snooze");
    assert!(
        matches!(outcome, RescheduleOutcome::Rescheduled { .. }),
        "state=prepared must route canonical, got {outcome:?}"
    );
    let deferred_before_flip: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.deferred_jobs")
        .fetch_one(&pool)
        .await
        .expect("count deferred before flip");
    assert_eq!(
        deferred_before_flip, 0,
        "nothing may reach queue storage before the routing flip"
    );

    stamp_queue_storage_target_runtime(&pool).await;
    sqlx::query("SELECT awa.storage_enter_mixed_transition()")
        .execute(&pool)
        .await
        .expect("storage_enter_mixed_transition");

    // ── Migrated routing: retry backoff leaves the canonical plane ──
    // `awa.jobs_id_seq` (canonical) and `awa.job_id_seq` (queue storage) are
    // independent and can hand out the same number, so advance the
    // queue-storage sequence: the id assertions below then actually prove the
    // successor was allocated from the queue-storage id space rather than
    // passing on a coincidence.
    sqlx::query("SELECT setval('awa.job_id_seq', 500000)")
        .execute(&pool)
        .await
        .expect("advance queue-storage id sequence");
    let key2 = b"reschedule-key-2";
    let job2 = seed_canonical_running_job(&pool, key2, 7).await;
    let error_entry = serde_json::json!({ "error": "boom", "attempt": 1 });
    let outcome = reschedule_canonical_attempt(
        &pool,
        job2,
        7,
        Reschedule::RetryBackoff,
        Some(&error_entry),
        Some(&serde_json::json!({ "step": 2 })),
    )
    .await
    .expect("migrated retry backoff");
    let new_id = match outcome {
        RescheduleOutcome::Migrated {
            job_id, attempt, ..
        } => {
            assert_ne!(job_id, job2, "migrated successor gets a fresh id");
            assert!(
                job_id > 500_000,
                "successor id must come from the queue-storage sequence, got {job_id}"
            );
            assert_eq!(attempt, 1, "retry backoff preserves the attempt count");
            job_id
        }
        other => panic!("expected Migrated, got {other:?}"),
    };

    let canonical_left: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.jobs_hot WHERE id = $1 \
         UNION ALL SELECT count(*)::bigint FROM awa.scheduled_jobs WHERE id = $1 \
         ORDER BY 1 DESC LIMIT 1",
    )
    .bind(job2)
    .fetch_one(&pool)
    .await
    .expect("canonical remnants");
    assert_eq!(canonical_left, 0, "migrated job left the canonical plane");

    let (state, attempt, run_lease, payload): (String, i16, i64, serde_json::Value) =
        sqlx::query_as(
            "SELECT state::text, attempt, run_lease, payload \
             FROM awa.deferred_jobs WHERE job_id = $1",
        )
        .bind(new_id)
        .fetch_one(&pool)
        .await
        .expect("deferred successor row");
    assert_eq!(state, "retryable");
    assert_eq!(attempt, 1);
    assert_eq!(run_lease, 7, "run_lease carries over to the successor");
    let errors = payload["errors"].as_array().expect("payload errors array");
    assert_eq!(errors.len(), 2, "seeded error plus appended retry error");
    assert_eq!(errors[1]["error"], "boom");
    assert_eq!(payload["metadata"]["tenant"], "t1");
    assert_eq!(payload["tags"], serde_json::json!(["tagged"]));
    assert_eq!(payload["progress"]["step"], 2);
    assert_eq!(
        claim_holder(&pool, key2).await,
        Some(new_id),
        "unique claim follows the successor id"
    );

    // A late completion from the taken attempt is stale.
    let stale = reschedule_canonical_attempt(
        &pool,
        job2,
        7,
        Reschedule::RetryBackoff,
        Some(&error_entry),
        None,
    )
    .await
    .expect("stale post-migration call");
    assert!(matches!(stale, RescheduleOutcome::Stale));

    // ── Migrated routing: snooze does not count the attempt ──
    let key3 = b"reschedule-key-3";
    let job3 = seed_canonical_running_job(&pool, key3, 1).await;
    let outcome = reschedule_canonical_attempt(
        &pool,
        job3,
        1,
        Reschedule::Snooze {
            delay_secs: 86_400.0,
        },
        None,
        None,
    )
    .await
    .expect("migrated snooze");
    match outcome {
        RescheduleOutcome::Migrated { attempt, .. } => assert_eq!(attempt, 0),
        other => panic!("expected Migrated, got {other:?}"),
    }

    // The perpetual-snoozer scenario: every job re-scheduled rather than
    // completing, yet nothing added by the post-flip re-schedules remains on
    // the canonical plane. `job1` and `job_prepared` are excluded because they
    // re-scheduled *before* the routing flip and correctly stayed canonical.
    let backlog: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*)::bigint FROM awa.jobs_hot \
          WHERE state NOT IN ('completed','failed','cancelled') AND kind = 'reschedule_test') \
         + (SELECT count(*)::bigint FROM awa.scheduled_jobs \
            WHERE kind = 'reschedule_test' AND id <> ALL($1))",
    )
    .bind(vec![job1, job_prepared])
    .fetch_one(&pool)
    .await
    .expect("canonical backlog");
    assert_eq!(backlog, 0);

    // Cleanup: drive the transition to `active` rather than resetting to
    // `canonical` — a later `migrations::run` against this shared test
    // database would otherwise hit the ADR-037 unfinalized-cluster gate.
    sqlx::query("DELETE FROM awa.deferred_jobs WHERE kind = 'reschedule_test'")
        .execute(&pool)
        .await
        .expect("cleanup deferred");
    sqlx::query("DELETE FROM awa.job_unique_claims")
        .execute(&pool)
        .await
        .expect("cleanup claims");
    clear_canonical_work(&pool).await;
    sqlx::query("SELECT awa.storage_finalize()")
        .execute(&pool)
        .await
        .expect("finalize after reschedule test");
}

/// #456 (review follow-up): the reschedule transaction must lock the
/// transition singleton, so a concurrent `storage_abort` cannot validate the
/// queue-storage tables as empty and restore canonical routing between the
/// routing decision and the deferred insert — which would strand the job in a
/// schema that is no longer active.
#[tokio::test]
async fn reschedule_holds_transition_lock_against_concurrent_abort() {
    use awa_model::reschedule::{reschedule_canonical_attempt_tx, Reschedule, RescheduleOutcome};

    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    clear_canonical_work(&pool).await;
    sqlx::query("DELETE FROM awa.deferred_jobs")
        .execute(&pool)
        .await
        .expect("clear deferred_jobs");
    sqlx::query("DELETE FROM awa.job_unique_claims")
        .execute(&pool)
        .await
        .expect("clear unique claims");
    reset_transition_state(&pool).await;

    sqlx::query("SELECT awa.storage_prepare('queue_storage', '{\"schema\": \"awa\"}'::jsonb)")
        .execute(&pool)
        .await
        .expect("storage_prepare");
    stamp_queue_storage_target_runtime(&pool).await;
    sqlx::query("SELECT awa.storage_enter_mixed_transition()")
        .execute(&pool)
        .await
        .expect("storage_enter_mixed_transition");

    let key = b"reschedule-lock-key";
    let job_id = seed_canonical_running_job(&pool, key, 5).await;

    // Hold an in-flight reschedule open: it has taken the routing decision
    // and written the deferred successor, but has not committed.
    let mut tx = pool.begin().await.expect("begin reschedule tx");
    let outcome = reschedule_canonical_attempt_tx(
        &mut tx,
        job_id,
        5,
        Reschedule::Snooze {
            delay_secs: 3_600.0,
        },
        None,
        None,
    )
    .await
    .expect("reschedule inside held tx");
    assert!(matches!(outcome, RescheduleOutcome::Migrated { .. }));

    // A concurrent abort must block on the share lock rather than racing the
    // uncommitted insert. `lock_timeout` turns the block into a deterministic
    // error instead of a hang.
    let abort_result = async {
        let mut conn = pool.acquire().await.expect("abort connection");
        sqlx::query("SET lock_timeout = '750ms'")
            .execute(&mut *conn)
            .await
            .expect("set lock_timeout");
        sqlx::query("SELECT awa.storage_abort()")
            .execute(&mut *conn)
            .await
    }
    .await;
    let err = abort_result.expect_err("concurrent abort must not proceed past the share lock");
    let code = err
        .as_database_error()
        .and_then(|db| db.code())
        .map(|c| c.to_string())
        .unwrap_or_default();
    assert_eq!(
        code, "55P03",
        "abort should fail on lock_timeout while the reschedule holds the singleton, got: {err}"
    );

    tx.commit().await.expect("commit reschedule");

    // Cleanup, leaving the transition finalized (see the note in the
    // reschedule routing test above).
    sqlx::query("DELETE FROM awa.deferred_jobs WHERE kind = 'reschedule_test'")
        .execute(&pool)
        .await
        .expect("cleanup deferred");
    sqlx::query("DELETE FROM awa.job_unique_claims")
        .execute(&pool)
        .await
        .expect("cleanup claims");
    clear_canonical_work(&pool).await;
    sqlx::query("SELECT awa.storage_finalize()")
        .execute(&pool)
        .await
        .expect("finalize after lock test");
}

/// Drive the cluster to `mixed_transition` with queue storage on the default
/// `awa` schema, leaving the canonical plane empty.
async fn enter_mixed_transition_for_test(pool: &PgPool) {
    clear_canonical_work(pool).await;
    sqlx::query("DELETE FROM awa.deferred_jobs")
        .execute(pool)
        .await
        .expect("clear deferred_jobs");
    sqlx::query("DELETE FROM awa.job_unique_claims")
        .execute(pool)
        .await
        .expect("clear unique claims");
    reset_transition_state(pool).await;
    sqlx::query("SELECT awa.storage_prepare('queue_storage', '{\"schema\": \"awa\"}'::jsonb)")
        .execute(pool)
        .await
        .expect("storage_prepare");
    stamp_queue_storage_target_runtime(pool).await;
    sqlx::query("SELECT awa.storage_enter_mixed_transition()")
        .execute(pool)
        .await
        .expect("storage_enter_mixed_transition");
}

/// Leave the transition finalized and the planes empty (see the note in the
/// reschedule routing test).
async fn finalize_after_test(pool: &PgPool) {
    sqlx::query("DELETE FROM awa.deferred_jobs WHERE kind = 'reschedule_test'")
        .execute(pool)
        .await
        .expect("cleanup deferred");
    sqlx::query("DELETE FROM awa.job_unique_claims")
        .execute(pool)
        .await
        .expect("cleanup claims");
    clear_canonical_work(pool).await;
    sqlx::query("SELECT awa.storage_finalize()")
        .execute(pool)
        .await
        .expect("finalize after test");
}

/// Seed a canonical `running` row with an explicit `unique_states` mask.
/// `get_bit(mask, 0)` is the leftmost character, so `B'01111111'` claims every
/// state except `scheduled`.
async fn seed_canonical_running_job_with_mask(
    pool: &PgPool,
    unique_key: &[u8],
    run_lease: i64,
    mask: &str,
) -> i64 {
    sqlx::query_scalar(&format!(
        r#"
        INSERT INTO awa.jobs_hot (
            kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, attempted_at, created_at, errors, metadata,
            tags, unique_key, unique_states, run_lease
        )
        VALUES (
            'reschedule_test', 'reschedule_q', '{{}}'::jsonb, 'running', 2, 1, 25,
            now(), now(), now(), now(), ARRAY[]::jsonb[],
            '{{}}'::jsonb, ARRAY[]::text[], $1, B'{mask}', $2
        )
        RETURNING id
        "#
    ))
    .bind(unique_key.to_vec())
    .bind(run_lease)
    .fetch_one(pool)
    .await
    .expect("seed canonical running job with mask")
}

/// #456 corner cases on the migrated path: a `unique_states` mask that does
/// not claim the destination state must leave the successor unclaimed, a
/// newer duplicate already holding the key must keep it, and `RetryAfter` must
/// honour the caller's delay rather than computing backoff.
#[tokio::test]
async fn migrated_reschedule_handles_claim_masks_duplicates_and_delays() {
    use awa_model::reschedule::{reschedule_canonical_attempt, Reschedule, RescheduleOutcome};

    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    enter_mixed_transition_for_test(&pool).await;

    // ── Mask does not claim `scheduled`: successor stays unclaimed ──
    let key1 = b"reschedule-mask-key";
    let job1 = seed_canonical_running_job_with_mask(&pool, key1, 2, "01111111").await;
    assert_eq!(
        claim_holder(&pool, key1).await,
        Some(job1),
        "running is inside the mask, so the seeded job holds the claim"
    );
    let outcome = reschedule_canonical_attempt(
        &pool,
        job1,
        2,
        Reschedule::Snooze { delay_secs: 60.0 },
        None,
        None,
    )
    .await
    .expect("masked snooze");
    assert!(matches!(outcome, RescheduleOutcome::Migrated { .. }));
    assert_eq!(
        claim_holder(&pool, key1).await,
        None,
        "scheduled is outside the mask, so no claim should survive the move"
    );

    // ── A newer duplicate holds the key: the holder wins ──
    let key2 = b"reschedule-dup-key";
    let job2 = seed_canonical_running_job_with_mask(&pool, key2, 4, "11111111").await;
    let decoy_id: i64 = 987_654;
    sqlx::query("UPDATE awa.job_unique_claims SET job_id = $1 WHERE unique_key = $2")
        .bind(decoy_id)
        .bind(key2.to_vec())
        .execute(&pool)
        .await
        .expect("simulate a newer duplicate taking the claim");
    let outcome = reschedule_canonical_attempt(
        &pool,
        job2,
        4,
        Reschedule::Snooze { delay_secs: 60.0 },
        None,
        None,
    )
    .await
    .expect("duplicate-claim snooze");
    let successor2 = match outcome {
        RescheduleOutcome::Migrated { job_id, .. } => job_id,
        other => panic!("expected Migrated, got {other:?}"),
    };
    assert_eq!(
        claim_holder(&pool, key2).await,
        Some(decoy_id),
        "the duplicate holder keeps the claim; the successor proceeds unclaimed"
    );
    let deferred_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM awa.deferred_jobs WHERE job_id = $1)")
            .bind(successor2)
            .fetch_one(&pool)
            .await
            .expect("successor row exists");
    assert!(
        deferred_exists,
        "a lost claim race must not lose the job itself"
    );

    // ── RetryAfter honours the caller delay; heartbeat/deadline cleared ──
    let key3 = b"reschedule-retryafter-key";
    let job3 = seed_canonical_running_job_with_mask(&pool, key3, 6, "11111111").await;
    let before = chrono::Utc::now();
    let outcome = reschedule_canonical_attempt(
        &pool,
        job3,
        6,
        Reschedule::RetryAfter { delay_secs: 900.0 },
        None,
        None,
    )
    .await
    .expect("migrated retry-after");
    let (successor3, run_at, attempt3) = match outcome {
        RescheduleOutcome::Migrated {
            job_id,
            run_at,
            attempt,
        } => (job_id, run_at, attempt),
        other => panic!("expected Migrated, got {other:?}"),
    };
    assert_eq!(attempt3, 1, "RetryAfter must not decrement the attempt");
    let delay = run_at - before;
    assert!(
        delay >= chrono::TimeDelta::seconds(890) && delay <= chrono::TimeDelta::seconds(960),
        "RetryAfter must schedule at ~now + delay, got {delay:?}"
    );
    // `deferred_jobs` has no heartbeat_at / deadline_at columns at all, so a
    // migrated successor structurally cannot inherit the finished attempt's
    // liveness fields; only the terminal-close stamp is asserted here.
    let (state, finalized_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT state::text, finalized_at FROM awa.deferred_jobs WHERE job_id = $1")
            .bind(successor3)
            .fetch_one(&pool)
            .await
            .expect("retry-after successor row");
    assert_eq!(state, "retryable");
    assert!(
        finalized_at.is_some(),
        "a retryable successor records when the attempt closed"
    );

    finalize_after_test(&pool).await;
}

/// A missing transition singleton resolves to canonical routing rather than
/// erroring — `active_queue_storage_schema()` is NULL-safe and the locking
/// read must preserve that. Runs inside a rolled-back transaction so the
/// singleton is never actually removed.
#[tokio::test]
async fn reschedule_without_transition_singleton_routes_canonical() {
    use awa_model::reschedule::{reschedule_canonical_attempt_tx, Reschedule, RescheduleOutcome};

    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    clear_canonical_work(&pool).await;
    reset_transition_state(&pool).await;

    let key = b"reschedule-no-singleton-key";
    let job_id = seed_canonical_running_job_with_mask(&pool, key, 3, "11111111").await;

    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("DELETE FROM awa.storage_transition_state")
        .execute(&mut *tx)
        .await
        .expect("drop singleton inside tx");
    let outcome = reschedule_canonical_attempt_tx(
        &mut tx,
        job_id,
        3,
        Reschedule::Snooze { delay_secs: 30.0 },
        None,
        None,
    )
    .await
    .expect("reschedule with no singleton must not error");
    match outcome {
        RescheduleOutcome::Rescheduled {
            job_id: same_id, ..
        } => assert_eq!(same_id, job_id, "canonical routing keeps the id"),
        other => panic!("expected canonical Rescheduled, got {other:?}"),
    }
    tx.rollback().await.expect("rollback");

    clear_canonical_work(&pool).await;
    sqlx::query("DELETE FROM awa.job_unique_claims")
        .execute(&pool)
        .await
        .expect("cleanup claims");
}

/// #456 (review follow-up): a job carrying callback wiring must **not** migrate
/// cross-plane. `deferred_jobs` has no callback columns, so migrating one would
/// silently drop `callback_id` and the CEL expressions and leave the callback
/// unresolvable. Such a job keeps the canonical write instead.
#[tokio::test]
async fn callback_carrying_job_is_not_migrated_to_queue_storage() {
    use awa_model::reschedule::{reschedule_canonical_attempt, Reschedule, RescheduleOutcome};

    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    enter_mixed_transition_for_test(&pool).await;

    let callback_id = Uuid::new_v4();
    let job_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO awa.jobs_hot (
            kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, attempted_at, created_at, errors, metadata,
            tags, run_lease, callback_id, callback_on_complete
        )
        VALUES (
            'reschedule_test', 'reschedule_q', '{}'::jsonb, 'running', 2, 1, 25,
            now(), now(), now(), now(), ARRAY[]::jsonb[], '{}'::jsonb,
            ARRAY[]::text[], 8, $1, 'payload.done == true'
        )
        RETURNING id
        "#,
    )
    .bind(callback_id)
    .fetch_one(&pool)
    .await
    .expect("seed callback-carrying running job");

    let deferred_before: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.deferred_jobs")
        .fetch_one(&pool)
        .await
        .expect("count deferred before");

    let outcome = reschedule_canonical_attempt(
        &pool,
        job_id,
        8,
        Reschedule::Snooze { delay_secs: 60.0 },
        None,
        None,
    )
    .await
    .expect("callback-carrying snooze");
    match outcome {
        RescheduleOutcome::Rescheduled {
            job_id: same_id, ..
        } => assert_eq!(
            same_id, job_id,
            "a callback-carrying job stays canonical under its own id"
        ),
        other => panic!("expected canonical Rescheduled, got {other:?}"),
    }

    let deferred_after: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.deferred_jobs")
        .fetch_one(&pool)
        .await
        .expect("count deferred after");
    assert_eq!(
        deferred_before, deferred_after,
        "no queue-storage row may be created for a callback-carrying job"
    );

    // The callback wiring survived on the canonical successor.
    let (kept_id, kept_on_complete): (Option<Uuid>, Option<String>) = sqlx::query_as(
        "SELECT callback_id, callback_on_complete FROM awa.scheduled_jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("canonical successor row");
    assert_eq!(kept_id, Some(callback_id));
    assert_eq!(kept_on_complete.as_deref(), Some("payload.done == true"));

    // A genuinely stale completion is still reported as stale, not silently
    // re-scheduled by the canonical fallback.
    let stale = reschedule_canonical_attempt(
        &pool,
        job_id,
        999,
        Reschedule::Snooze { delay_secs: 60.0 },
        None,
        None,
    )
    .await
    .expect("stale call");
    assert!(matches!(stale, RescheduleOutcome::Stale));

    sqlx::query("DELETE FROM awa.scheduled_jobs WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("cleanup callback job");
    finalize_after_test(&pool).await;
}
