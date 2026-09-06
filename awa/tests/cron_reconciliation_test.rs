//! Isolated database tests for the owner protocol, independent of other suites'
//! runtime snapshots and deliberately stale legacy participants.
mod ci_timing;

use awa::model::{cron, cron_reconciliation as reconcile, migrations};
use awa::{PeriodicJob, PeriodicReconciliation};
use chrono::{Duration as ChronoDuration, Utc};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

fn job(name: &str) -> PeriodicJob {
    PeriodicJob::builder(name, "* * * * *")
        .build_raw("cron_test".into(), serde_json::json!({"b":2,"a":1}))
        .unwrap()
}
fn config(owner: &str, grace: u64) -> PeriodicReconciliation {
    PeriodicReconciliation::authoritative(owner, "rev-1", Duration::from_millis(grace)).unwrap()
}
async fn runtime(conn: &mut sqlx::PgConnection, id: Uuid, capable: bool) {
    sqlx::query("INSERT INTO awa.runtime_instances(instance_id,pid,version,started_at,last_seen_at,snapshot_interval_ms,healthy,postgres_connected,poll_loop_alive,heartbeat_alive,maintenance_alive,shutting_down,leader,cron_protocol) VALUES ($1,1,'test',clock_timestamp(),clock_timestamp(),1000,true,true,true,true,true,false,false,$2) ON CONFLICT(instance_id) DO UPDATE SET last_seen_at=clock_timestamp(),cron_protocol=EXCLUDED.cron_protocol")
        .bind(id).bind(if capable {Some(reconcile::CRON_PROTOCOL_VERSION)} else {None}).execute(conn).await.unwrap();
}
async fn publish(pool: &PgPool, id: Uuid, c: &PeriodicReconciliation, jobs: &[PeriodicJob]) {
    let manifest = reconcile::PeriodicManifest::new(jobs).unwrap();
    let mut tx = pool.begin().await.unwrap();
    reconcile::lock(&mut tx).await.unwrap();
    runtime(&mut tx, id, true).await;
    reconcile::publish(&mut tx, id, c, &manifest).await.unwrap();
    tx.commit().await.unwrap();
}
async fn get(pool: &PgPool, name: &str) -> cron::CronJobRow {
    cron::list_cron_jobs(pool)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.name == name)
        .unwrap()
}
async fn init(pool: &PgPool) {
    static MIGRATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = MIGRATE.lock().await;
    migrations::run(pool).await.unwrap();
}

#[sqlx::test]
async fn migration_drains_cron_before_locking_job_relations(pool: PgPool) {
    init(&pool).await;
    // Replay the released 0.6.7 pending range on an empty, rerunnable schema.
    sqlx::query("DELETE FROM awa.schema_version WHERE version > 40")
        .execute(&pool)
        .await
        .unwrap();
    let mut old_enqueue = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE awa.cron_jobs IN ROW EXCLUSIVE MODE")
        .execute(&mut *old_enqueue)
        .await
        .unwrap();
    let migrating_pool = pool.clone();
    let migrating = tokio::spawn(async move { migrations::run(&migrating_pool).await });
    let conflicting_locks: i64 = tokio::time::timeout(
        ci_timing::scaled_timeout(Duration::from_secs(30)),
        async {
            loop {
                let waiting: Option<i32> = sqlx::query_scalar(
                    "SELECT pid FROM pg_locks WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database()) AND relation = 'awa.cron_jobs'::regclass AND NOT granted LIMIT 1")
                    .fetch_optional(&pool).await.unwrap();
                if let Some(pid) = waiting {
                    break sqlx::query_scalar(
                        "SELECT count(*) FROM pg_locks WHERE pid = $1 AND granted AND mode = 'AccessExclusiveLock' AND relation IS NOT NULL AND relation <> 'awa.cron_jobs'::regclass")
                        .bind(pid).fetch_one(&pool).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    ).await.unwrap();
    old_enqueue.rollback().await.unwrap();
    migrating.await.unwrap().unwrap();
    assert_eq!(
        conflicting_locks, 0,
        "migration must drain old cron enqueues before holding job-relation DDL locks"
    );
}

#[test]
fn explicit_authority_and_canonical_manifest() {
    assert!(PeriodicReconciliation::authoritative(" ", "revision", Duration::ZERO).is_err());
    assert!(PeriodicReconciliation::authoritative("owner", "", Duration::ZERO).is_err());
    assert!(reconcile::canonical_manifest(&[job("a"), job("a")]).is_err());
    assert_eq!(
        reconcile::canonical_manifest(&[job("b"), job("a")]).unwrap(),
        reconcile::canonical_manifest(&[job("a"), job("b")]).unwrap()
    );
    let mut first = job("json-order");
    let mut second = first.clone();
    first.metadata = serde_json::from_str(r#"{"z":[{"b":2,"a":1}],"a":0}"#).unwrap();
    second.metadata = serde_json::from_str(r#"{"a":0,"z":[{"a":1,"b":2}]}"#).unwrap();
    assert_eq!(
        reconcile::canonical_manifest(&[first]).unwrap().1,
        reconcile::canonical_manifest(&[second]).unwrap().1
    );
    assert_ne!(
        reconcile::canonical_manifest(&[]).unwrap().1,
        reconcile::canonical_manifest(&[job("a")]).unwrap().1
    );
}

#[sqlx::test]
async fn explicit_empty_retires_only_its_owner(pool: PgPool) {
    init(&pool).await;
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    publish(&pool, a, &config("billing", 0), &[job("invoice")]).await;
    publish(&pool, b, &config("mail", 0), &[job("reminder")]).await;
    cron::upsert_cron_job(&pool, &job("unowned")).await.unwrap();
    publish(&pool, a, &config("billing", 0), &[]).await;
    let preview = reconcile::plan(&pool, "billing").await.unwrap();
    assert_eq!(preview.retirements, vec!["invoice"]);
    assert!(!preview.applied);
    assert!(get(&pool, "invoice").await.retired_at.is_none());
    assert!(
        reconcile::reconcile(&pool, "billing")
            .await
            .unwrap()
            .applied
    );
    assert!(get(&pool, "invoice").await.retired_at.is_some());
    assert!(get(&pool, "reminder").await.retired_at.is_none());
    assert!(get(&pool, "unowned").await.retired_at.is_none());
}

#[sqlx::test]
async fn ownership_and_retirement_require_explicit_actions(pool: PgPool) {
    init(&pool).await;
    cron::upsert_cron_job(&pool, &job("invoice")).await.unwrap();
    let id = Uuid::new_v4();
    publish(&pool, id, &config("billing", 0), &[job("invoice")]).await;
    assert!(!reconcile::plan(&pool, "billing")
        .await
        .unwrap()
        .conflicts
        .is_empty());
    let adopt = reconcile::CronOwnerAction::Adopt {
        name: "invoice".into(),
        owner_id: "billing".into(),
        expected_owner: None,
    };
    assert!(
        !reconcile::operate(&pool, adopt.clone(), "test", false)
            .await
            .unwrap()
            .applied
    );
    assert!(get(&pool, "invoice").await.owner_id.is_none());
    reconcile::operate(&pool, adopt.clone(), "test", true)
        .await
        .unwrap();
    assert!(reconcile::operate(&pool, adopt, "test", true)
        .await
        .is_err());
    let mut changed = job("invoice");
    changed.queue = "stolen".into();
    assert!(cron::upsert_cron_job(&pool, &changed).await.is_err());
    assert_eq!(get(&pool, "invoice").await.queue, "default");
    publish(&pool, Uuid::new_v4(), &config("intruder", 0), &[changed]).await;
    assert!(!reconcile::plan(&pool, "intruder")
        .await
        .unwrap()
        .conflicts
        .is_empty());
    reconcile::operate(
        &pool,
        reconcile::CronOwnerAction::Retire {
            name: "invoice".into(),
        },
        "test",
        true,
    )
    .await
    .unwrap();
    publish(&pool, id, &config("billing", 0), &[job("invoice")]).await;
    assert!(get(&pool, "invoice").await.retired_at.is_some());
    assert!(cron::delete_cron_job(&pool, "invoice").await.is_err());
    assert!(cron::trigger_cron_job(&pool, "invoice").await.is_err());
    assert!(cron::atomic_enqueue(&pool, "invoice", Utc::now(), None)
        .await
        .unwrap()
        .is_none());
    let before = Utc::now();
    reconcile::operate(
        &pool,
        reconcile::CronOwnerAction::Restore {
            name: "invoice".into(),
        },
        "test",
        true,
    )
    .await
    .unwrap();
    let row = get(&pool, "invoice").await;
    assert!(row.retired_at.is_none());
    assert!(row.last_enqueued_at.unwrap() >= before);
}

#[sqlx::test]
async fn transient_conflict_resets_grace_between_leader_passes(pool: PgPool) {
    init(&pool).await;
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = config("billing", 150);
    publish(&pool, a, &c, &[job("invoice")]).await;
    publish(&pool, b, &c, &[job("invoice")]).await;
    tokio::time::sleep(Duration::from_millis(170)).await;
    assert!(reconcile::plan(&pool, "billing")
        .await
        .unwrap()
        .blockers
        .is_empty());
    // The leader never observes the mixed window; publication must reset it.
    publish(&pool, b, &c, &[]).await;
    assert!(reconcile::plan(&pool, "billing")
        .await
        .unwrap()
        .blockers
        .contains(&"mixed_desired_manifests".into()));
    publish(&pool, a, &c, &[]).await;
    let p = reconcile::reconcile(&pool, "billing").await.unwrap();
    assert!(!p.applied);
    assert!(p.grace_remaining_ms > 0);
    tokio::time::sleep(Duration::from_millis(170)).await;
    assert!(
        reconcile::reconcile(&pool, "billing")
            .await
            .unwrap()
            .applied
    );
}

#[sqlx::test]
async fn unknown_runtime_and_outage_never_authorize_retirement(pool: PgPool) {
    init(&pool).await;
    let id = Uuid::new_v4();
    let old = Uuid::new_v4();
    publish(&pool, id, &config("billing", 0), &[job("invoice")]).await;
    runtime(&mut pool.acquire().await.unwrap(), old, false).await;
    publish(&pool, id, &config("billing", 0), &[]).await;
    let p = reconcile::reconcile(&pool, "billing").await.unwrap();
    assert_eq!(p.blocking_instances, vec![old]);
    assert!(!p.applied);
    sqlx::query(
        "UPDATE awa.runtime_instances SET last_seen_at=clock_timestamp()-interval '1 hour'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let p = reconcile::reconcile(&pool, "billing").await.unwrap();
    assert!(p.blockers.contains(&"zero_live_declarations".into()));
    assert!(get(&pool, "invoice").await.retired_at.is_none());
    reconcile::operate(
        &pool,
        reconcile::CronOwnerAction::RetireOwner {
            owner_id: "billing".into(),
        },
        "decommission",
        true,
    )
    .await
    .unwrap();
    assert!(get(&pool, "invoice").await.retired_at.is_some());
}

#[sqlx::test]
async fn conflicting_publication_serializes_before_reconciliation(pool: PgPool) {
    init(&pool).await;
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    publish(&pool, a, &config("billing", 0), &[job("invoice")]).await;
    publish(&pool, a, &config("billing", 0), &[]).await;
    let mut tx = pool.begin().await.unwrap();
    reconcile::lock(&mut tx).await.unwrap();
    runtime(&mut tx, b, true).await;
    reconcile::publish(
        &mut tx,
        b,
        &config("billing", 0),
        &reconcile::PeriodicManifest::new(&[job("invoice")]).unwrap(),
    )
    .await
    .unwrap();
    let other = pool.clone();
    let task = tokio::spawn(async move { reconcile::reconcile(&other, "billing").await.unwrap() });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!task.is_finished());
    tx.commit().await.unwrap();
    let result = task.await.unwrap();
    assert!(!result.applied);
    assert!(result.blockers.contains(&"mixed_desired_manifests".into()));
    assert!(get(&pool, "invoice").await.retired_at.is_none());
}

#[sqlx::test]
async fn snapshot_trigger_fences_legacy_publication(pool: PgPool) {
    init(&pool).await;
    let mut tx = pool.begin().await.unwrap();
    reconcile::lock(&mut tx).await.unwrap();
    let other = pool.clone();
    let task = tokio::spawn(async move {
        runtime(&mut other.acquire().await.unwrap(), Uuid::new_v4(), false).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!task.is_finished());
    tx.commit().await.unwrap();
    task.await.unwrap();
}

#[sqlx::test]
async fn migration_and_grace_survive_restarts_but_not_evidence_gaps(pool: PgPool) {
    init(&pool).await;
    let id = Uuid::new_v4();
    let c = config("billing", 100);
    publish(&pool, id, &c, &[job("invoice")]).await;
    migrations::run(&pool).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../awa-model/migrations/v045_cron_reconciliation.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(reconcile::plan(&pool, "billing")
        .await
        .unwrap()
        .blockers
        .is_empty());
    sqlx::query("UPDATE awa.cron_owners SET evidence_until=$1")
        .bind(Utc::now() - ChronoDuration::seconds(1))
        .execute(&pool)
        .await
        .unwrap();
    publish(&pool, id, &c, &[]).await;
    assert!(
        reconcile::plan(&pool, "billing")
            .await
            .unwrap()
            .grace_remaining_ms
            > 0
    );
}

#[sqlx::test]
async fn proposed_manifest_is_read_only_and_explains_all_changes(pool: PgPool) {
    init(&pool).await;
    let id = Uuid::new_v4();
    publish(
        &pool,
        id,
        &config("billing", 0),
        &[job("update"), job("remove")],
    )
    .await;
    cron::upsert_cron_job(&pool, &job("unowned")).await.unwrap();
    let before = reconcile::plan(&pool, "billing").await.unwrap();
    let mut updated = job("update");
    updated.queue = "new_queue".into();
    let preview = reconcile::preview(&pool, "billing", &[updated, job("add"), job("unowned")])
        .await
        .unwrap();
    assert_eq!(preview.additions, ["add"]);
    assert_eq!(preview.updates, ["update"]);
    assert_eq!(preview.retirements, ["remove"]);
    assert_eq!(preview.conflicts.len(), 1);
    assert!(preview.blockers.contains(&"manifest_not_published".into()));
    assert!(preview
        .blockers
        .contains(&"ownership_or_retirement_conflict".into()));
    assert!(!preview.applied);
    let after = reconcile::plan(&pool, "billing").await.unwrap();
    assert_eq!(before.desired_hash, after.desired_hash);
    assert_eq!(before.agreed_since, after.agreed_since);
    assert_eq!(get(&pool, "update").await.queue, "default");
    assert!(get(&pool, "remove").await.retired_at.is_none());
    assert_eq!(cron::list_cron_jobs(&pool).await.unwrap().len(), 3);
}

#[sqlx::test]
async fn failed_start_does_not_publish_authority(pool: PgPool) {
    init(&pool).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = awa::Client::builder(pool.clone())
        .queue("test", awa::QueueConfig::default())
        .periodic_reconciliation(config("failed-start", 0))
        .health_addr(occupied.local_addr().unwrap())
        .build()
        .unwrap();
    assert!(client
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("health listener"));
    assert!(!reconcile::owners(&pool)
        .await
        .unwrap()
        .contains(&"failed-start".into()));
    let declarations: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.cron_declarations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(declarations, 0);
}

/// Released executable built by scripts/rehearse-cron-ownership.sh. Exercises
/// released functions and a real old maintenance leader, not copied SQL.
#[sqlx::test]
#[ignore = "requires the released 0.6.7 probe; scripts/rehearse-cron-ownership.sh"]
async fn released_old_leader_cannot_fire_retired_schedule(pool: PgPool) {
    use sqlx::ConnectOptions;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let binary = std::env::var("AWA_CRON_N_MINUS_ONE_PROBE").expect("released probe path");
    let url = pool.connect_options().to_url_lossy().to_string();
    let invoke = |mode: &'static str| {
        let binary = binary.clone();
        let url = url.clone();
        async move {
            let output = tokio::process::Command::new(binary)
                .arg(mode)
                .env("DATABASE_URL", url)
                .output()
                .await
                .unwrap();
            assert!(
                output.status.success(),
                "released {mode}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        }
    };
    invoke("bootstrap").await;
    let mut old = tokio::process::Command::new(&binary)
        .arg("serve")
        .env("DATABASE_URL", &url)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = tokio::io::BufReader::new(old.stdout.take().unwrap());
    let mut ready = String::new();
    tokio::time::timeout(
        ci_timing::scaled_timeout(Duration::from_secs(30)),
        output.read_line(&mut ready),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(ready.contains("READY"), "{ready}");
    // Binary-first: opted-in current startup refuses before v045, without
    // partially publishing evidence or launching background services.
    let current = awa::Client::builder(pool.clone())
        .queue(
            "current_cron_481",
            awa::QueueConfig {
                max_workers: 1,
                ..Default::default()
            },
        )
        .periodic_reconciliation(config("compat-owner", 0))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .build()
        .unwrap();
    assert!(current.start().await.is_err());
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM awa.schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(version < migrations::CURRENT_VERSION);
    // Migrate while the released worker is live and taking real maintenance turns.
    migrations::run(&pool).await.unwrap();
    current.start().await.unwrap();
    let adopt = reconcile::CronOwnerAction::Adopt {
        name: "compat_cron_481".into(),
        owner_id: "compat-owner".into(),
        expected_owner: None,
    };
    reconcile::operate(&pool, adopt, "rehearsal", true)
        .await
        .unwrap();
    publish(&pool, Uuid::new_v4(), &config("compat-owner", 0), &[]).await;
    let plan = reconcile::reconcile(&pool, "compat-owner").await.unwrap();
    assert!(plan
        .blockers
        .contains(&"unsupported_runtime_protocol".into()));
    // Explicit operator retirement is allowed even while automatic retirement
    // is blocked. The live old leader must honor the database fence afterward.
    reconcile::operate(
        &pool,
        reconcile::CronOwnerAction::Retire {
            name: "compat_cron_481".into(),
        },
        "rehearsal",
        true,
    )
    .await
    .unwrap();
    let boundary = get(&pool, "compat_cron_481").await.last_enqueued_at;
    invoke("upsert").await;
    assert_eq!(
        get(&pool, "compat_cron_481").await.owner_id.as_deref(),
        Some("compat-owner")
    );
    assert!(invoke("enqueue").await.contains("\"enqueued\":false"));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        get(&pool, "compat_cron_481").await.last_enqueued_at,
        boundary
    );
    // Crash the leader, then exercise the actual released enqueue path again.
    old.kill().await.unwrap();
    old.wait().await.unwrap();
    assert!(invoke("enqueue").await.contains("\"enqueued\":false"));
    // A fresh released process returning as leader also cannot clear the row.
    let mut returning = tokio::process::Command::new(&binary)
        .arg("serve")
        .env("DATABASE_URL", &url)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut out = tokio::io::BufReader::new(returning.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(
        ci_timing::scaled_timeout(Duration::from_secs(30)),
        out.read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    if line.contains("READY") {
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            get(&pool, "compat_cron_481").await.last_enqueued_at,
            boundary
        );
        returning
            .stdin
            .take()
            .unwrap()
            .write_all(b"stop\n")
            .await
            .unwrap();
        assert!(returning.wait().await.unwrap().success());
    } else {
        let result = returning.wait_with_output().await.unwrap();
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(
            !result.status.success()
                && (error.contains("schema")
                    || error.contains("Schema")
                    || error.contains("ledger")),
            "unexpected old startup refusal: {error}"
        );
    }
    current.shutdown(Duration::from_secs(5)).await;
    println!(
        "{}",
        serde_json::json!({"protocol":"cron-ownership-v1","old_artifact":"awa=0.6.7","schema":migrations::CURRENT_VERSION,"live_old_leader_fenced":true,"released_upsert_preserves_tombstone":true,"released_atomic_enqueue_fenced":true})
    );
}

#[sqlx::test]
async fn review_mixed_manifests_do_not_flip_definitions(pool: PgPool) {
    init(&pool).await;
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = config("billing", 0);
    let mut old = job("invoice");
    old.cron_expr = "0 9 * * *".into();
    let mut new = old.clone();
    new.cron_expr = "0 10 * * *".into();
    publish(&pool, a, &c, &[old.clone()]).await;
    for _ in 0..2 {
        publish(&pool, b, &c, &[new.clone(), job("new-name")]).await;
        assert_eq!(get(&pool, "invoice").await.cron_expr, old.cron_expr);
        publish(&pool, a, &c, &[old.clone()]).await;
        assert_eq!(get(&pool, "invoice").await.cron_expr, old.cron_expr);
    }
    assert_eq!(
        get(&pool, "new-name").await.owner_id.as_deref(),
        Some("billing")
    );
    publish(&pool, a, &c, &[new.clone(), job("new-name")]).await;
    assert_eq!(get(&pool, "invoice").await.cron_expr, new.cron_expr);
}

#[sqlx::test]
async fn review_operator_action_preserves_unrelated_owner_and_row_locks(pool: PgPool) {
    init(&pool).await;
    publish(
        &pool,
        Uuid::new_v4(),
        &config("billing", 60_000),
        &[job("invoice")],
    )
    .await;
    publish(
        &pool,
        Uuid::new_v4(),
        &config("mail", 60_000),
        &[job("reminder")],
    )
    .await;
    let before: (Option<String>, Option<chrono::DateTime<Utc>>) = sqlx::query_as(
        "SELECT synced_hash,agreed_since FROM awa.cron_owners WHERE owner_id='mail'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    reconcile::operate(
        &pool,
        reconcile::CronOwnerAction::Retire {
            name: "invoice".into(),
        },
        "test",
        true,
    )
    .await
    .unwrap();
    let after = sqlx::query_as::<_, (Option<String>, Option<chrono::DateTime<Utc>>)>(
        "SELECT synced_hash,agreed_since FROM awa.cron_owners WHERE owner_id='mail'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        before, after,
        "billing action must not restart mail's agreement"
    );
    let mut row_lock = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM awa.cron_jobs WHERE name='reminder' FOR UPDATE")
        .execute(&mut *row_lock)
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        reconcile::operate(
            &pool,
            reconcile::CronOwnerAction::Restore {
                name: "invoice".into(),
            },
            "test",
            true,
        ),
    )
    .await
    .expect("unrelated cron row lock must not block billing action")
    .unwrap();
    row_lock.rollback().await.unwrap();
}

#[sqlx::test]
async fn review_retired_additive_registration_is_quiet(pool: PgPool) {
    init(&pool).await;
    cron::upsert_cron_job(&pool, &job("unowned")).await.unwrap();
    reconcile::operate(
        &pool,
        reconcile::CronOwnerAction::Retire {
            name: "unowned".into(),
        },
        "test",
        true,
    )
    .await
    .unwrap();
    for _ in 0..2 {
        cron::upsert_cron_job(&pool, &job("unowned"))
            .await
            .expect("retired additive schedule is an inert skip");
    }
    assert!(get(&pool, "unowned").await.retired_at.is_some());
    let error = cron::delete_cron_job(&pool, "unowned")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("retired"), "{error}");
    assert!(!error.contains("durable ownership"), "{error}");
}

#[sqlx::test]
async fn review_read_only_plans_do_not_wait_for_evidence_writer(pool: PgPool) {
    init(&pool).await;
    let mut writer = pool.begin().await.unwrap();
    reconcile::lock(&mut writer).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        reconcile::preview(&pool, "billing", &[job("invoice")]),
    )
    .await
    .expect("preview must read a committed snapshot without the evidence lock")
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), reconcile::plan(&pool, "billing"))
        .await
        .expect("plan must not block evidence publication")
        .unwrap();
    writer.rollback().await.unwrap();
}

#[sqlx::test]
async fn review_conflicts_never_persist_agreement_on_heartbeat(pool: PgPool) {
    init(&pool).await;
    cron::upsert_cron_job(&pool, &job("unowned")).await.unwrap();
    let id = Uuid::new_v4();
    for _ in 0..2 {
        publish(&pool, id, &config("billing", 60_000), &[job("unowned")]).await;
        let since: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar("SELECT agreed_since FROM awa.cron_owners WHERE owner_id='billing'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            since.is_none(),
            "a conflicting heartbeat cannot start agreement"
        );
        reconcile::reconcile(&pool, "billing").await.unwrap();
    }
}

#[sqlx::test]
async fn retired_desired_names_allow_startup_and_owner_restore(pool: PgPool) {
    init(&pool).await;
    let c = config("billing", 0);
    publish(
        &pool,
        Uuid::new_v4(),
        &c,
        &[job("invoice"), job("statement")],
    )
    .await;
    cron::pause_cron_job(&pool, "invoice", Some("test"))
        .await
        .unwrap();
    reconcile::operate(
        &pool,
        reconcile::CronOwnerAction::RetireOwner {
            owner_id: "billing".into(),
        },
        "test",
        true,
    )
    .await
    .unwrap();
    let client = awa::Client::builder(pool.clone())
        .queue("test", awa::QueueConfig::default())
        .periodic_reconciliation(c)
        .periodic(job("invoice"))
        .periodic(job("statement"))
        .build()
        .unwrap();
    client
        .start()
        .await
        .expect("a retired desired schedule stays inert without failing startup");
    client.shutdown(Duration::from_secs(2)).await;
    let plan = reconcile::plan(&pool, "billing").await.unwrap();
    assert!(plan
        .blockers
        .contains(&"ownership_or_retirement_conflict".into()));
    assert!(plan.agreed_since.is_none());
    let action = reconcile::CronOwnerAction::RestoreOwner {
        owner_id: "billing".into(),
    };
    let preview = reconcile::operate(&pool, action.clone(), "test", false)
        .await
        .unwrap();
    assert_eq!(preview.schedules, ["invoice", "statement"]);
    assert!(get(&pool, "invoice").await.retired_at.is_some());
    let restored_after: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    reconcile::operate(&pool, action, "test", true)
        .await
        .unwrap();
    for name in ["invoice", "statement"] {
        let row = get(&pool, name).await;
        assert!(row.retired_at.is_none());
        assert!(row.last_enqueued_at.unwrap() >= restored_after);
    }
    assert!(get(&pool, "invoice").await.paused_at.is_some());
}

#[sqlx::test]
async fn protocol_capability_matches_schema_and_legacy_reset_is_idempotent(pool: PgPool) {
    init(&pool).await;
    let schema_version: i32 = sqlx::query_scalar("SELECT awa.cron_protocol_version()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(schema_version, reconcile::CRON_PROTOCOL_VERSION);
    publish(
        &pool,
        Uuid::new_v4(),
        &config("billing", 60_000),
        &[job("invoice")],
    )
    .await;
    let old = Uuid::new_v4();
    let mut conn = pool.acquire().await.unwrap();
    runtime(&mut conn, old, false).await;
    let version: String =
        sqlx::query_scalar("SELECT xmin::text FROM awa.cron_owners WHERE owner_id='billing'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    runtime(&mut conn, old, false).await;
    let repeated: String =
        sqlx::query_scalar("SELECT xmin::text FROM awa.cron_owners WHERE owner_id='billing'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(
        version, repeated,
        "already cleared agreement must not generate another dead tuple"
    );
}
