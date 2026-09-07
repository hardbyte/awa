//! The role-free transition must work on an unfinalized 0.6 schema, without DDL.
use awa_model::{migrations, storage};
use serde_json::json;
use sqlx::PgPool;

async fn prepared(pool: &PgPool) {
    // Stop at the 0.6 migration ceiling: neither fix may require a 0.7 migration.
    for (_, _, sql) in migrations::migration_sql_range(0, 40) {
        sqlx::raw_sql(awa_model::audited_sql(&sql))
            .execute(pool)
            .await
            .unwrap();
    }
    storage::prepare(pool, "queue_storage", json!({"schema": "awa"}))
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn quiesced_transition_observes_inflight_snapshot(pool: PgPool) {
    prepared(&pool).await;
    let mut writer = pool.begin().await.unwrap();
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(writer.as_mut())
        .await
        .unwrap();
    sqlx::query("INSERT INTO awa.runtime_instances(instance_id,pid,version,started_at,last_seen_at,snapshot_interval_ms,healthy,postgres_connected,poll_loop_alive,heartbeat_alive,maintenance_alive,shutting_down,leader,storage_capability,transition_role) VALUES ($1,1,'0.6.6',now(),now(),1000,true,true,true,true,true,false,false,'queue_storage','auto')")
        .bind(uuid::Uuid::new_v4()).execute(writer.as_mut()).await.unwrap();
    let flip_pool = pool.clone();
    let flip =
        tokio::spawn(async move { storage::enter_mixed_transition_quiesced(&flip_pool).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(writer_pid)
            .fetch_one(&pool)
            .await
            .unwrap();
            if blocked {
                break;
            }
            assert!(!flip.is_finished(), "flip passed an uncommitted snapshot");
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    writer.commit().await.unwrap();
    let error = flip.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("fresh runtime"), "{error}");
    assert_eq!(storage::status(&pool).await.unwrap().state, "prepared");
}

#[sqlx::test(migrations = false)]
async fn quiesced_transition_requires_materialized_schema(pool: PgPool) {
    prepared(&pool).await;
    // prepare records the target but does not install a custom substrate.
    storage::prepare(
        &pool,
        "queue_storage",
        json!({"schema":"missing_substrate"}),
    )
    .await
    .unwrap();
    assert!(storage::enter_mixed_transition_quiesced(&pool)
        .await
        .unwrap_err()
        .to_string()
        .contains("not prepared"));
    assert_eq!(storage::status(&pool).await.unwrap().state, "prepared");
}

#[sqlx::test(migrations = false)]
async fn quiesced_transition_without_witness(pool: PgPool) {
    prepared(&pool).await;
    assert!(storage::enter_mixed_transition(&pool).await.is_err());
    let before = storage::status(&pool).await.unwrap();
    let after = storage::enter_mixed_transition_quiesced(&pool)
        .await
        .unwrap();
    assert_eq!(after.state, "mixed_transition");
    assert_eq!(after.active_engine, "queue_storage");
    assert_eq!(after.current_engine, "canonical");
    assert_eq!(after.transition_epoch, before.transition_epoch + 1);
    assert!(storage::enter_mixed_transition_quiesced(&pool)
        .await
        .is_err());
    assert_eq!(storage::status(&pool).await.unwrap(), after);
    assert_eq!(storage::finalize(&pool).await.unwrap().state, "active");
}

#[sqlx::test(migrations = false)]
async fn quiesced_transition_refuses_every_fresh_runtime(pool: PgPool) {
    prepared(&pool).await;
    let id = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO awa.runtime_instances(instance_id,pid,version,started_at,last_seen_at,snapshot_interval_ms,healthy,postgres_connected,poll_loop_alive,heartbeat_alive,maintenance_alive,shutting_down,leader,storage_capability,transition_role) VALUES ($1,1,'0.6.6',now(),now(),1000,false,false,false,false,false,true,false,'queue_storage','auto')")
        .bind(id).execute(&pool).await.unwrap();
    let before = storage::status(&pool).await.unwrap();
    for capability in ["canonical", "canonical_drain_only", "queue_storage"] {
        sqlx::query("UPDATE awa.runtime_instances SET storage_capability=$1")
            .bind(capability)
            .execute(&pool)
            .await
            .unwrap();
        let error = storage::enter_mixed_transition_quiesced(&pool)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fresh runtime"), "{error}");
        assert_eq!(storage::status(&pool).await.unwrap(), before);
    }
    sqlx::query("UPDATE awa.runtime_instances SET last_seen_at=now()-interval '1 hour'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        storage::enter_mixed_transition_quiesced(&pool)
            .await
            .unwrap()
            .state,
        "mixed_transition"
    );
}

#[sqlx::test(migrations = false)]
async fn quiesced_transition_preserves_backlog_and_finalize_gate(pool: PgPool) {
    prepared(&pool).await;
    sqlx::query(
        "INSERT INTO awa.jobs(kind,args,queue) VALUES ('upgrade_probe','{}','upgrade_probe')",
    )
    .execute(&pool)
    .await
    .unwrap();
    storage::enter_mixed_transition_quiesced(&pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT awa.canonical_live_backlog()")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(storage::finalize(&pool).await.is_err());
}

#[sqlx::test(migrations = false)]
async fn quiesced_transition_rejects_partial_substrate(pool: PgPool) {
    prepared(&pool).await;
    sqlx::raw_sql("CREATE SCHEMA partial_substrate; CREATE TABLE partial_substrate.queue_ring_state(id int); CREATE TABLE partial_substrate.ready_entries(id int); CREATE TABLE partial_substrate.leases(id int)")
        .execute(&pool).await.unwrap();
    storage::prepare(
        &pool,
        "queue_storage",
        json!({"schema":"partial_substrate"}),
    )
    .await
    .unwrap();
    assert!(
        !storage::queue_storage_schema_ready(&pool, "partial_substrate")
            .await
            .unwrap()
    );
    let before = storage::status(&pool).await.unwrap();
    assert!(storage::enter_mixed_transition_quiesced(&pool)
        .await
        .is_err());
    assert_eq!(storage::status(&pool).await.unwrap(), before);
}

#[sqlx::test(migrations = false)]
async fn quiesced_transition_preserves_snapshot_milliseconds(pool: PgPool) {
    prepared(&pool).await;
    // 91 seconds is stale with integer truncation (90s), but fresh at 92.997s.
    sqlx::query("INSERT INTO awa.runtime_instances(instance_id,pid,version,started_at,last_seen_at,snapshot_interval_ms,healthy,postgres_connected,poll_loop_alive,heartbeat_alive,maintenance_alive,shutting_down,leader,storage_capability,transition_role) VALUES ($1,1,'0.6.6',now(),clock_timestamp()-interval '91 seconds',30999,true,true,true,true,true,false,false,'queue_storage','auto')")
        .bind(uuid::Uuid::new_v4()).execute(&pool).await.unwrap();
    assert!(storage::enter_mixed_transition_quiesced(&pool)
        .await
        .is_err());
    assert_eq!(storage::status(&pool).await.unwrap().state, "prepared");
}
