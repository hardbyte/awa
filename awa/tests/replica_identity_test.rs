//! Every awa table must stay writable when it is in a logical-replication
//! publication (v047). PostgreSQL refuses UPDATE and DELETE on a published
//! table that has no replica identity, so each table needs a primary key, a
//! replica-identity index, or REPLICA IDENTITY FULL — including the
//! partitions the queue-storage installer creates.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::audited_sql;
use awa::model::cron::{
    delete_cron_job, pause_cron_job, resume_cron_job, trigger_cron_job, upsert_cron_job,
};
use awa::model::{
    dlq, insert_with, PeriodicJob, PruneOutcome, QueueStorage, QueueStorageConfig, RetryFromDlqOpts,
};
use awa::{InsertOpts, JobArgs, JobContext, JobError, JobResult, UniqueOpts, Worker};
use awa_testing::setup::TestDatabase;
use awa_testing::{TestClient, WorkResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;

const CUSTOM_SCHEMA: &str = "awa_ri_custom";

/// Tables that may use REPLICA IDENTITY FULL. Anything else must carry a
/// primary key or a replica-identity index, so a new table cannot silently
/// opt into FULL's per-row WAL cost.
fn full_identity_is_expected(schema: &str, table: &str) -> bool {
    let delta_table = table == "queue_terminal_rollup_deltas"
        || table
            .strip_prefix("queue_terminal_count_deltas")
            .is_some_and(|suffix| {
                suffix.is_empty()
                    || suffix.strip_prefix('_').is_some_and(|slot| {
                        !slot.is_empty() && slot.bytes().all(|b| b.is_ascii_digit())
                    })
            });
    delta_table
        || (schema == "awa"
            && matches!(table, "admin_dirty_queue_marks" | "admin_dirty_kind_marks"))
}

#[derive(Debug, sqlx::FromRow)]
struct TableIdentity {
    schema: String,
    table: String,
    relkind: String,
    replident: String,
    has_primary_key: bool,
    has_identity_index: bool,
}

async fn table_identities(pool: &PgPool, schemas: &[&str]) -> Vec<TableIdentity> {
    sqlx::query_as(
        r#"
        SELECT
            n.nspname::text AS schema,
            c.relname::text AS "table",
            c.relkind::text AS relkind,
            c.relreplident::text AS replident,
            EXISTS (
                SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary
            ) AS has_primary_key,
            EXISTS (
                SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident
            ) AS has_identity_index
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1)
          AND c.relkind IN ('r', 'p')
        ORDER BY 1, 2
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await
    .expect("list awa tables")
}

async fn install_custom_schema(pool: &PgPool) -> QueueStorage {
    let store = QueueStorage::new(QueueStorageConfig {
        schema: CUSTOM_SCHEMA.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })
    .expect("custom queue-storage config");
    store
        .prepare_schema(pool)
        .await
        .expect("prepare custom queue-storage schema");
    store
}

/// Fresh installs, upgrades, and custom-schema installs all go through the
/// same DDL, so one migrated database plus one custom substrate covers every
/// shape the installer produces.
#[tokio::test]
async fn every_awa_table_has_a_usable_replica_identity() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool();
    install_custom_schema(pool).await;

    let tables = table_identities(pool, &["awa", CUSTOM_SCHEMA]).await;
    assert!(
        tables
            .iter()
            .any(|t| t.table == "queue_terminal_count_deltas_15"),
        "expected the default substrate's partitions to be present"
    );
    assert!(
        tables
            .iter()
            .any(|t| t.schema == CUSTOM_SCHEMA && t.table == "queue_terminal_count_deltas_3"),
        "expected the custom substrate's partitions to be present"
    );

    let mut problems = Vec::new();
    for table in &tables {
        let usable = match table.replident.as_str() {
            "d" => table.has_primary_key,
            "i" => table.has_identity_index,
            "f" => true,
            _ => false,
        };
        if !usable {
            problems.push(format!(
                "{}.{} ({}) has no usable replica identity (replident={}, pk={})",
                table.schema, table.table, table.relkind, table.replident, table.has_primary_key
            ));
        }
        if table.replident == "f" && !full_identity_is_expected(&table.schema, &table.table) {
            problems.push(format!(
                "{}.{} uses REPLICA IDENTITY FULL; give it a primary key or a replica-identity index instead",
                table.schema, table.table
            ));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));

    let unique_claims = tables
        .iter()
        .find(|t| t.schema == "awa" && t.table == "job_unique_claims")
        .expect("awa.job_unique_claims exists");
    assert_eq!(
        (
            unique_claims.replident.as_str(),
            unique_claims.has_identity_index
        ),
        ("i", true),
        "job_unique_claims should identify rows by its uniqueness index"
    );
}

async fn create_all_tables_publication(pool: &PgPool) -> bool {
    let superuser: bool =
        sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            .fetch_one(pool)
            .await
            .expect("read role");
    if !superuser {
        eprintln!("skipping: CREATE PUBLICATION FOR ALL TABLES needs a superuser");
        return false;
    }
    sqlx::query("CREATE PUBLICATION awa_ri_all FOR ALL TABLES")
        .execute(pool)
        .await
        .expect("create publication");
    true
}

/// Runs the statements PostgreSQL rejects on an identity-less published
/// table against every table in the schemas, including each partition. The
/// check happens at executor start, so `WHERE false` exercises it without
/// touching data.
async fn assert_every_table_accepts_update_and_delete(pool: &PgPool, schemas: &[&str]) {
    let tables: Vec<(String, String, String)> = sqlx::query_as(
        r#"
        SELECT n.nspname::text, c.relname::text, a.attname::text
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = 1
        WHERE n.nspname = ANY($1)
          AND c.relkind = 'r'
        ORDER BY 1, 2
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await
    .expect("list tables");
    assert!(
        tables.len() > 100,
        "expected the full awa schema, found {}",
        tables.len()
    );

    for (schema, table, column) in tables {
        let mut tx = pool.begin().await.expect("begin");
        sqlx::query(audited_sql(format!(
            "UPDATE {schema}.{table} SET {column} = {column} WHERE false"
        )))
        .execute(tx.as_mut())
        .await
        .unwrap_or_else(|err| panic!("UPDATE {schema}.{table} rejected while published: {err}"));
        sqlx::query(audited_sql(format!(
            "DELETE FROM {schema}.{table} WHERE false"
        )))
        .execute(tx.as_mut())
        .await
        .unwrap_or_else(|err| {
            panic!("DELETE FROM {schema}.{table} rejected while published: {err}")
        });
        tx.rollback().await.expect("rollback");
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ReplicaIdentityJob {
    id: i64,
}

struct CompleteWorker;

#[async_trait::async_trait]
impl Worker for CompleteWorker {
    fn kind(&self) -> &'static str {
        "replica_identity_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ReplicaIdentityFailingJob {
    id: i64,
}

struct FailWorker;

#[async_trait::async_trait]
impl Worker for FailWorker {
    fn kind(&self) -> &'static str {
        "replica_identity_failing_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Err(JobError::terminal("boom"))
    }
}

fn unique_opts(queue: &str) -> InsertOpts {
    InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts {
            by_queue: true,
            by_args: true,
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn exercise_cron_and_admin_paths(pool: &PgPool) {
    let cron = PeriodicJob::builder("replica_identity_cron", "0 9 * * *")
        .queue("replica_identity_q")
        .build(&ReplicaIdentityJob { id: 99 })
        .expect("build periodic job");
    upsert_cron_job(pool, &cron).await.expect("upsert cron job");
    upsert_cron_job(pool, &cron)
        .await
        .expect("upsert cron job again");
    trigger_cron_job(pool, "replica_identity_cron")
        .await
        .expect("trigger cron job");
    pause_cron_job(pool, "replica_identity_cron", Some("test"))
        .await
        .expect("pause cron job");
    resume_cron_job(pool, "replica_identity_cron")
        .await
        .expect("resume cron job");
    delete_cron_job(pool, "replica_identity_cron")
        .await
        .expect("delete cron job");

    sqlx::query("SELECT awa.recompute_dirty_admin_metadata(100)")
        .execute(pool)
        .await
        .expect("drain admin dirty marks");
    sqlx::query("SELECT awa.refresh_admin_metadata()")
        .execute(pool)
        .await
        .expect("refresh admin metadata");
}

/// The canonical engine: unique-claim release and the dirty-mark drain are
/// DELETEs on tables with no primary key.
#[tokio::test]
async fn canonical_paths_work_with_an_all_tables_publication() {
    let db = TestDatabase::canonical().await;
    let pool = db.pool();
    if !create_all_tables_publication(pool).await {
        return;
    }
    let queue = "replica_identity_q";
    let client = TestClient::from_pool(pool.clone()).await;

    let job = insert_with(pool, &ReplicaIdentityJob { id: 1 }, unique_opts(queue))
        .await
        .expect("insert unique job");
    let claims: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.job_unique_claims WHERE job_id = $1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .expect("count claims");
    assert_eq!(claims, 1, "unique insert should record a claim");

    let worked = client
        .work_one_in_queue(&CompleteWorker, Some(queue))
        .await
        .expect("complete unique job");
    assert!(matches!(worked, WorkResult::Completed(_)), "got {worked:?}");

    sqlx::query("DELETE FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .execute(pool)
        .await
        .expect("delete completed job, releasing its unique claim");
    let claims: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.job_unique_claims WHERE job_id = $1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .expect("count claims");
    assert_eq!(claims, 0, "the claim trigger should have deleted the claim");

    exercise_cron_and_admin_paths(pool).await;
    assert_every_table_accepts_update_and_delete(pool, &["awa"]).await;
}

/// The queue-storage engine: completion, rotation, prune (TRUNCATE plus the
/// rollup-delta append), the delta folds, unique jobs and the DLQ all run
/// while every table is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_storage_paths_work_with_an_all_tables_publication() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool();
    let custom = install_custom_schema(pool).await;
    if !create_all_tables_publication(pool).await {
        return;
    }
    let queue = "replica_identity_q";
    let client = TestClient::from_pool(pool.clone()).await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("default store");

    for id in 1..=3 {
        insert_with(pool, &ReplicaIdentityJob { id }, unique_opts(queue))
            .await
            .expect("insert unique job");
    }
    for _ in 0..3 {
        let worked = client
            .work_one_in_queue(&CompleteWorker, Some(queue))
            .await
            .expect("complete job");
        assert!(matches!(worked, WorkResult::Completed(_)), "got {worked:?}");
    }
    let deltas: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.queue_terminal_count_deltas")
        .fetch_one(pool)
        .await
        .expect("count terminal deltas");
    assert!(
        deltas > 0,
        "completions should append terminal count deltas"
    );

    let failing = insert_with(
        pool,
        &ReplicaIdentityFailingJob { id: 1 },
        InsertOpts {
            queue: queue.to_string(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .expect("insert failing job");
    let worked = client
        .work_one_in_queue(&FailWorker, Some(queue))
        .await
        .expect("fail job");
    assert!(matches!(worked, WorkResult::Failed(..)), "got {worked:?}");
    dlq::move_failed_to_dlq(pool, failing.id, "test")
        .await
        .expect("move to dlq")
        .expect("failed job lands in the dlq");
    dlq::retry_from_dlq(pool, failing.id, &RetryFromDlqOpts::default())
        .await
        .expect("retry from dlq")
        .expect("dlq row is retried");
    let worked = client
        .work_one_in_queue(&FailWorker, Some(queue))
        .await
        .expect("fail retried job");
    assert!(matches!(worked, WorkResult::Failed(..)), "got {worked:?}");
    dlq::move_failed_to_dlq(pool, failing.id, "test again")
        .await
        .expect("move to dlq again")
        .expect("failed job lands in the dlq again");
    assert!(
        dlq::purge_dlq_job(pool, failing.id)
            .await
            .expect("purge dlq job"),
        "purge should delete the dlq row"
    );

    for _ in 0..2 {
        store.rotate(pool).await.expect("rotate queue ring");
        store.rotate_leases(pool).await.expect("rotate lease ring");
        store.rotate_claims(pool).await.expect("rotate claim ring");
    }
    store
        .rollup_terminal_count_deltas(pool, 16)
        .await
        .expect("roll up terminal count deltas");
    let pruned = store
        .prune_oldest(pool, Duration::ZERO)
        .await
        .expect("prune queue ring");
    assert!(
        matches!(pruned, PruneOutcome::Pruned { .. }),
        "the sealed slot holding the completed jobs should be pruned, got {pruned:?}"
    );
    store
        .prune_oldest_leases(pool)
        .await
        .expect("prune lease ring");
    store
        .prune_oldest_claims(pool)
        .await
        .expect("prune claim ring");
    store
        .fold_terminal_rollup_deltas(pool)
        .await
        .expect("fold rollup deltas");
    store
        .fold_ring_rotation_ledgers(pool)
        .await
        .expect("fold rotation ledgers");
    store
        .rebuild_terminal_counters(pool)
        .await
        .expect("rebuild terminal counters");

    custom.rotate(pool).await.expect("rotate custom queue ring");
    custom
        .prune_oldest(pool, Duration::ZERO)
        .await
        .expect("prune custom queue ring");

    exercise_cron_and_admin_paths(pool).await;
    assert_every_table_accepts_update_and_delete(pool, &["awa", CUSTOM_SCHEMA]).await;
}
