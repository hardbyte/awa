use awa::{JobArgs, JobResult, QueueConfig};
use awa_seaorm::{client_builder, insert, insert_raw, migrate, SeaOrmAwaExt};
use sea_orm::{ConnectionTrait, DatabaseConnection, TransactionTrait};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

fn test_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_database() -> (sqlx::PgPool, DatabaseConnection) {
    let pool = awa_testing::setup::setup(2).await;
    let db = DatabaseConnection::from(pool.clone());
    migrate(&db).await.expect("awa migration should succeed");
    (pool, db)
}

async fn create_app_table(pool: &sqlx::PgPool, table_name: &str) {
    let table_name = quoted_identifier(table_name);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id TEXT PRIMARY KEY, note TEXT NOT NULL)"
    ))
    .execute(pool)
    .await
    .expect("create app table");
}

async fn drop_app_table(pool: &sqlx::PgPool, table_name: &str) {
    let table_name = quoted_identifier(table_name);
    sqlx::query(&format!("DROP TABLE IF EXISTS {table_name}"))
        .execute(pool)
        .await
        .expect("drop app table");
}

fn quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[tokio::test]
async fn seaorm_adapter_can_migrate_build_client_and_insert() {
    let (_pool, db) = setup_database().await;

    let client = client_builder(&db)
        .queue("email", QueueConfig::default())
        .register::<SendEmail, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .build()
        .expect("awa client should build from seaorm connection");
    drop(client);

    let job = insert(
        &db,
        &SendEmail {
            to: "ada@example.com".into(),
            subject: "hello".into(),
        },
    )
    .await
    .expect("awa insert should succeed through seaorm connection");

    let stored_kind: String = sqlx::query_scalar("SELECT kind FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(db.awa_pool())
        .await
        .expect("inserted job should be visible in awa.jobs");

    assert_eq!(stored_kind, "send_email");
}

#[tokio::test]
async fn seaorm_pool_helper_matches_database_connection() {
    let db = sea_orm::Database::connect(&test_database_url())
        .await
        .expect("database connection should succeed");

    let pool = db.awa_pool();
    let row: (i64,) = sqlx::query_as("SELECT 1::bigint")
        .fetch_one(pool)
        .await
        .expect("underlying postgres pool should execute sqlx queries");

    assert_eq!(row.0, 1);
}

#[tokio::test]
async fn enqueue_commits_atomically_with_app_writes() {
    let (pool, db) = setup_database().await;
    let table_name = "seaorm_commit_rows";
    create_app_table(&pool, table_name).await;

    let txn = db.begin().await.expect("begin transaction");
    txn.execute_unprepared(&format!(
        "INSERT INTO {table_name} (id, note) VALUES ('commit-app-row', 'persisted')"
    ))
    .await
    .expect("insert app row inside transaction");

    let job = insert(
        &txn,
        &SendEmail {
            to: "commit@example.com".into(),
            subject: "atomic commit".into(),
        },
    )
    .await
    .expect("insert job inside transaction");

    txn.commit().await.expect("commit transaction");

    let app_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table_name} WHERE id = 'commit-app-row'"
    ))
    .fetch_one(&pool)
    .await
    .expect("count committed app row");
    assert_eq!(app_count, 1);

    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .expect("count committed job");
    assert_eq!(job_count, 1);

    drop_app_table(&pool, table_name).await;
}

#[tokio::test]
async fn enqueue_rolls_back_with_app_writes() {
    let (pool, db) = setup_database().await;
    let table_name = "seaorm_rollback_rows";
    create_app_table(&pool, table_name).await;

    let txn = db.begin().await.expect("begin transaction");
    txn.execute_unprepared(&format!(
        "INSERT INTO {table_name} (id, note) VALUES ('rollback-app-row', 'discarded')"
    ))
    .await
    .expect("insert app row inside transaction");

    let job = insert_raw(
        &txn,
        "rollback_seaorm_job",
        serde_json::json!({"rolled_back": true}),
        awa::InsertOpts {
            queue: "seaorm_rollback_queue".into(),
            ..Default::default()
        },
    )
    .await
    .expect("insert raw job inside transaction");

    txn.rollback().await.expect("rollback transaction");

    let app_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table_name} WHERE id = 'rollback-app-row'"
    ))
    .fetch_one(&pool)
    .await
    .expect("count rolled-back app row");
    assert_eq!(app_count, 0);

    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .expect("count rolled-back job");
    assert_eq!(job_count, 0);

    drop_app_table(&pool, table_name).await;
}
