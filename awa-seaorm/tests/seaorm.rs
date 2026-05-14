use awa::{JobArgs, JobResult, QueueConfig};
use awa_seaorm::{client_builder, insert, migrate, SeaOrmAwaExt};
use sea_orm::DatabaseConnection;
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

#[tokio::test]
async fn seaorm_adapter_can_migrate_build_client_and_insert() {
    let pool = awa_testing::setup::setup(2).await;
    let db = DatabaseConnection::from(pool.clone());

    migrate(&db).await.expect("awa migration should succeed");

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
    let row: (i64,) = sqlx::query_as("SELECT 1")
        .fetch_one(pool)
        .await
        .expect("underlying postgres pool should execute sqlx queries");

    assert_eq!(row.0, 1);
}
