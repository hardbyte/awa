//! Public adapter API tests - requires a running Postgres instance.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::adapter::postgres::{
    prepare_job_insert, prepare_raw_job_insert, PreparedJobInsert, INSERT_JOB_SQL,
    UNIQUE_VIOLATION_SQLSTATE,
};
use awa::{migrations, InsertOpts, JobArgs, JobRow, JobState, UniqueOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgExecutor, PgPool};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("failed to connect to database");

    migrations::run(&pool)
        .await
        .expect("failed to run migrations");
    pool
}

async fn clean_queue(pool: &PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("failed to clean queue jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("failed to clean queue meta");
    sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("failed to clean queue state counts");
}

async fn execute_adapter_insert<'e, E>(
    executor: E,
    prepared: &PreparedJobInsert,
) -> Result<JobRow, sqlx::Error>
where
    E: PgExecutor<'e>,
{
    let tags = prepared.tags().to_vec();
    let unique_key = prepared.unique_key().map(<[u8]>::to_vec);
    let ordering_key = prepared.ordering_key().map(<[u8]>::to_vec);

    sqlx::query_as::<_, JobRow>(INSERT_JOB_SQL)
        .bind(prepared.kind())
        .bind(prepared.queue())
        .bind(prepared.args())
        .bind(prepared.state_db_str())
        .bind(prepared.priority())
        .bind(prepared.max_attempts())
        .bind(prepared.run_at())
        .bind(prepared.metadata())
        .bind(&tags)
        .bind(&unique_key)
        .bind(prepared.unique_states_bit_string())
        .bind(&ordering_key)
        .fetch_one(executor)
        .await
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct AdapterJob {
    account_id: i64,
}

#[test]
fn public_facade_prepares_canonical_bind_values() {
    let prepared = prepare_job_insert(
        &AdapterJob { account_id: 42 },
        InsertOpts {
            queue: "adapter_api_facade".into(),
            priority: 4,
            max_attempts: 9,
            tags: vec!["adapter".into(), "facade".into()],
            unique: Some(UniqueOpts::default()),
            ..Default::default()
        },
    )
    .expect("prepare typed job");

    assert_eq!(prepared.kind(), "adapter_job");
    assert_eq!(prepared.queue(), "adapter_api_facade");
    assert_eq!(prepared.state(), JobState::Available);
    assert_eq!(prepared.state_db_str(), "available");
    assert_eq!(prepared.unique_states_bit_string(), Some("11111000"));
    assert_eq!(UNIQUE_VIOLATION_SQLSTATE, "23505");
    assert!(INSERT_JOB_SQL.contains("awa.insert_job_compat"));

    let raw_args = serde_json::Map::from_iter([("x".to_string(), serde_json::json!(1))]);
    let raw = prepare_raw_job_insert("raw_adapter_job", raw_args, Default::default())
        .expect("prepare raw job");
    assert_eq!(raw.kind(), "raw_adapter_job");
    assert_eq!(raw.args(), &serde_json::json!({"x": 1}));
}

#[tokio::test]
async fn public_adapter_sql_inserts_prepared_job() {
    let queue = "adapter_api_insert";
    let pool = setup_pool().await;
    clean_queue(&pool, queue).await;

    let prepared = prepare_job_insert(
        &AdapterJob { account_id: 7 },
        InsertOpts {
            queue: queue.into(),
            priority: 1,
            max_attempts: 3,
            metadata: serde_json::json!({"source": "adapter-test"}),
            tags: vec!["public-api".into()],
            ordering_key: Some(b"account-7".to_vec()),
            ..Default::default()
        },
    )
    .expect("prepare adapter job");

    let row = execute_adapter_insert(&pool, &prepared)
        .await
        .expect("adapter SQL insert");

    assert_eq!(row.kind, "adapter_job");
    assert_eq!(row.queue, queue);
    assert_eq!(row.args, serde_json::json!({"account_id": 7}));
    assert_eq!(row.state, JobState::Available);
    assert_eq!(row.priority, 1);
    assert_eq!(row.max_attempts, 3);
    assert_eq!(row.metadata, serde_json::json!({"source": "adapter-test"}));
    assert_eq!(row.tags, vec!["public-api"]);

    clean_queue(&pool, queue).await;
}

#[tokio::test]
async fn public_adapter_sql_participates_in_caller_transaction() {
    let queue = "adapter_api_transaction";
    let pool = setup_pool().await;
    clean_queue(&pool, queue).await;

    let prepared = prepare_raw_job_insert(
        "transaction_adapter_job",
        serde_json::json!({"rolled_back": true}),
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .expect("prepare adapter job");

    let mut tx = pool.begin().await.expect("begin transaction");
    let row = execute_adapter_insert(&mut *tx, &prepared)
        .await
        .expect("adapter SQL insert in transaction");
    assert_eq!(row.queue, queue);
    tx.rollback().await.expect("rollback transaction");

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .expect("count rolled-back jobs");

    assert_eq!(count, 0);
}
