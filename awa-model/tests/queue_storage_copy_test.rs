use awa_model::{
    migrations, AwaError, InsertOpts, InsertParams, QueueStorage, QueueStorageConfig, UniqueOpts,
};
use chrono::{TimeDelta, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;
use tokio::sync::Mutex;

static QUEUE_STORAGE_COPY_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_store_with_config(
    config: QueueStorageConfig,
    max_connections: u32,
) -> (PgPool, QueueStorage) {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("connect");
    migrations::run(&pool).await.expect("run migrations");

    let store = QueueStorage::new(config).expect("create queue storage");
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", store.schema()))
        .execute(&pool)
        .await
        .expect("drop queue storage schema");
    store
        .prepare_schema(&pool)
        .await
        .expect("prepare queue storage schema");
    store.reset(&pool).await.expect("reset queue storage");

    (pool, store)
}

async fn setup_store(schema: &str) -> (PgPool, QueueStorage) {
    setup_store_with_config(
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            ..Default::default()
        },
        4,
    )
    .await
}

async fn setup_striped_store(schema: &str, stripes: usize) -> (PgPool, QueueStorage) {
    setup_store_with_config(
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            queue_stripe_count: stripes,
            ..Default::default()
        },
        16,
    )
    .await
}

fn copy_job(kind: impl Into<String>, queue: &str, seq: i64) -> InsertParams {
    InsertParams {
        kind: kind.into(),
        args: serde_json::json!({"seq": seq}),
        opts: InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    }
}

fn copy_job_with_opts(
    kind: impl Into<String>,
    queue: &str,
    seq: i64,
    opts: InsertOpts,
) -> InsertParams {
    InsertParams {
        kind: kind.into(),
        args: serde_json::json!({"seq": seq}),
        opts: InsertOpts {
            queue: queue.to_string(),
            ..opts
        },
    }
}

fn bench_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[tokio::test]
async fn queue_storage_copy_enqueues_ready_and_deferred_rows() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store("awa_qs_copy_enqueues").await;
    let queue = "qs_copy_ready_deferred";

    let jobs = vec![
        InsertParams {
            kind: "copy_ready".to_string(),
            args: serde_json::json!({"seq": 0}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: 1,
                metadata: serde_json::json!({"source": "copy"}),
                tags: vec!["bulk".to_string()],
                ..Default::default()
            },
        },
        InsertParams {
            kind: "copy_ready".to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: 1,
                ..Default::default()
            },
        },
        InsertParams {
            kind: "copy_scheduled".to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: 3,
                run_at: Some(Utc::now() + TimeDelta::minutes(10)),
                ..Default::default()
            },
        },
    ];

    let inserted = store
        .enqueue_params_copy(&pool, &jobs)
        .await
        .expect("copy enqueue");
    assert_eq!(inserted, 3);

    let ready: Vec<(i64, serde_json::Value)> = sqlx::query_as(&format!(
        "SELECT lane_seq, payload FROM {}.ready_entries WHERE queue = $1 ORDER BY lane_seq",
        store.schema()
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read ready rows");
    assert_eq!(ready.len(), 2);
    assert_eq!(ready[1].0, ready[0].0 + 1);
    assert_eq!(ready[0].1["metadata"]["source"], "copy");
    assert_eq!(ready[0].1["tags"], serde_json::json!(["bulk"]));

    let deferred_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.deferred_jobs WHERE queue = $1 AND state = 'scheduled'",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count deferred rows");
    assert_eq!(deferred_count, 1);

    let available_count: i64 = sqlx::query_scalar(&format!(
        "SELECT available_count FROM {}.queue_lanes WHERE queue = $1 AND priority = 1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("read lane count");
    assert_eq!(available_count, 2);
}

#[tokio::test]
async fn queue_storage_copy_rolls_back_on_unique_conflict() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store("awa_qs_copy_unique_conflict").await;
    let queue = "qs_copy_unique_conflict";

    let opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts::default()),
        ..Default::default()
    };
    let jobs = vec![
        InsertParams {
            kind: "copy_unique".to_string(),
            args: serde_json::json!({"same": true}),
            opts: opts.clone(),
        },
        InsertParams {
            kind: "copy_unique".to_string(),
            args: serde_json::json!({"same": true}),
            opts,
        },
    ];

    let err = store
        .enqueue_params_copy(&pool, &jobs)
        .await
        .expect_err("duplicate unique batch should fail");
    assert!(
        matches!(err, AwaError::UniqueConflict { .. }),
        "unexpected error: {err:?}"
    );

    let ready_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count ready rows");
    assert_eq!(ready_count, 0);

    let lane_available: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(available_count), 0)::bigint FROM {}.queue_lanes WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count lane availability");
    assert_eq!(lane_available, 0);
}

#[tokio::test]
async fn queue_storage_batch_rolls_back_on_batched_unique_conflict() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store("awa_qs_batch_unique_conflict").await;
    let queue = "qs_batch_unique_conflict";

    let opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts::default()),
        ..Default::default()
    };
    let jobs = vec![
        InsertParams {
            kind: "batch_unique".to_string(),
            args: serde_json::json!({"same": true}),
            opts: opts.clone(),
        },
        InsertParams {
            kind: "batch_unique".to_string(),
            args: serde_json::json!({"same": true}),
            opts,
        },
    ];

    let err = store
        .enqueue_params_batch(&pool, &jobs)
        .await
        .expect_err("duplicate unique batch should fail");
    assert!(
        matches!(err, AwaError::UniqueConflict { .. }),
        "unexpected error: {err:?}"
    );

    let ready_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count ready rows");
    assert_eq!(ready_count, 0);

    let lane_available: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(available_count), 0)::bigint FROM {}.queue_lanes WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count lane availability");
    assert_eq!(lane_available, 0);
}

#[tokio::test]
async fn queue_storage_copy_rolls_back_on_existing_unique_conflict() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store("awa_qs_copy_existing_unique_conflict").await;
    let queue = "qs_copy_existing_unique_conflict";

    let opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts::default()),
        ..Default::default()
    };
    let job = InsertParams {
        kind: "copy_existing_unique".to_string(),
        args: serde_json::json!({"same": true}),
        opts,
    };

    assert_eq!(
        store
            .enqueue_params_batch(&pool, std::slice::from_ref(&job))
            .await
            .expect("seed unique job"),
        1
    );
    let err = store
        .enqueue_params_copy(&pool, std::slice::from_ref(&job))
        .await
        .expect_err("existing unique claim should fail");
    assert!(
        matches!(err, AwaError::UniqueConflict { .. }),
        "unexpected error: {err:?}"
    );

    let ready_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count ready rows");
    assert_eq!(ready_count, 1);

    let lane_available: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(available_count), 0)::bigint FROM {}.queue_lanes WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count lane availability");
    assert_eq!(lane_available, 1);
}

#[tokio::test]
async fn queue_storage_copy_concurrent_lane_seq_is_dense() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store_with_config(
        QueueStorageConfig {
            schema: "awa_qs_copy_concurrent".to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            ..Default::default()
        },
        16,
    )
    .await;
    let store = Arc::new(store);
    let queue = "qs_copy_concurrent";
    let task_count = 8_i64;
    let jobs_per_task = 16_i64;

    let mut handles = Vec::new();
    for task in 0..task_count {
        let pool = pool.clone();
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let jobs: Vec<_> = (0..jobs_per_task)
                .map(|idx| copy_job("copy_concurrent", queue, task * jobs_per_task + idx))
                .collect();
            store.enqueue_params_copy(&pool, &jobs).await
        }));
    }

    for handle in handles {
        assert_eq!(
            handle.await.expect("join copy task").expect("copy task"),
            jobs_per_task as usize
        );
    }

    let lane_seqs: Vec<i64> = sqlx::query_scalar(&format!(
        "SELECT lane_seq FROM {}.ready_entries WHERE queue = $1 ORDER BY lane_seq",
        store.schema()
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read lane seqs");
    let expected = task_count * jobs_per_task;
    assert_eq!(lane_seqs.len(), expected as usize);
    let first_seq = *lane_seqs.first().expect("at least one lane seq");
    for (offset, actual_seq) in lane_seqs.into_iter().enumerate() {
        assert_eq!(actual_seq, first_seq + offset as i64);
    }

    let available_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(available_count), 0)::bigint FROM {}.queue_lanes WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("read available count");
    assert_eq!(available_count, expected);
}

#[tokio::test]
async fn queue_storage_copy_distributes_across_stripes() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_striped_store("awa_qs_copy_striped", 4).await;
    let queue = "qs_copy_striped";
    let jobs: Vec<_> = (0..8)
        .map(|seq| copy_job("copy_striped", queue, seq))
        .collect();

    let inserted = store
        .enqueue_params_copy(&pool, &jobs)
        .await
        .expect("copy enqueue striped");
    assert_eq!(inserted, 8);

    let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT queue, lane_seq FROM {}.ready_entries ORDER BY queue, lane_seq",
        store.schema()
    ))
    .fetch_all(&pool)
    .await
    .expect("read striped ready rows");
    assert_eq!(rows.len(), 8);

    for stripe in 0..4 {
        let physical = format!("{queue}#{stripe}");
        let seqs: Vec<i64> = rows
            .iter()
            .filter_map(|(row_queue, lane_seq)| (row_queue == &physical).then_some(*lane_seq))
            .collect();
        assert_eq!(seqs.len(), 2, "unexpected row count for {physical}");
        assert_eq!(seqs[1], seqs[0] + 1, "unexpected lane seqs for {physical}");
    }

    let available_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(available_count), 0)::bigint FROM {}.queue_lanes WHERE queue = ANY($1)",
        store.schema()
    ))
    .bind((0..4).map(|stripe| format!("{queue}#{stripe}")).collect::<Vec<_>>())
    .fetch_one(&pool)
    .await
    .expect("read striped available count");
    assert_eq!(available_count, 8);
}

#[tokio::test]
async fn queue_storage_copy_escapes_csv_special_values() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store("awa_qs_copy_csv_escape").await;
    let queue = "qs_copy_csv_escape";
    let cases = [",", "\"", "\n", "\\", "", "__AWA_NULL__"];
    let run_at = Utc::now() + TimeDelta::minutes(5);
    let mut jobs = Vec::new();

    for (idx, value) in cases.iter().enumerate() {
        let opts = InsertOpts {
            queue: queue.to_string(),
            metadata: serde_json::json!({"special": value}),
            tags: vec![value.to_string()],
            ..Default::default()
        };
        jobs.push(InsertParams {
            kind: (*value).to_string(),
            args: serde_json::json!({"special": value, "seq": idx}),
            opts,
        });
    }

    jobs.push(copy_job_with_opts(
        "__AWA_NULL__",
        queue,
        cases.len() as i64,
        InsertOpts {
            run_at: Some(run_at),
            metadata: serde_json::json!({"special": "__AWA_NULL__"}),
            tags: vec!["__AWA_NULL__".to_string()],
            unique: Some(UniqueOpts::default()),
            ..Default::default()
        },
    ));

    let inserted = store
        .enqueue_params_copy(&pool, &jobs)
        .await
        .expect("copy enqueue escape matrix");
    assert_eq!(inserted, jobs.len());

    let ready: Vec<(String, serde_json::Value, serde_json::Value)> = sqlx::query_as(&format!(
        "SELECT kind, args, payload FROM {}.ready_entries WHERE queue = $1 ORDER BY (args->>'seq')::int",
        store.schema()
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read escaped ready rows");
    assert_eq!(ready.len(), cases.len());
    for (idx, (kind, args, payload)) in ready.into_iter().enumerate() {
        assert_eq!(kind, cases[idx]);
        assert_eq!(args["special"], cases[idx]);
        assert_eq!(payload["metadata"]["special"], cases[idx]);
        assert_eq!(payload["tags"], serde_json::json!([cases[idx]]));
    }

    let deferred: (
        String,
        serde_json::Value,
        serde_json::Value,
        Option<Vec<u8>>,
    ) = sqlx::query_as(&format!(
        "SELECT kind, args, payload, unique_key FROM {}.deferred_jobs WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("read escaped deferred row");
    assert_eq!(deferred.0, "__AWA_NULL__");
    assert_eq!(deferred.1["seq"], cases.len() as i64);
    assert_eq!(deferred.2["metadata"]["special"], "__AWA_NULL__");
    assert_eq!(deferred.2["tags"], serde_json::json!(["__AWA_NULL__"]));
    assert_eq!(
        deferred.3.as_ref().map(|bytes| !bytes.is_empty()),
        Some(true),
        "unique key bytea should round-trip through COPY hex encoding"
    );
}

#[tokio::test]
#[ignore = "benchmark; requires local Postgres and is not part of the default suite"]
async fn queue_storage_copy_benchmark_batch_vs_copy() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let total_jobs = bench_env_usize("AWA_QS_COPY_BENCH_JOBS", 8192);
    let batch_size = bench_env_usize("AWA_QS_COPY_BENCH_BATCH", 128);

    let (batch_pool, batch_store) = setup_store_with_config(
        QueueStorageConfig {
            schema: "awa_qs_copy_bench_batch".to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            ..Default::default()
        },
        8,
    )
    .await;
    let (copy_pool, copy_store) = setup_store_with_config(
        QueueStorageConfig {
            schema: "awa_qs_copy_bench_copy".to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            ..Default::default()
        },
        8,
    )
    .await;

    let jobs: Vec<_> = (0..total_jobs)
        .map(|seq| {
            copy_job_with_opts(
                "copy_bench",
                "qs_copy_bench",
                seq as i64,
                InsertOpts {
                    metadata: serde_json::json!({"bench": true, "seq": seq}),
                    tags: vec!["bench".to_string()],
                    ..Default::default()
                },
            )
        })
        .collect();

    let batch_start = Instant::now();
    for chunk in jobs.chunks(batch_size) {
        batch_store
            .enqueue_params_batch(&batch_pool, chunk)
            .await
            .expect("batch enqueue");
    }
    let batch_elapsed = batch_start.elapsed();

    let copy_start = Instant::now();
    for chunk in jobs.chunks(batch_size) {
        copy_store
            .enqueue_params_copy(&copy_pool, chunk)
            .await
            .expect("copy enqueue");
    }
    let copy_elapsed = copy_start.elapsed();

    let batch_rate = total_jobs as f64 / batch_elapsed.as_secs_f64();
    let copy_rate = total_jobs as f64 / copy_elapsed.as_secs_f64();
    println!(
        "[bench] queue_storage batch: {total_jobs} jobs in {:.3}s ({:.0} jobs/sec), batch_size={batch_size}",
        batch_elapsed.as_secs_f64(),
        batch_rate
    );
    println!(
        "[bench] queue_storage COPY:  {total_jobs} jobs in {:.3}s ({:.0} jobs/sec), batch_size={batch_size}",
        copy_elapsed.as_secs_f64(),
        copy_rate
    );
    println!(
        "[bench] queue_storage COPY speedup: {:.2}x",
        copy_rate / batch_rate
    );
}

#[tokio::test]
#[ignore = "benchmark; set DATABASE_URL and AWA_QS_COPY_BENCH_* to tune"]
async fn queue_storage_copy_benchmark_unique_batch_vs_copy() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let total_jobs = bench_env_usize("AWA_QS_COPY_BENCH_JOBS", 4096);
    let batch_size = bench_env_usize("AWA_QS_COPY_BENCH_BATCH", 128);

    let (batch_pool, batch_store) = setup_store_with_config(
        QueueStorageConfig {
            schema: "awa_qs_copy_unique_bench_batch".to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            ..Default::default()
        },
        8,
    )
    .await;
    let (copy_pool, copy_store) = setup_store_with_config(
        QueueStorageConfig {
            schema: "awa_qs_copy_unique_bench_copy".to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            ..Default::default()
        },
        8,
    )
    .await;

    let jobs: Vec<_> = (0..total_jobs)
        .map(|seq| {
            copy_job_with_opts(
                "copy_unique_bench",
                "qs_copy_unique_bench",
                seq as i64,
                InsertOpts {
                    metadata: serde_json::json!({"bench": true, "seq": seq}),
                    tags: vec!["bench".to_string(), "unique".to_string()],
                    unique: Some(UniqueOpts::default()),
                    ..Default::default()
                },
            )
        })
        .collect();

    let batch_start = Instant::now();
    for chunk in jobs.chunks(batch_size) {
        batch_store
            .enqueue_params_batch(&batch_pool, chunk)
            .await
            .expect("batch enqueue");
    }
    let batch_elapsed = batch_start.elapsed();

    let copy_start = Instant::now();
    for chunk in jobs.chunks(batch_size) {
        copy_store
            .enqueue_params_copy(&copy_pool, chunk)
            .await
            .expect("copy enqueue");
    }
    let copy_elapsed = copy_start.elapsed();

    let batch_rate = total_jobs as f64 / batch_elapsed.as_secs_f64();
    let copy_rate = total_jobs as f64 / copy_elapsed.as_secs_f64();
    println!(
        "[bench] queue_storage unique batch: {total_jobs} jobs in {:.3}s ({:.0} jobs/sec), batch_size={batch_size}",
        batch_elapsed.as_secs_f64(),
        batch_rate
    );
    println!(
        "[bench] queue_storage unique COPY:  {total_jobs} jobs in {:.3}s ({:.0} jobs/sec), batch_size={batch_size}",
        copy_elapsed.as_secs_f64(),
        copy_rate
    );
    println!(
        "[bench] queue_storage unique COPY speedup: {:.2}x",
        copy_rate / batch_rate
    );
}
