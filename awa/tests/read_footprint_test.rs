//! Opt-in diagnostics for relation locks and query plans in disposable databases.
//! Maintenance is disabled while public operations are sampled.

use awa::model::{
    admin, migrations, InsertOpts, InsertParams, JobState, QueueStorage, QueueStorageConfig,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::{collections::BTreeSet, str::FromStr, time::Duration};

type Error = Box<dyn std::error::Error + Send + Sync>;
fn check(ok: bool, message: &str) -> Result<(), Error> {
    if ok {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}
async fn pool(options: &PgConnectOptions, app: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone().application_name(app))
        .await
}
async fn point(
    store: &QueueStorage,
    options: &PgConnectOptions,
    app: &str,
    id: i64,
    state: JobState,
) -> Result<(), Error> {
    let pool = pool(options, app).await?;
    let result = store.load_job(&pool, id).await;
    pool.close().await;
    let job = result?.ok_or("point read omitted job")?;
    check(job.state == state, "point-read state differs")?;
    eprintln!("READ_FOOTPRINT_API {app} job={id} state={state:?} PASS");
    Ok(())
}
async fn page(
    options: &PgConnectOptions,
    app: &str,
    queue: Option<&str>,
    state: Option<JobState>,
    expected: Option<usize>,
) -> Result<(), Error> {
    let pool = pool(options, app).await?;
    let result = admin::list_jobs(
        &pool,
        &admin::ListJobsFilter {
            queue: queue.map(str::to_owned),
            state,
            limit: Some(20),
            ..Default::default()
        },
    )
    .await;
    pool.close().await;
    let rows = result?;
    let ids: BTreeSet<_> = rows.iter().map(|job| job.id).collect();
    check(ids.len() == rows.len(), "list page duplicated an identity")?;
    if let Some(queue) = queue {
        check(
            rows.iter().all(|job| job.queue == queue),
            "list page escaped queue scope",
        )?;
    }
    check(rows.len() <= 20, "list page exceeded requested limit")?;
    if let Some(expected) = expected {
        check(
            rows.len() == expected,
            "list page has unexpected cardinality",
        )?;
    }
    let states: Vec<_> = rows
        .iter()
        .map(|job| (job.id, format!("{:?}", job.state)))
        .collect();
    eprintln!(
        "READ_FOOTPRINT_API {app} page_limit=20 actual={} rows={} PASS",
        rows.len(),
        serde_json::to_string(&states)?
    );
    Ok(())
}

async fn dashboards(options: &PgConnectOptions) -> Result<(), Error> {
    // Fresh single-connection pools keep each API attributable. Every public
    // call finishes before close; there is no outer connection/transaction lease.
    let app = "awa_read_footprint_state_counts";
    let pool = pool(options, app).await?;
    let result = admin::state_counts(&pool).await;
    pool.close().await;
    let rows = result?;
    check(
        rows.values().sum::<i64>() == 40,
        "state counts lost seeded work",
    )?;
    eprintln!(
        "READ_FOOTPRINT_API {app} result={} PASS",
        serde_json::to_string(&rows)?
    );

    let app = "awa_read_footprint_queue_overviews";
    let queue_pool = self::pool(options, app).await?;
    let result = admin::queue_overviews(&queue_pool).await;
    queue_pool.close().await;
    let rows = result?;
    let queues: BTreeSet<_> = rows.iter().map(|row| &row.queue).collect();
    check(
        queues.len() == rows.len(),
        "queue overview duplicated a queue",
    )?;
    check(
        rows.iter()
            .filter(|row| row.queue.starts_with("footprint_"))
            .map(|row| row.total_queued)
            .sum::<i64>()
            == 40,
        "queue overview lost seeded work",
    )?;
    eprintln!(
        "READ_FOOTPRINT_API {app} result={} PASS",
        serde_json::to_string(&rows)?
    );

    let app = "awa_read_footprint_kind_overviews";
    let kind_pool = self::pool(options, app).await?;
    let result = admin::job_kind_overviews(&kind_pool).await;
    kind_pool.close().await;
    let rows = result?;
    let kinds: BTreeSet<_> = rows.iter().map(|row| &row.kind).collect();
    check(kinds.len() == rows.len(), "kind overview duplicated a kind")?;
    check(
        rows.iter().any(|row| row.kind == "footprint_job"),
        "kind overview omitted the seeded kind",
    )?;
    // v0.6.13 can omit raw running receipts here; record, do not require
    // output equality across that known behavior change.
    eprintln!(
        "READ_FOOTPRINT_API {app} result={} PASS",
        serde_json::to_string(&rows)?
    );

    let app = "awa_read_footprint_state_timeseries";
    let timeseries_pool = self::pool(options, app).await?;
    let result = admin::state_timeseries(&timeseries_pool, 60).await;
    timeseries_pool.close().await;
    let rows = result?;
    let buckets: BTreeSet<_> = rows
        .iter()
        .map(|row| (row.bucket, format!("{:?}", row.state)))
        .collect();
    check(
        buckets.len() == rows.len(),
        "timeseries duplicated a bucket/state",
    )?;
    check(!rows.is_empty(), "timeseries omitted all fresh seeded work")?;
    eprintln!(
        "READ_FOOTPRINT_API {app} minutes=60 result={} PASS",
        serde_json::to_string(&rows)?
    );
    Ok(())
}

async fn failed_jobs(
    store: &QueueStorage,
    setup: &PgPool,
    queue: &str,
    count: usize,
) -> Result<Vec<i64>, Error> {
    let params: Vec<_> = (0..count)
        .map(|n| InsertParams {
            kind: queue.to_owned(),
            args: serde_json::json!({"admin_n": n, "body": "retained-terminal-body"}),
            opts: InsertOpts {
                queue: queue.to_owned(),
                priority: 3,
                max_attempts: 1,
                ..Default::default()
            },
        })
        .collect();
    check(
        store.enqueue_params_batch(setup, &params).await? == count,
        "terminal seed enqueue count differs",
    )?;
    let mut claimed = 0;
    while claimed < count {
        let rows = store
            .claim_runtime_batch(
                setup,
                queue,
                (count - claimed).min(1000) as i64,
                Duration::ZERO,
            )
            .await?;
        check(!rows.is_empty(), "terminal seed claim made no progress")?;
        claimed += rows.len();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let mut failed = 0;
    while failed < count {
        check(
            std::time::Instant::now() < deadline,
            "terminal seed rescue timed out",
        )?;
        let rows = store.rescue_stale_heartbeats(setup, Duration::ZERO).await?;
        check(
            rows.iter().all(|row| row.state == JobState::Failed),
            "seed rescue did not exhaust its attempt",
        )?;
        failed += rows.len();
        if rows.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT job_id FROM awa.done_entries WHERE queue=$1 AND state='failed' ORDER BY job_id",
    )
    .bind(queue)
    .fetch_all(setup)
    .await?;
    check(ids.len() == count, "terminal seed count differs")?;
    Ok(ids)
}

async fn terminal_admin_operations(
    store: &QueueStorage,
    setup: &PgPool,
    options: &PgConnectOptions,
) -> Result<(), Error> {
    for (operation, count) in [
        ("retry", 1),
        ("single_dlq", 1),
        ("bulk_dlq", 10),
        ("discard", 10),
    ] {
        let queue = format!("footprint_terminal_{operation}");
        let ids = failed_jobs(store, setup, &queue, count).await?;
        let app = format!("awa_read_footprint_terminal_{operation}");
        let measured = pool(options, &app).await?;
        let operation_result: Result<(), Error> = async {
            match operation {
                "retry" => {
                    let row = store
                        .retry_job(&measured, ids[0])
                        .await?
                        .ok_or("terminal retry omitted job")?;
                    check(
                        row.args["body"] == "retained-terminal-body"
                            && row.max_attempts == 1
                            && row.priority == 3,
                        "terminal retry lost body",
                    )?;
                }
                "single_dlq" => {
                    let row = store
                        .move_failed_to_dlq(&measured, ids[0], "footprint")
                        .await?
                        .ok_or("single DLQ move omitted job")?;
                    check(
                        row.args["body"] == "retained-terminal-body" && row.max_attempts == 1,
                        "single DLQ move lost body",
                    )?;
                }
                "bulk_dlq" => {
                    check(
                        store
                            .bulk_move_failed_to_dlq(&measured, None, Some(&queue), "footprint")
                            .await?
                            == count as u64,
                        "bulk DLQ move count differs",
                    )?;
                }
                "discard" => {
                    check(
                        store.discard_failed_by_kind(&measured, &queue).await? == count as u64,
                        "discard count differs",
                    )?;
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        .await;
        measured.close().await;
        operation_result?;
        let terminal_left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM awa.done_entries WHERE queue=$1")
                .bind(&queue)
                .fetch_one(setup)
                .await?;
        check(terminal_left == 0, "terminal mutation left failed rows")?;
        eprintln!("READ_FOOTPRINT_API {app} affected={count} PASS");
        if operation == "single_dlq" {
            let app = "awa_read_footprint_terminal_dlq_retry";
            let measured = pool(options, app).await?;
            let result = store
                .retry_from_dlq(&measured, ids[0], &awa::model::RetryFromDlqOpts::default())
                .await;
            measured.close().await;
            let row = result?.ok_or("DLQ retry omitted job")?;
            check(
                row.args["body"] == "retained-terminal-body"
                    && row.max_attempts == 1
                    && row.priority == 3,
                "DLQ retry lost body",
            )?;
            eprintln!("READ_FOOTPRINT_API {app} affected=1 PASS");
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "opt-in diagnostic; creates a disposable database"]
async fn retained_relation_locks_for_public_operations() -> Result<(), Error> {
    let url = std::env::var("DATABASE_URL")?;
    let admin_options = PgConnectOptions::from_str(&url)?;
    let admin = pool(&admin_options, "footprint_setup").await?;
    let database = format!("awa_footprint_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {database}"))
        .execute(&admin)
        .await?;
    let options = admin_options.database(&database);
    let setup = pool(&options, "footprint_setup").await?;
    let result: Result<(), Error> = async {
        migrations::run(&setup).await?;
        let store = QueueStorage::new(QueueStorageConfig::default())?;
        // Installation prepares the actual16/8/8 substrate and activates both
        // routing registries. Merely setting the active backend is insufficient.
        store.install(&setup).await?;
        let jobs: Vec<_> = ["footprint_plain", "footprint_deadline"]
            .into_iter()
            .flat_map(|queue| {
                (0..20).map(move |n| InsertParams {
                    kind: "footprint_job".into(),
                    args: serde_json::json!({"n":n}),
                    opts: InsertOpts {
                        queue: queue.into(),
                        ..Default::default()
                    },
                })
            })
            .collect();
        check(
            store.enqueue_params_batch(&setup, &jobs).await? == 40,
            "seed count differs",
        )?;
        let available: i64 = sqlx::query_scalar(
            "SELECT min(job_id) FROM awa.ready_entries WHERE queue='footprint_plain'",
        )
        .fetch_one(&setup)
        .await?;
        point(
            &store,
            &options,
            "awa_read_footprint_get_available",
            available,
            JobState::Available,
        )
        .await?;
        page(
            &options,
            "awa_read_footprint_list_available",
            Some("footprint_plain"),
            Some(JobState::Available),
            Some(20),
        )
        .await?;
        page(
            &options,
            "awa_read_footprint_list_unfiltered",
            Some("footprint_plain"),
            None,
            Some(20),
        )
        .await?;
        page(
            &options,
            "awa_read_footprint_list_global",
            None,
            None,
            Some(20),
        )
        .await?;
        for (queue, suffix, deadline) in [
            ("footprint_plain", "compact", Duration::ZERO),
            ("footprint_deadline", "deadline", Duration::from_secs(300)),
        ] {
            let claim_pool = pool(&options, &format!("awa_read_footprint_claim_{suffix}")).await?;
            let claimed = store
                .claim_runtime_batch(&claim_pool, queue, 10, deadline)
                .await;
            claim_pool.close().await;
            let claimed = claimed?;
            check(claimed.len() == 10, "claim count differs")?;
            eprintln!("READ_FOOTPRINT_API claim_{suffix} batch=10 PASS");
            // v0.6.13 commonly lists only10available rows; the receipt-listing
            // fix can list20. Both outputs must remain unique and scoped.
            page(
                &options,
                &format!("awa_read_footprint_list_mixed_{suffix}"),
                Some(queue),
                None,
                None,
            )
            .await?;
            if suffix == "compact" {
                page(
                    &options,
                    "awa_read_footprint_list_global_mixed",
                    None,
                    None,
                    Some(20),
                )
                .await?;
                dashboards(&options).await?;
            }
            point(
                &store,
                &options,
                &format!("awa_read_footprint_get_running_{suffix}"),
                claimed[0].job.id,
                JobState::Running,
            )
            .await?;
            let complete_pool =
                pool(&options, &format!("awa_read_footprint_complete_{suffix}")).await?;
            let completed = store.complete_runtime_batch(&complete_pool, &claimed).await;
            complete_pool.close().await;
            check(completed?.len() == 10, "completion count differs")?;
            eprintln!("READ_FOOTPRINT_API complete_{suffix} batch=10 PASS");
            point(
                &store,
                &options,
                &format!("awa_read_footprint_get_completed_{suffix}"),
                claimed[0].job.id,
                JobState::Completed,
            )
            .await?;
            page(
                &options,
                &format!("awa_read_footprint_list_completed_{suffix}"),
                Some(queue),
                Some(JobState::Completed),
                Some(10),
            )
            .await?;
        }
        terminal_admin_operations(&store, &setup, &options).await?;
        Ok(())
    }
    .await;
    setup.close().await;
    let cleanup = sqlx::query(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .execute(&admin)
        .await;
    admin.close().await;
    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
#[ignore = "opt-in diagnostic; creates a disposable database"]
async fn bulk_failed_to_dlq_actual_plan_at_10k() -> Result<(), Error> {
    let url = std::env::var("DATABASE_URL")?;
    let admin_options = PgConnectOptions::from_str(&url)?;
    let admin = pool(&admin_options, "terminal_plan_setup").await?;
    let database = format!("awa_terminal_plan_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {database}"))
        .execute(&admin)
        .await?;
    let options = admin_options.database(&database);
    let setup = pool(&options, "terminal_plan_setup").await?;
    let result: Result<(), Error> = async {
        migrations::run(&setup).await?;
        let store = QueueStorage::new(QueueStorageConfig::default())?;
        store.install(&setup).await?;
        let queue = "terminal_plan_10k";
        let seed_started = std::time::Instant::now();
        let ids = failed_jobs(&store, &setup, queue, 10_000).await?;
        let expected: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "SELECT job_id,args FROM awa.ready_entries WHERE queue=$1 ORDER BY job_id",
        ).bind(queue).fetch_all(&setup).await?;
        check(expected.len() == 10_000 && expected.iter().map(|row| row.0).eq(ids.iter().copied()), "10k seed identity differs")?;
        sqlx::raw_sql("ANALYZE awa.done_entries; ANALYZE awa.ready_entries; ANALYZE awa.ready_tombstones")
            .execute(&setup).await?;
        eprintln!("TERMINAL_PLAN_SEED count=10000 seconds={:.6}", seed_started.elapsed().as_secs_f64());
        let measured = pool(&options, "awa_terminal_explain_bulk_dlq").await?;
        sqlx::raw_sql("LOAD 'auto_explain'; SET auto_explain.log_min_duration=0; SET auto_explain.log_analyze=on; SET auto_explain.log_buffers=on; SET auto_explain.log_timing=off; SET auto_explain.log_format=json; SET auto_explain.log_nested_statements=on; SET auto_explain.sample_rate=1")
            .execute(&measured).await?;
        let settings: Vec<(String, String)> = sqlx::query_as("SELECT name,setting FROM pg_settings WHERE name LIKE 'auto_explain.%' ORDER BY name")
            .fetch_all(&measured).await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&measured).await?;
        eprintln!("TERMINAL_PLAN_SETTINGS pid={pid} settings={}", serde_json::to_string(&settings)?);
        eprintln!("TERMINAL_PLAN_PHASE workload_start");
        let started = std::time::Instant::now();
        let moved = store.bulk_move_failed_to_dlq(&measured, None, Some(queue), "plan-10k").await;
        let seconds = started.elapsed().as_secs_f64();
        measured.close().await;
        check(moved? == 10_000, "10k DLQ move count differs")?;
        let actual: (i64, i64, i64) = sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE args->>'body'='retained-terminal-body' AND max_attempts=1 AND priority=3),(SELECT count(*) FROM awa.done_entries WHERE queue=$1) FROM awa.dlq_entries WHERE queue=$1")
            .bind(queue).fetch_one(&setup).await?;
        check(actual == (10_000, 10_000, 0), "10k move lost or corrupted bodies")?;
        let actual_bodies: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "SELECT job_id,args FROM awa.dlq_entries WHERE queue=$1 ORDER BY job_id",
        ).bind(queue).fetch_all(&setup).await?;
        check(actual_bodies == expected, "10k move changed an identity or per-job body")?;
        eprintln!("TERMINAL_PLAN_PHASE recovered");
        eprintln!("TERMINAL_PLAN_API moved=10000 seconds={seconds:.6} body_checks={actual:?} PASS");
        Ok(())
    }.await;
    setup.close().await;
    let cleanup = sqlx::query(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .execute(&admin)
        .await;
    admin.close().await;
    result?;
    cleanup?;
    Ok(())
}
