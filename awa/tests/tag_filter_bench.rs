//! Cost check for finding jobs by tag on queue storage.
//!
//! Inserts and completes a batch of tagged jobs, then times `list_jobs` with
//! a rare tag (one job), a common tag (one job in fifty) and no tag. It also
//! estimates what a `(tag, job_id)` side table would cost: WAL per job to
//! write it, and the time to answer the same lookups through it. Ignored by
//! default; run with:
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test --release -p awa --test tag_filter_bench -- --ignored --nocapture
//! ```
//!
//! `TAG_BENCH_JOBS` overrides the batch size (default 50000).

use awa::model::admin::{self, ListJobsFilter};
use awa::{Client, JobArgs, JobResult, QueueConfig};
use awa_testing::setup::TestDatabase;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct TagBench {
    n: i64,
}

const TENANTS: i64 = 50;
const LIST_CAP: Duration = Duration::from_secs(120);

async fn wal_lsn(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn median_of_three<F, Fut>(mut f: F) -> (Duration, usize)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let mut runs = Vec::new();
    let mut found = 0;
    for _ in 0..3 {
        let started = Instant::now();
        found = f().await;
        runs.push(started.elapsed());
    }
    runs.sort();
    (runs[1], found)
}

#[tokio::test]
#[ignore]
async fn tag_filter_cost() {
    let jobs: i64 = std::env::var("TAG_BENCH_JOBS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "tag_bench";

    let returned = Arc::new(AtomicI64::new(0));
    let handler_returned = returned.clone();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 64,
                poll_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .register::<TagBench, _, _>(move |_args, _ctx| {
            let returned = handler_returned.clone();
            async move {
                returned.fetch_add(1, Ordering::Relaxed);
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();

    // TAG_BENCH_GIN=1 adds a partial GIN index over the payload's tags on
    // every table a job's row passes through, so this run measures what
    // Postgres-maintained indexing costs instead of the side table.
    let gin = std::env::var("TAG_BENCH_GIN").is_ok_and(|v| v == "1");
    let schema = awa::model::queue_storage::QueueStorage::active_schema(&pool)
        .await
        .unwrap()
        .unwrap();
    if gin {
        for table in [
            "ready_entries",
            "deferred_jobs",
            "done_entries",
            "dlq_entries",
        ] {
            sqlx::query(awa::audited_sql(format!(
                "CREATE INDEX ON {schema}.{table} USING gin ((payload -> 'tags') jsonb_path_ops) \
                 WHERE payload ? 'tags'"
            )))
            .execute(&pool)
            .await
            .unwrap();
        }
    }

    let params: Vec<awa::InsertParams> = (0..jobs)
        .map(|n| awa::InsertParams {
            kind: TagBench::kind().into(),
            args: json!({ "n": n }),
            opts: awa::InsertOpts {
                queue: queue.into(),
                tags: vec![format!("tenant:{}", n % TENANTS), format!("order:{n}")],
                ..Default::default()
            },
        })
        .collect();
    let wal_insert_start = wal_lsn(&pool).await;
    for chunk in params.chunks(500) {
        awa::insert_many(&pool, chunk).await.unwrap();
    }
    let started = Instant::now();
    client.start().await.unwrap();
    while returned.load(Ordering::Relaxed) < jobs {
        assert!(
            started.elapsed() < Duration::from_secs(900),
            "drain stalled"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    client.shutdown(Duration::from_secs(30)).await;
    let lifecycle_wal = (wal_lsn(&pool).await - wal_insert_start) as f64 / jobs as f64;

    let rare = format!("order:{}", jobs / 2);
    let common = "tenant:7".to_string();
    println!("\n{jobs} completed jobs, two tags each; lifecycle WAL {lifecycle_wal:.0} B/job");
    println!("{:<44} {:>10} {:>8}", "lookup", "median", "rows");

    let skip_list = std::env::var("TAG_BENCH_SKIP_LIST").is_ok_and(|v| v == "1");
    for (label, tag, limit) in [
        ("list_jobs, no tag, limit 100", None, 100),
        (
            "list_jobs, common tag (1 in 50), limit 100",
            Some(common.clone()),
            100,
        ),
        (
            "list_jobs, rare tag (1 job), limit 20",
            Some(rare.clone()),
            20,
        ),
    ] {
        if skip_list {
            continue;
        }
        // One run each, capped: the scan path can take minutes per call.
        let filter = ListJobsFilter {
            kind: Some(TagBench::kind().into()),
            tag: tag.clone(),
            limit: Some(limit),
            ..Default::default()
        };
        let started = Instant::now();
        match tokio::time::timeout(LIST_CAP, admin::list_jobs(&pool, &filter)).await {
            Ok(rows) => println!(
                "{label:<44} {:>8.1}ms {:>8}",
                started.elapsed().as_secs_f64() * 1e3,
                rows.unwrap().len()
            ),
            Err(_) => println!(
                "{label:<44} {:>10} {:>8}",
                format!(">{}s", LIST_CAP.as_secs()),
                "-"
            ),
        }
    }

    if gin {
        for (label, tag, limit) in [
            ("gin, common tag, limit 100", common.clone(), 100i64),
            ("gin, rare tag, limit 20", rare.clone(), 20),
        ] {
            let sql = format!(
                "SELECT DISTINCT job_id FROM ( \
                   SELECT job_id FROM {schema}.ready_entries \
                    WHERE payload ? 'tags' AND payload -> 'tags' @> jsonb_build_array($1::text) \
                   UNION ALL SELECT job_id FROM {schema}.deferred_jobs \
                    WHERE payload ? 'tags' AND payload -> 'tags' @> jsonb_build_array($1::text) \
                   UNION ALL SELECT job_id FROM {schema}.done_entries \
                    WHERE payload ? 'tags' AND payload -> 'tags' @> jsonb_build_array($1::text) \
                   UNION ALL SELECT job_id FROM {schema}.dlq_entries \
                    WHERE payload ? 'tags' AND payload -> 'tags' @> jsonb_build_array($1::text) \
                 ) AS hits ORDER BY job_id DESC LIMIT $2"
            );
            let (elapsed, rows) = median_of_three(|| {
                let pool = pool.clone();
                let tag = tag.clone();
                let sql = sql.clone();
                async move {
                    let ids: Vec<i64> = sqlx::query_scalar(awa::audited_sql(sql))
                        .bind(&tag)
                        .bind(limit)
                        .fetch_all(&pool)
                        .await
                        .unwrap();
                    let mut found = 0;
                    for id in ids {
                        admin::get_job(&pool, id).await.unwrap();
                        found += 1;
                    }
                    found
                }
            })
            .await;
            println!(
                "{label:<44} {:>8.1}ms {rows:>8}",
                elapsed.as_secs_f64() * 1e3
            );
        }
        let index_bytes: i64 = sqlx::query_scalar(
            "SELECT COALESCE(sum(pg_relation_size(c.oid)), 0)::bigint FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relkind = 'i' AND c.relname LIKE '%expr%idx%'",
        )
        .bind(&schema)
        .fetch_one(&pool)
        .await
        .unwrap();
        println!(
            "gin index size: {:.1} B/job",
            index_bytes as f64 / jobs as f64
        );
        return;
    }

    // Prototype side table: one row per (tag, job), written in insert-sized
    // chunks so the WAL estimate matches the enqueue path's batching.
    sqlx::query(
        "CREATE TABLE tag_bench_job_tags (tag text NOT NULL, job_id bigint NOT NULL, \
         PRIMARY KEY (tag, job_id))",
    )
    .execute(&pool)
    .await
    .unwrap();
    let ids: Vec<i64> =
        sqlx::query_scalar("SELECT job_id FROM awa.terminal_jobs WHERE kind = $1 ORDER BY job_id")
            .bind(TagBench::kind())
            .fetch_all(&pool)
            .await
            .unwrap();
    let wal_side_start = wal_lsn(&pool).await;
    for chunk in ids.chunks(500) {
        let mut tags = Vec::with_capacity(chunk.len() * 2);
        let mut job_ids = Vec::with_capacity(chunk.len() * 2);
        for id in chunk {
            tags.push(format!("tenant:{}", id % TENANTS));
            job_ids.push(*id);
            tags.push(format!("order:{id}"));
            job_ids.push(*id);
        }
        sqlx::query(
            "INSERT INTO tag_bench_job_tags (tag, job_id) \
             SELECT * FROM unnest($1::text[], $2::bigint[])",
        )
        .bind(&tags)
        .bind(&job_ids)
        .execute(&pool)
        .await
        .unwrap();
    }
    let side_wal = (wal_lsn(&pool).await - wal_side_start) as f64 / ids.len() as f64;
    println!(
        "side table write: {side_wal:.0} B/job ({:.1}% of lifecycle WAL)",
        100.0 * side_wal / lifecycle_wal
    );

    let rare_id = format!("order:{}", ids[ids.len() / 2]);
    for (label, tag, limit) in [
        ("side table, common tag, limit 100", common, 100i64),
        ("side table, rare tag, limit 20", rare_id, 20),
    ] {
        let (elapsed, rows) = median_of_three(|| {
            let pool = pool.clone();
            let tag = tag.clone();
            async move {
                let ids: Vec<i64> = sqlx::query_scalar(
                    "SELECT job_id FROM tag_bench_job_tags WHERE tag = $1 \
                     ORDER BY job_id DESC LIMIT $2",
                )
                .bind(&tag)
                .bind(limit)
                .fetch_all(&pool)
                .await
                .unwrap();
                let mut found = 0;
                for id in ids {
                    admin::get_job(&pool, id).await.unwrap();
                    found += 1;
                }
                found
            }
        })
        .await;
        println!(
            "{label:<44} {:>8.1}ms {rows:>8}",
            elapsed.as_secs_f64() * 1e3
        );
    }
}
