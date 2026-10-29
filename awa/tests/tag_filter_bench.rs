//! ADR-049 cost check for finding jobs on queue storage.
//!
//! Inserts and completes a batch of jobs, two thirds of them tagged (one tag
//! shared by one job in fifty, one unique per job), then times `find_jobs`
//! and `count_jobs` for a rare tag, a common tag and a kind with no tag. It
//! also reports the lifecycle WAL per job and the tag indexes' size.
//! Ignored by default; run with:
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test -p awa --test tag_filter_bench -- --ignored --nocapture
//! ```
//!
//! `TAG_BENCH_JOBS` overrides the batch size (default 50000).

use awa::model::admin;
use awa::{Client, JobArgs, JobFilter, JobPage, JobResult, QueueConfig};
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
/// ADR-049's target for a rare-tag lookup at this scale.
const RARE_TAG_TARGET: Duration = Duration::from_millis(100);

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

    // Every third job is untagged: it keeps the compact completion path and
    // must add nothing to the tag indexes.
    let params: Vec<awa::InsertParams> = (0..jobs)
        .map(|n| awa::InsertParams {
            kind: TagBench::kind().into(),
            args: json!({ "n": n }),
            opts: awa::InsertOpts {
                queue: queue.into(),
                tags: if n % 3 == 2 {
                    Vec::new()
                } else {
                    vec![format!("tenant:{}", n % TENANTS), format!("order:{n}")]
                },
                ..Default::default()
            },
        })
        .collect();
    let wal_start = wal_lsn(&pool).await;
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
    let lifecycle_wal = (wal_lsn(&pool).await - wal_start) as f64 / jobs as f64;
    let index_bytes: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(pg_relation_size(c.oid)), 0)::bigint FROM pg_class c \
         WHERE c.relkind = 'i' AND c.relname LIKE '%\\_tags\\_idx'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    println!(
        "\n{jobs} completed jobs, two thirds tagged; lifecycle WAL {lifecycle_wal:.0} B/job, \
         tag indexes {:.0} B/job",
        index_bytes as f64 / jobs as f64
    );
    println!("{:<40} {:>10} {:>8}", "lookup", "median", "rows");
    let rare = format!("order:{}", jobs / 2 - (jobs / 2) % 3);
    let mut rare_median = Duration::ZERO;
    for (label, filter, limit) in [
        (
            "find_jobs, kind, limit 100",
            JobFilter::new().kind(TagBench::kind()),
            100,
        ),
        (
            "find_jobs, common tag, limit 100",
            JobFilter::new().tag("tenant:7"),
            100,
        ),
        (
            "find_jobs, rare tag",
            JobFilter::new().tag(rare.clone()),
            20,
        ),
    ] {
        let (elapsed, rows) = median_of_three(|| {
            let pool = pool.clone();
            let filter = filter.clone();
            async move {
                admin::find_jobs(&pool, &filter, JobPage::first(limit))
                    .await
                    .unwrap()
                    .len()
            }
        })
        .await;
        println!(
            "{label:<40} {:>8.1}ms {rows:>8}",
            elapsed.as_secs_f64() * 1e3
        );
        if label.contains("rare") {
            assert_eq!(rows, 1);
            rare_median = elapsed;
        }
    }
    let (elapsed, _) = median_of_three(|| {
        let pool = pool.clone();
        async move {
            admin::count_jobs(&pool, &JobFilter::new().tag("tenant:7"))
                .await
                .unwrap()
                .value() as usize
        }
    })
    .await;
    println!(
        "{:<40} {:>8.1}ms",
        "count_jobs, common tag",
        elapsed.as_secs_f64() * 1e3
    );
    assert!(
        rare_median < RARE_TAG_TARGET,
        "a rare-tag lookup took {rare_median:?}, over the {RARE_TAG_TARGET:?} target"
    );
}
