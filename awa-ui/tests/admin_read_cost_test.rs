//! The admin UI polls its read endpoints every few seconds from every open
//! tab, so a read whose cost grows with retained job history turns an idle
//! dashboard into a steady load on the database. These tests serve the UI's
//! polled endpoints from the real router against queue storage, once with a
//! little retained history and once with ten times as much, the live work
//! and the last hour held identical, and compare the work Postgres did for
//! each request.
//!
//! Work is read from Postgres' per-table counters (tuples read, buffers
//! touched), which depend on the data and the plan, not on timing, so the
//! comparison is stable on a loaded CI runner.
//!
//! The rule each endpoint must meet:
//! - Retained terminal headers (`receipt_completion_batches`,
//!   `receipt_completion_tombstones`, `done_entries`) are the designed source
//!   of terminal counts and are truncated by ring prune. A read may scan them
//!   at most once, so its reads there may grow by up to one pass over the
//!   added rows.
//! - Every other table's reads must stay flat as history grows.
//! - Nothing may expand retained completion arrays: TOAST reads must stay
//!   flat. A completion batch's member arrays are TOASTed, so expanding old
//!   batches shows up there.

use awa_model::{QueueStorage, QueueStorageConfig};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;
use tower::util::ServiceExt;

/// Endpoints the admin UI polls or loads on every visit, with the query the
/// UI sends. Keep in step with `refetchInterval` queries in `frontend/src`.
const POLLED_ENDPOINTS: &[&str] = &[
    "/api/stats",
    "/api/stats/timeseries?minutes=60",
    "/api/queues",
    "/api/queues/runtime",
    "/api/runtime",
    "/api/storage?history=true",
    "/api/capabilities",
    "/api/dlq?limit=50",
    "/api/dlq/depth",
    "/api/cron",
];

/// Polled endpoints that still read retained history on queue storage, with
/// why. The test fails if one of these stops growing, so the list can only
/// shrink.
const KNOWN_HISTORY_READS: &[(&str, &str)] = &[
    (
        "/api/kinds",
        "per-kind completed counts need each job's kind, which lives on its \
         ready row, not on the completion batch",
    ),
    (
        "/api/stats/kinds",
        "distinct kinds are read from current_jobs, including terminal_jobs",
    ),
    (
        "/api/stats/queues",
        "distinct queues are read from current_jobs, including terminal_jobs",
    ),
    (
        "/api/jobs?state=failed&limit=10",
        "the listing reads current_jobs; ADR-049's filter compiler (#572) \
         replaces it",
    ),
];

/// Tables whose rows are retained terminal headers: a read may pass over
/// them once.
const TERMINAL_HEADER_TABLES: &[&str] = &[
    "receipt_completion_batches",
    "receipt_completion_tombstones",
    "done_entries",
];

const QUEUES: [&str; 2] = ["cost_a", "cost_b"];
/// Large enough that a batch's member arrays are stored out of line.
const BATCH: i64 = 500;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, Default, Clone)]
struct TableWork {
    tuples: i64,
    blocks: i64,
    toast_blocks: i64,
    live_rows: i64,
}

type Work = BTreeMap<String, TableWork>;

fn header_family(table: &str) -> Option<&'static str> {
    TERMINAL_HEADER_TABLES
        .iter()
        .copied()
        .find(|family| table == *family || table.starts_with(&format!("{family}_")))
}

async fn table_work(pool: &PgPool) -> Result<Work, sqlx::Error> {
    let rows: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT t.relname::text, \
                (COALESCE(t.seq_tup_read, 0) + COALESCE(t.idx_tup_fetch, 0))::bigint, \
                (COALESCE(io.heap_blks_hit, 0) + COALESCE(io.heap_blks_read, 0) \
                 + COALESCE(io.idx_blks_hit, 0) + COALESCE(io.idx_blks_read, 0))::bigint, \
                (COALESCE(io.toast_blks_hit, 0) + COALESCE(io.toast_blks_read, 0) \
                 + COALESCE(io.tidx_blks_hit, 0) + COALESCE(io.tidx_blks_read, 0))::bigint, \
                COALESCE(t.n_live_tup, 0)::bigint \
         FROM pg_stat_user_tables AS t \
         JOIN pg_statio_user_tables AS io USING (relid) \
         WHERE t.schemaname = 'awa'",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(name, tuples, blocks, toast_blocks, live_rows)| {
            (
                name,
                TableWork {
                    tuples,
                    blocks,
                    toast_blocks,
                    live_rows,
                },
            )
        })
        .collect())
}

/// Serve one GET through the router and return the work it caused, per table.
async fn measure(
    app: &axum::Router,
    pool: &PgPool,
    uri: &str,
) -> Result<Work, Box<dyn std::error::Error>> {
    let mut best: Option<Work> = None;
    // Twice, keeping the smaller total, so a stray autovacuum or analyze
    // landing between the counter reads can't fail the comparison.
    for _ in 0..2 {
        let before = table_work(pool).await?;
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty())?)
            .await?;
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "{uri}: {}",
            String::from_utf8_lossy(&body)
        );
        // The single pooled connection served the request; flush its
        // pending table statistics before reading them back on it.
        sqlx::query("SELECT pg_stat_force_next_flush()")
            .execute(pool)
            .await?;
        sqlx::query("SELECT 1").execute(pool).await?;
        let after = table_work(pool).await?;
        let delta: Work = after
            .into_iter()
            .map(|(name, end)| {
                let start = before.get(&name).cloned().unwrap_or_default();
                (
                    name,
                    TableWork {
                        tuples: end.tuples - start.tuples,
                        blocks: end.blocks - start.blocks,
                        toast_blocks: end.toast_blocks - start.toast_blocks,
                        live_rows: end.live_rows,
                    },
                )
            })
            .filter(|(_, work)| work.tuples != 0 || work.blocks != 0 || work.toast_blocks != 0)
            .collect();
        let total = |work: &Work| work.values().map(|w| w.tuples + w.blocks).sum::<i64>();
        if best
            .as_ref()
            .is_none_or(|current| total(&delta) < total(current))
        {
            best = Some(delta);
        }
    }
    Ok(best.expect("measured at least once"))
}

/// Where a read with more history did more work than allowed, given its work
/// with less history and the size of each terminal-header table.
fn violations(
    uri: &str,
    small: &Work,
    large: &Work,
    header_sizes: &BTreeMap<&str, (i64, i64)>,
) -> Vec<String> {
    // Flat: allow plan noise of half again plus a constant per table.
    let flat = |before: i64| before + before / 2 + 64;
    let mut found = Vec::new();
    for (table, work) in large {
        let base = small.get(table).cloned().unwrap_or_default();
        let (tuple_cap, block_cap) = match header_family(table) {
            // One pass over the whole family: every row, and every page of
            // the heap and of one index.
            Some(family) => {
                let (rows, pages) = header_sizes.get(family).copied().unwrap_or((0, 0));
                (
                    flat(base.tuples).max(rows + 64),
                    flat(base.blocks).max(2 * pages + 64),
                )
            }
            None => (flat(base.tuples), flat(base.blocks)),
        };
        if work.tuples > tuple_cap {
            found.push(format!(
                "{uri}: {table} tuples read {} -> {} (cap {tuple_cap})",
                base.tuples, work.tuples
            ));
        }
        if work.blocks > block_cap {
            found.push(format!(
                "{uri}: {table} blocks {} -> {} (cap {block_cap})",
                base.blocks, work.blocks
            ));
        }
        if work.toast_blocks > flat(base.toast_blocks) {
            found.push(format!(
                "{uri}: {table} TOAST blocks {} -> {} (retained arrays expanded)",
                base.toast_blocks, work.toast_blocks
            ));
        }
    }
    found
}

struct Fixture {
    admin: PgPool,
    database: String,
    /// Builds the fixture.
    setup: PgPool,
    /// Serves the router: one connection, so every query a request makes
    /// runs on the backend whose statistics are read back. Parallel workers
    /// would report their reads asynchronously.
    pool: PgPool,
    store: QueueStorage,
}

impl Fixture {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let url = awa_testing::setup::database_url();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let database = format!("awa_read_cost_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
            .execute(&admin)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.database(&database);
        let setup = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options.clone())
            .await?;
        awa_model::migrations::run(&setup).await?;
        awa_testing::setup::activate_queue_storage(&setup).await;
        // Keep retained arrays out of line and uncompressed, so reading any
        // of them shows up as TOAST reads rather than as CPU the counters
        // can't see.
        sqlx::raw_sql(
            "ALTER TABLE awa.receipt_completion_batches \
                 ALTER COLUMN job_ids SET STORAGE EXTERNAL, \
                 ALTER COLUMN run_leases SET STORAGE EXTERNAL, \
                 ALTER COLUMN lane_seqs SET STORAGE EXTERNAL, \
                 ALTER COLUMN attempted_ats SET STORAGE EXTERNAL; \
             ALTER TABLE awa.lease_claim_batches \
                 ALTER COLUMN job_ids SET STORAGE EXTERNAL, \
                 ALTER COLUMN run_leases SET STORAGE EXTERNAL, \
                 ALTER COLUMN receipt_ids SET STORAGE EXTERNAL, \
                 ALTER COLUMN lane_seqs SET STORAGE EXTERNAL",
        )
        .execute(&setup)
        .await?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::raw_sql(
                        "SET max_parallel_workers_per_gather = 0; \
                         SET stats_fetch_consistency = 'none'",
                    )
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;
        let store = QueueStorage::new(QueueStorageConfig::default())?;
        Ok(Self {
            admin,
            database,
            setup,
            pool,
            store,
        })
    }

    async fn close(self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        self.setup.close().await;
        if std::env::var_os("AWA_KEEP_READ_COST_DB").is_some() {
            eprintln!("kept database {}", self.database);
            self.admin.close().await;
            return Ok(());
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.database
        )))
        .execute(&self.admin)
        .await?;
        self.admin.close().await;
        Ok(())
    }

    /// Complete `count` jobs on `queue` in batches of [`BATCH`], through the
    /// claim and completion path workers use.
    async fn complete(&self, queue: &str, count: i64) -> Result<(), Box<dyn std::error::Error>> {
        let mut remaining = count;
        while remaining > 0 {
            let n = remaining.min(BATCH);
            self.store.enqueue_batch(&self.setup, queue, 2, n).await?;
            let claims = self
                .store
                .claim_runtime_batch(&self.setup, queue, n, Duration::ZERO)
                .await?;
            assert_eq!(claims.len() as i64, n);
            let completed = self
                .store
                .complete_runtime_batch(&self.setup, &claims)
                .await?;
            assert_eq!(completed.len() as i64, n);
            remaining -= n;
        }
        Ok(())
    }

    /// Tombstone `count` completed members, as deleting or retrying them
    /// does: members of old (`old_jobs`) or recent completions, deleted now
    /// or (`recent_delete` false) two days ago.
    async fn tombstone(
        &self,
        count: i64,
        old_jobs: bool,
        recent_delete: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO awa.receipt_completion_tombstones \
                 (ready_slot, ready_generation, job_id, run_lease, queue, priority, enqueue_shard, \
                  lane_seq, deleted_at) \
             SELECT batch.ready_slot, batch.ready_generation, member.job_id, member.run_lease, \
                    batch.queue, batch.priority, batch.enqueue_shard, member.lane_seq, \
                    CASE WHEN $3 THEN clock_timestamp() \
                         ELSE clock_timestamp() - interval '2 days' END \
             FROM awa.receipt_completion_batches AS batch \
             CROSS JOIN LATERAL unnest(batch.job_ids, batch.run_leases, batch.lane_seqs) \
                 AS member(job_id, run_lease, lane_seq) \
             WHERE (batch.finalized_at < now() - interval '1 day') = $2 \
               AND NOT EXISTS ( \
                   SELECT 1 FROM awa.receipt_completion_tombstones AS tomb \
                   WHERE tomb.job_id = member.job_id AND tomb.run_lease = member.run_lease) \
             ORDER BY random() \
             LIMIT $1",
        )
        .bind(count)
        .bind(old_jobs)
        .bind(recent_delete)
        .execute(&self.setup)
        .await?;
        Ok(())
    }

    /// Rotate and prune the claim ring as the maintenance leader does, so
    /// claims of finished jobs leave it.
    async fn maintain_claim_ring(&self) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..=self.store.claim_slot_count() {
            self.store.rotate_claims(&self.setup).await?;
            self.store.prune_oldest_claims(&self.setup).await?;
        }
        Ok(())
    }

    /// Move the history queue's new completions two days into the past, so
    /// they are retained history rather than the last hour.
    async fn age_history(&self) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(
            "UPDATE awa.receipt_completion_batches \
                 SET finalized_at = finalized_at - interval '2 days' \
                 WHERE finalized_at > now() - interval '1 day' \
                   AND queue = 'cost_history'; \
             UPDATE awa.ready_entries \
                 SET created_at = created_at - interval '2 days', \
                     run_at = run_at - interval '2 days' \
                 WHERE created_at > now() - interval '1 day' \
                   AND queue = 'cost_history'",
        )
        .execute(&self.setup)
        .await?;
        Ok(())
    }

    /// Rows and heap pages of each terminal-header table, partitions
    /// included.
    async fn header_sizes(&self) -> Result<BTreeMap<&'static str, (i64, i64)>, sqlx::Error> {
        let mut sizes = BTreeMap::new();
        for family in TERMINAL_HEADER_TABLES {
            let size: (i64, i64) = sqlx::query_as(
                "SELECT COALESCE(sum(c.reltuples), 0)::bigint, COALESCE(sum(c.relpages), 0)::bigint \
                 FROM pg_class AS c \
                 JOIN pg_namespace AS n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'awa' AND c.relkind = 'r' \
                   AND (c.relname = $1 OR c.relname ~ ('^' || $1 || '_[0-9]+$'))",
            )
            .bind(*family)
            .fetch_one(&self.setup)
            .await?;
            sizes.insert(*family, size);
        }
        Ok(sizes)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn polled_admin_reads_do_not_scale_with_retained_history() -> TestResult {
    let fixture = Fixture::new().await?;
    let result = exercise(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise(fixture: &Fixture) -> TestResult {
    // Retained history: completions on their own queue, aged two days, with
    // some of them deleted since.
    fixture.complete("cost_history", 5 * BATCH).await?;
    fixture.age_history().await?;
    fixture.maintain_claim_ring().await?;
    fixture.tombstone(50, true, false).await?;

    // The last hour, identical in both measurements: completions, and recent
    // deletions of both recent and old completions.
    for queue in QUEUES {
        fixture.complete(queue, 2 * BATCH).await?;
    }
    fixture.tombstone(40, false, true).await?;
    fixture.tombstone(40, true, true).await?;

    // Live work: a backlog on each queue and some running claims.
    let mut running = Vec::new();
    for queue in QUEUES {
        fixture
            .store
            .enqueue_batch(&fixture.setup, queue, 2, 1_020)
            .await?;
        running.extend(
            fixture
                .store
                .claim_runtime_batch(&fixture.setup, queue, 20, Duration::ZERO)
                .await?,
        );
    }

    let app = awa_ui::router(fixture.pool.clone(), Duration::ZERO).await?;
    let known: Vec<&str> = KNOWN_HISTORY_READS.iter().map(|(uri, _)| *uri).collect();
    let endpoints: Vec<&str> = POLLED_ENDPOINTS
        .iter()
        .copied()
        .chain(known.iter().copied())
        .collect();

    sqlx::query("ANALYZE").execute(&fixture.setup).await?;
    let mut small = BTreeMap::new();
    for uri in &endpoints {
        small.insert(*uri, measure(&app, &fixture.pool, uri).await?);
    }

    // Ten times the retained history, with the same live work. An open claim
    // holds its claim slot, and claim-ring prune goes oldest first, so the
    // live claims finish before the history is added and the ring
    // maintained; then as many claims start again from a topped-up backlog.
    fixture
        .store
        .complete_runtime_batch(&fixture.setup, &running)
        .await?;
    fixture.maintain_claim_ring().await?;
    fixture.complete("cost_history", 45 * BATCH).await?;
    fixture.age_history().await?;
    fixture.maintain_claim_ring().await?;
    for queue in QUEUES {
        fixture
            .store
            .enqueue_batch(&fixture.setup, queue, 2, 20)
            .await?;
        fixture
            .store
            .claim_runtime_batch(&fixture.setup, queue, 20, Duration::ZERO)
            .await?;
    }
    fixture.tombstone(450, true, false).await?;
    sqlx::query("ANALYZE").execute(&fixture.setup).await?;
    let header_sizes = fixture.header_sizes().await?;

    let mut failures = Vec::new();
    let mut still_known = Vec::new();
    for uri in &endpoints {
        let large = measure(&app, &fixture.pool, uri).await?;
        let found = violations(uri, &small[uri], &large, &header_sizes);
        let total = |work: &Work| work.values().map(|w| w.tuples).sum::<i64>();
        eprintln!("{uri}: tuples {} -> {}", total(&small[uri]), total(&large));
        if known.contains(uri) {
            if found.is_empty() {
                failures.push(format!(
                    "{uri} no longer reads retained history: remove it from KNOWN_HISTORY_READS"
                ));
            } else {
                still_known.push(*uri);
            }
        } else {
            failures.extend(found);
        }
    }
    if !failures.is_empty() {
        return Err(failures.join("\n").into());
    }
    if still_known != known {
        return Err(format!(
            "known history reads {known:?}, still reading history {still_known:?}"
        )
        .into());
    }
    Ok(())
}
