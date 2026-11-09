use crate::error::{map_sqlx_error, AwaError};
use crate::job::{InsertOpts, InsertParams, JobRow, JobState};
use crate::queue_storage::ordering_key_hash64;
use crate::unique::compute_unique_key;
use crate::JobArgs;
use sqlx::postgres::PgConnection;
use sqlx::{PgExecutor, PgPool};

// ── Lane lock order ─────────────────────────────────────────────────────
//
// On queue storage every available-state insert holds a transaction-scoped
// advisory lock on its `(queue, priority, enqueue_shard)` lane until the
// enclosing transaction ends, so two transactions that enqueue into the same
// lanes in opposite orders can deadlock. Every multi-row enqueue therefore
// takes its lane locks in one canonical order: `(queue, priority, shard)`
// ascending, queues compared bytewise (`COLLATE "C"`, the order of Rust's
// `str`). `QueueStorage` batch enqueues group rows in a `BTreeMap` keyed that
// way.
//
// `insert_many_copy` and `insert_many_skip_unique_conflicts` use
// `awa.insert_jobs_compat_batch` (the `insert_jobs_compat_batch` schema
// patch), which does the lane-independent work first (job ids, unique claims,
// payloads) and then registers and reserves its lanes one at a time in
// canonical order. Each lane's lock is taken when the batch reaches that
// lane rather than at the batch's first ready row, and like every enqueue
// lock it is held until the transaction ends. While it holds claims or lanes
// it waits at most 500 ms (or half of its session's `deadlock_timeout`, if
// lower) for another; a longer wait releases them all and retries, so a
// per-row producer that holds a lane and waits for one of the batch's keys
// usually proceeds instead of deadlocking with it. After 2 s of retries it
// makes one last attempt with ordinary lock waits.
//
// Generic `insert_many` always uses the per-row path, as does
// `insert_many_copy` on a schema without the patch: rows are sorted by the
// same key, stably, before they reach `awa.insert_job_compat`, and returned in
// input order. Both paths take the same lane lock, so producers on either, and
// older binaries, share a lane without reordering it.
//
// The shard is derived as `insert_job_compat` derives it: rows with an
// ordering key use `ordering_key_hash64 % enqueue_shards`, other rows
// `pg_backend_pid() % enqueue_shards`, with `enqueue_shards` read from
// `awa.queue_meta` (default 1).
//
// A transaction that enqueues with separate statements takes its locks in
// statement order and is not covered; it must enqueue lanes in canonical order
// itself, or enqueue them with one batch call.

const COPY_NULL_SENTINEL: &str = "__AWA_NULL__";
const INSERT_COMPAT_SQL: &str = r#"
    SELECT *
    FROM awa.insert_job_compat(
        $1,
        $2,
        $3,
        $4::awa.job_state,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11::text::bit(8),
        $12
    )
"#;

/// Canonical SQL for inserting one prepared Awa job through a Postgres driver.
///
/// This statement accepts simple driver-friendly bind values for the custom
/// Awa enum and bitmask columns:
///
/// - `$4`: state as text, cast to `awa.job_state`
/// - `$11`: unique states as a text bit-string, cast to `bit(8)`
///
/// `state::text AS state_str` is included for drivers that cannot decode the
/// custom enum directly. SQLx callers can ignore the extra column.
pub(crate) const POSTGRES_INSERT_JOB_SQL: &str = r#"
    SELECT *, state::text AS state_str
    FROM awa.insert_job_compat(
        $1,
        $2,
        $3,
        $4::text::awa.job_state,
        $5::smallint,
        $6::smallint,
        $7,
        $8,
        $9::text[],
        $10,
        $11::text::bit(8),
        $12
    )
"#;

// ── Shared insert preparation ───────────────────────────────────────────
//
// Single source of truth for computing all derived insert values:
// kind, serialized args, null-byte validation, state, unique_key,
// unique_states. Used by:
// - insert_with (single sqlx insert)
// - precompute_row_values (batch insert_many / insert_many_copy)
// - bridge adapters (tokio-postgres, etc.)

/// Reject JSON values containing null bytes (`\u0000`), which Postgres
/// JSONB does not support. Produces a clear validation error instead of
/// an opaque database error.
pub(crate) fn reject_null_bytes(value: &serde_json::Value) -> Result<(), AwaError> {
    match value {
        serde_json::Value::String(s) if s.contains('\0') => Err(AwaError::Validation(
            "job args/metadata must not contain null bytes (\\u0000): Postgres JSONB does not support them".into(),
        )),
        serde_json::Value::Array(arr) => {
            for v in arr {
                reject_null_bytes(v)?;
            }
            Ok(())
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                if k.contains('\0') {
                    return Err(AwaError::Validation(
                        "job args/metadata keys must not contain null bytes (\\u0000)".into(),
                    ));
                }
                reject_null_bytes(v)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Pre-computed values for a single job row, ready to bind into any driver.
///
/// External adapters should construct this with [`prepare_job_insert`] or
/// [`prepare_raw_job_insert`], then bind values to
/// [`crate::adapter::postgres::INSERT_JOB_SQL`] in the getter order below.
#[derive(Debug, Clone)]
pub struct PreparedJobInsert {
    pub(crate) kind: String,
    pub(crate) queue: String,
    pub(crate) args: serde_json::Value,
    pub(crate) state: JobState,
    pub(crate) priority: i16,
    pub(crate) max_attempts: i16,
    pub(crate) run_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) metadata: serde_json::Value,
    pub(crate) tags: Vec<String>,
    pub(crate) unique_key: Option<Vec<u8>>,
    pub(crate) unique_states: Option<String>,
    pub(crate) ordering_key: Option<Vec<u8>>,
}

pub(crate) type PreparedRow = PreparedJobInsert;

impl PreparedJobInsert {
    /// Job kind string.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Queue name.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Serialized job arguments.
    pub fn args(&self) -> &serde_json::Value {
        &self.args
    }

    /// Initial job state.
    pub fn state(&self) -> JobState {
        self.state
    }

    /// Initial job state as the canonical Postgres enum text value.
    pub fn state_db_str(&self) -> &'static str {
        self.state.as_str()
    }

    /// Initial priority.
    pub fn priority(&self) -> i16 {
        self.priority
    }

    /// Maximum attempts.
    pub fn max_attempts(&self) -> i16 {
        self.max_attempts
    }

    /// Optional scheduled run time.
    pub fn run_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.run_at
    }

    /// Job metadata JSON.
    pub fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    /// Job tags.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Precomputed unique key bytes.
    pub fn unique_key(&self) -> Option<&[u8]> {
        self.unique_key.as_deref()
    }

    /// Precomputed unique-state bit mask as a Postgres `bit(8)` text value.
    pub fn unique_states_bit_string(&self) -> Option<&str> {
        self.unique_states.as_deref()
    }

    /// Optional ordering key used by sharded queue storage.
    pub fn ordering_key(&self) -> Option<&[u8]> {
        self.ordering_key.as_deref()
    }
}

/// Multi-row insert with one array bind per column, so the statement text and
/// bind count do not depend on the number of jobs (Postgres allows at most
/// 65,535 bind parameters per statement).
///
/// `state` and `unique_states` travel as text and `tags` as one JSON array per
/// job, because sqlx has no array encoding for the enum, the bit string, or a
/// ragged array of arrays. `ordering_hash` carries each row's
/// [`ordering_key_hash64`] as decimal text (the value exceeds `bigint`) so the
/// statement can place keyed rows on the shard `insert_job_compat` will pick.
///
/// One `awa.insert_job_compat` call per row, rows inserted in canonical lane
/// order (see "Lane lock order" above) and returned in input order. It works
/// with and without the `insert_jobs_compat_batch` schema patch, including when
/// one process writes to databases that differ in whether they have it.
const INSERT_MANY_ROW_FUNCTION_SQL: &str = r#"
    WITH input AS MATERIALIZED (
        SELECT
            raw.*,
            MOD(
                CASE WHEN raw.ordering_hash IS NULL
                     THEN pg_backend_pid()::numeric
                     ELSE raw.ordering_hash::numeric
                END,
                COALESCE(
                    (SELECT meta.enqueue_shards FROM awa.queue_meta AS meta WHERE meta.queue = raw.queue),
                    1
                )::numeric
            ) AS lane_shard
        FROM unnest(
            $1::text[],
            $2::text[],
            $3::jsonb[],
            $4::text[],
            $5::smallint[],
            $6::smallint[],
            $7::timestamptz[],
            $8::jsonb[],
            $9::jsonb[],
            $10::bytea[],
            $11::text[],
            $12::bytea[],
            $13::text[]
        ) WITH ORDINALITY AS raw(
            kind, queue, args, state, priority, max_attempts, run_at,
            metadata, tags, unique_key, unique_states, ordering_key, ordering_hash, ord
        )
        ORDER BY raw.queue COLLATE "C", raw.priority, lane_shard, raw.ord
    )
    SELECT inserted.*
    FROM input
    CROSS JOIN LATERAL awa.insert_job_compat(
        input.kind,
        input.queue,
        input.args,
        input.state::awa.job_state,
        input.priority,
        input.max_attempts,
        input.run_at,
        input.metadata,
        ARRAY(
            SELECT tag.value
            FROM jsonb_array_elements_text(input.tags) WITH ORDINALITY AS tag(value, ord)
            ORDER BY tag.ord
        ),
        input.unique_key,
        input.unique_states::bit(8),
        input.ordering_key
    ) AS inserted
    ORDER BY input.ord
"#;

/// Compute unique_key and unique_states from opts.
fn compute_unique_fields(
    kind: &str,
    args: &serde_json::Value,
    opts: &InsertOpts,
) -> (Option<Vec<u8>>, Option<String>) {
    let unique_key = opts.unique.as_ref().map(|u| {
        compute_unique_key(
            kind,
            if u.by_queue { Some(&opts.queue) } else { None },
            if u.by_args { Some(args) } else { None },
            u.by_period,
        )
    });

    let unique_states = opts.unique.as_ref().map(|u| {
        // Build a bit string where PG bit position N (leftmost = 0) corresponds
        // to Rust bit N (least-significant = 0). PostgreSQL's get_bit(bitmask, N)
        // reads from the left, so we place Rust bit 0 at the leftmost position.
        let mut bit_string = String::with_capacity(8);
        for bit_position in 0..8 {
            if u.states & (1 << bit_position) != 0 {
                bit_string.push('1');
            } else {
                bit_string.push('0');
            }
        }
        bit_string
    });

    (unique_key, unique_states)
}

/// Prepare a single job insert from typed job args and options.
///
/// This is the stable preparation entry point for external adapters. It
/// performs the same validation and derived-field computation as
/// [`insert_with`].
pub fn prepare_job_insert(
    args: &impl JobArgs,
    opts: InsertOpts,
) -> Result<PreparedJobInsert, AwaError> {
    let kind = args.kind_str().to_string();
    let args_value = args.to_args()?;
    prepare_raw_job_insert(kind, args_value, opts)
}

/// Prepare a single job insert from raw kind, JSON args, and options.
///
/// Use this when building adapters for dynamic producers that do not have a
/// Rust [`JobArgs`] implementation.
pub fn prepare_raw_job_insert(
    kind: impl Into<String>,
    args: impl Into<serde_json::Value>,
    opts: InsertOpts,
) -> Result<PreparedJobInsert, AwaError> {
    let kind = kind.into();
    let args = args.into();

    reject_null_bytes(&args)?;
    reject_null_bytes(&opts.metadata)?;

    let state = if opts.run_at.is_some() {
        JobState::Scheduled
    } else {
        JobState::Available
    };

    let (unique_key, unique_states) = compute_unique_fields(&kind, &args, &opts);

    Ok(PreparedJobInsert {
        kind,
        queue: opts.queue,
        args,
        state,
        priority: opts.priority,
        max_attempts: opts.max_attempts,
        run_at: opts.run_at,
        metadata: opts.metadata,
        tags: opts.tags,
        unique_key,
        unique_states,
        ordering_key: opts.ordering_key,
    })
}

/// Prepare a single row from typed job args and options.
pub(crate) fn prepare_row(args: &impl JobArgs, opts: InsertOpts) -> Result<PreparedRow, AwaError> {
    prepare_job_insert(args, opts)
}

/// Prepare a single row from raw kind, JSON args, and options.
pub(crate) fn prepare_row_raw(
    kind: String,
    args: serde_json::Value,
    opts: InsertOpts,
) -> Result<PreparedRow, AwaError> {
    prepare_raw_job_insert(kind, args, opts)
}

// ── sqlx insert functions ───────────────────────────────────────────────

/// Insert a job with default options.
pub async fn insert<'e, E>(executor: E, args: &impl JobArgs) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    insert_with(executor, args, InsertOpts::default()).await
}

/// Insert a job with custom options.
#[tracing::instrument(skip(executor, args), fields(job.kind = args.kind_str(), job.queue = %opts.queue))]
pub async fn insert_with<'e, E>(
    executor: E,
    args: &impl JobArgs,
    opts: InsertOpts,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = prepare_row(args, opts)?;
    sqlx::query_as::<_, JobRow>(INSERT_COMPAT_SQL)
        .bind(&row.kind)
        .bind(&row.queue)
        .bind(&row.args)
        .bind(row.state)
        .bind(row.priority)
        .bind(row.max_attempts)
        .bind(row.run_at)
        .bind(&row.metadata)
        .bind(&row.tags)
        .bind(&row.unique_key)
        .bind(&row.unique_states)
        .bind(&row.ordering_key)
        .fetch_one(executor)
        .await
        .map_err(map_sqlx_error)
}

/// Pre-compute all row values including unique keys from InsertParams.
fn precompute_rows(jobs: &[InsertParams]) -> Result<Vec<PreparedRow>, AwaError> {
    jobs.iter()
        .map(|job| prepare_row_raw(job.kind.clone(), job.args.clone(), job.opts.clone()))
        .collect()
}

/// [`ordering_key_hash64`] of a row's ordering key as decimal text, the form
/// the batch statements cast to `numeric` to derive the row's shard.
fn ordering_hash_text(row: &PreparedRow) -> Option<String> {
    row.ordering_key
        .as_deref()
        .map(|key| ordering_key_hash64(key).to_string())
}

/// Insert multiple jobs in a single statement.
///
/// Supports uniqueness constraints — jobs with `unique` opts will have their
/// `unique_key` and `unique_states` computed and included. A job whose unique
/// key is already claimed fails the whole batch with a unique violation
/// (SQLSTATE `23505`, as [`AwaError::Database`]); use
/// [`insert_many_skip_unique_conflicts`] to insert the rest instead.
///
/// Calls `awa.insert_job_compat` per row, in canonical lane order, on every
/// supported schema. A generic executor is consumed by one statement and
/// cannot be probed first, so this path never assumes the database has the
/// `insert_jobs_compat_batch` schema patch. [`insert_many_copy`] probes its
/// connection and uses the batch function when it is installed;
/// [`insert_many_skip_unique_conflicts`] requires it.
#[tracing::instrument(skip(executor, jobs), fields(job.count = jobs.len()))]
pub async fn insert_many<'e, E>(executor: E, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let rows = precompute_rows(jobs)?;
    let results = sqlx::query_as::<_, JobRow>(INSERT_MANY_ROW_FUNCTION_SQL)
        .bind(rows.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.queue.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| &row.args).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.state_db_str())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.priority).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.max_attempts).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| &row.metadata).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| serde_json::json!(row.tags))
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.unique_key.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.unique_states.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.ordering_key.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(ordering_hash_text).collect::<Vec<_>>())
        .fetch_all(executor)
        .await?;

    Ok(results)
}

/// `awa.insert_jobs_compat_batch` over parallel column arrays, one element per
/// job. `$12` is each row's [`ordering_key_hash64`] as decimal text, as in
/// [`INSERT_MANY_ROW_FUNCTION_SQL`]; `$13` skips rows whose unique key is
/// already claimed instead of failing the batch. Rows come back in input
/// order; skipped rows are absent.
const INSERT_MANY_BATCH_SQL: &str = r#"
    SELECT *
    FROM awa.insert_jobs_compat_batch(
        $1::text[],
        $2::text[],
        $3::jsonb[],
        $4::text[],
        $5::smallint[],
        $6::smallint[],
        $7::timestamptz[],
        $8::jsonb[],
        $9::jsonb[],
        $10::bytea[],
        $11::text[],
        $12::text[],
        $13
    )
"#;

/// TRUE when `awa.insert_jobs_compat_batch` exists with the body the
/// `insert_jobs_compat_batch` schema patch installs, whoever may call it: the
/// patch's probe. The body check matches the end of the bounded-wait phase
/// and the 10 ms claim probe, so `awa migrate` replaces an earlier body that
/// waited for a lane without bound while holding its claims, or that
/// registered every new lane before reserving any. Catalog lookup only.
pub(crate) const BATCH_FUNCTION_PATCH_APPLIED_SQL: &str = "SELECT EXISTS (\
    SELECT 1 FROM pg_proc AS p WHERE p.oid = to_regprocedure(\
    'awa.insert_jobs_compat_batch(text[],text[],jsonb[],text[],smallint[],smallint[],\
    timestamptz[],jsonb[],jsonb[],bytea[],text[],text[],boolean)') \
    AND p.prosrc ~ 'v_bounded_until := clock_timestamp\\(\\) \\+ 4 \\* v_wait_budget' \
    AND p.prosrc ~ 'GREATEST\\(1, LEAST\\(10, ceil')";

/// TRUE when the connected role may call `awa.insert_jobs_compat_batch`.
const BATCH_FUNCTION_USABLE_SQL: &str = "SELECT COALESCE(has_function_privilege(to_regprocedure(\
    'awa.insert_jobs_compat_batch(text[],text[],jsonb[],text[],smallint[],smallint[],\
    timestamptz[],jsonb[],jsonb[],bytea[],text[],text[],boolean)'), 'EXECUTE'), FALSE)";

/// Insert multiple jobs in a single statement, skipping those whose unique key
/// is already claimed.
///
/// Returns the rows that were inserted, in input order; a skipped job has no
/// entry in the result. Jobs without `unique` opts are always inserted. A key
/// repeated inside the batch inserts its first job. Unlike [`insert_many`],
/// which fails the batch with a unique violation, this is what a caller that
/// replays work wants, and it takes each lane lock once for the batch rather
/// than once per job as a row-by-row retry does.
///
/// Requires the `insert_jobs_compat_batch` schema patch (`awa migrate`); on a
/// schema without it, returns [`AwaError::Validation`]. [`insert_many_copy`]
/// skips unique conflicts with or without the patch, through a connection, and
/// is what Python's `insert_many_copy` calls.
#[tracing::instrument(skip(executor, jobs), fields(job.count = jobs.len()))]
pub async fn insert_many_skip_unique_conflicts<'e, E>(
    executor: E,
    jobs: &[InsertParams],
) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    if jobs.is_empty() {
        return Ok(Vec::new());
    }
    let rows = precompute_rows(jobs)?;
    match bind_batch_columns(sqlx::query_as::<_, JobRow>(INSERT_MANY_BATCH_SQL), &rows)
        .bind(true)
        .fetch_all(executor)
        .await
    {
        Ok(results) => Ok(results),
        Err(err) if is_missing_batch_function(&err) => Err(AwaError::Validation(
            "insert_many_skip_unique_conflicts needs the insert_jobs_compat_batch schema patch; \
             run `awa migrate`"
                .to_string(),
        )),
        Err(err) => Err(err.into()),
    }
}

/// `undefined_function` naming the batch function: the schema predates the
/// `insert_jobs_compat_batch` patch, or the function was dropped.
fn is_missing_batch_function(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Database(db_err)
            if db_err.code().as_deref() == Some("42883")
                && db_err.message().contains("insert_jobs_compat_batch")
    )
}

fn bind_batch_columns<'q>(
    query: sqlx::query::QueryAs<'q, sqlx::Postgres, JobRow, sqlx::postgres::PgArguments>,
    rows: &'q [PreparedRow],
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, JobRow, sqlx::postgres::PgArguments> {
    query
        .bind(rows.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.queue.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| &row.args).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.state_db_str())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.priority).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.max_attempts).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| &row.metadata).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| serde_json::json!(row.tags))
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.unique_key.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.unique_states.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(ordering_hash_text).collect::<Vec<_>>())
}

/// CTEs that number the staged rows in staging order and re-sort them into
/// canonical lane order (see "Lane lock order" above); `staged_in_lane_order`
/// carries the staging-order position as `ord`.
macro_rules! staged_in_lane_order_ctes {
    () => {
        r#"
        staged AS (
            SELECT *, row_number() OVER () AS ord
            FROM pg_temp.awa_copy_staging
        ),
        staged_in_lane_order AS MATERIALIZED (
            SELECT
                staged.*,
                MOD(
                    CASE WHEN staged.ordering_hash IS NULL
                         THEN pg_backend_pid()::numeric
                         ELSE staged.ordering_hash::numeric
                    END,
                    COALESCE(
                        (SELECT meta.enqueue_shards FROM awa.queue_meta AS meta WHERE meta.queue = staged.queue),
                        1
                    )::numeric
                ) AS lane_shard
            FROM staged
            ORDER BY staged.queue COLLATE "C", staged.priority, lane_shard, staged.ord
        )
        "#
    };
}

/// Insert many jobs through the compatibility COPY surface.
///
/// Uses a temp staging table with no constraints for fast COPY ingestion,
/// then inserts the staged rows through `awa.insert_jobs_compat_batch`, or, on
/// a schema without the `insert_jobs_compat_batch` patch, through
/// `awa.insert_job_compat` row by row. Jobs whose unique key is already
/// claimed are skipped and absent from the result. Accepts
/// `&mut PgConnection` so callers can use pool connections or transactions
/// (Transaction derefs to PgConnection). For high-volume queue-storage
/// producers, prefer [`QueueStorage::enqueue_params_copy`](crate::QueueStorage::enqueue_params_copy)
/// on a `QueueStorage` built with the same config as the worker fleet.
#[tracing::instrument(skip(conn, jobs), fields(job.count = jobs.len()))]
pub async fn insert_many_copy(
    conn: &mut PgConnection,
    jobs: &[InsertParams],
) -> Result<Vec<JobRow>, AwaError> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let rows = precompute_rows(jobs)?;

    // 1. Create or reuse a session-local staging table.
    //
    // Keeping the temp table structure across transactions avoids repeated
    // catalog churn under concurrent producers while preserving transactional
    // cleanup of staged rows at commit/rollback boundaries.
    sqlx::query(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS pg_temp.awa_copy_staging (
            kind        TEXT NOT NULL,
            queue       TEXT NOT NULL,
            args        JSONB NOT NULL,
            state       awa.job_state NOT NULL,
            priority    SMALLINT NOT NULL,
            max_attempts SMALLINT NOT NULL,
            run_at      TIMESTAMPTZ,
            metadata    JSONB NOT NULL,
            tags        TEXT[] NOT NULL,
            unique_key  BYTEA,
            unique_states BIT(8),
            ordering_key BYTEA,
            ordering_hash TEXT
        ) ON COMMIT DELETE ROWS
        "#,
    )
    .execute(&mut *conn)
    .await?;

    // 2. COPY data into staging table via CSV
    let mut csv_buf = Vec::with_capacity(rows.len() * 256);
    for row in &rows {
        write_csv_row(&mut csv_buf, row);
    }

    let mut copy_in = conn
        .copy_in_raw(
            "COPY pg_temp.awa_copy_staging (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states, ordering_key, ordering_hash) FROM STDIN WITH (FORMAT csv, NULL '__AWA_NULL__')",
        )
        .await?;
    copy_in.send(csv_buf).await?;
    copy_in.finish().await?;

    // 3. Insert the staged rows. With the batch function, unique conflicts are
    // skipped in the one statement; without it, each unique row goes through
    // the per-row function in its own savepoint.
    let has_unique = rows.iter().any(|r| r.unique_key.is_some());
    let results = if batch_function_usable(conn).await? {
        insert_staged_rows_with_batch_function(conn, has_unique).await?
    } else {
        insert_staged_rows_per_row(conn, has_unique).await?
    };

    // Keep the session-local staging table reusable across multiple COPY calls
    // within the same outer transaction.
    sqlx::query("DELETE FROM pg_temp.awa_copy_staging")
        .execute(&mut *conn)
        .await?;

    Ok(results)
}

/// Probed on every COPY call, on the caller's own connection: a process can
/// write to databases that differ in whether they have the patch, and a role
/// without EXECUTE on the function keeps the per-row path.
async fn batch_function_usable(conn: &mut PgConnection) -> Result<bool, AwaError> {
    Ok(sqlx::query_scalar(BATCH_FUNCTION_USABLE_SQL)
        .fetch_one(&mut *conn)
        .await?)
}

async fn insert_staged_rows_with_batch_function(
    conn: &mut PgConnection,
    skip_unique_conflicts: bool,
) -> Result<Vec<JobRow>, AwaError> {
    Ok(sqlx::query_as::<_, JobRow>(
        r#"
        WITH staged AS (
            SELECT *, row_number() OVER () AS ord
            FROM pg_temp.awa_copy_staging
        ),
        columns AS (
            SELECT
                array_agg(kind ORDER BY ord) AS kinds,
                array_agg(queue ORDER BY ord) AS queues,
                array_agg(args ORDER BY ord) AS args,
                array_agg(state::text ORDER BY ord) AS states,
                array_agg(priority ORDER BY ord) AS priorities,
                array_agg(max_attempts ORDER BY ord) AS max_attempts,
                array_agg(run_at ORDER BY ord) AS run_ats,
                array_agg(metadata ORDER BY ord) AS metadata,
                array_agg(to_jsonb(tags) ORDER BY ord) AS tags,
                array_agg(unique_key ORDER BY ord) AS unique_keys,
                array_agg(unique_states::text ORDER BY ord) AS unique_states,
                array_agg(ordering_hash ORDER BY ord) AS ordering_hashes
            FROM staged
        )
        SELECT inserted.*
        FROM columns
        CROSS JOIN LATERAL awa.insert_jobs_compat_batch(
            columns.kinds,
            columns.queues,
            columns.args,
            columns.states,
            columns.priorities,
            columns.max_attempts,
            columns.run_ats,
            columns.metadata,
            columns.tags,
            columns.unique_keys,
            columns.unique_states,
            columns.ordering_hashes,
            $1
        ) AS inserted
        "#,
    )
    .bind(skip_unique_conflicts)
    .fetch_all(&mut *conn)
    .await?)
}

async fn insert_staged_rows_per_row(
    conn: &mut PgConnection,
    has_unique: bool,
) -> Result<Vec<JobRow>, AwaError> {
    let results: Vec<JobRow> = if has_unique {
        // The compatibility `awa.jobs` surface is now a view backed by hot and
        // deferred tables, so the old `ON CONFLICT` path is no longer available
        // here. Keep COPY for staging/parsing, then insert unique rows one at a
        // time and skip duplicates explicitly.
        let staged_rows = sqlx::query_as::<
            _,
            (
                i64,
                String,
                String,
                serde_json::Value,
                String,
                i16,
                i16,
                Option<chrono::DateTime<chrono::Utc>>,
                serde_json::Value,
                Vec<String>,
                Option<Vec<u8>>,
                Option<String>,
                Option<Vec<u8>>,
            ),
        >(concat!(
            "WITH",
            staged_in_lane_order_ctes!(),
            r#"
            SELECT
                ord,
                kind,
                queue,
                args,
                state::text,
                priority,
                max_attempts,
                run_at,
                metadata,
                tags,
                unique_key,
                unique_states::text,
                ordering_key
            FROM staged_in_lane_order
            ORDER BY queue COLLATE "C", priority, lane_shard, ord
            "#
        ))
        .fetch_all(&mut *conn)
        .await?;

        let mut inserted = Vec::with_capacity(staged_rows.len());
        for (
            staged_position,
            kind,
            queue,
            args,
            state,
            priority,
            max_attempts,
            run_at,
            metadata,
            tags,
            unique_key,
            unique_states,
            ordering_key,
        ) in staged_rows
        {
            sqlx::query("SAVEPOINT awa_copy_unique_row")
                .execute(&mut *conn)
                .await?;

            let result = sqlx::query_as::<_, JobRow>(POSTGRES_INSERT_JOB_SQL)
                .bind(&kind)
                .bind(&queue)
                .bind(&args)
                .bind(&state)
                .bind(priority)
                .bind(max_attempts)
                .bind(run_at)
                .bind(&metadata)
                .bind(&tags)
                .bind(&unique_key)
                .bind(&unique_states)
                .bind(&ordering_key)
                .fetch_one(&mut *conn)
                .await;

            match result {
                Ok(row) => {
                    inserted.push((staged_position, row));
                    sqlx::query("RELEASE SAVEPOINT awa_copy_unique_row")
                        .execute(&mut *conn)
                        .await?;
                }
                Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("23505") => {
                    sqlx::query("ROLLBACK TO SAVEPOINT awa_copy_unique_row")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("RELEASE SAVEPOINT awa_copy_unique_row")
                        .execute(&mut *conn)
                        .await?;
                    continue;
                }
                Err(err) => {
                    sqlx::query("ROLLBACK TO SAVEPOINT awa_copy_unique_row")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("RELEASE SAVEPOINT awa_copy_unique_row")
                        .execute(&mut *conn)
                        .await?;
                    return Err(AwaError::Database(err));
                }
            }
        }

        inserted.sort_unstable_by_key(|(staged_position, _)| *staged_position);
        inserted.into_iter().map(|(_, row)| row).collect()
    } else {
        sqlx::query_as::<_, JobRow>(concat!(
            "WITH",
            staged_in_lane_order_ctes!(),
            r#"
            SELECT inserted.*
            FROM staged_in_lane_order AS staged
            CROSS JOIN LATERAL awa.insert_job_compat(
                staged.kind,
                staged.queue,
                staged.args,
                staged.state,
                staged.priority,
                staged.max_attempts,
                staged.run_at,
                staged.metadata,
                staged.tags,
                staged.unique_key,
                staged.unique_states,
                staged.ordering_key
            ) AS inserted
            ORDER BY staged.ord
            "#
        ))
        .fetch_all(&mut *conn)
        .await?
    };
    Ok(results)
}

/// Convenience wrapper that acquires a connection from the pool.
///
/// Wraps the operation in a transaction so the staging rows are cleaned up at
/// commit time even if the caller does not reuse the connection afterward.
#[tracing::instrument(skip(pool, jobs), fields(job.count = jobs.len()))]
pub async fn insert_many_copy_from_pool(
    pool: &PgPool,
    jobs: &[InsertParams],
) -> Result<Vec<JobRow>, AwaError> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let mut tx = pool.begin().await?;
    let results = insert_many_copy(&mut tx, jobs).await?;
    tx.commit().await?;

    Ok(results)
}

// ── CSV serialization helpers ────────────────────────────────────────

/// Write one PreparedRow as a CSV line to the buffer.
fn write_csv_row(buf: &mut Vec<u8>, row: &PreparedRow) {
    // kind
    write_csv_field(buf, &row.kind);
    buf.push(b',');
    // queue
    write_csv_field(buf, &row.queue);
    buf.push(b',');
    // args (JSONB as text)
    let args_str = serde_json::to_string(&row.args).expect("JSON serialization should not fail");
    write_csv_field(buf, &args_str);
    buf.push(b',');
    // state
    write_csv_field(buf, &row.state.to_string());
    buf.push(b',');
    // priority
    buf.extend_from_slice(row.priority.to_string().as_bytes());
    buf.push(b',');
    // max_attempts
    buf.extend_from_slice(row.max_attempts.to_string().as_bytes());
    buf.push(b',');
    // run_at (TIMESTAMPTZ as RFC 3339, or the COPY null sentinel)
    match &row.run_at {
        Some(dt) => write_csv_field(buf, &dt.to_rfc3339()),
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
    buf.push(b',');
    // metadata (JSONB as text)
    let metadata_str =
        serde_json::to_string(&row.metadata).expect("JSON serialization should not fail");
    write_csv_field(buf, &metadata_str);
    buf.push(b',');
    // tags (Postgres text[] literal)
    write_pg_text_array(buf, &row.tags);
    buf.push(b',');
    // unique_key (bytea hex format, or the COPY null sentinel)
    match &row.unique_key {
        Some(key) => {
            let bytea_hex = format!("\\x{}", hex::encode(key));
            write_csv_field(buf, &bytea_hex);
        }
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
    buf.push(b',');
    // unique_states (bit string, or the COPY null sentinel)
    match &row.unique_states {
        Some(bits) => write_csv_field(buf, bits),
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
    buf.push(b',');
    // ordering_key (bytea hex format, or the COPY null sentinel)
    match &row.ordering_key {
        Some(key) => {
            let bytea_hex = format!("\\x{}", hex::encode(key));
            write_csv_field(buf, &bytea_hex);
        }
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
    buf.push(b',');
    // ordering_hash (decimal text, or the COPY null sentinel)
    match ordering_hash_text(row) {
        Some(hash) => write_csv_field(buf, &hash),
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
    buf.push(b'\n');
}

/// Write a CSV field, quoting if it contains special characters.
fn write_csv_field(buf: &mut Vec<u8>, value: &str) {
    if value.contains(',')
        || value.contains('"')
        || value.contains('\n')
        || value.contains('\r')
        || value.contains('\\')
        || value == COPY_NULL_SENTINEL
    {
        buf.push(b'"');
        for byte in value.bytes() {
            if byte == b'"' {
                buf.push(b'"');
            }
            buf.push(byte);
        }
        buf.push(b'"');
    } else {
        buf.extend_from_slice(value.as_bytes());
    }
}

/// Write a Postgres text[] array literal: `{elem1,"elem with , comma"}`.
/// The entire literal is CSV-quoted because it always contains braces.
fn write_pg_text_array(buf: &mut Vec<u8>, values: &[String]) {
    buf.push(b'"');
    buf.push(b'{');
    for (i, val) in values.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        if val.is_empty()
            || val.contains(',')
            || val.contains('"')
            || val.contains('\\')
            || val.contains('{')
            || val.contains('}')
            || val.contains(' ')
            || val.eq_ignore_ascii_case("NULL")
        {
            buf.push(b'"');
            buf.push(b'"');
            for ch in val.chars() {
                match ch {
                    '"' => buf.extend_from_slice(b"\\\"\""),
                    '\\' => buf.extend_from_slice(b"\\\\"),
                    _ => {
                        let mut utf8_buf = [0u8; 4];
                        buf.extend_from_slice(ch.encode_utf8(&mut utf8_buf).as_bytes());
                    }
                }
            }
            buf.push(b'"');
            buf.push(b'"');
        } else {
            buf.extend_from_slice(val.as_bytes());
        }
    }
    buf.push(b'}');
    buf.push(b'"');
}

/// Convenience: create InsertParams from a JobArgs impl.
pub fn params(args: &impl JobArgs) -> Result<InsertParams, AwaError> {
    params_with(args, InsertOpts::default())
}

/// Convenience: create InsertParams from a JobArgs impl with options.
pub fn params_with(args: &impl JobArgs, opts: InsertOpts) -> Result<InsertParams, AwaError> {
    Ok(InsertParams {
        kind: args.kind_str().to_string(),
        args: args.to_args()?,
        opts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::UniqueOpts;
    use chrono::TimeZone;

    #[derive(serde::Serialize)]
    struct SendEmail {
        to: String,
        subject: String,
    }

    impl JobArgs for SendEmail {
        fn kind() -> &'static str {
            "send_email"
        }
    }

    #[test]
    fn prepare_job_insert_exposes_canonical_adapter_values() {
        let run_at = chrono::Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 0).unwrap();
        let prepared = prepare_job_insert(
            &SendEmail {
                to: "ada@example.com".to_string(),
                subject: "hello".to_string(),
            },
            InsertOpts {
                queue: "email".to_string(),
                priority: 1,
                max_attempts: 3,
                run_at: Some(run_at),
                metadata: serde_json::json!({"source": "test"}),
                tags: vec!["welcome".to_string(), "external".to_string()],
                unique: Some(UniqueOpts {
                    by_queue: true,
                    by_args: true,
                    by_period: Some(60),
                    states: 0b1000_0001,
                }),
                ordering_key: Some(vec![1, 2, 3]),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(prepared.kind(), "send_email");
        assert_eq!(prepared.queue(), "email");
        assert_eq!(prepared.args()["to"], "ada@example.com");
        assert_eq!(prepared.state(), JobState::Scheduled);
        assert_eq!(prepared.state_db_str(), "scheduled");
        assert_eq!(prepared.priority(), 1);
        assert_eq!(prepared.max_attempts(), 3);
        assert_eq!(prepared.run_at(), Some(run_at));
        assert_eq!(prepared.metadata()["source"], "test");
        assert_eq!(prepared.tags(), ["welcome", "external"]);
        assert!(prepared.unique_key().is_some());
        assert_eq!(prepared.unique_states_bit_string(), Some("10000001"));
        assert_eq!(prepared.ordering_key(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn prepare_raw_job_insert_reuses_null_byte_validation() {
        let err = prepare_raw_job_insert(
            "send_email",
            serde_json::json!({"subject": "hello\u{0000}world"}),
            InsertOpts::default(),
        )
        .unwrap_err();

        assert!(matches!(err, AwaError::Validation(_)));
    }

    #[test]
    fn postgres_adapter_sql_uses_driver_friendly_casts() {
        assert!(POSTGRES_INSERT_JOB_SQL.contains("state::text AS state_str"));
        assert!(POSTGRES_INSERT_JOB_SQL.contains("$4::text::awa.job_state"));
        assert!(POSTGRES_INSERT_JOB_SQL.contains("$11::text::bit(8)"));
        assert!(POSTGRES_INSERT_JOB_SQL.contains("$12"));
    }
}
