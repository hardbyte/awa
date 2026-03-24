use crate::error::AwaError;
use crate::job::{InsertOpts, InsertParams, JobRow, JobState};
use crate::unique::compute_unique_key;
use crate::JobArgs;
use sqlx::postgres::PgConnection;
use sqlx::{PgExecutor, PgPool};

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
/// This is the shared internal representation used by all insert paths.
pub(crate) struct PreparedRow {
    pub kind: String,
    pub queue: String,
    pub args: serde_json::Value,
    pub state: JobState,
    pub priority: i16,
    pub max_attempts: i16,
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
    pub metadata: serde_json::Value,
    pub tags: Vec<String>,
    pub unique_key: Option<Vec<u8>>,
    pub unique_states: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TargetTable {
    JobsHot,
    ScheduledJobs,
}

impl TargetTable {
    fn as_str(self) -> &'static str {
        match self {
            TargetTable::JobsHot => "awa.jobs_hot",
            TargetTable::ScheduledJobs => "awa.scheduled_jobs",
        }
    }
}

fn target_table_for_state(state: JobState) -> TargetTable {
    match state {
        JobState::Scheduled | JobState::Retryable => TargetTable::ScheduledJobs,
        _ => TargetTable::JobsHot,
    }
}

fn homogeneous_target_table(rows: &[PreparedRow]) -> Option<TargetTable> {
    let first = rows.first().map(|row| target_table_for_state(row.state))?;
    rows.iter()
        .all(|row| target_table_for_state(row.state) == first)
        .then_some(first)
}

fn map_sqlx_error(err: sqlx::Error) -> AwaError {
    if let sqlx::Error::Database(ref db_err) = err {
        if db_err.code().as_deref() == Some("23505") {
            return AwaError::UniqueConflict {
                constraint: db_err.constraint().map(|c| c.to_string()),
            };
        }
    }
    AwaError::Database(err)
}

fn build_multi_insert_query(target_table: &str, count: usize) -> String {
    let mut query = format!(
        "INSERT INTO {} (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states) VALUES ",
        target_table
    );

    let params_per_row = 11u32;
    let mut param_index = 1u32;
    for i in 0..count {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, COALESCE(${}, now()), ${}, ${}, ${}, ${}::bit(8))",
            param_index,
            param_index + 1,
            param_index + 2,
            param_index + 3,
            param_index + 4,
            param_index + 5,
            param_index + 6,
            param_index + 7,
            param_index + 8,
            param_index + 9,
            param_index + 10,
        ));
        param_index += params_per_row;
    }
    query.push_str(" RETURNING *");
    query
}

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

/// Prepare a single row from typed job args and options.
///
/// Validates null bytes, determines state, computes unique key.
pub(crate) fn prepare_row(args: &impl JobArgs, opts: InsertOpts) -> Result<PreparedRow, AwaError> {
    let kind = args.kind_str().to_string();
    let args_value = args.to_args()?;
    prepare_row_raw(kind, args_value, opts)
}

/// Prepare a single row from raw kind, JSON args, and options.
pub(crate) fn prepare_row_raw(
    kind: String,
    args: serde_json::Value,
    opts: InsertOpts,
) -> Result<PreparedRow, AwaError> {
    reject_null_bytes(&args)?;
    reject_null_bytes(&opts.metadata)?;

    let state = if opts.run_at.is_some() {
        JobState::Scheduled
    } else {
        JobState::Available
    };

    let (unique_key, unique_states) = compute_unique_fields(&kind, &args, &opts);

    Ok(PreparedRow {
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
    })
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
    let query = format!(
        r#"
        INSERT INTO {} (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states)
        VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9, $10, $11::bit(8))
        RETURNING *
        "#,
        target_table_for_state(row.state).as_str()
    );

    sqlx::query_as::<_, JobRow>(&query)
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

/// Insert multiple jobs in a single statement.
///
/// Supports uniqueness constraints — jobs with `unique` opts will have their
/// `unique_key` and `unique_states` computed and included.
#[tracing::instrument(skip(executor, jobs), fields(job.count = jobs.len()))]
pub async fn insert_many<'e, E>(executor: E, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let rows = precompute_rows(jobs)?;
    let target_table = homogeneous_target_table(&rows)
        .map(TargetTable::as_str)
        .unwrap_or("awa.jobs");
    let query = build_multi_insert_query(target_table, rows.len());

    let mut sql_query = sqlx::query_as::<_, JobRow>(&query);

    for row in &rows {
        sql_query = sql_query
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
            .bind(&row.unique_states);
    }

    let results = sql_query.fetch_all(executor).await?;

    Ok(results)
}

/// Insert many jobs using COPY for high throughput.
///
/// Uses a temp staging table with no constraints for fast COPY ingestion,
/// then INSERT...SELECT into `awa.jobs` with ON CONFLICT DO NOTHING for
/// unique jobs. Accepts `&mut PgConnection` so callers can use pool
/// connections or transactions (Transaction derefs to PgConnection).
#[tracing::instrument(skip(conn, jobs), fields(job.count = jobs.len()))]
pub async fn insert_many_copy(
    conn: &mut PgConnection,
    jobs: &[InsertParams],
) -> Result<Vec<JobRow>, AwaError> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let rows = precompute_rows(jobs)?;
    let target_table = homogeneous_target_table(&rows)
        .map(TargetTable::as_str)
        .unwrap_or("awa.jobs");

    // 1. Create temp staging table (dropped on transaction commit)
    sqlx::query(
        r#"
        CREATE TEMP TABLE awa_copy_staging (
            kind        TEXT NOT NULL,
            queue       TEXT NOT NULL,
            args        JSONB NOT NULL,
            state       TEXT NOT NULL,
            priority    SMALLINT NOT NULL,
            max_attempts SMALLINT NOT NULL,
            run_at      TEXT,
            metadata    JSONB NOT NULL,
            tags        TEXT NOT NULL,
            unique_key  TEXT,
            unique_states TEXT
        ) ON COMMIT DROP
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
            "COPY awa_copy_staging (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
        )
        .await?;
    copy_in.send(csv_buf).await?;
    copy_in.finish().await?;

    // 3. INSERT...SELECT from staging into real table
    let has_unique = rows.iter().any(|r| r.unique_key.is_some());

    let results = if has_unique {
        // The compatibility `awa.jobs` surface is now a view backed by hot and
        // deferred tables, so the old `ON CONFLICT` path is no longer available
        // here. Keep COPY for staging/parsing, then insert unique rows one at a
        // time and skip duplicates explicitly.
        let staged_rows = sqlx::query_as::<
            _,
            (
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
            ),
        >(
            r#"
            SELECT
                kind,
                queue,
                args,
                state,
                priority,
                max_attempts,
                CASE WHEN run_at = '\N' OR run_at IS NULL THEN NULL ELSE run_at::timestamptz END,
                metadata,
                tags::text[],
                CASE WHEN unique_key = '\N' OR unique_key IS NULL THEN NULL ELSE decode(unique_key, 'hex') END,
                unique_states
            FROM awa_copy_staging
            "#,
        )
        .fetch_all(&mut *conn)
        .await?;

        let mut inserted = Vec::with_capacity(staged_rows.len());
        for (
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
        ) in staged_rows
        {
            sqlx::query("SAVEPOINT awa_copy_unique_row")
                .execute(&mut *conn)
                .await?;

            let query = format!(
                r#"
                INSERT INTO {} (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states)
                VALUES ($1, $2, $3, $4::awa.job_state, $5, $6, COALESCE($7, now()), $8, $9, $10, $11::bit(8))
                RETURNING *
                "#,
                target_table
            );
            let result = sqlx::query_as::<_, JobRow>(&query)
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
            .fetch_one(&mut *conn)
            .await;

            match result {
                Ok(row) => {
                    inserted.push(row);
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

        inserted
    } else {
        let insert_sql = format!(
            r#"
            INSERT INTO {} (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states)
            SELECT
                s.kind,
                s.queue,
                s.args,
                s.state::awa.job_state,
                s.priority,
                s.max_attempts,
                CASE WHEN s.run_at = '\N' OR s.run_at IS NULL THEN now() ELSE s.run_at::timestamptz END,
                s.metadata,
                s.tags::text[],
                CASE WHEN s.unique_key = '\N' OR s.unique_key IS NULL THEN NULL ELSE decode(s.unique_key, 'hex') END,
                CASE WHEN s.unique_states = '\N' OR s.unique_states IS NULL THEN NULL ELSE s.unique_states::bit(8) END
            FROM awa_copy_staging s
            RETURNING *
        "#,
            target_table
        );

        sqlx::query_as::<_, JobRow>(&insert_sql)
            .fetch_all(&mut *conn)
            .await?
    };

    Ok(results)
}

/// Convenience wrapper that acquires a connection from the pool.
///
/// Wraps the operation in a transaction so the ON COMMIT DROP staging table
/// is cleaned up automatically.
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
    // run_at (TIMESTAMPTZ as RFC 3339, or \N for NULL)
    match &row.run_at {
        Some(dt) => write_csv_field(buf, &dt.to_rfc3339()),
        None => buf.extend_from_slice(b"\\N"),
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
    // unique_key (hex-encoded bytes, or \N for NULL)
    match &row.unique_key {
        Some(key) => {
            let hex = hex::encode(key);
            write_csv_field(buf, &hex);
        }
        None => buf.extend_from_slice(b"\\N"),
    }
    buf.push(b',');
    // unique_states (bit string, or \N for NULL)
    match &row.unique_states {
        Some(bits) => write_csv_field(buf, bits),
        None => buf.extend_from_slice(b"\\N"),
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
