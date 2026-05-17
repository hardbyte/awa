use crate::error::{map_sqlx_error, AwaError};
use crate::job::{InsertOpts, InsertParams, JobRow, JobState};
use crate::unique::compute_unique_key;
use crate::JobArgs;
use sqlx::postgres::PgConnection;
use sqlx::{PgExecutor, PgPool};

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
/// `state::text AS state_str` and `unique_states::text AS unique_states_str`
/// are included for drivers that cannot decode the custom enum or bitmask
/// directly. SQLx callers can ignore the extra columns.
pub(crate) const POSTGRES_INSERT_JOB_SQL: &str = r#"
    SELECT *, state::text AS state_str, unique_states::text AS unique_states_str
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

fn build_multi_insert_query(count: usize) -> String {
    let mut query = String::from(
        "WITH input (ord, kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states, ordering_key) AS (VALUES ",
    );

    let params_per_row = 12u32;
    let mut param_index = 1u32;
    for i in 0..count {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&format!(
            "({}, ${}, ${}::text, ${}::jsonb, ${}::awa.job_state, ${}::smallint, ${}::smallint, ${}::timestamptz, ${}::jsonb, ${}::text[], ${}, ${}::bit(8), ${})",
            i,
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
            param_index + 11,
        ));
        param_index += params_per_row;
    }
    query.push_str(
        ") \
         SELECT inserted.* \
         FROM input \
         CROSS JOIN LATERAL awa.insert_job_compat(\
             input.kind, \
             input.queue, \
             input.args, \
             input.state, \
             input.priority, \
             input.max_attempts, \
             input.run_at, \
             input.metadata, \
             input.tags, \
             input.unique_key, \
             input.unique_states, \
             input.ordering_key\
         ) AS inserted \
         ORDER BY input.ord",
    );
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
    let query = build_multi_insert_query(rows.len());

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
            .bind(&row.unique_states)
            .bind(&row.ordering_key);
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
            ordering_key BYTEA
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
            "COPY pg_temp.awa_copy_staging (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states, ordering_key) FROM STDIN WITH (FORMAT csv, NULL '__AWA_NULL__')",
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
                Option<Vec<u8>>,
            ),
        >(
            r#"
            SELECT
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
            FROM pg_temp.awa_copy_staging
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
        let insert_sql = r#"
            WITH staged AS (
                SELECT
                    *,
                    row_number() OVER () AS ord
                FROM pg_temp.awa_copy_staging
            )
            SELECT inserted.*
            FROM staged
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
        "#;

        sqlx::query_as::<_, JobRow>(insert_sql)
            .fetch_all(&mut *conn)
            .await?
    };

    // Keep the session-local staging table reusable across multiple COPY calls
    // within the same outer transaction.
    sqlx::query("DELETE FROM pg_temp.awa_copy_staging")
        .execute(&mut *conn)
        .await?;

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
        assert!(POSTGRES_INSERT_JOB_SQL.contains("unique_states::text AS unique_states_str"));
        assert!(POSTGRES_INSERT_JOB_SQL.contains("$4::text::awa.job_state"));
        assert!(POSTGRES_INSERT_JOB_SQL.contains("$11::text::bit(8)"));
        assert!(POSTGRES_INSERT_JOB_SQL.contains("$12"));
    }
}
