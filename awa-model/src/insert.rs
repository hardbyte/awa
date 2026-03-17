use crate::error::AwaError;
use crate::job::{InsertOpts, InsertParams, JobRow, JobState};
use crate::unique::compute_unique_key;
use crate::JobArgs;
use sqlx::PgExecutor;

/// Insert a job with default options.
pub async fn insert<'e, E>(executor: E, args: &impl JobArgs) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    insert_with(executor, args, InsertOpts::default()).await
}

/// Insert a job with custom options.
pub async fn insert_with<'e, E>(
    executor: E,
    args: &impl JobArgs,
    opts: InsertOpts,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let kind = args.kind_str();
    let args_json = args.to_args()?;

    let state = if opts.run_at.is_some() {
        JobState::Scheduled
    } else {
        JobState::Available
    };

    let unique_key = opts.unique.as_ref().map(|u| {
        compute_unique_key(
            kind,
            if u.by_queue { Some(&opts.queue) } else { None },
            if u.by_args { Some(&args_json) } else { None },
            u.by_period,
        )
    });

    let unique_states_bits: Option<String> = opts.unique.as_ref().map(|u| {
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

    let row = sqlx::query_as::<_, JobRow>(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states)
        VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9, $10, $11::bit(8))
        RETURNING *
        "#,
    )
    .bind(kind)
    .bind(&opts.queue)
    .bind(&args_json)
    .bind(state)
    .bind(opts.priority)
    .bind(opts.max_attempts)
    .bind(opts.run_at)
    .bind(&opts.metadata)
    .bind(&opts.tags)
    .bind(&unique_key)
    .bind(&unique_states_bits)
    .fetch_one(executor)
    .await
    .map_err(|err| {
        if let sqlx::Error::Database(ref db_err) = err {
            if db_err.code().as_deref() == Some("23505") {
                // Unique constraint violation. The conflicting row ID isn't
                // available from the PG error message directly — callers can
                // query by unique_key if they need it.
                return AwaError::UniqueConflict {
                    existing_id: db_err
                        .constraint()
                        .map(|c| c.to_string()),
                };
            }
        }
        AwaError::Database(err)
    })?;

    Ok(row)
}

/// Insert multiple jobs in a single statement.
///
/// Supports uniqueness constraints — jobs with `unique` opts will have their
/// `unique_key` and `unique_states` computed and included.
pub async fn insert_many<'e, E>(executor: E, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let count = jobs.len();

    // Pre-compute all values including unique keys
    struct RowValues {
        kind: String,
        queue: String,
        args: serde_json::Value,
        state: JobState,
        priority: i16,
        max_attempts: i16,
        run_at: Option<chrono::DateTime<chrono::Utc>>,
        metadata: serde_json::Value,
        tags: Vec<String>,
        unique_key: Option<Vec<u8>>,
        unique_states: Option<String>,
    }

    let rows: Vec<RowValues> = jobs
        .iter()
        .map(|job| {
            let unique_key = job.opts.unique.as_ref().map(|u| {
                compute_unique_key(
                    &job.kind,
                    if u.by_queue {
                        Some(job.opts.queue.as_str())
                    } else {
                        None
                    },
                    if u.by_args { Some(&job.args) } else { None },
                    u.by_period,
                )
            });

            let unique_states = job.opts.unique.as_ref().map(|u| {
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

            RowValues {
                kind: job.kind.clone(),
                queue: job.opts.queue.clone(),
                args: job.args.clone(),
                state: if job.opts.run_at.is_some() {
                    JobState::Scheduled
                } else {
                    JobState::Available
                },
                priority: job.opts.priority,
                max_attempts: job.opts.max_attempts,
                run_at: job.opts.run_at,
                metadata: job.opts.metadata.clone(),
                tags: job.opts.tags.clone(),
                unique_key,
                unique_states,
            }
        })
        .collect();

    // Build multi-row INSERT with all columns including unique_key/unique_states
    let mut query = String::from(
        "INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states) VALUES ",
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
