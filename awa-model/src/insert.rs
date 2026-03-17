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

    let unique_states_bits: Option<String> =
        opts.unique.as_ref().map(|u| format!("{:08b}", u.states));

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
        // Check for unique constraint violation
        if let sqlx::Error::Database(ref db_err) = err {
            if db_err.code().as_deref() == Some("23505") {
                // Try to extract the existing ID from the error
                return AwaError::UniqueConflict { existing_id: 0 };
            }
        }
        AwaError::Database(err)
    })?;

    Ok(row)
}

/// Insert multiple jobs in a single transaction.
pub async fn insert_many<'e, E>(executor: E, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    // Build a bulk INSERT using UNNEST for efficiency
    let count = jobs.len();
    let mut kinds: Vec<String> = Vec::with_capacity(count);
    let mut queues: Vec<String> = Vec::with_capacity(count);
    let mut args_list: Vec<serde_json::Value> = Vec::with_capacity(count);
    let mut states: Vec<JobState> = Vec::with_capacity(count);
    let mut priorities: Vec<i16> = Vec::with_capacity(count);
    let mut max_attempts_list: Vec<i16> = Vec::with_capacity(count);
    let mut run_ats: Vec<Option<chrono::DateTime<chrono::Utc>>> = Vec::with_capacity(count);
    let mut metadata_list: Vec<serde_json::Value> = Vec::with_capacity(count);
    let mut tags_list: Vec<Vec<String>> = Vec::with_capacity(count);

    for job in jobs {
        kinds.push(job.kind.clone());
        queues.push(job.opts.queue.clone());
        args_list.push(job.args.clone());
        states.push(if job.opts.run_at.is_some() {
            JobState::Scheduled
        } else {
            JobState::Available
        });
        priorities.push(job.opts.priority);
        max_attempts_list.push(job.opts.max_attempts);
        run_ats.push(job.opts.run_at);
        metadata_list.push(job.opts.metadata.clone());
        tags_list.push(job.opts.tags.clone());
    }

    // Build a multi-row INSERT with parameterized VALUES.
    let mut query = String::from(
        "INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags) VALUES ",
    );

    let mut param_index = 1u32;
    for i in 0..count {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, COALESCE(${}, now()), ${}, ${})",
            param_index,
            param_index + 1,
            param_index + 2,
            param_index + 3,
            param_index + 4,
            param_index + 5,
            param_index + 6,
            param_index + 7,
            param_index + 8,
        ));
        param_index += 9;
    }
    query.push_str(" RETURNING *");

    let mut sql_query = sqlx::query_as::<_, JobRow>(&query);

    for i in 0..count {
        sql_query = sql_query
            .bind(&kinds[i])
            .bind(&queues[i])
            .bind(&args_list[i])
            .bind(states[i])
            .bind(priorities[i])
            .bind(max_attempts_list[i])
            .bind(run_ats[i])
            .bind(&metadata_list[i])
            .bind(&tags_list[i]);
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
