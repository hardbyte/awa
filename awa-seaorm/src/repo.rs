use crate::mapping::{job_row_from_query_result, job_rows_from_query_results, map_db_err};
use crate::sql::JOB_COLUMNS;
use awa::adapter::postgres::{
    prepare_job_insert, prepare_raw_job_insert, PreparedJobInsert, INSERT_JOB_SQL,
};
use awa::{
    AwaError, CallbackConfig, CallbackPollResult, DefaultAction, DlqMetadata, InsertOpts,
    InsertParams, JobArgs, JobDump, JobRow, JobState, ListJobsFilter, ResolveOutcome, RunDump,
};
use sea_orm::{ConnectionTrait, Statement, TransactionSession, TransactionTrait};
use uuid::Uuid;

/// SeaORM-backed repository for Awa job enqueue and lifecycle operations.
pub struct JobRepository<'db, C> {
    db: &'db C,
}

impl<'db, C> JobRepository<'db, C>
where
    C: ConnectionTrait,
{
    /// Create a repository over a SeaORM connection or transaction.
    pub fn new(db: &'db C) -> Self {
        Self { db }
    }

    /// Insert a job using the type-provided job kind and serialized arguments.
    pub async fn insert(&self, args: &impl JobArgs) -> Result<JobRow, AwaError> {
        self.insert_with(args, InsertOpts::default()).await
    }

    /// Insert a job with explicit enqueue options.
    pub async fn insert_with(
        &self,
        args: &impl JobArgs,
        opts: InsertOpts,
    ) -> Result<JobRow, AwaError> {
        let prepared = prepare_job_insert(args, opts)?;
        self.insert_prepared(&prepared).await
    }

    /// Insert a job from a raw kind name, JSON-compatible arguments, and options.
    pub async fn insert_raw(
        &self,
        kind: impl Into<String>,
        args: impl Into<serde_json::Value>,
        opts: InsertOpts,
    ) -> Result<JobRow, AwaError> {
        let prepared = prepare_raw_job_insert(kind, args, opts)?;
        self.insert_prepared(&prepared).await
    }

    /// Fetch a job by id.
    pub async fn get_job(&self, job_id: i64) -> Result<JobRow, AwaError> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM awa.jobs WHERE id = $1");
        self.query_optional_job(&sql, vec![job_id.into()])
            .await?
            .ok_or(AwaError::JobNotFound { id: job_id })
    }

    /// List jobs using the same filter shape as Awa admin helpers.
    pub async fn list_jobs(&self, filter: &ListJobsFilter) -> Result<Vec<JobRow>, AwaError> {
        let state = filter.state.map(|state| state.as_str().to_string());
        let limit = filter.limit.unwrap_or(100).clamp(1, 1000);
        let sql = format!(
            r#"
            SELECT {JOB_COLUMNS}
            FROM awa.jobs
            WHERE ($1::text IS NULL OR state = $1::text::awa.job_state)
              AND ($2::text IS NULL OR kind = $2)
              AND ($3::text IS NULL OR queue = $3)
              AND ($4::text IS NULL OR tags @> ARRAY[$4]::text[])
              AND ($5::bigint IS NULL OR id < $5)
            ORDER BY id DESC
            LIMIT $6
            "#
        );
        self.query_jobs(
            &sql,
            vec![
                state.into(),
                filter.kind.clone().into(),
                filter.queue.clone().into(),
                filter.tag.clone().into(),
                filter.before_id.into(),
                limit.into(),
            ],
        )
        .await
    }

    /// Build an administrative dump for one job, including DLQ metadata when present.
    pub async fn dump_job(&self, job_id: i64) -> Result<JobDump, AwaError> {
        let job = self.get_job(job_id).await?;
        let dlq = self.get_dlq_metadata(job_id).await?;
        Ok(awa::admin::build_job_dump_from_row(job, dlq))
    }

    /// Build an administrative run dump for the current or selected attempt.
    pub async fn dump_run(&self, job_id: i64, attempt: Option<i16>) -> Result<RunDump, AwaError> {
        let job = self.get_job(job_id).await?;
        awa::admin::build_run_dump_from_row(&job, attempt)
    }

    /// Retry a failed, cancelled, or external-waiting job.
    pub async fn retry(&self, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'available', attempt = 0, run_at = now(),
                finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL,
                callback_id = NULL, callback_timeout_at = NULL,
                callback_filter = NULL, callback_on_complete = NULL,
                callback_on_fail = NULL, callback_transform = NULL
            WHERE id = $1 AND state IN ('failed', 'cancelled', 'waiting_external')
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_optional_job(&sql, vec![job_id.into()])
            .await?
            .ok_or(AwaError::JobNotFound { id: job_id })
            .map(Some)
    }

    /// Cancel a non-terminal job and notify running or externally waiting workers.
    pub async fn cancel(&self, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let sql = format!(
            r#"
            WITH target AS (
                SELECT id, state AS prior_state
                FROM awa.jobs
                WHERE id = $1
                FOR UPDATE
            ),
            updated AS (
                UPDATE awa.jobs AS jobs
                SET state = 'cancelled', finalized_at = now(),
                    callback_id = NULL, callback_timeout_at = NULL,
                    callback_filter = NULL, callback_on_complete = NULL,
                    callback_on_fail = NULL, callback_transform = NULL
                FROM target
                WHERE jobs.id = target.id
                  AND jobs.state NOT IN ('completed', 'failed', 'cancelled')
                RETURNING jobs.*, target.prior_state
            ),
            notified AS (
                SELECT pg_notify(
                    'awa:cancel',
                    json_build_object('job_id', id, 'run_lease', run_lease)::text
                )
                FROM updated
                WHERE prior_state IN ('running', 'waiting_external')
            )
            SELECT {JOB_COLUMNS}
            FROM updated
            "#
        );
        self.query_optional_job(&sql, vec![job_id.into()])
            .await?
            .ok_or(AwaError::JobNotFound { id: job_id })
            .map(Some)
    }

    /// Retry all retryable jobs in the provided id list.
    pub async fn bulk_retry(&self, ids: &[i64]) -> Result<Vec<JobRow>, AwaError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'available', attempt = 0, run_at = now(),
                finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL,
                callback_id = NULL, callback_timeout_at = NULL,
                callback_filter = NULL, callback_on_complete = NULL,
                callback_on_fail = NULL, callback_transform = NULL
            WHERE id = ANY($1) AND state IN ('failed', 'cancelled', 'waiting_external')
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_jobs(&sql, vec![ids.to_vec().into()]).await
    }

    /// Cancel all non-terminal jobs in the provided id list.
    pub async fn bulk_cancel(&self, ids: &[i64]) -> Result<Vec<JobRow>, AwaError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'cancelled', finalized_at = now(),
                callback_id = NULL, callback_timeout_at = NULL,
                callback_filter = NULL, callback_on_complete = NULL,
                callback_on_fail = NULL, callback_transform = NULL
            WHERE id = ANY($1) AND state NOT IN ('completed', 'failed', 'cancelled')
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_jobs(&sql, vec![ids.to_vec().into()]).await
    }

    /// Cancel the first active job matching the computed unique key.
    pub async fn cancel_by_unique_key(
        &self,
        kind: &str,
        queue: Option<&str>,
        args: Option<&serde_json::Value>,
        period_bucket: Option<i64>,
    ) -> Result<Option<JobRow>, AwaError> {
        let unique_key = awa::model::unique::compute_unique_key(kind, queue, args, period_bucket);
        let sql = r#"
            WITH candidates AS (
                SELECT id FROM awa.jobs_hot
                WHERE unique_key = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
                UNION ALL
                SELECT id FROM awa.scheduled_jobs
                WHERE unique_key = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
                ORDER BY id ASC
                LIMIT 1
            )
            SELECT id FROM candidates
        "#;
        let candidate = self
            .query_optional_i64(sql, vec![unique_key.into()])
            .await?;
        match candidate {
            Some(job_id) => match self.cancel(job_id).await {
                Ok(row) => Ok(row),
                Err(AwaError::JobNotFound { .. }) => Ok(None),
                Err(err) => Err(err),
            },
            None => Ok(None),
        }
    }

    /// Retry failed jobs with the given kind.
    pub async fn retry_failed_by_kind(&self, kind: &str) -> Result<Vec<JobRow>, AwaError> {
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'available', attempt = 0, run_at = now(),
                finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
            WHERE kind = $1 AND state = 'failed'
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_jobs(&sql, vec![kind.into()]).await
    }

    /// Retry failed jobs in the given queue.
    pub async fn retry_failed_by_queue(&self, queue: &str) -> Result<Vec<JobRow>, AwaError> {
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'available', attempt = 0, run_at = now(),
                finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
            WHERE queue = $1 AND state = 'failed'
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_jobs(&sql, vec![queue.into()]).await
    }

    /// Delete failed jobs with the given kind and return the affected row count.
    pub async fn discard_failed(&self, kind: &str) -> Result<u64, AwaError> {
        self.execute(
            "DELETE FROM awa.jobs WHERE kind = $1 AND state = 'failed'",
            vec![kind.into()],
        )
        .await
    }

    /// Mark a queue as paused.
    pub async fn pause_queue(&self, queue: &str, paused_by: Option<&str>) -> Result<(), AwaError> {
        self.execute(
            r#"
            INSERT INTO awa.queue_meta (queue, paused, paused_at, paused_by)
            VALUES ($1, TRUE, now(), $2)
            ON CONFLICT (queue) DO UPDATE
            SET paused = TRUE, paused_at = now(), paused_by = $2
            "#,
            vec![queue.into(), paused_by.into()],
        )
        .await?;
        Ok(())
    }

    /// Mark a queue as resumed.
    pub async fn resume_queue(&self, queue: &str) -> Result<(), AwaError> {
        self.execute(
            "UPDATE awa.queue_meta SET paused = FALSE WHERE queue = $1",
            vec![queue.into()],
        )
        .await?;
        Ok(())
    }

    /// Cancel all queued or waiting jobs in a queue.
    pub async fn drain_queue(&self, queue: &str) -> Result<u64, AwaError> {
        self.execute(
            r#"
            UPDATE awa.jobs
            SET state = 'cancelled', finalized_at = now(),
                callback_id = NULL, callback_timeout_at = NULL,
                callback_filter = NULL, callback_on_complete = NULL,
                callback_on_fail = NULL, callback_transform = NULL
            WHERE queue = $1
              AND state IN ('available', 'scheduled', 'retryable', 'waiting_external')
            "#,
            vec![queue.into()],
        )
        .await
    }

    /// Register a callback token for a running job.
    pub async fn register_callback(
        &self,
        job_id: i64,
        run_lease: i64,
        timeout: std::time::Duration,
    ) -> Result<Uuid, AwaError> {
        self.register_callback_with_config(job_id, run_lease, timeout, &CallbackConfig::default())
            .await
    }

    /// Register a callback token and optional CEL resolution configuration.
    pub async fn register_callback_with_config(
        &self,
        job_id: i64,
        run_lease: i64,
        timeout: std::time::Duration,
        config: &CallbackConfig,
    ) -> Result<Uuid, AwaError> {
        #[cfg(not(feature = "cel"))]
        if !config.is_empty() {
            return Err(AwaError::Validation(
                "CEL expressions require the 'cel' feature".into(),
            ));
        }

        let callback_id = Uuid::new_v4();
        let timeout_secs = timeout.as_secs_f64();
        let updated = self
            .execute(
                r#"
                UPDATE awa.jobs
                SET callback_id = $2,
                    callback_timeout_at = now() + make_interval(secs => $3),
                    callback_filter = $4,
                    callback_on_complete = $5,
                    callback_on_fail = $6,
                    callback_transform = $7
                WHERE id = $1 AND state = 'running' AND run_lease = $8
                "#,
                vec![
                    job_id.into(),
                    callback_id.into(),
                    timeout_secs.into(),
                    config.filter.clone().into(),
                    config.on_complete.clone().into(),
                    config.on_fail.clone().into(),
                    config.transform.clone().into(),
                    run_lease.into(),
                ],
            )
            .await?;
        if updated == 0 {
            return Err(AwaError::Validation("job is not in running state".into()));
        }
        Ok(callback_id)
    }

    /// Complete an external callback without resuming the worker.
    pub async fn complete_external(
        &self,
        callback_id: Uuid,
        payload: Option<serde_json::Value>,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        self.complete_external_inner(callback_id, payload, run_lease, false)
            .await
    }

    /// Resume a job from external wait and store the callback payload for polling.
    pub async fn resume_external(
        &self,
        callback_id: Uuid,
        payload: Option<serde_json::Value>,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        self.complete_external_inner(callback_id, payload, run_lease, true)
            .await
    }

    /// Mark an externally waiting or running callback job as failed.
    pub async fn fail_external(
        &self,
        callback_id: Uuid,
        error: &str,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'failed',
                finalized_at = now(),
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                heartbeat_at = NULL,
                deadline_at = NULL,
                errors = errors || jsonb_build_object(
                    'error', $2::text,
                    'attempt', attempt,
                    'at', now()
                )::jsonb
            WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
              AND ($3::bigint IS NULL OR run_lease = $3)
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_optional_job(
            &sql,
            vec![callback_id.into(), error.into(), run_lease.into()],
        )
        .await?
        .ok_or_else(|| callback_not_found(callback_id))
    }

    /// Return an externally waiting callback job to the available queue.
    pub async fn retry_external(
        &self,
        callback_id: Uuid,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'available',
                attempt = 0,
                run_at = now(),
                finalized_at = NULL,
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                heartbeat_at = NULL,
                deadline_at = NULL
            WHERE callback_id = $1 AND state = 'waiting_external'
              AND ($2::bigint IS NULL OR run_lease = $2)
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_optional_job(&sql, vec![callback_id.into(), run_lease.into()])
            .await?
            .ok_or_else(|| callback_not_found(callback_id))
    }

    /// Extend the timeout for an externally waiting callback.
    pub async fn heartbeat_callback(
        &self,
        callback_id: Uuid,
        timeout: std::time::Duration,
    ) -> Result<JobRow, AwaError> {
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET callback_timeout_at = now() + make_interval(secs => $2)
            WHERE callback_id = $1 AND state = 'waiting_external'
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_optional_job(&sql, vec![callback_id.into(), timeout.as_secs_f64().into()])
            .await?
            .ok_or_else(|| callback_not_found(callback_id))
    }

    /// Clear a running job's registered callback token.
    pub async fn cancel_callback(&self, job_id: i64, run_lease: i64) -> Result<bool, AwaError> {
        let updated = self
            .execute(
                r#"
                UPDATE awa.jobs
                SET callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL
                WHERE id = $1 AND callback_id IS NOT NULL
                  AND state = 'running' AND run_lease = $2
                "#,
                vec![job_id.into(), run_lease.into()],
            )
            .await?;
        Ok(updated > 0)
    }

    /// Move a running job with a registered callback into external wait.
    pub async fn enter_callback_wait(
        &self,
        job_id: i64,
        run_lease: i64,
        callback_id: Uuid,
    ) -> Result<bool, AwaError> {
        let updated = self
            .execute(
                r#"
                UPDATE awa.jobs
                SET state = 'waiting_external',
                    heartbeat_at = NULL,
                    deadline_at = NULL
                WHERE id = $1 AND state = 'running'
                  AND run_lease = $2 AND callback_id = $3
                "#,
                vec![job_id.into(), run_lease.into(), callback_id.into()],
            )
            .await?;
        Ok(updated > 0)
    }

    /// Poll the current state for a callback token.
    pub async fn check_callback_state(
        &self,
        job_id: i64,
        callback_id: Uuid,
    ) -> Result<CallbackPollResult, AwaError> {
        let sql = r#"
            SELECT state::text AS state_str, callback_id, metadata
            FROM awa.jobs
            WHERE id = $1
        "#;
        let row = self.query_optional(sql, vec![job_id.into()]).await?;
        let Some(row) = row else {
            return Ok(CallbackPollResult::NotFound);
        };
        let state_str: String = row
            .try_get("", "state_str")
            .map_err(|err| AwaError::Validation(format!("failed to decode state: {err}")))?;
        let state = state_str
            .parse::<JobState>()
            .map_err(AwaError::Validation)?;
        let current: Option<Uuid> = row
            .try_get("", "callback_id")
            .map_err(|err| AwaError::Validation(format!("failed to decode callback_id: {err}")))?;
        let metadata: serde_json::Value = row
            .try_get("", "metadata")
            .map_err(|err| AwaError::Validation(format!("failed to decode metadata: {err}")))?;

        match (state, current) {
            (JobState::Running, None) if metadata.get("_awa_callback_result").is_some() => {
                let payload = self.take_callback_payload(job_id, metadata).await?;
                Ok(CallbackPollResult::Resolved(payload))
            }
            (state, Some(current)) if current != callback_id => Ok(CallbackPollResult::Stale {
                token: callback_id,
                current,
                state,
            }),
            (JobState::WaitingExternal, Some(current)) if current == callback_id => {
                Ok(CallbackPollResult::Pending)
            }
            (state, _) => Ok(CallbackPollResult::UnexpectedState {
                token: callback_id,
                state,
            }),
        }
    }

    /// Take and clear a stored callback result payload from job metadata.
    pub async fn take_callback_payload(
        &self,
        job_id: i64,
        metadata: serde_json::Value,
    ) -> Result<serde_json::Value, AwaError> {
        let payload = metadata
            .get("_awa_callback_result")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        self.execute(
            "UPDATE awa.jobs SET metadata = metadata - '_awa_callback_result' WHERE id = $1",
            vec![job_id.into()],
        )
        .await?;
        Ok(payload)
    }

    async fn insert_prepared(&self, prepared: &PreparedJobInsert) -> Result<JobRow, AwaError> {
        let unique_key = prepared.unique_key().map(<[u8]>::to_vec);
        let unique_states = prepared.unique_states_bit_string().map(ToOwned::to_owned);
        let ordering_key = prepared.ordering_key().map(<[u8]>::to_vec);
        self.query_one_job(
            INSERT_JOB_SQL,
            vec![
                prepared.kind().into(),
                prepared.queue().into(),
                prepared.args().clone().into(),
                prepared.state_db_str().into(),
                prepared.priority().into(),
                prepared.max_attempts().into(),
                prepared.run_at().into(),
                prepared.metadata().clone().into(),
                prepared.tags().to_vec().into(),
                unique_key.into(),
                unique_states.into(),
                ordering_key.into(),
            ],
        )
        .await
    }

    async fn complete_external_inner(
        &self,
        callback_id: Uuid,
        payload: Option<serde_json::Value>,
        run_lease: Option<i64>,
        resume: bool,
    ) -> Result<JobRow, AwaError> {
        let sql = if resume {
            format!(
                r#"
                UPDATE awa.jobs
                SET state = 'running',
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    heartbeat_at = now(),
                    metadata = metadata || jsonb_build_object('_awa_callback_result', $3::jsonb)
                WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
                  AND ($2::bigint IS NULL OR run_lease = $2)
                RETURNING {JOB_COLUMNS}
                "#
            )
        } else {
            format!(
                r#"
                UPDATE awa.jobs
                SET state = 'completed',
                    finalized_at = now(),
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    heartbeat_at = NULL,
                    deadline_at = NULL,
                    progress = NULL
                WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
                  AND ($2::bigint IS NULL OR run_lease = $2)
                RETURNING {JOB_COLUMNS}
                "#
            )
        };
        let values = if resume {
            let payload = payload.unwrap_or(serde_json::Value::Null);
            vec![callback_id.into(), run_lease.into(), payload.into()]
        } else {
            vec![callback_id.into(), run_lease.into()]
        };
        self.query_optional_job(&sql, values)
            .await?
            .ok_or_else(|| callback_not_found(callback_id))
    }

    async fn fail_external_with_error_entry(
        &self,
        callback_id: Uuid,
        error_json: serde_json::Value,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let sql = format!(
            r#"
            UPDATE awa.jobs
            SET state = 'failed',
                finalized_at = now(),
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                heartbeat_at = NULL,
                deadline_at = NULL,
                errors = errors || $2::jsonb
            WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
              AND ($3::bigint IS NULL OR run_lease = $3)
            RETURNING {JOB_COLUMNS}
            "#
        );
        self.query_optional_job(
            &sql,
            vec![callback_id.into(), error_json.into(), run_lease.into()],
        )
        .await?
        .ok_or_else(|| callback_not_found(callback_id))
    }

    async fn query_one_job(
        &self,
        sql: &str,
        values: Vec<sea_orm::Value>,
    ) -> Result<JobRow, AwaError> {
        self.query_optional_job(sql, values)
            .await?
            .ok_or_else(|| AwaError::Validation("query did not return a job row".into()))
    }

    async fn query_optional_job(
        &self,
        sql: &str,
        values: Vec<sea_orm::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        self.query_optional(sql, values)
            .await?
            .as_ref()
            .map(job_row_from_query_result)
            .transpose()
    }

    async fn query_jobs(
        &self,
        sql: &str,
        values: Vec<sea_orm::Value>,
    ) -> Result<Vec<JobRow>, AwaError> {
        let rows = self
            .db
            .query_all_raw(statement(self.db, sql, values))
            .await
            .map_err(map_db_err)?;
        job_rows_from_query_results(rows)
    }

    async fn query_optional(
        &self,
        sql: &str,
        values: Vec<sea_orm::Value>,
    ) -> Result<Option<sea_orm::QueryResult>, AwaError> {
        self.db
            .query_one_raw(statement(self.db, sql, values))
            .await
            .map_err(map_db_err)
    }

    async fn query_optional_i64(
        &self,
        sql: &str,
        values: Vec<sea_orm::Value>,
    ) -> Result<Option<i64>, AwaError> {
        self.query_optional(sql, values)
            .await?
            .map(|row| {
                row.try_get("", "id").map_err(|err| {
                    AwaError::Validation(format!("failed to decode column id: {err}"))
                })
            })
            .transpose()
    }

    async fn active_queue_storage_schema(&self) -> Result<Option<String>, AwaError> {
        self.query_optional(
            "SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'",
            Vec::new(),
        )
        .await?
        .map(|row| {
            row.try_get("", "schema_name").map_err(|err| {
                AwaError::Validation(format!("failed to decode column schema_name: {err}"))
            })
        })
        .transpose()
    }

    async fn get_dlq_metadata(&self, job_id: i64) -> Result<Option<DlqMetadata>, AwaError> {
        let Some(schema) = self.active_queue_storage_schema().await? else {
            return Ok(None);
        };
        let schema = quote_identifier(&schema);
        let sql = format!(
            r#"
            SELECT dlq_reason, dlq_at, original_run_lease
            FROM {schema}.dlq_entries
            WHERE job_id = $1
            ORDER BY dlq_at DESC
            LIMIT 1
            "#
        );

        self.query_optional(&sql, vec![job_id.into()])
            .await?
            .map(|row| {
                Ok(DlqMetadata {
                    reason: row.try_get("", "dlq_reason").map_err(|err| {
                        AwaError::Validation(format!("failed to decode column dlq_reason: {err}"))
                    })?,
                    dlq_at: row.try_get("", "dlq_at").map_err(|err| {
                        AwaError::Validation(format!("failed to decode column dlq_at: {err}"))
                    })?,
                    original_run_lease: row.try_get("", "original_run_lease").map_err(|err| {
                        AwaError::Validation(format!(
                            "failed to decode column original_run_lease: {err}"
                        ))
                    })?,
                })
            })
            .transpose()
    }

    async fn execute(&self, sql: &str, values: Vec<sea_orm::Value>) -> Result<u64, AwaError> {
        self.db
            .execute_raw(statement(self.db, sql, values))
            .await
            .map(|result| result.rows_affected())
            .map_err(map_db_err)
    }
}

impl<'db, C> JobRepository<'db, C>
where
    C: ConnectionTrait + TransactionTrait,
{
    /// Insert multiple jobs atomically in a SeaORM transaction.
    pub async fn insert_many(&self, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self.db.begin().await.map_err(map_db_err)?;
        let result: Result<Vec<JobRow>, AwaError> = async {
            let repo = JobRepository::new(&txn);
            let mut rows = Vec::with_capacity(jobs.len());
            for job in jobs {
                rows.push(
                    repo.insert_raw(job.kind.clone(), job.args.clone(), job.opts.clone())
                        .await?,
                );
            }
            Ok(rows)
        }
        .await;

        match result {
            Ok(rows) => {
                txn.commit().await.map_err(map_db_err)?;
                Ok(rows)
            }
            Err(err) => {
                txn.rollback().await.map_err(map_db_err)?;
                Err(err)
            }
        }
    }

    /// Resolve a callback using configured expressions inside one transaction.
    pub async fn resolve_callback(
        &self,
        callback_id: Uuid,
        payload: Option<serde_json::Value>,
        default_action: DefaultAction,
        run_lease: Option<i64>,
    ) -> Result<ResolveOutcome, AwaError> {
        let txn = self.db.begin().await.map_err(map_db_err)?;
        let repo = JobRepository::new(&txn);
        let sql = format!(
            r#"
            SELECT {JOB_COLUMNS}
            FROM awa.jobs_hot
            WHERE callback_id = $1
              AND state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR run_lease = $2)
            FOR UPDATE
            "#
        );
        let job = repo
            .query_optional_job(&sql, vec![callback_id.into(), run_lease.into()])
            .await?
            .ok_or_else(|| callback_not_found(callback_id))?;

        let action = awa::admin::evaluate_callback_resolution(&job, &payload, default_action)?;
        let outcome = match action {
            awa::CallbackResolutionAction::Complete(transformed_payload) => {
                let completed = repo.complete_external(callback_id, None, run_lease).await?;
                ResolveOutcome::Completed {
                    payload: transformed_payload,
                    job: completed,
                }
            }
            awa::CallbackResolutionAction::Fail { error, expression } => {
                let mut error_json = serde_json::json!({
                    "error": error,
                    "attempt": job.attempt,
                    "at": chrono::Utc::now().to_rfc3339(),
                });
                if let Some(expression) = expression {
                    error_json["expression"] = serde_json::Value::String(expression);
                }
                let failed = repo
                    .fail_external_with_error_entry(callback_id, error_json, run_lease)
                    .await?;
                ResolveOutcome::Failed { job: failed }
            }
            awa::CallbackResolutionAction::Ignore(reason) => ResolveOutcome::Ignored { reason },
        };
        txn.commit().await.map_err(map_db_err)?;
        Ok(outcome)
    }
}

fn statement<C>(db: &C, sql: &str, values: Vec<sea_orm::Value>) -> Statement
where
    C: ConnectionTrait,
{
    Statement::from_sql_and_values(db.get_database_backend(), sql, values)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn callback_not_found(callback_id: Uuid) -> AwaError {
    AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    }
}
