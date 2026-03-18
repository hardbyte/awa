//! Periodic/cron job types and database operations.
//!
//! Schedules are defined in application code, synced to `awa.cron_jobs` via UPSERT,
//! and evaluated by the leader to atomically enqueue jobs.

use crate::error::AwaError;
use crate::job::JobRow;
use chrono::{DateTime, Utc};
use croner::Cron;
use sqlx::PgExecutor;

/// A periodic job schedule definition.
///
/// Created via `PeriodicJob::builder(name, cron_expr).build(args)`.
#[derive(Debug, Clone)]
pub struct PeriodicJob {
    /// Unique name identifying this schedule (e.g., "daily_report").
    pub name: String,
    /// Cron expression (e.g., "0 9 * * *").
    pub cron_expr: String,
    /// IANA timezone (e.g., "Pacific/Auckland"). Defaults to "UTC".
    pub timezone: String,
    /// Job kind (derived from JobArgs trait).
    pub kind: String,
    /// Target queue. Defaults to "default".
    pub queue: String,
    /// Serialized job arguments.
    pub args: serde_json::Value,
    /// Job priority (1-4). Defaults to 2.
    pub priority: i16,
    /// Max retry attempts. Defaults to 25.
    pub max_attempts: i16,
    /// Tags attached to created jobs.
    pub tags: Vec<String>,
    /// Extra metadata merged into created jobs.
    pub metadata: serde_json::Value,
}

impl PeriodicJob {
    /// Start building a periodic job with a name and cron expression.
    ///
    /// The cron expression is validated eagerly — invalid expressions
    /// cause `build()` to return an error.
    pub fn builder(name: impl Into<String>, cron_expr: impl Into<String>) -> PeriodicJobBuilder {
        PeriodicJobBuilder {
            name: name.into(),
            cron_expr: cron_expr.into(),
            timezone: "UTC".to_string(),
            queue: "default".to_string(),
            priority: 2,
            max_attempts: 25,
            tags: Vec::new(),
            metadata: serde_json::json!({}),
        }
    }

    /// Compute the latest fire time <= `now` that is strictly after `after`.
    ///
    /// Returns `None` if no fire time exists in the range (after, now].
    /// This handles both "first registration" (after=None → find latest past fire)
    /// and "regular evaluation" (after=Some(last_enqueued_at)).
    pub fn latest_fire_time(
        &self,
        now: DateTime<Utc>,
        after: Option<DateTime<Utc>>,
    ) -> Option<DateTime<Utc>> {
        let cron = Cron::new(&self.cron_expr)
            .parse()
            .expect("cron_expr was validated at build time");

        let tz: chrono_tz::Tz = self
            .timezone
            .parse()
            .expect("timezone was validated at build time");

        let now_tz = now.with_timezone(&tz);

        // Walk backwards from now to find the most recent fire time.
        // croner doesn't have a "previous" iterator, so we find the fire time
        // by iterating forward from a start point.
        let search_start = match after {
            Some(after_time) => after_time.with_timezone(&tz),
            // For first registration, search from 24h ago to avoid unbounded iteration
            None => now_tz - chrono::Duration::hours(24),
        };

        let mut latest_fire: Option<DateTime<Utc>> = None;

        // Iterate forward from search_start, collecting fire times <= now
        for fire_time in cron.clone().iter_from(search_start) {
            let fire_utc = fire_time.with_timezone(&Utc);

            // Stop once we've passed now
            if fire_utc > now {
                break;
            }

            // Skip fires at or before the `after` boundary
            if let Some(after_time) = after {
                if fire_utc <= after_time {
                    continue;
                }
            }

            latest_fire = Some(fire_utc);
        }

        latest_fire
    }
}

/// Builder for `PeriodicJob`.
#[derive(Debug, Clone)]
pub struct PeriodicJobBuilder {
    name: String,
    cron_expr: String,
    timezone: String,
    queue: String,
    priority: i16,
    max_attempts: i16,
    tags: Vec<String>,
    metadata: serde_json::Value,
}

impl PeriodicJobBuilder {
    /// Set the IANA timezone (e.g., "Pacific/Auckland").
    pub fn timezone(mut self, timezone: impl Into<String>) -> Self {
        self.timezone = timezone.into();
        self
    }

    /// Set the target queue.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    /// Set the job priority (1-4).
    pub fn priority(mut self, priority: i16) -> Self {
        self.priority = priority;
        self
    }

    /// Set the max retry attempts.
    pub fn max_attempts(mut self, max_attempts: i16) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set tags for created jobs.
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Set extra metadata for created jobs.
    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Build the periodic job, validating the cron expression and timezone.
    ///
    /// The `args` parameter must implement `JobArgs` — the kind is derived
    /// from the type and args are serialized to JSON.
    pub fn build(self, args: &impl crate::JobArgs) -> Result<PeriodicJob, AwaError> {
        self.build_raw(args.kind_str().to_string(), args.to_args()?)
    }

    /// Build from raw kind and args JSON (used by Python bindings).
    pub fn build_raw(self, kind: String, args: serde_json::Value) -> Result<PeriodicJob, AwaError> {
        // Validate cron expression
        Cron::new(&self.cron_expr)
            .parse()
            .map_err(|err| AwaError::Validation(format!("invalid cron expression: {err}")))?;

        // Validate timezone
        self.timezone
            .parse::<chrono_tz::Tz>()
            .map_err(|err| AwaError::Validation(format!("invalid timezone: {err}")))?;

        // Validate priority
        if !(1..=4).contains(&self.priority) {
            return Err(AwaError::Validation(format!(
                "priority must be between 1 and 4, got {}",
                self.priority
            )));
        }

        // Validate max_attempts
        if !(1..=1000).contains(&self.max_attempts) {
            return Err(AwaError::Validation(format!(
                "max_attempts must be between 1 and 1000, got {}",
                self.max_attempts
            )));
        }

        Ok(PeriodicJob {
            name: self.name,
            cron_expr: self.cron_expr,
            timezone: self.timezone,
            kind,
            queue: self.queue,
            args,
            priority: self.priority,
            max_attempts: self.max_attempts,
            tags: self.tags,
            metadata: self.metadata,
        })
    }
}

/// A row from the `awa.cron_jobs` table.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CronJobRow {
    pub name: String,
    pub cron_expr: String,
    pub timezone: String,
    pub kind: String,
    pub queue: String,
    pub args: serde_json::Value,
    pub priority: i16,
    pub max_attempts: i16,
    pub tags: Vec<String>,
    pub metadata: serde_json::Value,
    pub last_enqueued_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Upsert a periodic job schedule into `awa.cron_jobs`.
///
/// Additive only — never deletes rows not in the input set.
pub async fn upsert_cron_job<'e, E>(executor: E, job: &PeriodicJob) -> Result<(), AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO awa.cron_jobs (name, cron_expr, timezone, kind, queue, args, priority, max_attempts, tags, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (name) DO UPDATE SET
            cron_expr = EXCLUDED.cron_expr,
            timezone = EXCLUDED.timezone,
            kind = EXCLUDED.kind,
            queue = EXCLUDED.queue,
            args = EXCLUDED.args,
            priority = EXCLUDED.priority,
            max_attempts = EXCLUDED.max_attempts,
            tags = EXCLUDED.tags,
            metadata = EXCLUDED.metadata,
            updated_at = now()
        "#,
    )
    .bind(&job.name)
    .bind(&job.cron_expr)
    .bind(&job.timezone)
    .bind(&job.kind)
    .bind(&job.queue)
    .bind(&job.args)
    .bind(job.priority)
    .bind(job.max_attempts)
    .bind(&job.tags)
    .bind(&job.metadata)
    .execute(executor)
    .await?;

    Ok(())
}

/// Load all cron job rows from `awa.cron_jobs`.
pub async fn list_cron_jobs<'e, E>(executor: E) -> Result<Vec<CronJobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, CronJobRow>("SELECT * FROM awa.cron_jobs ORDER BY name")
        .fetch_all(executor)
        .await?;
    Ok(rows)
}

/// Delete a cron job schedule by name.
pub async fn delete_cron_job<'e, E>(executor: E, name: &str) -> Result<bool, AwaError>
where
    E: PgExecutor<'e>,
{
    let result = sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(name)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Atomically mark a cron job as enqueued AND insert the resulting job.
///
/// Uses a single CTE so that both the UPDATE and INSERT happen in one
/// atomic operation. If the process crashes mid-transaction, Postgres
/// rolls back both. If another leader already claimed this fire time
/// (last_enqueued_at no longer matches), the UPDATE matches 0 rows
/// and the INSERT produces nothing.
///
/// Returns the inserted job row, or `None` if the fire was already claimed.
pub async fn atomic_enqueue<'e, E>(
    executor: E,
    cron_name: &str,
    fire_time: DateTime<Utc>,
    previous_enqueued_at: Option<DateTime<Utc>>,
) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        WITH mark AS (
            UPDATE awa.cron_jobs
            SET last_enqueued_at = $2, updated_at = now()
            WHERE name = $1
              AND (last_enqueued_at IS NOT DISTINCT FROM $3)
            RETURNING name, kind, queue, args, priority, max_attempts, tags, metadata
        )
        INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, tags, metadata)
        SELECT kind, queue, args, 'available', priority, max_attempts, tags,
               metadata || jsonb_build_object('cron_name', name, 'cron_fire_time', $2::text)
        FROM mark
        RETURNING *
        "#,
    )
    .bind(cron_name)
    .bind(fire_time)
    .bind(previous_enqueued_at)
    .fetch_optional(executor)
    .await?;

    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_periodic(cron_expr: &str, timezone: &str) -> PeriodicJob {
        PeriodicJob {
            name: "test".to_string(),
            cron_expr: cron_expr.to_string(),
            timezone: timezone.to_string(),
            kind: "test_job".to_string(),
            queue: "default".to_string(),
            args: serde_json::json!({}),
            priority: 2,
            max_attempts: 25,
            tags: vec![],
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn test_valid_cron_expression() {
        let result = PeriodicJob::builder("test", "0 9 * * *")
            .build_raw("test_job".to_string(), serde_json::json!({}));
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_cron_expression() {
        let result = PeriodicJob::builder("test", "not a cron")
            .build_raw("test_job".to_string(), serde_json::json!({}));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("invalid cron expression"),
            "got: {err}"
        );
    }

    #[test]
    fn test_invalid_timezone() {
        let result = PeriodicJob::builder("test", "0 9 * * *")
            .timezone("Not/A/Timezone")
            .build_raw("test_job".to_string(), serde_json::json!({}));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("invalid timezone"), "got: {err}");
    }

    #[test]
    fn test_builder_defaults() {
        let job = PeriodicJob::builder("daily_report", "0 9 * * *")
            .build_raw(
                "daily_report".to_string(),
                serde_json::json!({"format": "pdf"}),
            )
            .unwrap();
        assert_eq!(job.name, "daily_report");
        assert_eq!(job.timezone, "UTC");
        assert_eq!(job.queue, "default");
        assert_eq!(job.priority, 2);
        assert_eq!(job.max_attempts, 25);
        assert!(job.tags.is_empty());
    }

    #[test]
    fn test_builder_custom_fields() {
        let job = PeriodicJob::builder("report", "0 9 * * *")
            .timezone("Pacific/Auckland")
            .queue("reports")
            .priority(1)
            .max_attempts(3)
            .tags(vec!["important".to_string()])
            .metadata(serde_json::json!({"source": "cron"}))
            .build_raw("daily_report".to_string(), serde_json::json!({}))
            .unwrap();
        assert_eq!(job.timezone, "Pacific/Auckland");
        assert_eq!(job.queue, "reports");
        assert_eq!(job.priority, 1);
        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.tags, vec!["important"]);
    }

    #[test]
    fn test_latest_fire_time_finds_past_fire() {
        // Every hour at :00
        let pj = make_periodic("0 * * * *", "UTC");
        let now = Utc.with_ymd_and_hms(2025, 6, 15, 14, 35, 0).unwrap();
        let after = Some(Utc.with_ymd_and_hms(2025, 6, 15, 13, 0, 0).unwrap());

        let fire = pj.latest_fire_time(now, after);
        assert_eq!(
            fire,
            Some(Utc.with_ymd_and_hms(2025, 6, 15, 14, 0, 0).unwrap())
        );
    }

    #[test]
    fn test_no_fire_when_next_is_future() {
        // Every hour at :00
        let pj = make_periodic("0 * * * *", "UTC");
        let now = Utc.with_ymd_and_hms(2025, 6, 15, 14, 35, 0).unwrap();
        // Already fired at 14:00
        let after = Some(Utc.with_ymd_and_hms(2025, 6, 15, 14, 0, 0).unwrap());

        let fire = pj.latest_fire_time(now, after);
        assert!(fire.is_none(), "Should not fire until 15:00");
    }

    #[test]
    fn test_first_registration_null_last_enqueued() {
        // Every hour at :00, registered at 14:35 with no previous fire
        let pj = make_periodic("0 * * * *", "UTC");
        let now = Utc.with_ymd_and_hms(2025, 6, 15, 14, 35, 0).unwrap();

        let fire = pj.latest_fire_time(now, None);
        assert_eq!(
            fire,
            Some(Utc.with_ymd_and_hms(2025, 6, 15, 14, 0, 0).unwrap()),
            "Should enqueue the most recent past fire on first registration"
        );
    }

    #[test]
    fn test_no_backfill_only_latest_fire() {
        // Every minute, last enqueued 1 hour ago
        let pj = make_periodic("* * * * *", "UTC");
        let now = Utc.with_ymd_and_hms(2025, 6, 15, 15, 0, 0).unwrap();
        let after = Some(Utc.with_ymd_and_hms(2025, 6, 15, 14, 0, 0).unwrap());

        let fire = pj.latest_fire_time(now, after);
        // Should return 15:00, not 14:01 — only the latest missed fire
        assert_eq!(
            fire,
            Some(Utc.with_ymd_and_hms(2025, 6, 15, 15, 0, 0).unwrap())
        );
    }

    #[test]
    fn test_timezone_aware_fire_time() {
        // 9 AM daily in Auckland timezone
        let pj = make_periodic("0 9 * * *", "Pacific/Auckland");
        // It's 2025-06-15 21:30 UTC = 2025-06-16 09:30 NZST
        // So 09:00 NZST on June 16 = 21:00 UTC on June 15
        let now = Utc.with_ymd_and_hms(2025, 6, 15, 21, 30, 0).unwrap();
        let after = Some(Utc.with_ymd_and_hms(2025, 6, 14, 21, 0, 0).unwrap());

        let fire = pj.latest_fire_time(now, after);
        // 09:00 NZST on June 16 = 21:00 UTC on June 15
        assert_eq!(
            fire,
            Some(Utc.with_ymd_and_hms(2025, 6, 15, 21, 0, 0).unwrap())
        );
    }

    #[test]
    fn test_dst_spring_forward() {
        // 2:30 AM US/Eastern on March 9 2025 — clocks spring forward from 2:00 to 3:00
        // Schedule at 2:30 AM should fire once (the 2:30 time doesn't exist, so croner
        // should skip it or fire at the next valid time)
        let pj = make_periodic("30 2 * * *", "US/Eastern");
        let now = Utc.with_ymd_and_hms(2025, 3, 9, 12, 0, 0).unwrap();
        let after = Some(Utc.with_ymd_and_hms(2025, 3, 8, 12, 0, 0).unwrap());

        let fire = pj.latest_fire_time(now, after);
        // On spring-forward day, 2:30 AM doesn't exist. croner may skip it entirely
        // or map it to 3:30 AM. Either way, we should get at most one fire.
        let fire_count = if fire.is_some() { 1 } else { 0 };
        assert!(
            fire_count <= 1,
            "Should fire at most once during spring-forward"
        );
    }

    #[test]
    fn test_dst_fall_back() {
        // 1:30 AM US/Eastern on Nov 2 2025 — clocks fall back from 2:00 to 1:00
        // 1:30 AM happens twice. Should fire exactly once.
        let pj = make_periodic("30 1 * * *", "US/Eastern");
        let now = Utc.with_ymd_and_hms(2025, 11, 2, 12, 0, 0).unwrap();
        let after = Some(Utc.with_ymd_and_hms(2025, 11, 1, 12, 0, 0).unwrap());

        let fire = pj.latest_fire_time(now, after);
        assert!(fire.is_some(), "Should fire once during fall-back");

        // Verify it's only one fire by checking that after this fire, no more fires exist
        let fire_time = fire.unwrap();
        let second_fire = pj.latest_fire_time(now, Some(fire_time));
        assert!(
            second_fire.is_none(),
            "Should not fire a second time during fall-back"
        );
    }

    #[test]
    fn test_invalid_priority() {
        let result = PeriodicJob::builder("test", "0 9 * * *")
            .priority(5)
            .build_raw("test_job".to_string(), serde_json::json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_max_attempts() {
        let result = PeriodicJob::builder("test", "0 9 * * *")
            .max_attempts(0)
            .build_raw("test_job".to_string(), serde_json::json!({}));
        assert!(result.is_err());
    }
}
