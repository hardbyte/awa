//! Bounded bulk admin operations.
//!
//! A batched operation changes at most `batch_size` matching rows per call,
//! each call in its own transaction, and walks the matching rows in job ID
//! order with a [`BulkCursor`]. A row whose change would violate a unique
//! claim is skipped, counted and reported, and the cursor moves past it, so
//! it is not tried again in the same run. A row another transaction holds is
//! never skipped: the cursor stops before it, and a batch that can take no
//! row because every remaining matching row is held reports
//! [`BulkBatch::busy`]. [`run_batches`] drives a batched operation to
//! completion.
//!
//! A run covers the jobs that existed when it started: its first batch
//! records the next job ID in [`BulkCursor::below_job_id`], and no batch of
//! the run touches a job with that ID or a higher one. A job enqueued during
//! the run is left alone, so a run against a busy producer still finishes.

use crate::error::AwaError;
use crate::job::JobRow;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;

/// Rows a batched admin operation changes per transaction by default.
pub const DEFAULT_BATCH_SIZE: i64 = 1_000;

/// Most conflicting job IDs a batch or a run reports.
pub const MAX_REPORTED_CONFLICTS: usize = 10;

/// How long [`run_batches`] waits by default for rows another transaction
/// holds before it gives up.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(60);

/// Where a batched run resumes. Pass back the cursor the previous batch
/// returned; start from `BulkCursor::default()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkCursor {
    /// Which pass of a multi-pass operation the run is in.
    #[serde(default)]
    pub stage: u8,
    /// The largest job ID this pass has already handled.
    #[serde(default)]
    pub after_job_id: i64,
    /// The run handles only jobs with smaller IDs: the jobs that existed
    /// when it started. Zero until the run's first batch sets it.
    #[serde(default)]
    pub below_job_id: i64,
}

/// What one batch did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BulkBatch {
    /// Rows changed.
    pub count: u64,
    /// Rows skipped because the change would violate a unique claim.
    pub skipped: u64,
    /// IDs of skipped jobs, at most [`MAX_REPORTED_CONFLICTS`].
    pub conflicts: Vec<i64>,
    /// Every remaining matching row is held by another transaction; nothing
    /// was changed. Call again after a short wait.
    pub busy: bool,
    /// No matching row remains past the cursor.
    pub done: bool,
    /// Where the next batch resumes.
    pub cursor: BulkCursor,
}

impl BulkBatch {
    pub(crate) fn new(cursor: BulkCursor) -> Self {
        Self {
            cursor,
            ..Self::default()
        }
    }

    pub(crate) fn skip(&mut self, job_id: i64) {
        self.skipped += 1;
        if self.conflicts.len() < MAX_REPORTED_CONFLICTS {
            self.conflicts.push(job_id);
        }
    }
}

/// What a bulk call on explicit job IDs did.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BulkIds {
    /// The jobs changed.
    pub jobs: Vec<JobRow>,
    /// Jobs skipped because the change would violate a unique claim.
    pub skipped: u64,
    /// IDs of skipped jobs, at most [`MAX_REPORTED_CONFLICTS`].
    pub conflicts: Vec<i64>,
}

impl BulkIds {
    pub(crate) fn skip(&mut self, job_id: i64) {
        self.skipped += 1;
        if self.conflicts.len() < MAX_REPORTED_CONFLICTS {
            self.conflicts.push(job_id);
        }
    }
}

/// What a run of batches did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BulkRun {
    /// Rows changed.
    pub count: u64,
    /// Rows skipped because the change would violate a unique claim.
    pub skipped: u64,
    /// IDs of skipped jobs, at most [`MAX_REPORTED_CONFLICTS`].
    pub conflicts: Vec<i64>,
    /// Batches run.
    pub batches: u64,
    /// No matching row remains. False when the run stopped at its limit or
    /// gave up on rows another transaction held.
    pub complete: bool,
    /// The run gave up because every remaining matching row stayed held by
    /// another transaction for the busy timeout.
    pub busy_timed_out: bool,
}

impl BulkRun {
    fn absorb(&mut self, batch: &BulkBatch) {
        self.count += batch.count;
        self.skipped += batch.skipped;
        for job_id in &batch.conflicts {
            if self.conflicts.len() < MAX_REPORTED_CONFLICTS {
                self.conflicts.push(*job_id);
            }
        }
    }
}

/// How [`run_batches`] runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct BulkRunOptions {
    /// Rows changed per transaction.
    pub batch_size: i64,
    /// Stop after changing this many rows in total.
    pub limit: Option<u64>,
    /// Give up after this long without progress while every remaining
    /// matching row is held by another transaction.
    pub busy_timeout: Duration,
}

impl Default for BulkRunOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            limit: None,
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
        }
    }
}

impl BulkRunOptions {
    pub fn batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn limit(mut self, limit: Option<u64>) -> Self {
        self.limit = limit;
        self
    }

    pub fn busy_timeout(mut self, busy_timeout: Duration) -> Self {
        self.busy_timeout = busy_timeout;
        self
    }
}

pub(crate) fn validate_batch_size(batch_size: i64) -> Result<(), AwaError> {
    if batch_size < 1 {
        return Err(AwaError::Validation("batch_size must be at least 1".into()));
    }
    Ok(())
}

/// One statement that locks up to `limit` matching rows with IDs between
/// `after` and `below` in ID order, skipping rows another transaction holds.
/// It returns the locked IDs, the lowest matching ID below the last locked
/// one that another transaction holds, and whether any matching row in the
/// range was not locked.
pub(crate) fn pick_sql(
    relation: &str,
    id_column: &str,
    filter: &str,
    after: &str,
    below: &str,
    limit: &str,
) -> String {
    let filter = format!("({filter}) AND {id_column} > {after} AND {id_column} < {below}");
    format!(
        r#"
        WITH picked AS MATERIALIZED (
            SELECT {id_column} AS id
            FROM {relation}
            WHERE {filter}
            ORDER BY {id_column}
            LIMIT {limit}
            FOR UPDATE SKIP LOCKED
        )
        SELECT
            ARRAY(SELECT DISTINCT id FROM picked ORDER BY id) AS picked,
            (
                SELECT min({id_column})
                FROM {relation}
                WHERE {filter}
                  AND {id_column} < (SELECT max(id) FROM picked)
                  AND {id_column} <> ALL(ARRAY(SELECT id FROM picked))
            ) AS first_held,
            EXISTS (
                SELECT 1
                FROM {relation}
                WHERE {filter}
                  AND {id_column} <> ALL(ARRAY(SELECT id FROM picked))
            ) AS more
        "#
    )
}

/// The rows a batch may handle: the locked rows below the first row another
/// transaction holds, so the cursor never passes a held row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pick {
    pub(crate) ids: Vec<i64>,
    more: bool,
}

impl Pick {
    pub(crate) fn new(picked: Vec<i64>, first_held: Option<i64>, more: bool) -> Self {
        let ids = picked
            .into_iter()
            .filter(|id| first_held.is_none_or(|held| *id < held))
            .collect();
        Self { ids, more }
    }

    /// Move the batch's cursor past the handled rows and set `busy` or
    /// `done`. With `next_stage`, a finished pass moves the cursor to the
    /// start of that pass instead of finishing the run.
    pub(crate) fn settle(&self, batch: &mut BulkBatch, next_stage: Option<u8>) {
        if let Some(last) = self.ids.last() {
            batch.cursor.after_job_id = *last;
        }
        batch.busy = self.ids.is_empty() && self.more;
        if !self.more {
            match next_stage {
                Some(stage) => {
                    batch.cursor = BulkCursor {
                        stage,
                        after_job_id: 0,
                        below_job_id: batch.cursor.below_job_id,
                    }
                }
                None => batch.done = true,
            }
        }
    }
}

/// Run `batch` until no matching row remains, `options.limit` rows have
/// changed, or the remaining rows stay held past `options.busy_timeout`.
/// `on_batch` sees each batch and the running total.
pub async fn run_batches<F, Fut, P>(
    options: BulkRunOptions,
    mut batch: F,
    mut on_batch: P,
) -> Result<BulkRun, AwaError>
where
    F: FnMut(i64, BulkCursor) -> Fut,
    Fut: Future<Output = Result<BulkBatch, AwaError>>,
    P: FnMut(&BulkBatch, &BulkRun),
{
    validate_batch_size(options.batch_size)?;
    let mut run = BulkRun::default();
    let mut cursor = BulkCursor::default();
    let mut busy_since: Option<Instant> = None;
    let mut busy_wait = Duration::from_millis(100);
    loop {
        let take = match options.limit {
            Some(limit) if run.count >= limit => return Ok(run),
            Some(limit) => options
                .batch_size
                .min(i64::try_from(limit - run.count).unwrap_or(i64::MAX)),
            None => options.batch_size,
        };
        let result = batch(take, cursor).await?;
        run.batches += 1;
        run.absorb(&result);
        cursor = result.cursor;
        on_batch(&result, &run);
        if result.done {
            run.complete = true;
            return Ok(run);
        }
        if result.busy {
            let since = *busy_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= options.busy_timeout {
                run.busy_timed_out = true;
                return Ok(run);
            }
            tokio::time::sleep(busy_wait).await;
            busy_wait = (busy_wait * 2).min(Duration::from_secs(2));
        } else {
            busy_since = None;
            busy_wait = Duration::from_millis(100);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(count: u64, busy: bool, done: bool, after: i64) -> BulkBatch {
        BulkBatch {
            count,
            busy,
            done,
            cursor: BulkCursor {
                after_job_id: after,
                ..BulkCursor::default()
            },
            ..BulkBatch::default()
        }
    }

    #[tokio::test]
    async fn runs_until_done_and_passes_the_cursor_on() {
        let mut seen = Vec::new();
        let mut results = vec![batch(2, false, false, 5), batch(1, false, true, 9)].into_iter();
        let run = run_batches(
            BulkRunOptions::default().batch_size(2),
            |size, cursor| {
                seen.push((size, cursor.after_job_id));
                let next = results.next().unwrap();
                async move { Ok(next) }
            },
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(seen, vec![(2, 0), (2, 5)]);
        assert_eq!((run.count, run.batches, run.complete), (3, 2, true));
    }

    #[tokio::test]
    async fn stops_at_the_limit() {
        let mut sizes = Vec::new();
        let run = run_batches(
            BulkRunOptions::default().batch_size(4).limit(Some(6)),
            |size, _| {
                sizes.push(size);
                async move { Ok(batch(size as u64, false, false, 0)) }
            },
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(sizes, vec![4, 2]);
        assert_eq!((run.count, run.complete), (6, false));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_on_rows_that_stay_held() {
        let run = run_batches(
            BulkRunOptions::default().busy_timeout(Duration::from_secs(1)),
            |_, _| async { Ok(batch(0, true, false, 0)) },
            |_, _| {},
        )
        .await
        .unwrap();
        assert!(run.busy_timed_out);
        assert!(!run.complete);
    }

    #[test]
    fn caps_reported_conflicts() {
        let mut batch = BulkBatch::default();
        for job_id in 0..20 {
            batch.skip(job_id);
        }
        assert_eq!(batch.skipped, 20);
        assert_eq!(batch.conflicts.len(), MAX_REPORTED_CONFLICTS);
    }
}
