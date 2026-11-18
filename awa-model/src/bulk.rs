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
//! A run covers the jobs that existed when it started. Its first batch
//! records the next value of the job ID sequence in
//! [`BulkCursor::sequence_bound`], the database time in
//! [`BulkCursor::started_at`], and one past the highest existing job ID in
//! [`BulkCursor::before_job_id`]. A job with an ID below the sequence bound
//! belongs to the run; a job at or above it, which only an explicit ID can
//! produce before the run starts, belongs to the run when it was created no
//! later than the run started. No batch passes `before_job_id`. A job
//! enqueued during the run is left alone, so a run against a busy producer
//! still finishes.

use crate::error::AwaError;
use crate::job::JobRow;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;

/// Rows a batched admin operation changes per transaction by default.
pub const DEFAULT_BATCH_SIZE: i64 = 1_000;

/// Largest `batch_size` a batched admin operation accepts. A larger batch
/// is one transaction holding row locks, and on queue storage the ring
/// partitions it reads, for longer than an operator action should; a run
/// over more rows takes more batches.
pub const MAX_BATCH_SIZE: i64 = 10_000;

/// How long [`run_batches`] waits by default between batches on queue
/// storage, so the maintenance leader's ring prune gets its lock.
///
/// A queue-storage batch reads ring partitions, holding `ACCESS SHARE` on
/// them until it commits, and prune truncates a sealed partition under
/// `ACCESS EXCLUSIVE`, waiting 10 ms for it at first and retrying with
/// longer waits up to 320 ms. A prune that is waiting when a batch commits
/// gets the lock, because its request is queued ahead of the next batch's;
/// one that starts while a batch runs and gives up first reports `Blocked`.
/// Back-to-back batches leave no gap, so a long run can stop ring prune for
/// its whole length. In a 100 ms gap a prune that starts gets the lock on its
/// first try, and the gap costs about a tenth of a one-second batch.
pub const DEFAULT_QUEUE_STORAGE_PAUSE: Duration = Duration::from_millis(100);

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
    /// The run pages through job IDs below this: one past the highest job
    /// ID that existed when it started. Zero until the run's first batch
    /// sets it.
    #[serde(default)]
    pub before_job_id: i64,
    /// The next value of the job ID sequence when the run started. A job
    /// with a smaller ID belongs to the run; a job with this ID or a higher
    /// one belongs to it only if it was created no later than
    /// [`Self::started_at`]. Zero in a cursor from a release without it,
    /// when every job below `before_job_id` belongs to the run.
    #[serde(default)]
    pub sequence_bound: i64,
    /// The database time when the run started.
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
}

impl BulkCursor {
    /// The cursor for the start of pass `stage` of the same run.
    pub(crate) fn next_pass(self, stage: u8) -> Self {
        Self {
            stage,
            after_job_id: 0,
            ..self
        }
    }
}

/// SQL predicate that a row with job ID `id` and creation time `created_at`
/// belongs to the run whose bounds `below`, `sequence_bound` and
/// `started_at` (a `bigint`, a `bigint` and a `timestamptz`) bind. An ID
/// below the sequence bound decides it alone, so in the usual case, with no
/// explicit IDs above the sequence, the predicate is on job IDs only.
/// `created_at` is read only for IDs at or above the sequence bound.
pub(crate) fn run_scope_sql(
    id: &str,
    created_at: &str,
    below: &str,
    sequence_bound: &str,
    started_at: &str,
) -> String {
    format!(
        "{id} < {below} AND ({sequence_bound}::bigint = 0 OR {id} < {sequence_bound}::bigint \
         OR {created_at} <= {started_at}::timestamptz)"
    )
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
    /// How long a run should wait before the next batch so other work gets
    /// the locks this batch held; [`DEFAULT_QUEUE_STORAGE_PAUSE`] on queue
    /// storage, zero on canonical storage. Not sent over HTTP.
    #[serde(skip)]
    pub pause_hint: Duration,
}

impl BulkBatch {
    pub(crate) fn new(cursor: BulkCursor) -> Self {
        Self {
            cursor,
            ..Self::default()
        }
    }

    /// A queue-storage batch, which asks the run to pause before the next.
    pub(crate) fn queue_storage(cursor: BulkCursor) -> Self {
        Self {
            cursor,
            pause_hint: DEFAULT_QUEUE_STORAGE_PAUSE,
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

/// A bulk call that failed partway, with what it had committed before the
/// error. Batches are separate transactions, so the rows in `partial` stay
/// changed: report them, record their metrics and invalidate caches before
/// surfacing `error`.
#[derive(Debug)]
pub struct BulkError<T> {
    /// What committed before the error.
    pub partial: T,
    /// The error that stopped the call.
    pub error: AwaError,
}

impl<T> BulkError<T> {
    pub(crate) fn new(partial: T, error: AwaError) -> Self {
        Self { partial, error }
    }
}

impl<T> std::fmt::Display for BulkError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<T: std::fmt::Debug> std::error::Error for BulkError<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl<T> From<BulkError<T>> for AwaError {
    fn from(err: BulkError<T>) -> Self {
        err.error
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
    /// Wait this long between batches that made progress. `None`, the
    /// default, uses each batch's [`BulkBatch::pause_hint`]: a pause on
    /// queue storage, none on canonical storage.
    pub pause_between_batches: Option<Duration>,
}

impl Default for BulkRunOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            limit: None,
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            pause_between_batches: None,
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

    pub fn pause_between_batches(mut self, pause: Option<Duration>) -> Self {
        self.pause_between_batches = pause;
        self
    }
}

/// Reject a `batch_size` below 1 or above [`MAX_BATCH_SIZE`].
pub fn validate_batch_size(batch_size: i64) -> Result<(), AwaError> {
    if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
        return Err(AwaError::Validation(format!(
            "batch_size must be between 1 and {MAX_BATCH_SIZE}"
        )));
    }
    Ok(())
}

/// One statement that locks up to `limit` matching rows with IDs after
/// `after` that belong to the run (`scope` binds its `below`,
/// `sequence_bound` and `started_at`, see [`run_scope_sql`]) in ID order,
/// skipping rows another transaction holds.
/// It returns the locked IDs, the lowest matching ID below the last locked
/// one that another transaction holds, and whether any matching row in the
/// range was not locked.
pub(crate) fn pick_sql(
    relation: &str,
    id_column: &str,
    filter: &str,
    after: &str,
    scope: [&str; 3],
    limit: &str,
) -> String {
    let [below, sequence_bound, started_at] = scope;
    let filter = format!(
        "({filter}) AND {id_column} > {after} AND {}",
        run_scope_sql(id_column, "created_at", below, sequence_bound, started_at)
    );
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
                Some(stage) => batch.cursor = batch.cursor.next_pass(stage),
                None => batch.done = true,
            }
        }
    }
}

/// Run `batch` until no matching row remains, `options.limit` rows have
/// changed, or the remaining rows stay held past `options.busy_timeout`.
/// `on_batch` sees each batch as it commits, with the running total, so a
/// caller can record metrics and invalidate caches per committed batch. When
/// a batch fails, the error carries the run so far.
pub async fn run_batches<F, Fut, P>(
    options: BulkRunOptions,
    mut batch: F,
    mut on_batch: P,
) -> Result<BulkRun, BulkError<BulkRun>>
where
    F: FnMut(i64, BulkCursor) -> Fut,
    Fut: Future<Output = Result<BulkBatch, AwaError>>,
    P: FnMut(&BulkBatch, &BulkRun),
{
    let mut run = BulkRun::default();
    if let Err(error) = validate_batch_size(options.batch_size) {
        return Err(BulkError {
            partial: run,
            error,
        });
    }
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
        let result = match batch(take, cursor).await {
            Ok(result) => result,
            Err(error) => {
                return Err(BulkError {
                    partial: run,
                    error,
                })
            }
        };
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
            let pause = options.pause_between_batches.unwrap_or(result.pause_hint);
            if !pause.is_zero() {
                tokio::time::sleep(pause).await;
            }
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

    #[tokio::test]
    async fn a_failed_batch_returns_the_run_so_far() {
        let mut calls = 0;
        let err = run_batches(
            BulkRunOptions::default().batch_size(2),
            |_, _| {
                calls += 1;
                let result = if calls == 1 {
                    Ok(batch(2, false, false, 4))
                } else {
                    Err(AwaError::Validation("second batch failed".into()))
                };
                async move { result }
            },
            |_, _| {},
        )
        .await
        .expect_err("second batch fails");
        assert_eq!((err.partial.count, err.partial.batches), (2, 1));
        assert!(matches!(err.error, AwaError::Validation(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn pauses_between_batches_as_each_batch_asks() {
        let mut calls = 0;
        let started = Instant::now();
        let mut at = Vec::new();
        run_batches(
            BulkRunOptions::default(),
            |_, _| {
                calls += 1;
                at.push(started.elapsed());
                let mut next = batch(1, false, calls == 3, 0);
                next.pause_hint = Duration::from_millis(100);
                async move { Ok(next) }
            },
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(
            at,
            vec![
                Duration::ZERO,
                Duration::from_millis(100),
                Duration::from_millis(200)
            ]
        );

        let started = Instant::now();
        let mut at = Vec::new();
        let mut calls = 0;
        run_batches(
            BulkRunOptions::default().pause_between_batches(Some(Duration::ZERO)),
            |_, _| {
                calls += 1;
                at.push(started.elapsed());
                let mut next = batch(1, false, calls == 2, 0);
                next.pause_hint = Duration::from_millis(100);
                async move { Ok(next) }
            },
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(at, vec![Duration::ZERO, Duration::ZERO]);
    }

    #[test]
    fn rejects_batch_sizes_outside_the_bounds() {
        assert!(validate_batch_size(0).is_err());
        assert!(validate_batch_size(1).is_ok());
        assert!(validate_batch_size(MAX_BATCH_SIZE).is_ok());
        assert!(validate_batch_size(MAX_BATCH_SIZE + 1).is_err());
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
