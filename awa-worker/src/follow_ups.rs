//! Handler-staged follow-up jobs (ADR-046).
//!
//! A handler stages follow-up requests while it runs. The executor seals the
//! buffer when the handler returns and, if the attempt returned
//! `JobResult::Completed`, inserts the staged rows in the attempt's
//! lease-guarded completion transaction. Every other outcome discards them.
//!
//! Requests are serialized and their unique keys computed when staged, so
//! nothing type-dependent runs inside the completion transaction.

use crate::enqueue_specs::EnqueueRequest;
use crate::executor::JobError;
use awa_model::{prepare_raw_job_insert, AwaError, JobArgs, PreparedJobInsert};
use chrono::Datelike;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Most follow-ups one attempt may stage. Each unique insert keeps a released
/// savepoint until the completion commits, plus, on canonical, the
/// unique-claim trigger's exception block. A backend caches only 64
/// subtransactions before spilling to `pg_subtrans`, so this leaves headroom
/// for registered specs in the same transaction.
pub const MAX_STAGED_FOLLOW_UPS: usize = 16;

/// Most serialized request bytes (args, metadata and tags) one attempt may
/// stage in total. Staged follow-ups stay in memory until finalization.
pub const MAX_STAGED_BYTES: usize = 64 * 1024;

// The insert rejects anything outside these (the `jobs_hot` and
// `scheduled_jobs` check constraints in v001), so staging rejects it first
// instead of letting it roll the completion back.
const PRIORITY_RANGE: std::ops::RangeInclusive<i16> = 1..=4;
const MAX_ATTEMPTS_RANGE: std::ops::RangeInclusive<i16> = 1..=1000;
const MAX_NAME_CHARS: usize = 200;
const MAX_TAGS: usize = 20;
/// Postgres rejects timestamps outside 4713 BC to 294276 AD; staging accepts
/// the years a schedule can plausibly mean, well inside that.
const RUN_AT_YEARS: std::ops::RangeInclusive<i32> = 1..=9999;

/// Why a follow-up could not be staged.
///
/// Every variant is deterministic for the request that caused it, so it
/// converts to [`JobError::Terminal`]. A handler whose fan-out size varies
/// between attempts can map it to a retryable error instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StageError {
    /// The follow-up could not be serialized, or breaks a rule its insert
    /// would enforce (priority, `max_attempts`, name length, tag count, NUL
    /// bytes).
    #[error("follow-up could not be prepared: {0}")]
    Invalid(String),
    /// The attempt already staged [`MAX_STAGED_FOLLOW_UPS`] follow-ups.
    #[error("too many staged follow-ups (limit {limit})")]
    TooMany { limit: usize },
    /// The attempt's staged requests would exceed [`MAX_STAGED_BYTES`].
    #[error("staged follow-ups exceed {limit_bytes} bytes")]
    TooLarge { limit_bytes: usize },
    /// The handler already returned, so nothing staged now could commit.
    #[error("the handler already returned; follow-ups can no longer be staged")]
    Sealed,
}

impl From<StageError> for JobError {
    fn from(err: StageError) -> Self {
        JobError::Terminal(err.to_string())
    }
}

/// A cloneable handle for staging follow-ups on the current attempt.
///
/// Obtained from [`JobContext::follow_ups`](crate::JobContext::follow_ups). It
/// is `'static`, so a `ClientBuilder::register` closure can move it into the
/// future it returns.
#[derive(Clone)]
pub struct FollowUps {
    inner: Arc<Mutex<Staging>>,
}

#[derive(Default)]
struct Staging {
    sealed: bool,
    rows: Vec<PreparedJobInsert>,
    bytes: usize,
    poisoned: Option<StageError>,
}

/// The staged follow-ups of a finished handler.
pub(crate) struct SealedFollowUps {
    pub(crate) rows: Vec<PreparedJobInsert>,
    /// The first staging failure, if any. A `Completed` attempt that staged
    /// unsuccessfully must not commit without the follow-up it asked for.
    pub(crate) poisoned: Option<StageError>,
}

impl std::fmt::Debug for FollowUps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let staging = self.lock();
        f.debug_struct("FollowUps")
            .field("staged", &staging.rows.len())
            .field("sealed", &staging.sealed)
            .finish()
    }
}

impl FollowUps {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Staging::default())),
        }
    }

    /// A staging buffer with no attempt behind it, for tests that exercise
    /// staging and insertion without running a handler.
    #[doc(hidden)]
    pub fn for_testing() -> Self {
        Self::new()
    }

    // Nothing panics while holding the lock, and the executor must still
    // seal after a handler panic, so a poisoned lock is not an error here.
    fn lock(&self) -> MutexGuard<'_, Staging> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stage a follow-up job that is enqueued in the same transaction as
    /// this attempt's completion, and discarded if the attempt ends any other
    /// way.
    ///
    /// A follow-up with unique options whose key already exists is skipped
    /// when the completion commits, as for registered `on_*_enqueue` specs.
    /// A failed call poisons the attempt: if the handler still returns
    /// `JobResult::Completed`, the attempt fails terminally with this error.
    pub fn enqueue_on_completion<F: JobArgs>(
        &self,
        request: impl Into<EnqueueRequest<F>>,
    ) -> Result<(), StageError> {
        let request = request.into();
        let prepared = request
            .args
            .to_args()
            .map_err(|err| StageError::Invalid(err.to_string()))
            .map(|args| (request.args.kind_str().to_string(), args, request.opts));
        self.stage(prepared)
    }

    /// Record a staging failure detected by a language bridge before the
    /// request reached [`Self::enqueue_raw_on_completion`], so it poisons the
    /// attempt like any other. Returns the error for the caller to raise.
    #[doc(hidden)]
    pub fn poison(&self, err: StageError) -> StageError {
        self.stage(Err(err.clone())).err().unwrap_or(err)
    }

    /// Stage a follow-up from a raw kind and JSON args, for language bridges.
    #[doc(hidden)]
    pub fn enqueue_raw_on_completion(
        &self,
        kind: String,
        args: serde_json::Value,
        opts: awa_model::InsertOpts,
    ) -> Result<(), StageError> {
        self.stage(Ok((kind, args, opts)))
    }

    fn stage(
        &self,
        request: Result<(String, serde_json::Value, awa_model::InsertOpts), StageError>,
    ) -> Result<(), StageError> {
        let mut staging = self.lock();
        if staging.sealed {
            return Err(StageError::Sealed);
        }
        let result = request.and_then(|(kind, args, opts)| {
            if staging.rows.len() >= MAX_STAGED_FOLLOW_UPS {
                return Err(StageError::TooMany {
                    limit: MAX_STAGED_FOLLOW_UPS,
                });
            }
            let row = prepare_raw_job_insert(kind, args, opts)
                .map_err(|err: AwaError| StageError::Invalid(err.to_string()))?;
            validate_row(&row)?;
            let bytes = serialized_len(&row)?;
            if staging.bytes + bytes > MAX_STAGED_BYTES {
                return Err(StageError::TooLarge {
                    limit_bytes: MAX_STAGED_BYTES,
                });
            }
            Ok((row, bytes))
        });
        match result {
            Ok((row, bytes)) => {
                staging.bytes += bytes;
                staging.rows.push(row);
                Ok(())
            }
            Err(err) => {
                staging.poisoned.get_or_insert_with(|| err.clone());
                Err(err)
            }
        }
    }

    /// Close staging and take what was staged. Later calls to
    /// `enqueue_on_completion` return [`StageError::Sealed`].
    pub(crate) fn seal(&self) -> SealedFollowUps {
        let mut staging = self.lock();
        staging.sealed = true;
        staging.bytes = 0;
        SealedFollowUps {
            rows: std::mem::take(&mut staging.rows),
            poisoned: staging.poisoned.take(),
        }
    }

    /// Seal staging for a test harness that finalizes the job itself.
    /// Returns the staged rows, or the first staging error.
    #[doc(hidden)]
    pub fn seal_for_testing(&self) -> Result<Vec<PreparedJobInsert>, StageError> {
        let sealed = self.seal();
        match sealed.poisoned {
            Some(err) => Err(err),
            None => Ok(sealed.rows),
        }
    }
}

/// Insert sealed follow-ups inside a completion transaction, with the
/// runtime's duplicate handling. For test harnesses that finalize jobs
/// themselves.
#[doc(hidden)]
pub async fn insert_staged_follow_ups_in_tx(
    conn: &mut sqlx::PgConnection,
    job: &awa_model::JobRow,
    rows: &[PreparedJobInsert],
) -> Result<(), AwaError> {
    crate::enqueue_specs::insert_staged_in_tx(conn, job, rows).await
}

/// Reject what the insert would reject, so it fails the stage rather than
/// the completion transaction.
fn validate_row(row: &PreparedJobInsert) -> Result<(), StageError> {
    let invalid = |message: String| Err(StageError::Invalid(message));
    if !PRIORITY_RANGE.contains(&row.priority()) {
        return invalid(format!(
            "priority {} is outside {}..={}",
            row.priority(),
            PRIORITY_RANGE.start(),
            PRIORITY_RANGE.end()
        ));
    }
    if !MAX_ATTEMPTS_RANGE.contains(&row.max_attempts()) {
        return invalid(format!(
            "max_attempts {} is outside {}..={}",
            row.max_attempts(),
            MAX_ATTEMPTS_RANGE.start(),
            MAX_ATTEMPTS_RANGE.end()
        ));
    }
    for (field, value) in [("kind", row.kind()), ("queue", row.queue())] {
        if value.chars().count() > MAX_NAME_CHARS {
            return invalid(format!(
                "{field} is longer than {MAX_NAME_CHARS} characters"
            ));
        }
    }
    if let Some(run_at) = row.run_at() {
        if !RUN_AT_YEARS.contains(&run_at.year()) {
            return invalid(format!(
                "run_at {run_at} is outside the years {}..={}",
                RUN_AT_YEARS.start(),
                RUN_AT_YEARS.end()
            ));
        }
    }
    if row.tags().len() > MAX_TAGS {
        return invalid(format!("more than {MAX_TAGS} tags"));
    }
    let has_nul = |value: &str| value.contains('\0');
    if has_nul(row.kind()) || has_nul(row.queue()) || row.tags().iter().any(|tag| has_nul(tag)) {
        return invalid("kind, queue and tags must not contain NUL bytes".into());
    }
    Ok(())
}

/// Bytes a staged request holds: its kind, queue, args, metadata, tags and
/// ordering key.
fn serialized_len(row: &PreparedJobInsert) -> Result<usize, StageError> {
    let json_len = |value: &serde_json::Value| {
        serde_json::to_vec(value)
            .map(|bytes| bytes.len())
            .map_err(|err| StageError::Invalid(err.to_string()))
    };
    Ok(row.kind().len()
        + row.queue().len()
        + json_len(row.args())?
        + json_len(row.metadata())?
        + row.tags().iter().map(String::len).sum::<usize>()
        + row.ordering_key().map_or(0, <[u8]>::len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, awa_model::JobArgs)]
    struct Next {
        item_id: i64,
    }

    #[test]
    fn staged_follow_ups_are_sealed_once_taken() {
        let follow_ups = FollowUps::new();
        follow_ups
            .enqueue_on_completion(Next { item_id: 1 })
            .unwrap();

        let sealed = follow_ups.seal();
        assert_eq!(sealed.rows.len(), 1);
        assert!(sealed.poisoned.is_none());
        assert_eq!(
            follow_ups.enqueue_on_completion(Next { item_id: 2 }),
            Err(StageError::Sealed)
        );
        assert!(follow_ups.seal().rows.is_empty());
    }

    #[test]
    fn the_count_limit_rejects_and_poisons() {
        let follow_ups = FollowUps::new();
        for item_id in 0..MAX_STAGED_FOLLOW_UPS as i64 {
            follow_ups.enqueue_on_completion(Next { item_id }).unwrap();
        }
        let err = follow_ups
            .enqueue_on_completion(Next { item_id: -1 })
            .unwrap_err();
        assert_eq!(
            err,
            StageError::TooMany {
                limit: MAX_STAGED_FOLLOW_UPS
            }
        );
        let sealed = follow_ups.seal();
        assert_eq!(sealed.rows.len(), MAX_STAGED_FOLLOW_UPS);
        assert_eq!(sealed.poisoned, Some(err));
    }

    #[test]
    fn the_size_limit_counts_requests_across_calls() {
        let follow_ups = FollowUps::new();
        let half = "x".repeat(MAX_STAGED_BYTES / 2);
        follow_ups
            .enqueue_raw_on_completion(
                "next".into(),
                serde_json::json!({ "pad": half }),
                Default::default(),
            )
            .unwrap();
        let err = follow_ups
            .enqueue_raw_on_completion(
                "next".into(),
                serde_json::json!({ "pad": half }),
                Default::default(),
            )
            .unwrap_err();
        assert_eq!(
            err,
            StageError::TooLarge {
                limit_bytes: MAX_STAGED_BYTES
            }
        );
    }

    #[test]
    fn invalid_args_poison_the_attempt() {
        let follow_ups = FollowUps::new();
        let err = follow_ups
            .enqueue_raw_on_completion(
                "next".into(),
                serde_json::json!({ "text": "nul\u{0000}byte" }),
                Default::default(),
            )
            .unwrap_err();
        assert!(matches!(err, StageError::Invalid(_)));
        assert_eq!(follow_ups.seal().poisoned, Some(err));
    }

    fn at_year(year: i32) -> EnqueueRequest<Next> {
        use chrono::TimeZone;
        EnqueueRequest::new(Next { item_id: 1 }).with_opts(awa_model::InsertOpts {
            run_at: chrono::Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single(),
            ..Default::default()
        })
    }

    #[test]
    fn run_at_is_bounded_to_plausible_years() {
        for year in [*RUN_AT_YEARS.start(), *RUN_AT_YEARS.end()] {
            FollowUps::new()
                .enqueue_on_completion(at_year(year))
                .unwrap_or_else(|err| panic!("year {year}: {err}"));
        }
    }

    #[test]
    fn rules_the_insert_enforces_are_rejected_when_staged() {
        let cases = [
            EnqueueRequest::new(Next { item_id: 1 }).priority(9),
            EnqueueRequest::new(Next { item_id: 1 }).max_attempts(0),
            EnqueueRequest::new(Next { item_id: 1 }).queue("q".repeat(MAX_NAME_CHARS + 1)),
            EnqueueRequest::new(Next { item_id: 1 }).with_opts(awa_model::InsertOpts {
                tags: (0..=MAX_TAGS).map(|tag| tag.to_string()).collect(),
                ..Default::default()
            }),
            EnqueueRequest::new(Next { item_id: 1 }).queue("nul\u{0000}queue"),
            at_year(0),
            at_year(10_000),
        ];
        for request in cases {
            let follow_ups = FollowUps::new();
            let err = follow_ups.enqueue_on_completion(request).unwrap_err();
            assert!(matches!(err, StageError::Invalid(_)), "{err}");
            assert_eq!(follow_ups.seal().poisoned, Some(err));
        }
    }

    #[test]
    fn the_ordering_key_counts_toward_the_size_limit() {
        let follow_ups = FollowUps::new();
        let err = follow_ups
            .enqueue_on_completion(EnqueueRequest::new(Next { item_id: 1 }).with_opts(
                awa_model::InsertOpts {
                    ordering_key: Some(vec![7; MAX_STAGED_BYTES]),
                    ..Default::default()
                },
            ))
            .unwrap_err();
        assert!(matches!(err, StageError::TooLarge { .. }));
    }

    #[test]
    fn a_bridge_failure_poisons_the_attempt() {
        let follow_ups = FollowUps::new();
        let err = follow_ups.poison(StageError::Invalid("bad unique_opts".into()));
        assert_eq!(follow_ups.seal().poisoned, Some(err));
    }

    #[test]
    fn a_stage_error_is_terminal() {
        assert!(matches!(
            JobError::from(StageError::Sealed),
            JobError::Terminal(_)
        ));
    }
}
