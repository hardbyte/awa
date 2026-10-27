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
use std::sync::{Arc, Mutex};

/// Most follow-ups one attempt may stage. Each unique insert keeps a released
/// savepoint until the completion commits, plus, on canonical, the
/// unique-claim trigger's exception block. A backend caches only 64
/// subtransactions before spilling to `pg_subtrans`, so this leaves headroom
/// for registered specs in the same transaction.
pub const MAX_STAGED_FOLLOW_UPS: usize = 16;

/// Most serialized request bytes (args, metadata and tags) one attempt may
/// stage in total. Staged follow-ups stay in memory until finalization.
pub const MAX_STAGED_BYTES: usize = 64 * 1024;

/// Why a follow-up could not be staged.
///
/// Every variant is deterministic for the request that caused it, so it
/// converts to [`JobError::Terminal`]. A handler whose fan-out size varies
/// between attempts can map it to a retryable error instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StageError {
    /// The follow-up's args or metadata could not be serialized or failed
    /// validation.
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
#[derive(Clone, Default)]
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
        let staging = self.inner.lock().expect("follow-up staging lock poisoned");
        f.debug_struct("FollowUps")
            .field("staged", &staging.rows.len())
            .field("sealed", &staging.sealed)
            .finish()
    }
}

impl FollowUps {
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
        let mut staging = self.inner.lock().expect("follow-up staging lock poisoned");
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

    /// Number of follow-ups staged so far on this attempt.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("follow-up staging lock poisoned")
            .rows
            .len()
    }

    /// Whether nothing has been staged on this attempt.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Close staging and take what was staged. Later calls to
    /// `enqueue_on_completion` return [`StageError::Sealed`].
    pub(crate) fn seal(&self) -> SealedFollowUps {
        let mut staging = self.inner.lock().expect("follow-up staging lock poisoned");
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

/// Bytes a staged request holds: its args, metadata and tags as JSON.
fn serialized_len(row: &PreparedJobInsert) -> Result<usize, StageError> {
    let json_len = |value: &serde_json::Value| {
        serde_json::to_vec(value)
            .map(|bytes| bytes.len())
            .map_err(|err| StageError::Invalid(err.to_string()))
    };
    Ok(json_len(row.args())?
        + json_len(row.metadata())?
        + row.tags().iter().map(String::len).sum::<usize>())
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
        let follow_ups = FollowUps::default();
        follow_ups
            .enqueue_on_completion(Next { item_id: 1 })
            .unwrap();
        assert_eq!(follow_ups.len(), 1);

        let sealed = follow_ups.seal();
        assert_eq!(sealed.rows.len(), 1);
        assert!(sealed.poisoned.is_none());
        assert_eq!(
            follow_ups.enqueue_on_completion(Next { item_id: 2 }),
            Err(StageError::Sealed)
        );
        assert!(follow_ups.is_empty());
    }

    #[test]
    fn the_count_limit_rejects_and_poisons() {
        let follow_ups = FollowUps::default();
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
        let follow_ups = FollowUps::default();
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
        let follow_ups = FollowUps::default();
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

    #[test]
    fn a_stage_error_is_terminal() {
        assert!(matches!(
            JobError::from(StageError::Sealed),
            JobError::Terminal(_)
        ));
    }
}
