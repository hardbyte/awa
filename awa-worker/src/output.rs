//! Retained job output (ADR-048).
//!
//! A handler records a size-bounded JSON output on its attempt. The executor
//! seals the slot when the handler returns, at the same point as the staging
//! buffer (ADR-046), and publishes the value with the attempt's own
//! transition: as the job's output on completion, or in the error entry of a
//! failure. Nothing in Awa reads it to decide what runs.

use awa_model::output::validate_output;
pub use awa_model::output::{OutputError, MAX_OUTPUT_BYTES};
use serde::Serialize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// A cloneable handle to the current attempt's output.
///
/// Obtained from [`JobContext::output_slot`](crate::JobContext::output_slot).
/// It is `'static`, so a `ClientBuilder::register` closure can move it into
/// the future it returns.
#[derive(Clone, Default)]
pub struct OutputSlot {
    inner: Arc<Mutex<Slot>>,
}

#[derive(Default)]
struct Slot {
    sealed: bool,
    value: Option<serde_json::Value>,
}

impl std::fmt::Debug for OutputSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slot = self.lock();
        f.debug_struct("OutputSlot")
            .field("recorded", &slot.value.is_some())
            .field("sealed", &slot.sealed)
            .finish()
    }
}

impl OutputSlot {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A slot with no attempt behind it, for tests that exercise recording
    /// without running a handler.
    #[doc(hidden)]
    pub fn for_testing() -> Self {
        Self::new()
    }

    // The executor must still seal after a handler panic, and nothing panics
    // while holding the lock, so a poisoned lock is not an error here.
    fn lock(&self) -> MutexGuard<'_, Slot> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record this attempt's output, replacing any earlier one.
    ///
    /// The value is serialized and checked now. Recording JSON null clears
    /// the slot. A failed call also clears it, so an earlier value never
    /// stands in for the one that failed, and it does not fail the attempt.
    pub fn set<T: Serialize + ?Sized>(&self, output: &T) -> Result<(), OutputError> {
        // User `Serialize` impls run outside the lock.
        let value = serde_json::to_value(output)
            .map_err(|err| OutputError::Invalid(err.to_string()))
            .and_then(validate_output);
        self.store(value)
    }

    /// Record an already-converted JSON value, for language bridges.
    #[doc(hidden)]
    pub fn set_json(&self, output: serde_json::Value) -> Result<(), OutputError> {
        self.store(validate_output(output))
    }

    /// Clear the slot after a language bridge failed to convert a value, so
    /// the failure behaves like a failed [`Self::set`]. Returns the error.
    #[doc(hidden)]
    pub fn fail(&self, err: OutputError) -> OutputError {
        match self.store(Err(err.clone())) {
            Err(stored) => stored,
            Ok(()) => err,
        }
    }

    fn store(
        &self,
        value: Result<Option<serde_json::Value>, OutputError>,
    ) -> Result<(), OutputError> {
        let mut slot = self.lock();
        if slot.sealed {
            return Err(OutputError::Sealed);
        }
        match value {
            Ok(value) => {
                slot.value = value;
                Ok(())
            }
            Err(err) => {
                slot.value = None;
                Err(err)
            }
        }
    }

    /// Remove any recorded output, so the attempt publishes none. After the
    /// handler returns this does nothing; `set(&())` reports
    /// [`OutputError::Sealed`] for a caller that needs to know.
    pub fn clear(&self) {
        let mut slot = self.lock();
        if !slot.sealed {
            slot.value = None;
        }
    }

    /// The output recorded so far.
    pub fn get(&self) -> Option<serde_json::Value> {
        self.lock().value.clone()
    }

    /// Close the slot and return its value, which [`Self::get`] keeps
    /// reporting. Later calls to [`Self::set`] return
    /// [`OutputError::Sealed`].
    pub(crate) fn seal(&self) -> Option<serde_json::Value> {
        let mut slot = self.lock();
        slot.sealed = true;
        slot.value.clone()
    }

    /// Seal the slot for a test harness that finalizes the job itself.
    #[doc(hidden)]
    pub fn seal_for_testing(&self) -> Option<serde_json::Value> {
        self.seal()
    }
}

/// `job` without outputs: no `output`, and no `output` in its error entries.
/// The row a handler receives at dispatch and the row enqueue specs receive
/// are both built this way, so neither the next attempt nor a follow-up can
/// be derived from an output (ADR-048).
pub(crate) fn without_outputs(job: &awa_model::JobRow) -> std::borrow::Cow<'_, awa_model::JobRow> {
    let has_error_output = job.errors.iter().flatten().any(|entry| {
        entry
            .get(awa_model::output::ERROR_ENTRY_OUTPUT_KEY)
            .is_some()
    });
    if job.output.is_none() && !has_error_output {
        return std::borrow::Cow::Borrowed(job);
    }
    let mut stripped = job.clone();
    stripped.output = None;
    if let Some(errors) = stripped.errors.as_mut() {
        awa_model::output::strip_error_outputs(errors);
    }
    std::borrow::Cow::Owned(stripped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn last_value_wins_and_null_clears() {
        let slot = OutputSlot::new();
        slot.set(&json!({"n": 1})).unwrap();
        slot.set(&json!({"n": 2})).unwrap();
        assert_eq!(slot.get(), Some(json!({"n": 2})));
        slot.set(&serde_json::Value::Null).unwrap();
        assert_eq!(slot.get(), None);
        slot.set(&Option::<i32>::None).unwrap();
        assert_eq!(slot.seal(), None);
    }

    #[test]
    fn a_failed_set_clears_the_slot() {
        let slot = OutputSlot::new();
        slot.set(&json!("started")).unwrap();
        let err = slot.set(&"x".repeat(MAX_OUTPUT_BYTES)).unwrap_err();
        assert!(matches!(err, OutputError::TooLarge { .. }), "{err:?}");
        assert_eq!(slot.get(), None);
        slot.set(&json!("started")).unwrap();
        assert!(matches!(
            slot.set(&json!("a\u{0}b")),
            Err(OutputError::Invalid(_))
        ));
        assert_eq!(slot.seal(), None);
    }

    #[test]
    fn serialization_errors_are_invalid() {
        struct Fails;
        impl Serialize for Fails {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("no"))
            }
        }
        let slot = OutputSlot::new();
        assert!(matches!(slot.set(&Fails), Err(OutputError::Invalid(_))));
    }

    #[test]
    fn sealing_returns_the_value_and_closes_the_slot() {
        let slot = OutputSlot::new();
        let handle = slot.clone();
        handle.set(&json!([1, 2])).unwrap();
        assert_eq!(slot.seal(), Some(json!([1, 2])));
        assert_eq!(handle.set(&json!(3)), Err(OutputError::Sealed));
        assert_eq!(handle.set(&()), Err(OutputError::Sealed));
        handle.clear();
        assert_eq!(handle.get(), Some(json!([1, 2])));
    }
}
