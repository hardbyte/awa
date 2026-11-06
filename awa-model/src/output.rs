//! Retained job output (ADR-048).
//!
//! A handler may record a size-bounded JSON value describing what its
//! attempt produced. It is published by the attempt's own lease-guarded
//! transition: as the job's output on completion, or in the error entry of a
//! failure. Nothing in Awa reads it to decide what runs.

use serde_json::Value;

/// A job kind's declared output type (ADR-048).
///
/// Implemented by `#[derive(JobArgs)]` with `#[awa(output = MyOutput)]`. The
/// stored output is still JSON, written by any language or version, so every
/// typed read decodes and can fail; see [`crate::JobRow::output_as`].
pub trait JobOutput: crate::JobArgs {
    /// What this kind's handler records as its output.
    type Output: serde::Serialize + serde::de::DeserializeOwned;
}

/// How a job kind's output is described to operators, through its kind
/// descriptor (ADR-048).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct OutputDescriptor {
    /// The output type's name, as declared in code.
    #[serde(rename = "type")]
    pub type_name: String,
    /// A JSON Schema for the output, when one is available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
}

impl OutputDescriptor {
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            schema: None,
        }
    }

    pub fn schema(mut self, schema: Value) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Largest output an attempt may record, in bytes of compact JSON text.
pub const MAX_OUTPUT_BYTES: usize = 4 * 1024;

/// Key of a failed attempt's output inside its error entry.
pub const ERROR_ENTRY_OUTPUT_KEY: &str = "output";

/// Why an output could not be recorded.
///
/// There is deliberately no conversion into a job error: nothing depends on
/// the output, so a handler decides whether failing to record one should
/// fail the attempt.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OutputError {
    /// The value would not serialize to JSON, or contains a NUL character,
    /// which Postgres `jsonb` cannot store.
    #[error("output is not storable JSON: {0}")]
    Invalid(String),
    /// The serialized value is larger than [`MAX_OUTPUT_BYTES`].
    #[error("output is {size_bytes} bytes; the limit is {limit_bytes}")]
    TooLarge {
        size_bytes: usize,
        limit_bytes: usize,
    },
    /// The handler already returned, so its output is sealed.
    #[error("the handler already returned; its output can no longer be recorded")]
    Sealed,
}

/// Check that `value` can be stored as an output. JSON null means "no
/// output" and returns `Ok(None)`.
pub fn validate_output(value: Value) -> Result<Option<Value>, OutputError> {
    if value.is_null() {
        return Ok(None);
    }
    if contains_nul(&value) {
        return Err(OutputError::Invalid(
            "strings and keys must not contain NUL (\\u0000)".into(),
        ));
    }
    let size_bytes = serialized_len(&value)?;
    if size_bytes > MAX_OUTPUT_BYTES {
        return Err(OutputError::TooLarge {
            size_bytes,
            limit_bytes: MAX_OUTPUT_BYTES,
        });
    }
    Ok(Some(value))
}

/// Bytes of compact JSON text `value` serializes to.
pub fn serialized_len(value: &Value) -> Result<usize, OutputError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|err| OutputError::Invalid(err.to_string()))
}

fn contains_nul(value: &Value) -> bool {
    match value {
        Value::String(s) => s.contains('\0'),
        Value::Array(items) => items.iter().any(contains_nul),
        Value::Object(map) => map.iter().any(|(k, v)| k.contains('\0') || contains_nul(v)),
        _ => false,
    }
}

/// The message of a failed attempt's error entry, and the output the
/// attempt recorded, if any.
///
/// Queue-storage failure transitions take `impl Into<AttemptError>`, so a
/// plain message still works where there is no output.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct AttemptError {
    pub message: String,
    pub output: Option<Value>,
}

impl AttemptError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            output: None,
        }
    }

    /// Attach the attempt's output to its error entry.
    pub fn with_output(mut self, output: Option<Value>) -> Self {
        self.output = output;
        self
    }
}

impl From<&str> for AttemptError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

impl From<String> for AttemptError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&String> for AttemptError {
    fn from(message: &String) -> Self {
        Self::new(message.as_str())
    }
}

/// Remove failed outputs from error entries. Applied to the row a handler
/// receives at dispatch: a failed output is a report for people, not a
/// checkpoint for the next attempt (ADR-048).
pub fn strip_error_outputs(errors: &mut [Value]) {
    for entry in errors {
        if let Value::Object(map) = entry {
            map.remove(ERROR_ENTRY_OUTPUT_KEY);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn null_clears() {
        assert_eq!(validate_output(Value::Null), Ok(None));
    }

    #[test]
    fn accepts_values_up_to_the_limit() {
        let fits = json!("x".repeat(MAX_OUTPUT_BYTES - 2));
        assert_eq!(serialized_len(&fits).unwrap(), MAX_OUTPUT_BYTES);
        assert_eq!(validate_output(fits.clone()), Ok(Some(fits)));
        let over = json!("x".repeat(MAX_OUTPUT_BYTES - 1));
        assert_eq!(
            validate_output(over),
            Err(OutputError::TooLarge {
                size_bytes: MAX_OUTPUT_BYTES + 1,
                limit_bytes: MAX_OUTPUT_BYTES
            })
        );
    }

    #[test]
    fn rejects_nul_in_strings_and_keys() {
        assert!(matches!(
            validate_output(json!({"a": ["ok", "b\u{0}"]})),
            Err(OutputError::Invalid(_))
        ));
        assert!(matches!(
            validate_output(json!({"k\u{0}": 1})),
            Err(OutputError::Invalid(_))
        ));
    }

    #[test]
    fn strips_only_the_output_key() {
        let mut errors = vec![
            json!({"error": "a", "attempt": 1, "output": {"n": 1}}),
            json!({"error": "b", "attempt": 2}),
            json!("not an object"),
        ];
        strip_error_outputs(&mut errors);
        assert_eq!(
            errors,
            vec![
                json!({"error": "a", "attempt": 1}),
                json!({"error": "b", "attempt": 2}),
                json!("not an object"),
            ]
        );
    }
}
