//! Distributed trace propagation across the enqueue → execution boundary.
//!
//! At enqueue time the producer's active OpenTelemetry span context is
//! serialized as a W3C `traceparent` string and stored under a reserved key
//! in the job's `metadata` JSONB. The worker reads it back when it claims
//! the job and sets it as the remote parent of the `job.execute` span, so
//! the producer's trace continues into (every attempt of) the execution.
//!
//! Storing the context in `metadata` keeps propagation engine-agnostic: the
//! value rides the canonical row and the queue-storage payload envelope the
//! same way, needs no schema migration, and SQL-native producers (see
//! `docs/bridge-adapters.md`) can participate by setting the key themselves.
//! Keys prefixed `awa:` are reserved for awa (see `docs/stability.md`); a
//! caller-supplied value under this key always wins over ambient capture.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Reserved metadata key carrying the W3C `traceparent` of the enqueue site.
pub const TRACEPARENT_METADATA_KEY: &str = "awa:traceparent";

/// Environment variable disabling ambient capture at enqueue
/// (`off`/`false`/`0`). Explicit caller-supplied metadata keys still pass
/// through untouched.
pub const TRACE_CAPTURE_ENV: &str = "AWA_TRACE_CAPTURE";

fn capture_disabled() -> bool {
    match std::env::var(TRACE_CAPTURE_ENV) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "off" | "false" | "0"
        ),
        Err(_) => false,
    }
}

/// The current span's OpenTelemetry context as a W3C `traceparent` string,
/// or `None` when there is no valid context (no OpenTelemetry layer
/// installed, or not inside a span).
pub fn current_traceparent() -> Option<String> {
    let context = tracing::Span::current().context();
    let span = context.span();
    let span_context = span.span_context();
    if !span_context.is_valid() {
        return None;
    }
    Some(format!(
        "00-{}-{}-{:02x}",
        span_context.trace_id(),
        span_context.span_id(),
        span_context.trace_flags().to_u8()
    ))
}

/// Parse a W3C `traceparent` string into a remote [`SpanContext`].
///
/// Accepts any version except the invalid `ff`; version `00` must have
/// exactly four fields. Returns `None` for malformed or all-zero IDs.
pub fn parse_traceparent(value: &str) -> Option<SpanContext> {
    let mut parts = value.trim().split('-');
    let version = parts.next()?;
    if version.len() != 2
        || version.eq_ignore_ascii_case("ff")
        || u8::from_str_radix(version, 16).is_err()
    {
        return None;
    }
    let trace_id_hex = parts.next()?;
    let span_id_hex = parts.next()?;
    let flags_hex = parts.next()?;
    if trace_id_hex.len() != 32 || span_id_hex.len() != 16 || flags_hex.len() != 2 {
        return None;
    }
    if version == "00" && parts.next().is_some() {
        return None;
    }
    let trace_id = TraceId::from_hex(trace_id_hex).ok()?;
    let span_id = SpanId::from_hex(span_id_hex).ok()?;
    let flags = u8::from_str_radix(flags_hex, 16).ok()?;
    let span_context = SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::new(flags),
        true, // remote: the producer ran in another process
        TraceState::default(),
    );
    span_context.is_valid().then_some(span_context)
}

/// An OpenTelemetry [`Context`] carrying the parsed `traceparent` as a
/// remote span context, ready for `Span::set_parent`.
pub fn remote_context(value: &str) -> Option<Context> {
    parse_traceparent(value)
        .map(|span_context| Context::new().with_remote_span_context(span_context))
}

/// Capture the ambient span context into job metadata at enqueue.
///
/// No-op when capture is disabled, there is no valid ambient context, the
/// caller already set the reserved key, or metadata is a non-object value.
pub(crate) fn inject_traceparent(metadata: &mut serde_json::Value) {
    match metadata {
        serde_json::Value::Object(map) if !map.contains_key(TRACEPARENT_METADATA_KEY) => {
            if capture_disabled() {
                return;
            }
            if let Some(traceparent) = current_traceparent() {
                map.insert(TRACEPARENT_METADATA_KEY.to_string(), traceparent.into());
            }
        }
        _ => {}
    }
}

/// The stored `traceparent` of a job's enqueue site, if one was captured.
pub fn traceparent_from_metadata(metadata: &serde_json::Value) -> Option<&str> {
    metadata.get(TRACEPARENT_METADATA_KEY)?.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn parse_roundtrip() {
        let span_context = parse_traceparent(SAMPLE).expect("valid traceparent");
        assert_eq!(
            format!(
                "00-{}-{}-{:02x}",
                span_context.trace_id(),
                span_context.span_id(),
                span_context.trace_flags().to_u8()
            ),
            SAMPLE
        );
        assert!(span_context.is_remote());
        assert!(span_context.is_sampled());
    }

    #[test]
    fn parse_rejects_malformed() {
        for bad in [
            "",
            "not-a-traceparent",
            // all-zero trace id is invalid per W3C
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            // all-zero span id
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            // version ff is forbidden
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            // version 00 must have exactly four fields
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            // truncated ids
            "00-4bf92f3577b34da6-00f067aa0ba902b7-01",
        ] {
            assert!(parse_traceparent(bad).is_none(), "should reject {bad:?}");
        }
    }

    #[test]
    fn parse_accepts_future_versions_with_extra_fields() {
        let future = "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-anything";
        assert!(parse_traceparent(future).is_some());
    }

    #[test]
    fn inject_preserves_caller_supplied_value() {
        let mut metadata = serde_json::json!({ TRACEPARENT_METADATA_KEY: SAMPLE });
        inject_traceparent(&mut metadata);
        assert_eq!(traceparent_from_metadata(&metadata), Some(SAMPLE));
    }

    #[test]
    fn inject_without_ambient_span_is_a_noop() {
        // No OpenTelemetry layer is installed in unit tests, so the ambient
        // context is invalid and nothing should be written.
        let mut metadata = serde_json::json!({});
        inject_traceparent(&mut metadata);
        assert_eq!(metadata, serde_json::json!({}));
    }

    #[test]
    fn inject_leaves_non_object_metadata_untouched() {
        let mut metadata = serde_json::Value::Null;
        inject_traceparent(&mut metadata);
        assert_eq!(metadata, serde_json::Value::Null);
    }
}
