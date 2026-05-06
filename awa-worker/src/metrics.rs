//! Backwards-compatible re-export of OpenTelemetry metric definitions.
//!
//! The metric type and `record_*` methods now live in the internal
//! `awa-metrics` crate so non-runtime callers (`awa-ui`) can share the same
//! attribute sets without depending on the dispatcher/runtime crate graph.
//! `awa-worker` re-exports them so existing `awa_worker::AwaMetrics::*`
//! call sites keep working.

pub use awa_metrics::AwaMetrics;
