//! Shared benchmark output schema for machine-readable JSONL results.
//!
//! Both Rust and Python benchmarks emit one JSON line per scenario using
//! this schema (schema_version=1). Human-readable summaries go to stdout
//! as before; these JSONL records are printed on a separate line prefixed
//! with `@@BENCH_JSON@@` for easy extraction.

use serde::Serialize;
use std::collections::HashMap;

pub const SCHEMA_VERSION: u32 = 1;

/// Marker prefix so scripts can extract JSONL from mixed stdout.
pub const JSONL_PREFIX: &str = "@@BENCH_JSON@@";

#[derive(Debug, Serialize)]
pub struct BenchmarkResult {
    pub schema_version: u32,
    pub scenario: String,
    pub language: String,
    pub seeded: u64,
    pub metrics: BenchMetrics,
    pub outcomes: HashMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct BenchMetrics {
    pub throughput: BenchThroughput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain_time_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<BenchLatency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescue: Option<BenchRescue>,
}

#[derive(Debug, Serialize)]
pub struct BenchThroughput {
    pub handler_per_s: f64,
    pub db_finalized_per_s: f64,
}

#[derive(Debug, Serialize)]
pub struct BenchLatency {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct BenchRescue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_rescued: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_timeouts: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_rescued: Option<u64>,
}

impl BenchmarkResult {
    /// Emit this result as a JSONL line to stdout.
    pub fn emit(&self) {
        let json = serde_json::to_string(self).expect("failed to serialize benchmark result");
        println!("{JSONL_PREFIX}{json}");
    }
}
