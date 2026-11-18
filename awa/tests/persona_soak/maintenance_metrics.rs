//! Captures the maintenance leader's OpenTelemetry metrics of every in-process runtime
//! and the ring generation gauge, and writes them to `maintenance-metrics.jsonl`, one
//! cumulative data point per line. Runtimes in their own process (the python and
//! claim-and-abandon personas) are not covered; when one of those leads, its maintenance
//! activity is missing from the file.

use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PREFIX: &str = "awa.maintenance.";
const RING_GENERATION: &str = "awa.ring.generation";
const PRUNE_ATTEMPTS: &str = "awa.maintenance.prune.attempts";
const ROTATE_ATTEMPTS: &str = "awa.maintenance.rotate.attempts";
const PINNED_READY_AGE: &str = "awa.maintenance.prune.pinned_ready_age";
const CLAIM_CURSOR_ADVANCES: &str = "awa.maintenance.prune.claim_cursor_advances";
const EXPORT_EVERY: Duration = Duration::from_secs(10);

pub struct Capture {
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
}

/// Installs the global meter provider. Call before any runtime is built.
pub fn install() -> Capture {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone())
        .with_interval(EXPORT_EVERY)
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    opentelemetry::global::set_meter_provider(provider.clone());
    Capture { exporter, provider }
}

fn epoch_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn attributes<'a>(attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> Value {
    let mut map = Map::new();
    for attribute in attributes {
        map.insert(
            attribute.key.as_str().to_string(),
            Value::String(attribute.value.as_str().into_owned()),
        );
    }
    Value::Object(map)
}

macro_rules! rows {
    ($name:expr, $data:expr, $rows:expr) => {
        match $data {
            MetricData::Sum(sum) => {
                let at = epoch_ms(sum.time());
                for point in sum.data_points() {
                    $rows.push(json!({
                        "at_ms": at, "name": $name, "kind": "sum",
                        "attributes": attributes(point.attributes()),
                        "value": point.value(),
                    }));
                }
            }
            MetricData::Gauge(gauge) => {
                let at = epoch_ms(gauge.time());
                for point in gauge.data_points() {
                    $rows.push(json!({
                        "at_ms": at, "name": $name, "kind": "gauge",
                        "attributes": attributes(point.attributes()),
                        "value": point.value(),
                    }));
                }
            }
            MetricData::Histogram(histogram) => {
                let at = epoch_ms(histogram.time());
                for point in histogram.data_points() {
                    $rows.push(json!({
                        "at_ms": at, "name": $name, "kind": "histogram",
                        "attributes": attributes(point.attributes()),
                        "count": point.count(), "sum": point.sum(),
                        "max": point.max(),
                        "bounds": point.bounds().collect::<Vec<_>>(),
                        "bucket_counts": point.bucket_counts().collect::<Vec<_>>(),
                    }));
                }
            }
            MetricData::ExponentialHistogram(_) => {}
        }
    };
}

impl Capture {
    /// Writes `maintenance-metrics.jsonl` to `dir` and returns the summary of its rows.
    pub fn write(&self, dir: &Path) -> std::io::Result<Summary> {
        let rows = self.rows();
        std::fs::create_dir_all(dir)?;
        let mut file = std::fs::File::create(dir.join("maintenance-metrics.jsonl"))?;
        for row in &rows {
            writeln!(file, "{row}")?;
        }
        Ok(Summary::from_rows(&rows))
    }

    /// Every captured maintenance data point so far, one JSON object per point.
    pub fn rows(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        let mut rows: Vec<Value> = Vec::new();
        for resource in self.exporter.get_finished_metrics().unwrap_or_default() {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let name = metric.name();
                    if !name.starts_with(PREFIX) && name != RING_GENERATION {
                        continue;
                    }
                    match metric.data() {
                        AggregatedMetrics::F64(data) => rows!(name, data, rows),
                        AggregatedMetrics::U64(data) => rows!(name, data, rows),
                        AggregatedMetrics::I64(data) => rows!(name, data, rows),
                    }
                }
            }
        }
        rows
    }
}

/// The ring metrics of a run, read from the captured cumulative data points.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Summary {
    /// `awa.maintenance.prune.attempts` by `ring/outcome`, summed over reasons.
    pub prune_attempts: BTreeMap<String, u64>,
    /// `awa.maintenance.rotate.attempts` by `ring/outcome`, summed over blockers, so a
    /// busy skip counts once per kind of row that blocked it.
    pub rotate_attempts: BTreeMap<String, u64>,
    /// `awa.ring.generation` by ring: the first and the last value recorded.
    pub ring_generation: BTreeMap<String, (i64, i64)>,
    /// `awa.maintenance.prune.claim_cursor_advances` by `awa.ring.outcome`, summed over
    /// lanes; `None` when no runtime recorded it.
    pub claim_cursor_advances: Option<BTreeMap<String, u64>>,
    /// Lanes `awa.maintenance.prune.pinned_ready_age` reported pinning the queue ring at
    /// any export, and those still pinning it at the last one.
    pub pinning_lanes: usize,
    pub pinning_lanes_at_end: usize,
    /// The largest `awa.maintenance.prune.pinned_ready_age` recorded, in seconds.
    pub oldest_pinned_ready_age_seconds: Option<f64>,
}

fn attribute<'a>(row: &'a Value, key: &str) -> &'a str {
    row["attributes"][key].as_str().unwrap_or("none")
}

fn ring_and_outcome(row: &Value) -> String {
    format!(
        "{}/{}",
        attribute(row, "awa.ring"),
        attribute(row, "awa.ring.outcome")
    )
}

impl Summary {
    pub fn from_rows(rows: &[Value]) -> Self {
        let mut summary = Summary::default();
        let mut pinning_lanes: BTreeSet<String> = BTreeSet::new();
        // Each export repeats every series cumulatively, in export order, so the last
        // point of a series is its value for the whole run.
        let mut last: BTreeMap<(String, String), &Value> = BTreeMap::new();
        for row in rows {
            let name = row["name"].as_str().unwrap_or_default();
            let series = row["attributes"].to_string();
            match name {
                RING_GENERATION => {
                    let generation = row["value"].as_i64().unwrap_or_default();
                    summary
                        .ring_generation
                        .entry(attribute(row, "awa.ring").to_string())
                        .and_modify(|(_, latest)| *latest = generation)
                        .or_insert((generation, generation));
                }
                PINNED_READY_AGE => {
                    let age_seconds = row["value"].as_f64().unwrap_or_default();
                    if age_seconds > 0.0 {
                        pinning_lanes.insert(series.clone());
                        summary.oldest_pinned_ready_age_seconds = Some(
                            summary
                                .oldest_pinned_ready_age_seconds
                                .unwrap_or_default()
                                .max(age_seconds),
                        );
                    }
                }
                _ => {}
            }
            last.insert((name.to_string(), series), row);
        }
        summary.pinning_lanes = pinning_lanes.len();
        for ((name, _), row) in last {
            let total = row["value"].as_u64().unwrap_or_default();
            match name.as_str() {
                PRUNE_ATTEMPTS => {
                    *summary
                        .prune_attempts
                        .entry(ring_and_outcome(row))
                        .or_default() += total;
                }
                ROTATE_ATTEMPTS => {
                    *summary
                        .rotate_attempts
                        .entry(ring_and_outcome(row))
                        .or_default() += total;
                }
                CLAIM_CURSOR_ADVANCES => {
                    *summary
                        .claim_cursor_advances
                        .get_or_insert_with(BTreeMap::new)
                        .entry(attribute(row, "awa.ring.outcome").to_string())
                        .or_default() += total;
                }
                PINNED_READY_AGE => {
                    summary.pinning_lanes_at_end +=
                        usize::from(row["value"].as_f64().unwrap_or_default() > 0.0);
                }
                _ => {}
            }
        }
        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, attributes: Value, value: Value) -> Value {
        json!({"at_ms": 0, "name": name, "kind": "sum", "attributes": attributes, "value": value})
    }

    #[test]
    fn summary_reads_the_last_cumulative_point_of_each_series() {
        let pruned =
            json!({"awa.ring": "queue", "awa.ring.outcome": "pruned", "awa.ring.reason": "none"});
        let skipped = json!({"awa.ring": "queue", "awa.ring.outcome": "skipped_active", "awa.ring.reason": "queue.pending_ready"});
        let lane = |queue: &str| json!({"awa.ring": "queue", "awa.job.queue": queue, "awa.job.priority": "2", "awa.queue.enqueue_shard": "0"});
        let rows = [
            row(PRUNE_ATTEMPTS, pruned.clone(), json!(3)),
            row(PRUNE_ATTEMPTS, skipped.clone(), json!(1)),
            row(RING_GENERATION, json!({"awa.ring": "queue"}), json!(7)),
            row(PINNED_READY_AGE, lane("paused"), json!(12.5)),
            row(PINNED_READY_AGE, lane("starved"), json!(40.0)),
            row(PRUNE_ATTEMPTS, pruned, json!(5)),
            row(PRUNE_ATTEMPTS, skipped, json!(4)),
            row(RING_GENERATION, json!({"awa.ring": "queue"}), json!(19)),
            row(PINNED_READY_AGE, lane("paused"), json!(30.0)),
            row(PINNED_READY_AGE, lane("starved"), json!(0.0)),
        ];
        let summary = Summary::from_rows(&rows);
        assert_eq!(
            summary.prune_attempts,
            BTreeMap::from([
                ("queue/pruned".to_string(), 5),
                ("queue/skipped_active".to_string(), 4),
            ])
        );
        assert_eq!(
            summary.ring_generation,
            BTreeMap::from([("queue".to_string(), (7, 19))])
        );
        assert_eq!(summary.pinning_lanes, 2);
        assert_eq!(summary.pinning_lanes_at_end, 1);
        assert_eq!(summary.oldest_pinned_ready_age_seconds, Some(40.0));
        assert_eq!(summary.claim_cursor_advances, None);
    }
}
