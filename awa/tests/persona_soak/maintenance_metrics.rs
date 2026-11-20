//! Captures the maintenance leader's OpenTelemetry metrics of every in-process runtime
//! and the ring generation gauge, and writes them to `maintenance-metrics.jsonl`, one
//! cumulative data point per line. Runtimes in their own process (the python and
//! claim-and-abandon personas) are not covered; when one of those leads, its maintenance
//! activity is missing from the file.
//!
//! This process is also the one whose resident memory the soak gates, so the exporter
//! keeps no history: each export's selected data points are appended to the file as they
//! arrive, and only the latest point of each series is held for the summary.

use awa::model::{QueueStorage, QueueStorageConfig};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PREFIX: &str = "awa.maintenance.";
const RING_GENERATION: &str = "awa.ring.generation";
const PRUNE_ATTEMPTS: &str = "awa.maintenance.prune.attempts";
const ROTATE_ATTEMPTS: &str = "awa.maintenance.rotate.attempts";
const PINNED_READY_AGE: &str = "awa.maintenance.prune.pinned_ready_age";
const CLAIM_CURSOR_ADVANCES: &str = "awa.maintenance.prune.claim_cursor_advances";
const EXPORT_EVERY: Duration = Duration::from_secs(10);
const FILE_NAME: &str = "maintenance-metrics.jsonl";

struct Sink {
    file: BufWriter<std::fs::File>,
    /// The first write error; later exports are dropped once the file is incomplete.
    error: Option<String>,
    summary: Accumulator,
}

/// Appends each export's selected data points to the file and folds them into the
/// summary, then drops them.
#[derive(Clone)]
struct StreamingExporter(Arc<Mutex<Sink>>);

impl PushMetricExporter for StreamingExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let rows = selected_rows(metrics);
        let mut sink = self
            .0
            .lock()
            .map_err(|_| OTelSdkError::InternalFailure("metrics sink poisoned".into()))?;
        if sink.error.is_some() {
            return Ok(());
        }
        for row in &rows {
            if let Err(error) = writeln!(sink.file, "{row}") {
                sink.error = Some(error.to_string());
                return Ok(());
            }
            sink.summary.add(row);
        }
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        let mut sink = self
            .0
            .lock()
            .map_err(|_| OTelSdkError::InternalFailure("metrics sink poisoned".into()))?;
        sink.file
            .flush()
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.force_flush()
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

pub struct Capture {
    sink: Arc<Mutex<Sink>>,
    provider: SdkMeterProvider,
}

/// Creates `maintenance-metrics.jsonl` in `dir` and installs the global meter provider
/// that streams into it. Call before any runtime is built.
pub fn install(dir: &Path) -> std::io::Result<Capture> {
    let capture = capture(dir)?;
    opentelemetry::global::set_meter_provider(capture.provider.clone());
    Ok(capture)
}

fn capture(dir: &Path) -> std::io::Result<Capture> {
    std::fs::create_dir_all(dir)?;
    let sink = Arc::new(Mutex::new(Sink {
        file: BufWriter::new(std::fs::File::create(dir.join(FILE_NAME))?),
        error: None,
        summary: Accumulator::default(),
    }));
    let reader = PeriodicReader::builder(StreamingExporter(sink.clone()))
        .with_interval(EXPORT_EVERY)
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    Ok(Capture { sink, provider })
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

/// The selected data points of one export, one JSON row each.
fn selected_rows(metrics: &ResourceMetrics) -> Vec<Value> {
    let mut rows: Vec<Value> = Vec::new();
    for scope in metrics.scope_metrics() {
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
    rows
}

impl Capture {
    /// Exports once more, completes `maintenance-metrics.jsonl` and returns the summary of
    /// its rows. `pinning_lanes_at_end` is left for the caller to read from storage.
    pub fn finish(&self) -> Result<Summary, String> {
        let _ = self.provider.force_flush();
        let mut sink = self.sink.lock().map_err(|_| "metrics sink poisoned")?;
        if let Err(error) = sink.file.flush() {
            sink.error.get_or_insert(error.to_string());
        }
        match &sink.error {
            Some(error) => Err(format!("could not write {FILE_NAME}: {error}")),
            None => Ok(sink.summary.summary()),
        }
    }
}

/// Lanes holding ready rows at or past their claim cursor in the oldest sealed queue ring
/// slot, read from storage. The pinned-age gauge cannot answer this at the end of a run:
/// a leader zeroes it when it gives up leadership, pinned or not.
pub async fn pinning_lanes_now(pool: &sqlx::PgPool, schema: &str) -> Result<usize, String> {
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.to_string(),
        ..QueueStorageConfig::default()
    })
    .map_err(|error| error.to_string())?;
    let pin = store
        .queue_ring_pin(pool)
        .await
        .map_err(|error| error.to_string())?;
    Ok(pin.map_or(0, |pin| pin.lanes.len()))
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
    /// any export.
    pub pinning_lanes: usize,
    /// Lanes pinning the queue ring once the runtimes had stopped, read from storage;
    /// `None` when that read failed.
    pub pinning_lanes_at_end: Option<usize>,
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

/// Folds rows into a [`Summary`] one at a time. It holds the latest row of each series,
/// so its size follows the number of series, not the length of the run.
#[derive(Default)]
struct Accumulator {
    ring_generation: BTreeMap<String, (i64, i64)>,
    pinning_lanes: BTreeSet<String>,
    oldest_pinned_ready_age_seconds: Option<f64>,
    // Each export repeats every series cumulatively, in export order, so the last point
    // of a series is its value for the whole run.
    last: BTreeMap<(String, String), Value>,
}

impl Accumulator {
    fn add(&mut self, row: &Value) {
        let name = row["name"].as_str().unwrap_or_default();
        let series = row["attributes"].to_string();
        match name {
            RING_GENERATION => {
                let generation = row["value"].as_i64().unwrap_or_default();
                self.ring_generation
                    .entry(attribute(row, "awa.ring").to_string())
                    .and_modify(|(_, latest)| *latest = generation)
                    .or_insert((generation, generation));
            }
            PINNED_READY_AGE => {
                let age_seconds = row["value"].as_f64().unwrap_or_default();
                if age_seconds > 0.0 {
                    self.pinning_lanes.insert(series.clone());
                    self.oldest_pinned_ready_age_seconds = Some(
                        self.oldest_pinned_ready_age_seconds
                            .unwrap_or_default()
                            .max(age_seconds),
                    );
                }
            }
            _ => {}
        }
        if matches!(
            name,
            PRUNE_ATTEMPTS | ROTATE_ATTEMPTS | CLAIM_CURSOR_ADVANCES
        ) {
            self.last.insert((name.to_string(), series), row.clone());
        }
    }

    fn summary(&self) -> Summary {
        let mut summary = Summary {
            ring_generation: self.ring_generation.clone(),
            pinning_lanes: self.pinning_lanes.len(),
            oldest_pinned_ready_age_seconds: self.oldest_pinned_ready_age_seconds,
            ..Summary::default()
        };
        for ((name, _), row) in &self.last {
            let total = row["value"].as_u64().unwrap_or_default();
            let counts = match name.as_str() {
                PRUNE_ATTEMPTS => &mut summary.prune_attempts,
                ROTATE_ATTEMPTS => &mut summary.rotate_attempts,
                CLAIM_CURSOR_ADVANCES => {
                    *summary
                        .claim_cursor_advances
                        .get_or_insert_with(BTreeMap::new)
                        .entry(attribute(row, "awa.ring.outcome").to_string())
                        .or_default() += total;
                    continue;
                }
                _ => continue,
            };
            *counts.entry(ring_and_outcome(row)).or_default() += total;
        }
        summary
    }
}

impl Summary {
    #[cfg(test)]
    fn from_rows(rows: &[Value]) -> Self {
        let mut accumulator = Accumulator::default();
        for row in rows {
            accumulator.add(row);
        }
        accumulator.summary()
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
        assert_eq!(summary.pinning_lanes_at_end, None);
        assert_eq!(summary.oldest_pinned_ready_age_seconds, Some(40.0));
        assert_eq!(summary.claim_cursor_advances, None);
    }

    #[test]
    fn capture_streams_only_the_selected_metrics_to_the_file() {
        use opentelemetry::metrics::MeterProvider;
        let dir = std::env::temp_dir().join(format!(
            "awa-soak-maintenance-metrics-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let capture = capture(&dir).expect("create the capture");
        let meter = capture.provider.meter("soak-test");
        let attempts = meter.u64_counter(PRUNE_ATTEMPTS).build();
        let other = meter.u64_counter("awa.job.completed").build();
        let pruned = [
            opentelemetry::KeyValue::new("awa.ring", "queue"),
            opentelemetry::KeyValue::new("awa.ring.outcome", "pruned"),
        ];
        for _ in 0..3 {
            attempts.add(2, &pruned);
            other.add(1, &[]);
            capture.provider.force_flush().expect("export");
        }
        let summary = capture.finish().expect("finish the capture");
        assert_eq!(
            summary.prune_attempts,
            BTreeMap::from([("queue/pruned".to_string(), 6)])
        );
        let file = std::fs::read_to_string(dir.join(FILE_NAME)).expect("read the file");
        let rows: Vec<Value> = file
            .lines()
            .map(|line| serde_json::from_str(line).expect("JSON row"))
            .collect();
        assert!(rows.len() >= 3, "one row per export: {rows:?}");
        assert!(rows.iter().all(|row| row["name"] == PRUNE_ATTEMPTS));
        assert_eq!(rows.last().expect("a row")["value"], 6);
        let sink = capture.sink.lock().expect("sink");
        assert_eq!(
            sink.summary.last.len(),
            1,
            "one series, however many exports"
        );
        drop(sink);
        let _ = capture.provider.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
