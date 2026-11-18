//! Captures the maintenance leader's OpenTelemetry metrics of every in-process runtime
//! and writes them to `maintenance-metrics.jsonl`, one cumulative data point per line.
//! Runtimes in their own process (the python and claim-and-abandon personas) are not
//! covered; when one of those leads, its maintenance activity is missing from the file.

use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use serde_json::{json, Map, Value};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PREFIX: &str = "awa.maintenance.";
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
    pub fn write(&self, dir: &Path) -> std::io::Result<()> {
        let _ = self.provider.force_flush();
        let mut rows: Vec<Value> = Vec::new();
        for resource in self.exporter.get_finished_metrics().unwrap_or_default() {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let name = metric.name();
                    if !name.starts_with(PREFIX) {
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
        std::fs::create_dir_all(dir)?;
        let mut file = std::fs::File::create(dir.join("maintenance-metrics.jsonl"))?;
        for row in rows {
            writeln!(file, "{row}")?;
        }
        Ok(())
    }
}
