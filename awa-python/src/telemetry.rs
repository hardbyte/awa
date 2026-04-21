//! OpenTelemetry initialisation exposed to Python.
//!
//! Rust's `opentelemetry` crate keeps a process-global `MeterProvider` that
//! awa's Rust code records against via `opentelemetry::global::meter(...)`.
//! That global is separate from Python's `opentelemetry-sdk` state, so a
//! Python-side provider does not flow through to awa. This module exposes
//! `awa.init_telemetry(...)` to Python, which installs a Rust-side OTLP
//! pipeline so awa's metrics reach an OTel collector.
//!
//! The function is idempotent: calling it twice is safe but only the first
//! call installs a provider.

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Holds the installed provider so it stays alive for the process lifetime
/// and can be flushed on shutdown.
static PROVIDER: OnceLock<Mutex<Option<SdkMeterProvider>>> = OnceLock::new();

/// Initialise OTLP metrics export for awa's Rust runtime.
///
/// Args:
///     endpoint: OTLP gRPC endpoint, e.g. ``"http://localhost:4317"``.
///     service_name: ``service.name`` resource attribute on emitted metrics.
///     export_interval_ms: How often the exporter drains metric readings.
///         Defaults to 5 seconds.
///
/// Returns ``True`` if a provider was installed, ``False`` if one was already
/// installed (subsequent calls are no-ops).
///
/// The function is safe to call from any Python thread. It does not need to
/// be awaited — configuration is synchronous, export happens on a background
/// tokio task.
#[pyfunction]
#[pyo3(signature = (endpoint, service_name, export_interval_ms = 5000))]
pub(crate) fn init_telemetry(
    endpoint: String,
    service_name: String,
    export_interval_ms: u64,
) -> PyResult<bool> {
    let cell = PROVIDER.get_or_init(|| Mutex::new(None));
    let mut guard = cell
        .lock()
        .map_err(|_| PyRuntimeError::new_err("awa telemetry init lock poisoned"))?;
    if guard.is_some() {
        return Ok(false);
    }

    // MetricExporter::build uses the current tokio runtime to set up its
    // client. Use awa-python's pyo3-async-runtimes tokio runtime so the
    // exporter lives on a well-known reactor for the life of the process.
    let runtime = pyo3_async_runtimes::tokio::get_runtime();

    let exporter = runtime
        .block_on(async {
            opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&endpoint)
                .build()
        })
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "failed to build OTLP metric exporter for {endpoint}: {err}"
            ))
        })?;

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_millis(export_interval_ms))
        .build();

    let resource = Resource::builder().with_service_name(service_name).build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    global::set_meter_provider(provider.clone());
    *guard = Some(provider);
    Ok(true)
}

/// Force the installed provider to flush pending metrics.
///
/// Useful in tests and short-lived scripts where the periodic reader may not
/// have fired before process exit.
#[pyfunction]
pub(crate) fn shutdown_telemetry() -> PyResult<()> {
    let Some(cell) = PROVIDER.get() else {
        return Ok(());
    };
    let mut guard = cell
        .lock()
        .map_err(|_| PyRuntimeError::new_err("awa telemetry init lock poisoned"))?;
    if let Some(provider) = guard.take() {
        provider
            .shutdown()
            .map_err(|err| PyRuntimeError::new_err(format!("telemetry shutdown failed: {err}")))?;
    }
    Ok(())
}
