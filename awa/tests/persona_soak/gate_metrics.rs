use super::config::Persona;
use super::shared::Shared;
use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry::{global, InstrumentationScope, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::Resource;
use serde::Serialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const ENV_ENDPOINT: &str = "AWA_SOAK_METRICS_ENDPOINT";
pub const ENV_DIRECTORY: &str = "AWA_SOAK_METRICS_DIR";
pub const OUTPUT_PREFIX: &str = "AWA_SOAK_METRICS ";
pub const ERROR_MARKER: &str = "AWA_SOAK_METRICS_INCOMPLETE";
const MAX_HOSTS: u64 = 4096;
const MAX_EVENTS: u64 = 100_000;
const MAX_LINE: usize = 4096;
const MAX_BYTES: u64 = 64 * 1024 * 1024;
static STATE: OnceLock<Result<Option<Telemetry>, String>> = OnceLock::new();
thread_local! { static ISSUER: RefCell<Option<HostIdentity>> = const { RefCell::new(None) }; }

struct Telemetry {
    _provider: SdkMeterProvider,
    role: String,
    process_instance: String,
    file: Mutex<File>,
    path: PathBuf,
    events: AtomicU64,
    bytes: AtomicU64,
    next_host: AtomicU64,
    failed: AtomicBool,
    runtimes: Mutex<HashMap<String, HostIdentity>>,
    child: Mutex<Option<HostIdentity>>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct HostIdentity {
    persona: String,
    client: String,
    instance: u64,
    application: String,
    thread: String,
}

struct ScopeGuard {
    previous: Option<HostIdentity>,
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        ISSUER.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}

fn with_identity<T>(identity: &HostIdentity, factory: impl FnOnce() -> T) -> T {
    let previous = ISSUER.with(|slot| slot.replace(Some(identity.clone())));
    let _guard = ScopeGuard {
        previous,
        _not_send: PhantomData,
    };
    factory()
}

fn attributed_scope(scope: InstrumentationScope, role: &str) -> InstrumentationScope {
    let identity = ISSUER.with(|slot| slot.borrow().clone());
    let mut attributes: Vec<_> = scope.attributes().cloned().collect();
    let reserved = [
        "soak.persona",
        "soak.client",
        "soak.client.instance",
        "soak.pg_application",
        "soak.process.role",
    ];
    if attributes
        .iter()
        .any(|attribute| reserved.contains(&attribute.key.as_str()))
    {
        if let Some(telemetry) = state() {
            telemetry.failed.store(true, Ordering::Relaxed);
        }
    } else {
        attributes.extend([
            KeyValue::new(
                "soak.persona",
                identity
                    .as_ref()
                    .map_or("unassigned", |identity| identity.persona.as_str())
                    .to_string(),
            ),
            KeyValue::new(
                "soak.client",
                identity
                    .as_ref()
                    .map_or("unassigned", |identity| identity.client.as_str())
                    .to_string(),
            ),
            KeyValue::new(
                "soak.client.instance",
                identity.as_ref().map_or(0, |identity| identity.instance) as i64,
            ),
            KeyValue::new(
                "soak.pg_application",
                identity
                    .as_ref()
                    .map_or("unassigned", |identity| identity.application.as_str())
                    .to_string(),
            ),
            KeyValue::new("soak.process.role", role.to_string()),
        ]);
    }
    let mut builder =
        InstrumentationScope::builder(scope.name().to_string()).with_attributes(attributes);
    if let Some(version) = scope.version() {
        builder = builder.with_version(version.to_string());
    }
    if let Some(schema) = scope.schema_url() {
        builder = builder.with_schema_url(schema.to_string());
    }
    builder.build()
}

struct ScopedProvider {
    provider: SdkMeterProvider,
    role: String,
}
impl MeterProvider for ScopedProvider {
    fn meter_with_scope(&self, scope: InstrumentationScope) -> Meter {
        self.provider
            .meter_with_scope(attributed_scope(scope, &self.role))
    }
}

fn state() -> Option<&'static Telemetry> {
    STATE
        .get()
        .and_then(|result| result.as_ref().ok())
        .and_then(Option::as_ref)
}

impl Telemetry {
    fn record(&self, event: Value) {
        let number = self.events.fetch_add(1, Ordering::Relaxed);
        let line=json!({"utc":chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis,true),
            "pid":std::process::id(),"process_role":self.role,"process_instance":self.process_instance,"event":event}).to_string()+"\n";
        let bytes = self.bytes.fetch_add(line.len() as u64, Ordering::Relaxed);
        if number >= MAX_EVENTS
            || line.len() > MAX_LINE
            || bytes.saturating_add(line.len() as u64) > MAX_BYTES
        {
            self.failed.store(true, Ordering::Relaxed);
            if number == MAX_EVENTS || bytes < MAX_BYTES {
                eprintln!("{ERROR_MARKER}");
            }
            return;
        }
        let written = self
            .file
            .lock()
            .map_err(|_| ())
            .and_then(|mut file| file.write_all(line.as_bytes()).map_err(|_| ()));
        if written.is_err() {
            self.failed.store(true, Ordering::Relaxed);
            eprintln!("{ERROR_MARKER}");
        }
    }
}

pub fn install(role: &str) -> Result<(), String> {
    STATE
        .get_or_init(|| {
            let endpoint = match std::env::var(ENV_ENDPOINT) {
                Err(std::env::VarError::NotPresent) => return Ok(None),
                Ok(value) if !value.is_empty() => value,
                _ => return Err(format!("{ENV_ENDPOINT} must be a nonempty HTTP endpoint")),
            };
            let url = reqwest::Url::parse(&endpoint)
                .map_err(|_| "invalid metrics endpoint".to_string())?;
            if !matches!(url.scheme(), "http" | "https")
                || !url.username().is_empty()
                || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            {
                return Err("metrics endpoint must be HTTP(S) without credentials, query or fragment".into());
            }
            let directory = PathBuf::from(
                std::env::var(ENV_DIRECTORY).map_err(|_| format!("{ENV_DIRECTORY} required"))?,
            );
            if !directory.is_absolute() {return Err("metrics directory must be absolute".into());}
            std::fs::create_dir_all(&directory)
                .map_err(|error| format!("metrics directory: {error}"))?;
            let epoch = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "metrics clock before epoch".to_string())?
                .as_nanos();
            let process_instance = format!("{}-{epoch}", std::process::id());
            let path = directory.join(format!("{role}-{process_instance}.jsonl"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|error| format!("metrics receipt: {error}"))?;
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&endpoint)
                .with_timeout(Duration::from_secs(3))
                .with_temporality(Temporality::Cumulative)
                .build()
                .map_err(|error| format!("metrics exporter: {error}"))?;
            let reader = PeriodicReader::builder(exporter)
                .with_interval(Duration::from_secs(5))
                .build();
            let resource = Resource::builder()
                .with_service_name(format!("awa-soak-{role}"))
                .with_attributes([
                    KeyValue::new("service.instance.id", process_instance.clone()),
                    KeyValue::new("process.pid", std::process::id() as i64),
                    KeyValue::new("soak.process.role", role.to_string()),
                ])
                .build();
            let provider = SdkMeterProvider::builder()
                .with_resource(resource)
                .with_reader(reader)
                .build();
            global::set_meter_provider(ScopedProvider {
                provider: provider.clone(),
                role: role.to_string(),
            });
            let telemetry = Telemetry {
                _provider: provider,
                role: role.to_string(),
                process_instance,
                file: Mutex::new(file),
                path,
                events: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                next_host: AtomicU64::new(1),
                failed: AtomicBool::new(false),
                runtimes: Mutex::new(HashMap::new()),
                child: Mutex::new(None),
            };
            telemetry.record(
                json!({"kind":"initialized","endpoint":endpoint,"export_interval_ms":5000,
            "delivery":"unverified","final_flush":"not_attempted","tail":"potentially_incomplete",
            "caps":{"hosts":MAX_HOSTS,"events":MAX_EVENTS,"bytes":MAX_BYTES,"line_bytes":MAX_LINE}}),
            );
            if telemetry.failed.load(Ordering::Relaxed) {
                return Err("metrics receipt initialization failed".into());
            }
            if role == "claim_child" {
                println!("{OUTPUT_PREFIX}{}", json!({"kind":"initialized","pid":std::process::id(),
                    "process_role":role,"process_instance":telemetry.process_instance,"receipt":telemetry.path,
                    "export_interval_ms":5000,"delivery":"unverified","final_flush":"not_attempted","tail":"potentially_incomplete"}));
            }
            Ok(Some(telemetry))
        })
        .as_ref()
        .map(|_| ())
        .map_err(Clone::clone)
}

pub fn begin_host(
    persona: Persona,
    client: &str,
    application: &str,
) -> Result<Option<HostIdentity>, String> {
    let Some(telemetry) = state() else {
        return Ok(None);
    };
    let instance = telemetry.next_host.fetch_add(1, Ordering::Relaxed);
    if instance > MAX_HOSTS {
        telemetry.failed.store(true, Ordering::Relaxed);
        return Err("metrics host identity cap exceeded".into());
    }
    let identity = HostIdentity {
        persona: persona.name().to_string(),
        client: client.to_string(),
        instance,
        application: application.to_string(),
        thread: format!("soak-{client}-{instance}"),
    };
    telemetry
        .runtimes
        .lock()
        .map_err(|_| "metrics runtime map poisoned".to_string())?
        .insert(identity.thread.clone(), identity.clone());
    if telemetry.role == "claim_child" {
        *telemetry
            .child
            .lock()
            .map_err(|_| "metrics child map poisoned".to_string())? = Some(identity.clone());
    }
    telemetry.record(json!({"kind":"host_construct","issuer":identity}));
    if telemetry.failed.load(Ordering::Relaxed) {
        return Err("metrics host receipt failed".into());
    }
    Ok(Some(identity))
}

pub fn thread_name(identity: Option<&HostIdentity>, fallback: &str) -> String {
    identity.map_or_else(|| fallback.to_string(), |identity| identity.thread.clone())
}

pub fn build_client(
    identity: Option<&HostIdentity>,
    factory: impl FnOnce() -> awa::Client,
) -> awa::Client {
    match identity {
        Some(identity) => with_identity(identity, factory),
        None => factory(),
    }
}

pub fn lifecycle(identity: Option<&HostIdentity>, kind: &str) {
    if let (Some(telemetry), Some(identity)) = (state(), identity) {
        telemetry.record(json!({"kind":kind,"issuer":identity,"final_flush":"not_attempted","tail":"potentially_incomplete"}));
    }
}

pub fn prune_ring(message: &str) -> Option<&'static str> {
    match message.trim_matches('"') {
        "Failed to prune queue storage queue segments" => Some("queue"),
        "Failed to prune queue storage lease segments" => Some("lease"),
        "Failed to prune queue storage claim segments" => Some("claim"),
        _ => None,
    }
}

pub fn prune_error(ring: &str) {
    let Some(telemetry) = state() else {
        return;
    };
    let current = std::thread::current();
    let thread = current.name().unwrap_or("unassigned");
    let identity = telemetry
        .runtimes
        .lock()
        .ok()
        .and_then(|runtimes| runtimes.get(thread).cloned())
        .or_else(|| {
            telemetry
                .child
                .lock()
                .ok()
                .and_then(|identity| identity.clone())
        });
    telemetry.record(json!({"kind":"prune_error_event","ring":ring,"issuer":identity,"thread":thread,
        "attribution":if identity.is_some(){"known_runtime"}else{"unassigned"},"source":"rust_trace_supplement"}));
}

pub fn absorb_output(shared: &Shared, line: &str) -> bool {
    if line == ERROR_MARKER {
        shared.incomplete("child metrics instrumentation failed", vec![]);
        return true;
    }
    let Some(payload) = line.strip_prefix(OUTPUT_PREFIX) else {
        return false;
    };
    if payload.len() > MAX_LINE || serde_json::from_str::<Value>(payload).is_err() {
        shared.incomplete("invalid child metrics receipt", vec![]);
    } else {
        shared.note("gate_metrics_child", payload);
    }
    true
}

pub fn reconcile(shared: &Shared) {
    if let Some(telemetry) = state() {
        telemetry.record(json!({"kind":"measurement_end","final_flush":"not_attempted","tail":"potentially_incomplete",
            "python_baseline_prune_errors":"unavailable"}));
        shared.note("gate_metrics",format!("receipt={}; fixed 5s OTLP collection; delivery and missing tails require external raw-file validation; no flush wait added",telemetry.path.display()));
        if telemetry.failed.load(Ordering::Relaxed) {
            shared.incomplete(
                "metrics receipt cap, scope collision, or write failure",
                vec![],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity(name: &str, instance: u64) -> HostIdentity {
        HostIdentity {
            persona: name.into(),
            client: name.into(),
            instance,
            application: format!("soak_{name}"),
            thread: name.into(),
        }
    }
    fn scope() -> InstrumentationScope {
        InstrumentationScope::builder("awa")
            .with_version("1")
            .with_schema_url("https://example.invalid/schema")
            .with_attributes([KeyValue::new("original", "kept")])
            .build()
    }
    fn attribute(scope: &InstrumentationScope, key: &str) -> String {
        scope
            .attributes()
            .find(|attribute| attribute.key.as_str() == key)
            .expect("attribute")
            .value
            .to_string()
    }
    #[test]
    fn scopes_preserve_original_metadata_and_unknown_issuers() {
        let attributed = attributed_scope(scope(), "parent");
        assert_eq!(attributed.name(), "awa");
        assert_eq!(attributed.version(), Some("1"));
        assert_eq!(
            attributed.schema_url(),
            Some("https://example.invalid/schema")
        );
        assert_eq!(attribute(&attributed, "original"), "kept");
        assert_eq!(attribute(&attributed, "soak.persona"), "unassigned");
    }
    #[test]
    fn nested_scope_and_panic_restore_previous_identity() {
        let outer = identity("outer", 1);
        let inner = identity("inner", 2);
        with_identity(&outer, || {
            let captured = with_identity(&inner, || attributed_scope(scope(), "parent"));
            assert_eq!(attribute(&captured, "soak.persona"), "inner");
            let _ = std::panic::catch_unwind(|| with_identity(&inner, || panic!("scope control")));
            assert_eq!(
                attribute(&attributed_scope(scope(), "parent"), "soak.persona"),
                "outer"
            );
            assert_eq!(attribute(&captured, "soak.persona"), "inner");
        });
        assert_eq!(
            attribute(&attributed_scope(scope(), "parent"), "soak.persona"),
            "unassigned"
        );
    }
    #[test]
    fn concurrent_construction_scopes_survive_later_records_outside_the_factory() {
        let exporter = opentelemetry_sdk::metrics::InMemoryMetricExporter::default();
        let sdk = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(exporter.clone())
                    .with_interval(Duration::from_secs(60))
                    .build(),
            )
            .build();
        let provider = std::sync::Arc::new(ScopedProvider {
            provider: sdk.clone(),
            role: "parent".into(),
        });
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = [("mia", 1), ("priya", 2)]
            .into_iter()
            .map(|(name, instance)| {
                let barrier = barrier.clone();
                let provider = provider.clone();
                std::thread::spawn(move || {
                    with_identity(&identity(name, instance), || {
                        barrier.wait();
                        provider
                            .meter_with_scope(scope())
                            .u64_counter("scope.control")
                            .build()
                    })
                })
            })
            .collect();
        let counters: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().expect("thread"))
            .collect();
        for counter in counters {
            counter.add(1, &[]);
        }
        sdk.force_flush().expect("in-memory flush");
        let metrics = exporter.get_finished_metrics().expect("captured metrics");
        let mut personas: Vec<_> = metrics
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .map(|metrics| attribute(metrics.scope(), "soak.persona"))
            .collect();
        personas.sort();
        assert_eq!(personas, vec!["mia", "priya"]);
        for metrics in metrics.iter().flat_map(|resource| resource.scope_metrics()) {
            assert_eq!(metrics.scope().name(), "awa");
            assert_eq!(metrics.scope().version(), Some("1"));
            assert_eq!(attribute(metrics.scope(), "original"), "kept");
        }
        sdk.shutdown().expect("in-memory shutdown");
    }
    #[test]
    fn only_stable_prune_error_messages_match() {
        assert_eq!(
            prune_ring("Failed to prune queue storage queue segments"),
            Some("queue")
        );
        assert_eq!(
            prune_ring("Failed to prune queue storage lease segments"),
            Some("lease")
        );
        assert_eq!(
            prune_ring("Failed to prune queue storage claim segments"),
            Some("claim")
        );
        assert_eq!(
            prune_ring("wrapped: Failed to prune queue storage claim segments"),
            None
        );
    }
}
