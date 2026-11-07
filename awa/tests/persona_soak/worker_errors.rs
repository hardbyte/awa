//! A structured gate for a known worker validation failure, independent of log text.
//!
//! This subscriber covers Rust runtimes in this process. The separately spawned
//! Rust claiming child forwards only a fixed marker over its existing output pipe.
//! Python-extension internals have no tracing bridge and are not covered by it.

use super::shared::Shared;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

const MISSING_READY: &str = "ready row missing for deleted lease";
const AMBIGUOUS_READY: &str = "ready body ambiguous for deleted lease";
const CHILD_MARKER: &str = "AWA_SOAK_WORKER_ERROR ready_body_unavailable";
static EVIDENCE: OnceLock<Result<Arc<WorkerErrors>, String>> = OnceLock::new();

#[derive(Default)]
pub struct WorkerErrors {
    invalid_ready_body: AtomicU64,
    child_readers: AtomicUsize,
}

#[derive(Default)]
struct ErrorFields {
    invalid_ready_body: bool,
    prune_ring: Option<&'static str>,
}

impl Visit for ErrorFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.prune_ring = super::gate_metrics::prune_ring(value);
        }
        if matches!(field.name(), "error" | "message") {
            self.invalid_ready_body |=
                value.contains(MISSING_READY) || value.contains(AMBIGUOUS_READY);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if matches!(field.name(), "error" | "message") {
            let formatted = format!("{value:?}");
            if field.name() == "message" {
                self.prune_ring = super::gate_metrics::prune_ring(&formatted);
            }
            self.invalid_ready_body |=
                formatted.contains(MISSING_READY) || formatted.contains(AMBIGUOUS_READY);
        }
    }
}

#[derive(Clone)]
struct ErrorLayer {
    evidence: Arc<WorkerErrors>,
    child: bool,
}

impl<S: Subscriber> Layer<S> for ErrorLayer {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if *event.metadata().level() > tracing::Level::WARN {
            return;
        }
        let mut fields = ErrorFields::default();
        event.record(&mut fields);
        if *event.metadata().level() == tracing::Level::ERROR {
            if let Some(ring) = fields.prune_ring {
                super::gate_metrics::prune_error(ring);
            }
        }
        if fields.invalid_ready_body {
            self.evidence
                .invalid_ready_body
                .fetch_add(1, Ordering::Relaxed);
            if self.child {
                // One short synchronous line only for this invariant failure.
                // The parent's reader is tracked through EOF before reconciliation.
                eprintln!("{CHILD_MARKER}");
            }
        }
    }
}

/// Install once, before spawning any worker. An existing unrelated subscriber
/// makes setup fail rather than silently dropping the error gate.
pub fn install(child: bool) -> Result<Arc<WorkerErrors>, String> {
    EVIDENCE
        .get_or_init(|| {
            let evidence = Arc::new(WorkerErrors::default());
            let subscriber = Registry::default()
                .with(
                    ErrorLayer {
                        evidence: evidence.clone(),
                        child,
                    }
                    .with_filter(LevelFilter::WARN),
                )
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_ansi(false)
                        .with_writer(std::io::stderr)
                        .with_filter(LevelFilter::WARN),
                );
            tracing::subscriber::set_global_default(subscriber)
                .map_err(|error| format!("worker error tracing unavailable: {error}"))?;
            Ok(evidence)
        })
        .clone()
}

pub struct ChildOutputGuard(Arc<WorkerErrors>);

impl Drop for ChildOutputGuard {
    fn drop(&mut self) {
        self.0.child_readers.fetch_sub(1, Ordering::Release);
    }
}

/// Register before spawning the existing output reader, so a fast child exit
/// cannot race the final pending-reader check.
pub fn child_output_guard() -> ChildOutputGuard {
    let evidence = EVIDENCE
        .get()
        .expect("worker error tracing installed before child spawn")
        .as_ref()
        .expect("worker error tracing installation succeeded")
        .clone();
    evidence.child_readers.fetch_add(1, Ordering::Relaxed);
    ChildOutputGuard(evidence)
}

pub fn absorb_child_marker(line: &str) -> bool {
    if line != CHILD_MARKER {
        return false;
    }
    EVIDENCE
        .get()
        .expect("worker error tracing installed before child spawn")
        .as_ref()
        .expect("worker error tracing installation succeeded")
        .invalid_ready_body
        .fetch_add(1, Ordering::Relaxed);
    true
}

/// Call after actor shutdown and the admin child stop, before final report.
pub async fn reconcile(shared: &Shared, evidence: &WorkerErrors) {
    let drained = tokio::time::timeout(
        crate::ci_timing::scaled_timeout(Duration::from_secs(5)),
        async {
            while evidence.child_readers.load(Ordering::Acquire) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .is_ok();
    if !drained {
        shared.incomplete("claiming-child error output did not drain", vec![]);
    }
    let missing = evidence.invalid_ready_body.load(Ordering::Relaxed);
    if missing != 0 {
        shared
            .run
            .add("worker_invalid_ready_body_events", missing as i64);
        shared.violation(
            "worker_ready_body_invalid",
            format!("{missing} worker event(s) reported missing or ambiguous ready bodies; see WARN/ERROR output"),
            vec![],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DisplayOnly;

    impl std::fmt::Display for DisplayOnly {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                formatter,
                "queue storage {MISSING_READY} job 17 run_lease 2"
            )
        }
    }

    impl std::fmt::Debug for DisplayOnly {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("debug form omits the error")
        }
    }

    fn count_events(emit: impl FnOnce()) -> u64 {
        let evidence = Arc::new(WorkerErrors::default());
        let subscriber = Registry::default().with(ErrorLayer {
            evidence: evidence.clone(),
            child: false,
        });
        tracing::subscriber::with_default(subscriber, emit);
        evidence.invalid_ready_body.load(Ordering::Relaxed)
    }

    #[test]
    fn display_error_field_is_counted_once_even_when_message_also_matches() {
        assert_eq!(
            count_events(|| {
                tracing::warn!(error = %DisplayOnly, "{MISSING_READY}");
            }),
            1
        );
    }

    #[test]
    fn string_error_and_message_only_forms_are_counted() {
        assert_eq!(
            count_events(|| {
                tracing::error!(error = MISSING_READY, "rescue failed");
                tracing::warn!("queue storage {MISSING_READY} job 18 run_lease 3");
            }),
            2
        );
    }

    #[test]
    fn ambiguous_ready_body_failure_is_also_gated() {
        assert_eq!(
            count_events(|| {
                tracing::error!(error = AMBIGUOUS_READY, "hydrate failed");
            }),
            1
        );
    }

    #[test]
    fn unrelated_chaos_warnings_and_payload_text_are_not_gated() {
        assert_eq!(
            count_events(|| {
                tracing::warn!(error = "canceling statement due to user request", "chaos");
                tracing::error!(error = "lock timeout", "expected contention");
                tracing::warn!(payload = MISSING_READY, "a job argument");
                tracing::info!(error = MISSING_READY, "not a worker failure event");
            }),
            0
        );
    }
}
