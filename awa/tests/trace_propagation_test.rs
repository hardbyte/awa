//! #110 end-to-end trace propagation: a producer's OpenTelemetry span
//! context, captured at enqueue as `metadata["awa:traceparent"]`, becomes
//! the remote parent of the worker's `job.execute` span — one connected
//! trace across processes, asserted with an in-memory span exporter.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use std::sync::OnceLock;
use std::time::Duration;

use awa::{Client, JobArgs, JobResult, QueueConfig};
use awa_model::{insert_with, migrations, trace::TRACEPARENT_METADATA_KEY, InsertOpts};
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

const QUEUE: &str = "trace_prop";
const RETRY_QUEUE: &str = "trace_retry";

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TracedJob {
    pub n: i64,
}

/// Install a global tracing subscriber with an OpenTelemetry layer backed
/// by an in-memory exporter. Global because the executor's `job.execute`
/// span is created on a spawned dispatcher task, which only sees the
/// global default. `Once`-guarded so in-process test threads share it;
/// under nextest each test is its own process anyway.
fn init_tracing() -> InMemorySpanExporter {
    static EXPORTER: OnceLock<InMemorySpanExporter> = OnceLock::new();
    EXPORTER
        .get_or_init(|| {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            use opentelemetry::trace::TracerProvider as _;
            let layer =
                tracing_opentelemetry::layer().with_tracer(provider.tracer("trace-prop-test"));
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::set_global_default(subscriber)
                .expect("set global tracing subscriber");
            exporter
        })
        .clone()
}

async fn setup(queue: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    awa_testing::setup::reset_runtime_backend(&pool).await;
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .expect("queue cleanup");
    pool
}

/// Enqueue one job inside a fresh producer span (instrumenting the insert
/// so ambient capture sees it) and return the producer's span context plus
/// the stored traceparent. The job is inserted BEFORE the worker starts so
/// the engine choice is stable and the claim is immediate.
async fn enqueue_traced(
    pool: &PgPool,
    queue: &str,
    n: i64,
) -> (opentelemetry::trace::SpanContext, String) {
    let producer_span = tracing::info_span!("test.enqueue");
    let producer_context = producer_span.context().span().span_context().clone();
    assert!(
        producer_context.is_valid(),
        "test subscriber should produce a valid span context"
    );
    let inserted = insert_with(
        pool,
        &TracedJob { n },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .instrument(producer_span)
    .await
    .expect("insert should succeed");

    let stored = inserted
        .metadata
        .get(TRACEPARENT_METADATA_KEY)
        .and_then(|v| v.as_str())
        .expect("traceparent should be captured into metadata");
    assert!(
        stored.contains(&producer_context.trace_id().to_string()),
        "stored traceparent {stored} should carry producer trace id"
    );
    (producer_context, stored.to_string())
}

#[tokio::test]
async fn traceparent_flows_from_enqueue_to_execution_span() {
    // No exporter.reset(): under threaded `cargo test` the exporter is
    // shared with the other tests in this file, so every lookup below is
    // scoped by this test's own trace/span ids instead.
    let exporter = init_tracing();
    let pool = setup(QUEUE).await;

    let (producer_context, stored) = enqueue_traced(&pool, QUEUE, 1).await;

    // Execute the job and channel the handler-visible metadata out.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<serde_json::Value>(1);
    let client = Client::builder(pool.clone())
        .queue(
            QUEUE,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(100),
                ..QueueConfig::default()
            },
        )
        .register::<TracedJob, _, _>(move |_args: TracedJob, ctx| {
            let tx = tx.clone();
            let handler_metadata = ctx.job.metadata.clone();
            async move {
                let _ = tx.send(handler_metadata).await;
                Ok(JobResult::Completed)
            }
        })
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");

    let handler_metadata = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .expect("job should execute within 20s")
        .expect("handler should send metadata");
    assert_eq!(
        handler_metadata
            .get(TRACEPARENT_METADATA_KEY)
            .and_then(|v| v.as_str()),
        Some(stored.as_str()),
        "handler-visible metadata should carry the captured traceparent"
    );

    client.shutdown(Duration::from_secs(10)).await;

    // The execution span joined the producer's trace as a remote child.
    let spans = exporter.get_finished_spans().expect("exported spans");
    let execute_span = spans
        .iter()
        .find(|s| {
            s.name.starts_with("job.execute")
                && s.span_context.trace_id() == producer_context.trace_id()
        })
        .unwrap_or_else(|| {
            panic!(
                "job.execute span should continue the producer trace; saw: {:?}",
                spans.iter().map(|s| s.name.clone()).collect::<Vec<_>>()
            )
        });
    // The captured traceparent names the nearest enqueue-site span —
    // `insert_with`'s own instrumented producer span, a child of the test
    // span — so the exported chain is test.enqueue → send → job.execute.
    let enqueue_site = awa_model::trace::parse_traceparent(&stored).expect("stored is well-formed");
    assert_eq!(
        execute_span.parent_span_id,
        enqueue_site.span_id(),
        "execution span's parent should be the enqueue-site span"
    );
    assert_eq!(
        execute_span.span_kind,
        opentelemetry::trace::SpanKind::Consumer,
        "execution span should map otel.kind = consumer"
    );
    assert!(
        execute_span
            .attributes
            .iter()
            .any(|kv| { kv.key.as_str() == "messaging.system" && kv.value.as_str() == "awa" }),
        "execution span should carry messaging semantic conventions"
    );
    let insert_span = spans
        .iter()
        .find(|s| s.span_context.span_id() == enqueue_site.span_id())
        .expect("enqueue-site span should be exported");
    assert_eq!(
        insert_span.parent_span_id,
        producer_context.span_id(),
        "enqueue-site span should be a child of the producer span"
    );
    assert_eq!(
        insert_span.span_kind,
        opentelemetry::trace::SpanKind::Producer,
        "enqueue-site span should map otel.kind = producer"
    );
    assert_eq!(
        insert_span.name.as_ref(),
        format!("send {QUEUE}"),
        "enqueue-site span should use the messaging send naming"
    );

    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(QUEUE)
        .execute(&pool)
        .await
        .expect("queue cleanup");
}

/// Retries deliberately do NOT extend the producer's trace: a second
/// attempt starts a fresh root trace carrying a span LINK back to the
/// enqueue site (OTel messaging guidance for deferred processing — a job
/// retried hours later must not stretch the original trace across its
/// backoff schedule or inherit its sampling decision).
#[tokio::test]
async fn retry_starts_new_trace_linked_to_enqueue_site() {
    let exporter = init_tracing();
    let pool = setup(RETRY_QUEUE).await;

    let (producer_context, stored) = enqueue_traced(&pool, RETRY_QUEUE, 2).await;
    let enqueue_site = awa_model::trace::parse_traceparent(&stored).expect("stored is well-formed");

    // Fail attempt 1 with an immediate retry; complete attempt 2.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<i16>(2);
    let client = Client::builder(pool.clone())
        .queue(
            RETRY_QUEUE,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(100),
                ..QueueConfig::default()
            },
        )
        .register::<TracedJob, _, _>(move |_args: TracedJob, ctx| {
            let tx = tx.clone();
            let attempt = ctx.job.attempt;
            async move {
                let _ = tx.send(attempt).await;
                if attempt <= 1 {
                    Ok(JobResult::RetryAfter(Duration::ZERO))
                } else {
                    Ok(JobResult::Completed)
                }
            }
        })
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");

    let first = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .expect("attempt 1 should run within 20s")
        .expect("handler should report attempt");
    assert_eq!(first, 1);
    let second = tokio::time::timeout(Duration::from_secs(60), rx.recv())
        .await
        .expect("retry should run within 60s")
        .expect("handler should report attempt");
    assert_eq!(second, 2);

    client.shutdown(Duration::from_secs(10)).await;

    let spans = exporter.get_finished_spans().expect("exported spans");
    let retry_span = spans
        .iter()
        .find(|s| {
            s.name.starts_with("job.execute")
                && s.links
                    .links
                    .iter()
                    .any(|l| l.span_context.span_id() == enqueue_site.span_id())
        })
        .expect("retry attempt should export a span linked to the enqueue site");
    assert_ne!(
        retry_span.span_context.trace_id(),
        producer_context.trace_id(),
        "retry should start a fresh trace, not extend the producer's"
    );
    assert_eq!(
        retry_span.parent_span_id,
        opentelemetry::trace::SpanId::INVALID,
        "retry span should be a root span"
    );

    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(RETRY_QUEUE)
        .execute(&pool)
        .await
        .expect("queue cleanup");
}

/// E8 gate (#110): ambient capture must add <2% to sampled enqueue latency.
/// The added work all lives in `prepare_raw_job_insert` (ambient context
/// read + traceparent formatting + one metadata key), so this measures that
/// path directly — a full enqueue also pays a Postgres roundtrip (~1ms),
/// which end-to-end timing cannot resolve a μs-scale delta against.
/// Not a CI assertion — run with:
/// `cargo test -p awa --test trace_propagation_test --release -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn enqueue_overhead_e8_gate() {
    const N: usize = 200_000;
    let _exporter = init_tracing();

    fn run_batch(label: &str, n: usize) -> Duration {
        let span = tracing::info_span!("bench.enqueue");
        let _guard = span.enter();
        let started = std::time::Instant::now();
        for i in 0..n {
            let prepared = awa_model::prepare_raw_job_insert(
                "bench_kind",
                serde_json::json!({ "n": i }),
                InsertOpts {
                    queue: QUEUE.into(),
                    ..Default::default()
                },
            )
            .expect("prepare");
            std::hint::black_box(prepared);
        }
        let elapsed = started.elapsed();
        println!(
            "{label}: {n} prepares in {elapsed:?} ({:.2}µs/prepare)",
            elapsed.as_secs_f64() * 1e6 / n as f64
        );
        elapsed
    }

    // Warmup, then capture-on vs capture-off inside an identical span.
    run_batch("warmup     ", N / 10);
    let with_capture = run_batch("capture on ", N);
    std::env::set_var(awa_model::trace::TRACE_CAPTURE_ENV, "off");
    let without_capture = run_batch("capture off", N);
    std::env::remove_var(awa_model::trace::TRACE_CAPTURE_ENV);

    let per_insert = (with_capture.as_secs_f64() - without_capture.as_secs_f64()) / N as f64;
    println!(
        "capture cost: {:.3}µs/enqueue ({:+.3}% of a 1ms enqueue roundtrip)",
        per_insert * 1e6,
        per_insert / 1e-3 * 100.0
    );
}
