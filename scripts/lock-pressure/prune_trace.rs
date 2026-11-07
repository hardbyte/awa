#[derive(Default)]
struct PruneCallTrace {
    call_id: String,
    ring: &'static str,
    application: String,
    lock_ok: std::sync::atomic::AtomicU64,
    lock_failed: std::sync::atomic::AtomicU64,
}
static PRUNE_CALL_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
tokio::task_local! {
    static PRUNE_CALL_TRACE: std::sync::Arc<PruneCallTrace>;
}
fn prune_trace_utc_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_nanos() as u64
}
fn prune_trace_emit(context: &PruneCallTrace, event: &str, details: serde_json::Value) {
    use std::io::Write;
    let row = serde_json::json!({
        "event": event, "call_id": context.call_id, "ring": context.ring,
        "application": context.application, "client_pid": std::process::id(),
        "utc_ns": prune_trace_utc_ns(), "details": details
    });
    let _ = writeln!(std::io::stderr().lock(), "AWA_PRUNE_CALL {}", row);
}
trait PruneTraceOutcome {
    fn trace_status(&self) -> &'static str;
}
impl PruneTraceOutcome for PruneOutcome {
    fn trace_status(&self) -> &'static str {
        match self {
            Self::Blocked { .. } => "blocked",
            Self::Pruned { .. } => "pruned",
            Self::Noop => "noop",
            Self::SkippedActive { .. } => "skipped",
        }
    }
}
impl PruneTraceOutcome for TerminalDeltaSlotRollup {
    fn trace_status(&self) -> &'static str {
        match self {
            Self::Blocked => "blocked",
            Self::Rolled { .. } => "pruned",
            Self::Empty => "noop",
            Self::SkippedActive | Self::SkippedMvccPinned => "skipped",
        }
    }
}
async fn prune_trace_call<T: std::fmt::Debug + PruneTraceOutcome>(
    pool: &PgPool,
    ring: &'static str,
    future: impl std::future::Future<Output = Result<T, AwaError>>,
) -> Result<T, AwaError> {
    let context = std::sync::Arc::new(PruneCallTrace {
        call_id: format!("{}:{}", std::process::id(), PRUNE_CALL_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
        ring,
        application: pool.connect_options().get_application_name().unwrap_or("").to_owned(),
        ..Default::default()
    });
    let started = std::time::Instant::now();
    prune_trace_emit(&context, "call_start", serde_json::json!({}));
    let result = PRUNE_CALL_TRACE.scope(context.clone(), future).await;
    let debug = format!("{:?}", result);
    let ok = context.lock_ok.load(Ordering::Relaxed);
    let failed = context.lock_failed.load(Ordering::Relaxed);
    let status = result.as_ref().map(|value| value.trace_status()).unwrap_or("error");
    let blocked_or_error = matches!(status, "blocked" | "error");
    let classification = if failed > 0 {
        "initial_lock_failed"
    } else if ok > 0 && blocked_or_error {
        "lock_succeeded_then_BlockedorError"
    } else if blocked_or_error {
        "pre_lock_other"
    } else if status == "pruned" {
        "pruned"
    } else {
        status
    };
    prune_trace_emit(&context, "call_end", serde_json::json!({
        "elapsed_ns": started.elapsed().as_nanos() as u64,
        "classification": classification, "result_debug": debug,
        "initial_lock_successes": ok, "initial_lock_failures": failed
    }));
    result
}
async fn prune_trace_lock<T>(
    slot: i32,
    generation: i64,
    future: impl std::future::Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, sqlx::Error> {
    let context = PRUNE_CALL_TRACE.try_with(std::sync::Arc::clone).ok();
    let started = std::time::Instant::now();
    if let Some(context) = &context {
        prune_trace_emit(context, "initial_lock_start", serde_json::json!({"slot": slot, "generation": generation}));
    }
    let result = future.await;
    if let Some(context) = &context {
        let (status, code, error) = match &result {
            Ok(_) => { context.lock_ok.fetch_add(1, Ordering::Relaxed); ("ok", None, None) }
            Err(error) => {
                context.lock_failed.fetch_add(1, Ordering::Relaxed);
                ("error", error.as_database_error().and_then(|e| e.code()).map(|v| v.into_owned()), Some(format!("{:?}", error)))
            }
        };
        prune_trace_emit(context, "initial_lock_end", serde_json::json!({
            "slot": slot, "generation": generation, "status": status,
            "sqlstate": code, "error_debug": error,
            "elapsed_ns": started.elapsed().as_nanos() as u64
        }));
    }
    result
}

async fn prune_trace_stage<T>(
    stage: &'static str,
    future: impl std::future::Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, sqlx::Error> {
    let context = PRUNE_CALL_TRACE.try_with(std::sync::Arc::clone).ok();
    let started = std::time::Instant::now();
    if let Some(context) = &context {
        prune_trace_emit(context, "stage_start", serde_json::json!({"stage": stage}));
    }
    let result = future.await;
    if let Some(context) = &context {
        prune_trace_emit(context, "stage_end", serde_json::json!({
            "stage": stage, "ok": result.is_ok(),
            "sqlstate": result.as_ref().err().and_then(|e| e.as_database_error()).and_then(|e| e.code()).map(|v| v.into_owned()),
            "error_debug": result.as_ref().err().map(|e| format!("{:?}", e)),
            "elapsed_ns": started.elapsed().as_nanos() as u64
        }));
    }
    result
}
