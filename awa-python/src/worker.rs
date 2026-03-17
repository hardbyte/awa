//! Python worker dispatch runtime.

use crate::client::{PyCancel, PyRetryAfter, PySnooze, WorkerEntry};
use crate::job::{json_to_py, PyJob};
use awa_model::JobRow;
use pyo3::prelude::*;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Acquire the GIL on background threads.
///
/// SAFETY: We're called from a Python extension module — the interpreter
/// is always initialized. `attach_unchecked` bypasses the `Py_IsInitialized`
/// check which incorrectly fails on extension-module background threads.
fn with_gil<F, R>(f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    unsafe { Python::attach_unchecked(f) }
}

/// Run the worker dispatch loop.
pub async fn run_workers(
    pool: PgPool,
    workers: Arc<RwLock<HashMap<String, WorkerEntry>>>,
    queues: Vec<(String, u32)>,
    cancel: CancellationToken,
    poll_interval: Duration,
) -> PyResult<()> {
    info!(queues = queues.len(), "Python worker dispatch started");

    let semaphores: HashMap<String, Arc<tokio::sync::Semaphore>> = queues
        .iter()
        .map(|(q, max)| (q.clone(), Arc::new(tokio::sync::Semaphore::new(*max as usize))))
        .collect();

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let mut claimed_any = false;

        for (queue, _) in &queues {
            let sem = match semaphores.get(queue) {
                Some(s) => s.clone(),
                None => continue,
            };
            if sem.available_permits() == 0 {
                continue;
            }

            let batch = sem.available_permits().min(10) as i32;
            let jobs: Vec<JobRow> = match sqlx::query_as(
                r#"
                WITH c AS (
                    SELECT id FROM awa.jobs
                    WHERE state = 'available' AND queue = $1 AND run_at <= now()
                      AND NOT EXISTS (SELECT 1 FROM awa.queue_meta WHERE queue = $1 AND paused = TRUE)
                    ORDER BY priority ASC, run_at ASC, id ASC
                    LIMIT $2 FOR UPDATE SKIP LOCKED
                )
                UPDATE awa.jobs SET state = 'running', attempt = attempt + 1,
                    attempted_at = now(), heartbeat_at = now(),
                    deadline_at = now() + interval '5 minutes'
                FROM c WHERE awa.jobs.id = c.id RETURNING awa.jobs.*
                "#,
            )
            .bind(queue)
            .bind(batch)
            .fetch_all(&pool)
            .await
            {
                Ok(j) => j,
                Err(e) => { warn!(error = %e, "claim failed"); continue; }
            };

            if !jobs.is_empty() {
                claimed_any = true;
                debug!(queue, count = jobs.len(), "claimed");
            }

            for job in jobs {
                let permit = match sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let pool = pool.clone();
                let workers = workers.clone();
                tokio::spawn(async move {
                    if let Err(e) = dispatch(&pool, &workers, &job).await {
                        error!(job_id = job.id, error = %e, "dispatch error");
                    }
                    drop(permit);
                });
            }
        }

        if !claimed_any {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(poll_interval) => {},
            }
        }
    }

    info!("Python worker dispatch stopped");
    Ok(())
}

async fn dispatch(
    pool: &PgPool,
    workers: &Arc<RwLock<HashMap<String, WorkerEntry>>>,
    job: &JobRow,
) -> PyResult<()> {
    let (handler, args_type) = {
        let guard = workers.read().await;
        match guard.get(&job.kind) {
            Some(e) => with_gil(|py| (e.handler.clone_ref(py), e.args_type.clone_ref(py))),
            None => {
                let _ = sqlx::query("UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1")
                    .bind(job.id).execute(pool).await;
                return Ok(());
            }
        }
    };

    // Run Python handler on blocking thread (holds GIL)
    let job_data = job.clone();
    let result: Result<Py<PyAny>, PyErr> = tokio::task::spawn_blocking(move || {
        with_gil(|py| {
            let pj = PyJob::from(job_data);
            let args_json = json_to_py(py, &pj.args_json)?;

            let args_inst = if args_type.bind(py).hasattr("model_validate")? {
                args_type.call_method1(py, "model_validate", (args_json,))?
            } else if args_type.bind(py).hasattr("__dataclass_fields__")? {
                let kw = args_json.bind(py).cast::<pyo3::types::PyDict>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err("expected dict")
                })?;
                args_type.call(py, (), Some(kw))?
            } else {
                args_json
            };

            let d = pyo3::types::PyDict::new(py);
            d.set_item("args", args_inst)?;
            d.set_item("id", pj.id)?;
            d.set_item("kind", &pj.kind)?;
            d.set_item("queue", &pj.queue)?;
            d.set_item("attempt", pj.attempt)?;
            d.set_item("max_attempts", pj.max_attempts)?;

            let ns = py.import("types")?.getattr("SimpleNamespace")?.call((), Some(&d))?;
            let r = handler.call1(py, (ns,))?;

            let inspect = py.import("inspect")?;
            if inspect.call_method1("iscoroutine", (r.bind(py),))?.extract::<bool>()? {
                Ok(py.import("asyncio")?.call_method1("run", (r.bind(py),))?.unbind())
            } else {
                Ok(r)
            }
        })
    })
    .await
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    // Update DB based on result
    match result {
        Ok(val) => {
            let state = with_gil(|py| {
                let r = val.bind(py);
                if r.is_none() { "completed" }
                else if r.is_instance_of::<PyRetryAfter>() { "retry_after" }
                else if r.is_instance_of::<PySnooze>() { "snooze" }
                else if r.is_instance_of::<PyCancel>() { "cancel" }
                else { "completed" }
            });

            match state {
                "completed" => {
                    sqlx::query("UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1")
                        .bind(job.id).execute(pool).await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                }
                "retry_after" => {
                    let s: f64 = with_gil(|py| val.bind(py).getattr("seconds")?.extract())?;
                    sqlx::query("UPDATE awa.jobs SET state = 'retryable', run_at = now() + make_interval(secs => $2), finalized_at = now() WHERE id = $1")
                        .bind(job.id).bind(s).execute(pool).await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                }
                "snooze" => {
                    let s: f64 = with_gil(|py| val.bind(py).getattr("seconds")?.extract())?;
                    sqlx::query("UPDATE awa.jobs SET state = 'scheduled', run_at = now() + make_interval(secs => $2), attempt = attempt - 1 WHERE id = $1")
                        .bind(job.id).bind(s).execute(pool).await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                }
                "cancel" => {
                    sqlx::query("UPDATE awa.jobs SET state = 'cancelled', finalized_at = now() WHERE id = $1")
                        .bind(job.id).execute(pool).await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                }
                _ => {}
            }
        }
        Err(err) => {
            let msg = with_gil(|py| err.value(py).to_string());
            let is_terminal = with_gil(|py| {
                py.import("awa")
                    .and_then(|m| m.getattr("TerminalError"))
                    .map(|t| err.get_type(py).is_subclass(&t).unwrap_or(false))
                    .unwrap_or(false)
            });
            let ej = serde_json::json!({"error": msg, "attempt": job.attempt, "terminal": is_terminal});

            if is_terminal || job.attempt >= job.max_attempts {
                let _ = sqlx::query("UPDATE awa.jobs SET state = 'failed', finalized_at = now(), errors = errors || $2::jsonb WHERE id = $1")
                    .bind(job.id).bind(&ej).execute(pool).await;
            } else {
                let _ = sqlx::query("UPDATE awa.jobs SET state = 'retryable', run_at = now() + awa.backoff_duration($2, $3), finalized_at = now(), errors = errors || $4::jsonb WHERE id = $1")
                    .bind(job.id).bind(job.attempt).bind(job.max_attempts).bind(&ej).execute(pool).await;
            }
        }
    }

    Ok(())
}
