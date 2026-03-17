//! Phase 0 spike: Prove PyO3 async interop works for Awa's Python worker model.
//!
//! Four things to prove:
//! 1. Rust tokio can call a Python `async def` and await its result
//! 2. A Rust background task can heartbeat while a Python handler runs
//! 3. Python exceptions propagate to Rust as structured errors
//! 4. `ctx.is_cancelled()` works from Python when Rust signals shutdown

use pyo3::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Simulates heartbeat state — tracks how many heartbeats occurred
/// while a Python handler was running.
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
struct HeartbeatCounter {
    inner: Arc<AtomicU64>,
}

#[pymethods]
impl HeartbeatCounter {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(AtomicU64::new(0)),
        }
    }

    fn count(&self) -> u64 {
        self.inner.load(Ordering::SeqCst)
    }
}

/// Cancellation context passed to Python handlers.
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
struct JobContext {
    cancelled: Arc<AtomicBool>,
}

#[pymethods]
impl JobContext {
    #[new]
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if the job has been cancelled (e.g., shutdown or deadline).
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

/// Spike 1: Call a Python async def from Rust tokio and await the result.
#[pyfunction]
fn spike_call_async_handler<'py>(
    py: Python<'py>,
    handler: Py<PyAny>,
    arg: String,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        // Call handler(arg) → coroutine, then convert to Rust future, then await
        let future = Python::attach(|py| {
            let coro = handler.call1(py, (arg,))?;
            pyo3_async_runtimes::tokio::into_future(coro.into_bound(py))
        })?;
        let result = future.await?;
        let value: String = Python::attach(|py| result.extract::<String>(py))?;
        Ok(value)
    })
}

/// Spike 2: Run a background heartbeat task while a Python handler executes.
/// Returns the heartbeat count after the handler completes.
#[pyfunction]
fn spike_heartbeat_during_handler<'py>(
    py: Python<'py>,
    handler: Py<PyAny>,
    counter: HeartbeatCounter,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let heartbeat_counter = counter.inner.clone();

        // Spawn a background heartbeat task (simulates DB heartbeat writes)
        let heartbeat_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                        heartbeat_counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        // Call the Python async handler
        let future = Python::attach(|py| {
            let coro = handler.call0(py)?;
            pyo3_async_runtimes::tokio::into_future(coro.into_bound(py))
        })?;
        let _result = future.await?;

        // Stop heartbeat
        cancel.cancel();
        let _ = heartbeat_handle.await;

        let final_count = counter.inner.load(Ordering::SeqCst);
        Ok(final_count)
    })
}

/// Spike 3: Call a Python async handler that raises an exception.
/// Returns (error_type, error_message, traceback) tuple.
#[pyfunction]
fn spike_exception_propagation<'py>(
    py: Python<'py>,
    handler: Py<PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let future = Python::attach(|py| {
            let coro = handler.call0(py)?;
            pyo3_async_runtimes::tokio::into_future(coro.into_bound(py))
        })?;
        let result = future.await;

        match result {
            Ok(_) => Ok((
                "no_error".to_string(),
                "handler succeeded unexpectedly".to_string(),
                String::new(),
            )),
            Err(err) => Python::attach(|py| {
                let error_type = err
                    .get_type(py)
                    .qualname()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "Unknown".to_string());
                let error_message = err.value(py).to_string();
                let traceback = err
                    .traceback(py)
                    .map(|tb| tb.format().unwrap_or_default())
                    .unwrap_or_default();
                Ok((error_type, error_message, traceback))
            }),
        }
    })
}

/// Spike 4: Pass a JobContext to Python, signal cancellation from Rust
/// while the Python handler is running, and verify is_cancelled() returns True.
#[pyfunction]
fn spike_cancellation<'py>(
    py: Python<'py>,
    handler: Py<PyAny>,
    ctx: JobContext,
) -> PyResult<Bound<'py, PyAny>> {
    let ctx_cancelled = ctx.cancelled.clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        // Signal cancellation after 100ms from a background task
        let cancel_flag = ctx_cancelled.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            cancel_flag.store(true, Ordering::SeqCst);
        });

        // Create a new context sharing the same flag for the handler
        let handler_ctx = JobContext {
            cancelled: ctx_cancelled,
        };

        // Call the Python handler(ctx) → coroutine → future → await
        let future = Python::attach(|py| {
            let coro = handler.call1(py, (handler_ctx,))?;
            pyo3_async_runtimes::tokio::into_future(coro.into_bound(py))
        })?;
        let result = future.await?;

        let was_cancelled: bool = Python::attach(|py| result.extract::<bool>(py))?;
        Ok(was_cancelled)
    })
}

#[pymodule]
fn pyo3_async_spike(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<HeartbeatCounter>()?;
    m.add_class::<JobContext>()?;
    m.add_function(wrap_pyfunction!(spike_call_async_handler, m)?)?;
    m.add_function(wrap_pyfunction!(spike_heartbeat_during_handler, m)?)?;
    m.add_function(wrap_pyfunction!(spike_exception_propagation, m)?)?;
    m.add_function(wrap_pyfunction!(spike_cancellation, m)?)?;
    Ok(())
}
