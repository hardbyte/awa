//! Join Python-facing native work before interpreter finalization.
//!
//! `future_into_py` schedules the asyncio result before its blocking callback
//! releases all Python objects. An awaited result is therefore not a native
//! join. CPython 3.12 can terminate that thread during GIL reacquisition and
//! unwind Rust destructors without a thread state (the captured wheel SIGSEGV).
//!
//! Track async bodies as well as completion callbacks: argument conversion and
//! result construction can acquire the GIL before completion. At atexit, fence
//! new work, cancel async tasks, and join all accepted work with the GIL released.
//! This does not own worker shutdown or the shared Tokio runtime. Applications
//! must still shut down workers and close pools.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::generic::{self, ContextExt, Runtime};
use pyo3_async_runtimes::TaskLocals;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Condvar, Mutex};
use std::task::{Context, Poll};

#[derive(Default)]
struct State {
    closing: bool,
    callbacks: usize,
    next_task: u64,
    tasks: BTreeMap<u64, tokio::task::AbortHandle>,
}

static STATE: Mutex<State> = Mutex::new(State {
    closing: false,
    callbacks: 0,
    next_task: 0,
    tasks: BTreeMap::new(),
});
static IDLE: Condvar = Condvar::new();

struct Completion;

impl Completion {
    fn enter() -> Option<Self> {
        let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
        if state.closing {
            return None;
        }
        state.callbacks += 1;
        Some(Self)
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
        state.callbacks -= 1;
        if state.callbacks == 0 {
            IDLE.notify_all();
        }
    }
}

struct BridgeRuntime;

struct TaskCompletion(u64);

impl Drop for TaskCompletion {
    fn drop(&mut self) {
        let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
        state.tasks.remove(&self.0);
        IDLE.notify_all();
    }
}

// Fields drop in declaration order, including cancellation and panic paths.
// Release the future's Python handles before reporting that the task is joined.
struct TrackedFuture<F> {
    future: Pin<Box<F>>,
    _completion: TaskCompletion,
}

impl<F: Future> Future for TrackedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

tokio::task_local! {
    static LOCALS: TaskLocals;
}

impl Runtime for BridgeRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle = tokio::task::JoinHandle<()>;

    fn spawn<F>(future: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
        if state.closing {
            drop(state);
            drop(future);
            return pyo3_async_runtimes::tokio::get_runtime().spawn(async {});
        }
        let id = state.next_task;
        state.next_task += 1;
        // Hold the lock through registration so a fast task cannot remove its
        // entry before it is inserted, or escape the shutdown snapshot.
        let task = pyo3_async_runtimes::tokio::get_runtime().spawn(TrackedFuture {
            future: Box::pin(future),
            _completion: TaskCompletion(id),
        });
        state.tasks.insert(id, task.abort_handle());
        task
    }

    fn spawn_blocking<F>(callback: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let Some(completion) = Completion::enter() else {
            // A Rust future may finish after shutdown starts. Dropping its
            // owned Py handles detached is supported by PyO3; do not attach to
            // a finalizing interpreter just to report a result nobody can use.
            drop(callback);
            return Self::spawn(async {});
        };
        pyo3_async_runtimes::tokio::get_runtime().spawn_blocking(move || {
            let _completion = completion;
            callback();
        })
    }
}

impl ContextExt for BridgeRuntime {
    fn scope<F, R>(locals: TaskLocals, future: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        Box::pin(LOCALS.scope(locals, future))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        LOCALS.try_with(Clone::clone).ok()
    }
}

pub(crate) fn future_into_py<F, T>(py: Python<'_>, future: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    if STATE.lock().unwrap_or_else(|e| e.into_inner()).closing {
        return Err(PyRuntimeError::new_err("Awa async bridge is shutting down"));
    }
    let locals = get_current_locals(py)?;
    generic::future_into_py_with_locals::<BridgeRuntime, _, _>(py, locals, future)
}

pub(crate) fn get_current_locals(py: Python<'_>) -> PyResult<TaskLocals> {
    match BridgeRuntime::get_task_locals() {
        Some(locals) => Ok(locals),
        None => pyo3_async_runtimes::tokio::get_current_locals(py),
    }
}

#[pyfunction]
pub(crate) fn _shutdown_async_bridge(py: Python<'_>) {
    py.detach(|| {
        let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
        state.closing = true;
        let tasks: Vec<_> = state.tasks.values().cloned().collect();
        drop(state);
        for task in tasks {
            task.abort();
        }
        let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
        while state.callbacks != 0 || !state.tasks.is_empty() {
            state = IDLE.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    });
}

pub(crate) fn register_shutdown(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let shutdown = wrap_pyfunction!(_shutdown_async_bridge, m)?;
    m.py()
        .import("atexit")?
        .call_method1("register", (&shutdown,))?;
    m.add_function(shutdown)?;
    Ok(())
}
