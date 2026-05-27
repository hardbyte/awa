# Lifecycle hooks

Lifecycle hooks let application code react to the points in a job's life —
when it starts, finishes, retries, parks on an external callback, and so on —
without putting that logic inside the job handler itself. Typical uses are
metrics, alerting, and job-type-specific cleanup on permanent failure.

Hooks are registered on the `ClientBuilder` and keyed by job kind. They work
for both the typed (`register::<T, _>()`) and raw (`register_worker(...)`)
worker paths.

## Registering a hook

```rust
use awa::{Client, JobEvent, QueueConfig};

let client = Client::builder(pool)
    .queue("email", QueueConfig::default())
    .register::<SendEmail, _, _>(|args, _ctx| async move { /* ... */ Ok(awa::JobResult::Completed) })
    // Typed hook: args are deserialized from the committed job snapshot.
    .on_event::<SendEmail, _, _>(|event| async move {
        match event {
            JobEvent::Started { job, .. }    => metrics::started(&job.queue),
            JobEvent::Completed { duration, .. } => metrics::completed(duration),
            JobEvent::Exhausted { error, .. } => alert::job_failed(&error),
            _ => {}
        }
    })
    .build()?;
```

`on_event::<T>()` gives you typed `args`; `on_event_kind("send_email", ...)`
gives an untyped handler keyed by the kind string. Multiple hooks may be
registered for the same kind; they stack and run sequentially.

## Events

| Event | Fires when |
|---|---|
| `Started` | the claim transaction commits, just before the handler runs (`job.state == running`) |
| `WaitingForCallback` | the handler returned `WaitForCallback` and the job parked (`job.state == waiting_external`; `job.callback_id` is set) |
| `Completed` | the job finished successfully |
| `Retried` | the attempt failed (or `RetryAfter`) and the job will run again |
| `Exhausted` | the job exhausted its retries, or failed terminally, and moved to `failed` |
| `Cancelled` | the job was cancelled |

How `JobResult` / outcomes map to events:

| Outcome | Event |
|---|---|
| claim commits | `Started` |
| `Ok(Completed)` | `Completed` |
| `Ok(RetryAfter)` / retryable `Err` (retries left) | `Retried` |
| retries exhausted, or `Err(Terminal)` | `Exhausted` |
| `Ok(Cancel)` | `Cancelled` |
| `Ok(Snooze)` | none — an internal reschedule, not an outcome |
| `Ok(WaitForCallback)` | `WaitingForCallback` |

## Callbacks

A job that returns `WaitForCallback` parks in `waiting_external` and emits
`WaitingForCallback`. The terminal event fires later, when the callback is
resolved — **but only if you resolve it through the worker `Client`**, which
owns the hook registry:

```rust
// In your callback endpoint (same process as the worker Client):
let outcome = client.resolve_callback(callback_id, payload, default_action, None).await?;
// or the lower-level forms:
client.complete_external(callback_id, payload, None).await?; // -> Completed
client.fail_external(callback_id, error, None).await?;       // -> Exhausted
client.retry_external(callback_id, None).await?;             // -> Retried
```

Resolving through the bare `awa_model::admin::*` functions (or the CLI, or a
process that has no worker `Client`) still transitions the job correctly but
fires no hook — hooks are an in-process, worker-side facility. Dispatch happens
once, in the process that performs the resolution, mirroring how an inline
outcome fires once from the worker that claimed the job.

For a callback-driven `Completed`, `duration` is `Duration::ZERO` (no handler
ran during the parked phase).

A callback that *resumes* the job to `running` emits no event itself; the job
re-enters execution and emits `Started` plus a terminal event as usual.

## Semantics

Hooks are **best-effort, in-process notifications** — not a durable workflow
mechanism:

- They observe **committed** state. An outcome hook carries the post-transition
  `JobRow`; a stale/rejected finalization emits no outcome event (though a
  `Started` may already have fired for that attempt).
- They do not block executor progress or queue capacity, and `shutdown()` does
  not wait for them. They can be lost on process crash or shutdown.
- `Started` is dispatched detached, so a very fast job may finish before its
  `Started` hook completes — do not rely on strict started-before-outcome
  ordering for short jobs.
- A panicking hook is logged; later hooks for the same kind still run.

If a side effect must be durable or retried, enqueue another job from the hook
instead of relying on the hook itself.

See [ADR-015](adr/015-post-commit-lifecycle-hooks.md) for the design rationale.
