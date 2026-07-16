# Lifecycle hooks

Lifecycle hooks let application code react to the points in a job's life — when it starts, finishes, retries, parks on an external callback, and so on — without putting that logic inside the job handler itself. Typical uses are metrics, alerting, and job-type-specific cleanup on permanent failure.

Hooks are registered on the `ClientBuilder` and keyed by job kind. They work for both the typed (`register::<T, _>()`) and raw (`register_worker(...)`) worker paths.

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

`on_event::<T>()` gives you typed `args`; `on_event_kind("send_email", ...)` gives an untyped handler keyed by the kind string. Multiple hooks may be registered for the same kind; they stack and run sequentially.

## Events

| Event | Fires when |
| --- | --- |
| `Started` | the claim transaction commits, just before the handler runs (`job.state == running`) |
| `WaitingForCallback` | the handler returned `WaitForCallback` and the job parked (`job.state == waiting_external`; `job.callback_id` is set) |
| `Completed` | the job finished successfully |
| `Retried` | the attempt failed (or `RetryAfter`) and the job will run again |
| `Exhausted` | the job exhausted its retries, or failed terminally, and moved to `failed` |
| `Cancelled` | the job was cancelled |
| `Rescued` | maintenance rescued the job (expired callback, stale heartbeat, or exceeded deadline) — `reason: RescueReason` carries which |

How `JobResult` / outcomes map to events:

| Outcome | Event |
| --- | --- |
| claim commits | `Started` |
| `Ok(Completed)` | `Completed` |
| `Ok(RetryAfter)` / retryable `Err` (retries left) | `Retried` |
| retries exhausted (final attempt returned `RetryAfter` or a retryable `Err`), or `Err(Terminal)` | `Exhausted` |
| `Ok(Cancel)` | `Cancelled` |
| `Ok(Snooze)` | none — an internal reschedule, not an outcome |
| `Ok(WaitForCallback)` | `WaitingForCallback` |

## Callbacks

A job that returns `WaitForCallback` parks in `waiting_external` and emits `WaitingForCallback`. The terminal event fires later, when the callback is resolved — **but only if you resolve it through the worker `Client`**, which owns the hook registry:

```rust
// In your callback endpoint (same process as the worker Client):
let outcome = client.resolve_callback(callback_id, payload, default_action, None).await?;
// or the lower-level forms:
client.complete_external(callback_id, payload, None).await?; // -> Completed
client.fail_external(callback_id, error, None).await?;       // -> Exhausted
client.retry_external(callback_id, None).await?;             // -> Retried
```

Resolving through the bare `awa_model::admin::*` functions (or the CLI, or a process that has no worker `Client`) still transitions the job correctly but fires no hook — hooks are an in-process, worker-side facility. Dispatch happens once, in the process that performs the resolution, mirroring how an inline outcome fires once from the worker that claimed the job.

For a callback-driven `Completed`, `duration` is `Duration::ZERO` (no handler ran during the parked phase).

A callback that _resumes_ the job to `running` emits no event itself; the job re-enters execution and emits `Started` plus a terminal event as usual.

## Semantics

Hooks are **best-effort, in-process notifications** — not a durable workflow mechanism:

- They observe **committed** state. An outcome hook carries the post-transition `JobRow`; a stale/rejected finalization emits no outcome event (though a `Started` may already have fired for that attempt).
- They do not block executor progress or queue capacity, and `shutdown()` does not wait for them. They can be lost on process crash or shutdown.
- **Dispatch is not atomic with the state transition.** The hook runs _after_ the job's transition commits, as a separate in-process step — it is not a transactional outbox. If the process dies between the commit and the dispatch, the job state is durable but the event is lost. The same applies to callback resolution: `complete_external` / `resolve_callback` commit the outcome first, then dispatch.
- `Started` is dispatched detached, so a very fast job may finish before its `Started` hook completes — do not rely on strict started-before-outcome ordering for short jobs.
- A panicking hook is logged; later hooks for the same kind still run.

If a side effect must fire exactly once or survive a crash, do not drive it from a hook at all — the dispatch may never run, and enqueueing the follow-up work _from_ the hook just inherits that same unreliability. See the next section for the durable mechanism.

## Durable follow-up jobs (`on_*_enqueue`)

For side effects that **must survive a process crash** — sending a welcome email after signup, kicking off downstream work, persisting an audit row — use the transactional follow-up API instead of an `on_event` hook. For worker-driven outcomes (the trigger handler returned `Ok`/`Err`) and for callback resolution via the worker `Client` — `complete_external`, `fail_external`, `retry_external`, and `resolve_callback` — the follow-up `INSERT`s in the **same database transaction** as the triggering state transition: either both commit or both roll back, with no in-between. Maintenance rescue dispatches follow-ups in a **separate transaction** (best-effort) — see the [atomicity table below](#atomicity-guarantees) before designing a workflow that depends on rescue-driven follow-ups landing.

```rust
use awa::{Client, EnqueueRequest, QueueConfig};

let client = Client::builder(pool)
    .queue("signup", QueueConfig::default())
    .register::<Signup, _, _>(|_, _| async move { Ok(awa::JobResult::Completed) })
    // When a Signup completes, enqueue a WelcomeEmail in the *same* tx.
    // Closure receives the trigger args and the post-completion JobRow.
    .on_completed_enqueue::<Signup, WelcomeEmail, _>(|args, job| WelcomeEmail {
        user_id: args.user_id,
        signup_job_id: job.id,
    })
    .build()?;
```

There is one builder per outcome, plus a `_with` variant whose closure returns `EnqueueRequest<F>` so you can override `InsertOpts` on the follow-up (queue, priority, max_attempts, metadata, tags, unique, run_at, deadline_duration, ordering_key):

| Builder | Fires when | Closure signature |
| --- | --- | --- |
| `on_completed_enqueue` | `JobResult::Completed` | `(args, &job)` |
| `on_cancelled_enqueue` | `JobResult::Cancel(reason)` | `(args, &job, &reason)` |
| `on_retried_enqueue` | retryable err (retries left) or `RetryAfter` | `(args, &job, &error, attempt, next_run_at)` |
| `on_exhausted_enqueue` | retries exhausted, or `Err(Terminal)` | `(args, &job, &error, attempt)` |
| `on_waiting_for_callback_enqueue` | `JobResult::WaitForCallback` | `(args, &job)` |
| `on_rescued_enqueue` | maintenance rescue (expired callback / stale heartbeat / deadline) | `(args, &job, RescueReason)` |

`on_*_enqueue_with` returns `EnqueueRequest<F>`:

```rust
.on_completed_enqueue_with::<Signup, WelcomeEmail, _>(|args, job| {
    EnqueueRequest::new(WelcomeEmail { user_id: args.user_id })
        .queue("email")
        .priority(1)
})
```

### Atomicity guarantees

| Emission site | Atomic with state commit? |
| --- | --- |
| Worker-driven outcomes (handler returns `Ok`/`Err`) on either storage engine | **yes** — one transaction, one commit |
| Callback resolution on `Client` (`complete_external`, `fail_external`, `retry_external`, `resolve_callback`) on either storage engine | **yes** — one transaction, one commit. A spec INSERT failure (or a panic in the user-supplied closure) rolls the callback transition back together with the follow-up; `Client::*_external` returns `Err` and the external sender can retry. |
| Maintenance rescue (stale heartbeat / deadline / expired callback) | **no — best-effort.** The rescue commits first, then specs dispatch in a separate tx. A spec failure leaves the rescue applied and is logged. |

The trigger transition and the follow-up `INSERT` either both commit or neither does — exactly-once _enqueue_ per committed worker outcome or callback resolution. The follow-up itself, once enqueued, is delivered with Awa's usual at-least-once semantics, so the follow-up handler still needs to be safe under re-execution.

Rescue stays best-effort by design: making it atomic would couple rescue liveness to the correctness of user follow-up code (a panic in `on_rescued_enqueue` would roll back a stale-heartbeat rescue and leave the job stuck in `running`). Zero-loss rescue notifications belong in an outbox/sweeper, not inlined into the rescue tx.

### When to choose which API

- **Observation** (metrics, traces, alerts) → `on_event` — cheap, in-process, acceptable to drop a sample on a crash.
- **Side effect that must survive a crash** (send email, persist record, kick off downstream work) → `on_*_enqueue` — slightly more expensive (it's an Awa job) but durable, retried, DLQ'd, and visible in admin.

If you can't tell which you want, use `on_*_enqueue`. The cost of an unnecessary follow-up is bounded; the cost of a lost reliable side effect is not.

See [ADR-015](adr/015-post-commit-lifecycle-hooks.md) for hook rationale, and [ADR-029](adr/029-transactional-followup-jobs.md) for the follow-up enqueue design and atomicity matrix.
