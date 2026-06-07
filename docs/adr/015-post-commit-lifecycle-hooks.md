# ADR-015: Builder-Side Lifecycle Hooks

## Status

Accepted

## Context

An earlier attempt added `Worker::on_exhausted()` as a trait method, then backed it out after review because it was the wrong abstraction:

- It was inaccessible from the typed `register::<T, F>()` path
- It coupled job execution with follow-up side effects
- It encouraged growing the `Worker` trait for every new lifecycle need

Awa still needed a way for applications to react to job execution starts and committed job outcomes:

- Job-type-specific cleanup on permanent failure
- Cross-cutting metrics or alerting for a specific kind
- A hook model that works for both typed and raw worker registration
- Outcome semantics that do not fire for stale completions or uncommitted outcomes

The solution also had to fit Awa's existing execution model. Finalization is guarded by `run_lease` (ADR-013), and queue capacity must not be held open by slow side-effect handlers.

## Decision

Add builder-side lifecycle hooks keyed by job kind:

- `ClientBuilder::on_event::<T>()` for typed handlers with deserialized args
- `ClientBuilder::on_event_kind()` for untyped handlers keyed by kind string
- Multiple handlers may be registered for the same kind; they stack

Supported events are:

- `Started`
- `Completed`
- `Retried`
- `Exhausted`
- `Cancelled`

> Later extended with `WaitingForCallback` and callback-resolution events — see [Amendment: callback lifecycle events](#amendment-callback-lifecycle-events).

No lifecycle outcome event is emitted for:

- `Snooze`, because it is an internal reschedule rather than a meaningful outcome for observers
- `WaitForCallback`, because the job has only parked; the meaningful outcome happens later when the callback completes, retries, fails, or is cancelled (superseded by the amendment, which emits `WaitingForCallback` at park and a terminal event at resolution)

### Emission Semantics

`Started` dispatch is scheduled after the claim transaction commits and before the worker handler is invoked. The event carries the claimed `JobRow`, so `job.state` reflects `running`.

Outcome hooks run only after the corresponding database state transition commits successfully. The event carries the updated `JobRow`, so `job.state` reflects the post-transition state.

If guarded finalization rejects the outcome as stale, no lifecycle outcome event is emitted. A `Started` event may already have been emitted for the claimed attempt.

### Execution Semantics

Hooks are best-effort, in-process notifications:

- `Started` hooks run in detached tasks while the attempt is in flight; outcome hooks run in detached tasks after the in-flight permit is released
- Hooks do not block executor progress or queue capacity
- `shutdown()` does not wait for them
- Handlers for a kind run sequentially; panics are logged and later handlers still run
- `Started` dispatch is detached so a slow hook does not delay handler execution; very fast jobs may therefore complete before a started hook finishes, and observers should not rely on strict started-before-outcome delivery ordering for short jobs
- If a hook side effect must be durable or retried, that logic should enqueue another job instead

### API Shape

The final API is builder-wide rather than returning a per-registration sub-builder. Handlers are still logically scoped by job kind, but the builder API keeps registration order flexible and avoids introducing a temporary registration type solely to attach hooks.

Typed handlers deserialize args from the committed job snapshot immediately before dispatch. This keeps hook dispatch aligned with the post-commit storage snapshot instead of depending on executor-local typed state.

## Consequences

### Positive

- Works with both `register::<T, F>()` and `register_worker(...)`
- Keeps the `Worker` trait minimal
- Gives application code typed args for job-specific cleanup logic
- Ensures hooks observe committed state, not speculative handler outcomes
- Avoids stale-event emission by reusing the guarded finalization model
- Prevents slow hooks from blocking queue throughput or graceful drain

### Negative

- Hooks are not durable; they can be lost on process crash or shutdown
- Typed hooks pay a second arg deserialization step during dispatch
- There is no single global hook surface for all job kinds
- `Snooze` and `WaitForCallback` do not produce immediate notifications, which may surprise users expecting every `JobResult` variant to map to an event

## Alternatives Considered

### Trait methods on `Worker`

Rejected. The earlier `on_exhausted()` direction expands the `Worker` trait surface, does not help the typed registration path, and mixes execution with side-effect policy.

### Typed per-registration sub-builder

Considered, but not adopted. A separate registration builder adds API complexity without changing the underlying storage model, which is keyed by job kind on `ClientBuilder`.

### Global untyped hooks only

Rejected as the primary interface. Global untyped hooks are suitable for metrics or alerting, but most cleanup logic is job-type-specific and benefits from typed args.

### Middleware / layer system

Rejected as unnecessary abstraction for Awa's current scope. Lifecycle hooks cover the concrete post-commit use case without turning the worker runtime into an extensible middleware stack.

## Relationship to ADR-019

Hooks fire after the job's final state has been committed and its in-flight permit released, regardless of whether that commit targeted the canonical `awa.jobs_hot` row or a queue-storage `active_leases` deletion plus terminal append. The guarded-finalization contract that gates hook emission lives on `(job_id, run_lease)` and is unchanged under queue storage. See [ADR-019](019-queue-storage-redesign.md).

## Amendment: callback lifecycle events

The original decision left `WaitForCallback` without any event, on the reasoning that "the meaningful outcome happens later when the callback completes, retries, fails, or is cancelled." In practice nothing was emitted at that later point either: callback resolution runs in `awa_model::admin` (the storage layer, which has no handler registry and is often invoked from a different process than the worker), so a job that parked on a callback could fire `Started` and then go silent through to its terminal state — invisible to lifecycle hooks. This amendment closes that gap.

### `WaitingForCallback`

A new event, emitted by the executor when a handler returns `JobResult::WaitForCallback` and the `waiting_external` transition commits. It carries the parked `JobRow` (so `job.state` is `waiting_external`, and `job.callback_id` / `job.callback_timeout_at` identify the pending callback). This makes the park observable and, paired with the resolution event below, lets callers measure external-wait latency. The supported event set becomes:

- `Started`
- `WaitingForCallback`
- `Completed`
- `Retried`
- `Exhausted`
- `Cancelled`

`Snooze` remains event-free: it is an internal reschedule, not a job outcome.

### Resolution events

Callback resolution maps onto the existing terminal events rather than introducing callback-specific variants — an outcome is an outcome, and `WaitingForCallback` already marks that the job took the callback path:

| Resolution | Event |
| --- | --- |
| callback completes the job | `Completed` |
| callback fails the job (terminal) | `Exhausted` |
| callback requeues the job for retry | `Retried` |
| callback is cancelled | `Cancelled` |
| callback resumes the job to `running` | none — the job re-enters execution and the executor emits `Started` plus a terminal event as usual |

For a callback-driven `Completed`, the `duration` field is `Duration::ZERO`: no handler executed during this phase, and conflating it with the parked-wait time would overload the field's "handler execution time" meaning. If wait latency is needed later, it should be a dedicated field sourced from a recorded park timestamp, not this one.

### Where resolution events are dispatched

Resolution events are dispatched **in-process, at the resolving call site**, by worker-`Client` methods (`Client::resolve_callback`, `complete_external`, `fail_external`, `retry_external`). Each calls the corresponding `awa_model::admin` function and then, after the transition commits, dispatches the mapped event through the same guarded path as inline outcome hooks.

This keeps the storage layer free of the handler registry and preserves at-most-once dispatch: exactly one process performs a given resolution, so exactly one dispatch occurs — mirroring how an inline outcome fires once from the single worker that claimed the job. A broadcast/NOTIFY fan-out (as used for `awa:cancel`) was rejected here precisely because it would re-fire the hook in every process that holds handlers, breaking at-most-once for metrics.

The boundary is therefore: resolving a callback through the worker `Client` fires hooks; resolving it through the bare `awa_model::admin::*` functions, the CLI, or a process without the worker `Client` does not. This matches the existing contract that lifecycle hooks are a worker-side, best-effort, in-process facility.

### Superseded consequences

The original Negative bullet — "`Snooze` and `WaitForCallback` do not produce immediate notifications" — now applies only to `Snooze`. `WaitForCallback` produces a `WaitingForCallback` event at park and a terminal event at resolution (when resolved via the worker `Client`).
