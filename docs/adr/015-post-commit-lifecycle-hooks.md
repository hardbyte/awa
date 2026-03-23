# ADR-015: Builder-Side Post-Commit Lifecycle Hooks

## Status

Accepted

## Context

PR #59 added `Worker::on_exhausted()` as a trait method. PR #66 removed it
after review because it was the wrong abstraction:

- It was inaccessible from the typed `register::<T, F>()` path
- It coupled job execution with follow-up side effects
- It encouraged growing the `Worker` trait for every new lifecycle need

Awa still needed a way for applications to react to committed job outcomes:

- Job-type-specific cleanup on permanent failure
- Cross-cutting metrics or alerting for a specific kind
- A hook model that works for both typed and raw worker registration
- Semantics that do not fire for stale completions or uncommitted outcomes

The solution also had to fit Awa's existing execution model. Finalization is
guarded by `run_lease` (ADR-013), and queue capacity must not be held open by
slow side-effect handlers.

## Decision

Add builder-side lifecycle hooks keyed by job kind:

- `ClientBuilder::on_event::<T>()` for typed handlers with deserialized args
- `ClientBuilder::on_event_kind()` for untyped handlers keyed by kind string
- Multiple handlers may be registered for the same kind; they stack

Supported events are:

- `Completed`
- `Retried`
- `Exhausted`
- `Cancelled`

No lifecycle event is emitted for:

- `Snooze`, because it is an internal reschedule rather than a meaningful
  outcome for observers
- `WaitForCallback`, because the job has only parked; the meaningful outcome
  happens later when the callback completes, retries, fails, or is cancelled

### Emission Semantics

Hooks run only after the corresponding database state transition commits
successfully. The event carries the updated `JobRow`, so `job.state` reflects
the post-transition state.

If guarded finalization rejects the outcome as stale, no lifecycle event is
emitted.

### Execution Semantics

Hooks are best-effort, in-process notifications:

- They run in detached tasks after the in-flight permit is released
- They do not block executor progress or queue capacity
- `shutdown()` does not wait for them
- Handlers for a kind run sequentially; panics are logged and later handlers
  still run
- If a hook side effect must be durable or retried, that logic should enqueue
  another job instead

### API Shape

The final API is builder-wide rather than returning a per-registration
sub-builder. Handlers are still logically scoped by job kind, but the builder
API keeps registration order flexible and avoids introducing a temporary
registration type solely to attach hooks.

Typed handlers deserialize args from the committed job row immediately before
dispatch. This keeps hook dispatch aligned with the post-commit row snapshot
instead of depending on executor-local typed state.

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
- `Snooze` and `WaitForCallback` do not produce immediate notifications, which
  may surprise users expecting every `JobResult` variant to map to an event

## Alternatives Considered

### Trait methods on `Worker`

Rejected. This was the `on_exhausted()` direction from PR #59. It expands the
`Worker` trait surface, does not help the typed registration path, and mixes
execution with side-effect policy.

### Typed per-registration sub-builder

Considered in issue #61, but not adopted. A separate registration builder adds
API complexity without changing the underlying storage model, which is keyed by
job kind on `ClientBuilder`.

### Global untyped hooks only

Rejected as the primary interface. Global untyped hooks are suitable for
metrics or alerting, but most cleanup logic is job-type-specific and benefits
from typed args.

### Middleware / layer system

Rejected as unnecessary abstraction for Awa's current scope. Lifecycle hooks
cover the concrete post-commit use case without turning the worker runtime into
an extensible middleware stack.
