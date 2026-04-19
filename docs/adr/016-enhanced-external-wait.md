# ADR-016: Sequential Callbacks and Callback Heartbeats

## Status

Accepted

## Context

Awa's `waiting_external` state supports single-shot callback completion: a handler registers a callback, parks the job, and an external system resolves it. This covers webhooks, payment confirmations, and approval gates well.

Production integrations revealed three recurring patterns the current design doesn't support cleanly:

1. **Sequential callbacks** — a job that calls API A, waits for confirmation, then calls API B and waits again. Today this requires completing the first job and enqueuing a second, losing continuity.

2. **Callback heartbeats** — long-running external processes (document generation, ML training, manual review queues) need to signal "still working" without completing. Today the callback timeout fires and the job gets rescued.

3. **Timeout visibility** — when a callback times out, the lifecycle hook system can react (via `Retried` events), but the external system has no way to know the callback expired.

## Decision

Enhance the single-callback model with two backward-compatible additions:

### 1. Resume-from-wait (sequential callbacks)

`complete_external` gains an optional `resume: bool` flag (default `false`). When `true`, instead of transitioning to `completed`, the job transitions back to `running` and the handler resumes execution with the callback payload.

**Rust API:**
```rust
// Handler registers first callback
let token = ctx.register_callback(Duration::from_secs(3600)).await?;
send_to_payment_provider(token.id());

// Wait for first callback — handler suspends here
let payment_result = ctx.wait_for_callback(token).await?;

// Process the result, then register second callback
let shipping_token = ctx.register_callback(Duration::from_secs(7200)).await?;
notify_shipping_provider(shipping_token.id(), &payment_result);

// Wait for second callback
let shipping_result = ctx.wait_for_callback(shipping_token).await?;
finalize_order(&payment_result, &shipping_result);

Ok(JobResult::Completed)
```

**Python API:**
```python
@client.task(ProcessOrder, queue="orders")
async def handle(job):
    # First wait: payment
    token = await job.register_callback(timeout_seconds=3600)
    send_to_payment_provider(token.id)
    payment = await job.wait_for_callback(token)

    # Second wait: shipping
    token2 = await job.register_callback(timeout_seconds=7200)
    notify_shipping(token2.id, payment)
    shipping = await job.wait_for_callback(token2)

    finalize(payment, shipping)
```

**Implementation:** `wait_for_callback(token)` is a new method on `JobContext` / `Job` that:
1. Transitions the job to `waiting_external` (same as today's `WaitForCallback` return)
2. Suspends the handler's async task and polls the job row until the callback resolves
3. `resume_external(callback_id, payload)` transitions the job back to `running` and stores the payload in `metadata._awa_callback_result`
4. The handler consumes that payload and resumes execution

The wait is token-specific: the handler only waits on the exact callback ID it registered, and stale tokens are rejected once a newer callback is registered.

`resume_external` also accepts the job while it is still `running` to handle the early-callback race where the external system responds before the handler finishes its transition into `waiting_external`.

The job stays in `running` → `waiting_external` → `running` → `waiting_external` → ... → `completed` without being re-dispatched. The existing heartbeat keeps the job alive between waits. The run_lease stays valid throughout.

**State transitions:**
```
running → waiting_external (register_callback + wait)
waiting_external → running (complete_external with resume=true)
running → waiting_external (second register_callback + wait)
waiting_external → completed (complete_external with resume=false, or handler returns Completed)
```

### 2. Callback heartbeats

New `heartbeat_callback(callback_id)` function that resets `callback_timeout_at` without completing the job. External systems call this periodically for long-running operations.

**API:**
```python
# External system (e.g., ML training service)
while training_in_progress:
    await client.heartbeat_callback(callback_id)
    time.sleep(60)

# When done:
await client.complete_external(callback_id, payload={"model_url": "s3://..."})
```

**Rust:**
```rust
awa::admin::heartbeat_callback(&pool, callback_id).await?;
```

**Implementation:** Single SQL update:
```sql
UPDATE awa.jobs
SET callback_timeout_at = now() + make_interval(secs => $2)
WHERE callback_id = $1 AND state = 'waiting_external'
```

No schema changes — reuses existing `callback_timeout_at` column.

## Consequences

### Positive

- Users get sequential callbacks and heartbeat support with almost zero new learning curve.
- Maintains Awa's core differentiators (Postgres-only, Rust runtime, formal verification, simplicity).
- Documentation and examples become even stronger selling points ("you don't need Temporal for 90% of cases").
- Easy to implement and ship in v0.5 alongside existing work.

### Negative / Risks

- Some advanced users may still outgrow Awa and migrate to Temporal/Restate for 20+-step workflows — that's expected and healthy.
- We must clearly document the boundary ("if you need fan-in, child workflows, or sagas → use a coordinator job or Temporal").

### Migration / Compatibility

- 100% backward compatible. Existing single-callback jobs continue to work unchanged.
- New APIs are opt-in.

## What we explicitly don't build

- **Fan-in** (wait for N signals) — model as separate jobs with a coordinator. If demand materialises, address it alongside a future job-dependency design rather than extending the callback primitive itself.
- **Named callback channels** — over-engineering for current use cases.
- **Full resumable execution / durable promises** — that's Temporal territory. Awa is a job queue, not a workflow engine.
- **Automatic timeout compensation** — the lifecycle hook system already handles this. `Retried` events from callback timeout rescue can trigger cleanup logic.

## Implementation Notes

1. `heartbeat_callback` — new SQL function, admin endpoint, Python/Rust API method. No schema changes.
2. `wait_for_callback` on JobContext/Job — transitions to `waiting_external`, then polls the job row until `resume_external` stores the callback payload and returns the job to `running`.
3. Update TLA+ models for the new `waiting_external → running` resume transition.
4. CLI/UI: add visibility for "waiting for callback #N" in the job detail view.
5. Documented example patterns in `/docs/callbacks.md`.
6. Benchmarks: ensure resume loop doesn't regress hot-path performance.

## Prior Art

| System | Pattern | Awa equivalent |
|--------|---------|----------------|
| Temporal signals | Multiple `waitForSignal()` in sequence | `wait_for_callback()` in a loop |
| Restate awakeables | `ctx.awakeable().promise` | `job.wait_for_callback(token)` |
| Step Functions task tokens | `.waitForTaskToken` with heartbeat | `register_callback` + `heartbeat_callback` |
| Inngest wait-for-event | `step.waitForEvent()` | `wait_for_callback()` per step |
