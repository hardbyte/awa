---
name: awa-jobs
description: Author, enqueue, and handle jobs with the Awa Postgres-native job queue in Rust or Python. Use when defining a job kind, registering a worker or handler, configuring queues, enqueuing transactionally, choosing retry versus snooze, scheduling cron jobs, wiring in-process or webhook callbacks, or reporting job progress.
license: MIT
compatibility: Requires the awa Rust crate or the awa-pg Python wheel. Written for the awa 0.7 API and semantics; install the skill version matching your awa release (pin to the release tag) because field availability and defaults change across minor versions.
---

# Awa Jobs

Use the awa version the project depends on and match its documentation. Awa is
Postgres-only: a job is a typed payload stored in Postgres and run by a worker
that claims it. Defaults across both languages: queue `"default"`, priority `2`
(1 = highest … 4 = lowest), `max_attempts` 25.

## Define A Job Kind

Every job kind has a stable string name and a serializable payload. Choose the
name deliberately — it is a durable identifier stored on every row.

**Rust** — implement `JobArgs`, or derive it. The derived `kind()` is the
snake_case of the struct name; override with `#[awa(kind = "...")]`. The `awa`
facade re-exports both the trait and the derive macro, so depending on `awa`
alone is enough — no separate `awa-model` dependency is needed. (If you only
enqueue and never run a worker, depend on `awa-model` directly instead.)

```rust
use awa::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail { to: String, subject: String } // kind() == "send_email"
```

snake_case derivation splits acronyms in non-obvious ways (`SMTPEmail` →
`smtp_email`, `HTMLToPDF` → `html_to_pdf`). If the exact kind string matters,
set it explicitly.

**Python** — define a `@dataclass` (or Pydantic model) and register a handler
with `@client.task`. The kind defaults to the snake_cased class name.

```python
@dataclass
class SendEmail:
    to: str
    subject: str

@client.task(SendEmail, queue="email")
async def handle_email(job):
    ...  # job.args is a reconstructed SendEmail
```

`client.worker(...)` is the deprecated alias for `client.task(...)`; it emits a
`DeprecationWarning`.

Regardless of language, job args and metadata must not contain NUL bytes (`\u0000`) in string values
or keys — they are rejected before reaching Postgres JSONB.

## Run A Worker

**Rust** uses a builder; `start()` spawns background tasks and returns
immediately, `shutdown(timeout)` drains gracefully.

```rust
let client = Client::builder(pool.clone())
    .queue("email", QueueConfig { max_workers: 2, ..Default::default() })
    .register::<SendEmail, _, _>(|args, _ctx| async move {
        Ok(JobResult::Completed)
    })
    .build()?;
client.start().await?;
```

`QueueConfig` defaults worth knowing: `max_workers` 50, `poll_interval` 200ms,
`deadline_duration` 300s, `claimers` 1, `claim_batch_size` 512. The trait form
(`impl Worker` + `.register_worker(w)`) is equivalent to the typed closure.

**Python** passes a queue list to `start`:

```python
client = awa.AsyncClient(DATABASE_URL)
# handlers registered via @client.task above
await client.start([("email", 2)])        # (name, max_workers)
```

Concurrency has two modes. **Hard-reserved** (the default) gives each queue its
own fixed `max_workers`. **Weighted** shares one pool: call `.global_max_workers(N)`
(Rust) or pass `global_max_workers=N` with dict-form queues (Python), then use
`min_workers` as a floor and `weight` to split overflow. Per-queue rate limiting
is a token bucket (`RateLimit { max_rate, burst }` / `rate_limit=(rate, burst)`).
Enabling a rate limit live is not supported — you can only retune a queue that
was built with one.

## Enqueue

**Rust** — `insert(executor, &args)` or `insert_with(executor, &args, InsertOpts)`.
The executor is any `PgExecutor`, so pass `&pool` for a standalone enqueue or
`&mut *tx` to enqueue inside your own transaction — the job commits or rolls back
with your data. Batch inserts use `insert_many` / `insert_many_copy`.

```rust
awa::insert_with(&mut *tx, &SendEmail { to, subject },
    InsertOpts { queue: "email".into(), priority: 1, ..Default::default() }).await?;
```

**Python** — `await client.insert(args, queue=..., priority=..., run_at=..., ...)`.
For transactional enqueue:

```python
async with await client.transaction() as tx:
    await tx.insert(args)
```

Or use `awa.bridge.insert_job(session, args, ...)` to enqueue inside an existing
asyncpg / psycopg3 / SQLAlchemy / Django transaction.

`InsertOpts` / keyword options: `queue`, `priority`, `max_attempts`, `run_at`
(a future time starts the job `Scheduled` rather than `Available`),
`deadline_duration`, `metadata`, `tags`, `unique`, `ordering_key`. Dispatch
order is strict `priority ASC, run_at ASC, id ASC`. Priority aging then improves
a waiting job's effective priority one level per aging interval (default 60s) so
low-priority work cannot starve indefinitely.

**Unique jobs** deduplicate on a BLAKE3 key over kind + optionally queue + args +
a time-period bucket. Defaults: dedup by args, not by queue. Conflict handling
depends on the API: a single-row `insert` / `insert_with` and the row-by-row
`insert_many` **raise `AwaError::UniqueConflict`** (Python raises the
corresponding error) on a duplicate — you must handle it, do not assume a silent
no-op. Only the COPY bulk path (`insert_many_copy`) silently skips duplicates.
Cancel without storing the id via `cancel_by_unique_key(kind, queue=, args=,
period_bucket=)` — the components must match those used at insert.
Args-based uniqueness relies on canonical (sorted-key) JSON, pinned by
cross-language golden tests; the default state set includes `completed`, so a
still-retained completed job suppresses a re-insert with the same args. Scope
uniqueness with `by_period`, or drop `completed` from the state set, if that is
not what you want.

## Retry, Snooze, Cancel

A handler's return value decides the outcome:

- **Completed** — Rust `Ok(JobResult::Completed)`; Python return `None`.
- **Retry after a delay** — `JobResult::RetryAfter(dur)` / `awa.RetryAfter(secs)`.
  This **consumes an attempt** and exhausts into the DLQ on the final one.
- **Snooze** — `JobResult::Snooze(dur)` / `awa.Snooze(secs)`. Reschedules
  **without** consuming an attempt and fires no lifecycle event.
- **Cancel** — `JobResult::Cancel(reason)` / `awa.Cancel(reason)`. Terminal, no
  DLQ, no failure event.

An uncaught error is retryable by default. Make it terminal with
`JobError::terminal(msg)` (Rust) or by raising a subclass of `awa.TerminalError`
(Python). Automatic backoff is `min(2^attempt seconds + up to 25% jitter, 24h)`.

**The key gotcha:** use `Snooze` — not `RetryAfter` — for "not ready yet" polling
or rate-limit waits, so `max_attempts` bounds genuine failures only.

## Cron / Periodic Jobs

Declare the schedule in code. It is UPSERT-synced to the database and evaluated
by the single elected maintenance leader; enqueue is atomic, so leader failover
cannot double-fire an occurrence.

```rust
let job = PeriodicJob::builder("nightly-report", "0 0 * * *")
    .timezone("Pacific/Auckland")
    .queue("reports")
    .build(&ReportArgs { region: "nz".into() })?;
// .periodic(job) on the client builder
```

```python
client.periodic("nightly-report", "0 0 * * *", ReportArgs, ReportArgs(...),
                timezone="Pacific/Auckland", queue="reports")
```

Timezone is an IANA name (default UTC), validated eagerly. `missed_fire_policy`
is `coalesce` (default — enqueue only the latest missed fire after a delay) or
`catch_up` (enqueue each missed fire in order). Pause, resume, and trigger are
admin operations (the web UI and admin API, and the Python client), not part of
the code-defined builder; a paused schedule leaves `last_enqueued_at` untouched,
so `missed_fire_policy` governs catch-up on resume. Cron pause and queue pause
are independent. To purge in-flight cron work, pause the schedule, then
bulk-cancel jobs filtered by `metadata.cron_name`.

## Callbacks

**In-process callbacks** park a job on an external event and resume the same
handler when it arrives.

- `ctx.register_callback(timeout)` (Rust) / `job.register_callback(timeout_seconds=)`
  (Python) writes the callback id to the database. Register it **before** handing
  the id to the external system, or the completion can race ahead of registration.
- For a long wait, return `JobResult::WaitForCallback(guard)` /
  `awa.WaitForCallback(token)` — this releases the worker permit while parked.
- `ctx.wait_for_callback(guard).await` suspends the handler in place but **holds
  the worker permit** while polling; use it only for short waits.
- Resolve from a process holding the worker client so hooks fire:
  `resolve_callback(id, payload, default_action)`, or `complete_external` /
  `fail_external` / `retry_external` / `resume_external`. Resolving through the
  bare admin/CLI path transitions the job correctly but fires no hooks.

**Webhook callbacks** (Rust `http-worker` feature) dispatch a job to a serverless
function over HTTP and park it for an async callback. The receiver exposes
`POST {prefix}/{callback_id}/{complete,fail,heartbeat}`. The `X-Awa-Signature`
header is a BLAKE3 keyed hash of the callback-id string (not RFC HMAC, not over
the body); the secret is 32 bytes / 64 hex chars. Function responses map 2xx →
park, 5xx → retryable, 4xx → terminal. The HTTP `complete` endpoint completes the
job; it does not resume an in-process `wait_for_callback` handler.

**CEL filtering** (feature `cel`) gates and reshapes callback payloads via
`CallbackConfig` expressions (`filter`, `on_fail`, `on_complete`, `transform`),
with `DefaultAction` when nothing matches. Only the `payload` variable is
available; any other variable is rejected at registration rather than failing
open at resolve time.

## Progress And Lifecycle Hooks

Report progress from inside a handler: `set_progress(percent, message)` and
`update_metadata(obj)` buffer in memory; `flush_progress().await` writes durably
(only while the job is `running` with a matching run lease). Progress metadata
survives retries and snoozes, so read it back at the start of a handler to resume
from a checkpoint. Flush periodically, not every iteration.

Rust exposes two hook families on the client builder:

- **Observation hooks** (`.on_event` / `.on_event_kind`) fire best-effort in
  process for `Started`, `Completed`, `Retried`, `Exhausted`, `Cancelled`,
  `Rescued`, `WaitingForCallback`. They are **not** a durable workflow — they can
  be lost on crash and `shutdown()` does not wait for them.
- **Durable follow-up hooks** (`on_completed_enqueue`, `on_exhausted_enqueue`,
  and the other `on_*_enqueue`) INSERT a follow-up job for side effects that must
  survive a crash. Atomicity depends on what triggered them:
  - Worker-driven outcomes (a handler returning `Ok`/`Err`) and callback
    resolution on the `Client` (`complete_external`, `resolve_callback`, …) commit
    the follow-up in the **same transaction** as the transition — exactly-once per
    committed outcome.
  - **`on_rescued_enqueue` is the exception: it is best-effort, not atomic.**
    Maintenance rescue (stale-heartbeat / deadline / expired-callback) commits the
    rescue first, then dispatches the follow-up in a separate transaction; a
    failure there leaves the rescue applied and is only logged. Do not build a
    rescue-notification workflow that assumes the follow-up cannot be lost — use an
    outbox/sweeper if you need zero-loss rescue signals.

  Every follow-up, once enqueued, is delivered at-least-once, so its handler must
  be safe to re-run.

Enqueues capture the current trace span into reserved metadata
(`awa:traceparent`) automatically; disable with `AWA_TRACE_CAPTURE=off`. To
propagate a trace onward from inside a handler, use the current span
(`awa_model::trace::current_traceparent()`), not the stored enqueue-site value.

## Version-Aware Notes

- The canonical (row-mutating) storage engine is deprecated in 0.7 and removed in
  0.8; queue-storage is the default. Coordinate storage transitions and upgrades
  with the operator — see the `awa-operations` skill.
- Partitioned FIFO (`enqueue_shards`, `ordering_key`) landed in 0.6; `ordering_key`
  is ignored while a queue's shard count is 1.
- Read the CHANGELOG for the exact version that introduced any field before using
  it against an older deployment.
