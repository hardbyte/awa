# ADR-029: Transactional follow-up jobs for durable lifecycle effects

## Status

Accepted. Worker-driven outcomes and callback resolution via the worker `Client` commit follow-ups atomically with the state transition; maintenance rescue dispatches follow-ups best-effort in a separate transaction (see [Atomicity matrix](#atomicity-matrix)).

## Context

Awa exposes two ways for application code to react to a job's lifecycle:

- **Builder-side lifecycle hooks** (ADR-015). Closures registered on `ClientBuilder::on_event` fire after the state commit, in-process, best-effort. They are explicitly not durable: a process crash between the state commit and the hook dispatch loses the event. ADR-015 states the guidance: _"If a hook side effect must be durable or retried, that logic should enqueue another job instead."_ That guidance lives in prose, not in the API.
- **Tracing spans and structured logs** (PRD §15). Useful for observability, but not a delivery mechanism for side effects.

Two adjacent design tracks make the gap acute:

- **Callback ingress as a separate surface** (ADR-027, proposed). A callback-only receiver has no `Client` and no in-process hook registry, so resolving a callback there fires no hook. ADR-027 punts: _"If callback completion should produce lifecycle notifications beyond the storage transition itself, that mechanism needs to be durable or otherwise runtime-independent."_
- **Maintenance-only runtime role** (ADR-028, proposed). Rescue paths (expired callback, stale heartbeat, exceeded deadline) commit state transitions from the maintenance worker, with no notion of a per-kind hook registry. Today an attempt that times out emits a `Started` and then goes silent; the rescue itself produces no lifecycle event.

Users routinely ask for "reliable events" for things like "send a welcome email when this signup completes." They reach for hooks — the only event surface that exists — and ship code that quietly drops events on a deploy or when the resolving process changes. The naming and ergonomics of the current hooks API encourage this mistake.

Two distinct needs are being conflated:

| Need | Examples | Acceptable loss | Latency budget |
| --- | --- | --- | --- |
| Observation | metrics, traces, logs, alerting | low single-digit % | sub-second |
| Side effects | send email, kick off workflow, persist record | zero | seconds to minutes |

A single mechanism cannot honestly serve both.

## Decision

Treat observation and side effects as separate mechanisms with separate APIs. Keep ADR-015's in-process hooks unchanged for observation. Add a first-class **transactional follow-up enqueue** API for durable side effects.

### Principle

A side effect that must survive process crash, deployment, or a split deployment topology (per ADR-027) is delivered as an Awa job. For worker-driven outcomes that follow-up `INSERT`s **in the same database transaction** as the triggering state transition — both commit together or both roll back. Callback resolution and maintenance rescue dispatch their follow-ups in a separate transaction (best-effort, see [Atomicity matrix](#atomicity-matrix)) because the resolution / rescue transition has already committed by the time the dispatcher sees it. Either way the follow-up inherits Awa's existing durability properties: at-least-once delivery, retries, dead-letter (ADR-020), DLQ replay, observability through the admin UI, and crash recovery via run-lease guarded finalization (ADR-013).

### Builder API

`ClientBuilder` gains transactional enqueue counterparts to `on_event`. The naming mirrors the existing hook surface so the choice between observation and side effect is explicit at the registration site:

```rust
Client::builder(pool)
    .register::<Signup, _, _>(handle_signup)
    // Observation — best-effort, in-process, no delivery guarantee.
    .on_event::<Signup, _, _>(|event| async move { metrics::record(event) })
    // Durable side effect — committed atomically with the Signup outcome.
    .on_completed_enqueue::<Signup, SendWelcomeEmail, _>(|signup_args, _job| {
        SendWelcomeEmail { user_id: signup_args.user_id }
    })
    .on_exhausted_enqueue::<Signup, NotifyOps, _>(|signup_args, job| {
        NotifyOps {
            user_id: signup_args.user_id,
            failure: job.errors.clone(),
        }
    });
```

The closure returns a follow-up `JobArgs` value (default `InsertOpts`) or an `EnqueueRequest<F>` with `InsertOpts` overrides. For worker-driven outcomes and callback resolution via the worker `Client`, the engine `INSERT`s the follow-up in the same transaction as the triggering state change; the follow-up commits with the trigger or rolls back with it (a spec INSERT failure or a panic in the user-supplied closure rolls the trigger transition back as well). For maintenance rescue the engine `INSERT`s the follow-up in a separate transaction opened _after_ the rescue commits (see [Atomicity matrix](#atomicity-matrix)). In either case, once the follow-up `INSERT` commits the row is durably visible and rides Awa's existing retry / DLQ machinery.

Counterparts cover the outcomes that benefit most from durable delivery: `on_completed_enqueue`, `on_retried_enqueue`, `on_exhausted_enqueue`, `on_cancelled_enqueue`, `on_waiting_for_callback_enqueue`, and `on_rescued_enqueue`. `Started` is intentionally excluded — claim-time enqueue would join the dispatcher's hot path and the use case for "job started" is observation, which `on_event` already covers. `Snooze` continues to emit nothing on either surface.

### Where the enqueue runs

The enqueue is performed by whatever process performs the state transition:

| Transition | Performed by | Follow-up enqueued by | Atomic? |
| --- | --- | --- | --- |
| Inline outcomes (Completed, Retried, Exhausted, Cancelled, WaitingForCallback) | Worker executor | Worker executor, in the finalization transaction | yes |
| Callback resolution via worker `Client` | The resolving process | That process, in the resolution transaction (via `admin::*_external_in_tx` / `store::*_external_in_tx`) | yes |
| Callback resolution via callback-only ingress (ADR-027) | Callback receiver | Pending: callback-only ingress does not yet own a worker registry. When that surface lands it can reuse the same `_in_tx` admin / store helpers used by the worker `Client`. | pending |
| Expired-callback / stale-heartbeat / deadline rescue (ADR-028) | Maintenance runtime | Maintenance runtime, in a separate transaction after the rescue commits | no — best-effort |

Why rescue stays best-effort: a panic in a user-supplied `on_rescued_enqueue` closure would otherwise roll the rescue UPDATE back, coupling rescue liveness (the maintenance loop's job recovery guarantee) to the correctness of user follow-up code. A zero-loss rescue-notification story is better served by an outbox/sweeper than by inlining user code into the rescue tx. The current best-effort behaviour means the trigger has already committed, so the worst case is a missing follow-up — not a phantom follow-up. See [Atomicity matrix](#atomicity-matrix) for the call sites involved.

### Registry lives in process — for now

The mapping from "trigger kind + outcome" to "make follow-up args" is a Rust closure (or a Python callable, when the Python API gains parity). Closures cannot be portably stored in the database, so each process that performs a state transition must have the spec registered locally:

- A worker process: registers via its own `ClientBuilder`.
- A callback-only ingress process (ADR-027): registers via the callback router's config (planned in #279).
- A maintenance-only process (ADR-028): registers via its own `ClientBuilder` in the library form, or via a future config file for the pool-only CLI form.

In normal deployments every process is built from the same code, so the registries agree. When they disagree — a worker on a new build registers a new follow-up that an older callback-receiver doesn't know about — the emitter that _doesn't_ know the spec simply doesn't enqueue; the trigger still commits. This is the same drift behavior as in-process hooks today and is acceptable for v1.

A future ADR may introduce a database-stored registry (mapping trigger → follow-up kind, with the args transformation expressed declaratively — for example, JSONata-style mapping or a CEL expression like ADR-021's callback filters use). That option is deferred until the in-process surface is in use and the missing mapping in another process becomes a real operational problem.

### NOTIFY is a wakeup hint, not the delivery channel

Postgres `LISTEN` / `NOTIFY` is suitable for low-latency wakeup but not for delivery: payloads are capped, undelivered notifications are dropped, and a process not currently listening misses the event. The follow-up enqueue already provides durability; pairing it with a `pg_notify` of the follow-up job's queue (in the same transaction) is a useful latency optimisation — listening workers can claim immediately rather than waiting for the next poll interval — but the notification carries no payload and no event semantics. It is purely a wakeup signal layered on top of a durable INSERT.

### Boundary with hooks

The two APIs are deliberately separate and orthogonal:

- A user may register `on_event` and `on_*_enqueue` for the same trigger. Both fire; the hook is observation, the enqueue is delivery.
- An `on_*_enqueue` registration on a process that does _not_ perform the relevant state transition is dormant; the engine has nothing to insert for that process. This is consistent with the registry being per-process.
- Snooze and rescue produce no hooks today; only the enqueue path covers them. This is intentional — rescues happen in the maintenance process, which has no user handler registry, but can still insert a follow-up job from its transaction. Cancellation has both: `Cancelled` is a hook event in ADR-015 and gains an `on_cancelled_enqueue` counterpart here.

`docs/lifecycle-hooks.md` describes the hook path. The follow-up enqueue path gets its own section that names the trade-off explicitly: pick the API whose guarantees match the side effect.

### What this is not

- Not a generic event bus. The mechanism is one Awa job per follow-up registration per transition. Fan-out across application components happens by registering multiple follow-ups, each its own job.
- Not a replacement for tracing spans or logs (PRD §15). Those continue to describe execution; this describes deliberate downstream work.
- Not a CDC stream. WAL-level change capture is out of scope (alternative E below).
- Not a sub-millisecond reactor. Follow-up latency is bounded by claim cadence (helped, but not eliminated, by the NOTIFY-as-wakeup hint).

## Consequences

### Positive

- A single durable mechanism covers every state transition Awa makes, across every deployment role ADR-027 and ADR-028 introduce: inline outcomes, callback resolution (worker-`Client` or callback-only), rescue, and any future transition.
- The rescue / timeout event gap closes: a rescue can dispatch `job_failed_rescue` (or any user-defined follow-up) — best-effort, in a separate transaction after the rescue commits, but the follow-up itself is then a durable Awa job with full retry / DLQ semantics. Closing this gap fully atomically is tracked as an open extension.
- The "what should be in a hook?" mistake disappears at the API level — observation and delivery have different names and obviously different guarantees.
- No new event log table, no new subscriber protocol, no separate retention policy. The follow-up is a job, with all of Awa's existing operator and developer affordances.
- Composes with the bridge adapters (ADR-016, ADR-017): a user resolving a callback from their own application can pass their own transaction to the resolution path, and the follow-up enqueues in _their_ transaction with the app rows.

### Negative / Risks

- The cost of a "durable event" equals the cost of an Awa job: a row in storage, a claim cycle, a worker pass. That is correctly more expensive than a hook. Users who reach for the durable path for what is really observation will pay that cost without benefit; docs and naming have to make the cheaper option obvious for metrics-style use cases.
- Latency for downstream work is bounded by Awa's poll cadence rather than by an in-process callback. The NOTIFY-as-wakeup hint reduces but does not eliminate this.
- More API surface to teach. The split into two APIs is the design's whole point, but the difference between `on_event` and `on_*_enqueue` must be unmistakable in docs and examples; otherwise users will pick the wrong one and observe one of the two failure modes (lost reliable side effect or expensive metrics counter).
- Cross-process registry drift is possible. The in-process registry choice trades portability for simplicity; future work may have to harden it with a database-stored spec if real deployments hit drift.
- The maintenance runtime now has a way to insert user-defined jobs from its rescue transactions. That is a deliberate broadening of what maintenance writes and must be documented alongside ADR-028.

## Alternatives considered

### A. Stay with in-process hooks only

Status quo. Loses on every cross-process and crash scenario above. The prose-only guidance from ADR-015 does not survive contact with real deployments where the resolver process is different from the worker process. Rejected as the long-term answer.

### B. Postgres NOTIFY / LISTEN fan-out as the event channel

Tempting because `awa:cancel` is precedent. NOTIFY is at-most-once delivery to _currently-connected_ listeners, with an ~8 KB payload cap, and dropped notifications are not replayed. It is a viable wakeup primitive but cannot provide durability on its own. Using NOTIFY as the substrate would re-introduce the same losses this ADR exists to fix; using it as a wakeup hint on top of a durable enqueue is the right role.

### C. Dedicated `awa.events` outbox table

A standard transactional-outbox pattern. Considered seriously. The decision weighed:

- A dedicated events table is durable and replayable and supports cursor- based subscribers.
- But Awa is _already_ a durable, retry-capable, dead-letter-aware queue. Building an events log next to it duplicates the engine: a new subscriber protocol, cursor management, retention policy, admin UI surface, and test matrix.
- "Queryable event history" — the main argument for an events table — is addressable with tagged follow-up jobs and the existing completed/failed history (ADR-026).
- The follow-up-job pattern is composable: a follow-up may itself emit follow-ups, may fan out across kinds, and is observable in the existing UI.

Rejected because the engine for it already exists; building another would trade familiarity for parallel infrastructure.

### D. (Chosen) Transactional follow-up jobs

Described above. Reuses Awa's existing primitives; needs only an API on `ClientBuilder` and an enqueue hook in each state-transition path.

### E. Postgres logical decoding / WAL CDC

Powerful and durable but heavy infra (logical replication slot, Debezium-class consumer), inconsistent support across managed Postgres providers, and contrary to Awa's "self-contained queue in Postgres" positioning (ADR-001). Out of scope.

## Relationship to other ADRs

- **ADR-015** (lifecycle hooks). This ADR codifies ADR-015's prose advice ("enqueue another job") into a first-class API. ADR-015's hooks remain the correct mechanism for observation; this ADR adds the mechanism for delivery.
- **ADR-027** (callback ingress, proposed). Addresses the open question ADR-027 punts on: durable callback notifications. The worker `Client` drives resolution + follow-up dispatch in a single transaction via `admin::*_external_in_tx`. A callback-only ingress process built per ADR-027 will need its own registry hookup to reuse the same `_in_tx` admin path; until that hookup lands, callback ingress outside the worker `Client` is a separate transition with no follow-up dispatch. See the "Open extensions" section below for the current implementation boundary.
- **ADR-028** (maintenance-only runtime, proposed). Gives rescue paths a way to dispatch durable lifecycle work without owning a handler registry. The dispatch is best-effort (separate transaction); the follow-up itself is a regular Awa job once enqueued.
- **ADR-013** (run-lease, guarded finalization). For worker-driven outcomes the follow-up enqueue joins the same transaction as the guarded-finalization UPDATE; if the UPDATE matches zero rows (stale outcome), the transaction rolls back and the follow-up is not enqueued, preserving the at-most-once finalization contract.
- **ADR-020** (DLQ). Follow-up jobs participate in the DLQ family unchanged; a side effect that exhausts retries lands in the DLQ with full args and history.
- **ADR-021** (sequential callbacks, callback heartbeats). The callback resolution surface is unchanged; this ADR adds a follow-up dispatch that runs in a separate transaction after the resolution commits.
- **ADR-006** (insert-only transaction bridge) and **ADR-016/017** (Postgres adapter / Python transaction bridge). The same transactional insert primitive is reused; nothing new is added to the bridge contract.

## Implementation

- **Outcome surface.** `Outcome::{Completed, Retried, Exhausted, Cancelled, WaitingForCallback, Rescued}`. `Started` is intentionally excluded — claim-time enqueue would join the dispatcher's hot path, and the durable-side-effect use case for "job started" is uncommon. Observation belongs to `on_event`. `on_started_enqueue` is not blocked by anything in this design; whether to add it is left as an open question rather than a v1 commitment.
- **Builder API.** Each outcome has a pair of builders: `on_<outcome>_enqueue` (closure returns `JobArgs`) and `on_<outcome>_enqueue_with` (closure returns `EnqueueRequest<F>` for `InsertOpts` overrides — queue, priority, max_attempts, metadata, tags, unique, run_at, deadline_duration, ordering_key).
- **Closure signatures** carry the per-outcome context: Completed/WaitingForCallback: `(args, &job_row)`; Cancelled: `(args, &job_row, &reason)`; Retried: `(args, &job_row, &error, attempt, next_run_at)`; Exhausted: `(args, &job_row, &error, attempt)`; Rescued: `(args, &job_row, RescueReason)`.
- **Rescued event variant.** Maintenance rescues fire `JobEvent::Rescued { args, job, reason: RescueReason }` alongside the follow-up spec dispatch, keeping rescue context distinct from Retried/Exhausted rather than overloading either.

### Atomicity matrix

| Emission site | Atomic with state commit? |
| --- | --- |
| Worker-driven outcomes on canonical storage | yes — `*_canonical_with_followups` helpers open one tx, run the state UPDATE (UPDATE...RETURNING on `awa.jobs_hot`; for `retryable` the DELETE+INSERT move into `awa.scheduled_jobs` is in a CTE), dispatch specs, commit |
| Worker-driven outcomes on queue storage | yes — `*_queue_storage_with_followups` helpers run inside `complete_runtime_batch_slow_in_tx` / `cancel_running_in_tx` / `fail_*_in_tx` / `retry_*_in_tx` / `enter_callback_wait_in_tx` |
| Callback resolution on `Client` (`complete_external`, `fail_external`, `retry_external`, `resolve_callback`) on either storage engine | yes — `Client::*_external` opens one tx, drives the transition via `admin::*_external_in_tx` / `store::*_external_in_tx`, dispatches specs via `dispatch_specs_in_tx`, commits. A spec failure rolls the resolution back so the external sender can retry. |
| Maintenance rescue (stale heartbeat, deadline exceeded, expired callback) | **no — best-effort.** Spec dispatch runs in a separate tx after the rescue commits. A spec failure leaves the rescue applied and is logged. Kept best-effort by design (see [Open extensions](#open-extensions)). |

Worker-driven outcomes inherit ADR-013's run-lease guard (zero rows matched → tx rollback → no follow-up); callback resolution inherits a matching guard from the `SELECT ... FOR UPDATE` lookup on `awa.jobs_hot` or `FOR UPDATE OF lease` on the queue-storage leases table. The trigger transition and the follow-up `INSERT` either both commit or neither does, so each committed worker-driven outcome or callback resolution enqueues each registered follow-up exactly once. _Delivery_ of the follow-up itself is at-least-once — once enqueued the row rides Awa's normal claim/retry semantics, and the follow-up handler must remain safe under re-execution. The maintenance rescue path remains best-effort because coupling rescue liveness to the correctness of user follow-up code (a panic in `on_rescued_enqueue`) would be a worse trade than a missed Rescued follow-up; the rescue UPDATE has already committed when the dispatch runs, so the worst case is a missing follow-up — not a phantom follow-up.

### Open extensions

- **Python parity.** The Python binding does not yet expose `on_*_enqueue` or `on_event`. The implementation path mirrors `register_worker` in `awa-python/src/worker.rs`: a `PythonFollowUp` type that wraps a Python callable, takes the GIL inside `dispatch_specs_in_tx`, serialises `JobRow + OutcomeContext` to Python, invokes the callable, and decodes the returned dict to (`JobArgs`, `InsertOpts`) before the underlying `insert_with` fires. ADR-015's hook surface (`@on_event`) needs the same scaffold first.
- **Atomic callback-resolution for callback-only ingress (ADR-027).** The worker `Client::*_external` paths now drive the resolution and spec dispatch in a single tx via `admin::*_external_in_tx` and the matching `store::*_external_in_tx` helpers. The callback-only ingress process landed by ADR-027 will need its own registry hookup to reuse the same `_in_tx` admin path; until then, callback ingress outside the worker `Client` remains a separate transition with no follow-up dispatch.
- **Atomic rescue + follow-up enqueue: deliberately not pursued.** Threading `&mut tx` through `maintenance::rescue_*` would couple rescue liveness to the correctness of user follow-up code — a panic in `on_rescued_enqueue` could roll back a rescue UPDATE and prevent maintenance from recovering stale jobs. A future zero-loss rescue-notification design should use an outbox/sweeper rather than inlining user code into the rescue tx.
- **`on_started_enqueue`.** Not blocked by the design; deliberately out of scope to keep the claim/dispatch hot path uncontended. The observation use case is already covered by `on_event`.
- **Conditional spec dispatch.** Skipping enqueue based on a predicate over the triggering row is not part of the API; conditioning belongs inside the follow-up handler instead.
- **In-DB spec registry.** Not part of the design; the in-process registry is sufficient while specs are declared in one place per deployment.
- **NOTIFY-as-wakeup-hint for follow-up queue poll cadence.** An implementation-level tuning lever rather than an architectural concern.
