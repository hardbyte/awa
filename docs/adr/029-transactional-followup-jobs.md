# ADR-029: Transactional follow-up jobs for durable lifecycle effects

## Status

Proposed

## Context

Awa exposes two ways for application code to react to a job's lifecycle:

- **Builder-side lifecycle hooks** (ADR-015). Closures registered on
  `ClientBuilder::on_event` fire after the state commit, in-process,
  best-effort. They are explicitly not durable: a process crash between the
  state commit and the hook dispatch loses the event. ADR-015 states the
  guidance: *"If a hook side effect must be durable or retried, that logic
  should enqueue another job instead."* That guidance lives in prose, not in
  the API.
- **Tracing spans and structured logs** (PRD §15). Useful for observability,
  but not a delivery mechanism for side effects.

Two adjacent design tracks make the gap acute:

- **Callback ingress as a separate surface** (ADR-027, proposed). A
  callback-only receiver has no `Client` and no in-process hook registry, so
  resolving a callback there fires no hook. ADR-027 punts: *"If callback
  completion should produce lifecycle notifications beyond the storage
  transition itself, that mechanism needs to be durable or otherwise
  runtime-independent."*
- **Maintenance-only runtime role** (ADR-028, proposed). Rescue paths
  (expired callback, stale heartbeat, exceeded deadline) commit state
  transitions from the maintenance worker, with no notion of a per-kind hook
  registry. Today an attempt that times out emits a `Started` and then goes
  silent; the rescue itself produces no lifecycle event.

Users routinely ask for "reliable events" for things like "send a welcome
email when this signup completes." They reach for hooks — the only event
surface that exists — and ship code that quietly drops events on a deploy or
when the resolving process changes. The naming and ergonomics of the current
hooks API encourage this mistake.

Two distinct needs are being conflated:

| Need | Examples | Acceptable loss | Latency budget |
|---|---|---|---|
| Observation | metrics, traces, logs, alerting | low single-digit % | sub-second |
| Side effects | send email, kick off workflow, persist record | zero | seconds to minutes |

A single mechanism cannot honestly serve both.

## Decision

Treat observation and side effects as separate mechanisms with separate APIs.
Keep ADR-015's in-process hooks unchanged for observation. Add a first-class
**transactional follow-up enqueue** API for durable side effects.

### Principle

A side effect that must survive process crash, deployment, or a split
deployment topology (per ADR-027) is delivered as an Awa job that is
**enqueued in the same database transaction** as the triggering state
transition. The follow-up job inherits Awa's existing durability properties:
at-least-once delivery, retries, dead-letter (ADR-020), DLQ replay,
observability through the admin UI, and crash recovery via run-lease guarded
finalization (ADR-013).

### Builder API

`ClientBuilder` gains transactional enqueue counterparts to `on_event`. The
naming mirrors the existing hook surface so the choice between observation
and side effect is explicit at the registration site:

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

The closure returns a follow-up `JobArgs` value (or an `Option<...>` /
`InsertParams` for conditional / option-bearing follow-ups; final shape is an
implementation detail of the follow-up issue). The engine inserts the
follow-up job in the transaction that commits the trigger's state change. If
the transaction rolls back, the follow-up is not enqueued; if it commits,
the follow-up is durably visible.

Counterparts cover the same outcomes as the typed `JobEvent` variants:
`on_started_enqueue`, `on_completed_enqueue`, `on_retried_enqueue`,
`on_exhausted_enqueue`, `on_cancelled_enqueue`,
`on_waiting_for_callback_enqueue`. `Snooze` continues to emit nothing.

### Where the enqueue runs

The enqueue is performed by whatever process performs the state transition:

| Transition | Performed by | Follow-up enqueued by |
|---|---|---|
| Claim / inline outcomes (Started, Completed, Retried, Exhausted, Cancelled) | Worker executor | Worker executor, in the finalization transaction |
| Callback resolution via worker `Client` | The resolving process | That process, in the resolution transaction |
| Callback resolution via callback-only ingress (ADR-027) | Callback receiver | Callback receiver, in the resolution transaction |
| Expired-callback / stale-heartbeat / deadline rescue (ADR-028) | Maintenance runtime | Maintenance runtime, in the rescue transaction |

Every emitter shares one rule: the follow-up `INSERT` is in the same
transaction as the state `UPDATE`. The follow-up is then claimed by an
ordinary worker; the original transition does not need to wait for it.

### Registry lives in process — for now

The mapping from "trigger kind + outcome" to "make follow-up args" is a Rust
closure (or a Python callable, when the Python API gains parity). Closures
cannot be portably stored in the database, so each process that performs a
state transition must have the spec registered locally:

- A worker process: registers via its own `ClientBuilder`.
- A callback-only ingress process (ADR-027): registers via the callback
  router's config (planned in #279).
- A maintenance-only process (ADR-028): registers via its own
  `ClientBuilder` in the library form, or via a future config file for the
  pool-only CLI form.

In normal deployments every process is built from the same code, so the
registries agree. When they disagree — a worker on a new build registers a
new follow-up that an older callback-receiver doesn't know about — the
emitter that *doesn't* know the spec simply doesn't enqueue; the trigger
still commits. This is the same drift behavior as in-process hooks today and
is acceptable for v1.

A future ADR may introduce a database-stored registry (mapping trigger →
follow-up kind, with the args transformation expressed declaratively — for
example, JSONata-style mapping or a CEL expression like ADR-021's callback
filters use). That option is deferred until the in-process surface is in
use and the missing mapping in another process becomes a real operational
problem.

### NOTIFY is a wakeup hint, not the delivery channel

Postgres `LISTEN` / `NOTIFY` is suitable for low-latency wakeup but not for
delivery: payloads are capped, undelivered notifications are dropped, and
a process not currently listening misses the event. The follow-up enqueue
already provides durability; pairing it with a `pg_notify` of the follow-up
job's queue (in the same transaction) is a useful latency optimisation —
listening workers can claim immediately rather than waiting for the next
poll interval — but the notification carries no payload and no event
semantics. It is purely a wakeup signal layered on top of a durable
INSERT.

### Boundary with hooks

The two APIs are deliberately separate and orthogonal:

- A user may register `on_event` and `on_*_enqueue` for the same trigger.
  Both fire; the hook is observation, the enqueue is delivery.
- An `on_event_enqueue` registration on a process that does *not* perform
  the relevant state transition is dormant; the engine has nothing to insert
  for that process. This is consistent with the registry being per-process.
- Cancellation, snooze, and rescue produce no hooks today; only the enqueue
  path covers them. This is intentional — rescues happen in the maintenance
  process, which has no user handler registry, but can still insert a
  follow-up job from its transaction.

`docs/lifecycle-hooks.md` describes the hook path. The follow-up enqueue
path gets its own section that names the trade-off explicitly: pick the API
whose guarantees match the side effect.

### What this is not

- Not a generic event bus. The mechanism is one Awa job per follow-up
  registration per transition. Fan-out across application components happens
  by registering multiple follow-ups, each its own job.
- Not a replacement for tracing spans or logs (PRD §15). Those continue to
  describe execution; this describes deliberate downstream work.
- Not a CDC stream. WAL-level change capture is out of scope (alternative
  E below).
- Not a sub-millisecond reactor. Follow-up latency is bounded by claim
  cadence (helped, but not eliminated, by the NOTIFY-as-wakeup hint).

## Consequences

### Positive

- A single durable mechanism covers every state transition Awa makes,
  across every deployment role ADR-027 and ADR-028 introduce: inline
  outcomes, callback resolution (worker-`Client` or callback-only), rescue,
  and any future transition.
- The rescue / timeout event gap closes naturally: a rescue transaction can
  enqueue `job_failed_rescue` (or any user-defined follow-up) atomically
  with the rescue UPDATE.
- The "what should be in a hook?" mistake disappears at the API level —
  observation and delivery have different names and obviously different
  guarantees.
- No new event log table, no new subscriber protocol, no separate retention
  policy. The follow-up is a job, with all of Awa's existing operator and
  developer affordances.
- Composes with the bridge adapters (ADR-016, ADR-017): a user resolving a
  callback from their own application can pass their own transaction to
  the resolution path, and the follow-up enqueues in *their* transaction
  with the app rows.

### Negative / Risks

- The cost of a "durable event" equals the cost of an Awa job: a row in
  storage, a claim cycle, a worker pass. That is correctly more expensive
  than a hook. Users who reach for the durable path for what is really
  observation will pay that cost without benefit; docs and naming have to
  make the cheaper option obvious for metrics-style use cases.
- Latency for downstream work is bounded by Awa's poll cadence rather than
  by an in-process callback. The NOTIFY-as-wakeup hint reduces but does not
  eliminate this.
- More API surface to teach. The split into two APIs is the design's whole
  point, but the difference between `on_event` and `on_*_enqueue` must be
  unmistakable in docs and examples; otherwise users will pick the wrong
  one and observe one of the two failure modes (lost reliable side effect
  or expensive metrics counter).
- Cross-process registry drift is possible. The in-process registry choice
  trades portability for simplicity; future work may have to harden it
  with a database-stored spec if real deployments hit drift.
- The maintenance runtime now has a way to insert user-defined jobs from
  its rescue transactions. That is a deliberate broadening of what
  maintenance writes and must be documented alongside ADR-028.

## Alternatives considered

### A. Stay with in-process hooks only

Status quo. Loses on every cross-process and crash scenario above. The
prose-only guidance from ADR-015 does not survive contact with real
deployments where the resolver process is different from the worker
process. Rejected as the long-term answer.

### B. Postgres NOTIFY / LISTEN fan-out as the event channel

Tempting because `awa:cancel` is precedent. NOTIFY is at-most-once delivery
to *currently-connected* listeners, with an ~8 KB payload cap, and dropped
notifications are not replayed. It is a viable wakeup primitive but cannot
provide durability on its own. Using NOTIFY as the substrate would
re-introduce the same losses this ADR exists to fix; using it as a wakeup
hint on top of a durable enqueue is the right role.

### C. Dedicated `awa.events` outbox table

A standard transactional-outbox pattern. Considered seriously. The decision
weighed:

- A dedicated events table is durable and replayable and supports cursor-
  based subscribers.
- But Awa is *already* a durable, retry-capable, dead-letter-aware queue.
  Building an events log next to it duplicates the engine: a new subscriber
  protocol, cursor management, retention policy, admin UI surface, and
  test matrix.
- "Queryable event history" — the main argument for an events table — is
  addressable with tagged follow-up jobs and the existing completed/failed
  history (ADR-026).
- The follow-up-job pattern is composable: a follow-up may itself emit
  follow-ups, may fan out across kinds, and is observable in the existing
  UI.

Rejected because the engine for it already exists; building another would
trade familiarity for parallel infrastructure.

### D. (Chosen) Transactional follow-up jobs

Described above. Reuses Awa's existing primitives; needs only an API on
`ClientBuilder` and an enqueue hook in each state-transition path.

### E. Postgres logical decoding / WAL CDC

Powerful and durable but heavy infra (logical replication slot,
Debezium-class consumer), inconsistent support across managed Postgres
providers, and contrary to Awa's "self-contained queue in Postgres"
positioning (ADR-001). Out of scope.

## Relationship to other ADRs

- **ADR-015** (lifecycle hooks). This ADR codifies ADR-015's prose advice
  ("enqueue another job") into a first-class API. ADR-015's hooks remain
  the correct mechanism for observation; this ADR adds the mechanism for
  delivery.
- **ADR-027** (callback ingress, proposed). Resolves the open question
  ADR-027 punts on: durable callback notifications. A callback-only
  ingress process enqueues follow-ups in the resolution transaction; no
  in-process handler registry is required for delivery.
- **ADR-028** (maintenance-only runtime, proposed). Gives rescue paths a
  way to participate in lifecycle delivery without owning a handler
  registry, closing the rescue / timeout event gap.
- **ADR-013** (run-lease, guarded finalization). The follow-up enqueue
  joins the same transaction as the guarded-finalization UPDATE; if the
  UPDATE matches zero rows (stale outcome), the transaction rolls back and
  the follow-up is not enqueued, preserving the at-most-once finalization
  contract.
- **ADR-020** (DLQ). Follow-up jobs participate in the DLQ family
  unchanged; a side effect that exhausts retries lands in the DLQ with
  full args and history.
- **ADR-021** (sequential callbacks, callback heartbeats). The callback
  resolution surface remains as defined; this ADR adds a follow-up insert
  to its resolving transaction.
- **ADR-006** (insert-only transaction bridge) and **ADR-016/017**
  (Postgres adapter / Python transaction bridge). The same transactional
  insert primitive is reused; nothing new is added to the bridge contract.

## Open questions for follow-up issues

These are deferred and intentionally not decided here:

- Exact `ClientBuilder` method names and the closure return type
  (`Args`, `Option<Args>`, or `InsertParams` for finer control of queue /
  scheduling / unique opts on the follow-up).
- Whether follow-up specs may be conditional on the trigger's args / row
  state in v1, or whether all conditioning must live in the follow-up
  handler.
- Python parity: how the same surface is expressed in the Python API
  (likely via a decorator mirroring `@on_event` with an `enqueue=` form).
- Whether the in-DB spec registry (deferred above) should be sketched in
  a separate ADR before any production deployment is split across
  versions.
- The exact NOTIFY-as-wakeup-hint integration with the follow-up's queue
  poll cadence — implementation-level, not architectural.
