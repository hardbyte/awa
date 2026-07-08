# ADR-039: End-to-end trace propagation

## Status

Accepted — implemented for 0.7 ([#110](https://github.com/hardbyte/awa/issues/110)).

## Context

awa emits OpenTelemetry spans on both sides of the queue, but they were
component-local: the producer's `insert_with` span and the worker's
`job.execute` span belonged to different traces, so a request that enqueued a
job and the execution of that job could not be viewed as one flow. The 0.7
roadmap (decision D8) committed to storing serialized trace context on the
job at enqueue and reconnecting it at execution, gated on enqueue overhead
staying under 2% (evidence gate E8).

Two engines complicate storage: canonical mutates job rows in place while
queue storage carries an append-only payload envelope, and SQL-native
producers (ADR-032 bridge adapters, `insert_job_compat`) enqueue without any
Rust in the path.

## Decision

**Carry the context in job metadata under a reserved key, not a schema
column.** At enqueue, `prepare_raw_job_insert` — the single choke point every
Rust producer path funnels through (typed, raw, batch, COPY, transactional,
Python) — serializes the ambient span context as a W3C `traceparent` string
into `metadata["awa:traceparent"]` when a valid context exists and the key is
absent. Metadata keys prefixed `awa:` are reserved (documented in
`docs/stability.md`). This needs no migration, rides both engines
identically, survives the 0.8 canonical removal unchanged, and lets SQL
producers participate by setting the key themselves. A caller-supplied value
always wins over ambient capture, which also keeps the pre-0.7 opt-in
pattern working.

**Restore follows OTel messaging semantics.** At execution the worker parses
the stored context and shapes the `job.execute` span by attempt:

- **First attempt: remote parent.** The enqueue→execute flow reads as one
  connected trace — what users of request-triggered jobs expect.
- **Retries: fresh root trace with a span *link*** back to the enqueue
  site, per OTel messaging guidance for deferred processing. A job retried
  hours later must not stretch the original trace across its backoff
  schedule, inherit a stale sampling decision, or nest under the
  dispatcher's per-poll claim span (retries explicitly detach from that
  ambient context). Cron fires carry no producer context and start fresh
  traces naturally.

Both sides emit [messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/):
producer spans are `SpanKind::Producer` named `send {queue}` (batch inserts:
`send`), execution spans are `SpanKind::Consumer` with `messaging.system =
"awa"`, `messaging.destination.name`, `messaging.operation.type`, and
`messaging.message.id`. The execution span keeps its established
`job.execute {kind}` name — kind is the operationally useful dimension for a
job queue and the name predates this ADR — with the semconv shape carried by
kind and attributes.

Capture is **default-on** with a process-wide kill switch
(`AWA_TRACE_CAPTURE=off`): measured cost is ~0.23µs per enqueue, +0.023% of
a 1ms enqueue roundtrip (E8 gate, rerunnable via the ignored
`enqueue_overhead_e8_gate` test). Sampling flags propagate in the
`traceparent`, so parent-based samplers on the worker respect the producer's
decision on first attempts and re-decide at the root for retries.

Handlers inspect the stored enqueue-site context via `ctx.traceparent()`
(Rust) and `job.traceparent` (Python). Rust handlers propagating onward
(outgoing HTTP headers) use the ambient `trace::current_traceparent()`
instead, so downstream spans nest under the execution span rather than
becoming its siblings.

## Consequences

**Positive**

- One connected trace from request to job execution, on both engines, with
  zero migrations and zero SQL-contract changes.
- Retries and their links match collector/vendor expectations for
  messaging systems; trace UIs with link support navigate back to the
  producing trace.
- The reserved-key contract gives Python producers and SQL producers the
  same participation path (set the key; done).

**Negative / limits**

- The traceparent is visible in job metadata (UI, handlers). Treated as a
  feature — it aids debugging — but it is user-visible surface.
- Python *producers* are not captured automatically: the Rust core cannot
  see opentelemetry-python's ambient context across the FFI boundary.
  Documented pattern: set `metadata["awa:traceparent"]` at enqueue.
  Automatic capture at the awa-python binding layer is the remaining #110
  follow-up.
- `tracestate` is not carried (only `traceparent`); vendor-specific state
  is dropped at the queue boundary.
- Far-future scheduled first attempts still parent to the enqueue trace,
  stretching it across the schedule delay. Acceptable for 0.7; a
  delay-based cutover to link mode is a possible refinement.

## Alternatives considered

- **Dedicated `trace_context` column** — a migration on canonical plus an
  envelope change on queue storage, an asymmetric pair the metadata key
  avoids; SQL producers would also need contract changes (#342 tension).
- **Always link, never parent** — spec-pure but breaks the headline use
  case: request-triggered jobs appearing in the request's trace. Rejected
  as the default; the attempt-1 parent covers the common case.
- **Always parent, including retries** — inflates trace duration across
  backoff schedules and pins every attempt to the producer's sampling
  decision. Rejected per messaging guidance.

## Relationship to other ADRs

Builds on ADR-004 (Rust core owns the execution path — one capture point
covers Python); composes with ADR-032 bridge adapters via the documented
reserved key; the reserved `awa:` metadata prefix is a normative addition to
the ADR-036 stability policy. Pairs with the attempt-timeline trace link-out
([#375](https://github.com/hardbyte/awa/issues/375)).
