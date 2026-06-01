# ADR-028: Maintenance-only runtime role

## Status

Proposed

## Context

Awa's normal runtime process does several jobs:

- claims and executes user work,
- runs in-process Rust or Python handlers,
- dispatches `HttpWorker` jobs,
- promotes scheduled and retryable jobs,
- rescues stale heartbeats, expired deadlines, and expired callbacks,
- rotates and prunes queue-storage structures,
- refreshes admin metadata and runtime snapshots.

That composition is sensible for a traditional worker deployment: if a worker
process is running, it can also own the maintenance leader duties.

Callback-only ingress changes the deployment shape. A team may want:

- a public callback receiver,
- private admin UI,
- HTTP workers that dispatch to serverless functions,
- application-owned API servers such as FastAPI or axum,
- no public `awa serve` process,
- maintenance duties running somewhere predictable.

The callback receiver cannot replace the worker runtime or maintenance
runtime. It only resolves external waits. If no runtime performs maintenance,
scheduled jobs may not promote, retryable jobs may not become available,
stale heartbeats may not rescue, expired callbacks may remain parked, and
storage hygiene may drift.

There are related but distinct efforts:

- isolating user-visible maintenance lanes from background hygiene,
- serverless-friendly bounded `tick()` execution,
- splitting callback ingress away from the UI.

This ADR defines a persistent maintenance-only role that composes with those
efforts without requiring it to claim user jobs.

## Decision

Add a maintenance-only runtime role. It runs Awa maintenance loops and leader
election but does not claim or execute ordinary jobs.

### Public shape

Expose the role through both library and CLI APIs.

Library:

```rust
Client::builder(pool)
    .maintenance_only()
    .build()?
    .run()
    .await?;
```

CLI:

```text
awa maintenance run
```

The library API is for applications that declare cron jobs, queue
descriptors, job-kind descriptors, or other code-owned runtime metadata. The
CLI command is for pool-only maintenance where no application-specific
handlers or declarations are needed.

### What maintenance-only runs

The maintenance-only role should run internal state maintenance that does not
require executing user handler code:

- scheduled and retryable promotion,
- stale heartbeat rescue,
- expired deadline rescue,
- expired callback rescue,
- queue-storage rotation and pruning,
- completed / failed / DLQ cleanup,
- admin metadata refresh,
- runtime liveness publication for the maintenance process itself.

When constructed from `ClientBuilder`, it may also run configured
declaration-owned work:

- cron scheduling for cron definitions registered on that builder,
- descriptor publication for queues and job kinds declared by that builder.

The pool-only CLI form must not invent application declarations. It should not
publish descriptors or schedule cron jobs unless those definitions are
provided through a future explicit config format.

### What maintenance-only does not run

Maintenance-only mode must not:

- claim available jobs for execution,
- invoke Rust or Python user handlers,
- dispatch `HttpWorker` function calls,
- expose HTTP callback or admin routes by itself,
- hide the fact that a separate dispatcher is still required for normal job
  execution.

For HTTP-worker deployments, a dispatcher process is still needed to claim jobs
and POST to the remote function URL. Maintenance-only keeps time-based and
hygiene guarantees moving; it is not a serverless dispatcher.

### Composition with callback ingress

Callback-only servers may optionally run maintenance in the same process for
small deployments:

```text
awa callbacks serve --with-maintenance
```

That option should be implemented by composing the same maintenance runtime,
not by embedding a separate maintenance loop in the callback server. Larger
deployments should run callback ingress and maintenance as separate roles.

### Leadership and cancellation

Maintenance-only mode uses the same leader-election semantics as the normal
runtime. Maintenance tasks start only while the process owns the maintenance
lock and stop promptly when leadership is lost or the process shuts down.

The internal lane isolation work should still apply: user-visible maintenance
such as promotion and rescue should not be blocked by slower admin hygiene or
cleanup branches.

### Relationship to bounded tick execution

The persistent maintenance-only role does not replace a future bounded
`tick()` API. They serve different deployment models:

- `maintenance run`: persistent, leader-elected, predictable internal role,
- `tick()`: bounded, externally invoked, useful for scale-to-zero or very
  low-traffic deployments.

Both should share the same underlying maintenance steps where possible.

## Consequences

### Positive

- Deployments can keep maintenance guarantees without running a job-executing
  worker in the same process.
- Callback-only ingress has an obvious companion role for time-based rescue
  and storage hygiene.
- The role split makes operational docs clearer: admin UI, callback ingress,
  workers, and maintenance can be scaled and exposed independently.
- Library users with code-declared cron jobs or descriptors can still publish
  those declarations without registering handlers.

### Negative / Risks

- Operators may confuse maintenance-only with a full worker. The CLI and docs
  must be explicit that it does not dispatch or execute user jobs.
- Pool-only CLI maintenance cannot know application-defined cron jobs or
  descriptors. That limitation must be documented.
- More runtime modes increase test matrix size around shutdown, leadership
  loss, and task cancellation.
- Running maintenance alongside callback ingress is convenient but can blur
  deployment boundaries if presented as the default.

## Alternatives considered

### Keep maintenance only inside normal workers

This is simple and works for traditional deployments. It forces users who only
need maintenance to run a process that is also capable of claiming and
executing jobs, which is unnecessarily broad for callback/API-heavy
deployments.

### Put maintenance inside the callback receiver

This is convenient for the callback-only story but creates the wrong
ownership. HTTP routing and database maintenance are separate concerns. The
callback server may compose maintenance, but it should not own the
implementation.

### Only build bounded `tick()`

`tick()` is attractive for low-traffic and scale-to-zero deployments, but it
depends on an external caller and has tick-interval latency. A persistent
maintenance role is still useful for production systems that want timely
promotion and rescue without job execution in that process.

### Use database-native scheduling only

Database schedulers such as `pg_cron` can trigger maintenance calls, but they
do not solve application-defined cron descriptors, runtime metadata, or
portable deployment across Postgres providers. They may be a driver for
bounded `tick()`, not a replacement for the runtime role.

## Relationship to other ADRs

- ADR-027 separates callback ingress from the admin UI. This ADR defines the
  internal maintenance role that may accompany that deployment.
- ADR-018 explains that HTTP-worker async mode still requires an always-on
  dispatcher. Maintenance-only does not change that requirement.
- ADR-021 relies on expired callback rescue. This ADR gives deployments a way
  to run that rescue without also executing handlers in the same process.
- ADR-019 owns queue-storage maintenance concerns such as rotation and
  pruning. Maintenance-only runs those existing storage tasks; it does not
  redefine the storage engine.
- ADR-029 defines transactional follow-up jobs. Maintenance-only uses that
  mechanism to dispatch durable lifecycle effects for rescues (expired
  callback, stale heartbeat, exceeded deadline). The dispatch is
  best-effort (in a separate transaction after the rescue UPDATE
  commits — see ADR-029's atomicity matrix); once the follow-up
  `INSERT` lands, the row is a regular Awa job. This closes the
  rescue/timeout event gap without requiring a handler registry in the
  maintenance process.
