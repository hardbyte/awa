# ADR-042: Caller-owned finalization transactions

## Status

Proposed. Tracked in
[#401](https://github.com/hardbyte/awa/issues/401). This ADR defines the contract and validation
boundary; it does not claim that the Rust or SQL surfaces have shipped. The roadmap's status column
is the acceptance target, not the current status.

## Context

Transactional enqueue makes creation of business state and future work atomic, but the consumer
side still has a common crash window:

1. a handler commits business rows or a local outbox;
2. the process crashes before Awa commits the job's completion; and
3. rescue runs the handler again.

Idempotent handlers remain the general answer, especially for external side effects. For effects
contained in the same PostgreSQL database, however, Awa already has the ingredients for a stronger
contract: guarded finalization keyed by `(job_id, run_lease)`, transaction-aware queue-storage
completion helpers, and ADR-029 follow-up enqueues that can join a finalization transaction.

The desired primitive is therefore:

> Let application SQL and completion of this exact Awa attempt commit in one caller-owned
> PostgreSQL transaction.

If the transaction rolls back, neither the application writes nor the completion is visible and
the attempt remains recoverable. If rescue wins first, the stale finalization must abort the
application transaction rather than allowing the business writes to commit without their matching
job acknowledgement.

This is "exactly-once state effects in one Postgres database," not exactly-once execution. Network
calls and effects in another database or provider remain outside the atomic boundary.

## Decision

Expose successful completion as an opt-in transaction-aware operation for Rust and as a versioned
SQL worker contract for Python and other Postgres drivers. The caller owns the transaction and its
business statements; Awa owns the guarded finalization protocol inside it.

### Attempt and finalization receipts

Every dispatched queue-storage attempt exposes an opaque, versioned `FinalizationToken` containing
enough immutable identity to route directly to its claim evidence. Conceptually it includes:

- job id and run lease;
- claim-ring slot;
- receipt id and, for compact claims, batch id/item index; and
- ready-lane identity needed by the terminal transition.

The Rust type is non-constructible outside Awa. The SQL contract takes an explicit portable form of
the same fields rather than requiring another language to decode a Rust blob. Resolution is
representation-aware: it finds either the original receipt claim or the mutable `leases` row
created when an attempt registers or waits for an external callback. Materialization changes the
physical authority, not the token or `run_lease`.

Successful guarded completion returns a `FinalizationReceipt`. The receipt identifies the token,
committed outcome expected by the handler, and enough terminal evidence for the runtime to
reconcile an ambiguous commit. A receipt is produced by the finalization statement before the
outer transaction commits; it becomes authoritative only when matching evidence is visible in the
database.

### Rust surface

Caller-owned completion is declared on the job kind at worker registration. The exact builder
spelling is an implementation detail, but the registration contract distinguishes ordinary
executor-owned completion from `CompletionMode::CallerOwned`. A handler registered in ordinary
mode cannot return `Finalized`; a caller-owned kind always takes the reconciliation path described
below. Making the mode static lets startup reject incompatible follow-up specifications, expose the
capability in health/diagnostics, and avoid adding a reconciliation query to every ordinary job.

The ergonomic Rust path operates on the caller's `sqlx::Transaction`:

```rust
async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
    let provider_state = fetch_provider_state().await?; // no DB tx held here

    let app_db = ctx.extract::<AppDb>().ok_or(JobError::misconfigured("AppDb"))?;
    let mut tx = app_db.pool.begin().await?;
    sqlx::query("UPDATE billing_operations SET state = $1 WHERE id = $2")
        .bind(provider_state)
        .bind(self.operation_id)
        .execute(&mut *tx)
        .await?;

    let receipt = ctx.complete_in_tx(&mut tx).await?;
    tx.commit().await?;
    Ok(JobResult::Finalized(receipt))
}
```

`complete_in_tx` performs the same attempt guard, terminal-history write, receipt closure,
uniqueness transition, progress flush, and exact-key grant closure as normal completion. It is a
single-job slow path and deliberately bypasses the completion batcher: a caller transaction cannot
be merged with unrelated jobs' transactions.

`JobResult::Finalized(receipt)` carries the caller's expected outcome, but it is not an unchecked
`AlreadyFinalized` assertion. For every exit from a caller-owned handler -- success, error, panic,
or cancellation -- the executor first reconciles durable evidence for the attempt's
`FinalizationToken`. If the handler returned a receipt, it must match that evidence. Before
releasing the dispatch permit, in-flight state, or ADR-033 execution-grant accounting, the executor
classifies the result:

1. matching terminal evidence visible: accept the outcome and run post-commit observation hooks;
2. matching attempt still open: treat the result as a protocol error and drive the ordinary
   retry/error path;
3. attempt closed by a different or newer outcome: treat it as stale and do not mutate the winner;
4. database result unknown/unavailable: retain the in-flight record and retry reconciliation within
   a bounded budget rather than finalizing again.

This verification covers connection loss after PostgreSQL commits but before the caller receives a
commit acknowledgement: even if `tx.commit().await?` returns an error and the handler never returns
its receipt, the statically selected executor path checks the original attempt token before
applying retry policy. A definitely rolled-back or never-finalized transaction leaves the attempt
open and follows the normal handler result. An ambiguous result that did commit is observed as the
matching terminal outcome.

If matching completion is visible after the handler returned an error or panicked, durable state
wins: the executor accepts completion and emits a warn-level protocol event containing the job,
attempt, and handler exit classification. If PostgreSQL remains unavailable through the configured
reconciliation budget or the attempt's rescue deadline, the executor releases its heavyweight
dispatch permit and leaves a lightweight unresolved-attempt watcher. It does not issue completion
or retry. The durable attempt and any ADR-033 grant remain open until reconciliation or maintenance
rescue closes them, so bounded local resource use does not manufacture database capacity.

### SQL surface for Python and other drivers

A live `sqlx::Transaction` is not passed through PyO3 or bridged into psycopg, asyncpg,
SQLAlchemy, SeaORM, or tokio-postgres. Instead Awa exposes a versioned worker-only SQL function,
conceptually:

```sql
SELECT *
FROM awa.complete_job_compat(
    job_id       => $1,
    run_lease    => $2,
    claim_slot   => $3,
    receipt_id   => $4,
    batch_id     => $5,
    batch_index  => $6,
    progress     => $7
);
```

The exact signature is frozen with the SQL contract work in #342. The function is `SECURITY
INVOKER`, is granted only to the worker role, does not commit, and returns the portable
`FinalizationReceipt` fields. It routes through the active queue-storage representation and applies
the same guard and writes as the Rust helper.

A stale token raises a dedicated SQLSTATE exception. Returning `false` is insufficient: callers
could accidentally commit their business writes after ignoring the result. The exception aborts
the current transaction unless the caller deliberately contains it with a savepoint; doing so and
committing application writes is outside the supported contract.

Driver documentation must name nested-transaction and savepoint wrappers that can swallow this
exception, including SQLAlchemy `begin_nested()`. Such a transaction is unsupported even if the ORM
later commits successfully, because it has deliberately separated application writes from the
stale finalization failure.

Python bindings construct `JobResult.finalized(receipt)` only from the function result. As on the
Rust path, caller-owned mode makes the worker runtime reconcile the dispatched attempt token after
every handler exit, including a driver exception while committing.

### Scope of v1

The first public operation is successful `Completed` finalization. Retry, snooze, terminal failure,
DLQ routing, cancellation, and callback parking continue to be selected by `JobResult`/`JobError`
and committed by the executor. Those transitions combine attempt budgets, DLQ policy, callback
state, and outcome context; exposing them all in the first SQL contract would freeze too much
surface before the completion primitive is proven.

A failure of the caller-owned transaction therefore returns a normal handler error. Once
reconciliation proves that the attempt remains open, Awa applies its existing retry/exhaustion
policy outside the rolled-back transaction.

### Follow-ups, hooks, and metrics

ADR-029's registered `on_completed_enqueue` closure is process-local application code. A generic
SQL function invoked through another driver cannot evaluate that closure inside the caller's
transaction. To avoid silently weakening atomicity, registration rejects a job kind that combines
`CompletionMode::CallerOwned` and registered Completed follow-up specs.

Handlers using caller-owned completion enqueue explicit outbox/follow-up jobs inside their
transaction through ADR-006/016/017-compatible insert surfaces. This is the most direct expression
of the atomic business effect. A future database-stored declarative follow-up registry could remove
the restriction without running process-local code from SQL.

Storage-level finalization obligations are different from process-local hooks. If ADR-034 job
dependencies are enabled, successful caller-owned completion must promote or resolve dependants in
the same transaction just like ordinary completion. Dependants are per-job data and cannot be
rejected reliably at registration, so `complete_in_tx` / `complete_job_compat` owns this step.

Best-effort lifecycle hooks, metrics, and terminal tracing run only after the executor verifies the
committed receipt. They retain their existing crash-loss boundary: a process that commits and dies
before observation dispatch has still completed the durable work correctly.

### Interaction with fleet-exact per-key execution

ADR-042 does not depend on ADR-033 and can ship while exact key control remains E5-gated. When
ADR-033 exact per-key control is enabled, the attempt's execution-grant closure is part of
`complete_in_tx`. Therefore one commit makes all three facts visible together:

- the application's billing/inbox/outbox rows;
- the Awa terminal/claim-closure evidence; and
- release of the per-key execution grant.

Until that commit, later work for the same key remains gated. On rollback the grant stays open. If
maintenance rescue wins, both paths serialize on the existing per-attempt advisory lock; the stale
caller finalization raises and aborts its business transaction. If caller finalization wins, rescue
observes closed evidence and skips.

### Transaction discipline

Caller-owned finalization transactions must be short:

- perform provider/network calls before opening the transaction;
- avoid user interaction, sleeps, and unbounded computation while it is open;
- use the same PostgreSQL database as Awa and the application rows;
- prefer a separate application pool for caller transactions, leaving the runtime pool available
  for claim, heartbeat, reconciliation, completion, and maintenance; and
- let serialization/deadlock failures abort and retry through the normal handler policy.

Holding a transaction across the whole handler would pin MVCC horizons and undo the queue-storage
work in ADR-019/023/026. The API documentation and examples always open it only for the final local
state write and acknowledgement. `complete_in_tx` accepts a transaction from any pool connected to
the same database and schema; `ctx.pool()` is not the recommended business-transaction source. If a
deployment deliberately shares the runtime pool, startup validates that configured caller-owned
concurrency leaves a reserved runtime connection budget. Admission must cap caller-owned handlers
below that boundary rather than relying on pool-acquire timeouts during heartbeat or reconciliation.
Deployment guidance also budgets both pools against PostgreSQL's database-wide connection limit.

## Guarantees and non-guarantees

When used as specified, caller-owned completion guarantees:

- application writes and successful completion of the exact run lease commit or roll back together;
- a stale/rescued attempt cannot commit supported application writes without its completion;
- an ambiguous commit is never interpreted as completion or retry without durable reconciliation;
  after the bounded outage path, heavyweight local capacity may be released while the durable
  attempt remains open for rescue; and
- ADR-033 key capacity is released in the same commit.

It does not guarantee:

- exactly-once handler execution;
- exactly-once external API, email, filesystem, or cross-database effects;
- automatic idempotency for SQL written outside the finalization transaction;
- atomic process-local hooks; or
- completion batching performance for opted-in jobs.

The recommended billing-inbox shape is: fetch/reconcile provider state using provider idempotency,
then open one short transaction that updates the local operation/inbox rows, optionally inserts a
local outbox or Awa follow-up job, and completes the Awa attempt.

## Rolling upgrade and compatibility

The SQL function and receipt fields are additive but affect the worker protocol and stored claim
representation. They follow ADR-041:

- old workers ignore the surface while it is disabled;
- caller-owned completion cannot be enabled for a queue until every claiming runtime advertises
  receipt support;
- the SQL contract is versioned against `awa.schema_version` and covered by #342 conformance tests;
  and
- mixed-version rehearsal proves normal executor completion still works while the new path is
  disabled.

The feature is queue-storage-only. The canonical engine is deprecated by ADR-037 and does not gain
a second public finalization implementation.

## Validation

Acceptance requires:

- Rust integration tests for commit, rollback, stale lease, follow-up insertion, uniqueness, and
  progress/terminal hydration;
- completion through both receipt-claim and materialized-lease authority, including a sequential
  callback wait/resume before caller-owned completion;
- executor tests proving every caller-owned handler exit reconciles before completion/retry,
  including a commit error where the server committed but no receipt was returned, and that permit
  release without a database answer occurs only through the bounded unresolved-attempt path;
- a committed completion followed by handler error/panic is accepted from durable evidence and
  emits the required warn-level protocol event;
- asyncpg, psycopg, SQLAlchemy, and tokio-postgres/SeaORM conformance examples using caller-owned
  transactions;
- negative conformance tests for each driver's savepoint/nested-transaction pattern, proving a stale
  finalization cannot be swallowed and followed by an application commit in supported usage;
- a crash-after-server-commit/before-client-ack test proving receipt reconciliation;
- a completion-versus-rescue TLA+ model where either completion commits all application/finalization
  facts or rescue wins and the application transaction cannot commit;
- lock-order coverage for application transaction -> per-attempt advisory lock -> claim/lease and
  terminal children;
- conditional ADR-033 integration cells, when Tier 2 is enabled, proving the key grant closes on
  commit and stays open on rollback;
- pool-starvation tests proving caller-owned transactions cannot consume the runtime connection
  reserve needed by heartbeat and reconciliation;
- a pinned-MVCC test rejecting transactions held across the handler and measuring the documented
  short-transaction path; and
- performance comparison against batched completion, reported honestly as an opt-in per-job
  slow-path cost.

## Alternatives considered

### Trust `JobResult::AlreadyFinalized`

Rejected. A caller may return it after rollback, before commit, or after an ambiguous failure. The
runtime needs a receipt and durable verification before releasing capacity.

### Let Awa own and commit the transaction through a closure

Useful as a convenience API and safer for simple sqlx callers, but insufficient as the only
surface: it cannot join an existing psycopg/SQLAlchemy/SeaORM transaction. It may be added on top of
the caller-owned primitive.

### Pass a live sqlx transaction through Python FFI

Rejected for the same reasons as ADR-006/017 driver bridging. Two drivers cannot safely share one
Postgres protocol session, and the lifetime/commit boundary is not portable through PyO3.

### Return finalization SQL and parameters to each driver

Rejected as the stable contract. It avoids an installed function but makes every binding reproduce
multi-statement ordering, stale-guard interpretation, storage-representation routing, dependency
promotion, and future additive compatibility. A versioned `SECURITY INVOKER` function keeps that
protocol server-side, matching #342's SQL-contract direction while still running inside the
caller's transaction.

### Transactional outbox without Awa completion

Still leaves the job acknowledgement crash window. An outbox is complementary when the eventual
effect is external; its insertion should join the caller-owned finalization transaction.

### Distributed transaction across provider and Postgres

Rejected. External providers generally do not participate in PostgreSQL two-phase commit, and Awa
does not introduce a distributed transaction coordinator.

## Relationship to other ADRs

- **ADR-006/016/017:** reuse the caller's Postgres transaction and driver-specific insert adapters;
  do not bridge live connections between drivers.
- **ADR-013:** `(job_id, run_lease)` remains the stale-writer guard. The finalization token adds
  direct receipt routing but does not replace lease identity.
- **ADR-015:** observation remains post-commit and best-effort.
- **ADR-021:** the token resolves both receipt and materialized-lease authority across sequential
  `running` / `waiting_external` transitions without changing the run lease.
- **ADR-023/026:** caller-owned completion writes the same claim-closure and terminal evidence as
  the ordinary single-job slow path and participates in the same prune proof.
- **ADR-029:** explicit follow-up enqueues may join the caller transaction; process-local registered
  completion specs are incompatible with v1 caller-owned completion for the same kind.
- **ADR-033:** exact key-grant closure joins the same atomic commit.
- **ADR-034:** when dependencies ship, their storage-level promotion/resolution is part of the
  caller-owned finalization statement; it is not a process-local follow-up.
- **ADR-036/041:** the Rust and SQL surfaces are versioned public contracts delivered through an
  additive, capability-gated rollout.
