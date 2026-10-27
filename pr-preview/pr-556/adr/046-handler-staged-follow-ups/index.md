> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# ADR-046: Handler-staged follow-up jobs

## Status

Proposed. Extends [ADR-029](../029-transactional-followup-jobs/index.md) transactional follow-ups. No schema change.

## Context

A two-stage pipeline: a stage-A handler computes a value at run time, such as the ID of the item it resolved, and stage B must run for that value, once per distinct value however many A jobs produce it. B should exist only if A completes.

ADR-029 enqueues follow-ups transactionally, but its registered specs (`on_completed_enqueue_with::<A, B, _>(|args, job| ...)`) see only A's args and job row, not what A's handler computed. Unique options on a spec do deduplicate it: a follow-up whose key already exists is skipped under a savepoint and the trigger still commits ([Deduplicating follow-ups](../../lifecycle-hooks/index.md#deduplicating-follow-ups)). So the pipeline works only when B's value can be derived from A's args.

The report that prompted this ADR asked for typed or untyped metadata on `JobResult` or `JobRow` that a registered spec could read. The alternatives below explain why that shape was not chosen. Today a handler has three ways to pass the value, none adequate:

- **Write it where a registered spec can read it.** Completion clears the progress buffer before specs run, on both engines. Progress also survives retries by design ([ADR-014](../014-structured-progress/index.md)), so a value written by a failed attempt could reach a later attempt's follow-up.
- **Insert B from the handler** with `insert_with(ctx.pool(), ...)` and unique options. B then exists before A commits, survives if A fails or is rescued, and a non-deterministic handler can insert a different B on each attempt.
- **Use caller-owned completion** ([ADR-042](../042-caller-owned-finalization-transactions/index.md)). It is atomic, but it needs a distinct handler type, a dedicated finalization function and a per-job transaction. It is the right tool when A writes application rows, and far heavier than passing one value.

The requirement:

> A handler can declare follow-up jobs built from values it computed. They are enqueued if and only if the handler's own completion of this attempt commits, and they are deduplicated by their own unique keys.

## Decision

The handler **stages follow-ups** on its context while it runs. A *staged follow-up* is an `EnqueueRequest` passed to `enqueue_on_completion`; once inserted it is an ordinary *follow-up job*, the same as one produced by a *registered spec*. The runtime inserts staged follow-ups in the transaction that completes the attempt and discards them on every other outcome. `JobResult` and the `Worker` trait do not change, no `on_*_enqueue*` builder is added, and nothing is persisted on A's row.

### Rust surface

```rust
impl JobContext {
    /// A cloneable handle for staging follow-ups, usable from futures that
    /// cannot borrow the context.
    pub fn follow_ups(&self) -> FollowUps;

    /// Shorthand for `self.follow_ups().enqueue_on_completion(request)`.
    pub fn enqueue_on_completion<F: JobArgs>(
        &self,
        request: impl Into<EnqueueRequest<F>>,
    ) -> Result<(), StageError>;
}

#[derive(Clone)]
pub struct FollowUps { /* Arc to the attempt's staging buffer */ }

impl FollowUps {
    pub fn enqueue_on_completion<F: JobArgs>(
        &self,
        request: impl Into<EnqueueRequest<F>>,
    ) -> Result<(), StageError>;
}

/// Why a follow-up could not be staged. Converts to `JobError::Terminal`.
#[non_exhaustive]
pub enum StageError {
    /// The args or metadata would not serialize or failed validation.
    Invalid(String),
    TooMany { limit: usize },
    TooLarge { limit_bytes: usize },
    /// The handler already returned.
    Sealed,
}

impl<F: JobArgs> EnqueueRequest<F> {
    /// Set unique options without replacing the rest of the `InsertOpts`.
    pub fn unique(self, unique: UniqueOpts) -> Self;
    /// Set metadata without replacing the rest of the `InsertOpts`.
    pub fn metadata(self, metadata: serde_json::Value) -> Self;
}
```

The pipeline, as a typed handler:

```rust
client_builder.register::<StageA, _, _>(|args, ctx| {
    let follow_ups = ctx.follow_ups();
    async move {
        let item_id = resolve_item(&args).await?;
        follow_ups.enqueue_on_completion(
            EnqueueRequest::new(StageB { item_id }).unique(UniqueOpts::default()),
        )?;
        Ok(JobResult::Completed)
    }
})
```

`register` closures return a `'static` future that cannot borrow the context, which is why the handle exists. A `Worker` impl can call `ctx.enqueue_on_completion` directly.

Staging serializes the args, computes the unique key, checks the limits and validates every rule the insert would otherwise enforce in the database (priority 1–4, `max_attempts` 1–1000, kind and queue names of at most 200 characters, at most 20 tags, no NUL bytes), so nothing type-dependent or predictably rejected reaches the completion transaction. `StageError` converts to `JobError::Terminal` by default. That suits `Invalid`, which is deterministic for the request. `TooMany` and `TooLarge` depend on how much this attempt has already staged, so a handler whose fan-out varies between attempts can map them to a retryable error instead. `Sealed` means the call came after the handler returned, which is a handler bug rather than a property of the request.

A failed stage **poisons the attempt**. If any `enqueue_on_completion` call failed and the handler still returns `Completed`, the runtime fails the attempt terminally with the staging error, so the job goes to the DLQ when one is enabled and registered `on_exhausted_enqueue` specs fire. A swallowed error (`let _ = ...`, or `except Exception` in Python) therefore cannot let A complete without the follow-up it asked for.

`EnqueueRequest::unique` and `::metadata` exist because `with_opts` replaces the whole `InsertOpts`, silently discarding an earlier `.queue()` or `.priority()`. Metadata matters here: the unique key covers B's kind and all of its args (with `UniqueOpts::default()`), so context that must not affect deduplication, such as A's ID, belongs in metadata.

### Python surface

```python
async def handle(job):
    item_id = await resolve_item(job.args)
    job.enqueue_on_completion(StageB(item_id=item_id), unique_opts={})
```

The keyword arguments mirror `Transaction.insert`: `kind`, `queue`, `priority`, `max_attempts`, `tags`, `metadata`, `run_at`, `unique_opts` and `ordering_key`. `unique_opts={}` means the default options; `states` is an integer bitmask, as for `Transaction.insert`. A request that cannot be staged, including args or options that do not convert, raises `awa.StagingError`, a subclass of `awa.TerminalError` chained from the original error, and poisons the attempt as in Rust.

Args are converted to JSON when staged, while the handler holds the GIL, so the completion transaction is pure Rust and never calls back into Python. This is Python's first follow-up primitive: registered specs remain Rust-only (ADR-029, open extensions). Return values do not carry follow-ups, because Python already maps any unrecognised return value to `Completed`.

### Lifetime of the staging buffer

The buffer belongs to one attempt. It is created with the attempt's context and **sealed when the handler returns**: the executor takes its contents at that point, and later calls, for example from a task the handler spawned and did not await, return `StageError::Sealed` (`awa.StagingError` in Python). A retried attempt starts with an empty buffer and stages again when its handler re-runs.

### When staged follow-ups commit

The guarantee covers the **handler's own `Completed` finalization**. Every other way an attempt can end discards the buffer.

| Attempt ends with | Staged follow-ups |
| --- | --- |
| `Completed`, and the lease-guarded completion commits | Inserted in the completion transaction |
| `Completed`, but the guard matches no row (rescued, cancelled or taken over first) | Discarded with the rolled-back transaction |
| `Completed` after a failed stage | Discarded; the attempt fails terminally with the staging error |
| `RetryAfter`, `Snooze`, `Cancel`, `Err`, panic | Discarded |
| `WaitForCallback` (the handler returns and parks the job) | Discarded. The context is gone before the callback resolves. Logged at `warn` with kind, job ID and count, never args, and counted in a metric, because it is almost always a mistake |
| `ctx.wait_for_callback(...)` inside the handler ([ADR-021](../021-enhanced-external-wait/index.md)), resumed, then `Completed` | Inserted: the handler and its buffer stay alive across the wait |
| `ctx.wait_for_callback(...)` inside the handler, but `Client::complete_external` completes the job | Discarded. The external completion finalizes the job from a process with no handler context; registered specs fire there, staged follow-ups cannot. The waiting handler then gets an error from `wait_for_callback`, and even a `Completed` from it would find no running row |
| Deadline expiry abandons the handler, or shutdown cancels it before finalization | Discarded; rescue re-runs the handler if attempts remain |
| Shutdown cancels finalization, or the finalize deadline expires | Committed with the completion if the commit landed, otherwise not; rescue re-runs the handler if attempts remain |

### How they are inserted

- **One transaction.** The completion transaction runs registered `Completed` specs first, then the staged follow-ups in staging order. Either all of it commits with the completion or none of it does.
- **Deduplication.** A staged follow-up with unique options is inserted under a savepoint; a conflict on the deduplication index (`idx_awa_jobs_unique`) is skipped and the completion still commits, exactly as for registered specs. Any other error rolls the completion back. Skips are logged at `debug` with kinds and job IDs, never args.
- **Path selection is per job.** A job that staged nothing keeps the batched and receipt-plane completion paths. A job that staged something takes the per-job completion transaction, as every job of a kind with registered `Completed` specs already does.
- **Finalize retries reuse the same follow-ups.** On a deadlock or lost connection, `finalize_with_retry` runs the completion again in a new transaction. The sealed contents are owned by the finalizer and borrowed by each attempt, never drained, so a retry carries them. A commit whose acknowledgement was lost replays as stale and inserts nothing.

### Limits

Per attempt, at most **16 staged follow-ups** and **64 KiB of serialized requests** (kind, queue, args, metadata, tags and ordering key) in total, across any number of `wait_for_callback` resumes. Both are constants in v1.

- **Count.** Each unique insert runs in a savepoint, and on canonical the unique-claim trigger's exception block opens a second subtransaction. Releasing a savepoint ends it, but a subtransaction that wrote keeps its subtransaction ID, which stays in the backend's per-transaction cache until the completion commits. That cache holds 64 IDs before overflowing into `pg_subtrans`, which slows snapshots for every other session. 16 unique follow-ups use at most 32 and leave room for registered specs.
- **Size.** Staged follow-ups stay in memory from staging until finalization, both in running handlers and in completions waiting for or holding a finalizer slot. Finalizer capacity is `max(batch_size × shards × 2, worker capacity)`: 1,024 or 4,096 with queue-storage defaults, 8,192 on canonical. The worst case is 64 KiB × (finalizer capacity + worker capacity), reached only if every such job is at the limit: about 576 MiB with canonical defaults and 1,024 workers, more with more workers.

Larger fan-out belongs in a dedicated fan-out job or a bulk insert, not in one completion transaction.

### Visibility and testing

Staged follow-ups appear only as the inserted jobs: not in `UntypedJobEvent::Completed`, lifecycle hooks or A's row. Each insert opens the usual producer span inside the completion transaction, but B's trace context is captured when it is staged, so B joins the trace of A's handler. The value A computed is visible in B's args. Args are never logged.

The test harnesses behave like the runtime. `awa-testing`'s `work_one` and Python's `AwaTestClient.work_one` insert staged follow-ups in the same transaction as their own completion, apply poisoning, and discard them on every other outcome. The `awa.testing.HandlerJob` stand-in gains `enqueue_on_completion`, which applies the same validation and limits and records each request in `job.staged`, for unit tests that never touch a database.

## Guarantees

For a job A whose handler staged follow-ups R:

- **Committed together or not at all.** R's inserts run, and either commit or are skipped as duplicates, if and only if the handler's own `Completed` finalization of that attempt committed under its run lease. No row comes from an attempt that was rescued, cancelled, superseded or completed externally, or whose completion rolled back.
- **One attempt's follow-ups.** A job row completes at most once (admin retry does not reopen completed jobs), so at most one attempt's staged follow-ups are inserted, even when a rescued attempt and its replacement staged different values.
- **Deduplication.** For a follow-up with unique options, A's completion commits with either a new row or an existing row holding the same key in one of its `UniqueOpts::states`. The default states include `completed`, so a finished B blocks duplicates until its row is pruned, which ties the window to terminal retention. Removing `completed` limits it to Bs still scheduled, available, running or retryable. Either way this is "at most one B per key in those states", not "at most one B ever".
- **Delivery.** Each inserted follow-up is an ordinary job, delivered at least once.

Not guaranteed:

- **Handler side effects.** If an attempt creates an external resource and its completion does not commit, the resource is orphaned and the next attempt may create another. Awa guarantees only that no follow-up refers to it.
- **Exactly-once handler execution.** A may run several times; only the committing attempt's follow-ups survive.
- **Freedom from deadlock.** A completion that inserts several follow-ups holds each insert's locks until it commits: on queue storage the enqueue-lane lock, on both engines the unique claim. Two completions that take these in opposite orders can deadlock: two unique keys staged in opposite orders, or two lanes (different queues, or one queue with `enqueue_shards > 1`). Postgres aborts one transaction and `finalize_with_retry` retries it, up to three times, after which the job waits for stale-heartbeat rescue. A completion that inserts a single follow-up takes its unique claim before its lane lock, the order `AwaUniqueClaimLockOrder` checks, and cannot deadlock this way. Registered specs that insert several follow-ups share the same exposure today.
- **Prompt failure on a non-retryable insert error.** Such an error rolls the completion back. Today `finalize_with_retry` returns it, the executor logs it, and the job stays `running` until stale-heartbeat rescue re-runs it, as it does for registered specs. Staging moves serialization and every validation the schema enforces into the handler, so the remaining causes are database errors that cannot be predicted when staging, such as a lost connection mid-insert or a trigger added outside Awa.

Two consequences of deduplication need care:

- A follow-up of A's own kind and args, with A's own unique options, collides with A itself, because A's row holds the key while it is running and, by default, once completed. It is skipped and the chain stops. Give a continuation different args or options.
- Two follow-ups with the same key staged in one attempt collapse to one, as do different args when `by_args` is false.

## Non-goals: this is not a workflow engine

The [PRD](https://github.com/hardbyte/awa/blob/main/docs/archive/prd.md#35-non-goals) sets Awa's boundary: "No DAGs, no fan-out/fan-in, no saga orchestration. If you need Temporal, use Temporal." The 0.7 roadmap (D7) repeats it for job dependencies, and [#81](https://github.com/hardbyte/awa/issues/81) asks any proposal that moves values between jobs to restate it. Staging stays inside the boundary. This ADR does not provide, and must not grow into:

- **A pipeline definition.** Awa stores no relation between A and B. No graph, step list or topology exists in the builder, the schema or the runtime; the only place the "pipeline" exists is the handler's code.
- **Fan-in or joins.** Nothing waits for several jobs, or for B to finish. A is complete before B exists, and B's outcome never reaches A.
- **Workflow state or step results.** A's value travels only in B's args. Nothing is recorded on A, and there is no result store (see [Persist the output](#persist-the-output-on-the-job-or-its-terminal-history)).
- **Guarantees across steps.** B's failure does not affect A. There is no compensation, saga, rollback across jobs, or retry of a chain as a unit.
- **Durable execution.** An attempt is never resumed or replayed. A retried attempt starts from the beginning with an empty staging buffer.
- **Predicates in the engine.** A conditional follow-up is an `if` in the handler. The engine inserts the rows it is handed and evaluates no conditions.

The PRD's "no fan-out" refers to a fan-out construct: a step that spawns children and tracks them as a group, usually paired with a fan-in. Staging several follow-ups is not that. It is the same as an application inserting several jobs in one transaction, which Awa has always supported ([ADR-006](../006-awa-transaction/index.md)). The children are independent jobs and nothing tracks them as a set.

The guarantee is scoped to one transaction of one attempt: B is enqueued if and only if A's completion commits. That is ADR-006's transactional enqueue applied to the completion transaction. Once the completion commits, Awa keeps no link between A and B.

This gives a test for later proposals. A feature crosses into workflow-engine territory if it requires Awa to remember a relationship between jobs after the triggering transaction commits: a join, a step's result, a chain's status, or a compensation. Such a feature needs its own ADR under #81's rules, not an extension of this one. The provenance key in [Open extensions](#open-extensions) passes this test: it is a value written into B's metadata, not state the engine reads.

## Rollout

No schema change. A stage-B kind is new code, and an older worker that claims a job of an unknown kind fails it terminally. Because A has already completed, nothing re-creates that B; it must be retried from the DLQ or the admin UI. So:

1. Deploy workers that handle B everywhere B's queue is polled, or put B on a new queue that only those workers poll.
2. Then deploy the stage-A code that stages B.

## Relationship to other ADRs

- **ADR-029.** Registered specs keep their role for unconditional follow-ups derived from args. Both run in the same completion transaction. This ADR does not change ADR-029's atomicity matrix; it adds a source of follow-ups to the worker-driven `Completed` row.
- **ADR-042.** Caller-owned handlers insert follow-ups in their own transaction, as ADR-042 specifies, and staging is not offered on the caller-owned context. Moving a handler from staging to caller-owned completion swaps `enqueue_on_completion` for an insert in its transaction; no follow-up spec wiring changes.
- **ADR-026.** A job that stages completes through the general terminal path, not the compact one.
- **ADR-034 (proposed).** A dependent child exists before its parent runs, so it cannot carry a value the parent computes. Staging is complementary.
- **ADR-041.** No schema change; the rollout order above covers the new kinds.

## Validation

The implementation adds:

- **A TLA+ model**, `correctness/storage/AwaStagedFollowUps.tla`. Two jobs whose attempts stage a nondeterministic key, lease-guarded completion with the staged inserts in one step, lost acknowledgements and finalize retries, rescue and reclaim under a new lease, external completion, and release of a key's unique claim. Invariants: every follow-up traces to a committed handler completion of the attempt that staged it; at most one row per key; every committed completion has its follow-up or a row holding its key; only the current lease completes a job. Negative configs, each expected to fail, insert before completion, insert after it in a separate transaction, complete without the lease guard, drain the staged follow-ups on the first finalize attempt, and send a staging job down the batched path.
- **Tests on both engines.** Insertion with completion; discard on each non-`Completed` outcome; poisoning after a failed stage; sealing after return; a stale completion inserts nothing; deduplication across concurrent A jobs; a non-dedup unique violation rolls the completion back; a finalize retry inserts once; a job that stages nothing still completes through the batcher; limits; registered specs and staged follow-ups together; the `awa-testing` harness; Python staging, `AwaTestClient.work_one` and the `HandlerJob` stand-in.

## Consequences

### Positive

- The handler states, in one place, what it produced and what should happen next, with the atomicity ADR-029 gives registered specs.
- No public type changes shape. The additions are new methods and types.
- Fan-out is staging more than once; a conditional follow-up is an `if` in the handler.
- Unlike a registered `Completed` spec, which sends every job of its kind through the per-job transaction, staging sends only the jobs that stage.
- Python gets transactional follow-ups without running Python code inside a database transaction.

### Negative

- Pipeline topology is no longer fully visible in the client builder; a reader must look in handlers for staged follow-ups. A handler that should stay reusable across pipelines can take its request from an injected factory (`ctx.extract`).
- The staged value is transient. Debugging relies on B's args, and a deduplication skip leaves no trace on A beyond a debug log.
- A job that stages pays a per-job completion transaction and loses the compact completion path.

## Alternatives considered

### Handler output read by an output-typed spec

`JobResult::completed_with(value)` plus `on_completed_enqueue_with_output::<A, O, B, _>(|args, &output, job| ...)`. Rejected. `JobResult` is not `#[non_exhaustive]`, so the new variant is breaking today; the reasons below hold even after that is fixed.

### An attempt-scoped output slot on the context, read by a registered spec

`ctx.set_output(value)` and a spec that reads it. No `JobResult` change and no leakage across retries. Rejected:

- The output type meets the spec's expected type only at run time, inside the completion transaction. A mismatch rolls the completion back, which today leaves the job running until rescue, which then repeats the failure.
- `Client::complete_external` has no output, so output-reading specs need conditional dispatch, which ADR-029 keeps out of the registry.
- It adds a 13th `on_*_enqueue*` builder and invites siblings for every outcome.
- Python would have to run a Python callable inside the completion transaction.
- Staging is strictly more direct: the handler that has the value builds the request, and the type is checked at compile time.

### Persist the output on the job or its terminal history

Rejected. Custom metadata takes completion off the compact path of [ADR-026](../026-narrow-terminal-history/index.md), it widens a stable read surface, the value vanishes when terminal rows are pruned, and it starts turning Awa into a results store.

### Insert from the handler with `ctx.pool()`

Kept as a documented workaround, rejected as the pattern: B exists before A commits and survives A's failure, and a non-deterministic handler can insert a different B on each attempt.

### Wait for caller-owned completion

Rejected as the only answer. ADR-042 remains the tool when A also writes application rows. Passing a value to a follow-up should not require a distinct handler type and a dedicated finalization function.

### Job dependencies

Rejected for this problem: the child is created before the parent runs, so it cannot carry or deduplicate on a value the parent computes.

## Open extensions

- **Mark `JobResult` `#[non_exhaustive]`** before 0.7.0 is stable, independent of this decision.
- **Prompt failure for non-retryable finalize errors.** Record a lease-guarded failure with the error instead of waiting for heartbeat rescue, for registered specs and staged follow-ups alike.
- **Deadlock-free multi-insert completion.** Acquire every unique claim and enqueue-lane lock a completion needs, registered and staged, in one global order before inserting.
- **A deduplication metric** counting follow-ups skipped on an existing key, and **provenance**: stamp each follow-up with the triggering job ID under the reserved `awa:` metadata namespace so B can be traced to A. Both help answer "why is there no B for this value?"
- **Staging for other outcomes**, such as a follow-up on `RetryAfter`, if a use case appears.
