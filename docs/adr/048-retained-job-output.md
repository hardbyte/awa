# ADR-048: Retained job output

## Status

Proposed. Builds on [ADR-046](046-handler-staged-follow-ups.md) staged follow-ups and [ADR-026](026-narrow-terminal-history.md) narrow terminal history. Adds migration v052 (queue storage only).

## Context

ADR-046 split a request for "metadata on `JobResult` or `JobRow`" into three questions:

1. How does A pass a value it computed into B reliably?
2. Can someone inspect A's result afterwards?
3. Does B depend on fetching A's stored result later?

Staging answers the first, and its answer to the third is no: B gets its value in its own args. This ADR answers the second. Today a handler's return value is discarded: a Rust handler returns only an outcome, Python maps any unrecognised return value to `Completed`, and `complete_external` drops its payload on completion. Operators who want to know what a job produced read logs, or the handler writes the value somewhere else.

Most queues that keep a result keep it for one of two reasons. Some keep it for inspection: River's `RecordOutput` is opt-in, size-capped and pruned with the job, and pg-boss stores an `output` column. Others keep it because a declared workflow engine needs it: Oban Pro workflows, Celery chords, Hatchet, Temporal and Step Functions. Awa disclaims the second ([ADR-046, non-goals](046-handler-staged-follow-ups.md#non-goals-this-is-not-a-workflow-engine)). The failures those systems document come from the second use or from defaults: result stores bloated by default-on storage (Celery, BullMQ), and downstream steps that fetch a result after it has expired (Celery chords that never run their body, Oban Pro's `{:error, :missing}`).

The requirement:

> A handler can record an optional, size-bounded JSON value describing what an attempt produced. It is published only with that attempt's own lease-guarded transition, readable for as long as the job is, and never read by Awa to decide what runs.

### Cost, measured

The deciding constraint on queue storage was the compact completion path of ADR-026, which writes one batch row for many jobs instead of a terminal row per job. Measurements on Postgres 16 (fsync on, 512-job batches) of the terminal write with a 100-byte value on every job:

| Representation | Keeps the compact path | Extra WAL per job | Completion statement throughput |
| --- | --- | --- | --- |
| None (today) | yes | 0 | 502k jobs/s |
| Array on the compact batch row | yes | +46 B | 295k jobs/s |
| Side table keyed by job | yes | +294 B | 251k jobs/s |
| Column on `done_entries`, or a key in the payload | no | +507 B | 174k jobs/s |

End to end, forcing a job onto the general terminal path costs about two thirds of throughput (11.2k to 3.6–5.7k jobs/s) and +427 B of WAL per job. A 100-byte value carried on the compact batch adds about 3.5% to a job's lifecycle WAL. A 2 KB value on every job roughly doubles it, which is why the value is capped. For comparison, staging one follow-up per job adds about 2,400 B of WAL per job.

## Decision

A handler may **record an output** for its attempt. The output is a JSON value of at most 4 KiB. It is attempt-scoped like ADR-046's staging buffer and sealed with it when the handler returns. It is published by the transition the attempt's own executor writes:

- on completion it becomes the job's **output**;
- on a failure the executor records, it is kept in that attempt's **error entry**.

The engine never reads it.

The name is *output*, not *result*: `JobResult` is already the outcome enum and every handler returns a `Result`, so a third "result" would be ambiguous. It matches River's `RecordOutput`.

### Rust surface

```rust
impl JobContext {
    /// Record this attempt's output. The last call before the handler returns wins.
    pub fn set_output<T: Serialize + ?Sized>(&self, output: &T) -> Result<(), OutputError>;

    /// A cloneable handle for futures that cannot borrow the context.
    pub fn output_slot(&self) -> OutputSlot;
}

#[derive(Clone)]
pub struct OutputSlot { /* Arc to the attempt's output slot */ }

impl OutputSlot {
    pub fn set<T: Serialize + ?Sized>(&self, output: &T) -> Result<(), OutputError>;
    /// Remove a recorded output, so the attempt publishes none.
    pub fn clear(&self) -> Result<(), OutputError>;
}

/// Why an output could not be recorded. Converts to `JobError::Terminal`.
#[non_exhaustive]
pub enum OutputError {
    /// The value would not serialize, or contains a NUL character.
    Invalid(String),
    TooLarge { limit_bytes: usize },
    /// The handler already returned.
    Sealed,
}

pub const MAX_OUTPUT_BYTES: usize = 4 * 1024;

#[non_exhaustive]
pub struct JobRow {
    // ...existing fields...
    /// Set when the job completed with an output (queue storage only).
    #[sqlx(default)]
    pub output: Option<serde_json::Value>,
}

impl JobRow {
    pub fn output_as<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error>;
}
```

With staging, a pipeline stage that both hands a value on and leaves it for inspection:

```rust
client_builder.register::<StageA, _, _>(|args, ctx| {
    let (follow_ups, output) = (ctx.follow_ups(), ctx.output_slot());
    async move {
        let item_id = resolve_item(&args).await?;
        output.set(&Resolved { item_id })?;
        follow_ups.enqueue_on_completion(
            EnqueueRequest::new(StageB { item_id }).unique(UniqueOpts::default()),
        )?;
        Ok(JobResult::Completed)
    }
})
```

The value is written twice, once as A's output and once in B's args, and nothing checks that the two agree. That is deliberate: they serve different readers, and B never depends on A's output surviving.

`set_output` serializes the value immediately and checks its size and content, so nothing that can fail reaches the transition. **A failed call does not poison the attempt.** Nothing depends on the output, so a missing one breaks no guarantee, unlike a missing follow-up. The error converts to `JobError::Terminal`, so `?` fails the attempt when the handler wants that. A failed call leaves any earlier value in place.

`JobRow` gains a field, not a method, because the row is the value that flows to HTTP, the admin UI, lifecycle events and Python. `JobRow` is `#[non_exhaustive]` from 0.7 ([#564](https://github.com/hardbyte/awa/pull/564)), so the field is not a source break, and `#[sqlx(default)]` keeps queries that select an explicit column list working.

### Python surface

```python
async def handle(job):
    item_id = await resolve_item(job.args)
    job.set_output({"item_id": item_id})
    job.enqueue_on_completion(StageB(item_id=item_id), unique_opts={})

row = await client.get_job(job_id)
row.output            # {"item_id": ...}, or None
```

The value is converted to JSON when set, while the handler holds the GIL. A value that cannot be recorded raises `awa.OutputError`, a subclass of `awa.TerminalError`, without poisoning the attempt. During dispatch `job.output` returns the value recorded so far; on a row read back from the database it returns the stored output.

Return values stay as they are. A worker that returns a dict today has it discarded, so making return values the output would change existing handlers. An opt-in such as `@client.worker(..., output=True)` is listed under [Open extensions](#open-extensions).

### Lifetime of the output slot

The slot belongs to one attempt and is created with its context. Setting it again replaces the value; `clear` empties it. It is **sealed when the handler returns**, at the same point as ADR-046's staging buffer, and later calls return `OutputError::Sealed`. The finalizer owns the sealed value and lends it to every finalize retry, so a retry after a lost connection publishes the same value. A retried attempt starts with an empty slot.

### Where an attempt's output goes

| Attempt ends with | Output |
| --- | --- |
| `Completed`, and the lease-guarded completion commits | Stored as the job's output |
| `Completed`, but the guard matches no row (rescued, cancelled or taken over first) | Discarded with the rolled-back transaction |
| `Completed` after a failed stage (ADR-046 poisoning) | Kept in the terminal error entry of the poisoned attempt |
| `Err`, retryable or terminal, including retries exhausted | Kept in that attempt's error entry |
| A panic | Kept in the error entry the executor writes for the panic |
| `Cancel` | Kept in the cancellation's error entry |
| `RetryAfter` on the last attempt | Kept in the exhaustion error entry |
| `RetryAfter` with attempts left, `Snooze` | Discarded. Neither writes an error entry, and neither is a failure |
| `WaitForCallback` | Discarded; the context is gone before the callback resolves, as for staged follow-ups |
| `ctx.wait_for_callback(...)` inside the handler, resumed, then `Completed` | Stored: the handler and its slot stay alive across the wait |
| Heartbeat or deadline rescue, or shutdown before finalization | None. Rescue writes the error entry from another process, which never saw the value |

An error entry with an output is `{"error": ..., "attempt": ..., "at": ..., "output": ...}`. Error entries are already per attempt, append-only, carried through retries, and moved with the job into the DLQ and back. So a failed attempt's output can never be mistaken for the job's output or for a later attempt's, and it needs no new storage. A job that failed twice and then completed has two outputs in `errors` and one in `output`.

This keeps output separate from [ADR-014](014-structured-progress.md) progress. Progress is a mutable checkpoint, flushed while the attempt runs, seeded into the next attempt and cleared on completion. Output is a sealed report of one attempt, published once, with its transition.

### External completion

`admin::complete_external` and `Client::complete_external` discard their payload on completion today, and storing it by default would expose callback bodies that may hold tokens or personal data. An external completion stores an output only when the caller passes one:

- Rust: `admin::complete_external_with_output(pool, callback_id, output, run_lease)` and `Client::complete_external_with_output(..)`, which also runs registered `Completed` specs as `complete_external` does.
- Python: `client.complete_external(callback_id, payload=None, *, output=None)`.
- HTTP: `POST /api/callbacks/{id}/complete` accepts an optional `output` field beside `payload`.

The value is validated against the same rules, and an invalid one fails the call before anything is written. `resolve_callback` stores no output; see [Open extensions](#open-extensions).

### Storage

Queue storage, in every substrate schema (migration v052, applied through the substrate installer):

- **Compact completions.** `receipt_completion_batches` gains `outputs jsonb[]`, aligned with `job_ids`. It is NULL when no job in the batch recorded an output, so a batch without outputs costs nothing. A check constraint keeps `outputs IS NULL OR cardinality(outputs) = completed_count`. Recording an output does not take a job off the compact path.
- **General-path completions.** `done_entries` gains a nullable `output jsonb` column, separate from `payload`, so the payload-elision rule never touches it. Jobs with tags, unique keys, custom metadata, errors or staged follow-ups already take this path.
- **Failed attempts.** The `output` key in the attempt's error entry, inside the existing payload.
- **Batch size.** A compact batch row is one value, and reading one job's output detoasts the whole array. The completion flush splits a batch once its recorded outputs exceed 256 KiB, so no batch row holds more than that plus one output.

`{schema}.terminal_jobs` gains a final `output` column: `done.output` on the general branch, `batch.outputs[item.ord]` on the compact branch. `load_job`, and so `get_job`, `list_jobs` and the HTTP API, read it into `JobRow.output`.

**Canonical** stores no output, successful or failed. It is deprecated in 0.7 and removed in 0.8 ([ADR-037](037-canonical-engine-deprecation.md)). A column on `awa.jobs_hot` would take an `ACCESS EXCLUSIVE` lock on the hot table and change the row type that `awa.insert_job_compat` returns, for an engine that is going away. A canonical worker that receives an output discards it and logs a warning once per kind.

**The `awa.jobs` compatibility view is not extended.** It selects from `awa.jobs_compat()`, whose `RETURNS TABLE` cannot gain a column in place, and `awa.insert_job_compat` returns its row type. SQL readers of queue storage query `{schema}.terminal_jobs`.

### Retention and erasure

An output lives exactly as long as its job is queryable: it is truncated with its ready slot when queue storage prunes, together with the job's args and payload. Deleting a terminal job removes a general-path output with the `done_entries` row. Deleting a compact-completed job writes a tombstone, which hides the job and its output from every read, but the bytes stay in the batch row until prune. The job's args have the same property: they stay in `ready_entries` until prune. Output therefore does not change what an operator must already know about erasing job data. Keep secrets and personal data out of outputs, as out of args.

### Visibility and testing

`UntypedJobEvent::Completed` carries the job row re-read after commit, so lifecycle hooks see the output. The admin UI's job detail shows it next to progress and errors. Outputs are never logged.

The test harnesses behave like the runtime:

- `awa-testing`'s `work_one` publishes the output with its own completion on queue storage, and in the error entry on failure.
- The `awa.testing.HandlerJob` stand-in gains `set_output` with the same validation, cap and seal, and exposes the value as `job.output`.
- Python's `AwaTestClient.work_one`, which finalizes canonical rows, exposes the sealed value on its result without storing it.

## Guarantees

For a job A whose attempt recorded output O:

- **Published with its own transition.** O is stored as A's output if and only if that attempt's own `Completed` finalization committed under its run lease. O appears in an error entry only if that attempt's own failure transition committed under its run lease. No output comes from an attempt that was rescued, superseded or completed externally, or whose transition rolled back.
- **One output per job.** A job row completes at most once, so at most one attempt's output is stored as the job's output.
- **Never on a dispatch path.** No scheduler, spec, dependency or follow-up reads an output.

Not guaranteed:

- **That O matches anything else.** A handler that writes the same value into a follow-up's args writes it twice.
- **An output for every failure.** Rescue, `RetryAfter` with attempts left and `Snooze` record none.
- **Retention beyond the job.** O goes when A is pruned or deleted.
- **Outputs on canonical.**

## Non-goals: not a results store for workflows

ADR-046's test applies: a feature crosses into workflow-engine territory if Awa must remember a relationship between jobs after the triggering transaction commits. An output is a value on one job, read by people and tools, and the engine never reads it. So:

- **B must not read A's output.** A value B needs travels in B's args through staging. A B that fetches A's output by ID recreates question 3: it fails once A is pruned, which is exactly the Celery and Oban Pro failure. The docs say so, and no API offers "the output of the job that enqueued me".
- **No output-driven follow-ups.** No `on_*_enqueue*` spec receives an output, and no predicate evaluates one. A seal-time mapper that derives follow-ups from the output can be written in user code on top of `OutputSlot` and staging; it is not core API.
- **Not on by default.** A job records an output only when its handler asks.

## Rollout

Migration v052 is additive. It adds two nullable columns per substrate schema, a check constraint, and a column appended to the `terminal_jobs` view. Each step is a catalog-only change; adding the check scans `receipt_completion_batches`, which holds only retained compact batches.

- **Migrate first.** N−1 binaries insert with explicit column lists and write NULL. They read `terminal_jobs` by name and ignore the new column.
- **Binary first.** A new binary probes once whether its schema has the output columns and caches a positive answer. Until it does, it re-probes at most every 30 seconds, and a recorded output is discarded with a warning rather than failing the completion. Health-gated workers already wait for the schema to reach their version.
- **Mixed fleets.** An output is stored only by binaries that record one. Nothing in N−1 reads or writes the columns.

## Relationship to other ADRs

- **ADR-046.** Staging remains the only way to hand a value to another job. The output slot shares the staging buffer's seal point and finalize-retry rule.
- **ADR-026.** Outputs ride the compact completion batch; the general path gains a column.
- **ADR-014.** Progress and output stay separate, as above.
- **ADR-036.** `JobRow.output`, `terminal_jobs.output` and the `output` key in error entries become stable surface.
- **ADR-037.** Canonical is not extended.
- **ADR-041.** v052 follows the additive path; the binary-first probe keeps the current binary operable before the migration.
- **ADR-042 (proposed).** When caller-owned completion ships, its completion function should accept an output.

## Validation

- **A TLA+ extension** of `AwaStagedFollowUps`: attempts record a nondeterministic output; `Commit` publishes it as the job's output; a failure transition publishes it in its own error entry; rescue publishes none. Invariants: a stored output traces to the committing attempt and equals what that attempt sealed; an output appears only on a completed job; an error entry's output belongs to the failed attempt that wrote the entry; every committed completion whose attempt sealed an output stored it. Negative configs, each expected to fail: write the output when it is set, before the transition commits; write it after the commit in a separate step; drop it on the batched path.
- **Integration tests on queue storage**: output stored on the compact and the general path; failed outputs in error entries across retries, the DLQ and DLQ retry; discard on snooze, `RetryAfter` and `WaitForCallback`; sealing; non-poisoning; external completion with and without an output; a stale completion stores nothing; canonical discards. The binary-first probe is tested against a schema without the columns.
- **Property tests**: any value either fails `set_output` or reads back equal through both terminal paths; the Python fuzz test extends to `set_output` and the `HandlerJob` stand-in.
- **Benchmark gates**: completion throughput and WAL with outputs of 100 B and 4 KiB on every job against the compact baseline, and the latency of `get_job` for a compact-completed job when batches are full of 4 KiB outputs.

## Consequences

### Positive

- "What did this job produce?" is answerable from the API, the admin UI and SQL, for the job's retained lifetime.
- Failed attempts keep what they produced, attached to the error that ended them.
- No public type changes shape beyond one field on a `#[non_exhaustive]` struct.
- A job that records an output keeps the compact completion path.

### Negative

- More surface to keep stable: an accessor, a column, an error-entry key and an HTTP field.
- A value that must be both inspected and handed on is written twice.
- Each retry rewrites the job's error history, so outputs on failed attempts grow the rewritten payload by up to 4 KiB per attempt. A job with 25 attempts can carry up to 100 KiB of failed outputs.
- Canonical users get nothing until they move to queue storage.
- Compact-completed outputs, like args, persist physically until prune after a delete.

## Alternatives considered

### Completed only

Store only the successful attempt's output and discard it on every failure. Simpler, and enough for "what did this job produce?". Rejected because the most useful output is often from the attempt that failed. Error entries make failure outputs nearly free and unambiguous.

### One output per job, last write wins across every transition

Publish the output on retries, snoozes and failures as well as completion, overwriting the previous value. Rejected:

- It is progress under another name, with the same carriage and survival across retries.
- It needs three stores (deferred rows, terminal rows and DLQ rows).
- After a rescue, the previous attempt's value survives and looks current.

### A column on `done_entries` or a key in the payload, for every output

One representation, no batch array. Rejected on cost: every job that records an output leaves the compact path (measured above).

### A side table keyed by job

A uniform point lookup and one shape for every terminal family, at about six times the WAL of the batch array. Kept as the fallback if large outputs or indexed lookups become a requirement.

### `metadata._awa_result`

Merge the output into metadata on read, as callback results are. Rejected: metadata belongs to the user, `_awa_callback_result` is a handler *input*, and reads would differ between storage families.

### Python return values as the output

Rejected as a default, because it silently changes existing handlers that return values today. Kept as an opt-in extension.

### Outputs read by specs, or derived follow-ups in core

Rejected by ADR-046 for dispatch: the type meets the spec only inside the completion transaction, and Python would run inside it. A mapper in user code keeps the topology in the handler without engine support.

### Record what the attempt staged

Make A's output list the follow-ups it inserted, with their IDs or "duplicate". That would answer "why is there no B?", but B's ID is known only after the insert, which means updating A's append-only row or reordering the completion's inserts and its lock order. Deferred; ADR-046's planned provenance key and deduplication metric answer most of it more cheaply.

## Open extensions

- **Python return values**: `@client.worker(..., output=True)` makes a handler's return value its output.
- **`resolve_callback`**: an opt-in to store the transformed callback payload as the output.
- **A side table** for outputs larger than 4 KiB or indexed lookup, if a use case appears.
- **Outputs for `RetryAfter` and `Snooze`**, by giving those transitions an attempt record.
