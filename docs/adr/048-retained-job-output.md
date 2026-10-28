# ADR-048: Retained job output

## Status

Proposed. Builds on [ADR-046](046-handler-staged-follow-ups.md) staged follow-ups and [ADR-026](026-narrow-terminal-history.md) narrow terminal history. Depends on `JobRow` becoming `#[non_exhaustive]` ([#564](https://github.com/hardbyte/awa/pull/564)). Adds migration v052, for queue storage only.

## Context

ADR-046 split a request for "metadata on `JobResult` or `JobRow`" into three questions:

1. How does A pass a value it computed into B reliably?
2. Can someone inspect A's result afterwards?
3. Does B depend on fetching A's stored result later?

Staging answers the first. Its answer to the third is no: B gets its value in its own args. This ADR answers the second.

Today a handler's return value is discarded. A Rust handler returns only an outcome. Python maps any unrecognised return value to `Completed`. `complete_external` drops its payload on completion. Operators who want to know what a job produced read logs, or the handler writes the value somewhere else.

Queues that keep a result do so for one of two reasons:

- **Inspection.** River's `RecordOutput` is opt-in, size-capped and pruned with the job. pg-boss stores an `output` column.
- **A declared workflow engine needs it.** Oban Pro workflows, Celery chords, Hatchet, Temporal and Step Functions.

Awa disclaims the second ([ADR-046, non-goals](046-handler-staged-follow-ups.md#non-goals-this-is-not-a-workflow-engine)). The failures those systems document come from the second use or from defaults:

- result stores bloated by default-on storage (Celery, BullMQ);
- downstream steps that fetch a result after it has expired: Celery chords whose body never runs, and Oban Pro's `{:error, :missing}`.

The requirement:

> A handler can record an optional, size-bounded JSON value describing what an attempt produced. It is published only with that attempt's own lease-guarded transition, readable for as long as the job is, and never read by Awa or by other jobs to decide what runs.

### Cost, measured

The deciding constraint on queue storage was the compact completion path of ADR-026, which writes one batch row for many jobs instead of a terminal row per job. These measurements, on Postgres 16 with fsync on and 512-job batches, are of the terminal write with a 100-byte value on every job:

| Representation | Keeps the compact path | Extra WAL per job | Completion statement throughput |
| --- | --- | --- | --- |
| None (today) | yes | 0 | 502k jobs/s |
| Array on the compact batch row | yes | +46 B | 295k jobs/s |
| Side table keyed by job | yes | +294 B | 251k jobs/s |
| Column on `done_entries`, or a key in the payload | no | +507 B | 174k jobs/s |

End to end, forcing a job onto the general terminal path costs about two thirds of throughput (11.2k down to 3.6–5.7k jobs/s) and adds 427 B of WAL per job.

A 100-byte value carried on the compact batch adds about 3.5% to a job's lifecycle WAL. A 2 KB value on every job roughly doubles it, which is why the value is capped. For comparison, staging one follow-up per job adds about 2,400 B of WAL per job.

Stored jsonb can be larger than the JSON text: a 4 KiB array of small numbers takes about 24 KiB as jsonb. Every bound below is therefore stated in text bytes, with headroom for that expansion. The benchmark gates in [Validation](#validation) include worst-case shapes.

## Decision

A handler may **record an output** for its attempt: a JSON value of at most 4 KiB of text. The output is attempt-scoped, like ADR-046's staging buffer, and is sealed with it when the handler returns. The transition that the attempt's own executor writes publishes it:

- **on completion**, it becomes the job's **output**;
- **on a failure** the executor records, it is kept in that attempt's **error entry**; at most one entry, the latest to record one, keeps an output.

Neither Awa nor any job reads it to decide what runs.

It is called *output* rather than *result*. `JobResult` is already the outcome enum, and every handler returns a `Result`.

### Rust surface

```rust
impl JobContext {
    /// Record this attempt's output, replacing any earlier one. Recording
    /// JSON null clears it. A failed call also clears it.
    pub fn set_output<T: Serialize + ?Sized>(&self, output: &T) -> Result<(), OutputError>;
    pub fn clear_output(&self);
    /// The output recorded so far.
    pub fn output(&self) -> Option<serde_json::Value>;
    /// A cloneable handle for futures that cannot borrow the context.
    pub fn output_slot(&self) -> OutputSlot;
}

#[derive(Clone)]
pub struct OutputSlot { /* Arc to the attempt's output slot */ }
impl OutputSlot {
    pub fn set<T: Serialize + ?Sized>(&self, output: &T) -> Result<(), OutputError>;
    /// After the handler returns this does nothing; `set(&())` reports
    /// `Sealed` for a caller that needs to know.
    pub fn clear(&self);
    pub fn get(&self) -> Option<serde_json::Value>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OutputError {
    /// The value would not serialize, or contains a NUL character.
    Invalid(String),
    TooLarge { size_bytes: usize, limit_bytes: usize },
    /// The handler already returned.
    Sealed,
}

pub const MAX_OUTPUT_BYTES: usize = 4 * 1024;

#[non_exhaustive]
pub struct JobRow {
    // ...existing fields...
    /// The completed attempt's output (queue storage). `None` for jobs that
    /// did not complete with one, and on list reads, which do not load it.
    /// A failed attempt's output is in its entry in `errors`.
    #[sqlx(default)]
    #[serde(default)]
    pub output: Option<serde_json::Value>,
}
```

For example, a handler that leaves a summary of what it did:

```rust
client_builder.register::<SyncInventory, _, _>(|args, ctx| {
    let output = ctx.output_slot();
    async move {
        let report = sync(&args).await?;
        if let Err(err) = output.set(&json!({ "updated": report.updated, "skipped": report.skipped })) {
            tracing::warn!(%err, "inventory summary not recorded");
        }
        Ok(JobResult::Completed)
    }
})
```

**Recording.** `set_output` serializes the value at once and checks its size and content, so nothing that can fail reaches the transition. Serialization runs outside the slot's lock.

**Failures do not poison the attempt.** Nothing depends on the output, so a missing one breaks no guarantee, unlike a missing follow-up.

**No automatic conversion to `JobError`.** `OutputError` deliberately has no `From` impl, so a bare `?` does not compile. A data-dependent `TooLarge` should not send a job to the DLQ after its side effects have run unless the handler chooses that explicitly. Staging differs on purpose: there, a missing follow-up does break a guarantee.

**A failed call clears the slot.** An earlier value left in place would look like the current one.

**Field rather than method.** `JobRow` gains a field because the row is the value that flows to HTTP, the admin UI, lifecycle events and Python. `#[non_exhaustive]` makes the field a non-breaking addition. `#[sqlx(default)]` and `#[serde(default)]` keep explicit-column queries and JSON from older servers working.

**Getters.** `ctx.output()` and `OutputSlot::get` let unit tests assert what a handler recorded. `ctx.job` stays the row as claimed, before the run.

### Python surface

```python
async def handle(job):
    report = await sync(job.args)
    job.set_output({"updated": report.updated, "skipped": report.skipped})

row = await client.get_job(job_id)
row.output            # {"updated": ..., "skipped": ...}, or None
```

**Methods.** `job.set_output(value)` records a value, `job.clear_output()` empties the slot, and `set_output(None)` clears it too. During dispatch, `job.output` returns the value recorded so far. On a row read from the database it returns the stored output.

**Conversion.** Values are converted to JSON when set, while the handler holds the GIL, by one converter shared with the test stand-in:

- pydantic models are dumped in JSON mode;
- dataclasses are converted at any depth;
- tuples become lists.

Sets, non-finite floats, non-string keys and integers outside 64 bits are rejected.

**Errors.** A value that cannot be recorded raises `awa.OutputError`, a subclass of `awa.TerminalError`, and clears the slot without poisoning the attempt. This differs from Rust on purpose: Python has no `Result` to ignore, so an uncaught error must be classified, and failing terminally is better than retrying the handler's side effects up to `max_attempts` times. Handlers whose output size depends on their data should record it before their side effects, or catch the error. `awa.MAX_OUTPUT_BYTES` is exported.

**Return values are unchanged.** A worker that returns a dict today has it discarded, so making return values the output would change existing handlers. An opt-in is listed under [Open extensions](#open-extensions).

### Lifetime of the output slot

The slot belongs to one attempt and is created with its context. It is **sealed when the handler returns**, at the same point as ADR-046's staging buffer. Later calls return `OutputError::Sealed`.

The finalizer owns the sealed value and lends it to every finalize retry, so a retry after a lost connection publishes the same value. A retried attempt starts with an empty slot.

### Where an attempt's output goes

The rule:

- **`Completed`** stores it as the job's output.
- **Any failure the attempt's executor records** keeps it in that failure's error entry: an error, a panic, `Cancel`, `RetryAfter`, exhaustion, poisoning, or a contract violation.
- **`Snooze` and `WaitForCallback`** discard it.
- **A process that never saw it**, meaning rescue, an admin action or an external completer, publishes none of it.

| Attempt ends with | Output |
| --- | --- |
| `Completed`, and the lease-guarded completion commits | Stored as the job's output |
| `Completed`, but the guard matches no row (rescued, cancelled, taken over or completed externally first) | Discarded with the transaction |
| `Completed` after a failed stage (ADR-046 poisoning) | Kept in the poisoned attempt's terminal error entry |
| `Err`, retryable or terminal, including retries exhausted | Kept in that attempt's error entry |
| A panic | Kept in the error entry the executor writes for it |
| `JobResult::Cancel` from the handler | Kept in the cancellation's error entry |
| `RetryAfter` | Kept in an error entry, `retry requested after Ns`. One is written when, and only when, an output was recorded; `RetryAfter` already marks the attempt as failed. A job with an error entry completes on the general path, so a polling handler that records an output on every `RetryAfter` gives up the compact path for its eventual completion |
| `WaitForCallback` without a registered callback | Kept in the contract-violation error entry |
| `Snooze`, `WaitForCallback` | Discarded and counted. Neither ends the attempt with a record; use progress ([ADR-014](014-structured-progress.md)) for state a later run should see |
| `ctx.wait_for_callback(...)` inside the handler, resumed, then `Completed` | Stored: the handler and its slot stay alive across the wait |
| Heartbeat, deadline or callback-timeout rescue; admin cancel; shutdown before finalization | None |

An error entry with an output is `{"error": ..., "attempt": ..., "at": ..., "output": ...}`.

**At most one entry keeps an output: the latest that recorded one.** An entry that carries an output strips `output` from the earlier entries in the row it is written to. A later entry without one, such as a rescue or an admin cancel, leaves the earlier output in place. Retries copy the error history into each new ready row, so keeping every attempt's output would grow retained bytes with the square of the attempt count; with one, the live row carries at most 4 KiB of failed output, and the append-only ready rows of earlier attempts keep their copies until prune, so retained bytes are linear in the attempt count. River keeps a single output per job too, overwritten by each attempt. Attempt numbers restart after a DLQ retry, so the entry is identified by its position, not its `attempt`.

**The dispatch row strips failed outputs.** The row a handler receives at dispatch has `output` removed from its error entries. A failed output is a report for people, not a checkpoint for the next attempt. Progress remains the sanctioned checkpoint: it is mutable, flushed while the attempt runs, seeded into the next attempt and cleared on completion.

### External completion

`complete_external` discards its payload on completion today. Storing it by default would expose callback bodies that may hold tokens or personal data. An external completion stores an output only when the caller passes one, through an options struct, so later options do not multiply signatures:

```rust
#[non_exhaustive]
#[derive(Default)]
pub struct ExternalCompletion { /* output, run_lease */ }
impl ExternalCompletion {
    pub fn new() -> Self;
    pub fn output(self, output: serde_json::Value) -> Self;
    pub fn run_lease(self, run_lease: i64) -> Self;
}
admin::complete_external_with(pool, callback_id, opts) -> Result<JobRow, AwaError>
admin::complete_external_with_in_tx(tx, callback_id, opts)
Client::complete_external_with(callback_id, opts)    // also runs registered Completed specs
```

- **Python:** `client.complete_external(callback_id, payload=None, *, output=None)` and `complete_external_sync` take the same `output` keyword. The `payload` is still not stored.
- **HTTP:** `POST /api/callbacks/{id}/complete` accepts an optional `output` field beside `payload`. An invalid one is a 400 and completes nothing.

**Validation.** The value goes through the same validation as `set_output`, and an invalid one fails the call before anything is written. A valid output that the engine cannot store (canonical, or a schema before v052) is dropped and counted, as a handler's would be, and the completion goes ahead: a webhook sender cannot recover from a refused completion.

**Compatibility.** An N−1 callback server ignores an unknown `output` field, so a mixed fleet behind one endpoint can drop it. The upgrade notes say so.

`resolve_callback` stores no output; see [Open extensions](#open-extensions).

### Storage

**Queue storage.** Migration v052 changes every substrate schema:

- **Compact completions.** `receipt_completion_batches` gains `outputs jsonb[]`, aligned with `job_ids` by the same `ORDER BY lane_seq, job_id`.
  - It is NULL when no job in the batch recorded an output, so a batch without outputs costs nothing.
  - A `NOT VALID` check constraint enforces `outputs IS NULL OR cardinality(outputs) = completed_count` on new rows without scanning old ones.
  - Recording an output does not take a job off the compact path.
- **General-path completions.** `done_entries` gains a nullable `output jsonb` column. It is separate from `payload`, so the payload-elision rule never touches it. Jobs with tags, unique keys, custom metadata, errors or staged follow-ups already take this path.
- **Failed attempts.** The output is the `output` key of the attempt's error entry, inside the existing payload. It needs no schema change and is kept even before v052.
- **Batch size.** A compact batch row is one value, and reading one job's output detoasts the whole array. The completion flush therefore starts a new batch row once the recorded outputs in a batch exceed 64 KiB of text. The rows are written in the flush's one transaction, so a flush still commits or fails as a whole.

Only the compact-receipt and general per-job completion paths carry outputs. The completion flush partitions its requests, so requests with a claim always take those paths even when another request in the same flush has none. Queue-storage dispatch always passes a claim today; a request that carries an output and no claim is completed without it and counted as `no_claim`.

The batch split happens before the flush's statement, so each run of jobs becomes its own statement and batch rows. Outputs are aggregated from the completed rows in the same `ORDER BY` as `job_ids`, so a job that drops out as stale shifts nothing.

**Reading.**

- `{schema}.terminal_jobs` gains a final `output` column: `done.output` on the general branch, and `batch.outputs[item.ord]` on the compact branch.
- The compact branch subscripts the array rather than unnesting it, so only matching rows detoast it.
- `get_job`, the admin UI's job detail and its HTTP endpoint read the column into `JobRow.output`.
- List reads (`list_jobs`, the jobs page, `GET /api/jobs`) do not load it, and their rows carry `None`.
- `UntypedJobEvent::Completed` carries the sealed output, set in memory rather than re-read, so lifecycle hooks see it without detoasting a batch array.
- Registered enqueue specs (ADR-029) receive the completed row with `output` cleared, so no spec can derive a follow-up from an output.

**Canonical** stores no output, successful or failed. It is deprecated in 0.7 and removed in 0.8 ([ADR-037](037-canonical-engine-deprecation.md)), and every 0.7 feature needs only one storage implementation.

- A canonical worker accepts `set_output` and discards the value at finalization, logging at most once per kind per minute and counting it.
- An output passed to `complete_external_with` is dropped and counted.

**The `awa.jobs` compatibility view is not extended.** It selects from `awa.jobs_compat()`, whose `RETURNS TABLE` cannot gain a column in place, and `awa.insert_job_compat` returns its row type. SQL readers of queue storage query `{schema}.terminal_jobs`.

### Retention and erasure

An output lives as long as its job is queryable. It is truncated with its ready slot when queue storage prunes, together with the job's args and payload. Failed jobs carried over by prune keep their error entries, including the retained failed output.

Deletion does not erase the bytes:

- **General path.** Deleting a terminal job deletes its `done_entries` row with the output. The tuple stays until vacuum.
- **Compact path.** Deleting a compact-completed job writes a tombstone. The tombstone hides the job and its output from every read, but the bytes stay in the batch row until prune.
- **Failed outputs.** These live in payloads that earlier ready rows also hold. They too stay until prune.

So a completion output's maximum physical residency is the retention window plus slot rotation, plus whatever WAL archives and backups keep. A failed output lives as long as the row that carries its error entry: through failed-job retention, through DLQ retention while dead-lettered, and through completed-job retention on a job that later completes. There is no call that erases an output and keeps its job.

Outputs often hold derived data, which can be more sensitive than the args that produced it. Keep secrets and personal data out of them. Erasing an output on delete is listed under [Open extensions](#open-extensions).

### Observability

Outputs are never logged, and `OutputError` messages name the type and position of a failure, not the value. Metrics:

- `awa.job.output_recorded`, a counter with the existing `awa.job.kind` and `awa.job.queue` attributes and `awa.job.outcome` (`completed` or `failed`);
- `awa.job.output_size`, a histogram of text bytes;
- `awa.job.output_dropped`, a counter with `awa.job.kind`, `awa.job.queue` and `awa.output.reason`: `canonical`, `schema_missing`, `outcome` (`Snooze` or `WaitForCallback`), `abandoned` or `no_claim`.

Invalid and oversized values never reach the slot; the handler sees the error.

Warnings for dropped outputs are rate-limited to one per kind and reason per minute, and external-completion drops to one per minute. The Grafana dashboard gains recorded and dropped panels. A sustained `schema_missing` rate means a binary is running ahead of its migration.

### Testing

The test harnesses behave like the runtime:

- **`awa-testing`'s `work_one` (queue storage).**
  - It publishes the output with its own completion, and in the error entry on failure. A retryable failure now writes its error entry, as the runtime does.
  - `WorkResult` carries the row re-read after finalization, so `WorkResult::Completed(row).output` is the stored output.
- **`awa.testing.HandlerJob`.**
  - It gains `set_output`, `clear_output` and a read-only `output` property.
  - It validates through the same converter and limits as the runtime, by calling into the Rust validator.
- **Python's `AwaTestClient`.** It finalizes canonical rows, so it exposes the sealed value as `WorkResult.output`, documented as harness-only. Moving it to queue storage is tracked separately.

## Guarantees

For a job A whose attempt recorded output O:

- **Published with its own transition.** O is stored as A's output if and only if that attempt's own `Completed` finalization committed under its run lease. O appears in an error entry only if that attempt's own failure transition committed under its run lease. No output comes from an attempt that was rescued or superseded, or whose transition rolled back.
- **One output per job.** A job row completes at most once. Its output is therefore either the sealed output of the attempt whose completion committed, or the value passed to the external completion that committed. An external completion that wins a race with a handler waiting in `ctx.wait_for_callback` stores its own output, if any, and the handler's later completion is stale.
- **Never on a dispatch path.** No scheduler, spec, dependency or follow-up reads an output, and the dispatch row carries none.

Not guaranteed:

- **That O matches anything else.** A handler that also writes the value into a follow-up's args writes it twice, and nothing compares the two.
- **An output for every attempt.** Rescue, admin actions, `Snooze` and `WaitForCallback` record none, and only the most recent failure keeps its output.
- **Retention beyond the job.** O goes when A is pruned. Deleting A hides O but does not erase its bytes.
- **Outputs on canonical.**

## Non-goals: not a results store for workflows

[ADR-046's test](046-handler-staged-follow-ups.md#non-goals-this-is-not-a-workflow-engine), verbatim: "A feature crosses into workflow-engine territory if it requires Awa to remember a relationship between jobs after the triggering transaction commits: a join, a step's result that the engine or another job reads, a chain's status, or a compensation."

An output is a value on one job. People and tools read it; neither the engine nor another job does. Several rules follow:

- **B must not read A's output.** A value B needs travels in B's args, through staging. A B that fetches A's output by ID recreates question 3: it fails once A is pruned, which is exactly the failure Celery and Oban Pro document. No API offers "the output of the job that enqueued me", there is no typed accessor, and the docs say so.
- **No output-driven follow-ups.** No `on_*_enqueue*` spec receives an output, and no predicate evaluates one. A seal-time mapper that derives follow-ups from the output can be written in user code on top of `OutputSlot` and staging. It is not core API.
- **No checkpoints across attempts.** Handlers do not see failed outputs at dispatch, so an attempt cannot resume from the previous attempt's output. That is what progress is for, and progress is cleared on completion and on DLQ retry.
- **Not on by default.** A job records an output only when its handler asks. Each open extension needs its own ADR that passes the test above.

## Rollout

Migration v052 first redefines the v023 installer, without running it, so fresh installs and `prepare_schema` create the new columns. A targeted tail then updates each schema that `awa.queue_storage_schemas()` discovers. It does not replay the installer.

The tail, per schema:

- adds the two nullable columns, each guarded by a catalog check so a re-run takes no lock;
- adds the `NOT VALID` check;
- replaces the `terminal_jobs` view.

**Locking.** Adding a column to a partitioned parent takes an `ACCESS EXCLUSIVE` lock on the parent and every partition, and replacing the view takes one on the view, even on a re-run. The runner applies the whole pending range in one transaction, so these locks are held until it commits; while they are held, completions and `terminal_jobs` reads wait. The helper locks `terminal_jobs` before its tables, the order readers take them, so a read that starts mid-migration waits rather than deadlocking. When no `lock_timeout` is set, the tail sets 2 s and restores the previous value afterwards, as v050 does, so a busy system fails the migration fast instead of stalling the fleet. A timeout or deadlock rolls back every pending migration, which can then be re-run. Nothing is rewritten or scanned.

**Migrate first.** N−1 binaries insert with explicit column lists, so they write NULL. They read `terminal_jobs` by explicit column, so they ignore the new one. They do not strip failed outputs from the row they dispatch, so while versions are mixed an N−1 worker that claims a retry sees the previous attempt's output in `errors`.

**Binary first.**

- A new binary checks the catalog of its own schema for both columns, so a custom schema that the migration skipped is handled correctly. A positive answer is cached; a negative one is re-checked at most every 30 seconds, so the binary picks the columns up without a restart. Until then it neither writes nor reads them: its terminal reads leave `output` out.
- Completion outputs are dropped and counted as `schema_missing`. Outputs in error entries need no schema change and are kept.
- Startup is unchanged. `/readyz` reports not ready until the migration runs, as for any version bump, so binary-first applies to deployments that do not gate on readiness.

**Mixed versions.** v052 adds nullable columns and changes no representation or authority, so [ADR-041](041-rolling-upgrade-policy.md)'s expand, flip and contract steps do not apply. A job without an output runs byte-for-byte the statements N−1 binaries run: the output columns are named only when an output is present. The integration suite runs every existing terminal path against a v052 schema, and the binary-first tests run the new binary against a v051 schema.

## Relationship to other ADRs

- **ADR-046.** Staging remains the only way to hand a value to another job. The output slot shares the staging buffer's seal point and finalize-retry rule. ADR-046 sketched a starting contract in which a failed attempt's value is never visible and the value is deleted with the job. This ADR departs from both, for the reasons in [Where an attempt's output goes](#where-an-attempts-output-goes) and [Retention and erasure](#retention-and-erasure): a failed attempt's output is kept in its own error entry, never as the job's output, and deletion hides an output while its bytes stay until prune.
- **ADR-026.** Outputs ride the compact completion batch; the general path gains a column.
- **ADR-014.** Progress and output stay separate, as above.
- **ADR-036.** These join the stable surface in 0.7:
  - the Rust and Python APIs: `set_output`, `clear_output`, `output`, `OutputSlot`, `OutputError`, `MAX_OUTPUT_BYTES`, `JobRow.output`, `ExternalCompletion`;
  - the `output` column of `{schema}.terminal_jobs`, part of the covered terminal read surface;
  - the `output` field of the HTTP callback body and of the job-detail response;
  - the metric names and attribute keys above.

  The `outputs` array on compact batches and the `output` column of `done_entries` are internal, as all storage tables are. The `output` key in error entries is documented but best-effort, like the rest of the error-entry shape, which `docs/stability.md` does not list.
- **ADR-037.** Canonical is not extended.
- **ADR-041.** v052 is additive, and the binary-first behaviour keeps the current binary operable before the migration.
- **ADR-042 (proposed).** When caller-owned completion ships, its completion function should accept an output.

## Validation

- **A TLA+ model**, `correctness/storage/AwaJobOutput.tla`.
  - **What it models.** Attempts record a nondeterministic output, then complete or fail under a lease guard. Completion is sent on the batched or direct path, with lost acknowledgements and finalize retries. Rescue, reclaim, and external completion with or without an output are modelled too.
  - **Invariants.**
    - A stored output traces to the committing attempt and equals what it sealed, or to the external completion.
    - Only completed jobs have an output.
    - An error entry's output belongs to the failed attempt that wrote it.
    - Every committed completion whose attempt sealed an output stored it.
  - **Negative configs**, each expected to fail:
    - write the output when it is recorded;
    - write it after the commit in a separate step;
    - drain it on the first finalize send;
    - drop it on the batched path;
    - complete without the lease guard.
- **Integration tests on queue storage.**
  - Every existing terminal path still passes against a v052 schema, and a new binary runs against a v051 schema.
  - Output is stored on the compact and the general path.
  - Failed outputs land in error entries:
    - only the latest entry to record one keeps it, across retries, rescue, the DLQ and DLQ retry;
    - they are stripped from the dispatch-time row.
  - Discard on snooze and `WaitForCallback`, and an entry on `RetryAfter`.
  - Sealing, clearing on failure and on null, and non-poisoning.
  - External completion with and without an output.
  - A stale completion stores nothing.
  - Canonical discards.
  - Reads and writes against a schema without the columns.
  - List reads omit outputs.
- **Property tests.** Any value either fails `set_output` or reads back equal through both terminal paths. The Python fuzz test extends to `set_output` and the `HandlerJob` stand-in.
- **Benchmark gates.**
  - **Throughput and WAL** of completions with 100 B and 4 KiB outputs on every job, against the compact baseline, including a numeric-array and a many-key worst case.
  - **Read latency:** `get_job` for a compact-completed job when batches are full of 4 KiB outputs.
  - **Retry-storm WAL:** every attempt fails with an output.

## Consequences

### Positive

- "What did this job produce?" can be answered from the API, the admin UI and SQL for as long as the job is retained.
- The most recent failed attempt keeps what it produced, attached to the error that ended it.
- No public type changes shape beyond one field on a `#[non_exhaustive]` struct.
- A job that records an output keeps the compact completion path.

### Negative

- **More surface to keep stable:** an API on the context, a row field, an options struct and an HTTP field.
- **A value that must be both inspected and handed on is written twice.**
- **Larger compact batches cost more to read.** With large outputs a compact batch holds fewer jobs: as few as 17 at the 4 KiB cap. Reading one output still detoasts its batch's outputs: about 64 KiB of text, and up to several times that as jsonb.
- **Earlier failures lose their outputs.** Only the latest entry to record one keeps it.
- **Canonical users get nothing** until they move to queue storage.
- **Deleted outputs persist** physically until prune, as args do.

## Alternatives considered

### Completed only

Store only the successful attempt's output. That is simpler, and enough for "what did this job produce?". Rejected because the most useful output is often from the attempt that failed.

### Keep every failed attempt's output

Rejected on cost. Retries copy the payload into each new ready row, and those rows are append-only until prune, so retained bytes grow with the square of the attempt count. At the default 25 attempts that is over a megabyte per job before jsonb expansion.

### One output per job, last write wins across every transition

Publish the output on retries, snoozes and failures as well as on completion, each overwriting the last. Rejected:

- It is progress under another name: it is carried the same way and survives retries the same way.
- It needs three stores: deferred rows, terminal rows and DLQ rows.
- After a rescue, the previous attempt's value survives and looks current.

### A column on `done_entries` or a key in the payload, for every output

One representation and no batch array. Rejected on cost: every job that records an output would leave the compact path (measured above).

### A side table keyed by job

This gives a uniform point lookup and one shape for every terminal family, at about six times the WAL of the batch array. It is kept as the fallback if outputs larger than 4 KiB become a requirement.

### `metadata._awa_result`

Merge the output into metadata on read, as callback results are. Rejected:

- metadata belongs to the user;
- `_awa_callback_result` is a handler *input*;
- reads would differ between storage families.

### `?` converts `OutputError` into a terminal job error

This would match `StageError`. Rejected: a data-dependent size failure would send jobs to the DLQ after their side effects, for a value nothing depends on.

### A marker instead of an error

Store `{"_awa": "output_too_large", ...}` and return `Ok`. Rejected: it puts a reserved shape into user outputs and hides the failure from the handler. The metric and the cleared slot make the failure visible instead.

### Python return values as the output

Rejected as a default, because it would silently change existing handlers that return values today. Kept as an opt-in extension.

### Outputs read by specs, or derived follow-ups in core

Rejected by ADR-046 for dispatch. The type meets the spec only inside the completion transaction, and Python would run inside it. A mapper in user code keeps the topology in the handler without engine support.

### Record what the attempt staged

A's output would list the follow-ups it inserted, with their IDs or "duplicate", which would answer "why is there no B?". But B's ID is known only after the insert, so this would mean updating A's append-only row, or reordering the completion's inserts and its lock order. Deferred: ADR-046's planned provenance key and deduplication metric answer most of the question more cheaply.

## Open extensions

Each needs its own ADR that passes ADR-046's test.

- **Python return values**: `@client.worker(..., output=True)` would make a handler's return value its output.
- **Callback outputs**: a registration-time `store_output` option on the callback, covering `complete_external`, `resolve_callback` and its transformed payload, and the HTTP endpoint alike.
- **Erasure on delete**: deleting a compact-completed job also sets its element of `outputs` to NULL.
- **Larger outputs** in a side table, if a use case appears.
