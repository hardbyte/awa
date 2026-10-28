# Job output

A handler can record an **output**: a small JSON value describing what its
attempt produced, kept for people and tools to inspect. If the attempt
completes, it becomes the job's output. If the attempt fails, it is kept in
that failure's entry in `errors`. Neither Awa nor any other job reads it.
The design is [ADR-048](adr/048-retained-job-output.md).

Outputs are stored on [queue storage](queue-storage-substrate.md) only. The
deprecated canonical engine accepts them and discards them.

## Recording an output

Rust:

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

A `Worker` impl can call `ctx.set_output(&value)` directly. `ctx.output()`
returns what has been recorded so far, and `ctx.clear_output()` removes it.

Python:

```python
@client.task(SyncInventory, queue="inventory")
async def sync_inventory(job):
    report = await sync(job.args)
    job.set_output({"updated": report.updated, "skipped": report.skipped})
```

### Rules

- **Last write wins.** Recording again replaces the earlier value.
- **Null clears it.** Recording JSON null (`None` in Python) clears the
  output.
- **Size limit.** The value is serialized when you record it and may be at
  most `MAX_OUTPUT_BYTES` (4 KiB) of compact JSON text.
- **Python conversion.** Pydantic models are dumped in JSON mode, and
  dataclasses and tuples are converted. Sets, non-finite floats, non-string
  keys and integers outside 64 bits are rejected.
- **A failed recording clears the output and does not fail the attempt.**
  - In Rust, `OutputError` deliberately has no conversion into `JobError`, so
    a bare `?` does not compile. Decide whether a missing output should fail
    the job.
  - In Python, `awa.OutputError` is a `TerminalError`. Uncaught, it fails the
    attempt without retrying it. If the size of an output depends on your data,
    record it before your side effects, or catch the error.
- **Sealed when the handler returns.** Later calls fail with
  `OutputError::Sealed`.

## Where an attempt's output goes

| The attempt ends with | Output |
| --- | --- |
| `Completed` | Stored as the job's output |
| An error, a panic, `Cancel`, `RetryAfter`, retries exhausted, or a failed follow-up stage | Kept in that attempt's entry in `errors`, under `output` |
| `Snooze`, `WaitForCallback` | Discarded |
| Rescue after a lost heartbeat or deadline, an admin cancel, shutdown before finalization | None; that process never saw it |

Only one error entry keeps an output: the latest one to record it. An entry
that carries an output removes `output` from earlier entries in the same
row. A later entry without one, such as a rescue, leaves the earlier output
in place.

The row a handler receives when it is dispatched has outputs removed from its
`errors`. An attempt therefore cannot resume from the previous attempt's
output; use [progress](https://github.com/hardbyte/awa/blob/main/docs/adr/014-structured-progress.md) for
state the next run should see.

## Reading outputs

- **Rust:** `JobRow.output` is set on rows from `get_job`, and on the job in
  `JobEvent::Completed`.
- **Python:** `job.output` is set on jobs from `client.get_job`.
- **Admin UI and HTTP:** the job detail view and `GET /api/jobs/{id}` show the
  output.
- **SQL:** read the `output` column of `{schema}.terminal_jobs`. The
  `awa.jobs` compatibility view does not carry it.

List reads (`list_jobs`, the jobs page, `GET /api/jobs`) do not load outputs,
and their rows carry `None`. A failed attempt's output is in
`job.errors[i]["output"]`.

Outputs are stored as `jsonb`, which keeps numbers as `numeric`. A float with
no fractional part, such as `1e19`, therefore reads back as the integer of the
same value. Job args behave the same way.

## External completion

`complete_external` does not store its payload. To store an output when
completing a job from outside, pass it explicitly:

```rust
client
    .complete_external_with(callback_id, ExternalCompletion::new().output(json!({ "approved": true })))
    .await?;
```

```python
await client.complete_external(callback_id, output={"approved": True})
```

Over HTTP, `POST /api/callbacks/{id}/complete` takes an optional `output`
field beside `payload`.

An invalid output fails the call and completes nothing. A valid output that
the engine cannot store (canonical, or a schema before migration v052) is
dropped and counted, and the job still completes. Callback servers older
than 0.7 ignore the `output` field.

## Not for passing values between jobs

A job must not read another job's output. Outputs are pruned with their job,
so a follow-up that fetches its parent's output by ID fails once the parent
is gone. To hand a value to a follow-up, put it in the follow-up's args with
[`enqueue_on_completion`](lifecycle-hooks.md#handler-staged-follow-ups).
Registered enqueue specs receive the completed row without its output.

## Storage, retention and cost

- **Compact completions.** An output rides the same compact batch row as the
  rest of the completion, so recording one keeps the fast completion path.
  A batch row starts afresh once its outputs pass 64 KiB.
- **Lifetime.** An output lives as long as its job is retained, and is pruned
  with it.
- **Deletion.** Deleting a job hides its output immediately, but the bytes
  stay on disk until the job's slot is pruned, as the job's args do. They
  also stay in WAL archives and backups. Keep secrets and personal data out
  of outputs.
- **Cost.** The cost is roughly the output's size in WAL per job. A 100-byte
  output adds a few percent to a job's lifecycle WAL; a 4 KiB one on every
  job roughly doubles it.

## Metrics

| Metric | Attributes |
| --- | --- |
| `awa.job.output_recorded` | `awa.job.kind`, `awa.job.queue`, `awa.job.outcome` (`completed` or `failed`) |
| `awa.job.output_size` | the same, as a histogram of JSON text bytes |
| `awa.job.output_dropped` | `awa.job.kind`, `awa.job.queue`, `awa.output.reason` |

The `awa.output.reason` values:

- `canonical`;
- `schema_missing`, for a binary running ahead of migration v052;
- `outcome`, for `Snooze` or `WaitForCallback`;
- `abandoned`, for a handler cut off past its deadline, or a shutdown that
  ends before the attempt is finalized;
- `no_claim`.

Warnings about dropped outputs are logged at most once a minute for each kind
and reason.
