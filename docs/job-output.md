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
- **Rust conversion.** Values serialize as `serde_json` does, which turns a
  non-finite float into null: `set_output(&f64::NAN)` clears the output, and
  `json!({"ratio": f64::NAN})` records `{"ratio": null}`.
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

Reading `output` for one job of a compact batch reads the batch's whole
output array, up to 64 KiB. A SQL query that reads `output` across many
compact-completed jobs pays that once per row, so filter to the jobs you need
before selecting it.

A completion's output is stored as `json`, the text exactly as recorded, so it
reads back unchanged: `1.0` stays a float and key order is kept. In SQL, cast
it to `jsonb` for equality or containment (`output::jsonb @> '{"ok":true}'`).
A failed attempt's output lives in the job's `jsonb` error entries, which keep
numbers as `numeric`, as job args do: there `1.0` reads back as `1`.

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
- **Lifetime.** A completion's output lives as long as its job is retained,
  and is pruned with it. A failed attempt's output lives in that attempt's
  error entry: through failed-job retention, in the DLQ while the job is
  dead-lettered, and on a job that later completes, until that job is pruned.
  Each job keeps at most one failed output, the latest.
- **Deletion.** There is no call that removes an output and keeps its job.
  Deleting a job hides its output immediately, but the bytes stay on disk
  until the job's slot is pruned, as the job's args do. They also stay in WAL
  archives and backups. Keep secrets and personal data out of outputs.
- **Cost.** A job's whole lifecycle writes about 1.3 KB of WAL. A 100-byte
  output adds under 1%. A 4 KiB output that does not compress adds about
  4.4 KB, more than quadrupling it, and one that compresses well costs a small
  fraction of that. Large outputs with many elements, such as long numeric
  arrays or objects with hundreds of keys, also cut completion throughput by
  three to five times.

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
and reason, and once a minute for external completions.

## Testing

- **Rust.** `TestClient::work_one` runs the handler with a real output slot
  and stores what it records, as the runtime does. On canonical storage the
  output is discarded.
- **Python.** `HandlerJob` records outputs with the same conversion, limit and
  errors as a real job, so a unit test can assert on `job.output` after
  calling the handler. `AwaTestClient.work_one` returns the sealed value as
  `WorkResult.output`; the harness finalizes canonical rows, so
  `result.job.output` stays `None`.
