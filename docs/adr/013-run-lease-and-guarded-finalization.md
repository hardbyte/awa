# ADR-013: Durable Run Leases and Guarded Finalization

## Status

Accepted

## Context

The earlier completion contract allowed a stale worker to finalize the wrong
running attempt if a job had been rescued, reclaimed, and started again before
the old completion reached Postgres.

That made completion correctness too weak for further optimization work,
especially batching. A batched finalizer is only safe if the database can
distinguish "the current running attempt" from "an old completion arriving
late".

The TLA+ models already treated running attempts as lease-bearing identities,
but the Rust runtime still needed a durable database-level token to match that
model.

## Decision

Add a monotonic `run_lease` column to running job rows and require all
heartbeat/finalization/callback state transitions to match on:

- `id`
- `state = 'running'`
- `run_lease`

The runtime increments `run_lease` on every claim to `running`. Local in-flight
state is keyed by `(job_id, run_lease)`.

### Finalization Rule

Finalization succeeds only if the current row still matches the claiming
attempt's lease. If the guarded update affects zero rows, the result is stale
and must be discarded.

### Batched Completion

`Completed` outcomes may be flushed in batches, but local in-flight tracking and
capacity are not released until the batch flush acknowledges either:

- successful guarded transition, or
- stale rejection (`rows_affected = 0`)

This keeps shutdown drain, heartbeat, and stale-completion safety aligned.

## Consequences

### Positive

- Prevents stale completions from mutating a newer running attempt
- Aligns the Rust runtime with the existing correctness model
- Makes lease-safe batched completion possible
- Allows heartbeat and cancellation to address exact running attempts

### Negative

- Every claim now mutates an additional column
- Runtime state is slightly more complex because local tracking is per-attempt
  rather than per-job
- Any code path that touches running jobs must propagate the lease token

## Notes

`attempt` is not used as the safety guard because it has ABA holes: snooze and
admin retry can reset or decrement it. `run_lease` is monotonic and only
advances on a new running claim, which makes it suitable as the durable attempt
identity.

## Relationship to ADR-019

The `run_lease` guard decision carries into queue storage unchanged. Under
ADR-019, `run_lease` becomes the second component of the composite key
`(job_id, run_lease)` identifying `{schema}.active_leases` and
`{schema}.attempt_state` rows. Every heartbeat, flush-progress,
register-callback, complete, and rescue path still matches on both columns;
stale completions are rejected because the UPDATE matches zero rows when
the lease row has been deleted or replaced. The `AwaSegmentedStorageRaces`
TLA+ model checks that stale `run_lease` commits cannot land a lease in a
retired segment, and `AwaSegmentedStorageTrace` replays concrete test
sequences to validate the guard end-to-end. See
[ADR-019](019-queue-storage-redesign.md).
