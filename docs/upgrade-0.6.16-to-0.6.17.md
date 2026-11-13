# Upgrading from 0.6.16 to 0.6.17

0.6.17 is a rolling patch upgrade: the schema stays at v40 and traffic keeps running throughout. Claims commit asynchronously and reuse the runtime's claimer lease, promotion and rescue run in their own maintenance loop, a runtime can opt out of maintenance leadership, and queue prune no longer runs its checks under an exclusive lock.

Binaries and the migration can roll in either order. One response shape changes for callers of the admin API and `admin::state_counts`.

## Upgrade

1. Install the 0.6.17 CLI.
2. Run `awa migrate` once with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   On a 0.6.16 database the output reports `Schema patch runtime_maintenance_eligibility: applied` and `Schema patch correlated_claim_tombstones: applied`. Each patch commits in its own transaction and neither takes a lock on an existing `awa` or queue-storage table.

3. Grant runtime roles `SELECT, INSERT, DELETE` on `awa.runtime_maintenance_eligibility`. The default privileges in the [security guide](security.md) already cover it for roles set up that way.
4. Roll workers, producers and admin processes to 0.6.17, in any order.
5. Update callers of `/api/stats` and `admin::state_counts` for the [new shape](#state-counts-report-whether-available-is-an-estimate).

0.6.16 binaries ignore the new table and run unchanged against the patched claim function.

### External migration runners

`awa migrate --sql --pending` prints `R__runtime_maintenance_eligibility` and `R__correlated_claim_tombstones` on a 0.6.16 database. Apply them as repeatable scripts after `V40`; `R__correlated_claim_tombstones.sql` must run as the owner of the claim functions. Extract the full set with:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate --extract-to ./sql/awa --from 40
```

Replace all `R__` files together with that set and keep previously applied `V` files unchanged.

## Claims commit asynchronously

This part needs no migration and applies to each queue-storage runtime as it rolls. Queue-storage claim transactions and the claim-cursor advance that follows them run with `synchronous_commit = off`, set with `SET LOCAL` in those transactions only. Enqueues, completions and every other transaction stay synchronous, and so do claims on the canonical backend, including during a storage transition.

What changes:

- A claim acknowledged to a worker can be lost if the server crashes before its WAL is flushed, or if a standby is promoted before it received it. The job is then claimable again, which is at-least-once delivery as before.
- A worker still running a claim lost this way is not fenced by its run lease, because the job's next claim reuses it. See [architecture](architecture.md#enqueue-and-claim).
- Throughput rises where WAL flushes were the limit. Against 0.6.16 on PostgreSQL 18 with `synchronous_commit = on`, three interleaved rounds gave 19 to 21% more jobs per second at 4, 16 and 64 workers, and end-to-end p99 at 800 jobs/s fell from 31 ms to 24 ms. PostgreSQL used about a quarter more CPU for the extra work.

A queue-storage claim also reuses the runtime's claimer lease for up to 250 ms instead of reading it on every call. No configuration changes either behaviour.

## Promotion and rescue have their own loop

The maintenance leader runs promotion and the three rescues in a loop of their own, so a slow cleanup, cron sync, rollup or rotation no longer delays them. A leader holds one more pool connection at a time. While a rescue pass runs, lease prune may report more `blocked` outcomes and claim prune may wait out a rescue batch. See [ADR-051](adr/051-maintenance-recovery-loop.md).

## Opting a runtime out of maintenance leadership

`ClientBuilder::maintenance_leader(false)` (Python `client.start(..., maintenance_leader=False)`) keeps a runtime from contending for leadership. It needs the `runtime_maintenance_eligibility` patch to report itself. Register `Rescued` hooks and enqueue specs on a runtime that may lead, because only the leader's registrations fire. See [leader election](configuration.md#leader-election).

## Ring prune lock waits

Queue prune takes `SHARE ROW EXCLUSIVE` on the sealed slot's partitions, runs its checks under it, and requests `ACCESS EXCLUSIVE` only for the `TRUNCATE`. For the queue and claim rings that request waits `prune_lock_timeout`, then retries with twice the wait up to 320 ms before the attempt counts as `blocked`.

What changes for operators:

- A reader of a sealed slot's partitions can wait up to 320 ms behind a contended prune, where it waited 10 ms.
- A prune that gets the lock on the queue ring no longer stalls readers for the duration of its checks.
- A deadlock that PostgreSQL resolves by cancelling a prune counts as `blocked` in `awa.maintenance.prune.attempts`, not `error`.
- Startup warns when 320 ms (or a longer `prune_lock_timeout`) reaches `deadlock_timeout`, or when eight times that reaches `statement_timeout`. Keep `deadlock_timeout` at its 1 s default or above.

With a large backlog of abandoned claims, prune is still blocked in most attempts, as on 0.6.16; rescue has to drain the backlog first.

## State counts report whether `available` is an estimate

The per-queue `available` count reads at most 5,000 unclaimed rows per lane. A deeper lane reports its head-sequence depth and marks the count as an estimate.

- `admin::state_counts` returns `StateCounts { counts, available_is_estimate }`, where it returned the state map.
- `/api/stats` returns `{ "counts": {...}, "available_is_estimate": bool }`, where it returned the bare state map.
- `QueueOverview` gains `available_is_estimate`; the UI shows `≈N` and `awa queue stats` shows `~N`. While a queue's count is an estimate, its `lag_seconds` is a lower bound.

## Python handlers are cancelled when their attempt is abandoned

A Python handler receives `asyncio.CancelledError` when the executor abandons its attempt after the deadline grace, or when shutdown exceeds the worker timeout. Release resources in `finally`. Cancellation is cooperative: it does not stop blocking code, threads or handlers that suppress it. Shutdown waits up to one more second for handlers to finish cancelling.

## Rollback

Roll binaries back to 0.6.16 in any order. Leave both patches in place: 0.6.16 ignores `awa.runtime_maintenance_eligibility` and runs against the patched `claim_ready_runtime`. Runtimes started with `maintenance_leader(false)` contend for leadership again once they run 0.6.16.
