> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# Claims, receipts and leases

A claim hands one attempt of a job to one worker. AWA records that ownership with as little storage as the attempt needs: most attempts only append rows, and only an attempt that waits for an external callback gets a row that is updated in place.

## `run_lease`: the attempt's number

Every claim increments the job's `run_lease`. Every later write for that attempt (heartbeat, progress, completion, retry, snooze, cancel, callback) names the job and its `run_lease`, and is applied only if that attempt is still open. A write for a closed attempt changes nothing.

<div class="awa-diagram" markdown>
![Worker A's late finish for run_lease 7 is rejected after rescue closed it, while worker B finishes run_lease 8](../../assets/how-it-works/run-lease-fencing.svg)
</div>

This is what makes rescue safe. Worker A in the diagram is not dead; it is paused, partitioned or slow. When it wakes and tries to finish, the attempt it names has already been closed and the job belongs to a newer attempt, so its result is discarded.

`run_lease` protects the database, not the outside world. Anything worker A did before it stalled, such as sending an email or charging a card, has still happened. Make those effects idempotent; [transactional enqueue](../transactional-enqueue/index.md) and the [failure lab](../failure-lab/index.md) show how.

## What an attempt writes

<div class="awa-diagram" markdown>
![Rows written by one attempt: a receipt at claim, attempt state on heartbeat and progress, a lease while waiting for a callback, and terminal history at finish](../../assets/how-it-works/attempt-records.svg)
</div>

**Claim receipt.** The claim appends one receipt row per claim batch to the current slot of the claim ring, listing every job, `run_lease` and receipt id in the batch. It is never updated. When an attempt ends, a closure row is appended next to it; an attempt is open while its receipt has no closure.

**Attempt state.** The first heartbeat or progress update creates a small row for the attempt, and later ones update it. It holds the heartbeat time and the latest progress, and is deleted when the attempt finishes. A job that finishes before its first heartbeat never creates one.

**Lease.** An attempt that registers a callback and waits for it needs state that changes while no handler is running: the callback id, its timeout and the waiting state. That attempt gets a lease row in the lease ring, which the callback or a timeout later resolves. With `lease_claim_receipts` turned off, every claim writes a lease instead of a receipt.

**Terminal history.** Finishing appends the outcome to the queue ring, in the same slot as the job's ready row. Successful short jobs share one compact row per completion batch. Read finished jobs through `terminal_jobs` or the admin API, not the physical tables.

The common path, claim then finish, therefore only appends rows to partitions that are later truncated whole. That keeps the hot tables free of dead rows; see [rings](../rings/index.md).

## Heartbeats

While a handler runs, its worker heartbeats each attempt it holds, every `heartbeat_interval` (30 s by default). Heartbeats come from the worker's runtime, not from the handler, so a handler that blocks or computes for a long time still keeps its attempt alive. A heartbeat proves the process is alive; it does not extend the queue's `deadline_duration`, which bounds the attempt however healthy the worker is.

## Rescue

The maintenance leader closes attempts that can no longer finish on their own:

| Rescue | Triggers when | Default |
| --- | --- | --- |
| Stale heartbeat | the attempt's last heartbeat, or its claim if it never heartbeated, is older than `heartbeat_staleness` | 90 s |
| Deadline | the attempt has run longer than the queue's `deadline_duration` | per queue |
| Callback timeout | a callback wait passed its timeout | per callback |

Rescue closes the attempt and then sends the job down the normal path: back to deferred jobs for a retry if attempts remain, otherwise to `failed` or the dead-letter queue. The rescued attempt counts against `max_attempts`, which is how a job that crashes its worker every time eventually stops; see [poison jobs](../job-lifecycle/index.md#poison-jobs).

Rescue takes the same per-attempt lock as completion, so an attempt that finishes at the moment rescue examines it is either completed or rescued, never both.

## When a worker loses the database

A worker that finished a handler but cannot reach the database keeps the result and retries the write for up to two minutes. If the connection comes back, the job completes without running again. If rescue closed the attempt in the meantime, the saved result is discarded by the `run_lease` check like any other stale write.

## Related

- [Lanes and claiming](../lanes-and-claiming/index.md): which job a claim picks.
- [Rings: rotation and prune](../rings/index.md): when receipts, leases and terminal rows are reclaimed.
- [Configuration: reliability timings](../../configuration/index.md#reliability-timings-heartbeat-deadline-rescue): heartbeat, deadline and rescue settings.
- [ADR-013](../../adr/013-run-lease-and-guarded-finalization/index.md) and [ADR-023](../../adr/023-receipt-plane-ring-partitioning/index.md): the design records.
