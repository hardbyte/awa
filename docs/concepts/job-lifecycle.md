# Job lifecycle

A job moves through durable states in PostgreSQL. The handler returns the next outcome; AWA validates that the current worker still owns the claim before applying it.

<div class="awa-diagram" markdown>
![AWA job lifecycle from available through execution to completion, retry, callback wait, cancellation, or failure](../assets/job-lifecycle.svg)
</div>

## Common paths

`available → running → completed`
: The normal path. Claiming increments the attempt before the handler runs.

`running → retryable → available`
: A retry records the error and a future run time. Once due, the job is runnable again if attempts remain.

`running → scheduled → available`
: A snooze defers the same attempt. It is useful for polling an external system without consuming the retry budget.

`running → waiting_external → available`
: The handler waits for an external callback. Resolving the callback makes the job runnable with the persisted callback result.

`running → failed | cancelled`
: Terminal outcomes. Failed jobs can be retained in the dead-letter queue according to policy.

## What crash recovery means

A running job is owned only for as long as its claim remains live. Heartbeats extend that claim. If the process stops, another worker can rescue the job after the claim expires. The original worker may still finish late, so completion writes are guarded by ownership and handlers must tolerate duplicate execution.

## Where to go next

- Configure attempts, delays, timeouts, and queue concurrency in [Configuration](../configuration.md).
- Design idempotent effects with [Transactional enqueue](transactional-enqueue.md).
- Inspect and redrive terminal failures with the [Dead-letter queue](../dead-letter-queue.md).
- Use [Callbacks](../http-callbacks.md) for externally completed work.
