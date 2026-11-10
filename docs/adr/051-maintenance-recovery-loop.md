# ADR-051: Run promotion and rescue in their own maintenance loop

## Status

Accepted. Implements [#303](https://github.com/hardbyte/awa/issues/303) on the 0.6 line.

Numbered 051 so it does not collide with ADR-038 to ADR-050 when it is forward-ported to `main`.

## Context

The maintenance leader ran every branch from one `tokio::select!`, and awaited each branch's body inside its arm. While one body waited, no other arm was polled. Four of those branches are what users wait on:

- promotion of scheduled and retryable jobs;
- stale-heartbeat rescue;
- deadline rescue;
- callback-timeout rescue.

The rest are hygiene: cleanup and retention, cron sync, queue statistics, admin metadata, priority aging, batch operations, terminal-count rollup, and ring rotation and prune.

0.6 shipped observability for this (#242): `awa.maintenance.branch.duration` and `awa.maintenance.branch.overrun`. It deferred the split until fleet telemetry showed overruns.

Two things changed that.

- **A reproduction.** The leader upserts its declared schedules into `awa.cron_jobs` when it takes leadership, with no lock timeout. With one cron row locked by another session, that upsert waits. Measured on PostgreSQL 17 and 18, a job due in 200 ms stayed `scheduled` and an attempt past its 500 ms deadline stayed `running` for the whole 15 s observation window. A control run without the lock completes both. The stall lasts as long as the lock is held.
- **The same shape elsewhere.** A batch operation's scan reads every page before it returns, and it runs without the interactive read timeout (comment on #303). Cleanup, metadata refresh and descriptor cleanup are ordinary statements that can wait on locks. Any of them holds back promotion and rescue while it runs.

The branch metrics do not show this either. The leader's startup cron sync is not a tracked branch, so it records nothing. A tracked hygiene branch blocked in its body records its duration only when it finishes, and against itself, not against the promotion and rescue branches it held back.

## Decision

The leader term runs two loops concurrently:

- **Recovery loop:** promotion and the three rescues.
- **Hygiene loop:** the initial cron sync, the cron evaluator, and every other branch.

Both run on the leader-term task (`tokio::join!`), not in a spawned task. Every branch waits on the database, so running them concurrently on one task is enough for one loop's wait not to block the other. Borrowing the service and its trackers needs no `'static` bounds or `Arc` wrapping.

Leadership and shutdown are unchanged:

- Each loop selects on the same leadership-lost and shutdown tokens and returns on either.
- `run_leader_term` still bounds the leader term as a whole. On shutdown, the in-flight branch in either loop gets `MAINTENANCE_BRANCH_SHUTDOWN_GRACE`.
- Leadership is still one session advisory lock. Losing it ends both loops.

Branch registration is unchanged. Every arm still goes through `MaintenanceBranchTracker::try_begin` and `PruneControl::admit`. `scheduler_registers_branches_with_the_closed_scope_vocabulary` now also asserts that the recovery loop admits exactly the four user-visible branches and the hygiene loop admits none of them.

## Why concurrent branches are safe

Before the split, two branches never ran at once on one leader. They could already run at once across two leaders: on leadership loss, the old leader's in-flight branch finishes within its grace period while the new leader starts. Operator commands (cancel, retry, batch operations through the API) and producers also run concurrently with every branch. Each branch is its own transaction or sequence of transactions, and their safety rests on SQL locks, not on the loop serialising them.

The pairs that newly overlap within one leader:

- **Rescue and claim or lease prune.** Prune takes `ACCESS EXCLUSIVE` on the partition with a 10 ms lock timeout and re-checks emptiness under the lock. A rescue transaction holding row locks makes prune time out and retry, counted as `blocked`. This already happens with claimers.
- **Promotion and queue rotation.** Promotion inserts into the current ready slot as producers do. Rotation changes the current slot under the same locks producers wait on.
- **Promotion and batch operations.** Both are already concurrent with operator API calls and take row locks in their own transactions.

The TLA+ models do not change, because each branch is still its own transaction.

## Consequences

**Positive**

- A hygiene branch waiting on a lock or scanning a large table no longer delays promotion or rescue.
- Shutdown stays bounded while a hygiene branch is stuck.
- No configuration, schema or protocol change, and rolling upgrades are unaffected. Old and new leaders differ only in how they schedule their own branches.

**Negative**

- A leader now holds up to two maintenance connections at once instead of one, plus the existing promotion lane waiters. The pool is not partitioned, so a hygiene loop that holds connections can still slow recovery's acquires under pool pressure. Reserving recovery capacity is left for later; the hygiene loop runs one branch at a time.
- Prune may report more `blocked` outcomes while a rescue pass runs, as described above.
- Branch timing histograms for hygiene branches no longer include time spent waiting behind recovery branches, and the reverse. They measure each branch's own body, as intended.

## Alternatives considered

- **A `lock_timeout` on each hygiene statement.** Bounds lock waits but not long scans, and every new statement has to remember it.
- **A spawned task for recovery.** Gives OS-thread parallelism the branches do not need. It needs `'static` captures and a second cancellation path, and still shares the pool.
- **A separate leader lock for recovery.** Lets two runtimes split the work, but adds an election, a fencing story and status reporting for something that is a scheduling problem within one leader.
- **Keep deferring until fleet telemetry shows overruns.** The overrun counter does not see a branch blocked in its body (see Context), so waiting for it could wait indefinitely.

## Relationship to other ADRs

- **ADR-028 (maintenance-only role)** is a separate process dedicated to maintenance. This ADR is about scheduling within whichever process leads. They compose.
- **ADR-030 (batch operations)** runs in the hygiene loop. Its scans no longer delay recovery, but they still delay other hygiene branches.
