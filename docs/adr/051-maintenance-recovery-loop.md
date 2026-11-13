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
- **Hygiene loop:** the initial cron sync and every other branch. It also starts the cron evaluator, which runs on its own task.

The hygiene loop starts the cron evaluator before the initial cron sync, not after it. A sync waiting on a locked `awa.cron_jobs` row therefore no longer stops schedules already in the table from firing. Until the sync commits, a schedule whose declaration changed is evaluated as stored, which is how the previous leader left it, and `last_enqueued_at` still guards against a double fire.

Both run on the leader-term task (`tokio::join!`), not in a spawned task. Every branch waits on the database, so running them concurrently on one task is enough for one loop's wait not to block the other. Borrowing the service and its trackers needs no `'static` bounds or `Arc` wrapping.

Leadership and shutdown are unchanged:

- Each loop selects on the same leadership-lost and shutdown tokens and returns on either.
- On shutdown, `run_leader_term` gives the branches in flight one shared `MAINTENANCE_BRANCH_SHUTDOWN_GRACE`.
- Leadership is still one session advisory lock. Losing it ends both loops at their next select. Neither loop starts a branch after the loss. A branch already in flight runs to completion, as before; `run_leader_term` bounds that drain only on shutdown.
- `maintenance_branch_isolation_test` covers both: it kills the lock session while the hygiene loop is stuck, checks that nothing is promoted while no session holds the lock, and checks that shutdown is bounded.

Branch registration is unchanged. Every arm still goes through `MaintenanceBranchTracker::try_begin` and `PruneControl::admit`. `scheduler_registers_branches_with_the_closed_scope_vocabulary` now also asserts that the recovery loop admits exactly the four user-visible branches and the hygiene loop admits none of them.

## Why concurrent branches are safe

Before the split, two branches never ran at once on one leader. They could already run at once across two leaders. When a leader loses its lock, its in-flight branch keeps running until it finishes, with no time bound except on shutdown. That covers the 1–3 s before `LeaderObserver` notices a dead session, too. Meanwhile the new leader starts its own branches. Operator commands (cancel, retry, batch operations through the API) and producers also run concurrently with every branch. Each branch is its own transaction or sequence of transactions, and their safety rests on SQL locks, not on the loop serialising them.

The pairs that newly overlap within one leader:

- **Rescue and lease prune.** Lease prune takes `ACCESS EXCLUSIVE` on the partition under a 10 ms `lock_timeout` and re-checks emptiness under the lock. A rescue transaction holding row locks makes it time out and retry, counted as `blocked`.
- **Rescue and claim prune.** Receipt rescue locks the claim slot's `claim_ring_slots` row `FOR UPDATE`, oldest slot first. Claim prune takes `claim_ring_state` and then that same slot row `FOR UPDATE` before setting any `lock_timeout`, so it waits for the rescue batch's transaction to commit. That blocks the hygiene loop for one rescue batch per collision, which is routine while a backlog drains and rescue re-arms every second. There is no lock cycle: rescue reads `claim_ring_state` without locking it, only rotation and prune lock it (both in the hygiene loop), and where rescue's closure inserts meet prune's `ACCESS EXCLUSIVE`, prune's `lock_timeout` breaks the wait.
- **Promotion and queue rotation.** Promotion re-enqueues through the same insert path as retries and callback resumes, reading the current ready slot without a lock. Rotation serialises on its own `queue_ring_state` `FOR UPDATE` and its busy checks, which already tolerate those writers.
- **Promotion and batch operations.** Both are already concurrent with operator API calls and take row locks in their own transactions.

The TLA+ models do not change. `AwaStorageLockOrder` interleaves transactions arbitrarily, and `AwaExtended` and `AwaCron` model each maintenance action atomically, with no serialisation within a leader.

One mapping does depend on the loop structure. `correctness/storage/MAPPING.md` relies on the leader running one rescue batch at a time. That still holds, because all three rescues run in the recovery loop, one after another. `scheduler_registers_branches_with_the_closed_scope_vocabulary` fails if a rescue moves to the hygiene loop.

## Consequences

**Positive**

- A hygiene branch waiting on a lock or scanning a large table no longer delays promotion or rescue.
- A locked cron row no longer stops stored schedules from firing.
- Shutdown stays bounded while a hygiene branch is stuck.
- No configuration, schema or protocol change, and rolling upgrades are unaffected. Old and new leaders differ only in how they schedule their own branches.

**Negative**

- A leader now holds one more pool connection at a time. The total is its lock session, one connection per loop, the cron evaluator's, and up to `lane_waiter_limit` promotion lane waiters: one in eight pool connections, clamped to between one and four. A 10-connection pool gives about five to maintenance instead of four. The pool is not partitioned, so under pool pressure the hygiene loop can still slow recovery's acquires. Reserving recovery capacity is left for later.
- Lease prune may report more `blocked` outcomes while a rescue pass runs, and claim prune may wait out a rescue batch, as described above.
- Hygiene branches still delay one another. A stuck cleanup, cron sync, batch-operation scan or metadata refresh holds back ring rotation and prune, rollup and the other hygiene branches until it finishes, and the overrun counter still does not show a branch blocked in its body. Bounding those statements individually, or exporting the age of the in-flight branch, is left for later.

## Alternatives considered

- **A `lock_timeout` on each hygiene statement.** Bounds lock waits but not long scans, and every new statement has to remember it.
- **A spawned task for recovery.** Gives OS-thread parallelism the branches do not need. It needs `'static` captures and a second cancellation path, and still shares the pool.
- **A separate leader lock for recovery.** Lets two runtimes split the work, but adds an election, a fencing story and status reporting for something that is a scheduling problem within one leader.
- **Keep deferring until fleet telemetry shows overruns.** The overrun counter does not see a branch blocked in its body (see Context), so waiting for it could wait indefinitely.

## Relationship to other ADRs

- **ADR-028 (maintenance-only role)** is a separate process dedicated to maintenance. This ADR is about scheduling within whichever process leads. They compose.
- **ADR-030 (batch operations)** runs in the hygiene loop. Its scans no longer delay recovery, but they still delay other hygiene branches.
