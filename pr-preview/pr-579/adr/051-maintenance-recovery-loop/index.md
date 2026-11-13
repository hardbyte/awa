> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# ADR-051: Run promotion and rescue in their own maintenance loop

## Status

Accepted. Implements [#303](https://github.com/hardbyte/awa/issues/303). First shipped in 0.6.17; forward-ported to `main` for 0.7.

## Context

The maintenance leader ran every branch from one `tokio::select!`, and awaited each branch's body inside its arm. While one body waited, no other arm was polled, including the leader check. Four of those branches are what users wait on:

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

The leader term runs three futures concurrently:

- **Recovery loop:** promotion and the three rescues.
- **Hygiene loop:** the initial cron sync and every other branch.
- **Leader check:** the periodic `SELECT 1` on the session that holds the leader lock.

The cron evaluator still runs on its own task. It now starts before the initial cron sync, not after it, so a sync waiting on a locked `awa.cron_jobs` row no longer stops schedules already in the table from firing. Until the sync commits, a schedule whose declaration changed is evaluated as stored, which is how the previous leader left it, and `last_enqueued_at` still guards against a double fire.

All three run on the leader-term task (`tokio::join!`), not in spawned tasks. Every branch waits on the database, so running them concurrently on one task is enough for one loop's wait not to block the other. Borrowing the service, its trackers and the leader connection needs no `'static` bounds or `Arc` wrapping.

Leadership and shutdown:

- The term has one cancellation token, a child of the service's shutdown token. The leader check cancels it when the lock session fails; shutdown cancels it through its parent. Each loop selects on it and returns at its next select, and neither starts a branch after it is cancelled.
- The leader check no longer waits behind a branch body, so a lost lock session is noticed within `leader_check_interval` even while both loops are inside a branch. The runtime reports itself as not leader and stops the cron evaluator at that point.
- A branch already in flight runs to completion, as before. The term ends when the leader check and both loops have returned; then the runtime re-enters election, or on shutdown releases the leader lock.
- Shutdown still waits for the branches in flight, as before the split. A hygiene branch stuck on a lock therefore still delays shutdown until the lock is released. Bounding that wait is separate work.
- `maintenance_branch_isolation_test` covers the split: with the startup cron sync stuck on a locked row, it checks that a due job is promoted and an overdue attempt rescued, that a stored every-second schedule fires, and that after the lock session is killed nothing is promoted while no session holds the lock.

Branch registration is unchanged. Every arm still goes through `MaintenanceBranchTracker::try_begin` or `try_begin_without_cooldown`, and the tracker is shared by both loops. A unit test asserts that the recovery loop runs exactly the four user-visible branches and the hygiene loop runs none of them.

## Why concurrent branches are safe

Before the split, two branches never ran at once on one leader. They could already run at once across two leaders: a leader whose lock session dies keeps running its in-flight branch until it finishes, and meanwhile the new leader starts its own branches. Operator commands (cancel, retry, batch operations through the API) and producers also run concurrently with every branch. Each branch is its own transaction or sequence of transactions, and their safety rests on SQL locks, not on the loop serialising them.

The pairs that newly overlap within one leader:

- **Rescue and lease prune.** Lease prune takes `ACCESS EXCLUSIVE` on the partition under a short `lock_timeout` and re-checks emptiness under the lock. A rescue transaction holding row locks makes it time out and retry, counted as `blocked`.
- **Rescue and claim prune.** Receipt rescue locks the claim slot's `claim_ring_slots` row `FOR UPDATE`. Claim prune takes the claim-ring serializer and then that same slot row `FOR UPDATE` before setting any `lock_timeout`, so it waits for the rescue batch's transaction to commit. That blocks the hygiene loop for one rescue batch per collision, which is routine while a backlog drains and rescue re-arms every second. There is no lock cycle: rescue does not take the ring serializer, only rotation and prune do (both in the hygiene loop), and where rescue's closure inserts meet prune's `ACCESS EXCLUSIVE`, prune's `lock_timeout` breaks the wait.
- **Promotion and queue rotation.** Promotion re-enqueues through the same insert path as retries and callback resumes, reading the current ready slot without a lock. Rotation serialises on its own ring serializer and its busy checks, which already tolerate those writers.
- **Promotion and batch operations.** Both are already concurrent with operator API calls and take row locks in their own transactions.

The TLA+ models do not change. `AwaStorageLockOrder` interleaves transactions arbitrarily, and `AwaExtended` and `AwaCron` model each maintenance action atomically, with no serialisation within a leader.

The leader still runs one rescue batch at a time, because all three rescues run in the recovery loop, one after another. The unit test above fails if a rescue moves to the hygiene loop.

## Consequences

**Positive**

- A hygiene branch waiting on a lock or scanning a large table no longer delays promotion or rescue.
- A locked cron row no longer stops stored schedules from firing.
- A lost leader lock is noticed while a branch is in flight, so recovery stops at its next select instead of after the stuck branch.
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
