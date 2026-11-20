# ADR-053: Coordination stays off the commit path

## Status

Accepted, superseding [ADR-045](045-hot-path-trigger-lock-contract.md), whose trigger contract is rule 1 below. Rule 1 shipped in migration v046 and the 0.6 schema patch `wait_free_dirty_marks`. Rule 3 shipped in 0.6.19 and on main. Rule 2 ships on 0.6 and main for the paths awa commits itself. Phase 2 (wake-ups from caller-owned transactions) and Phase 3 (adaptive backoff) are proposed. Tracked in [#841](https://github.com/hardbyte/awa/issues/841).

## Context

Besides jobs, awa keeps coordination state: dirty marks for the admin counts, wake-ups for idle claimers, the shared claimer target per queue, leases, ring cursors. It is small, but it is written by or alongside job transitions: enqueue, claim, heartbeat, completion, retry, rescue and promotion, including every application transaction that enqueues. Three mechanisms turn that coordination into a wait on another transaction or on the commit flush, and each one grows with the number of instances and with commit latency.

**A keyed write waits on uncommitted rows.** Since v006 the admin-count triggers marked a queue or kind dirty with `INSERT ... ON CONFLICT DO NOTHING` into tables keyed by queue and kind. Against a keyed row another transaction has inserted or deleted and not committed, that insert waits. The 2 s drain deleted the keyed rows inside its recompute transaction and the 60 s refresh `TRUNCATE`d them, so job transitions waited behind maintenance and then stampeded. Because a re-schedule fires two statement triggers and a claim fires one, two transitions that shared only a queue name could take the queue and kind rows in opposite orders and deadlock. One deployment recorded 37 such deadlocks in a week, each leaving a job `running` until heartbeat rescue, and about 3,000 transitions a day waiting more than a second.

**`NOTIFY` serializes notifying commits.** A transaction that called `pg_notify` takes a cluster-wide heavyweight lock in `PreCommit_Notify` and holds it until its commit record is flushed (`src/backend/commands/async.c`; unchanged in PostgreSQL 19). Every notifying transaction in the cluster commits one at a time, each waiting for its own flush, so enqueues that notify inside their transaction are capped at one commit per flush time. PostgreSQL 18 also signals every listening backend on each such commit; 19 signals only those listening on the channel.

**A no-op upsert still locks.** `INSERT ... ON CONFLICT DO UPDATE ... WHERE` locks the conflicting row before it evaluates its `WHERE`, so a refresh that skips the update still takes an xid, a row lock and a commit flush. Every instance refreshed `queue_claimer_state` this way once the row was 500 ms old, so refreshes queued on one row lock per queue, each holding it through its own flush.

Measured with `pgbench` on PostgreSQL 18.7 and 19 (19rc2), commit flush emulated with `commit_delay` and `commit_siblings = 0`:

| Workload | Fast commit | 10 ms flush | 50 ms flush |
| --- | --- | --- | --- |
| 32 producers, no `NOTIFY` | ~9,600 tx/s | ~2,600 tx/s | |
| 32 producers, `NOTIFY` in the insert transaction | ~2,900 tx/s | 93 tx/s, p99 ~355 ms | |
| 32 producers, coalesced `NOTIFY` after commit | ~9,600 tx/s | ~2,600 tx/s | |
| claimer refresh, `ON CONFLICT DO UPDATE`, 64 instances × 4/s | p99 ~10 ms | p99 ~15 ms | p99 2–7 s |
| claimer refresh, plain `UPDATE` only on change | p99 ~13 ms | p99 ~4 ms | p99 ~3 ms |

With 200 idle listeners on other channels, `NOTIFY` in the insert transaction at fast commit fell to ~1,000 tx/s on 18 and ~3,750 tx/s on 19. Adding `IS DISTINCT FROM` to the claimer upsert's `WHERE` made it worse: one xid and one WAL record per call, and p99 13 s at a 10 ms flush.

## Decision

A job transition never waits on another transaction's coordination work, and coordination work that changes nothing writes and locks nothing. The one permitted wait is the unique claim, where waiting is the feature.

1. **Hot-path triggers write only append-only, unconstrained rows.** A trigger on `awa.jobs_hot` or `awa.scheduled_jobs` writes dirty marks into `awa.admin_dirty_queue_marks` and `awa.admin_dirty_kind_marks`, which carry no index and no constraint, with plain heap inserts; nothing on the hot path updates or deletes a mark, and readers collapse duplicates with `DISTINCT`. The maintenance reader never takes a lock that conflicts with the hot path: the drain deletes exactly the marks it can see and recounts under READ COMMITTED statement snapshots, the full refresh deletes visible marks, and `TRUNCATE` is not used on any table a hot-path trigger writes. The only other trigger write is `awa.sync_job_unique_claims`, which writes the transitioning job's own claim rows into `awa.job_unique_claims`, whose unique index is the uniqueness feature.
2. **Wake-ups are hints sent after commit, coalesced.** A path that commits its own transaction hands its queues to `awa_model::wakeup` after `COMMIT`. Per pool, that sends at most one `NOTIFY` per channel per 50 ms: the first wake-up of a channel not woken within the window at once, later ones when the window ends, one statement at a time, so a burst costs a few notifications and at most one connection. Every hand-off is followed by a notification sent after it. Batch and COPY enqueue, retries, DLQ retries, priority aging and promotion use it. Paths whose caller commits, the SQL insert functions (`insert_job_compat`, `insert_jobs_compat_batch`) and the public `*_tx` methods, notify inside the caller's transaction until Phase 2.
3. **Coordination state is written only when it changes, and a no-op takes no lock.** No keyed upsert refreshes a value that has not changed. The claimer target is read once, recomputed from the claimer signal when stale, and written only when it differs, with a plain `UPDATE ... WHERE target_claimers IS DISTINCT FROM $2`, which takes no lock on a row it skips. `updated_at` records the last change, which stands for the control interval.
4. **The contract is executable.** `hot_path_triggers_cannot_wait_on_other_transactions` enumerates every non-internal trigger on `jobs_hot` and `scheduled_jobs`, extracts each body's write targets, and fails on any target outside the allowlist, on `ON CONFLICT`, `TRUNCATE`, `LOCK` or any row lock in a trigger body, on `UPDATE` or `DELETE` outside the claim trigger, on any index or constraint on a mark table, and on `TRUNCATE` in the maintenance functions; the `AwaCanonicalDirtyMarks` TLA+ model keeps the v006 cycle as an expected counterexample and proves the wait-free plans never block. `test_queue_storage_enqueue_wakes_after_commit_coalesced` fails if an enqueue that commits its own transaction notifies inside it. `test_queue_storage_claimer_target_refresh_writes_only_a_change` and `..._does_not_wait_on_a_locked_row` fail if an unchanged claimer target is written or locked. Every finalize path re-runs a transaction aborted with SQLSTATE `40P01` (up to three retries, 10/20/40 ms backoff) and counts it in `awa.completion.deadlock_retry`; a non-zero rate means a new wait has entered the hot path.

A change that adds a wait to a job transition, a write anywhere else from a hot-path trigger, a constraint on a mark table, a keyed upsert on the hot path, or a `NOTIFY` inside a transaction awa commits needs a new ADR explaining why the wait cannot form a cycle or queue behind the commit flush.

### Phase 2 (proposed): wake-ups from caller-owned transactions

The SQL insert functions stop calling `pg_notify`. The Rust and Python insert APIs hand the queue to the coalescer instead, with the send delayed by one window after the insert. A caller transaction that commits within the window gets a wake-up about 30 ms after its commit (measured); a longer one falls back to polling. Producers that insert through raw SQL lose their wake-up and rely on polling. This changes migrated function bodies, so it ships in 0.7 under [ADR-041](041-rolling-upgrade-policy.md).

### Phase 3 (proposed): adaptive backoff

Claim, poll and maintenance work back off when awa's own statement latency or lock waits climb, and recover additively when they fall. Heartbeats and completions never back off, because slowing them triggers rescues and adds work; rescue backs off with the rest of maintenance. On PostgreSQL 19, `pg_stat_lock` gives per-lock-type wait counts that can feed the signal. Needs its own design and measurement before implementation.

## Consequences

- An enqueue or retry that awa commits no longer waits on other notifying commits. Pickup from an idle claimer goes from ~0.1 ms to ~1.6 ms (p50) on those paths, and listeners receive fewer notifications. A notification carries no payload and only triggers a claim, so nothing depended on their count.
- A wake-up lost because the process exits between `COMMIT` and the send delays the claim until the claimer's next poll: on main at most the idle poll backoff (2 s, or `poll_interval` when larger), on 0.6 at most `poll_interval`.
- Until Phase 2, enqueues through `insert`, `insert_with`, `insert_many` and cron still notify inside the caller's transaction and take the notify lock at its commit.
- A steady claimer target costs two reads per refresh and no write. Each instance recomputes it on its own refresh instead of reading one instance's result, trading a read of the queue's head sequences for a contended lock and write.
- One small heap insert per DML statement on the canonical tables replaces a keyed upsert. Marks accumulate between drains, and while a maintenance leader is alive the mark tables stay a few rows long. The runtime role does not need `TRUNCATE` for the canonical maintenance path.
- Wake-up state is per process and per pool, so two pools in one process to one database each send their own notifications.

## Alternatives considered

- **Sorted acquisition or a per-queue advisory lock in the trigger.** Ordering does not remove the wait; a drain between two triggers of one statement still reverses it. A per-queue advisory lock serialises every transition in a queue behind the slowest one.
- **`pg_notify` instead of dirty-mark rows.** Wait-free on the trigger side, but it adds a notifying commit to every transition, and a leader that is not listening loses the signal for up to a full refresh interval.
- **Keep `NOTIFY` in the transaction.** Exactly-on-commit delivery and the lowest latency, but every notifying commit in the cluster is serialised behind the flush, and a full notify queue fails the caller's commit with "too many notifications in the NOTIFY queue".
- **One `NOTIFY` after each commit, not coalesced.** Takes the lock out of the enqueue transaction, but each enqueue then spends an extra statement and connection on the lock, and under slow flushes those statements queue: ~4,500 tx/s against ~9,600 coalesced at fast commit.
- **Signal at insert time and send immediately for caller-owned transactions.** The notification usually arrives before the caller commits, the claimer finds nothing, and pickup falls back to polling (p50 ~86 ms measured). Phase 2 delays the send instead.
- **Poll only.** Simplest, but pickup becomes uniform over the poll interval (p50 ~100 ms at 200 ms), and a shorter interval adds idle claim queries for every queue.
- **`IS DISTINCT FROM` in the claimer upsert's `WHERE`.** Skips the write but not the lock, and stops refreshing `updated_at`, so every refresh takes the lock: measured worse than the original.
