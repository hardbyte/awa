> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# ADR-045: Hot-path triggers never wait on another transaction

## Status

Accepted. Tracked in [#492](https://github.com/hardbyte/awa/issues/492). Implemented by migration v046 (`awa-model/migrations/v046_wait_free_dirty_marks.sql`), the 0.6 schema patch `wait_free_dirty_marks`, the `hot_path_triggers_cannot_wait_on_other_transactions` test in `awa/tests/dirty_marks_wait_free_test.rs`, and the `AwaCanonicalDirtyMarks` TLA+ model.

## Context

Every INSERT, UPDATE, or DELETE on `awa.jobs_hot` and `awa.scheduled_jobs` fires triggers, so whatever a trigger does executes inside every job transition: enqueue, claim, heartbeat, completion, re-schedule, rescue, and every application transaction that enqueues through `awa.jobs`. Since v006 the admin-metadata triggers recorded "this queue / kind needs its cached counts recomputed" with

```sql
INSERT INTO awa.admin_dirty_queues (queue) ... ON CONFLICT (queue) DO NOTHING
```

into tables keyed by queue and kind. `ON CONFLICT DO NOTHING` is free only when the keyed row is committed. Against a row another transaction has inserted or deleted and not yet committed, Postgres waits for that transaction. Two maintenance paths made that wait common: the 2 s drain deleted the keyed rows inside its recompute transaction, and the 60 s full refresh `TRUNCATE`d both tables under `ACCESS EXCLUSIVE` for its whole run.

A production cluster showed the consequence on 2026-09-12. The refresh held every job transition for about three seconds each minute; when it committed the tables were empty and the queued transitions stampeded onto them. A re-schedule CTE fires two statement triggers (the `jobs_hot` DELETE trigger, then the `scheduled_jobs` INSERT trigger), each marking queue then kind, while a claim UPDATE marks only the queue. With a drain landing between one re-schedule's two triggers, two re-schedules in the same queue acquired the queue row and the kind row in opposite orders and deadlocked. The victim's completion was only logged, and its job sat `running` until heartbeat rescue two minutes later. The cluster recorded 37 such deadlocks in a week and roughly 3,000 transitions a day waiting more than a second behind the refresh.

The v006 mitigation (sorted inserts, `ON CONFLICT DO NOTHING`) addressed lock *ordering* within one statement. The defect is that the trigger could *wait* at all: a hot-path transaction that waits on another while holding its own locks is a deadlock edge, and this edge joined transactions that shared nothing but a queue name. No amount of ordering removes an edge whose direction depends on what a concurrent drain deleted a millisecond earlier.

## Decision

A trigger on a canonical hot table may not perform any operation that can wait on another transaction, except for the one case where waiting expresses the feature itself.

1. **Shared bookkeeping written from triggers is append-only and unconstrained.** The dirty marks live in `awa.admin_dirty_queue_marks` and `awa.admin_dirty_kind_marks`, which carry no index and no constraint. The trigger `INSERT` is a plain heap insert. Nothing on the hot path updates or deletes a mark. Duplicate marks are collapsed by `DISTINCT` on read.
2. **The maintenance reader never takes a lock that conflicts with the hot path.** The drain selects the distinct keys it can see, deletes exactly those visible rows, then recounts under READ COMMITTED statement snapshots; a mark from a transaction that commits later stays for the next drain, so the cached counts remain exact. The full refresh deletes visible marks instead of truncating. `TRUNCATE` is not used on any table a hot-path trigger writes.
3. **The one permitted wait is the unique claim.** `awa.sync_job_unique_claims` inserts into `awa.job_unique_claims`, whose unique index is the uniqueness feature. A wait there is a genuine duplicate racing a transition, and the transaction that loses gets the documented unique-violation outcome. It writes only the transitioning job's own claim rows.
4. **The contract is executable.** `hot_path_triggers_cannot_wait_on_other_transactions` enumerates every non-internal trigger on `jobs_hot` and `scheduled_jobs`, extracts each body's write targets, and fails on any target outside the allowlist, on `ON CONFLICT`, `TRUNCATE`, `LOCK`, or any row lock (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, `FOR KEY SHARE`) in a trigger body, on `UPDATE`/`DELETE` outside the claim trigger, on any index or constraint on a mark table, and on `TRUNCATE` in the maintenance functions. `AwaCanonicalDirtyMarks` keeps the v006 cycle, including the production interleaving, as an expected counterexample and proves the wait-free plans never block.
5. **A deadlock abort is retried, not surfaced.** Every finalize path is one lease-guarded transaction, so the executor re-runs a transaction Postgres aborted with SQLSTATE `40P01` (up to three retries after the initial attempt, 10/20/40 ms backoff) and counts it in `awa.completion.deadlock_retry`. A non-zero rate of that metric means a new wait has entered the hot path.

Adding a trigger that writes anywhere else, adding a constraint to a mark table, or re-introducing a keyed upsert on the hot path requires a new ADR that explains why the wait cannot form a cycle.

## Consequences

- One small heap insert per DML statement on the canonical tables replaces a keyed upsert that was a read in the steady state and a wait after every drain. Marks accumulate between drains and are removed by every drain and refresh; while a maintenance leader is alive the tables stay a few rows long.
- The runtime role no longer needs `TRUNCATE` for the canonical maintenance path; queue-storage segment pruning still uses it on partitions.
- The old `admin_dirty_queues` / `admin_dirty_kinds` tables remain, unread, until a later contract migration removes them under ADR-041.
- The 0.6 series cannot add migration versions, so the same SQL ships there as an idempotent, versionless schema patch applied at v040 and included in the SQL export for external runners.

## Alternatives considered

- **Sorted acquisition or a per-queue advisory lock in the trigger.** Ordering does not remove the edge; a drain between two triggers of one statement still reverses it. An advisory lock per queue would serialise every transition in a queue behind the slowest one and still leave the refresh's `TRUNCATE` stall.
- **Drain in a separate short transaction, keep `ON CONFLICT`.** Shortens one window but keeps the first-inserter-after-drain wait and the stampede after every refresh.
- **`pg_notify` instead of mark rows.** Wait-free and durable enough given the reconciliation backstop, but `NOTIFY` serialises briefly at commit under the async queue lock, and a leader that is not listening loses the signal for up to a full refresh interval. Append-only rows give the same wait-freedom with exact counts.
- **Drop the cached counts and compute on demand.** Exactness without maintenance, but the admin UI and the queue-health metrics read those counts on every tick.
