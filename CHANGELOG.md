# Changelog

Notable changes between releases. Detailed migration notes for storage transitions live in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

## [Unreleased]

## [0.6.16] — 2026-11-09

Recommended for everyone on 0.6.15 using queue storage. Batch inserts (`insert_many_copy`, `insert_many_skip_unique_conflicts`) give way to per-row producers that share their keys or lanes instead of deadlocking with them, and creating a new queue lane no longer deadlocks between `QueueStorage` and SQL producers. Operators can durably halt and resume `maintenance.prune` across the fleet. `admin::get_job` no longer misses a failed job while prune moves it, and `awa migrate` re-applies the claim batch index and batch insert patches with fixes.

### Upgrading

- The schema stays at v40. Run `awa migrate` once as the schema owner while traffic keeps running. On a 0.6.15 database it applies three repeatable patches, each in its own transaction and none taking a table lock: `claim_batch_job_index` again, replacing each queue-storage schema's `ensure_claim_batch_job_index`; `insert_jobs_compat_batch`, replacing the body of `awa.insert_jobs_compat_batch`, so 0.6.15 producers get the deadlock fix as soon as it commits; and the new `prune_controls`, which adds the `awa.runtime_controls` and `awa.runtime_control_observations` tables and the halt and resume functions without altering existing tables. See the [upgrade guide](docs/upgrade-0.6.15-to-0.6.16.md).
- The patch does not grant runtime or operator access. Grant runtime roles `SELECT` on `awa.runtime_controls` and `SELECT, INSERT, UPDATE, DELETE` on `awa.runtime_control_observations`, and an operator role `SELECT` on both and on `awa.runtime_instances` plus `EXECUTE` on `awa.halt_runtime_scope` and `awa.resume_runtime_scope` ([runtime controls](docs/runtime-controls.md#install)). A 0.6.16 runtime that cannot read the control stops admitting prune within five seconds of the patch committing, while other maintenance continues, so apply the grants before rolling binaries, or expect prune to pause until they are in place.
- External migration runners: apply the new `R__claim_batch_job_index.sql` and `R__insert_jobs_compat_batch.sql`, then `R__prune_controls.sql`, after `V40`. Replace all `R__` files together with the full set from `awa migrate --extract-to <dir> --from 40`; `--pending` lists only the patches a database lacks.
- During the roll, a 0.6.15 `QueueStorage` process that is the first to use a new `(queue, priority)` lane can still deadlock with another producer of that lane. Create a new queue's lanes ahead of traffic with the 0.6.16 `awa storage prepare-queue`, or retry the failed transaction.
- 0.6.15 binaries ignore a prune halt and can still become maintenance leader. Upgrade or fence every process that can lead before relying on a halt, and check `awa control status`.
- Binaries can roll back to 0.6.15 with the patches left in place. A 0.6.15 leader ignores a stored halt.

### Fixed

**Enqueue deadlocks**

- `insert_many_copy` and `insert_many_skip_unique_conflicts` no longer deadlock with per-row producers (generic `insert_many`, single-job inserts, 0.6.14 and earlier binaries) that share their unique keys or lanes. Since 0.6.15 the batch function took all of a batch's unique claims before its lanes, so a per-row transaction that held a lane and then inserted a key the batch had claimed waited on the batch while the batch waited on it, and PostgreSQL aborted one of them with `deadlock detected` (`40P01`). For its first 2 s the function now limits each lock wait to 500 ms, or half of its own session's `deadlock_timeout` if lower. A longer wait rolls back the claims and lanes it holds and retries, so the per-row transaction proceeds and the batch then skips, or fails on, the keys that transaction committed. The same covers a transaction that enqueues lanes in separate statements in the opposite order to a batch. This holds when the batch call is the first thing its transaction locks, the per-row session's `deadlock_timeout` is above 500 ms (PostgreSQL's default is 1 s), and the batch inserts its unique claims within 500 ms: roughly 10,000 to 20,000 keys on a fast server, fewer on a slow or busy one. Results, the caller's `lock_timeout` on each wait, and the lane lock window are unchanged.
- Creating a new queue lane through `QueueStorage` (its batch enqueues, and workers promoting scheduled or retried jobs) no longer deadlocks with a concurrent `insert`, `insert_many` or `insert_many_copy`, or a 0.6.14 or 0.6.15 per-row producer, that is also the first to use the same `(queue, priority)` (#692). `QueueStorage` took the lane's provisioning lock before writing its `queue_lanes` row, while SQL inserts write the row first and take the lock in the head-table trigger that creates the lane's sequences, so each waited on the other and one failed with `40P01`. `QueueStorage` now writes the row first too. Lanes that already existed were never affected. No schema change.

**Claim batch index**

- Claim prune no longer treats a partial GIN index on `lease_claim_batches.job_ids`, or a session with `quote_all_identifiers` on, as the claim batch index. It recognises only a valid, non-partial GIN index on `job_ids` alone. When another index or relation already has the index's name, `ensure_claim_batch_job_index` leaves it in place, returns false and raises a warning naming it, instead of reporting the slot indexed; the slot keeps scanning until an operator drops or renames it.
- Refreshing `ensure_claim_batch_job_index` as a migration role that owns it but may only `SET ROLE` to the table owner (PostgreSQL 16+ membership `WITH INHERIT FALSE`) no longer leaves `EXECUTE` with `PUBLIC`. The refresh used to hand the function to the table owner first, after which that role could no longer revoke or grant on it, and PostgreSQL only warned. It now resets `EXECUTE` after the transfer, acting as the new owner when it cannot as itself, so exactly the roles holding `TRUNCATE` on `lease_claim_batches` can execute it, the former owner included. Migrations run with the owner as the effective role, as documented, were not affected.

**Reads during prune**

- `admin::get_job` and the other queue-storage point reads of one job no longer return `JobNotFound` for a failed job while queue prune moves its row out of the slot being pruned. Since 0.6.14 the read used one snapshot, and a prune that committed after that snapshot was taken left the old slot empty and the moved row invisible to it. Prune moves each retained failed row once per ring cycle, and on a new schema prunes slot 0 right after the first rotation, so a read polling for `failed` could hit it within a second of a worker starting. A read that finds nothing now reads once more in a new snapshot.

### Changed

- When the batch function's bounded retries run out, it makes one last attempt with ordinary lock waits, as 0.6.15 did. A per-row transaction that holds a lane for longer than about 2.5 s before it inserts one of the batch's keys can therefore still deadlock with it, as can cycles of three or more transactions. A cycle through a lock the batch's own transaction took in an earlier statement, which rolling back the batch cannot release, now ends in `40P01` after about 3 to 4 s instead of 1 to 2 s, and usually aborts the batch's transaction rather than the per-row one. A batch whose unique claims take longer than 500 ms to insert behaves as in 0.6.15; split such batches if they share keys or lanes with per-row producers.

### Added

- Durable operator halt and resume for `maintenance.prune`, in drain mode: `awa control halt`, `status` and `resume`, or the `awa.halt_runtime_scope` and `awa.resume_runtime_scope` functions from an existing SQL session. Compatible leaders observe it on their existing session about once a second and stop admitting rotation, prune, rollups, cleanup, admin metadata refresh and descriptor publication; enqueue, dispatch, handlers, cron, promotion, rescue and admin batches continue. Resume names the current halt's UUID. Status lists live runtimes without supported, recent reports and cannot report the fleet halted while any remain. Older binaries ignore the control. See [runtime controls](docs/runtime-controls.md).

## [0.6.15] — 2026-11-09

Recommended for everyone on 0.6.14 or 0.6.13 using queue storage. A worker whose completions or lifecycle hooks fall behind stops claiming instead of holding every finished job in memory until it runs out, by-job lookups of compact claims no longer read every retained claim batch, and large COPY batches take a queue lane's enqueue lock when they write that lane rather than at their first ready row.

### Upgrading

- The schema stays at v40. Run `awa migrate` once as the schema owner, before or after rolling binaries, while traffic keeps running. It applies two repeatable patches, `claim_batch_job_index` and `insert_jobs_compat_batch`, which add functions and take no table locks. Binaries can roll back to 0.6.14 with the patches left in place. See the [upgrade guide](docs/upgrade-0.6.14-to-0.6.15.md).
- External migration runners: apply `R__claim_batch_job_index.sql` and `R__insert_jobs_compat_batch.sql` after `V40`. Replace all `R__` files together with the full set from `awa migrate --extract-to <dir> --from 40`; `--pending` lists only the patches a database lacks.
- Deployments that revoke `EXECUTE` on new functions from `PUBLIC` should grant it on `awa.insert_jobs_compat_batch` to runtime roles; without it, `insert_many_copy` keeps its per-row path.

### Fixed

**Worker memory**

- Worker finalization is bounded to the larger of the runtime's worker capacity and twice the completion batch size times the completion shard count (1,024 attempts with the queue-storage defaults). A finished handler keeps its worker slot while it waits for finalizer capacity, so when durable completion falls behind the runtime stops claiming instead of holding an unbounded backlog of finished attempts and their payloads in memory. Before, a 120-second completion stall could leave about 77,000 finished attempts and 1 GB in one worker. Attempt heartbeats continue until finalization settles. Lowering `AWA_COMPLETION_BATCH_SIZE` lowers the bound in proportion, for jobs with large payloads.
- Lifecycle hooks (`on_event`, `on_event_kind`) no longer accumulate without bound when they are slower than job throughput. Each attempt's `Started` and outcome hooks now hold its finalizer slot until they return, so slow hooks slow claiming instead of growing worker memory, and a worker-finalized outcome hook runs after that attempt's `Started` hook has returned. Maintenance's `Rescued` and duplicate `Cancelled` events allow 1,024 outstanding hook tasks per worker and drop further events with a warning.

**Claim lookups**

- By-job lookups of zero-deadline (compact) receipt claims — heartbeat and progress flush, completion fallbacks, receipt close, cancellation, rescue and `load_job` — use a GIN index on `lease_claim_batches.job_ids` instead of reading every retained claim batch. Under load those scans could pick parallel plans and exhaust dynamic shared memory. The `claim_batch_job_index` patch installs the index function and takes no table locks; claim prune builds the index on each claim partition right after emptying it, so every partition gains it within one claim-ring cycle and live claims are never blocked behind a build.
- Retried queue-storage jobs and jobs carrying metadata complete by their compact claim batch and item index, instead of reading every retained claim batch.

**Rescue, events and admin reads**

- Heartbeat, deadline and callback rescue of lease-backed attempts reach the stale attempts behind a full batch of attempts whose retained ready bodies are ambiguous. Before, every pass selected the same ambiguous attempts first, left them running, and rescued nothing else.
- A `Completed` lifecycle event, and the job a `Completed` follow-up enqueue spec receives, no longer carry the finished attempt's `heartbeat_at` and `deadline_at`; both are `None`, as a read of the completed job returns them.
- Admin job lists and kind counts work on a queue-storage schema installed with more claim slots than the default, instead of failing with `invalid physical claim slot`, and no longer read other claim slots' receipt closure partitions when they read one slot's open claims.
- Queue-storage job lists without a kind filter select receipt candidate IDs before loading their ready bodies. Kind filters still apply before the limit.

### Changed

- Compatibility COPY inserts (`insert_many_copy` in Rust and Python) take a queue lane's enqueue lock when the batch writes that lane, instead of at the batch's first ready row. The lock is transaction-scoped, so it is still held until the transaction ends. Many producers enqueuing large batches into one queue no longer queue behind each other's whole batches, and promotion into a busy queue waits less. The batch goes through the new `awa.insert_jobs_compat_batch` function, which prepares job ids, unique claims and payloads first, then registers each new lane and reserves each lane once, in the sorted `(queue, priority, shard)` order every other producer uses. Each call checks its connection for the function and keeps the per-row path where it is missing or not executable. Results are unchanged: input order, with jobs whose unique key is already claimed skipped.
- A batch through `awa.insert_jobs_compat_batch` takes all its unique claims before any lane lock, while per-row inserts (generic `insert_many`, single-job inserts, 0.6.14 and earlier binaries) take each job's claim and lane together, lane by lane. A per-row transaction that already holds a lane the batch needs and then inserts a job whose unique key the batch has claimed but not committed now fails with `deadlock detected` (SQLSTATE `40P01`) instead of skipping the job or failing with a unique violation (`23505`). Retry the aborted transaction. Batches through the function do not deadlock with each other this way.

### Added

- `insert_many_skip_unique_conflicts` (Rust) inserts a batch through `awa.insert_jobs_compat_batch`, skipping jobs whose unique key is already claimed, and returns the inserted rows in input order. Use it instead of retrying a failed `insert_many` one job at a time, which takes the lane lock once per job. It needs the `insert_jobs_compat_batch` patch. Generic `insert_many` is unchanged.
- `enqueue_lane_contention_test` benchmarks many producers on one lane, with a nightly gate on per-lane throughput and lock hold time.

## [0.6.14] — 2026-11-08

Recommended for everyone on 0.6.13 using queue storage. A busy queue no longer stalls promotion of the others, prune backs off cleanly under lock contention, new queue lanes are usable by every runtime role, and jobs no longer stay running, run again or get misreported after priority aging.

### Upgrading

- The schema stays at v40. Run `awa migrate` once as the schema owner while workers keep running. It applies the repeatable `definer_lane_provisioning` patch and repairs existing lane sequences. Then roll workers, producers and admin processes in any order. Binaries can roll back to 0.6.13 with the patch left in place.
- `awa storage lane-sequences` checks lane-sequence ownership and grants and exits with code 2 while problems remain; `--repair` fixes what the connected role may.
- External migration runners: apply `R__definer_lane_provisioning.sql` after `V40` (`awa migrate --extract-to <dir> --pending`), replacing all `R__` files together, then run `awa storage lane-sequences --repair` as the migration role. See the [upgrade guide](docs/upgrade-0.6.13-to-0.6.14.md).

### Fixed

**Lane ownership**

- A queue lane created by one runtime role is usable by every other runtime role. Before, its sequences belonged to whichever role created it, so other roles could fail with `permission denied for sequence`, for example after `enqueue_shards` was raised. New lanes now belong to the schema owner with the head table's grants, and migration repairs existing ones, including when the migration role can only `SET ROLE` to the schema owner.

**Promotion, prune and lock contention**

- A busy queue lane no longer delays promotion of other queues or maintenance. Promotion waits 50 ms for a lane's locks and hands a busy lane to a dedicated waiter.
- A lock timeout anywhere in prune or terminal rollup rolls the attempt back and reports `Blocked` instead of an error, and prune backs off.
- Admin job details, lists and kind counts release partition locks between read phases, so they block prune less, and lists of retained history do less repeated work.
- A read cancelled while starting its transaction closes its connection, so the next caller cannot inherit an open transaction and its partition locks.

**Job correctness after priority aging and recovery**

- Physical priority aging no longer moves a ready row that is being claimed. Before, it could tombstone the body of a just-claimed job, after which completion, cancellation and rescue failed and the claim ring stayed pinned. Bodies left tombstoned by older releases are recovered when the job is read, completed, cancelled or rescued.
- Reads and callbacks find attempts that priority aging or a priority change moved, and admin lists and kind counts include running jobs whose claims have not yet become leases.
- Failed jobs keep their arguments, attempt limit and lane in terminal history; manual retries and DLQ moves keep the correct priority and lane.
- Schema-only admin clients work with ready rings larger than the default.

**Workers**

- Workers retry lock contention during finalization for up to 120 s with the handler's saved result, keeping the job's heartbeat until the outcome settles.
- Graceful shutdown waits for pending finalization before stopping heartbeat and maintenance, within the existing drain timeout.
- A completion or callback wait that has committed is no longer reported as failed if re-reading the job for its lifecycle event fails.

**CLI**

- `awa migrate` rejects `--sql` combined with `--extract-to`. Before, `--sql` silently won and nothing was extracted.

### Since 0.6.14-alpha.1

For testers of the prerelease, these fix behaviour that alpha.1 introduced:

- Promotion no longer holds a lane's locks while waiting for PostgreSQL's cluster-wide NOTIFY lock at commit, where under concurrent enqueues the 50 ms lane timeout could expire and roll the batch back. Claimers are woken once after the lanes commit.
- With lifecycle hooks registered, a completion builds its event from the job the worker holds instead of re-reading it, which cost about 24 round trips per completion.
- Rescue skips an attempt with ambiguous retained bodies and logs an error on each pass, instead of failing the whole rescue.

### Added

- `ClientBuilder::prune_lock_timeout` in Rust and `queue_storage_prune_lock_timeout_ms` in Python configure prune's lock wait. The timeout applies to each acquisition; queue prune can wait on up to eight partition locks. The default remains 10 ms. `awa.maintenance.prune.attempts` includes the outcomes `backed_off` and `error`.
- `awa.maintenance.promote.lane_contention` records promotion lane contention and waiter outcomes by queue.
- Multi-runtime soak coverage for killed workers, shard changes, mixed Rust and Python workloads, durable recovery evidence and ring-lock diagnostics.

### Changed

- Python CI tests 3.14 and 3.15. Older interpreter versions remain best-effort; package classifiers list 3.14 and 3.15.
- Operator guidance explains that longer rotation intervals reduce churn while increasing rows and index size per slot and extending retained history.

### Known limitations

- A job-detail read can race prune and briefly show a completed job as available or missing; reading again returns the current state.
- If an older release's aging race left two equally plausible bodies for one running attempt, reads and direct transitions of that attempt fail with an ambiguity error, and rescue skips it until the bodies are repaired.
- A worker completing a job with the body it already holds does not retire an unclaimed copy left by older aging, so that copy can run again (at-least-once).
- Direct API calls that recover a body left by older aging (external callback complete, fail and retry; admin cancel and retry; completion by ID) can fail with SQLSTATE 55P03 under contention and must be retried. Workers retry automatically.
- The installed `terminal_jobs` SQL view still reports the stored priority for older rows whose priority changed.

## [0.6.14-alpha.1] — 2026-11-07

Prerelease of 0.6.14; every change is listed under 0.6.14.

## [0.6.13] — 2026-11-06

Ships everything in 0.6.13-alpha.1, which soaked without regressions, plus the batch-enqueue deadlock fix.

### Added

- **`QueueStorage::rescue_stale_heartbeats_with_dlq`,
  `rescue_expired_deadlines_with_dlq` and `rescue_expired_callbacks_with_dlq`**
  take a per-queue predicate and dead-letter exhausted attempts in the rescue
  transaction. The existing methods keep their behaviour and fail exhausted
  attempts into `done_entries`.

### Changed

- **One LISTEN connection per runtime, idle polling backoff and bounded claims.** Queue dispatchers share a listener and a runtime-wide claim semaphore (`ClientBuilder::max_concurrent_claims`). Empty queues back off from `poll_interval` to a 2 s cap when notifications are available; poll-only runtimes retain `poll_interval`. Saturated claim waits observe shutdown, and late-starting tasks receive cancellation. Dispatcher exit and worker drain share the timeout. The idle cap stays internal to preserve `QueueConfig` struct literals.

### Fixed

- **Batch enqueues take their lane locks in one order, so they no longer
  deadlock with each other.** On queue storage every enqueue holds a
  transaction-scoped advisory lock on its `(queue, priority, shard)` lane
  until commit. `insert_many` and `insert_many_copy` took those locks in input
  order, while `QueueStorage` batch enqueues took them in sorted order, so a
  batch spanning several queues listed differently from another batch or from
  a `QueueStorage` enqueue could deadlock with it (`40P01`). The two insert
  paths now sort rows by `(queue, priority, shard)`, queues compared bytewise,
  before they reach `awa.insert_job_compat`, the order `QueueStorage` already
  used, and still return rows in input order. Job ids within a batch that
  spans several lanes are now allocated lane by lane rather than in input
  order, and when an `insert_many_copy` batch carries the same unique key in
  more than one lane, the row in the lowest lane is the one inserted. A
  transaction that enqueues with separate statements still takes locks in
  statement order; see "`deadlock detected` while enqueueing" in the
  troubleshooting guide.

- **Scheduled, snoozed and retry-after jobs are no longer held back while
  the maintenance leader is overloaded.** The leader backs a maintenance
  branch off for a cooldown after its body repeatedly runs longer than its
  interval, and promotion, which makes every scheduled, snoozed and
  retry-after job runnable, was backed off like the rest, so those jobs
  waited out the cooldown. The cooldown also counted ticks, and under CPU
  starvation the leader loop services a branch's tick only once many other
  branches have run, so a cooldown of a few seconds could last several times
  that. Promotion is no longer backed off; other branches (rescues, cleanup)
  back off for a wall-clock duration of twice the overrunning body, capped
  at five seconds. The leader's interval timers also delay rather than burst
  after a stall, so a stall no longer queues back-to-back catch-up ticks. This replaces the fixed 120-tick
  cooldown, which a branch running slightly over 1.5x its interval re-armed
  on every overrun: a 250 ms promote interval could go quiet for about 30
  seconds at a time, and slow rescue sweeps left expired claims waiting the
  same way.
- **Queue storage's admin counts no longer report zero running jobs when
  lease-claim receipts are on** (backport of the running count in
  [#581](https://github.com/hardbyte/awa/pull/581)). A receipt-held job is
  running without a `leases` row, and `admin::queue_overviews`,
  `admin::queue_overview`, `admin::state_counts` and `admin::state_timeseries`
  counted only `leases`, so `awa queue stats` and the web UI showed no running
  jobs. They now also count open receipt claims.

- **`insert_many` no longer fails above 5,461 jobs in one call.** It built one
  multi-row `VALUES` statement with twelve bind parameters per job, so a call
  with more than 5,461 jobs exceeded PostgreSQL's 65,535-parameter limit and
  failed client-side with `too many arguments for query` before reaching the
  server. It now sends one array per column through a single fixed-text
  statement over `unnest(...)`, so the bind count no longer depends on the
  batch size and the call keeps its single-statement, caller-transaction
  semantics, on both the canonical and queue-storage engines.

- **Cancelling a running job on queue storage no longer races its completion
  into two outcomes.** The fast completion path closes a receipt-backed
  attempt under the attempt's completion lock without locking its claim, and
  the receipt-only cancel locked only the claim. A cancel that ran while a
  completion was uncommitted saw no closure, so both committed: the attempt
  ended with a completion record and a cancelled terminal row. Cancel now finds
  the open attempt, takes its completion lock, and looks again, so it either
  sees the completion and leaves the job alone or cancels it before the
  completion can close it. Bulk cancels take every attempt's lock up front, in
  the order completion batches use.

- **A duplicate unique follow-up no longer fails the job that triggered it.**
  A follow-up enqueued with unique options whose key already existed returned
  `UniqueConflict` from the follow-up `INSERT`, which aborted the trigger's
  transaction and rolled back its completion (or other outcome). The executor
  does not retry a unique conflict, so the attempt stayed running until
  rescue, re-ran, hit the same conflict, and failed once its attempts ran out;
  every trigger after the first failed this way. Each follow-up with unique
  options now inserts under a savepoint, and a conflict on the unique-key
  index skips the duplicate while the trigger commits and the existing
  follow-up stands. Any other unique violation, or a follow-up without unique
  options, still rolls the trigger back as before. The guide to
  [lifecycle hooks](docs/lifecycle-hooks.md) gains a section on deduplicating
  follow-ups.
- **Python: a query after `AsyncClient.migrate()` no longer stalls until the
  pool's acquire timeout.** `migrate()` and `install_queue_storage(reset=True)`
  ran the migrations on a throwaway tokio runtime but with the client's pool,
  so connections the pool opened during the migration went back into it tied
  to a runtime that no longer existed. A later query that picked one up could
  wait 30 s and fail with `pool timed out while waiting for an open
  connection`. The migrations now run on the bindings' shared runtime.
- **JSON floats read back from Postgres exactly.** Postgres prints `jsonb`
  numbers in plain decimal, and serde_json's default parser could round a long
  one to the neighbouring `f64`, so a value like `5.130112436774387e-96` in job
  args read back one unit in the last place off. serde_json's
  `float_roundtrip` feature is now enabled in the workspace and in the Python
  bindings.
- **Poison jobs no longer cycle forever.** A job whose worker crashed or hung
  on every attempt was rescued back to `retryable` indefinitely: stale-heartbeat
  and deadline rescue never checked `max_attempts`, so `attempt` climbed past
  it, on both storage engines. Those rescues now fail the job once its attempts
  are spent, as callback timeouts already did, and on queue storage a
  DLQ-enabled queue dead-letters it with reason `heartbeat_stale` or
  `deadline_exceeded`. On queue storage the DLQ row is written in the rescue
  transaction for all three rescue reasons, including `callback_timeout`; an
  exhausted callback timeout used to be moved in a second transaction, so an
  error between the two left it `failed` outside the DLQ. A job with `max_attempts = 1` that loses its worker
  therefore fails instead of running again. Rescue runs on the maintenance
  leader, so the new behaviour applies once the leader runs this release.
- **A hung handler no longer holds its worker slot forever.** Rescue only
  signalled cancellation, so a handler awaiting something that never resolved
  kept its permit: the rescued retry could not be claimed, and a one-worker
  queue stopped. A handler still running two `deadline_rescue_interval`s past
  its deadline is now dropped at its next `.await`. Blocking code that never
  yields still keeps its slot. For a Python handler the slot is freed but the
  coroutine is not cancelled: it keeps running on its event loop until it
  finishes on its own, so concurrency can exceed `max_workers` while it runs.
  Python handlers should enforce their own timeouts.
- **`JobResult::RetryAfter` now respects `max_attempts`.** `RetryAfter` is
  documented as "this attempt failed; try again after delay X", but the
  completion path never checked exhaustion: a handler that always returned
  `RetryAfter` looped forever, invisible to the DLQ and appending nothing to
  `errors`. A retry requested on the final attempt (`attempt >= max_attempts`)
  now exhausts the job exactly like a retryable error: `failed` state,
  `dlq_reason = max_attempts_exhausted` when the DLQ is enabled, an `Exhausted`
  event, and `on_exhausted_enqueue` follow-ups. A job gets exactly
  `max_attempts` executions regardless of which failure primitive reports
  them. **If your `RetryAfter` loop meant "not yet" rather than "failed",
  return `Snooze`**, which reschedules without consuming the attempt budget.
  `TestClient` (Rust and Python) mirrors the bound.
- **A panicking Rust handler is a failed attempt.** A panic used to escape the
  job task: the job stayed registered in flight, kept heartbeating until its
  deadline rescued it (5 minutes by default), and its message was lost. The
  executor now catches the panic and records `handler panicked: <message>` as
  a retryable failure.
- **An attempt lost to a stall or to a shutdown whose drain timed out now
  costs a real attempt.** On the final attempt, the stale-heartbeat or deadline
  rescue fails the job, or dead-letters it when the queue's DLQ is enabled,
  instead of running it again, and the late completion from the original
  worker is ignored as stale. A job with `max_attempts = 1` therefore fails if
  a deploy's drain times out while it runs.
- **Rescue-exhausted jobs are reported as rescues.** When a stale-heartbeat,
  deadline or callback-timeout rescue takes a job's last attempt, the job emits
  the `Rescued` lifecycle event and runs its `on_rescued_enqueue` follow-ups,
  not `Exhausted` and `on_exhausted_enqueue`. It counts in `maintenance_rescues`
  (and in `dlq_moved` when queue storage dead-letters it) rather than
  `jobs_failed`, as an exhausted callback timeout already did.

- **Duration and size histograms now export explicit bucket boundaries, so latency quantiles are accurate.** `awa.job.duration`, `awa.dispatch.claim_duration`, `awa.dispatch.wake_to_claim_duration`, `awa.completion.flush_duration` and `awa.maintenance.promote_duration` record seconds but were exported with the OpenTelemetry SDK's default millisecond-scale buckets (0, 5, 10, 25, ... 10000), so almost every observation landed in the first bucket and `histogram_quantile` p50/p95/p99 were flat lines near 2.5 s, 4.75 s and 4.95 s whatever the real latency. Latency histograms now use 0.5 ms to 30 s buckets and job duration uses 1 ms to 1 h buckets. The batch-size, capacity and skipped-row histograms (`awa.enqueue.batch_size`, `awa.dispatch.capacity_available`, `awa.dispatch.claim_batch_size`, `awa.completion.flush_batch_size`, `awa.maintenance.promote_batch_size`, `awa.maintenance.rotate.skipped_rows`, `awa.maintenance.prune.skipped_rows`) now use power-of-two buckets from 0 to 8192. Quantile panels become accurate after upgrading. The `le` labels of these histograms change, so a quantile computed over a range that spans the upgrade mixes old and new buckets until the old series age out.

- **Stale-heartbeat and deadline rescue on queue storage no longer reopen an
  attempt that a worker has just completed.** The rescues checked an
  attempt's closure evidence and took its completion lock in the same
  statement. Under READ COMMITTED that statement's snapshot is taken first, so
  a completion that committed between the snapshot and the lock released the
  lock and let the rescue take it while its checks still could not see the
  completion, most easily when the rescue had waited on the claim slot's
  cursor row. The attempt then had a `completed` closure and a `rescued` or
  `deadline_expired` one, plus a retryable row, and the job ran again after it
  had finished. Each rescue now takes the completion locks in a statement of
  its own and decides in a second statement that starts after them, as the
  completion, retry and failure paths already do.

## [0.6.12] — 2026-11-05

Patch release: queue storage no longer drives PostgreSQL 18 into a backend crash when a rescue, cancel or claim close locks a claim that was just rewritten, batch completion and bulk "move failed to DLQ" on queue storage no longer fail once a batch grows past PostgreSQL's bind-parameter limit, draining a queue cancels its backlog in bulk, and cancelling or moving an available job no longer acts on one a worker has already claimed. No migration, no schema patch and no public API change. Upgrading from 0.6.11 is a rolling upgrade of processes; the rescues run on the maintenance leader and any runtime can become leader, so roll all of them.

- **Queue storage no longer drives PostgreSQL 18 into a backend crash when a
  rescue, cancel or claim close locks a claim that was just rewritten.** A
  statement that locks a row another transaction updated after the
  statement's snapshot rechecks the row (EvalPlanQual). In PostgreSQL 18
  (reproduced on 18.6) that recheck rebuilds the statement's run-time partition
  pruning state in memory that is freed when the lock runs out of rows, so a
  later pruning pass in the same statement reads freed memory and the backend
  segfaults ([pgsql-bugs report](https://www.postgresql.org/message-id/CAP2G_gGpV%3DrKMLuET4RW-qO41GvBvi6Czsjsj16eT9hAwHxG6w%40mail.gmail.com)).
  The server then restarts, dropping every connection; on managed services
  with `restart_after_crash` off it fails over instead. The deadline and
  stale-heartbeat rescues hit this whenever a heartbeat rewrote an expired or
  stale claim while they ran: they locked the claim, then marked it closed
  through a pruned scan of `lease_claims`. The rescues now update the slot's
  own claim partition, the completion, heartbeat, callback and close paths
  lock their attempts' claims in a statement that prunes nothing before the
  statements that lock them again, and cancel looks up row and batch receipts
  in separate statements.
- **Completing a large batch and moving many failed jobs to the DLQ no longer
  fail on queue storage** (backport of the writer approach in
  [#506](https://github.com/hardbyte/awa/pull/506)). The `done_entries` and
  `dlq_entries` writers bound every column of every row as its own parameter,
  so a single statement of more than 4,681 completed jobs (3,276 for rows
  written without ready-entry backing) or more than 3,449 DLQ rows exceeded
  PostgreSQL's 65,535-parameter limit and failed with
  `too many arguments for query`, rolling the whole operation back. Bulk
  "move failed to DLQ" on a queue with more than about 3,450 failed jobs
  always failed this way, and completion batches could hit it when
  `AWA_COMPLETION_BATCH_SIZE` is raised above the default of 512. Both writers
  now send one array per column through a fixed-text
  `INSERT ... SELECT FROM unnest(...)`.
- **`admin::drain_queue` on queue storage cancels a backlog in bulk and
  drains every stripe of a striped queue.** It found the queue's jobs through
  a lookup that read every retained ready row, then cancelled them one at a
  time, several statements each, inside one transaction, so draining a large
  backlog took minutes and held its row locks throughout. Scheduled,
  retryable and available jobs are now cancelled 1,000 at a time, each chunk
  one short transaction, with available rows read from each lane's claim
  cursor; waiting jobs are still cancelled one by one and running jobs are
  left alone. Jobs enqueued after the drain starts are left for a later
  call, so draining a queue that keeps receiving work still returns, and
  the drain moves each lane's claim cursor past the rows it cancelled, so
  workers reach the jobs behind them on their next poll. It also matched
  only the logical queue name, so draining a striped queue cancelled
  nothing; it now covers every stripe. `awa queue drain` and the Python
  `drain_queue` use this path.
- **Cancelling, moving or reprioritising an available job on queue storage
  no longer acts on a job a worker has already claimed.** A claim moves its
  lane's claim cursor past the rows it took only after it commits, so
  briefly, or until the next claim if that advance failed, a claimed row
  still looked available. A cancel in that window wrote a cancelled terminal
  row while the attempt kept running, and a move re-enqueued a job that was
  already executing. A claim still in progress was missed the same way.
  These paths now wait for any claim in progress on the job's lane and skip
  rows a claim has recorded, the same evidence claims use to skip them, so a
  cancel reaches the running attempt instead.

## [0.6.11] — 2026-11-04

Patch release: promotion of due scheduled and retryable jobs on queue storage no longer stalls once a backlog builds. No migration, no schema patch and no public API change. Upgrading from 0.6.10 is a rolling upgrade of processes; promotion runs on the maintenance leader, so a stalled deployment recovers once the leader runs 0.6.11, and any runtime can become leader, so roll all of them.

- **Promoting due jobs no longer fails above 3,640 rows in one tick on queue
  storage** (backport of the writer changes in
  [#506](https://github.com/hardbyte/awa/pull/506)). Promotion moves up to
  4,096 due rows from `deferred_jobs` to `ready_entries` in one statement that
  bound every column of every row as its own parameter, so a tick with more
  than 3,640 due rows exceeded PostgreSQL's 65,535-parameter limit and failed
  with `too many arguments for query`. The transaction rolled back and every
  later tick failed the same way, so the backlog only grew. The ready,
  ready-segment and deferred writers now send one array per column through a
  fixed-text `INSERT ... SELECT FROM unnest(...)`, which also lifts the same
  ceiling from batch enqueues of more than 3,640 ready or 4,095 scheduled jobs.

## [0.6.10] — 2026-11-03

Patch release: the admin UI's queue and dashboard reads on queue storage no longer scan retained history, a request the UI's client abandons no longer holds a database connection, and restricted runtime roles can run against pre-provisioned queue lanes without schema `CREATE`. No new migration version and no public API change. The lane fix ships as the idempotent schema patch `prepared_lane_sequences`: `awa migrate` applies it on any v040 database; external runners apply the exported `R__prepared_lane_sequences.sql` (and `R__wait_free_dirty_marks.sql` if not yet applied) after `V40`. The admin fix needs no migration; only the process serving the admin UI has to run 0.6.10 to get it. Upgrading from 0.6.9 is a rolling upgrade: run `awa migrate` once as the owner while traffic continues, then roll processes; binaries can roll back to 0.6.9 with the patch left in place. [`docs/upgrade-0.6.9-to-0.6.10.md`](https://github.com/hardbyte/awa/blob/v0.6.10/docs/upgrade-0.6.9-to-0.6.10.md) has the steps, the rollback, and the optional procedure for running the runtime role without schema `CREATE`.

- **Admin queue and dashboard reads on queue storage follow live work, not
  retained history** (backport of
  [#580](https://github.com/hardbyte/awa/pull/580) and
  [#581](https://github.com/hardbyte/awa/pull/581)). `admin::queue_overviews`
  (the queues and dashboard pages, `awa queue stats`, the Python queue
  stats), `admin::state_counts` and `admin::state_timeseries` built every
  current job through `terminal_jobs`, expanding each completion batch and
  joining it back to `ready_entries`. One production deployment saw the queue
  overview take 22 s, re-run by an open dashboard. They now read the
  live-work tables the worker queue gauges read, count completions from
  batch sizes, and bound the timeseries to rows finalised within its window.
  Counts are unchanged; `running` keeps 0.6's leases-only definition.
  Available rows are found through each lane's segments from its claim
  cursor, as claims find them; recent deletions are matched against each
  affected completion batch once, rather than one batch scan per deletion;
  and queue lag is the age of the oldest available job, as in 0.6.9. A new
  test, `admin_read_cost_test`, serves every endpoint the UI polls against
  ten times more retained history and fails if a read does more work
  (backport of [#584](https://github.com/hardbyte/awa/pull/584)).
- **DLQ retries, purges and moves refresh the admin UI's cached stats and
  queue counts**, which otherwise lagged a DLQ action by up to the cache TTL.
- **The admin UI's runtime page no longer shows the storage transition card
  once the transition is finalized.** It stayed up on every cluster running
  queue storage; it now appears only while an engine is prepared or a mixed
  transition is in flight.
- **`admin::cancel_by_unique_key` and `cancel_by_unique_key_tx` on queue
  storage find running jobs and no longer read retained history**
  (backport of [#586](https://github.com/hardbyte/awa/pull/586), for
  [#585](https://github.com/hardbyte/awa/issues/585)). The candidate lookup
  built every available ready row before filtering by key, reading every
  retained ready row, and missed jobs running on receipt claims not yet
  materialised into `leases`. Given a queue, the lookup now starts from that
  queue's ready rows holding the key and checks only those jobs for being
  available, leased or on an open receipt claim; with no such row, only the
  `deferred_jobs` key index is read. Its cost follows that queue's own rows,
  not other queues' backlogs or the claim ring, which a long-running job can
  keep from pruning. On a database with 180,000 retained ready rows and
  120,000 retained claims in other queues, a lookup took 1.7 ms for a missing
  key and 11–20 ms to cancel a scheduled or available job, against 2.8 s for
  0.6.9. Without a queue, available rows are walked lane by lane from the
  claim cursor and open receipt claims are read in full. Two older gaps are
  also closed: on a striped queue, a lookup by the logical name now finds
  jobs in its `queue#N` lanes, and a job that finishes between the lookup
  and the cancel returns `None` instead of `JobNotFound`, as on the
  canonical engine.
- **A request the admin UI's client abandons no longer holds a database
  connection for good.** When a browser closes a tab or a proxy times out,
  the server drops the request but Postgres keeps running its query. If that
  query then ended in an error, sqlx closed the connection with a hard close,
  which hangs on a TLS connection, so the pool lost that slot permanently.
  `awa serve` now sets a 60-second statement timeout (`--statement-timeout`,
  `AWA_STATEMENT_TIMEOUT`), recovers such connections when they are released,
  and on Postgres 14+ sets `client_connection_check_interval` so a query stops
  when its connection closes. Applications embedding `awa_ui::router` can
  build their pool with the new `awa_ui::admin_pool_options` and
  `admin_connect_options`. Backport of
  [#583](https://github.com/hardbyte/awa/pull/583).
- Restricted runtimes can use pre-provisioned queue lanes without schema
  `CREATE` ([#501](https://github.com/hardbyte/awa/pull/501)). The lane helpers
  skip `CREATE SEQUENCE` when the sequence exists and raise SQLSTATE `42501`
  with a provisioning hint when it does not. Ships as the schema patch
  `prepared_lane_sequences` (applied by `awa migrate`, exported as
  `R__prepared_lane_sequences.sql`). The patch replaces only those three
  functions on each queue-storage schema, so it takes no table locks and
  can run under live workers. New `awa storage prepare-queue` and
  `QueueStorage::prepare_queue` provision lanes as the migrator; runtime roles
  need `USAGE, SELECT, UPDATE` on the lane sequences.
- **The patch counts as applied only when every queue-storage schema has it.**
  The `prepared_lane_sequences` probe checked the helpers in `awa` alone, so a
  custom schema restored or prepared with the 0.6.9 helpers was missed once
  `awa` passed, and a database whose only active substrate was a custom schema
  never passed. The probe now checks the installer, the refresh function and
  every substrate's helpers, and the new `migrations::schema_patch_status`
  names each schema still lacking the patch.
- **`awa migrate` reports what it did and refuses ranges it cannot honour.**
  It prints the schema version, each patch and what it updated, and storage
  routing before and after, and exits 1 naming any schema a patch could not
  reach (`migrations::run_with_report` returns the same report). `--from`,
  `--to` and `--version` are refused when applying, where they used to be
  ignored in favour of a full migration. `--sql` output is one transaction
  holding the migration advisory lock, with `ON_ERROR_STOP` guidance in its
  header; `--no-transaction` prints it bare.
- **`awa storage prepare-queue` works on lanes a 0.6.9 runtime created.**
  A runtime role with schema `CREATE` owns the lane sequences it creates, and
  provisioning such a lane as the owner failed with `permission denied for
  sequence`: the head tables' `BEFORE INSERT` triggers run even for a row that
  `ON CONFLICT DO NOTHING` then skips. Lane provisioning now inserts only the
  rows that are missing.
- **The 0.6.9 → 0.6.10 → 0.6.9 journey is a release gate.** A new CI job,
  `patch_upgrade_rehearsal_test` on PostgreSQL 17 and 18, installs with the
  released 0.6.9 CLI, runs the released 0.6.9 Python worker as a separate
  runtime role in a custom queue-storage schema, patches under that traffic
  (no Awa table locks, sequence positions kept, routing unchanged), runs both
  versions together through worker kills, retries and scheduled jobs, rolls
  back to 0.6.9, tightens privileges, and accounts for every job.

## [0.6.9] — 2026-09-14

Patch release: widens the OpenTelemetry dependency range so `awa-metrics` unifies with a consumer's existing provider. No schema change, no migration, and no public API change.

- **`awa-metrics` no longer forces a second `opentelemetry` crate into the dep
  tree ([#489](https://github.com/hardbyte/awa/pull/489)).** The workspace
  pinned `opentelemetry` and `opentelemetry_sdk` at `0.31`, so a consumer held
  on 0.30 — for example through `tower-otel-http-metrics` 0.16, which has no
  0.31+ release — ended up with both versions linked. Each carries its own
  static global meter provider, so `AwaMetrics::from_global()` bound to a no-op
  provider and the queue depth and lag gauges recorded into a void. The range is
  now `>=0.30, <0.33` for both, letting Cargo unify on whatever the consumer
  already has; the metric API surface used by `awa-metrics` (`Meter`, `Counter`,
  `Gauge`, `Histogram`, `UpDownCounter`, `KeyValue`, `global::meter`) is
  identical across 0.30–0.32. Backported from main; thanks to
  [@sean-nzl](https://github.com/sean-nzl).

## [0.6.8] — 2026-09-14

Patch release: removes a deadlock class in the canonical engine's admin dirty-key triggers, retries finalize transactions aborted as deadlock victims, and carries the Python shutdown and storage-upgrade fixes merged since 0.6.7. No new migration version and no public API changes. The fix arrives as the idempotent schema patch `wait_free_dirty_marks`: `awa migrate` applies it on any v040 database; external runners apply the exported `R__wait_free_dirty_marks.sql` (from `awa migrate --sql`, `--extract-to`, or `awa.schema_patches()`) after `V40`.

- **Job transitions no longer wait on, or deadlock through, the admin
  dirty-key triggers ([#492](https://github.com/hardbyte/awa/issues/492)).**
  The v006 triggers marked touched queues and kinds with `INSERT ... ON
  CONFLICT DO NOTHING` into keyed tables, which blocks on another transaction
  whenever the keyed row is uncommitted; the 2 s drain and the
  `TRUNCATE`-based 60 s refresh made that routine, and two re-schedules in one
  queue could deadlock through it. The fix ships as the idempotent schema patch
  `wait_free_dirty_marks` (the same SQL as awa 0.7 migration v046): append-only
  `admin_dirty_queue_marks` / `admin_dirty_kind_marks` with no unique index, a
  drain that deletes only the visible rows it recounts, and a refresh without
  `TRUNCATE`. `awa migrate` applies the patch on a v040 schema; `awa migrate
  --sql` / `--pending` / `--extract-to` and Python `awa.schema_patches()` emit
  it as a repeatable `R__wait_free_dirty_marks` script for external runners; it
  records no `schema_version` row. The executor re-runs a finalize transaction
  aborted as a deadlock victim (SQLSTATE `40P01`, three retries) instead of
  leaving the job to heartbeat rescue, counted in
  `awa.completion.deadlock_retry`.

- Python `Transaction`/`SyncTransaction` handles released without commit or
  rollback now roll back on the shared Tokio runtime. Previously garbage
  collection on a thread without a runtime, including interpreter exit,
  panicked inside sqlx's pool return with "this functionality requires a
  Tokio context".

- Emit a stderr diagnostic after five seconds of stalled native Python shutdown,
  while retaining the join required for safe interpreter finalization.

- Rejected Python bridge operations preserve client lifecycle state; canonical
  callback retries notify dispatchers only when their transaction commits.

- Cancel and join native Python async operations and join completion callbacks
  before interpreter finalization, preventing the reproduced exit-time CPython 3.12 SIGSEGV after
  `await client.close()`. Worker shutdown and pool close remain explicit.

- Preserve canonical callback resolution during mixed storage transitions (#462),
  including handler registration, polling, lease fencing, and transaction rollback.
- Add `storage enter-mixed-transition --quiesced` for a stopped fleet, refusing
  fresh runtime snapshots with millisecond precision, checking the complete target
  substrate, and retaining canonical backlog/finalize gates (#457).

## [0.6.7] — 2026-08-31

Patch release: CLI help credential redaction and dependency security updates. No migrations, schema changes, or public API changes.

### Fixed

- **CLI help no longer prints credential-bearing environment values.** `awa --help` hides the resolved `DATABASE_URL`, while `awa serve --help` and `awa callbacks serve --help` hide `AWA_CALLBACK_HMAC_SECRET`. The CLI still accepts those environment variables and help still identifies their names, but their current values are never rendered.
  - If help output containing either value was captured in CI logs, support bundles, or another shared location, remove that output where possible and rotate the exposed credential.
- **The bundled admin UI no longer resolves the vulnerable `seroval` 1.5.1 transitive dependency.** The lockfile now selects 1.5.6, which contains the fix for [GHSA-mv8w-475r-vwqw](https://github.com/advisories/GHSA-mv8w-475r-vwqw).
- **Rust transitive dependencies with published advisories are updated.** Both Rust lockfiles now select `h2` 0.4.16, fixing [RUSTSEC-2026-0258](https://rustsec.org/advisories/RUSTSEC-2026-0258), and the main workspace selects `crossbeam-epoch` 0.9.20, fixing [RUSTSEC-2026-0204](https://rustsec.org/advisories/RUSTSEC-2026-0204).

## [0.6.6] — 2026-08-14

Patch release: storage-transition convergence and recovery fixes. No migrations, schema changes, or public API changes.

### Fixed

- **Canonical drain now converges for perpetually snoozing jobs ([#456](https://github.com/hardbyte/awa/issues/456)).** During a storage transition, the re-scheduling completions of canonical-claimed jobs — snooze, retry backoff, and `RetryAfter` — now leave the canonical plane instead of writing back into canonical `scheduled_jobs`. Ordinarily the fresh-id successor lands in the active queue-storage schema's `deferred_jobs`, with any required unique claim acquired and attempt/errors/progress preserved. If a newer duplicate already owns that claim, the successor is recorded as cancelled terminal evidence with `rescheduled as duplicate`; it is never executable, and the newer holder wins. A workload whose handlers snooze on every run previously replenished `canonical_live_backlog` forever, so `awa storage finalize --wait` could never pass. No migration and no schema change: the fix is runtime SQL against the existing v040 schema, because the clusters that need it are exactly the unfinalized ones. Stale-completion protection is unchanged — the take of the canonical row carries the same `state = 'running' AND run_lease = $n` guard, the successor takes a fresh id, and the transaction holds `FOR SHARE` on the transition singleton so a concurrent `storage abort` cannot restore canonical routing between the routing decision and the insert. Maintenance leaders also promote the other plane's deferred backlog for the duration of the transition, so drain and post-flip execution no longer depend on which runtime holds leadership. The rescue sweeps (stale heartbeat, expired deadline, callback timeout) mirror the same way, covering both planes from either leader, so jobs mid-flight at the routing flip no longer stay stuck `running` — the issue's related sharp edge. The canonical rescue and terminal completion writes (fail, cancel, callback park) now target the physical tables directly: the `awa.jobs` compatibility view routes to the queue-storage plane once routing flips, so writing through it silently missed canonical rows and wedged the very jobs the drain must retire — callback-carrying jobs above all, since they deliberately stay canonical.

## [0.6.5] — 2026-07-29

Patch release: one telemetry fix. No migrations, no schema changes, no API changes.

### Fixed

- **Dispatcher and heartbeat traces no longer grow for the lifetime of the worker ([#449](https://github.com/hardbyte/awa/issues/449)).** `Dispatcher::run` and `HeartbeatService::run` own loops that run until shutdown, so instrumenting them kept one span open and accumulated every poll, claim, lease heartbeat, and progress flush under it. Traces grew past backend size limits — Tempo rejected them with `TRACE_TOO_LARGE` at 5 MB — and the export pressure surfaced as `BatchSpanProcessor` timeouts. Neither loop is instrumented now, and each poll and heartbeat tick is its own `debug`-level trace root. Poll spans keep their `queue` attribute.
  - The roots are `debug` rather than `info` so the fix does not trade one telemetry problem for another: they tick whether or not there is work, and at the default 200 ms `poll_interval` an info-level poll span would open ~5 traces/s per queue-claimer recording "claimed nothing". An `info` pipeline creates neither span; empty polls stay visible through `awa.dispatch.empty_claim` and the claim-duration histogram, which are metrics. Raise your filter to `debug` for the dispatcher target when you want them.
  - Span names are unchanged on this line. The 0.7 fix additionally renames these roots to follow OTel messaging conventions and adds `messaging.destination.name` alongside `queue`; neither is backported, because this line has no ADR-039 producer-side conventions for them to pair with and a patch release should not move or grow telemetry names.
  - Both PG-listener fallback paths now log `queue` explicitly and log at `warn`: poll-only is degraded but working, and is the expected path under a transaction-mode pooler ([#374](https://github.com/hardbyte/awa/issues/374)).
  - No schema, migration, or API change. Backport of [#450](https://github.com/hardbyte/awa/pull/450); the regression test added there covers the same code on `main` (0.6 predates the span-exporter harness it uses).

## [0.6.4] — 2026-07-22

Patch release: one performance fix for idle background IO. No migrations, no schema changes, no API changes.

### Performance

- **Idle rings no longer `TRUNCATE` empty sealed slots every maintenance tick.** On an idle queue the ring-rotation cursor keeps advancing over empty slots, so `prune_oldest` / `prune_oldest_leases` / `prune_oldest_claims` re-took `ACCESS EXCLUSIVE` and issued a destructive `TRUNCATE` on an already-empty oldest sealed slot every rotation tick — continuous relfilenode churn and WAL-flushing commits with **zero jobs in the system**. That idle DDL is near-free on local-SSD Postgres but saturates the IO path on disaggregated-storage engines such as AlloyDB, inflating p99 latency for co-located application queries. Each prune now proves the target slot's child partitions are empty before locking and returns `PruneOutcome::Noop` when there is nothing to reclaim.
  - Non-empty slots still follow the same lock + liveness-recheck + truncate path, so no reclamation is skipped and no data can be stranded; the queue prune also keeps its per-slot `queue_terminal_live_counts` cleanup on the skip path so exact per-queue counters cannot drift.
  - Measured on an idle queue against AlloyDB Omni (30 s window, zero jobs): destructive `TRUNCATE`s dropped from ~82/s to 0 and WAL generation fell ~50×.
  - This is the prune-side equivalent of the rotation-side idle-skip that ships in 0.7.0 ([#409](https://github.com/hardbyte/awa/pull/409)).

## [0.6.3] — 2026-07-15

- **Migrations no longer require superuser ([#431](https://github.com/hardbyte/awa/pull/431)).** The queue-storage migrations (v024 onward) discovered substrate schemas by probing every schema in `pg_namespace` with `to_regprocedure()` / `to_regclass()`, which raise `permission denied for schema pg_toast` (or any other schema the role lacks `USAGE` on) for a non-superuser — so on managed Postgres, where the migration role owns the `awa` schema but is deliberately not a superuser, provisioning failed at migration v024. Discovery now uses shared-catalog lookups (`pg_proc` / `pg_class`) gated on `has_schema_privilege()`, which never trigger per-schema permission checks. No behavioural change for superusers or single-schema installs; on shared databases, discovery is additionally restricted to substrates the role can actually access. A regression test now runs the full migration chain as a `NOSUPERUSER` owner role, so the scan idiom cannot creep back in. If you run migrations through your own external tooling, note that the historical migration files changed in place (semantically equivalent SQL); Awa's built-in migrator tracks applied versions by number and is unaffected.

## [0.6.2] — 2026-07-13

- **Safe 0.7 rolling-upgrade stepping-stone ([#392](https://github.com/hardbyte/awa/issues/392)).** The 0.6 migrator now refuses unknown newer schemas before legacy-version normalization can rewrite `awa.schema_version`. It recognizes exactly the additive v043 schema while every installed ring cursor remains in `columns` authority, allowing a 0.6.2/0.7 mixed fleet during the expand phase; it refuses after the one-way ledger-authority flip. Upgrade every 0.6.0/0.6.1 runtime to 0.6.2 before applying 0.7 migrations. If migration runs first, roll 0.7 workers promptly: crash and heartbeat rescue remain available, but deadline-based rescue for jobs claimed on v043 resumes when the first 0.7 maintenance runtime starts. Awa's built-in migrator applies v041-v043 atomically, so other sessions see v040 or v043. An external runner that commits each version separately can expose v041/v042; a restarting 0.6.2 worker refuses those transient schemas and reconnects after v043 is committed.
- **Queue-storage finalization no longer requires restarting drained workers.** Migration v040 narrows the finalization gate to live canonical-only runtimes. Pre-flip auto runtimes still report `canonical_drain_only`, but once `canonical_live_backlog()` is zero all supported writes already route to queue storage, so `awa storage finalize --wait` can complete immediately and those idle runtimes can roll normally afterward.

## [0.6.1] — 2026-07-07

Patch release: one canonical-engine bug fix, no migrations, no API changes.

### Fixed

- **Rescue sweeps no longer wedge on a unique-key conflict ([#388](https://github.com/hardbyte/awa/issues/388)) — both engines.** A unique job whose `unique_states` mask excludes `running` holds no claim while executing, so a newer duplicate can legitimately be enqueued and take the claim; returning the older job to a claiming state then conflicts. On the **canonical** engine the batched rescue UPDATEs (`running -> retryable`, `waiting_external -> retryable`) tripped the `sync_job_unique_claims` trigger (`idx_awa_jobs_unique`) and the whole 500-row sweep aborted every maintenance tick. On the **queue-storage** engine the same shape hid one layer deeper: rescue re-inserts each rescued job as a `retryable` deferred row inside one batch transaction, and `sync_unique_claim` raised `UniqueConflict` — aborting the whole batch identically. Both engines now degrade per row: clean rows rescue as before; the conflicted row is **cancelled** with an error entry (`rescued as duplicate: unique claim held by a newer job`) — the claim holder wins — via a savepoint on queue storage (terminal counters stay exact) and a row-at-a-time retry on canonical. Fallback cancellations dispatch the normal `Cancelled` lifecycle event and are counted under `awa.rescue.kind = "<kind>_duplicate_cancelled"`. Modelled in `correctness/storage/AwaCanonicalUniqueRescue.tla` (per-row config passes the `Convergence` liveness property; the batch-only config is the retained production-wedge counterexample), with end-to-end regression tests on both engines.

- **Admin UI no longer renders storage *capability* as the engine in use.** The per-instance badge previously showed `storage_capability` — which reads `queue_storage` for every capable worker even on a fully canonical cluster — and the sidebar cluster chip aggregated the same field. The runtime page now derives and shows the **effective engine** (from capability, transition role, and the authoritative transition state), noting capability only when it adds information, and the sidebar chip labels the cluster from `/api/storage` instead. Publishing the resolved engine in runtime snapshots (so the UI need not derive it) is tracked in [#391](https://github.com/hardbyte/awa/issues/391).

### Operator note

If a cluster is currently wedged (repeating `Failed to rescue ... idx_awa_jobs_unique` logs), upgrading is sufficient — the next sweep resolves it. The mask itself is still worth revisiting: on the canonical engine, a `unique_states` mask that a runtime transition can *enter* from outside (e.g. including `retryable` but not `running`) always risks superseded-job cancellations; masks closed under retry/rescue/promotion (`{}` or the full non-terminal set) avoid the conflict entirely. See the new [troubleshooting entry](docs/troubleshooting.md#rescue-fails-with-idx_awa_jobs_unique).

## [0.6.0] — 2026-07-04

The 0.6 line makes the append-only **queue-storage engine** the default and the supported substrate for high-throughput, low-bloat operation on managed Postgres. The [#169](https://github.com/hardbyte/awa/issues/169) pinned-MVCC dead-tuple gate **passed** on `main`: after the rc line removed the last dominant hot-row update path (the `queue_claim_heads` ready-segment routing cache), a 60-minute idle-in-transaction soak holds bounded queue depth through the pinned hour and drains fully in recovery. See "Benchmark evidence" below for the caveats — awa is not immune to long readers, and this release does not claim otherwise.

This entry frames the consolidated `0.5.x → 0.6.0` diff as one release; the dated `0.6.0-{alpha,beta,rc}` sections below remain the granular development log. Migrations v001–v039 apply via `awa migrate`; existing installs walk the staged transition (see "Upgrading from 0.5.x"). The SQL-only path for external migration tooling is documented in [`docs/migrations.md`](docs/migrations.md).

### What's new since 0.5.x

**Storage and durability**

- **Queue-storage engine, default-on** ([ADR-019](docs/adr/019-queue-storage-redesign.md)). Append-only `ready_entries`, `deferred_jobs`, `done_entries`, and `dlq_entries` paired with a partitioned receipt ring keep dead-tuple footprint bounded under sustained load. Replaces the row-mutating canonical engine for fresh installs.
- **No remaining hot-row update paths on the claim/complete loop.**
  - Lane cursors moved from MVCC-updated rows to Postgres sequences ([#321](https://github.com/hardbyte/awa/pull/321), v027).
  - Completion, DLQ, retry, and discard no longer `DELETE FROM ready_entries`; they route through append-only ready segments plus a tombstone ledger ([#323](https://github.com/hardbyte/awa/pull/323), v028).
  - Terminal counts append signed rows to a delta ledger that the maintenance leader folds asynchronously ([#329](https://github.com/hardbyte/awa/pull/329), v030, [ADR-026](docs/adr/026-narrow-terminal-history.md)).
  - The per-claim `queue_claim_heads` routing-cache update — the last dominant dead-tuple accumulator under a pinned horizon — is gone; claim routes through the `ready_segments` control plane instead ([#355](https://github.com/hardbyte/awa/pull/355), v039).
  - Rollup mutation and segment truncation stand down while another backend pins the MVCC horizon, then catch up once it clears ([#333](https://github.com/hardbyte/awa/pull/333)).
- **Compact receipt claim and completion batches** ([#352](https://github.com/hardbyte/awa/pull/352), v036–v038, [ADR-026](docs/adr/026-narrow-terminal-history.md)). Receipt-backed successful completions write compact terminal batches instead of one `done_entries` row per job, with compact claim-local closure, ready-segment, claim-attempt, and lease-claim evidence so stale scans stay bounded. `{schema}.terminal_jobs` is the public terminal read surface — direct SQL against `done_entries` must not assume all completed jobs are physically stored there.
- **Failed-terminal retention floor** ([#337](https://github.com/hardbyte/awa/issues/337), v032, [ADR-032](docs/adr/032-failed-terminal-retention.md)). Non-DLQ `failed` terminal rows stay retryable for at least `failed_retention` (default `72h`); rows aged past the floor are folded into a cumulative, monotonic `QueueCounts.pruned_failed` so operators have standing visibility of loss that was previously silent. `retry_failed_by_kind` / `retry_failed_by_queue` report the matched/retried/pruned split rather than silently dropping ids.
- **Receipt-plane ring partitioning** ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)). `lease_claims` and `lease_claim_closures` partitioned by claim slot, rotated by the maintenance leader, with bounded per-slot cursors so rescue/close/deadline sweeps cost work proportional to live receipts ([#349](https://github.com/hardbyte/awa/pull/349), v033–v035).
- **Per-claim deadlines** in receipts mode. Set `QueueConfig.deadline_duration` to bound a single attempt; rescue force-closes expired claims with `'deadline_expired'`.
- **Staged 0.5 → 0.6 transition tooling**: `awa storage prepare`, `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize` (with `--wait` / `--check`, [#298](https://github.com/hardbyte/awa/pull/298)), `abort`, plus a storage-transition readiness UI ([#299](https://github.com/hardbyte/awa/pull/299)). `awa migrate` owns the `awa.*` substrate DDL ([#308](https://github.com/hardbyte/awa/issues/308), v023) — it is no longer installed opportunistically, and `awa --reset --schema awa` is rejected. Fresh installs auto-finalize on first `awa migrate`. Full procedure in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

**Operator surfaces**

- **Dead Letter Queue** ([ADR-020](docs/adr/020-dead-letter-queue.md)). Per-queue `dlq_enabled` policy plus a full admin surface: `awa dlq depth | list | retry | retry-bulk | move | purge` (CLI), matching admin UI tab, Rust/Python client APIs.
- **Durable batch operations** ([#328](https://github.com/hardbyte/awa/pull/328), v029, [ADR-030](docs/adr/030-batch-operations.md)). Crash-safe, maintenance-driven bulk `set_priority` and `move_queue` over job selections, with preview/submit/list/get/cancel/purge through the HTTP API, CLI, and admin-UI Batch Ops tab; Python parity via [#332](https://github.com/hardbyte/awa/pull/332). The first-class reprioritization surface ([#307](https://github.com/hardbyte/awa/issues/307)).
- **Transaction-scoped admin cancel** ([#357](https://github.com/hardbyte/awa/pull/357)/[#358](https://github.com/hardbyte/awa/pull/358)). `admin::cancel_tx` and `admin::cancel_by_unique_key_tx` take a caller-provided `&mut PgConnection` and run the cancellation (and the cooperative `awa:cancel` NOTIFY) atomically with the caller's other work; the pool-based `cancel` / `cancel_by_unique_key` are unchanged.
- **Descriptor catalog** ([ADR-022](docs/adr/022-descriptor-catalog.md)). Code-declared queue and job-kind metadata (`display_name`, `description`, `owner`, `tags`, `docs_url`) drives admin UI labels and drift detection.
- **Cron missed-fire policy** (`coalesce` default / `catch_up`) plus **pause / resume** ([#320](https://github.com/hardbyte/awa/pull/320), v026): `POST /api/cron/{name}/pause` / `/resume`, evaluator skips paused rows, manual `trigger_cron_job` bypasses pause, `/cron` UI controls.
- **Metrics off the exact-scan path** ([#330](https://github.com/hardbyte/awa/pull/330), v031) — worker health reads lane-head cursor signals instead of scanning ready rows — plus **maintenance branch observability** ([#302](https://github.com/hardbyte/awa/pull/302)).
- **`awa-pg[ui]` Python extra** + `python -m awa serve`. The bare `awa-pg` install stays small; `pip install 'awa-pg[ui]'` pulls in the `awa-cli` wheel and launches the embedded React dashboard.
- **Managed-Postgres deployment guide** ([`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md)). Per-vCPU sizing data, Cloud SQL IAM grants, AlloyDB notes, auth-proxy sidecar pattern, and MVCC operator guidance (separate database for long readers, `idle_in_transaction_session_timeout`, `xact_start` alerting).

**Producer APIs and tuning**

- **`PartitionedQueue`** ([#348](https://github.com/hardbyte/awa/pull/348), [ADR-031](docs/adr/031-partitioned-queues.md)) — deterministic routing from one hot logical queue to multiple physical partitions. Partition 0 is the logical queue name itself (`email`, `email__p1`, …) so direct enqueues to the logical name stay consumable, and key→partition selection uses a domain-separated hash so it composes with ADR-025 enqueue-shard routing. Replaces the beta-series `QueueFanout` (see Breaking changes).
- **Per-job routing in Python COPY batch APIs** ([#348](https://github.com/hardbyte/awa/pull/348)). `insert_many_copy` / `enqueue_many_copy` accept `opts=[...]` entries that override `queue` and `ordering_key` per job, so one COPY call can carry a mixed-partition batch.
- **Transactional follow-up jobs** ([ADR-029](docs/adr/029-transactional-followup-jobs.md), [#285](https://github.com/hardbyte/awa/pull/285)/[#288](https://github.com/hardbyte/awa/pull/288)). `ClientBuilder::on_completed_enqueue` (and friends) registers a follow-up job inserted in the same transaction as the lifecycle transition.
- **Callback-only router and user-owned callback layers** ([#291](https://github.com/hardbyte/awa/pull/291)/[#293](https://github.com/hardbyte/awa/pull/293)) with a configurable URL prefix — embed Awa's router or implement the contract in your own API layer (axum and FastAPI examples). The `WaitingForCallback` lifecycle event plus client-side `resolve_callback` / `complete_external` / `fail_external` / `retry_external` ([#276](https://github.com/hardbyte/awa/pull/276)) make parked jobs visible to hooks and resolvable in-process.
- **Direct queue-storage COPY producer path** as the documented high-volume entry point. Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python: `Client.enqueue_many_copy(jobs)`. The compat `insert_many_copy` route remains for ad-hoc inserts.
- **`InsertOpts::ordering_key`** pins related jobs to the same shard at `enqueue_shards > 1` (a Kafka-partition-key analogue), and **`awa.queue_meta.enqueue_shards`** ([ADR-025](docs/adr/025-sharded-enqueue-heads.md)) is the per-queue semantic switch: default `1` preserves strict `(queue, priority)` FIFO; `≥ 2` switches to partitioned FIFO.
- **Completion-batcher defaults tuned** to `(batch=512, flush=1ms)` with adaptive completion shards, tunable via `AWA_COMPLETION_BATCH_SIZE` / `AWA_COMPLETION_FLUSH_MS` / `AWA_COMPLETION_SHARDS`.
- **`awa-seaorm` crate** ([#275](https://github.com/hardbyte/awa/pull/275)) for transactional enqueue alongside SeaORM application writes.
- **Registered handler futures no longer require `Sync`** ([#331](https://github.com/hardbyte/awa/pull/331)) — only `Send + 'static`, so handlers awaiting `sqlx` / `reqwest` compile without workarounds.

**Telemetry**

- **`awa-metrics` crate** so non-runtime callers (`awa-ui`, `awa-cli`) emit the same OTel counters as the worker (`awa-worker::AwaMetrics` re-exports for source compatibility).
- **Sub-second buckets on `awa.job.wait_duration`**, **`awa.enqueue.batch_size` / `awa.enqueue.duration` histograms**, and an **`awa.enqueue.shard` attribute on `awa.job.claimed`** for per-shard claim throughput.
- **`queue_counts_fast`** ([#289](https://github.com/hardbyte/awa/pull/289)) — index-only depth probe for high-cadence pollers. It under-counts unrolled terminal rows; use `queue_counts` when exact terminal counts matter.
- **Exact terminal counts without `done_entries` scans** ([#290](https://github.com/hardbyte/awa/issues/290) via [#304](https://github.com/hardbyte/awa/pull/304)/[#306](https://github.com/hardbyte/awa/pull/306)): folded counters serve exact reads when the terminal-counter trust marker is set (`awa storage rebuild-terminal-counters`; fresh installs auto-mark).
- **Maintenance branch duration histograms** + `awa_maintenance_branch_overrun_total{branch=…}` ([#302](https://github.com/hardbyte/awa/pull/302)).

### Breaking changes

Coming from 0.5.x, only the storage transition (below) needs action — the 0.5 public client API is otherwise source-compatible. The renames below landed within the 0.6 beta/rc series, so early-0.6 adopters must handle them:

- **`QueueFanout` → `PartitionedQueue`** ([#348](https://github.com/hardbyte/awa/pull/348)), no compatibility alias: `width` → `partitions`, `queue_fanout` → `partitioned_queue`, Python `*_per_queue` kwargs → `*_per_partition`. Routing also differs from the beta.2 `QueueFanout` (partition 0 is the logical name; domain-separated hash), so a given key may land on a different partition. Drain fanout queues before upgrading, or pin explicit names via `from_physical_queues`.
- **`QueueCounts.completed` → `QueueCounts.terminal`** ([#306](https://github.com/hardbyte/awa/pull/306)) — it counts completed, cancelled, and discarded terminal rows; `QueueCounts` also gains `pruned_failed`.
- **`retry_failed_by_kind` / `retry_failed_by_queue` return `RetryFailedOutcome { retried, matched, pruned_failed_count }`** ([#337](https://github.com/hardbyte/awa/issues/337)) instead of a bare `Vec<JobRow>`; Python `retry_failed` / `retry_failed_sync` return the outcome rather than a plain list.
- **`JobEvent` / `UntypedJobEvent` gain a `WaitingForCallback` variant** ([#276](https://github.com/hardbyte/awa/pull/276)) — exhaustive matches need a new arm.
- **`PruneOutcome::Pruned` gains `carried_failed_rows`** and **`QueueStorage::prune_oldest` takes a `failed_retention: Duration`** ([#337](https://github.com/hardbyte/awa/issues/337)) — `Duration::ZERO` disables the floor.

### Upgrading from 0.5.x

- **Fresh installs:** nothing to do. `awa migrate` auto-finalizes to the queue-storage engine on first run.
- **Existing 0.5.x clusters:** roll out 0.6 workers first (they understand both engines and the transition states), then walk `prepare → enter-mixed-transition → finalize`, following [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). **Rollback boundary:** one-way after `enter-mixed-transition` followed by any queue-storage write — plan to restore from backup if rollback is needed past that point.
- Update the dependency: `awa = "0.6.0"` (Rust) / `awa-pg==0.6.0` (Python).

### Operating notes

- **Set `enqueue_shards` explicitly per queue.** The default `1` is the conservative FIFO-preserving choice but serialises producers on a single head row; `4`/`8` is operator tuning for contended hot queues. The trade-off is semantic, not a free perf win — see [`docs/configuration.md`](docs/configuration.md#sharding-the-enqueue-head-per-queue).
- **MVCC discipline.** If your application runs long-lived `REPEATABLE READ` / `SERIALIZABLE` transactions on the same database awa lives in, give awa its own database. A long-pinned `xmin` blocks dead-tuple cleanup and degrades queue-storage throughput over long horizons — bound non-awa sessions with `idle_in_transaction_session_timeout` and alert on `pg_stat_activity.xact_start` age. Tracked in [#169](https://github.com/hardbyte/awa/issues/169).
- **Cloud SQL with IAM auth:** the runtime SA needs `cloudsqlsuperuser` for `prepare_schema` to install. Run `GRANT cloudsqlsuperuser TO "your-sa@project.iam"` once per instance. AlloyDB grants this automatically. Full guide: [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md).

### Benchmark evidence (#169)

Long-horizon pinned-MVCC behaviour, measured with the [companion harness](https://github.com/hardbyte/postgresql-job-queue-benchmarking) (1 replica, 64 workers, 800 jobs/s offered, 60-minute idle-in-transaction reader). **These are point-in-time results from that harness and hardware, not universal claims.**

- **Awa on the 0.6 storage engine:** the clean phase holds the offered rate at shallow median depth. Under the 60-minute pinned reader, completion sags but queue depth stays bounded (no dead-tuple cliff) and drains fully once the pin releases — the append-only segments hold zero dead tuples, with residual pressure confined to terminal-counter and ring-state metadata. This bounded-depth-through-the-pin / full-drain-in-recovery shape is the [#169](https://github.com/hardbyte/awa/issues/169) gate the 0.6.0 tag was held on, and it passed.
- **Reference comparison:** a feature-minimal reference queue (no per-row retries, heartbeats, cancellation, or deadlines) holds flat through the same phase at a fraction of the WAL/job. Every per-row-state-machine Postgres queue — River, Oban, pg-boss, Graphile, pgmq — shares awa's degradation shape under a pinned horizon. Awa is not immune to long readers; the MVCC-discipline rules above are the supported mitigation.
- A separate Cloud SQL PG18 16-vCPU staging sweep (v021 shard-aware lane indexes) moved end-to-end drain on a 3.5M-row backlog from ~1,300 jobs/s to ~8,800–10,600 jobs/s. That is a producer/claim-throughput result on contended backlog, independent of the MVCC question above.

## [0.6.0-rc.4] — 2026-06-30

### Fixed

- **`admin::cancel_by_unique_key` / `cancel_by_unique_key_tx` on the queue-storage engine** ([#359](https://github.com/hardbyte/awa/pull/359)). The candidate lookup's running-job branch read `unique_key` directly from `{schema}.leases`, but the `leases` table does not carry that column, so any cancel-by-unique-key issued while queue storage was the active engine failed at plan time with `column "unique_key" does not exist`. The running-job branch now recovers the key by joining each lease back to its originating `ready_entries` row — the same lane-identity join the candidate and jobs-compat views already use. Canonical-engine behaviour was unaffected.

## [0.6.0-rc.3] — 2026-06-25

### Changed

- **`admin::cancel_tx` / `admin::cancel_by_unique_key_tx` now take `&mut PgConnection`** ([#358](https://github.com/hardbyte/awa/pull/358)) instead of the `&mut sqlx::Transaction` introduced in rc.2. A `&mut Transaction` deref-coerces to `&mut PgConnection`, so existing callers are unaffected, while callers that only hold a connection mid-transaction (e.g. behind a `transact!`-style helper that derefs the transaction) can now use them too. This matches awa's existing multi-statement consumer convention (`insert_many_copy`). Internally the cancel runs in a nested transaction — a `SAVEPOINT` when the connection is already in one — so a cancel error rolls back only the cancel rather than poisoning the caller's transaction.

## [0.6.0-rc.2] — 2026-06-25

### Added

- **Transaction-scoped admin cancel** ([#357](https://github.com/hardbyte/awa/pull/357)). `admin::cancel_tx` and `admin::cancel_by_unique_key_tx` accept a caller-provided `&mut sqlx::Transaction` and run the cancellation inside it, so the cancel commits or rolls back atomically with the caller's other work and the cooperative `awa:cancel` NOTIFY to a running worker fires only on the caller's commit. The pool-based `cancel` / `cancel_by_unique_key` are unchanged. On the queue-storage engine the `_tx` variants skip the best-effort post-commit claim-cursor advance the pool variants perform — the derived queue depth can briefly over-count by one until later committed rows on the lane are claimed; canonical counts are unaffected.

## [0.6.0-rc.1] — 2026-06-24

First release candidate for the 0.6 line. It promotes the beta.2 queue-storage engine after the [#169](https://github.com/hardbyte/awa/issues/169) pinned-MVCC dead-tuple gate passed on `main`: a 60-minute idle-in-transaction soak now holds throughput with no dead-tuple cliff, and the last dominant pinned-horizon hot row (`queue_claim_heads`) is removed in this delta. Migrations v032–v039 apply via `awa migrate` (or the SQL-only path in [`docs/migrations.md`](docs/migrations.md) for external migration tooling).

### Added

- **Compact receipt claim and completion batches** ([#352](https://github.com/hardbyte/awa/pull/352), migrations v036-v038, [ADR-026](docs/adr/026-narrow-terminal-history.md)). Receipt-backed successful completions can now write compact terminal batches instead of one `done_entries` row per job, plus compact claim-local closure batches so stale receipt scans stay bounded. Queue storage also records compact `ready_segments` lane ranges so claim can route through committed ready segment metadata before validating `ready_entries`, compact `ready_claim_attempt_batches` range evidence so stale claim cursors do not need to scan claim-ring partitions while old slots are being truncated, and compact `lease_claim_batches` receipt evidence for zero-deadline claims. Deadline-backed receipt claims remain row-local in `lease_claims` for indexed deadline rescue. `{schema}.terminal_jobs` remains the public terminal read surface; direct SQL against `done_entries` should not assume all completed jobs are physically stored there.
- **Per-job routing in Python COPY batch APIs** ([#348](https://github.com/hardbyte/awa/pull/348), [ADR-031](docs/adr/031-partitioned-queues.md)). `insert_many_copy` / `enqueue_many_copy` (async and sync) accept `opts=[...]` entries that override `queue` and `ordering_key` per job, so one COPY call can carry a mixed-partition batch. Batch-level kwargs remain defaults; an explicit `"ordering_key": None` clears the key for that job.
- **Failed terminal retention floor** ([#337](https://github.com/hardbyte/awa/issues/337) via [#350](https://github.com/hardbyte/awa/pull/350), migration v032, [ADR-032](docs/adr/032-failed-terminal-retention.md)). Non-DLQ `failed` terminal rows now stay retryable for at least `failed_retention` (default `72h`) in queue storage. Queue-ring prune carries in-floor `failed` rows forward into the live segment as wide self-contained terminal rows before truncating the old slot, instead of dropping them on the next rotation. Failed rows aged past the floor are folded into the new `queue_terminal_rollups.pruned_failed_count` column and surfaced as `QueueCounts.pruned_failed` — a cumulative, monotonic count of failed rows no longer retryable, giving operators standing visibility of loss that was previously silent. `retry_failed_by_kind` / `retry_failed_by_queue` now return a `RetryFailedOutcome { retried, matched, pruned_failed_count }` so raced or pruned ids are reported rather than silently dropped, and the CLI surfaces the matched/retried split and a pruned-past-retention warning. `cancelled` rows are not held by the floor.

### Changed

- **Breaking (beta series): `QueueFanout` is replaced by `PartitionedQueue`** ([#348](https://github.com/hardbyte/awa/pull/348), [ADR-031](docs/adr/031-partitioned-queues.md)); no compatibility alias. Besides the rename (`width` → `partitions`, `queue_fanout` → `partitioned_queue`, Python `*_per_queue` config kwargs → `*_per_partition`), routing behavior differs from beta.2: partition 0 is now the logical queue name itself (`email`, `email__p1`, ...) so direct enqueues to the logical name stay consumable, and key→partition selection uses a domain-separated hash so it composes with ADR-025 enqueue-shard routing (the same key may land on a different partition than under beta.2 `QueueFanout`). Drain fanout queues before upgrading producers/workers, or pin explicit names via `from_physical_queues`.
- **Breaking (beta series): `retry_failed_by_kind` and `retry_failed_by_queue` return `RetryFailedOutcome`** ([#337](https://github.com/hardbyte/awa/issues/337)) instead of a bare `Vec<JobRow>`; `RetryFailedOutcome` carries `retried`, `matched`, and `pruned_failed_count`. Python `retry_failed` / `retry_failed_sync` return the outcome rather than a plain list of jobs.
- **Breaking (beta series): `PruneOutcome::Pruned` gains a `carried_failed_rows` field** ([#337](https://github.com/hardbyte/awa/issues/337)) — `Pruned { slot, carried_failed_rows }`; exhaustive matches on `PruneOutcome` need the new binding.
- **Breaking (beta series): `QueueStorage::prune_oldest` takes a `failed_retention: Duration`** ([#337](https://github.com/hardbyte/awa/issues/337)). `Duration::ZERO` disables the floor; `MaintenanceService` passes its configured `failed_retention`. `QueueCounts` gains a `pruned_failed: i64` field.
- **Maintenance prune lock avoidance + bounded receipt-plane scans** ([#349](https://github.com/hardbyte/awa/pull/349), migrations v033–v035). Queue-ring prune no longer queues behind a held lock — it skips a contended slot and retries on the next maintenance cycle, and genuine (non-lock) prune errors now propagate instead of being swallowed. Receipt rescue, receipt terminal-delete closures, and receipt deadline-rescue sweeps are bounded by per-slot cursors so they cost work proportional to live receipts rather than scanning whole rings; queue-storage metrics and maintenance emptiness checks stay off the receipt rings; and the default lease-ring rotation cadence is lowered to cut idle maintenance churn.

### Fixed

- **Stop writing the `queue_claim_heads` ready-segment routing cache** ([#355](https://github.com/hardbyte/awa/pull/355); completes the [#169](https://github.com/hardbyte/awa/issues/169) / [#197](https://github.com/hardbyte/awa/issues/197) HOT-update audit, migration v039). The per-claim `UPDATE` of the `ready_segment_*` routing cache on the singleton `queue_claim_heads` row was the dominant dead-tuple accumulator under a pinned MVCC horizon — a 60-minute idle-in-transaction soak left ~130k dead row versions on a single live row (~80% of all dead tuples), unreclaimable until the horizon released. `claim_ready_runtime` now resolves the target ready slot directly from the `ready_segments` control plane, ordered by `next_lane_seq` (ready segments are non-overlapping, so the smallest `next_lane_seq > claim_seq` is the next covering segment; the subsequent `ready_entries` lookup validates the row, so reservation gaps fall through safely) so the existing `(queue, priority, enqueue_shard, next_lane_seq, …)` index short-circuits at `LIMIT 1` instead of materialising and sorting the tail. The now-unused `ready_segment_slot` / `ready_segment_generation` / `ready_segment_next_lane_seq` columns are **retained** so a rolling-upgrade worker on the previous `queue_storage_schema_ready` check still starts (the additive-only migration policy defers dropping them to a major version). In the pinned-MVCC gate this removes the table's dead-tuple stream entirely and lowers claim p99 under the pin versus the previous cache. Continues the HOT-update audit from the `queue_lanes.available_count` removal (v016, [#251](https://github.com/hardbyte/awa/pull/251)) and the `leases` / `lease_claims` fillfactor pass ([#315](https://github.com/hardbyte/awa/pull/315)).
- **Compact claims close into the compact closure ledger on every path** ([#352](https://github.com/hardbyte/awa/pull/352)). Receipt rescue, terminal close (completion / fail / retryable / cancel), and admin cancel of a compact (zero-deadline) claim now write a `lease_claim_closure_batches` row instead of an explicit `lease_claim_closures` row. A compact claim has no `lease_claims` row, so an explicit closure was invisible to the queue-ring prune count proof and wedged the slot on `SkippedActive { QueueUnclosedClaimRefs }` until the claim ring pruned. Deadline-backed row-local claims continue to write explicit closures.

## [0.6.0-beta.2] — 2026-06-10

Second beta of the 0.6 line. The headline is storage-engine work under pinned MVCC horizons ([#169](https://github.com/hardbyte/awa/issues/169)): sequence-backed cursors, append-only ready segments, and a terminal-count delta ledger replace the remaining hot-row update paths that beta.1 still carried. This is also the release the #169 stable gate will be validated against — see "Benchmark evidence" below.

Migrations v022–v031 apply via `awa migrate` (or the SQL-only path in [`docs/migrations.md`](docs/migrations.md) for external migration tooling).

### Storage engine under pinned MVCC horizons (#169)

- **Sequence-backed lane cursors** ([#321](https://github.com/hardbyte/awa/pull/321), migration v027). Enqueue/claim lane cursors move from hot MVCC-updated rows to PostgreSQL sequences; the head tables remain as lane registries and lock targets. Claim cursors advance post-commit so non-transactional sequence state cannot skip work on rollback. `queue_terminal_live_counts` is striped by `job_id % 256` so completion-heavy lanes don't hammer one counter row.
- **Append-only ready segments + tombstone ledger** ([#323](https://github.com/hardbyte/awa/pull/323), migration v028). Completion, DLQ, retry, and discard no longer `DELETE FROM ready_entries`; rare ready invalidations append to `ready_tombstones`, claim treats tombstones as spent cursor evidence, and maintenance truncates ready/done/tombstone partitions together. Under a pinned reader, ready/done/tombstone partitions now hold zero dead tuples.
- **Terminal-count delta ledger + async rollup** ([#329](https://github.com/hardbyte/awa/pull/329), migration v030, [ADR-026](docs/adr/026-narrow-terminal-history.md)). Terminal mutations append signed rows to `queue_terminal_count_deltas` in the same transaction instead of upserting a live counter; the maintenance leader folds sealed-slot deltas into `queue_terminal_live_counts`. Exact reads stay honest while rollup lags: folded counts plus pending deltas.
- **Rollup deferral under pinned horizons** ([#333](https://github.com/hardbyte/awa/pull/333)). Maintenance stands down from mutating folded counters or truncating delta segments while another backend pins the MVCC horizon, then folds after the horizon clears. Prevents the counter table itself from bloating while vacuum is blocked.
- **Exact terminal counts without `done_entries` scans** ([#290](https://github.com/hardbyte/awa/issues/290) via [#304](https://github.com/hardbyte/awa/pull/304)/[#306](https://github.com/hardbyte/awa/pull/306)). `queue_counts_exact` reads the folded counters (bounded by slots × priorities × shards) when the terminal-counter trust marker is set, falling back to scanning `done_entries` mid-rolling-upgrade. Operators flip the marker with `awa storage rebuild-terminal-counters`; fresh installs auto-mark.
- **`queue_counts_fast`** ([#289](https://github.com/hardbyte/awa/pull/289)) — index-only depth probe for high-cadence pollers (dashboards, depth-target producers). Under-counts terminal rows the live segment hasn't rolled up; use `queue_counts` when exact terminal counts matter.
- **Metrics off the exact-scan path** ([#330](https://github.com/hardbyte/awa/pull/330), migration v031). Worker health/metrics availability reads lane-head cursor signals instead of exact ready-row scans; lag probing is bounded to the next claimable row per lane; a partial index serves the failed-count probe.
- **Maintenance and receipt-plane mitigations**: `fillfactor=50` + autovacuum knobs on `leases` / `lease_claims` partitions ([#315](https://github.com/hardbyte/awa/pull/315), migration v024); exponential prune backoff + branch-tracker hysteresis ([#316](https://github.com/hardbyte/awa/pull/316)); receipts mode drops the `leases.heartbeat_at` write and `state_hb` index — heartbeats live in `attempt_state` ([#317](https://github.com/hardbyte/awa/pull/317), migration v025); duration-margin hysteresis, per-branch cooldown, and a 250 ms lease-rotate default.

### Added

- **Durable batch operations** ([#328](https://github.com/hardbyte/awa/pull/328), migration v029, [ADR-030](docs/adr/030-batch-operations.md)). Crash-safe, maintenance-driven bulk `set_priority` and `move_queue` over job selections, with preview/submit/list/get/cancel/purge through the HTTP API, CLI, and a new admin-UI Batch Ops tab. Python parity — raw sync/async batch methods plus typed `Client` / `AsyncClient` helpers — via [#332](https://github.com/hardbyte/awa/pull/332). This is the first-class reprioritization surface ([#307](https://github.com/hardbyte/awa/issues/307)).
- **Transactional follow-up jobs** ([ADR-029](docs/adr/029-transactional-followup-jobs.md), [#285](https://github.com/hardbyte/awa/pull/285)/[#288](https://github.com/hardbyte/awa/pull/288)). `ClientBuilder::on_completed_enqueue` (and friends) registers a follow-up job inserted in the same transaction as the lifecycle transition for worker-driven outcomes; callback resolution through the worker `Client` commits transition + follow-up atomically too. Maintenance rescue stays best-effort by design.
- **Callback-only router and user-owned callback layers** ([#291](https://github.com/hardbyte/awa/pull/291)/[#293](https://github.com/hardbyte/awa/pull/293), closes [#281](https://github.com/hardbyte/awa/issues/281)). The callback ingress contract is shared and the URL prefix configurable; embed Awa's router or implement the contract in your own API layer — axum and FastAPI examples documented.
- **`WaitingForCallback` lifecycle event + client-side callback resolution** ([#276](https://github.com/hardbyte/awa/pull/276)). Jobs parking on `WaitForCallback` are now visible to lifecycle hooks, and `Client` gains `resolve_callback` / `complete_external` / `fail_external` / `retry_external` that dispatch the matching terminal event in-process.
- **Queue fanout helper** ([#327](https://github.com/hardbyte/awa/pull/327)). Rust `QueueFanout` + `ClientBuilder::queue_fanout` and Python `awa.QueueFanout` for deterministic routing from one hot logical queue to multiple physical queues; duplicate physical declarations are rejected. Replaced by `PartitionedQueue` in [#348](https://github.com/hardbyte/awa/pull/348).
- **Cron schedule pause / resume** ([#320](https://github.com/hardbyte/awa/pull/320), migration v026). `POST /api/cron/{name}/pause` / `/resume`; the evaluator skips paused rows and the `atomic_enqueue` CTE re-checks `paused_at IS NULL` so a pause asserted mid-evaluation still takes effect. `last_enqueued_at` is untouched while paused, so `missed_fire_policy` decides catch-up on resume. Manual `trigger_cron_job` bypasses pause. The `/cron` UI gains Pause/Resume controls.
- **`awa storage finalize --wait` / `--check`** ([#298](https://github.com/hardbyte/awa/pull/298)). `--wait` polls and finalizes once readiness gates stay clear for two consecutive observations; `--check` is a dry-run that exits 2 when blocked.
- **Storage-transition readiness UI** ([#299](https://github.com/hardbyte/awa/pull/299)). Time-in-state, epoch-anchored backlog history, a prominent `prepared_schema_ready=false` warning with the remediation command, and a rollback-boundaries panel.
- **`awa-seaorm` crate** ([#275](https://github.com/hardbyte/awa/pull/275)). Focused SeaORM adapter for transactional enqueue alongside application writes.
- **Maintenance branch observability** ([#302](https://github.com/hardbyte/awa/pull/302)). `awa.maintenance.branch.duration` histograms, delayed-tick warnings on on-time→delayed transitions, and `awa_maintenance_branch_overrun_total{branch=...}`.

### Changed

- **Breaking (beta series): `QueueCounts.completed` is renamed `QueueCounts.terminal`** ([#306](https://github.com/hardbyte/awa/pull/306)) — it counts completed, cancelled, and discarded terminal rows, and the old name misread as completed-only.
- **Breaking (beta series): `JobEvent` / `UntypedJobEvent` gain a `WaitingForCallback` variant** ([#276](https://github.com/hardbyte/awa/pull/276)) — exhaustive matches need a new arm.
- **`awa migrate` owns the default `awa.*` queue-storage substrate** ([#308](https://github.com/hardbyte/awa/issues/308) via [#310](https://github.com/hardbyte/awa/pull/310)/[#312](https://github.com/hardbyte/awa/pull/312)/[#313](https://github.com/hardbyte/awa/pull/313)/[#314](https://github.com/hardbyte/awa/pull/314), migration v023). Substrate DDL is migration-owned rather than installed opportunistically by `prepare_schema()`; `awa --reset --schema awa` is rejected; the SQL-only install/upgrade path for external migration tooling is documented and tested.
- **Registered handler futures no longer need `Sync`** ([#331](https://github.com/hardbyte/awa/pull/331)). Returned futures only require `Send + 'static`, so handlers awaiting `sqlx` / `reqwest` work compile without workarounds.
- **Chaos tests observe DB state, not stdout** ([#301](https://github.com/hardbyte/awa/pull/301), closes [#167](https://github.com/hardbyte/awa/issues/167)).

### Fixed

- **Concurrent queue-storage sequence reservations** ([#326](https://github.com/hardbyte/awa/pull/326)). `reserve_enqueue_seq` assumed `N × nextval()` returned a contiguous block; concurrent COPY producers could reserve overlapping ranges and hit `ready_entries_*_pkey`. Blocks now allocate under a per-sequence advisory lock (`nextval` + `setval`), and compatibility sync uses the same lock so it cannot rewind a hot enqueue sequence.
- **Flaky admin metadata cache test under parallel runners** ([#274](https://github.com/hardbyte/awa/pull/274)).

### Documentation

- **MVCC operator guidance** in [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md): keep long-running analytical readers on a separate database, bound non-Awa sessions with `idle_in_transaction_session_timeout`, alert on `pg_stat_activity.xact_start` age (query included), and give autovacuum instance-level capacity on managed engines.
- **Security guide: deployable roles and callback ingress boundaries** ([#294](https://github.com/hardbyte/awa/pull/294)).
- **Deadline-bounded polling example** ([#273](https://github.com/hardbyte/awa/pull/273)) — `poll_until_deadline.rs` plus an integration test pinning the retry-until-deadline pattern.
- App-owned database usage in examples ([#286](https://github.com/hardbyte/awa/pull/286)); callback ingress + maintenance ADR proposals ([#277](https://github.com/hardbyte/awa/pull/277)); the #169 storage spike archived under `docs/` ([#300](https://github.com/hardbyte/awa/pull/300)).

### Benchmark evidence and operating notes (#169)

Long-horizon pinned-MVCC behaviour, measured with the [companion harness](https://github.com/hardbyte/postgresql-job-queue-benchmarking) (1 replica, 64 workers, 800 jobs/s offered, 60-minute idle-in-transaction reader; numbers are point-in-time results from that harness and hardware, not universal claims):

- **Awa on this release's storage engine (post-#330 main, 2026-06-09 run):** clean phase holds 800/s at median depth 21. Under the 60-minute pinned reader, median completion is ~741/s, sagging to ~606/s by phase end with queue depth peaking at ~312k; peak sampled dead tuples ~91k (down from ~2.54M two iterations earlier — the append-only segments hold zero dead tuples, with the residual pressure in the terminal-counter and ring-state metadata). Recovery after the pin releases drained at ~963/s median but still held ~198k backlog after 10 minutes.
- **pgque reference on the same harness (2026-05-17/18):** holds ~800/s with median depth 0 through the same phase shape, at ~450 B WAL/job versus Awa's ~2.2–2.9 KiB/job. pgque buys that immunity by trading away per-row feature surface (per-row retries, heartbeats, cancellation, deadlines); every per-row-state-machine Postgres queue — River, Oban, pg-boss, Graphile, pgmq — shares Awa's degradation shape under a pinned horizon. Awa is not immune to long readers, and this release does not claim otherwise.
- **Operating guidance:** the MVCC-discipline rules above are the supported mitigation — separate database for long analytical readers, session timeouts, `xact_start` alerting. `enqueue_shards=1` remains the conservative default; `4`/`8` is operator tuning for contended hot queues, independent of the MVCC question.
- The `0.6.0` **stable** tag is gated on this release passing the long-horizon shape (bounded depth through the pinned hour, full drain in recovery) — tracked in [#169](https://github.com/hardbyte/awa/issues/169).

## [0.6.0-beta.1] — 2026-05-19

First beta of the 0.6 line. Frames the user-facing diff between `0.5.x` and `0.6.0-beta.1` as one release; the `0.6.0-alpha.N` entries below remain as the granular development log.

### What's new since 0.5.x

**Storage and durability**

- **Queue-storage engine, default-on** ([ADR-019](docs/adr/019-queue-storage-redesign.md)). Append-only `ready_entries`, `deferred_jobs`, `done_entries`, and `dlq_entries` paired with a partitioned receipt ring keep dead-tuple footprint bounded under sustained load. Replaces the row-mutating canonical engine for fresh installs.
- **Receipt-plane ring partitioning** ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)). `lease_claims` and `lease_claim_closures` partitioned by claim slot, rotated by the maintenance leader.
- **Per-claim deadlines** in receipts mode. Set `QueueConfig.deadline_duration` to bound a single attempt; rescue force-closes expired claims with `'deadline_expired'`.
- **Staged 0.5 → 0.6 transition tooling**: `awa storage prepare`, `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize`, `abort`. Fresh installs auto-finalize on first `awa migrate`. Full procedure in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

**Operator surfaces**

- **Dead Letter Queue** ([ADR-020](docs/adr/020-dead-letter-queue.md)). Per-queue `dlq_enabled` policy plus a full admin surface: `awa dlq depth | list | retry | retry-bulk | move | purge` (CLI), matching admin UI tab, Rust/Python client APIs.
- **Descriptor catalog** ([ADR-022](docs/adr/022-descriptor-catalog.md)). Code-declared queue and job-kind metadata (`display_name`, `description`, `owner`, `tags`, `docs_url`) drives admin UI labels and drift detection.
- **Cron missed-fire policy**. Periodic schedules persist an explicit `missed_fire_policy`: `coalesce` (default) or `catch_up`. Exposed in Rust/Python APIs and CLI schedule output.
- **`awa-pg[ui]` Python extra** + `python -m awa serve`. The bare `awa-pg` install stays small (workers/producers don't pay for the ~10 MB axum + dashboard bundle); `pip install 'awa-pg[ui]'` pulls in the `awa-cli` wheel and lets `python -m awa serve` launch the embedded React dashboard.
- **Managed-Postgres deployment guide** ([`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md)). Per-vCPU sizing data, Cloud SQL IAM grants, AlloyDB notes, auth-proxy sidecar pattern.

**Producer APIs and tuning**

- **Direct queue-storage COPY producer path** as the documented high-volume entry point. Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python: `Client.enqueue_many_copy(jobs)`. The compat-friendly `insert_many_copy_from_pool` / `client.insert_many_copy` routes through `awa.insert_job_compat()` once per row — fine for ad-hoc inserts but ~100–150 ms per row through a real DB proxy, not what you want for bursts.
- **`InsertOpts::ordering_key: Option<Vec<u8>>`** pins related jobs to the same shard at `enqueue_shards > 1`. Kafka-partition-key analogue: jobs sharing a key inherit that shard's strict FIFO. `None` rotates batches across shards.
- **`awa.queue_meta.enqueue_shards` per-queue semantic switch** ([ADR-025](docs/adr/025-sharded-enqueue-heads.md)). Default `1` preserves strict `(queue, priority)` FIFO. Setting `≥ 2` switches that queue to partitioned FIFO (strict within `(queue, priority, enqueue_shard)`, no order across shards) — comparable to choosing SQS Standard over FIFO.
- **Queue-storage completion-batcher defaults tuned** to `(batch=512, flush=1ms)`, with adaptive queue-storage completion shards: ordinary runtimes use `1`; runtimes configured for at least `512` workers use `4`. Tunable via `AWA_COMPLETION_BATCH_SIZE`, `AWA_COMPLETION_FLUSH_MS`, and `AWA_COMPLETION_SHARDS`.

**Telemetry**

- **`awa-metrics` crate**. `AwaMetrics` and its `record_*` methods live in their own crate so non-runtime callers (`awa-ui`, `awa-cli`) can emit the same OTel counters as the worker. `awa-worker::AwaMetrics` re-exports for source compatibility.
- **Sub-second buckets on `awa.job.wait_duration`**. Previous boundaries didn't resolve pickup latency below 5 s.
- **`awa.enqueue.batch_size` and `awa.enqueue.duration`** histograms on the direct COPY enqueue path.
- **`awa.enqueue.shard` attribute on `awa.job.claimed`** so dashboards can see per-shard claim throughput.

### Upgrading from 0.5.x

- Fresh installs: nothing to do. `awa migrate` auto-finalizes to the queue-storage engine on first run.
- Existing 0.5.x clusters: walk the staged transition documented in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). Roll out 0.6 workers first (they understand both engines and the transition states), then walk `prepare → enter-mixed-transition → finalize`. **Rollback boundary:** one-way after `enter-mixed-transition` followed by any queue-storage write. Plan to restore from backup if rollback is needed past that point.
- Update the dependency: `awa = "0.6.0-beta.1"` (Rust) / `awa-pg==0.6.0-beta.1` (Python).

### Operating notes

- **Set `enqueue_shards` explicitly per queue.** The default `1` is the conservative FIFO-preserving choice but serialises producers on a single head row. We recommend `4` for contended hot queues. The trade-off is semantic, not a free perf win — see [`docs/configuration.md`](docs/configuration.md#sharding-the-enqueue-head-per-queue).
- **MVCC discipline.** If your application runs long-lived `REPEATABLE READ` / `SERIALIZABLE` transactions on the same database awa lives in, give awa its own database. Long-pinned `xmin` blocks dead-tuple cleanup and degrades queue-storage throughput over long horizons. Tracked in [#169](https://github.com/hardbyte/awa/issues/169).
- **Cloud SQL with IAM auth:** the runtime SA needs `cloudsqlsuperuser` for `prepare_schema` to install. Run `GRANT cloudsqlsuperuser TO "your-sa@project.iam"` once per instance. AlloyDB grants this automatically. Full guide: [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md).

### Changes since 0.6.0-alpha.9

For users already running an alpha:

- v021 shard-aware lane indexes on `ready_entries` / `done_entries` / `leases` partitions (#263). The narrow `(queue, priority, lane_seq)` indexes from before v017 didn't include `enqueue_shard`; on heavy backlog the planner discarded `(shards-1)/shards` of rows per partition per claim. Drain throughput at depth restored to equilibrium rate.
- `prepare_schema` startup race fixed (#264, #266). Concurrent worker startup on a fresh DB previously hung on PG18; install now runs in a single transaction with `pg_advisory_xact_lock`.
- `queue_storage_schema_ready` tightened to require every object the runtime depends on, so a partial install can't report ready (#266).
- Managed-Postgres deploy guide added (#265).
- `awa.enqueue.batch_size` / `awa.enqueue.duration` histograms added (#265).
- Direct queue-storage COPY producer path is the documented high-volume entry point (#263); compat path semantics unchanged.

### Performance

- Drop the `queue_lanes.available_count` cache (migration `v016`). The dispatcher derives availability from `queue_enqueue_heads.next_seq - queue_claim_heads.claim_seq` and the admin API (`queue_counts`) scans `ready_entries` for an exact count. Removes the queue-storage schema's largest dead-tuple source under pinned-xmin workloads (long-running reader transactions) without changing public API behaviour. See ADR-019 § `lane_state` and segment cursor tables.
- Cache `(queue, priority, enqueue_shard)` lane presence in-process on the queue-storage path so subsequent enqueue batches skip the three `INSERT ... ON CONFLICT DO NOTHING` round-trips on `queue_lanes` / `queue_enqueue_heads` / `queue_claim_heads`. The cache invalidates and re-runs `ensure_lane` if a subsequent `UPDATE queue_enqueue_heads` finds no row (the observable signal of an earlier ensure_lane that ran inside a rolled-back transaction), so correctness is preserved.
- Shard the queue-storage active plane by `enqueue_shard` (migration `v017`). `awa.queue_meta.enqueue_shards` (default 1, range 1..=64) is a per-queue semantic mode switch — comparable to choosing SQS Standard over SQS FIFO, raising Kafka partition count, or using Pub/Sub ordering keys. The ordering contract at `enqueue_shards = 1` is unchanged (strict FIFO per `(queue, priority)`); at `enqueue_shards > 1` it becomes **partitioned FIFO** — strict FIFO within `(queue, priority, enqueue_shard)`, no order promised across shards. The shard column runs end-to-end: `queue_enqueue_heads`, `queue_claim_heads`, `ready_entries`, `leases`, and `done_entries` carry it in their primary keys; `lease_claims` carries it as a regular column. Raising the value per noisy queue delivers near-linear throughput scaling on contended enqueue workloads: a 16-producer same-queue local sweep measured 1.0× → 1.60× → 2.75× → 3.69× at S=1/2/4/8. Scope: this addresses producer-side enqueue contention. The high-worker-count rescue-path regression (1 replica × 256 workers, receipts on, `LEASE_DEADLINE_MS` A/B) is a separate effect on the claim / rescue side and remains open for follow-up measurement. See ADR-025 for the full design.
- **Shard-aware lane indexes on `ready_entries`, `done_entries`, and `leases` child partitions** (migration `v021`, [#263](https://github.com/hardbyte/awa/pull/263)). The narrow `(queue, priority, lane_seq)` lane indexes that predated v017 did not include `enqueue_shard`, while `claim_ready_runtime` (since v017) filters on it. Under `enqueue_shards > 1` and deep backlog the planner scanned the lane index forward and post-filtered shard, discarding roughly `(shards-1)/shards` of rows per partition per claim. v021 replaces those with `(queue, priority, enqueue_shard, lane_seq)` indexes and drops the originals; `prepare_schema` is updated to match for fresh installs. On a Cloud SQL PG18 16 vCPU staging instance with a 3.5M row backlog, end-to-end drain went from ~1,300 jobs/s to ~8,800–10,600 jobs/s (single-claim probe: 11.4 ms → 0.81 ms).

### Added

- `InsertOpts::ordering_key: Option<Vec<u8>>` pins related jobs to the same shard at `enqueue_shards > 1`. The key bytes are mapped by Awa's portable shard hash and reduced modulo the queue's shard count, so Rust, SQL, and Python enqueue paths agree on routing. Use it as a Kafka-partition-key analogue: jobs for the same customer / order / account share a shard and inherit that shard's strict FIFO. `None` falls back to one rotor pick per `(queue, priority)` sub-batch. Ignored at `enqueue_shards = 1`.
- `awa.job.claimed` counter now carries an `awa.enqueue.shard` attribute on the queue-storage claim path so dashboards can see per-shard claim throughput. Canonical claims continue to emit the un-decorated series; the two attribute sets do not overlap, so Prometheus `sum by (awa_enqueue_shard)(rate(awa_job_claimed[1m]))` is the per-shard fairness panel.
- Tune the queue-storage completion-batcher defaults to `(batch=512, flush=1ms)`. The larger batch cap keeps the durable completion path amortised under load, while the 1 ms flush keeps low-latency finalization for short jobs. Tunable via `AWA_COMPLETION_BATCH_SIZE` and `AWA_COMPLETION_FLUSH_MS`.
- **Direct queue-storage COPY producer path is the documented high-volume entry point** ([#263](https://github.com/hardbyte/awa/pull/263)). Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python: `Client.enqueue_many_copy(jobs)`. `insert_many_copy_from_pool` / `client.insert_many_copy` remain available as the compatibility COPY surface and route through `awa.insert_job_compat()`. See [`docs/configuration.md`](docs/configuration.md#producer-path-choice).
- **Explicit sub-second buckets on `awa.job.wait_duration`** ([#263](https://github.com/hardbyte/awa/pull/263)). Histogram now resolves pickup latency below 5 s, which the previous bucket boundaries did not.
- **`awa.enqueue.batch_size` and `awa.enqueue.duration` histograms** ([#265](https://github.com/hardbyte/awa/pull/265)). Per-batch observability on the direct COPY enqueue path. Lets producers tell "batches are tiny" from "batches are slow" when an enqueue rate target is missed — neither was distinguishable from existing metrics. Recorded by Python `Client.enqueue_many_copy` (sync and async). Rust callers using `QueueStorage::enqueue_params_copy` directly can record via `AwaMetrics::from_global().record_enqueue_batch(queue, count, duration)`.
- **Managed-Postgres deployment guide** ([#265](https://github.com/hardbyte/awa/pull/265)). New [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md) covering Cloud SQL and AlloyDB specifics: per-vCPU sizing measurements from staging, Postgres-version recommendations, IAM `cloudsqlsuperuser` grant for Cloud SQL IAM users, the auth-proxy native-sidecar pattern, and `enqueue_shards` operator guidance.

### Fixed

- **`prepare_schema` no longer deadlocks on fresh-DB / small-pool / concurrent worker startup** ([#264](https://github.com/hardbyte/awa/issues/264), [#266](https://github.com/hardbyte/awa/pull/266)). Previously the install acquired `pg_advisory_lock(…)` on one dedicated pool connection but issued every DDL through `.execute(pool)`, which acquired _different_ connections per statement. With a one- connection pool that self-deadlocked; with a small pool and concurrent worker startup it exhausted the pool with every worker holding one connection for the lock and competing for the rest for DDL. The PG18-on-fresh-DB hang reported in #264 is the most visible symptom. Install now runs in a single transaction on a single connection with `pg_advisory_xact_lock(…)`; the lock auto-releases on COMMIT/ROLLBACK and the install is atomic.
- **`queue_storage_schema_ready` checks the full required surface** ([#266](https://github.com/hardbyte/awa/pull/266)). Previously it only checked for `queue_ring_state`, `ready_entries`, and `leases`; if `prepare_schema` had failed partway through (a real failure mode under the old non-atomic install) the schema could report ready while `awa.job_id_seq`, `done_entries`, `deferred_jobs`, or the `claim_ready_runtime` function were missing — at which point producers got `relation "awa.job_id_seq" does not exist`. The check now requires every queue-storage substrate object the runtime depends on.
- **`AwaMetrics::from_global()` no longer rebuilt per call on hot paths** ([#265](https://github.com/hardbyte/awa/pull/265)). `from_global()` registers the entire instrument set on every invocation. The Python `Client`, `awa-ui` `AppState`, and `awa-cli` DLQ command now cache a single `AwaMetrics` handle and reuse it instead of rebuilding ~30 instruments per operation.

## [0.6.0-alpha.9] — 2026-05-08

### Fixed

- **Queue-storage priority-aging completions now preserve the lane priority in terminal storage.** Workers still receive the aged effective priority, with `_awa_original_priority` in metadata, but `done_entries` now uses the original queue lane priority for its storage key. This prevents aged low-priority jobs from colliding with high-priority jobs that share the same per-lane sequence.

## [0.6.0-alpha.8] — 2026-05-08

### Added

- **Lifecycle hooks now include `Started` events**. `JobEvent<T>` and `UntypedJobEvent` fire `Started` after a claim commits and just before the worker handler is invoked, so applications can observe job execution beginning as well as final outcomes.

### Changed

- **Breaking alpha API change:** exhaustive matches on `JobEvent<T>` or `UntypedJobEvent` must handle the new `Started` variant. No stable Awa release has been published yet, so this is documented as an alpha-series source break.

- **Reduced queue-storage per-claim deadline overhead.** Fresh queue-storage schemas now use a low-write BRIN index for `lease_claims.deadline_at`, and the claim helper computes claim/deadline timestamps once per batch instead of per returned row. Deadline rescue semantics are unchanged; this targets the alpha.8 benchmark path where non-zero `QueueConfig::deadline_duration` is enabled for short receipt-plane jobs.

## [0.6.0-alpha.7] — 2026-05-07

### Added

- **Cron missed-fire policy** ([#239](https://github.com/hardbyte/awa/issues/239), [#240](https://github.com/hardbyte/awa/pull/240)). Periodic schedules now persist an explicit `missed_fire_policy`: `coalesce` remains the default and enqueues only the latest due fire after delayed evaluation, while `catch_up` enqueues missed fires in timestamp order for idempotent reconciliation jobs. The policy is exposed through the Rust and Python APIs, CLI schedule output, docs, and migration v015.

### Changed

- **Cron evaluation runs in its own leader-scoped maintenance lane** ([#240](https://github.com/hardbyte/awa/pull/240)). Slow cleanup, rotation, or reconciliation work no longer starves the scheduler branch. Coalesced schedules also use a bounded latest-fire lookup so a stale high-frequency schedule does not scan every missed tick.
- **Runtime grants now document required `TRUNCATE` privileges** ([#239](https://github.com/hardbyte/awa/issues/239), [#240](https://github.com/hardbyte/awa/pull/240)). The security guide now includes `TRUNCATE` in runtime table grants and default privileges, with a DB-backed regression covering the previously documented grants.

### Fixed

- **Queue-storage claim transactions commit before rescueable runtime payload conversion** ([#222](https://github.com/hardbyte/awa/pull/222)). Runtime claims now keep hot claim transactions shorter while preserving rollback behavior for legacy zero-deadline claims that cannot be rescued after a conversion error.

## [0.6.0-alpha.6] — 2026-05-07

### Added

- **`awa-metrics` crate** ([#176](https://github.com/hardbyte/awa/issues/176), [#232](https://github.com/hardbyte/awa/pull/232)). The `AwaMetrics` type and its `record_*` methods move from `awa-worker` into a dedicated `awa-metrics` crate so non-runtime callers (`awa-ui`, `awa-cli`) can emit the same OTel counters as the worker without depending on the dispatcher/runtime crate graph. `awa-worker::AwaMetrics` re-exports for source compatibility — no semver break for `awa-cli` or `awa-python`. `awa-metrics::names` exposes public string constants for every metric, and `AwaMetrics::new()` is built from those constants so registered instruments and the public names can't drift.

### Changed

- **Multi-queue NOTIFY collapses to one round-trip per enqueue tx** ([#235](https://github.com/hardbyte/awa/pull/235)). The three queue-storage enqueue paths (`enqueue_runtime_rows`, `enqueue_params_batch`, `enqueue_params_copy`) used to issue one `pg_notify($1, '')` per distinct destination queue inside the enqueue transaction. Now routed through `notify_queues_tx`, which uses `SELECT pg_notify(channel, '') FROM unnest($1::text[])` so any number of queues becomes one round-trip. Single-queue enqueue (the common case) is unchanged.
- **Completion path: lease delete and `attempt_state` cleanup merge into a single CTE-as-DML statement** ([#235](https://github.com/hardbyte/awa/pull/235)). `complete_runtime_batch` previously issued two consecutive DELETEs (leases, then `attempt_state`) on the receipt-disabled path and on the materialized fallback inside the receipt-enabled path. They now share one statement — one fewer round-trip per completion batch, identical atomicity and return shape.

### Fixed

- **`awa.job.dlq_retried` tagged with the source queue** ([#232](https://github.com/hardbyte/awa/pull/232)). Single-job retry from `awa-ui` previously read `job.queue` from the returned `JobRow`, which carries the _destination_ queue if the request body supplied a `queue` override. The metric now looks the source queue up before retry runs (matching `record_dlq_purged`'s pattern).

## [0.6.0-alpha.5] — 2026-05-04

### Added

- **`awa-pg[ui]` optional extra** ([#186](https://github.com/hardbyte/awa/issues/186)). `pip install 'awa-pg[ui]'` pulls in the [`awa-cli`](https://pypi.org/project/awa-cli/) wheel so `python -m awa serve` (and `awa serve` directly) launches the embedded React dashboard. The default `awa-pg` install stays small — workers and producers don't pay for the ~10 MB axum + UI bundle they don't need.
- `python -m awa serve` is now a subcommand. It detects the `awa` binary in `sys.prefix/{bin,Scripts}` (where `awa-cli`'s wheel installs it) and forwards the full argument tail verbatim. If the extra isn't installed, it exits with a `pip install 'awa-pg[ui]'` hint.

### Fixed

- **Restored queue-storage dispatcher throughput under high concurrency** ([#223](https://github.com/hardbyte/awa/issues/223)). Capacity-release wakes still drain ready work immediately, but the dispatcher now uses the configured fixed fallback poll interval instead of geometrically backing off after empty or permit-saturated polls.

### Changed

- Queue-storage throughput benchmarks can run against a non-canonical storage schema and configurable worker count, making local A/B checks safer and easier to reproduce.
- Added TLA+ trace witnesses for receipt-only cancel, callback wait, and DLQ purge paths, plus documentation alignment for the queue-storage design.

## [0.6.0-alpha.4] — 2026-05-03

### Changed

- Added capacity-wake suppression to reduce empty claim churn in quiet queues. This improved some operational churn metrics but regressed high-concurrency queue-storage throughput; alpha.5 keeps the useful wake-drain repair while restoring fixed fallback polling.

## [0.6.0-alpha.3] — 2026-05-02

### Changed

- **Completion-batcher default size lowered from `512` to `128`.** Cross-system matrix runs (1–4 worker processes × 16–128 workers per process) showed `128` delivered the lowest p99 in every cell and `512` bought no throughput while hurting tail latency under multi-process deployments. Override via `AWA_COMPLETION_BATCH_SIZE`. See `docs/benchmarking.md` for tuning notes.
- **Reduced queue-storage claimer heartbeat churn.** Claimer leases now skip refresh writes while still fresh, cutting coordination writes in the dispatch path without changing claim ownership semantics.
- **Updated architecture documentation.** The architecture guide now reflects the queue-storage receipt path, lazy lease materialization, crash recovery, maintenance leadership, and callback orchestration.

### Fixed

- **Receipt completion now serializes with heartbeat materialization.** The queue-storage completion path locks the matching receipt claim before writing its closure, preventing a concurrent heartbeat from recreating `attempt_state` after completion.
- **Hardened mixed Rust/Python chaos smoke coverage.** The mixed-fleet smoke test now waits for worker-observed completions from both runtimes instead of relying on transient terminal-row presence.

## [0.6.0-alpha.2] — 2026-05-02

### Added

- **Vacuum-aware queue storage engine, default-on** ([ADR-019](docs/adr/019-queue-storage-redesign.md)). Append-only `ready_entries`, `deferred_jobs`, `done_entries`, and `dlq_entries` tables, paired with a partitioned receipt ring, keep the dead-tuple footprint bounded under sustained load. Replaces the canonical row-mutating engine for new installs.
- **Receipt-plane ring partitioning** ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)). `lease_claims` and `lease_claim_closures` are partitioned by claim slot and rotated by the maintenance leader.
- **Dead Letter Queue** ([ADR-020](docs/adr/020-dead-letter-queue.md)). Per-queue `dlq_enabled` policy and a full operator surface: `awa dlq depth | list | retry | retry-bulk | move | purge`, plus the matching admin UI tab. See [`docs/dead-letter-queue.md`](docs/dead-letter-queue.md).
- **Descriptor catalog** ([ADR-022](docs/adr/022-descriptor-catalog.md)). Code-declared queue and job-kind metadata (`display_name`, `description`, `owner`, `tags`, `docs_url`) drives admin UI labels and stale/drift detection.
- **Per-claim deadlines** in receipts mode. `QueueConfig.deadline_duration` writes `lease_claims.deadline_at`; the rescue path force-closes expired claims with `'deadline_expired'`.
- **Storage transition tooling**. `awa storage prepare`, `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize`, and `abort` cover the staged upgrade path. Fresh installs auto-finalize on first migrate.
- **`transition_role` runtime capability**. The `enter_mixed_transition` SQL gate requires a live `queue_storage_target` runtime, so a stale fleet cannot accidentally skip the staged path.
- **Migrations** v012, v013, and v014. All idempotent.

### Changed

- New installs default to the queue-storage engine; canonical row-mutating storage is no longer the implicit backend.
- Receipts mode is on by default for fresh deployments.

### Removed

- `benchmarks/portable/` extracted to its own repo at [hardbyte/postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking).
- The pre-0.6 `EXPERIMENTAL_LEASE_CLAIM_RECEIPTS` env alias.

### Upgrade notes

- Update your dependency to `awa = "0.6"` (Rust) / `awa-cli`, `awa-pg` (Python) at the matching version.
- Existing 0.5.x clusters with canonical data must walk the staged storage transition documented in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). Fresh installs auto-finalize.
- Rollback after `enter-mixed-transition` followed by queue-storage writes is one-way (database restore only).
