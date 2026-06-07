# ADR-025: Sharded Enqueue Heads

## Status

Accepted. The shard plane is operational; `awa.queue_meta.enqueue_shards` governs the per-queue shard count (default 1, range 1..=64), and every hot-path query — enqueue, claim, completion, receipt rescue, admin lookup, terminal storage — joins or filters on `enqueue_shard`.

## Context

The queue-storage engine allocates each enqueue batch a contiguous `lane_seq` range by advancing a single counter per `(queue, priority)` lane:

```sql
UPDATE queue_enqueue_heads
SET next_seq = next_seq + $count
WHERE queue = $1 AND priority = $2
RETURNING next_seq - $count;
```

This is one row-level lock per producer transaction. Concurrent producers for the same lane serialise on that lock. The semantics are correct and the contract is strict FIFO within the lane; the cost is that producer throughput on a single hot lane is bounded by the end-to-end cost of touching that one row — commit, WAL flush, and the next producer's lock acquire.

Wait-event sampling on a 16-producer same-queue workload attributes producer time as follows:

| Wait type      | Wait event       | % of producer time |
| -------------- | ---------------- | -----------------: |
| `Lock`         | `transactionid`  |              63.5% |
| `Lock`         | `tuple`          |              29.6% |
| running on CPU | —                |               4.4% |
| `IO`           | `WalSync`        |               1.1% |
| `Client`       | `ClientRead`     |               1.1% |
| `IO`           | `DataFileExtend` |               0.3% |

93% of producer wall-clock is row-lock wait, and 93% of that is on the single `UPDATE queue_enqueue_heads` query. The lock contention is structural: a single-counter scheme cannot amortise the lock across producers.

ADR-019 accepted the queue-storage engine as the vacuum-aware replacement for canonical `jobs_hot`. It inherits the single-row head pattern as the simplest correct mapping of a per-row state machine onto an append-only sequence space. The receipt plane (ADR-023) and the various rotation disciplines remove dead tuples but do not address producer-side contention on the live head row.

The two cheap producer-side mitigations that ship alongside this ADR do not address the head row directly:

- A per-store in-process cache of `(queue, priority, shard)` lane presence so subsequent enqueue batches skip the three `INSERT ... ON CONFLICT DO NOTHING` round-trips for known lane rows.
- Completion-batcher defaults of `(batch=512, flush=1ms)` plus the queue-storage fused receipt completion statement, so the completion path keeps worker permit latency low while still amortising per-batch SQL.

## Decision

Add an `enqueue_shard SMALLINT` column to every table in the active plane:

- `queue_enqueue_heads` and `queue_claim_heads` extend their PK to `(queue, priority, enqueue_shard)`.
- `ready_entries` extends its PK to `(ready_slot, queue, priority, enqueue_shard, lane_seq)`.
- `leases` extends its PK to `(lease_slot, queue, priority, enqueue_shard, lane_seq)`.
- `done_entries` extends its PK to `(ready_slot, queue, priority, enqueue_shard, lane_seq)`.
- `lease_claims` carries `enqueue_shard` as a regular column; its PK `(claim_slot, job_id, run_lease)` is unique through `job_id` and stays as-is.

Sharding is a per-queue tunable on `awa.queue_meta.enqueue_shards` (`SMALLINT NOT NULL DEFAULT 1 CHECK (BETWEEN 1 AND 64)`). With the default value of 1, only shard 0 exists and every code path reduces to the pre-shard behaviour observationally — single FIFO per lane, one head row per `(queue, priority)`, terminal keys colliding only on `job_id` (which is globally unique). Raising the value spreads producer writes across the configured shard count.

The producer-side helper `shard_for_enqueue` reads the per-queue shard count once (cached in-process, invalidated on `reset()`) and selects a shard for each no-key `(queue, priority)` sub-batch by advancing a per-store `AtomicU16` counter modulo the shard count. Rows with an `ordering_key` bypass the rotor and route by the deterministic key hash.

The claim-side function `claim_ready_runtime` walks every shard row for a `(queue, priority)` via a `lane_candidates` CTE, picks one candidate (ordered by effective priority, `run_at`, and priority), locks that shard's `queue_claim_heads` row with `FOR UPDATE OF claims SKIP LOCKED`, and drains rows from that shard's `ready_entries` slice. Gap recovery is per-shard.

Receipts and leases carry the shard end-to-end: the claim's `enqueue_shard` rides on `ClaimedEntry`, gets inserted into `lease_claims` (when receipts are on) and `leases` (when they materialise), is read back by the cancel-from-receipt path so the synthesized `done_entries` row lands on the correct shard, and is the join predicate for every admin lookup that joins `ready_entries` to `queue_claim_heads` (queue counts, cancel, priority aging). Without that predicate, a ready row from shard A could match shard B's `claim_seq` and an admin DELETE could remove or move rows from the wrong shard.

`dlq_entries` is unsharded: its PK is `job_id`, which is globally unique.

### Ordering contract: partitioned FIFO

`enqueue_shards > 1` is a semantic mode switch, not a hidden performance optimization. It changes the ordering contract the queue offers and operators opt into it per queue. The peer comparison is SQS Standard vs FIFO, Kafka partitions, Pub/Sub ordering keys, and RabbitMQ sharded queues: a partition is the ordering scope, the operator picks how many partitions, and producers route into them.

`lane_seq` is allocated by `UPDATE queue_enqueue_heads SET next_seq = ...` keyed by `(queue, priority, enqueue_shard)`. Each shard owns an independent strictly-increasing sequence. The contract:

- **`enqueue_shards = 1` (default): strict FIFO per `(queue, priority)`.** Identical to the pre-shard contract. Workloads that depend on cross-producer FIFO at the lane level stay here.
- **`enqueue_shards > 1`: partitioned FIFO per `(queue, priority, enqueue_shard)`.** Strict FIFO is preserved within each shard. No ordering is promised across shards. Two rows enqueued to different shards may be claimed in either order depending on which shard the claim path visits first.

Choosing `S > 1` is the same kind of decision as choosing SQS Standard over SQS FIFO: lock contention scales with the shard count and ordering scope shrinks to one shard. Choosing `S = 1` is the SQS-FIFO-equivalent contract.

### Routing producers into shards

Two modes share the `shard_for_enqueue` entry point:

1. **Rotor (default).** When the caller does not supply an `ordering_key`, the per-store `AtomicU16` rotor selects a shard modulo the queue's shard count. Selection is **per `(queue, priority)` sub-batch within a single `insert_*_tx` call** — all rotor-routed rows that share a destination lane in one batch collapse onto the same shard so a 500-row batch issues one `advance_enqueue_head` UPDATE and one INSERT instead of 500. The rotor advances on each pick, so successive batches spread across shards. This per-batch amortisation is what makes `enqueue_shards > 1` net-faster than `S = 1` at moderate producer concurrency; per-row pick was measured to invert the curve (S>1 slower than S=1) because each batch fanned into S sub-INSERTs.

2. **`InsertOpts::ordering_key` (hash-routed).** When the caller pins a key, `shard_for_ordering_key` maps the key bytes into `[0, shards)` deterministically. Awa uses a portable 64-bit rolling hash implemented in Rust and in the SQL compatibility function, so Rust, SQL, and Python producers route the same key bytes to the same shard without relying on a PostgreSQL extension.

   `ordering_key` is the same primitive as Kafka partition keys or Pub/Sub ordering keys: jobs that share a key share a shard, which preserves partitioned FIFO for that key even across separate `insert_*` calls. At `enqueue_shards = 1` the key is ignored (every key collapses to shard 0).

This trade is the point of the design: lifting the lock contention requires shrinking the ordering scope, and sharding lets each queue choose where on that trade-off it sits.

## Validation

Local A/B sweep on the in-tree `test_queue_storage_enqueue_contention` (16 producers × 15 k jobs each, same queue, post-cache, 3 runs per cell, with the full shard plane wired through claim, receipt, and admin paths):

| `enqueue_shards` | Mean throughput | vs S=1 |
| ---------------- | --------------: | -----: |
| 1 (default)      |   42,516 jobs/s |  1.00× |
| 2                |   68,065 jobs/s |  1.60× |
| 4                |  116,852 jobs/s |  2.75× |
| 8                |  157,000 jobs/s |  3.69× |

Scaling stays substantial up to `S=8` on a 2-producer-per-shard configuration. Per-shard claim-path work and WAL bandwidth set the diminishing-returns shape past S=8; the knee for this concurrency sits in the S=8..16 range. Combined with the in-process `ensure_lane` cache shipped alongside this work, the headline gain over the pre-cache, pre-shard baseline is ~5× (~30,000 jobs/s → 157,000 jobs/s at S=8).

`test_queue_storage_multi_shard_round_trip_through_completion` exercises the full plumbing at S=4: producers spread writes across four shards, workers drain them through completion, and terminal rows land in `done_entries` keyed by shard. The test asserts that every shard holds terminal rows and that at least one `(ready_slot, queue, priority, lane_seq)` tuple is reused across shards — i.e. the shard column is load-bearing in the PK and the end-to-end path correctly carries `enqueue_shard` from claim into the terminal write.

### What this ADR does not validate

The validation here is the producer-side enqueue-contention story. It does not replicate the high-worker-count rescue-path regression that motivated the original perf investigation (1 replica × 256 workers, receipts on, `LEASE_DEADLINE_MS=0` vs default A/B). That regression is on the claim / rescue side of the queue-storage engine; sharding the enqueue head row is a necessary but not sufficient fix. The A/B against the rescue-path workload is left for a follow-up so that the two effects can be measured independently.

## Fairness and observability

The claim-path SQL orders candidate shard heads by `(effective_priority, run_at, priority)` — `run_at` is the natural fairness mechanism. Under steady-state load every shard accumulates its own pending rows; the shard whose oldest waiting row has the earliest `run_at` wins the next claim, its lane head advances, and another shard's oldest row becomes the next pick. Concurrent claimers add a second fairness mechanism: `FOR UPDATE OF claims SKIP LOCKED` sends each claimer to a different shard's head.

The audit is enforced by `test_queue_storage_multi_shard_claim_path_does_not_starve_shards`, which loads four shards equally and asserts every shard's `claim_seq` advanced to its `next_seq` after a worker drains the queue.

Per-shard observability:

- `awa.job.claimed` (counter) carries an `awa.enqueue.shard` attribute on the queue-storage path. Operators sum by that label to see per-shard claim throughput and spot any shard that flatlines while its peers are draining.
- Ad-hoc inspection during incident response is a direct SQL query against `queue_enqueue_heads` and `queue_claim_heads`:

  ```sql
  SELECT priority, enqueue_shard,
         enqueues.next_seq, claims.claim_seq,
         enqueues.next_seq - claims.claim_seq AS lag
  FROM <schema>.queue_claim_heads AS claims
  JOIN <schema>.queue_enqueue_heads AS enqueues USING (queue, priority, enqueue_shard)
  WHERE queue = 'my_hot_queue'
  ORDER BY priority, enqueue_shard;
  ```

  A non-zero `lag` that stays non-zero on one shard while peers drain indicates a starved shard — but the in-tree fairness test exercises the contract and the `awa.job.claimed` per-shard counter is the live signal.

## Lowering `enqueue_shards`

Lowering is safe in the steady state because every claim, rescue, and admin path joins `queue_claim_heads` to `queue_enqueue_heads` on `(queue, priority, enqueue_shard)` without any `shard < current_count` predicate. Concretely:

- The claim function `claim_ready_runtime` walks every row in `queue_claim_heads` for the queue. Rows for shards `>= newS` continue to be picked up and drained as long as their `claim_seq < next_seq`.
- Heartbeat / deadline / callback rescue read the shard from the in-flight `leases` or `lease_claims` row, so a rescued job re-enters the lane it came from regardless of the current shard count.
- The promotion path (`deferred_jobs` → `ready_entries`) calls `shard_for_enqueue` with the _current_ shard count, so promoted rows land on `[0, newS)`. They cannot leak onto out-of-range shards.
- DLQ is unsharded; its PK is `job_id`.

The in-process `enqueue_shards_cache` on `QueueStorage` is the only caveat: a running runtime that cached the old shard count keeps producing to shards `>= newS` until the cache is invalidated by `reset()` or process restart. That is operator-intent-stale but correctness-safe — the rows still claim, run, and finalise through the same code paths. Operators rolling out a `S` reduction restart runtimes (or trigger `reset()`) so producers immediately observe the new value.

Operational procedure:

1. Upsert `awa.queue_meta.enqueue_shards` for the queue:

   ```sql
   INSERT INTO awa.queue_meta (queue, enqueue_shards)
   VALUES ('<q>', <newS>)
   ON CONFLICT (queue)
   DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards;
   ```

2. Restart runtime processes (or rely on natural restart cadence) so the in-process cache observes the new value.
3. Optionally watch the per-shard SQL above until `lag` reaches 0 on shards `>= newS`. The shards' `queue_*_heads` rows linger as harmless empty heads — they cost a few small rows and have no effect on throughput.

The contract is enforced by `test_queue_storage_lowering_enqueue_shards_drains_existing_rows`, which seeds rows on every shard at `S = 4`, lowers to `S = 2`, and asserts every row drains to `done_entries`.

## Consequences

### Positive

- **Row-lock contention on enqueue scales with the per-queue shard count, not with producer concurrency.** Operators have a direct lever for contended lanes without changing application code.
- **Default unchanged.** `enqueue_shards = 1` is observationally identical to the pre-shard layout. Existing tests, ADRs, and TLA+ models that index by `(queue, priority)` continue to describe deployed behaviour at S=1.
- **Cheap to revert.** Lowering `enqueue_shards` requires only that the operator drain rows from the now-out-of-range shards; the underlying tables continue to function during the drain. No schema rollback required for `S>1 → S=1`.
- **Co-located with the storage transition framework.** The migration refuses to run mid-`mixed_transition`, where reshaping the partitioned PKs would block the live engines. On `active` installs the migration takes a brief `ACCESS EXCLUSIVE` during the PK reshape; operators run it during a low-traffic window.

### Negative

- **Ordering scope shrinks from the lane to the shard at `enqueue_shards > 1`.** The contract becomes partitioned FIFO: strict order within `(queue, priority, enqueue_shard)`, no order promised across shards. Applications that document or rely on strict cross-producer FIFO at the lane level pin `S = 1`. Applications that need per-key FIFO (per customer, per order, per account) pass `InsertOpts::ordering_key` so rows for that key collapse onto one shard. Priority aging, deadline rescue, callback resume, and DLQ semantics are unaffected.
- **Claim-side cost is `O(S)` per claim call.** Each `claim_ready_runtime` invocation scans up to S candidate shard heads. With `S=64` and four priorities this is 256 candidate rows; trivial at the current per-claim cost. The `BETWEEN 1 AND 64` check constraint prevents pathological values.
- **Producer-side fairness is statistical.** The `AtomicU16` rotor spreads batches uniformly over time, but a producer that emits a burst of single-row batches lands them on consecutive shards rather than the same one. This is acceptable; the goal is reducing per-row lock pressure, not strict round-robin balance.

## Alternatives Considered

- **Separate `queue_enqueue_head_shards` table joined to `queue_lanes`.** Adds a join on the claim hot path for no benefit over an extended PK; co-locating shard rows inside the existing head tables keeps the claim-side query plan identical at S=1 and predictable at S>1.
- **Hash-shard the queue name itself.** Distributes contention only for cross-queue workloads; this ADR is about the single-queue case where the producer set spans one logical queue.
- **`CREATE SEQUENCE` per shard, `nextval`-based allocation.** Avoids the row lock but loses the batched `next_seq = next_seq + $count` allocation that lets one round-trip reserve a contiguous range for a COPY batch. Per-row `nextval` is strictly worse at high throughput.
- **Global `lane_seq` per `(queue, priority)`, shards allocate from it.** Reintroduces the single-counter contention this ADR exists to remove.

## Relationship to other ADRs

- **ADR-019 (queue-storage redesign).** This ADR refines the segmented-storage hot path. The append-only / rotate / prune discipline is unchanged; sharding lifts contention _within_ that discipline.
- **ADR-023 (receipt plane ring partitioning).** Independent. ADR-023 attacks dead-tuple density on the receipt plane; this ADR attacks row-lock wait on the enqueue plane. Receipts carry `enqueue_shard` so they route correctly through the cancel and rescue paths at S>1.
- **ADR-016 (priority aging).** Aging operates on `run_at` and the effective priority. The `claim_ready_runtime` per-shard candidate walk inherits the same aging clause; FIFO-within-shard does not change the aging contract.
- **ADR-002 (BLAKE3 uniqueness hashing).** `ordering_key` routing is deliberately separate from ADR-002 unique-key fingerprints. Shard routing needs a small, portable hash that can run identically in Rust and inside `awa.insert_job_compat`; uniqueness keeps using the BLAKE3 fingerprint contract from ADR-002.

## Implementation

- Migration `v017_shard_queue_enqueue_heads.sql` adds the column to every table in the active plane and reshapes each PK. The partitioned tables (`ready_entries`, `done_entries`, `leases`) drop the parent PK first so the cascaded inherited PKs come with it, then `ADD COLUMN` and `ADD PRIMARY KEY` on the parent — operations on a partitioned parent propagate to every leaf. The migration is gated on `storage_transition_state.state != 'mixed_transition'` to avoid reshaping under live cutover traffic.
- `awa-model/src/queue_storage.rs` threads `enqueue_shard` through the enqueue path (`ensure_lane`, `advance_enqueue_head`, the three `insert_*_tx` variants), the claim function `claim_ready_runtime`, the receipts path (the `claimed_cte` INSERT into `lease_claims`, the non-receipts INSERT into `leases`, the materialization helper, the cancel-from-receipt hydration, the heartbeat and deadline receipt-rescue scans), the admin lookups (`queue_counts_exact`, `cancel_job_tx`, `age_waiting_priorities`), and the terminal storage path (`done_entries` INSERT and the consumer SELECTs that hydrate `DoneJobRow`).
- The in-process lane cache is keyed on `(queue, priority, enqueue_shard)`. The rollback-recovery retry path in `advance_enqueue_head` invalidates by triple and calls `ensure_lane_inserts` directly so a concurrent re-marker cannot trick it into skipping the repair.
- The in-tree A/B bench `test_queue_storage_enqueue_contention` accepts `AWA_QS_CONTENTION_SHARDS` to seed `queue_meta.enqueue_shards` before driving the producer fleet.

## TLA+

`correctness/storage/AwaSegmentedStorage.tla` models one `(queue, priority, enqueue_shard)` lane. That keeps the lifecycle state-space small while still checking the per-shard FIFO, lease, receipt, terminal, DLQ, and rotate/prune invariants.

The cross-shard invariant lives in `correctness/storage/AwaShardedPrune.tla`. It models two shards with independent sequence counters, so both shards can legitimately contain `lane_seq = 1`. The passing config requires queue-ring prune to match ready rows to done rows by `(enqueue_shard, lane_seq)`. The broken config intentionally drops `enqueue_shard` from that match and produces the counterexample where shard 0's completed row masks shard 1's pending ready row.

`correctness/storage/AwaStorageLockOrder.tla` carries the lock-order side. At the lock abstraction level, each shard is a disjoint copy of the same row-level resources; enqueue touches one shard's head rows per transaction, and claim touches one physical claim row at a time, so the existing deadlock proof composes across shards.
