## Queue Storage Performance And Design Review

Date: 2026-04-22
Branch: `feature/vacuum-aware-storage-redesign`
Baseline commit for this review: `3919b05`

### Scope

This review summarizes the current state of the queue-storage redesign after:

- split queue heads (`queue_enqueue_heads` / `queue_claim_heads`)
- bounded receipt frontier (`open_receipt_claims`)
- hybrid lease plane (`lease_claims` + `attempt_state` + escalation to `leases`)
- verification-only startup for queue-storage targets
- claim locking on `queue_claim_heads`
- semantics/scaling/crash benchmark coverage

It is intended to answer two questions:

1. Is the core `0.6` storage design direction solid?
2. What are the remaining implementation and performance risks?

### High-level conclusion

The **core design direction is solid**.

The branch has now removed the two main architectural mistakes from the earlier queue-storage prototypes:

- hot mutable `queue_lanes` rows are gone from the steady-state claim/enqueue path
- receipt-backed claims no longer require scanning unbounded append-only history to determine "still open"

The remaining problems are **not** signs that the overall storage direction is wrong. They are concentrated in:

- lease-ring control-plane churn
- recovery/retry behavior under sustained oversupply
- latency tails during recovery or retry-heavy workloads

So the current question is no longer "was the redesign a good idea?".
It was.

The question is now "how much more of the lease/control plane should be made cold before `0.6` lands?".

### What now looks solid

#### 1. Queue plane

Append-only queue storage is the right foundation.

`ready_entries` / `done_entries` are no longer the dominant MVCC problem.
The old `jobs_hot` failure mode is not coming back.

#### 2. Lane/control plane

The `queue_lanes` hotspot is solved.

The split-head design removed the last "single hot metadata row per `(queue, priority)`" problem.
In the newer runs, `queue_lanes` contributes effectively zero dead tuples.

That means the current control-plane split is the right one:

- cold lane metadata
- hot enqueue head
- hot claim head

#### 3. Receipt-backed short path

The receipt-backed short path is directionally correct.

The important improvement was not append-only claims alone. It was append-only claims **plus** a bounded live frontier:

- `lease_claims` for history
- `open_receipt_claims` for "currently open"

That removed the earlier long-horizon regression where rescue and counts had to anti-join growing claim history.

#### 4. Multi-worker / multi-replica validation

We are no longer testing only single-process toy cases.

Current coverage includes:

- many-worker single-runtime pressure via the portable Awa adapter (`WORKER_COUNT=32`)
- two-client drain-without-duplication runtime test
- two-replica crash-under-load portable scenario
- worker-count scaling sweep (`1,4,16,32`)

That is enough to say the redesign is now being exercised under real concurrency shapes, not just unit-level happy paths.

### Current best evidence

#### Awa-only long-horizon pressure baseline

Bundle:

- `benchmarks/portable/results/custom-20260421T194144Z-6b8a25`

Phase medians:

- `clean_1`
  - throughput: `800/s`
  - subscriber p99: `14 ms`
  - end-to-end p99: `16.5 ms`
  - dead tuples: `315`
- `readers_1`
  - throughput: `800/s`
  - subscriber p99: `22 ms`
  - end-to-end p99: `24 ms`
  - dead tuples: `2534`
- `pressure_1`
  - throughput: `1200/s`
  - subscriber p99: `42 ms`
  - end-to-end p99: `44 ms`
  - dead tuples: `721.5`
- `recovery_1`
  - throughput: `796/s`
  - subscriber p99: `264 ms`
  - end-to-end p99: `266 ms`
  - dead tuples: `688.5`

Interpretation:

- clean steady-state is good
- active-reader behavior is much healthier than earlier queue-lane builds
- pressure behavior is good
- recovery is the weakest phase, but no longer catastrophically so

#### Table-level dead tuples in the current Awa baseline

From `custom-20260421T194144Z-6b8a25/raw.csv`, the dominant dead-tuple sources are now:

- `lease_ring_state`
- `lease_ring_slots`
- then small residual `queue_ring_state` / `queue_ring_slots`

Peak table dead tuples by phase:

- `clean_1`
  - `lease_ring_slots`: `167`
  - `lease_ring_state`: `142`
- `readers_1`
  - `lease_ring_state`: `2322`
  - `lease_ring_slots`: `2318`
- `pressure_1`
  - `lease_ring_slots`: `839`
  - `lease_ring_state`: `149`
- `recovery_1`
  - `lease_ring_slots`: `838`
  - `lease_ring_state`: `141`

That is a materially better situation than the old lane-row problem.

The remaining churn is now clearly in the lease control plane, not the queue plane.

#### Retry / priority semantics

Bundle:

- `benchmarks/portable/results/awa_semantics_retry_priority_mix_20260422_003340.json`

Important result:

- the old `retry ignored` correctness bug is gone
- first-attempt failures are now being retried through the real portable loop
- priority aging is now actually wired into maintenance, so the scenario is measuring real aging behavior instead of a benchmark-only fiction

But this scenario also shows the remaining weakness:

- retry-heavy oversupplied workloads produce large queue depths and very high delivery tails

Phase medians:

- `clean_1`
  - completion rate: `631.7/s`
  - retryable failure rate: `91.4/s`
  - queue depth: `646`
  - retryable depth: `240.5`
  - total backlog: `940`
  - subscriber p99: `6311.9 ms`
  - aged completion rate: `0`
- `pressure_1`
  - completion rate: `537.6/s`
  - retryable failure rate: `77.5/s`
  - queue depth: `27929.5`
  - retryable depth: `196.5`
  - total backlog: `28158.5`
  - subscriber p99: `46202.9 ms`
  - aged completion rate: `22.3`
- `recovery_1`
  - completion rate: `527.0/s`
  - retryable failure rate: `81.0/s`
  - queue depth: `55551`
  - retryable depth: `192`
  - total backlog: `55731`
  - subscriber p99: `3070 ms`
  - aged completion rate: `182.3`

This is not a correctness failure anymore. It is now clearly a throughput and queuing-behavior problem under retry-heavy oversupply:

- clean is near steady state
- pressure is the intentional overload window
- recovery does show real aging/drain of lower-priority work
- the backlog is dominated by `available` work, not deferred retries

#### Crash recovery under load

Bundle:

- `benchmarks/portable/results/awa_semantics_crash_recovery_under_load_20260422_014850.json`

Result:

- two replicas ran
- one worker was killed mid-run
- recovery completed without the earlier deadlock/slow-query deadlock cluster
- restart itself is healthy enough to proceed, but the read-side control plane remains expensive during kill/restart/recovery

Phase medians:

- `baseline`
  - completion rate: `261.7/s`
  - subscriber p99: `19046.4 ms`
  - total backlog: `27316.5`
- `pressure_1`
  - completion rate: `220.9/s`
  - subscriber p99: `46891.0 ms`
  - total backlog: `94553.5`
- `kill`
  - completion rate: `211.9/s`
  - total backlog: `167839.5`
- `restart`
  - completion rate: `167.1/s`
  - total backlog: `208071.0`
- `recovery_1`
  - completion rate: `133.8/s`
  - total backlog: `251684.0`

This says:

- restart-time deadlocks are no longer the story
- the remaining weakness is recovery throughput and read-side/control-plane cost under heavy backlog
- `queue_counts()` is the most obvious recurring slow query in this scenario
- startup also still pays a visible `open_receipt_claims` refill cost on the restarted replica

#### Worker scaling

Bundle:

- `benchmarks/portable/results/worker_scale_20260422_013951.json`

Observed `clean_1` throughput:

- `1 worker`: `15.4/s`
- `4 workers`: `40.3/s`
- `8 workers`: `107.2/s`
- `16 workers`: `184.1/s`
- `32 workers`: `339.5/s`
- `64 workers`: `528.9/s`

Observed `pressure_1` throughput:

- `1 worker`: `12.5/s`
- `4 workers`: `46.7/s`
- `8 workers`: `110.2/s`
- `16 workers`: `190.0/s`
- `32 workers`: `288.2/s`
- `64 workers`: `457.1/s`

Interpretation:

- throughput still scales upward with workers
- we are not seeing an obvious new serialization cliff at `16`, `32`, or `64`
- the limiting behavior here is queueing and expensive read-side bookkeeping, not an enqueue/claim serialization collapse
- low-worker runs are heavily oversupplied, so latency is dominated by backlog, but the scaling direction is still correct

### Current design assessment

#### What is solid enough to keep

Keep:

- append-only queue entries
- split enqueue/claim heads
- bounded receipt frontier (`open_receipt_claims`)
- lazy `attempt_state`
- escalation to mutable `leases` only when needed
- verification-only startup for queue-storage targets

I would not revisit any of those unless a correctness proof forces it.

#### What still needs redesign or deeper tuning

##### 1. Lease-ring control plane

This is still a real MVCC hotspot, but it is no longer the only or even always the first bottleneck seen in the broader pack.

The evidence is strong:

- queue-lane churn is gone
- append-only claim-history scans are no longer the bottleneck
- lease-ring state/slot tables now dominate dead tuples

This suggests the next meaningful storage improvement is still in the lease plane, but specifically in the **lease ring control plane**, not the queue plane.

Likely directions:

- colder lease rotation metadata
- fewer control-plane writes per claim/complete cycle
- a different bounded live-set representation for active lease slots

##### 2. Retry-heavy overload behavior

The semantics run is the current red flag.

Even after fixing the stale-ignore path, retry-heavy mixed-priority overload produces:

- large backlog growth
- bad subscriber and end-to-end tails
- weak recovery behavior

That means we still need a better story for one or more of:

- retry rescheduling policy
- retry prioritization / starvation protection
- backpressure / offered-load shaping in the benchmark path
- faster reclaim of retryable work under sustained retry pressure

##### 3. Recovery latency tails

Recovery remains the weakest phase in the main Awa-only runs and in the crash-under-load scenario.

This is now more of a performance issue than a lock-safety issue.
The system recovers, but the tails are still too large.

##### 4. Counts/admin read model

`queue_counts()` was the clearest recurring slow query in the earlier broader perf pack, and that is now partly addressed.

With the cached queue-count snapshot path used by the portable benchmark adapter, the count query is no longer the dominant steady-state warning in the rerun scaling/crash passes:

- worker scaling rerun:
  - `benchmarks/portable/results/worker_scale_20260422_025018.json`
- crash recovery rerun:
  - `benchmarks/portable/results/awa_semantics_crash_recovery_under_load_20260422_025919.json`

At low to mid worker counts, the old `queue_counts()` warning largely disappears. At higher worker counts it still appears occasionally, but it is no longer the main bottleneck.

That means the next target has shifted to:

- `age_waiting_priorities` under backlog pressure
- `claim_ready_runtime(...)` at higher worker counts
- completion-path receipt/lease reconciliation during crash recovery

### Cross-system position

The newest short `awa` vs `pgque` comparison is:

- `benchmarks/portable/results/custom-20260421T225449Z-2207b2`

But it should be treated as a **sanity check**, not a headline result:

- the phases are very short
- both systems carry visible backlog
- throughput is lower than in the better-established longer Awa-only profiles

The more trustworthy conclusion today is:

- Awa’s current design is strong enough to justify continued investment
- cross-system headline claims should still be based on longer, better-settled profiles after the next lease-plane work

### Overall judgement

#### Is the core design solid?

Yes.

More specifically:

- the `0.6` storage direction is solid
- the branch is no longer at risk of regressing to the `jobs_hot` failure mode
- the remaining issues are implementation/performance refinement, not foundational doubt

#### Is the current implementation ready?

Not yet.

The branch is now **correct enough to keep iterating confidently**, but still has obvious room to improve in:

- retry-heavy overload
- recovery latency
- lease-ring control-plane churn

### Recommended next steps

In order:

1. Investigate lease-ring control-plane churn
   - still a real MVCC hotspot

2. Investigate `age_waiting_priorities`
   - now one of the clearest recurring slow queries once count polling is cooled

3. Investigate claim/completion cost under recovery
   - especially `claim_ready_runtime(...)` and the receipt/lease completion reconciliation path

4. Investigate retry-heavy overload behavior
   - especially completion throughput vs queue depth growth vs priority behavior

5. Re-run a longer settled `awa` vs `pgque` comparison after those fixes
   - avoid using very short cross-system runs as the main narrative

6. Add a compact report view for semantics/scaling scenarios
   - not because it is urgent, but because these scenarios are now part of the real performance story

### Bottom line

The branch is in a materially better place than it was before the recent lease-plane and transition-fix work:

- queue-lane MVCC problem: solved
- restart-time schema/backfill contention: solved
- stale receipt retry path: solved
- long-horizon history-scan regression: solved

What remains is the next layer down:

- lease-ring control-plane churn
- priority aging / backlog reshaping cost
- claim/completion cost under recovery
- retry-heavy overload behavior
- recovery tails

That is a good place to be.
