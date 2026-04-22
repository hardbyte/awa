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

### Evaluation rubric

The `0.6` branch should be judged against these priorities, not just the
headline benchmark:

- transactional enqueue remains a first-class Postgres-native property
- no lost work under failure beats a faster but weaker design
- at-least-once delivery is stated honestly; retries and idempotency are part
  of the contract
- perf work should keep prioritizing:
  - claim path under contention
  - recovery path
  - retry storm behavior
  - observability of saturation and backlog
- any optimization that weakens crash/restart safety or makes retries amplify
  outages should be rejected, even if it wins a microbenchmark

### High-level conclusion

The **core design direction is solid**.

The branch has now removed the two main architectural mistakes from the earlier queue-storage prototypes:

- hot mutable `queue_lanes` rows are gone from the steady-state claim/enqueue path
- receipt-backed claims no longer require scanning unbounded append-only history to determine "still open"

The remaining problems are **not** signs that the overall storage direction is wrong. They are concentrated in:

- lease-ring control-plane churn
- recovery/retry behavior under sustained oversupply
- latency tails during recovery or retry-heavy workloads
- low-worker underfill from claim/start amortization

So the current question is no longer "was the redesign a good idea?".
It was.

The question is now "how much more of the lease/control plane should be made cold before `0.6` lands?".

One concrete outcome after this review: low-worker underfill was not another
queue-storage MVCC hotspot. The strongest fix so far is releasing local worker
capacity when handler execution ends, while keeping durable completion on the
existing `run_lease`-guarded finalization path. That improves `1/4/8/16`
worker throughput without changing the underlying storage state machine.

We also tested a reservation-plane spike for low-worker underfill:

- reserve work ahead in a bounded live frontier
- promote reservation -> active attempt only when a worker actually starts
- expire stale reservations back to ready work if the worker dies before start

The safety boundary was good, but the implementation was reverted. The first
version added too much per-job start cost, and the buffered-reservation
follow-up regressed throughput further. So the current branch does **not**
carry the reservation plane; it only carries the design learning from that
spike.

A later batched reservation/start frontier pass also kept the safety boundary
sound:

- direct promotion from reservation -> receipt-backed attempt was validated
- reserve -> expire -> re-enqueue -> late promotion loses was validated

But the realistic single-producer replica check was still not good enough:

- `1x32`: `clean_1 397/s`, `pressure_1 576/s`, `recovery_1 730/s`
- `4x8`: `clean_1 157/s`, `pressure_1 92/s`, `recovery_1 12/s`

That means the frontier did **not** solve the many-small-replica deployment
shape without introducing large throughput and latency regressions. It was
reverted as well.

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

#### Direct runtime benchmark sanity check

The direct Rust runtime benchmark now prints both:

- **post-insert throughput**: drain rate after the insert phase finishes
- **end-to-end throughput**: total jobs divided by total elapsed time from first insert to final completion

That distinction matters because the old single throughput number over-weighted the
drain phase and made very fast inserters look artificially better.

Current local Docker PG rerun (`postgres://bench:bench@localhost:15555/awa_bench`,
`5000` jobs):

- canonical
  - post-insert throughput: `46827 jobs/s`
  - end-to-end throughput: `6750 jobs/s`
  - exact final dead tuples: `826`
    - `jobs_hot=826`
    - `scheduled_jobs=0`
    - `queue_state_counts=0`
- queue storage
  - post-insert throughput: `11066 jobs/s`
  - end-to-end throughput: `5835 jobs/s`
  - exact final dead tuples: `0`
    - `queue_lanes=0`
    - `ready=0`
    - `done=0`
    - `leases=0`
    - `attempt_state=0`

Interpretation:

- canonical still drains a short preloaded burst faster on this microjob benchmark
- queue storage still has the much cleaner hot-path churn profile
- the direct runtime benchmark is useful as a smoke check, but the longer portable
  phase-driven scenarios remain the more trustworthy picture for sustained behavior
  under readers, pressure, and recovery

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

This is no longer a correctness red flag, but it is still a throughput/backlog
weakness.

After switching queue storage to claim-time effective priority aging and fixing
the receipt-backed retry close path:

- retryable failures are retried correctly
- aged lower-priority work is now visibly completing during recovery
- the backlog is dominated by `available` work, not by deferred retry buildup

The newest semantics run is:

- `benchmarks/portable/results/awa_semantics_retry_priority_mix_20260422_045458.json`

That run shows a materially healthier shape than the earlier benchmark-only
aging pass:

- `clean_1`: completion `642/s`, subscriber p99 `2.9s`, total backlog `247.5`
- `pressure_1`: completion `651/s`, subscriber p99 `23.8s`, total backlog `14.2k`
- `recovery_1`: completion `759/s`, subscriber p99 `28.1s`, total backlog `23.8k`
- `aged_completion_rate` is now real in `pressure_1` and `recovery_1`

So the remaining problem is not "retries are stuck in the wrong state". It is:

- offered load still outruns completion throughput under retry-heavy overload
- queue depth still grows too fast during overload phases
- recovery still has large delivery tails even though starvation is improved

##### 3. Recovery latency tails

Recovery remains the weakest phase in both the main Awa-only runs and the
explicit crash-under-load scenario.

The newest crash-under-load run is:

- `benchmarks/portable/results/awa_semantics_crash_recovery_under_load_20260422_062948.json`

It is important because it confirms two things at once:

- the old restart deadlock cluster is gone
- recovery tails are still too large to call this "done"

Observed shape:

- `baseline`: completion `731/s`, subscriber p99 `349 ms`, total backlog `34`
- `pressure_1`: completion `815/s`, subscriber p99 `15.5s`, total backlog `27.1k`
- `kill`: completion `971/s`, subscriber p99 `44.5s`, total backlog `52.3k`
- `restart`: completion `780/s`, subscriber p99 `58.1s`, total backlog `41.5k`
- `recovery_1`: completion `727/s`, subscriber p99 `30.0s`, total backlog `32.7k`

This is now clearly a performance issue, not a lock-safety issue. The system
recovers, but the recovery path is still too expensive under load.

##### 4. Counts/admin read model

`queue_counts()` was the clearest recurring slow query in the earlier broader perf pack, and that is now partly addressed.

With the cached queue-count snapshot path used by the portable benchmark adapter, the count query is no longer the dominant steady-state warning in the rerun scaling/crash passes:

- worker scaling rerun:
  - `benchmarks/portable/results/worker_scale_20260422_025018.json`
- crash recovery rerun:
  - `benchmarks/portable/results/awa_semantics_crash_recovery_under_load_20260422_025919.json`

At low to mid worker counts, the old `queue_counts()` warning largely
disappears. At higher worker counts it still appears occasionally, but it is no
longer the main bottleneck.

That means the next target has shifted to:

- `claim_ready_runtime(...)` at higher worker counts
- completion-path receipt/lease reconciliation during crash recovery
- commit / connection-acquire stalls during crash/restart pressure

##### 5. Worker scaling shape

The newest scaling sweep is:

- `benchmarks/portable/results/worker_scale_20260422_062119.json`

The high-level result is mixed:

- the branch still scales strongly once Awa has enough workers
- but Awa is underfilled at low worker counts in this benchmark shape

Representative results:

- `1` worker:
  - `awa clean_1`: `35.5/s`, subscriber p99 `55.9s`
  - `pgque clean_1`: `358.7/s`, subscriber p99 `142 ms`
- `16` workers:
  - `awa clean_1`: `309.9/s`, subscriber p99 `33.1s`
  - `pgque clean_1`: `422.3/s`, subscriber p99 `126 ms`
- `32` workers:
  - `awa clean_1`: `796.8/s`, subscriber p99 `68.5 ms`
  - `pgque clean_1`: `519.8/s`, subscriber p99 `112 ms`
- `64` workers:
  - `awa clean_1`: `798.0/s`, subscriber p99 `43.0 ms`
  - `pgque clean_1`: `506.6/s`, subscriber p99 `126 ms`

So the scaling story is now:

- the high-worker regime is strong
- the low-worker regime is still bad enough that we should not ignore it
- the current bottleneck is likely in the claim / completion pipeline shape,
  not the old storage-MVCC hotspot class

### Cross-system position

The newest short `awa` vs `pgque` comparison is:

- `benchmarks/portable/results/custom-20260422T045706Z-4a74b9`

It should still be treated as a **sanity check**, not a headline result.

That run shows:

- Awa leading on throughput in every presented phase
- Awa showing lower dead tuples than pgque in the same short profile
- but Awa also showing very large subscriber and end-to-end tails once the
  short run carries backlog

Representative medians:

- `clean_1`
  - `awa`: `706/s`, subscriber p99 `1.3s`, dead tuples `140`
  - `pgque`: `273/s`, subscriber p99 `177 ms`, dead tuples `514`
- `pressure_1`
  - `awa`: `705/s`, subscriber p99 `32.9s`, dead tuples `215`
  - `pgque`: `456/s`, subscriber p99 `126 ms`, dead tuples `726`

So the cross-system narrative is still not "Awa wins everything". The real
position is:

- Awa's storage design is now strong enough to compete on throughput and dead
  tuples
- but the runtime still has tail-latency problems under short overloaded
  profiles
- those tails need to be understood before we use these short compare runs as
  release messaging

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

- low-worker underfill
- recovery latency
- crash/restart claim-complete cost

### Recommended next steps

In order:

1. Investigate claim/completion cost under recovery
   - especially receipt/lease completion reconciliation
   - and the `COMMIT` / acquire stalls visible in crash-under-load

2. Investigate low-worker underfill
   - the `1/4/8/16` worker scaling results are still too weak
   - a direct `claim_ready_runtime(...)` broadening spike was tried and reverted:
     claiming across multiple ready slots for the chosen lane did help the
     higher-concurrency backlog case, but it regressed `1/4` worker throughput
     enough that it was not worth keeping
   - a later claim-function cleanup spike was also reverted:
     reusing the first candidate lookup to avoid the second ready-slot query,
     and reusing one captured timestamp across the batch, made the SQL look
     cleaner but regressed plugged-in `1/4` worker throughput enough that it
     was not worth keeping
   - so the next low-worker work should focus on fixed per-claim/start cost on
     the current design, not broader ready-slot selection

3. Investigate realistic multi-replica deployment shape
   - holding total workers constant but splitting them across many replicas is
     now clearly a major remaining weakness
   - one benchmark artifact was identified and fixed here:
     - the long-horizon Awa adapter was previously polling queue depth from
       every replica
     - only replica `0` now performs that observer work, which removes the
       artificial "every pod runs queue_counts() every second" load
   - that fix did **not** remove the core multi-replica problem
   - fixed-load replica-shape runs showed:
     - `1x32` remained healthy
     - `2x16` degraded badly
     - `4x8` and `8x4` collapsed
   - a single-producer variant (`PRODUCER_ONLY_INSTANCE_ZERO=1`) still showed
     the same degradation, so this is **not** just queue-enqueue-head
     contention from many producers
   - after the observer fix, the single-producer matrix still looked like:
     - `1x32`: `clean_1 662/s`, `pressure_1 896/s`, `recovery_1 676/s`
     - `2x16`: `clean_1 180/s`, `pressure_1 39/s`, `recovery_1 300/s`
     - `4x8`: `clean_1 76/s`, `pressure_1 25/s`, `recovery_1 77/s`
     - `8x4`: `clean_1 18/s`, `pressure_1 23/s`, `recovery_1 16/s`
   - the remaining blocker is now best understood as multi-process
     claim/start coordination on one queue, not one-big-process underfill alone

4. Investigate retry-heavy overload behavior
   - especially completion throughput vs queue depth growth
   - but now on the corrected claim-time aging baseline
   - note: a quick `PRIORITY_AGING_MS=0` probe was attempted and is not a valid
     comparison yet; it currently panics the maintenance timer because the
     queue-storage runtime still requires a non-zero aging interval

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
- physical priority aging on queue storage: replaced with claim-time effective aging

What remains is the next layer down:

- low-worker underfill
- claim/completion cost under recovery
- retry-heavy overload behavior under oversupply
- recovery tails

That is a good place to be.
