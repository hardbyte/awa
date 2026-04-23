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

After adding the dedicated `awa-canonical` adapter and fixing the
single-producer observer artifact, we tried one more explicit
reserved-but-not-started frontier on the same gate (`1x32` and `4x8`,
single-producer only). That version was also reverted.

- `1x32`: `clean_1 511/s`, `pressure_1 486/s`, `recovery_1 531/s`
- `4x8`: `clean_1 86/s`, `pressure_1 82/s`, `recovery_1 85/s`

So the first-principles conclusion is now stronger:

- a distinct reservation state is still the right conceptual fix
- but the current explicit frontier implementation shape adds too much
  transactional overhead to the hot start path
- the remaining blocker is still many-small-replica claim/start coordination,
  but it will not be solved by this frontier shape without another batching
  step or a cheaper promotion mechanism

### Next design pass: striped claim heads instead of explicit reservations

After the reverted frontier passes, the next recommended direction is **not**
another reserve/promote control plane. The stronger candidate is to reduce
multi-replica contention at the source by **striping the claim cursor itself**.

The current queue-storage design still serializes all claimers for one
`(queue, priority)` through a single `queue_claim_heads` row. That scales
acceptably for one large worker process (`1x32`) but collapses when many small
replicas (`4x8`, `8x4`) all contend on the same hot queue.

The proposed next shape is:

- keep the current direct `ready -> active attempt` transition
- keep `running_depth` honest
- keep crash/retry/rescue semantics unchanged
- replace one `queue_claim_heads(queue, priority)` row with a small fixed set of
  `queue_claim_heads(queue, priority, claim_shard)` rows

Conceptually:

- enqueue still appends immutable `ready_entries`
- each ready row carries a `claim_shard`
- claimers lock one shard-local claim head at a time using `SKIP LOCKED`
- many small replicas can claim from different shards concurrently
- no new reserved-but-not-started state is needed
- no extra promotion transaction is added to the short-job hot path

This is a better fit for the current evidence because it targets the measured
serialization point directly:

- `1x32` already shows that the current start/completion path can work when
  claim contention is low
- `4x8` shows that many small replicas are coordinating badly even with one
  producer and low dead tuples
- explicit reservations improved coordination but paid too much extra hot-path
  transaction cost

Key design constraints for a striped claim-head pass:

- preserve no-lost-work semantics under crash/restart
- keep claim and start in one hot-path transaction for short jobs
- preserve honest `running_depth`
- avoid reintroducing a hot mutable jobs table or a long-lived reservation
  frontier
- keep fairness good enough that low-priority or older work does not starve
  behind shard-local skew

The likely trade-off is that strict per-priority FIFO becomes **approximate**
across shards rather than globally serialized through one lane head. That is
acceptable only if:

- ordering skew is bounded by the shard count
- claim-time effective priority aging still works across shards
- real fairness/backlog behavior improves materially in the realistic
  `4x8`/`8x4` deployment shape

Acceptance bar for a striped-claim pass:

- `1x32` remains roughly competitive with the current branch
- `4x8` improves materially on throughput and latency
- crash-under-load still behaves correctly
- dead tuples remain low
- no new hidden control-plane state is needed to explain `running_depth`

We implemented and measured the first striped-claim-head version, then
reverted it.

Results on the same single-producer gate:

- `1x32`
  - `clean_1 739/s`
  - `pressure_1 897/s`
  - `recovery_1 747/s`
- `4x8`
  - `clean_1 152/s`
  - `pressure_1 168/s`
  - `recovery_1 52/s`

That means:

- the striped claim heads did reduce the single-row claim-head serialization
  point
- `1x32` stayed healthy
- `4x8` improved one pressure phase versus the prior queue-storage baseline,
  but recovery still collapsed badly and the overall shape was not good enough
  to keep

So the next many-small-replica fix still needs to do more than split one claim
cursor into shards. The remaining cost is not just the single-row cursor lock;
it is still the amount of per-start coordination each small replica pays on the
hot queue.

### Reverted stateful receipt-frontier pass

We then tried the next more integrated design: keep a distinct
`reserved-but-not-started` state, but make it live inside the receipt frontier
so batched worker start becomes an in-place state transition instead of a
second insert/delete promotion into another hot table.

That version was safe enough to test:

- a direct `reserve -> start` round-trip worked
- the existing short-job receipt runtime test passed again
- reserved work stayed distinct from running work semantically

But the realistic gate still regressed badly, so it was reverted.

Single-producer results on the same short gate:

- `1x32`
  - `clean_1 488/s`
  - `pressure_1 850/s`
  - `recovery_1 680/s`
  - subscriber p99 `599 / 3105 / 2144 ms`
- `4x8`
  - `clean_1 91/s`
  - `pressure_1 106/s`
  - `recovery_1 125/s`
  - subscriber p99 `25936 / 48464 / 62587 ms`

That means:

- folding reservation state into the receipt frontier still adds too much hot
  path cost
- the problem is not just *where* the state lives
- the remaining blocker is still the extra coordination work many small
  replicas pay before actual execution starts

So the current first-principles picture is:

### Queue striping first pass

We also moved queue striping up from a future escape hatch into a real measured track.
The first implementation used:

- logical queue -> physical stripes (`queue#0..N-1`)
- deterministic stripe selection on enqueue
- cyclic stripe probing on claim
- no extra per-job reservation or promotion state

Measured with `QUEUE_STRIPE_COUNT=4` on the same single-producer realistic gate:

The first striping readout looked promising on throughput, but alarming on
dead tuples and `running_depth`. A follow-up root-cause pass showed the
regression signal was mostly a benchmark artifact:

- the portable event-table set was **not sampling `open_receipt_claims`**, which
  is the real short-job churn hotspot
- the observer was using a hard `15s` queue-count cache TTL against very short
  phases, which overstated logical `running_depth`

After fixing the benchmark path:

- `open_receipt_claims` is now part of the sampled queue-storage event tables
- queue-count sampling uses a short, configurable max age (`QUEUE_COUNT_MAX_AGE_MS`)
  tied to the sample interval by default

Fresh `4x8` comparisons after that fix show:

Striped (`QUEUE_STRIPE_COUNT=4`)
- `clean_1`: throughput `797.7/s`, subscriber p99 `32 ms`, median dead tuples `8817`
- `pressure_1`: throughput `1198.0/s`, subscriber p99 `34 ms`, median dead tuples `3646`
- `recovery_1`: throughput `798.8/s`, subscriber p99 `36 ms`, median dead tuples `14033`

Unstriped (`QUEUE_STRIPE_COUNT=1`)
- `clean_1`: throughput `792.8/s`, subscriber p99 `21 ms`, median dead tuples `8771`
- `pressure_1`: throughput `1183.3/s`, subscriber p99 `24 ms`, median dead tuples `3683`
- `recovery_1`: throughput `786.0/s`, subscriber p99 `33 ms`, median dead tuples `13838`

And direct live-table inspection during a fresh striped `800 -> 1200 -> 800`
repro showed:

- `open_receipt_claims` carries the real dead-tuple churn
- `lease_claim_closures` dead tuples remain `0`
- `done_entries_*` dead tuples remain `0`
- exact queue-count snapshots remain plausible once the cache TTL is reduced

So the corrected conclusion is:

- striping did **not** introduce a special `lease_claim_closures` or
  `done_entries_*` regression
- the earlier “striping regression” mostly reflected a benchmark blind spot and
  stale observer counts
- the real hot table under both striped and unstriped short-job pressure is
  `open_receipt_claims`

That means queue striping is still experimental, but it is now being judged on
the right thing:

- does it improve realistic hot-queue distribution/tails enough to justify the
  added complexity?
- and does it avoid making the existing `open_receipt_claims` churn materially
  worse than the unstriped baseline?

A follow-up stripe-count sweep on the corrected path gave a clearer answer:

- `QUEUE_STRIPE_COUNT=8` was too much
- `QUEUE_STRIPE_COUNT=4` helped the hot phase but hurt the `1x32` shape
- `QUEUE_STRIPE_COUNT=2` is the first candidate that looks plausibly worth
  keeping

Short `4x8` sweep (`15s` warmup, `30s` phases):

- `1` stripe: `762 / 887 / 909/s` with subscriber p99 `336 / 2487 / 6029 ms`
- `2` stripes: `796 / 1054 / 726/s` with subscriber p99 `166 / 378 / 425 ms`
- `4` stripes: `762 / 1092 / 761/s` with subscriber p99 `142 / 439 / 713 ms`
- `8` stripes: `729 / 957 / 627/s` with subscriber p99 `562 / 558 / 1729 ms`

That sweep was still noisy, so a longer `4x8` confirmation pair (`30s`
warmup, `60s` phases) was run comparing `1` vs `2` stripes:

- unstriped:
  - `clean_1 446/s`, subscriber p99 `7823 ms`, queue depth `5537`
  - `pressure_1 465/s`, subscriber p99 `33579 ms`, queue depth `34286`
  - `recovery_1 520/s`, subscriber p99 `64455 ms`, queue depth `59008`
- two stripes:
  - `clean_1 667/s`, subscriber p99 `1728 ms`, queue depth `1070`
  - `pressure_1 553/s`, subscriber p99 `15630 ms`, queue depth `14150`
  - `recovery_1 762/s`, subscriber p99 `36340 ms`, queue depth `26252`

So the current striping conclusion is:

- a real `4x8` win does seem possible
- the promising candidate is specifically **2 stripes**
- the remaining problem is no longer “does striping help at all?”
- it is “can a 2-stripe hot-queue mode make tails boring enough without
  worsening `open_receipt_claims` churn?”

We also built and measured a first dynamic striping controller:

- stripe universe `4`
- queue-global `queue_stripe_state`
- monotonic `1 -> 2` active-stripe expansion
- enqueue and claim both restricted to the active stripe set

That controller was **not** keepable.

Initial controller:

- `1x32`
  - `clean_1 790/s`, subscriber p99 `347 ms`, queue depth `20`
  - `pressure_1 810/s`, subscriber p99 `9548 ms`, queue depth `8618`
  - `recovery_1 646/s`, subscriber p99 `18326 ms`, queue depth `12936`
- `4x8`
  - `clean_1 723/s`, subscriber p99 `550 ms`, queue depth `54`
  - `pressure_1 672/s`, subscriber p99 `13103 ms`, queue depth `13358`
  - `recovery_1 604/s`, subscriber p99 `43499 ms`, queue depth `30339`

Faster-expansion retune:

- `1x32`
  - `clean_1 528/s`, subscriber p99 `5226 ms`, queue depth `652`
  - `pressure_1 693/s`, subscriber p99 `8757 ms`, queue depth `6725`
  - `recovery_1 388/s`, subscriber p99 `34144 ms`, queue depth `15994`
- `4x8`
  - `clean_1 591/s`, subscriber p99 `5980 ms`, queue depth `3582`
  - `pressure_1 502/s`, subscriber p99 `20365 ms`, queue depth `16568`
  - `recovery_1 496/s`, subscriber p99 `48595 ms`, queue depth `28699`

So the updated striping recommendation is:

- static `2` stripes remains the leading candidate
- the current dynamic-striping controller should be reverted
- if dynamic striping is revisited later, it needs a substantially cheaper
  control path than queue-global state consulted from both enqueue and claim

One measurement caveat from this pass:

- the portable `awa-bench` adapter currently emits `claim_p99_ms` as the same
  value as `subscriber_p99_ms`
- so it should **not** be treated as an independent database-claim latency
  signal in these striping investigations
- the reliable striping signals here are throughput, queue depth/backlog
  growth, per-replica completion spread, and dead-tuple sources

### Reverted sticky shard leasing v1

We then tried the next design shift: make claim authority sticky at the shard
level instead of introducing another per-job pre-start state transition.

The v1 sticky-shard pass did this:

- added shard-local `queue_claim_heads(queue, priority, claim_shard)`
- assigned each ready row to a `claim_shard`
- added `lane_shard_leases(queue, priority, claim_shard, owner_instance_id, expires_at)`
- kept the direct short-job path:
  - `ready -> active_receipt`
- had claimers prefer owned or expired shards, with no explicit reservation or
  promotion step

That version compiled cleanly, passed targeted queue-storage runtime tests, and
was benchmarked on the same realistic single-producer gate.

Results:

- `1x32`
  - `clean_1 750/s`
  - `pressure_1 1063/s`
  - `recovery_1 723/s`
  - subscriber p99 `147 / 775 / 237 ms`
  - median dead tuples `197 / 153 / 128`
- `4x8`
  - `clean_1 131/s`
  - `pressure_1 175/s`
  - `recovery_1 0/s`
  - subscriber p99 `58 / 65 / 0 ms`
  - median dead tuples `178 / 130 / 138`

This tells us:

- sticky shard ownership is promising for the `1x32` shape
- it keeps churn low and does not need a second start-phase transaction
- but the naïve ownership/renewal rules over-localize work in the `4x8` shape
- recovery can stall entirely because shards are not being rebalanced or stolen
  aggressively enough once ownership goes stale or uneven

So sticky shard leasing as a concept is still alive, but **v1 is not
keepable**. Any next pass has to add a real rebalance / steal policy rather
than just sticky ownership over expired or unowned shards.

### Reverted sticky shard leasing v2

We then tried the next obvious refinement: keep the direct short-job path, but
make shard ownership **decay and rebalance** instead of staying sticky.

The v2 pass added:

- `last_claimed_at` on `lane_shard_leases`
- derived shard states:
  - `unowned`
  - `owned-active`
  - `owned-idle`
- renewals only on successful claims
- idle-shard steal eligibility based on recent inactivity
- per-instance queue-storage claiming, so shard ownership could be tied to the
  live runtime instance

The important part is that v2 still preserved the good short-job invariant:

- direct `ready -> active_receipt`
- no reserved-but-not-started job state
- no second start-phase promotion transaction

That version also compiled cleanly and passed the focused queue-storage runtime
tests we use as the fast gate:

- `test_queue_storage_claim_runtime_applies_priority_aging_dynamically`
- `test_queue_storage_short_jobs_complete_via_lease_claim_receipts`

Results on the realistic single-producer gate:

- `1x32`
  - `clean_1 799/s`
  - `pressure_1 1196/s`
  - `recovery_1 789/s`
  - subscriber p99 `58 / 74 / 100 ms`
  - median dead tuples `109.5 / 121.0 / 111.5`
- `4x8`
  - `clean_1 44/s`
  - `pressure_1 52/s`
  - `recovery_1 0/s`
  - subscriber p99 `17.5 / 10.5 / 0 ms`
  - median dead tuples `177.5 / 139.5 / 138.0`

This is not keepable.

What it tells us:

- the direct short-job path remains healthy in the `1x32` shape
- shard leasing with idle-aware rebalance does **not** by itself solve the
  many-small-replica problem
- `4x8` is still collapsing hard enough that the remaining coordination cost is
  deeper than “one hot claim-head row with overly sticky ownership”

So sticky shard leasing is now in the same bucket as striped claim heads:

- directionally useful
- not sufficient by itself

The remaining blocker is still the same one:

- many-small-replica claim/start coordination on a hot queue

But the search space is now tighter:

- unsafe local buffering: wrong
- explicit reserve/promote frontier: too expensive
- stateful receipt frontier: too expensive
- striped claim heads alone: not enough
- sticky shard leasing v1/v2: not enough

### Next design pass: bounded claimers per queue

The next direction to try is **bounded claimers per queue**.

The idea is to stop treating every replica with free workers as an active
claimer for one hot queue. Instead:

- only a small bounded set of replicas may claim from that queue at once
- all other replicas remain executor-only for that queue
- jobs still go directly `ready -> active_receipt`
- no reserved-but-not-started job state is introduced
- no second hot-path promotion transaction is added

This is different from a fragile leader:

- there can be more than one active claimer
- ownership is time-bounded
- expiry and takeover are automatic
- claimer ownership controls claim authority, not job state

Why this direction is promising:

- repeated experiments now suggest the main remaining cost is **how many
  independent small replicas are trying to coordinate on one hot queue**
- several reservation/frontier variants improved coordination but paid too much
  extra hot-path state-transition cost
- bounded claimers attacks the measured blocker directly while preserving the
  current job lifecycle semantics

The concrete design package for this next pass is tracked in
[`bounded-claimers-plan.md`](bounded-claimers-plan.md).

That plan also records a future escape hatch worth keeping on the table:

- **queue striping as a hot-queue mode**

That gives us both:

- a better default engine
- and an explicit escape hatch for extreme single-queue workloads

#### Fixed-cap bounded claimers (reverted)

The first bounded-claimers implementation used a small fixed queue-level cap:

- queue-level claimer leases in queue storage
- a fixed active claimer limit per queue
- direct `ready -> active_receipt` start path preserved
- no per-job reservation or promotion state

That version was safe enough to test and it preserved the good `1x32` shape:

- `1x32`
  - `clean_1 800/s`
  - `pressure_1 1197/s`
  - `recovery_1 800/s`

But it was **not** keepable, because the realistic hot-queue `4x8` shape still
collapsed once the workload moved beyond the calm phase:

- `4x8`
  - `clean_1 250/s`
  - `pressure_1 34/s`
  - `recovery_1 0.4/s`
  - subscriber p99 about `49 / 396 / 38 ms`
  - median dead tuples stayed low at about `181 / 138 / 153`

Interpretation:

- the fixed claimer cap does reduce contention enough to help the calm
  `4x8 clean_1` phase
- but it starves the hot queue under `pressure_1` and especially `recovery_1`
- so a **hard fixed cap is too blunt**

The next bounded-claimers pass should therefore be:

- **adaptive**, not fixed
- locality-first when the queue is calm
- able to expand the active claimer set during sustained saturation
- able to contract again after backlog drains

The target is no longer “choose the right fixed cap.” It is:

- keep the calm path cheap
- raise claim parallelism only when measured signals say the queue is genuinely
  saturated

In other words, the repeated lesson still holds:

- strong fairness over time is the right target
- but the runtime must still be able to raise claim parallelism when one hot
  queue is genuinely saturated

#### Adaptive bounded claimers MVP (reverted)

The next pass replaced the fixed cap with a small adaptive controller:

- queue-level claimer leases remained
- the dispatcher started at `1` active claimer target per queue
- repeated full-batch claims expanded the target toward `4`
- repeated empty claims contracted it back toward `1`

The safety/runtime checks stayed clean:

- targeted bounded-claimer runtime tests passed
- `1x32` stayed excellent

`1x32` realistic gate:

- `clean_1`
  - `800/s`
  - subscriber p99 `17 ms`
  - median dead tuples `175.5`
- `pressure_1`
  - `1199/s`
  - subscriber p99 `20 ms`
  - median dead tuples `137.0`
- `recovery_1`
  - `800/s`
  - subscriber p99 `19 ms`
  - median dead tuples `194.5`

But the realistic `4x8` gate failed badly:

- `clean_1`
  - `0/s`
- `pressure_1`
  - `0/s`
- `recovery_1`
  - `0/s`

Operationally, the `4x8` run showed:

- multi-second `COMMIT`s
- blocked `queue_claimer_leases` updates
- stale-heartbeat rescues during the phase transition
- pool timeouts while waiting for claim connections

So this version was also **not keepable**.

The useful learning is narrower than before:

- bounded claim authority is still directionally right
- but the naïve adaptive control loop is not enough
- once the queue enters the hot/recovery regime, the claimer-lease update path
  itself becomes part of the problem

That means the next design should not be “adaptive bounded claimers, but tuned
better.” It should reconsider the control plane itself:

- either a cheaper queue-level claimer authority mechanism
- or an explicit hot-queue mode such as queue striping that reduces
  cross-replica coordination at the source

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
   - the portable harness now has a true `awa-canonical` variant using the
     same `awa-bench` binary, so we can compare canonical and queue storage
     under the same replica matrix instead of relying only on the direct
     single-process benchmark
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
   - apples-to-apples canonical vs queue-storage checkpoints now show the
     remaining bar much more clearly:
     - `1x32`
       - `queue_storage`: `clean_1 692/s`, `pressure_1 1140/s`, `recovery_1 741/s`,
         median dead tuples `130.5 / 92.0 / 109.0`
       - `canonical`: `clean_1 712/s`, `pressure_1 981/s`, `recovery_1 700/s`,
         median dead tuples `43429 / 43940 / 54645`
     - `4x8`
       - `queue_storage`: `clean_1 181/s`, `pressure_1 143/s`, `recovery_1 148/s`,
         subscriber p99 `8962 / 23429 / 53658 ms`, median dead tuples
         `158.5 / 133.0 / 175.0`
       - `canonical`: `clean_1 163/s`, `pressure_1 217/s`, `recovery_1 155/s`,
         subscriber p99 `234 / 292 / 315 ms`, median dead tuples
         `39599.5 / 42828.5 / 55371.0`
   - interpretation:
     - queue storage already gives the intended churn profile under both shapes
     - at `1x32`, queue storage is competitive with canonical and better in
       `pressure` / `recovery` throughput
     - at `4x8`, canonical currently wins on latency and pressure/recovery
       throughput despite its huge dead-tuple cost
   - the remaining blocker is now best understood as multi-process
     claim/start coordination on one queue, not one-big-process underfill alone
   - one later spike reused the receipt-backed live frontier as a local
     prefetch buffer so each replica could claim ahead and dispatch from memory
   - that spike was the first thing that materially improved the realistic
     `4x8` shape:
     - `clean_1 174.8/s`
     - `pressure_1 208.0/s`
     - `recovery_1 161.2/s`
     - subscriber p99 about `140 / 269 / 237 ms`
   - but it was reverted because it crossed the safety/observability boundary:
     - prefetched receipt claims were indistinguishable from active attempts, so
       `running_depth` was inflated badly
     - crash-under-load runs surfaced stale rescue errors:
       - `queue storage ready row missing for deleted lease job ...`
   - the lesson is now stronger than before:
     - a buffered frontier probably **is** required to clear the many-small-
       replica blocker
     - but it cannot safely reuse `open_receipt_claims` as both
       reservation-state and active-attempt state
     - the proper remaining fix is an explicit reserved-but-not-started
       frontier with separate promotion into the active receipt/attempt plane
   - after fixing the multi-replica reporting bug, we ran a fixed bounded-
     claimers pass and learned something more precise:
     - `1x32` stayed healthy:
       - `clean_1 800/s`
       - `pressure_1 1199/s`
       - `recovery_1 800/s`
     - `4x8` no longer looked like `0/s`, but it was still not healthy:
       - `clean_1 778/s`
       - `pressure_1 829/s`
       - `recovery_1 754/s`
       - subscriber p99 `210 / 5583 / 17023 ms`
     - the per-replica summary made the actual failure mode obvious:
       - replica `0` did essentially all the work
       - replicas `1-3` stayed idle
       - backlog and running depth were effectively localized to the incumbent
         claimer
   - so fixed bounded claimers is not keepable as-is:
     - it reduces contention
     - but it over-localizes claim authority to one replica
     - the next bounded-claimers iteration needs queue-global adaptation and
       explicit anti-hoarding rather than a fixed small cap

#### Queue-global adaptive bounded claimers (current experiment)

The next bounded-claimers pass moved from a fixed small cap to a queue-depth
driven target while keeping the same core semantic boundary:

- no per-job reservation/start state
- direct `ready -> active_receipt`
- one claimer slot per instance per queue
- queue-global-ish target derived from shared queue depth / running depth,
  rather than replica-local streaks

The realistic single-producer gate on commit `a7043fb` produced:

- `1x32`
  - `clean_1 797.6/s`
  - `pressure_1 1194.2/s`
  - `recovery_1 799.5/s`
  - subscriber p99 `48 / 53 / 110 ms`
  - median dead tuples `146 / 186 / 146`
- `4x8`
  - `clean_1 701.2/s`
  - `pressure_1 611.4/s`
  - `recovery_1 738.3/s`
  - subscriber p99 `671 / 11420 / 31441 ms`
  - median dead tuples `129 / 152 / 147`

The important improvement over the earlier fixed-cap pass is that the corrected
per-replica raw adapter metrics now show real work spread across multiple
replicas after warmup, rather than replica `0` doing almost everything:

- `clean_1`
  - replica `0`: median nonzero completion rate `334.6/s`
  - replica `1`: `366.6/s`
  - replica `2`: some participation
  - replica `3`: some participation
- `pressure_1`
  - replicas `0..3`: roughly `143–166/s` median nonzero completion rate
- `recovery_1`
  - replicas `0..3`: roughly `167–191/s` median nonzero completion rate

So this version appears to address the worst over-localization problem that
made fixed bounded claimers unattractive.

However, it is not yet a shipping answer because:

- `4x8` subscriber tails are still far too high under `pressure_1` and
  `recovery_1`
- the adaptive controller is still likely too eager or too sticky under hot
  queue conditions
- we have not yet rerun crash-under-load on this version

Current conclusion:

- this is the first bounded-claimers variant worth keeping in the branch as an
  active candidate
- it improves realistic multi-replica throughput materially relative to the
  unbounded baseline
- but it still needs controller tuning before it clears the shipping bar

#### First investigation of the adaptive claimer result

Before discarding the adaptive claimer direction, the first follow-up check was
to look at the corrected per-replica adapter metrics from the realistic `4x8`
run rather than only the aggregate phase summary.

That investigation changes the diagnosis:

- this version does **not** collapse to `0/s`
- it does **not** over-localize to one replica the way the fixed-cap pass did
- under `pressure_1` and `recovery_1`, all four replicas show sustained nonzero
  completion rates

From `custom-20260423T123737Z-b7c2f9`:

- `clean_1`
  - replica `0` median nonzero completion rate `334.6/s`
  - replica `1` `366.6/s`
  - replicas `2` and `3` participate intermittently
- `pressure_1`
  - replicas `0..3` roughly `143–166/s` median nonzero completion rate
- `recovery_1`
  - replicas `0..3` roughly `167–191/s` median nonzero completion rate

So the remaining problem is not “only one replica is active.” The remaining
problem is:

- throughput under `4x8` is still below producer rate
- queue depth builds materially:
  - `clean_1` median queue depth `408`
  - `pressure_1` median queue depth `11066`
  - `recovery_1` median queue depth `24212`
- that backlog then drives very poor subscriber/end-to-end tails

One caveat from this investigation:

- `running_depth` in the current portable harness is still effectively an
  observer-instance metric, not a true cluster-wide aggregate, so it should not
  be over-interpreted in this multi-replica analysis

Working interpretation after this first investigation:

- the adaptive claimer idea is still alive
- the current queue-depth controller is likely too conservative or too
  expensive, so the queue accumulates backlog early and never regains latency
  control
- the next tuning pass should focus on the controller and claimer-lease
  overhead rather than discarding the design immediately

#### Adaptive bounded claimers after controller tuning

The next tuning pass moved the target calculation off the hot claim round:

- added a cheap shared `queue_claimer_state`
- refreshed the queue-wide claimer target at most every `500ms`
- used more aggressive expansion and slower contraction heuristics
- added jittered claimer slot probe order

This reduced the amount of `queue_counts_cached(...)` work paid directly on
every claim attempt and made the controller materially less replica-local.

Results on commit `84483bc`:

- `1x32`
  - `clean_1 800.0/s`
  - `pressure_1 1193.9/s`
  - `recovery_1 799.9/s`
  - subscriber p99 `88 / 114 / 206 ms`
  - median queue depth `20 / 0 / 0`
  - median dead tuples `128 / 164 / 153`
- `4x8`
  - `clean_1 693.9/s`
  - `pressure_1 727.4/s`
  - `recovery_1 742.6/s`
  - subscriber p99 `1727 / 7090 / 25149 ms`
  - median queue depth `375 / 7489 / 18639`
  - median dead tuples `155 / 185 / 181`

Compared with the first adaptive claimer run (`a7043fb`), the tuned controller
shows:

- `1x32`
  - still healthy overall, though with somewhat worse tails
- `4x8`
  - slightly lower `clean_1` throughput
  - materially better `pressure_1` throughput (`611 -> 727/s`)
  - slightly better `recovery_1` throughput (`738 -> 743/s`)
  - much lower `pressure_1` and `recovery_1` queue depth
  - substantially better `pressure_1` and `recovery_1` subscriber tails

Per-replica completion medians remain distributed across all four replicas
after warmup:

- `clean_1`
  - replicas `0..3`: `136–197/s`
- `pressure_1`
  - replicas `0..3`: `173–194/s`
- `recovery_1`
  - replicas `0..3`: `179–193/s`

So this tuning pass does not solve the realistic `4x8` tail problem yet, but
it is the first adaptive claimer version that is both:

- clearly multi-replica, not over-localized to one owner
- and measurably better than the earlier adaptive controller in the hot
  backlog phases

Current conclusion:

- keep this version in the branch as the active bounded-claimers candidate
- next work should focus on further controller tuning and crash/recovery
  validation, not discarding the design

#### Crash-under-load on the tuned controller

The tuned adaptive controller was also exercised through the existing
`crash_recovery_under_load` semantics scenario at `2x8`.

That run completed, but it is not operationally clean enough to count as a
shipping answer yet.

Observed behavior:

- repeated maintenance errors during restart/recovery:
  - `queue storage ready row missing for deleted lease job ...`
- completion continued, but backlog exploded during pressure/recovery
- the scenario spent long periods with very poor delivery latency

Recorded medians from `awa_semantics_crash_recovery_under_load_20260423_130433`:

- `baseline`
  - completion `528.2/s`
  - end-to-end p99 `14627 ms`
  - queue depth `22656.5`
- `pressure_1`
  - completion `372.4/s`
  - end-to-end p99 `46711 ms`
  - queue depth `130063`
- `restart`
  - completion `328.0/s`
  - queue depth `263199`
- `recovery_1`
  - completion `334.8/s`
  - queue depth `344323.5`

So the tuned adaptive claimer controller remains:

- viable enough to keep in-branch for further study
- not yet safe/boring enough to call production-ready

Most importantly, this means the remaining blocker is no longer just a `4x8`
throughput/tail issue. There is still at least one recovery-path correctness or
control-plane interaction issue to resolve before the design can be considered
ready.

Because the tuned adaptive claimer controller is now good enough to keep in the
branch, but still not boring enough to ship on hot queues, queue striping has
been promoted from "future escape hatch" to the next serious design track. See
[`queue-striping-plan.md`](queue-striping-plan.md) for the proposed logical
queue -> physical stripe model and the measurement plan against the current
`4x8` blocker.

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
