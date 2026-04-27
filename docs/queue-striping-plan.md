# Queue Striping

> **Status: shipped as an optional 0.6 queue-storage mode.** The release
> shape is static striping via `QueueStorageConfig::queue_stripe_count`
> (default `1`, disabled). Physical stripe queues use the internal
> `queue#N` naming convention; users still configure and observe the
> logical queue. The dynamic striping controller experiments below were
> not kept for 0.6.

## Release shape

Queue striping is now a queue-storage tuning knob, not a separate engine.

- Default: `queue_stripe_count = 1`, preserving the unstriped path.
- Hot-queue candidate: `queue_stripe_count = 2` based on the best measured
  `4x8` results in this investigation.
- Higher stripe counts are not generally recommended from the current data:
  `4` helped some pressure phases but hurt other shapes, and `8` regressed.
- Enqueue chooses one physical stripe deterministically. Unique-keyed jobs hash
  by `unique_key`; otherwise the insert sequence/salt spreads work.
- Claim probes the physical stripes cyclically from a per-runtime round-robin
  hint.
- Stats aggregate back to the logical queue; stripe names are an internal
  diagnostic surface.
- Maintenance rotates/prunes the existing queue-storage table families; striping
  does not add a new job lifecycle state.

The implementation lives in `awa-model/src/queue_storage.rs` (search for
`queue_stripe_count`). Operator-facing configuration is documented in
[`configuration.md`](configuration.md#queue-storage-tuning), and the runtime
architecture is summarized in [`architecture.md`](architecture.md#queue-striping-and-claim-authority).

## Why this exists

The `0.6` queue-storage engine is now in a good place for:

- transactional enqueue
- no lost work under failure
- honest at-least-once semantics
- low hot-path dead tuples
- direct short-job starts
- single large runtime shapes like `1x32`

The remaining blocker is the realistic many-small-replica hot-queue shape:

- `2x16`
- `4x8`
- `8x4`

on **one hot logical queue**.

We have now tried and measured several alternatives for that shape:

- unsafe receipt-buffer prefetch
- explicit reserve/promote frontiers
- stateful receipt frontiers
- striped claim heads
- sticky shard leasing v1/v2
- fixed bounded claimers
- adaptive bounded claimers

Those experiments established a few priors:

1. Adding another effective hot-path start transaction loses.
2. Letting every replica contend on the same hot queue loses.
3. Reusing active-attempt state as pre-start buffering is unsafe.
4. `1x32` is already healthy, so the core short-job path can work.
5. The remaining problem is concentrated in **many-small-replica coordination on one hot queue**.
6. Queue-storage is already dramatically cleaner than canonical in dead-tuple terms, so the next design should not reopen the queue-plane MVCC problem.

That means the next serious candidate should reduce **single-queue coordination pressure at the source**, not add another per-job state machine.

Queue striping is the clearest way to do that.

## Design summary

Introduce **logical queues** backed by a fixed number of **physical stripes**.

Example:

```text
logical queue: email

physical stripes:
  email#0
  email#1
  email#2
  email#3
```

Workers continue to subscribe to one logical queue, but internally:

- enqueues land on one stripe
- claims probe multiple stripes
- observability aggregates stripes back to one logical queue

This does **not** add a new per-job execution state.
Jobs still use the current lifecycle:

```text
ready -> active_receipt -> attempt_state -> active_lease
```

The only change is that one hot logical queue no longer funnels every claimer through one coordination path.

## Historical Design Goals

Queue striping should:

- materially improve `4x8` and `8x4` hot-queue behavior
- preserve the current good `1x32` shape
- avoid making hot-path churn materially worse than the current unstriped
  short-job baseline once `open_receipt_claims` is measured explicitly
- preserve current short-job and long-running job semantics
- preserve honest `running_depth`
- avoid a second start transaction
- remain compatible with bounded claimers as a later or optional enhancement

## Historical Non-goals

Queue striping should **not**:

- claim “exactly once”
- introduce a per-job reservation state
- require users to think in terms of physical subqueues unless we later decide to expose it
- require a queue leader or fragile queue-local owner
- degrade the calm/default single-queue path unnecessarily

## Why striping now

This is moving up from “future idea” to “real track” because:

- bounded claimers are promising, but not yet boring under `4x8` and crash/recovery
- the remaining tail problem is now a **single-hot-queue coordination** problem, not a storage-churn problem
- striping is the cleanest way to reduce that coordination surface without changing the attempt lifecycle

This should be treated as:

- an implementation/proof step first
- a product-shape decision later
- an **experimental / investigation-only** engine mode until the churn and
  counting regressions are understood

So the first implementation can still use an explicit stripe count internally.
Whether striping becomes automatic or always-on can be decided after measurement.

## Logical queue -> physical stripes

For a logical queue `Q` and stripe count `N`, define physical queues:

```text
Q#0 .. Q#(N-1)
```

The logical queue remains the public queue name.

Internally:

- enqueue chooses exactly one stripe
- claim scans stripes
- queue stats aggregate stripes
- maintenance rotates/prunes each stripe independently using the existing storage machinery

## Stripe selection on enqueue

First version should use deterministic stripe assignment:

```text
stripe = hash(stripe_key) % stripe_count
```

Suggested `stripe_key` priority:

1. `unique_key` if present
2. tenant/account key if later exposed
3. job id / generated random salt otherwise

Why:

- if `unique_key` exists, putting duplicate candidates on the same stripe is useful
- hashing random jobs spreads hot queues naturally

The first implementation should keep this simple and internal.

## Claim semantics

Workers subscribe to the logical queue, but claim from stripes.

Claim still goes directly:

```text
ready -> active_receipt
```

No reserve/promote step is added.

### Stripe probing policy

The first version should use:

- round-robin starting stripe with memory
- then probe remaining stripes if the first stripe is empty

That means each dispatcher remembers:

- `next_stripe_hint`

On each wake:

1. start from `next_stripe_hint`
2. try stripes in cyclic order
3. claim from the first stripe that yields work
4. advance `next_stripe_hint`

This keeps the hot path simple while avoiding global contention on one queue head.

### Why not fixed worker-to-stripe ownership first

We already know that over-sticky ownership can underutilize capacity.

The first striping pass should reduce coordination pressure without also
introducing another ownership control plane.

So v1 should prefer:

- simple deterministic enqueue
- simple cyclic stripe probing
- no explicit stripe lease state

If needed later, striping can still be combined with bounded claimers or stripe-local ownership.

## Fairness over time

Striping weakens perfect global ordering in exchange for lower contention.

That is acceptable only if fairness over time remains strong.

### Within a stripe

- preserve current priority ordering
- preserve claim-time effective priority aging

### Across stripes

- fairness is approximate, not globally optimal on every claim
- no stripe should monopolize all attention forever
- every stripe should be probed over time

The round-robin start hint gives a simple first approximation of this.

### Across priorities

Priorities still apply within stripes.
Global fairness is approximate, but claim-time aging should still guarantee eventual progress for low-priority work.

## Operational model

The first implementation treated striping as an internal engine mode for
experimentation. In the release shape, static striping remains optional and
off by default.

That means:

- a queue can have a `stripe_count`
- metrics and admin views still show one logical queue
- stripe breakdown is available for debugging/benchmarking

The investigated product shapes were:

- always-on striping
- automatic striping for hot queues
- or a visible queue-level policy

Measurement selected static optional striping for 0.6; the dynamic controller
experiment below was not kept.

## Observability

We should expose both:

- logical queue totals
- per-stripe internals for diagnosis

Important metrics:

- logical queue depth
- per-stripe queue depth
- logical completion / enqueue rate
- per-stripe claim rate
- skew ratio:
  - busiest stripe depth / median stripe depth
- dead tuples by stripe table family
- per-replica completion distribution

## Safety invariants

1. Enqueue chooses exactly one stripe per job.
2. A ready job exists in exactly one physical stripe.
3. Claiming a striped queue still starts the current active-attempt lifecycle directly.
4. Crash before claim loses no work because the job is still ready on its stripe.
5. Crash after claim uses the existing receipt / attempt / lease rescue path.
6. Queue striping must not change the at-least-once contract.
7. Queue striping must not reintroduce a new hot mutable jobs table.

## Historical Measurement Plan

This should be judged against the current `4x8` blocker.

### Primary gate

Single producer only:

- `PRODUCER_ONLY_INSTANCE_ZERO=1`

Shapes:

- `1x32`
- `2x16`
- `4x8`
- `8x4`

Phases:

- `warmup=warmup:15s`
- `clean_1=clean:45s`
- `pressure_1=high-load:45s`
- `recovery_1=clean:45s`

Metrics:

- throughput
- subscriber p99
- end-to-end p99
- logical queue depth
- per-stripe queue depth
- per-replica completion distribution
- dead tuples

### Secondary gate

- crash-under-load
- retry/priority semantics

### Success thresholds

We keep striping only if:

- `1x32` stays roughly where the current good branch is
- `4x8` materially improves over the current unstriped adaptive claimer result
- `4x8` tails are much lower than the current unstriped adaptive claimer result
- dead tuples do not materially exceed the current unstriped short-job baseline
  once `open_receipt_claims` is included in the sampled event tables
- logical `running_depth` remains plausible for the offered load and queue
  depth, rather than inflating into thousands of apparently-running jobs
- crash-under-load remains correct

## Comparison targets

The first striped pass should be compared to:

1. current queue-storage baseline
2. tuned adaptive bounded claimers
3. `awa-canonical`

Especially at:

- `1x32`
- `4x8`

## Historical Implementation Phases

### Phase 1: queue identity and enqueue

1. add stripe count to queue-storage config
2. derive physical stripe queue names internally
3. route enqueue into one stripe deterministically

### Phase 2: claim path

1. add logical-queue stripe probing
2. keep direct `ready -> active_receipt`
3. implement round-robin stripe hinting

### Phase 3: stats and observability

1. aggregate per-stripe counts back to logical queue
2. expose per-stripe breakdown for debugging

### Phase 4: measurement

1. rerun realistic gate
2. rerun crash-under-load
3. compare against current adaptive bounded claimers

## Priors going in

What we expect, based on the experiments so far:

- `1x32` may stay about the same or regress slightly
- `4x8` should benefit substantially because the hot queue is no longer one shared coordination point
- dead tuples should stay in roughly the same range as the current unstriped
  short-job path, because striping changes the coordination shape, not the
  receipt-claim lifecycle itself
- fairness should remain good enough if stripes are probed cyclically and priority aging remains active

The biggest risk is:

- stripe skew causing one stripe to stay disproportionately hot

The first version should measure that explicitly rather than trying to solve it prematurely.


## First experiment results

The first implementation pass was built and measured with `QUEUE_STRIPE_COUNT=4` on the
same single-producer realistic gate used for the bounded-claimer work.

### `1x32`

- `clean_1`: throughput about `883/s`, subscriber p99 about `250 ms`
- `pressure_1`: throughput about `1048/s`, subscriber p99 about `226 ms`
- `recovery_1`: throughput about `744/s`, subscriber p99 about `154 ms`

### `4x8`

- `clean_1`: throughput about `833/s`, subscriber p99 about `456 ms`
- `pressure_1`: throughput about `934/s`, subscriber p99 about `670 ms`
- `recovery_1`: throughput about `827/s`, subscriber p99 about `686 ms`

### What looked promising

- `4x8` throughput is materially better than the current adaptive bounded-claimer baseline.
- work is distributed across multiple replicas instead of collapsing to one claimer.
- the hot-queue throughput problem is plausibly reduced by striping.

### Investigation result: the first regression signal was misleading

The first write-up treated striping as if it had caused a new dead-tuple and
counting regression. A focused root-cause pass showed that conclusion was too
strong.

What was actually happening:

- the portable benchmark was **not sampling `open_receipt_claims`**, which is
  the real short-job churn hotspot under sustained load
- the observer was using a hard `15s` queue-count cache TTL against `10–15s`
  phases, which overstated logical `running_depth`

After fixing the benchmark:

- `open_receipt_claims` is included in the sampled event tables
- queue-count sampling is tied to the sample interval by default (and can be
  overridden via `QUEUE_COUNT_MAX_AGE_MS`)

On a fresh manual `4x8` striped repro with phase transitions (`800 -> 1200 ->
800`), the live database state was:

- `open_receipt_claims` carried the dead tuples (`n_dead_tup ~= 11611`)
- `lease_claim_closures` had `0` dead tuples
- `done_entries_*` partitions had `0` dead tuples
- exact queue-count snapshots remained plausible

And a matched unstriped repro showed essentially the same shape:

- `open_receipt_claims` was also the hot dead-tuple source
- `lease_claim_closures` and `done_entries_*` still had `0` dead tuples

So the corrected conclusion is:

- striping did **not** introduce a special `lease_claim_closures` or
  `done_entries_*` churn regression
- the dead-tuple hotspot is the existing `open_receipt_claims` insert/delete
  cycle
- striping should now be judged on whether it improves realistic `4x8`
  throughput/tails **without making that existing hotspot materially worse**

### Corrected comparison

With the corrected benchmark path, a short `4x8` single-producer comparison
looks like this:

Striped (`QUEUE_STRIPE_COUNT=4`)
- `clean_1`: throughput about `798/s`, subscriber p99 about `32 ms`
- `pressure_1`: throughput about `1198/s`, subscriber p99 about `34 ms`
- `recovery_1`: throughput about `799/s`, subscriber p99 about `36 ms`
- median dead tuples about `8817 / 3646 / 14033`

Unstriped (`QUEUE_STRIPE_COUNT=1`)
- `clean_1`: throughput about `793/s`, subscriber p99 about `21 ms`
- `pressure_1`: throughput about `1183/s`, subscriber p99 about `24 ms`
- `recovery_1`: throughput about `786/s`, subscriber p99 about `33 ms`
- median dead tuples about `8771 / 3683 / 13838`

So the first striping pass is now best understood as:

- **not** a clear regression
- **not yet** a clear win
- still an investigation-only implementation until it shows a stronger
  `4x8` benefit than the unstriped baseline

### Stripe-count sweep

With the corrected benchmark path in place, the next question was whether a
different stripe count could produce a real `4x8` win.

Short `4x8` sweep (`15s` warmup, `30s` phases):

- `QUEUE_STRIPE_COUNT=1`
  - `clean_1`: throughput about `762/s`, subscriber p99 about `336 ms`
  - `pressure_1`: throughput about `887/s`, subscriber p99 about `2487 ms`
  - `recovery_1`: throughput about `909/s`, subscriber p99 about `6029 ms`
- `QUEUE_STRIPE_COUNT=2`
  - `clean_1`: throughput about `796/s`, subscriber p99 about `166 ms`
  - `pressure_1`: throughput about `1054/s`, subscriber p99 about `378 ms`
  - `recovery_1`: throughput about `726/s`, subscriber p99 about `425 ms`
- `QUEUE_STRIPE_COUNT=4`
  - `clean_1`: throughput about `762/s`, subscriber p99 about `142 ms`
  - `pressure_1`: throughput about `1092/s`, subscriber p99 about `439 ms`
  - `recovery_1`: throughput about `761/s`, subscriber p99 about `713 ms`
- `QUEUE_STRIPE_COUNT=8`
  - `clean_1`: throughput about `729/s`, subscriber p99 about `562 ms`
  - `pressure_1`: throughput about `957/s`, subscriber p99 about `558 ms`
  - `recovery_1`: throughput about `627/s`, subscriber p99 about `1729 ms`

What this suggests:

- `8` stripes is too many for this workload shape
- `4` stripes helps the hot pressure phase but hurts `1x32` badly
- `2` stripes is the first count that looks plausibly worth keeping

The hot dead-tuple source remains unchanged across the sweep:

- `open_receipt_claims` is still the dominant dead-tuple table
- striping is changing queueing behavior, not introducing a new closure/done
  regression

### Longer `4x8` confirmation: `1` vs `2` stripes

To reduce the noise in the short sweep, we then reran a longer pair:

- `30s` warmup
- `60s` `clean_1`
- `60s` `pressure_1`
- `60s` `recovery_1`

Results:

Unstriped (`QUEUE_STRIPE_COUNT=1`)
- `clean_1`: throughput about `446/s`, subscriber p99 about `7823 ms`,
  queue depth about `5537`
- `pressure_1`: throughput about `465/s`, subscriber p99 about `33579 ms`,
  queue depth about `34286`
- `recovery_1`: throughput about `520/s`, subscriber p99 about `64455 ms`,
  queue depth about `59008`

Two stripes (`QUEUE_STRIPE_COUNT=2`)
- `clean_1`: throughput about `667/s`, subscriber p99 about `1728 ms`,
  queue depth about `1070`
- `pressure_1`: throughput about `553/s`, subscriber p99 about `15630 ms`,
  queue depth about `14150`
- `recovery_1`: throughput about `762/s`, subscriber p99 about `36340 ms`,
  queue depth about `26252`

This longer pair is the strongest evidence so far that striping can produce a
real `4x8` win:

- lower backlog growth
- materially lower tails
- better clean and recovery throughput

Measurement caveat:

- the portable `awa-bench` adapter currently reports `claim_p99_ms` using the
  same windowed value as `subscriber_p99_ms`
- so `claim_p99_ms` should not be used as an independent diagnosis signal for
  striping until the adapter emits a true claim-latency metric

But it is still not yet “boring” enough to call done:

- tails remain high in absolute terms
- `open_receipt_claims` churn remains the limiting table family
- the best candidate so far is specifically `QUEUE_STRIPE_COUNT=2`, not
  striping in general

### Dynamic striping controller experiment

We then tested the first dynamic controller for striping:

- fixed stripe universe from `QUEUE_STRIPE_COUNT=4`
- queue-global `queue_stripe_state`
- monotonic active-stripe expansion
- initial target `1`, widening toward `2`
- enqueue and claim both limited to the currently active stripe set

The intent was to preserve the proven `2`-stripe hot-queue win without paying
the steady-state cost of always-on striping.

That implementation was benchmarked twice:

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

The controller is therefore **not** the current answer:

- both versions regressed relative to the static `2`-stripe candidate
- the retuned version regressed relative to the initial dynamic version
- the failure is not a special new dead-tuple hotspot; `open_receipt_claims`
  remains the dominant table family
- the problem is the dynamic control path itself, not the basic striping idea

Current conclusion:

- keep static `2` stripes as the leading striping candidate
- do **not** keep the current dynamic-striping controller
- if dynamic striping is revisited, it needs a much cheaper control model than
  “queue-global state consulted on both enqueue and claim”
