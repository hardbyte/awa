# Queue Striping Plan

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

## Goals

Queue striping should:

- materially improve `4x8` and `8x4` hot-queue behavior
- preserve the current good `1x32` shape
- keep dead tuples in the current low-hundreds regime
- preserve current short-job and long-running job semantics
- preserve honest `running_depth`
- avoid a second start transaction
- remain compatible with bounded claimers as a later or optional enhancement

## Non-goals

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

The first implementation should treat striping as an internal engine mode for experimentation.

That means:

- a queue can have a `stripe_count`
- metrics and admin views still show one logical queue
- stripe breakdown is available for debugging/benchmarking

Later we can decide whether the final product shape is:

- always-on striping
- automatic striping for hot queues
- or a visible queue-level policy

That decision should come **after** measurement.

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

## Measurement plan

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
- dead tuples remain in the current low-hundreds regime
- crash-under-load remains correct

## Comparison targets

The first striped pass should be compared to:

1. current queue-storage baseline
2. tuned adaptive bounded claimers
3. `awa-canonical`

Especially at:

- `1x32`
- `4x8`

## Implementation phases

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
- dead tuples should remain low because striping changes the coordination shape, not the per-job lifecycle
- fairness should remain good enough if stripes are probed cyclically and priority aging remains active

The biggest risk is:

- stripe skew causing one stripe to stay disproportionately hot

The first version should measure that explicitly rather than trying to solve it prematurely.
