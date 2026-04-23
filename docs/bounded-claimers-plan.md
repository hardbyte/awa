# Bounded Claimers Per Queue Plan

## Why this exists

The queue-storage engine is now in a good place for:

- transactional enqueue
- no lost work under failure
- honest at-least-once semantics
- low hot-path dead tuples
- single large runtime shapes like `1x32`

The remaining blocker for `0.6` is the realistic many-small-replica shape on
one hot queue:

- `2x16`
- `4x8`
- `8x4`

The branch has already tried and rejected several ideas for that shape:

- unsafe receipt-buffer prefetch
- explicit reserve/promote frontiers
- striped claim heads
- stateful receipt frontiers
- sticky shard leasing v1/v2

The common failure mode is now clear:

- any design that adds a second effective hot-path start transaction loses
- any design that lets every replica contend on the same hot queue loses
- any design that lies about `running_depth` or starts rescue semantics early
  is unacceptable even if it benchmarks well

So the next proposal is to reduce many-small-replica contention by limiting
**how many replicas are allowed to claim from a queue at once**, while keeping
job lifecycle semantics unchanged.

## Design summary

Introduce a small, bounded set of **active claimers** per queue.

At any point in time, a queue has:

- a small set of replicas allowed to claim new work
- any number of replicas allowed to execute already-claimed work

This is not a fragile leader model:

- a queue can have more than one active claimer
- claim ownership is time-bounded
- expiry and takeover are automatic
- no queue-level owner ever holds jobs hostage

The core idea is:

- batch and bound **claim authority**
- do **not** introduce another per-job reservation state
- keep short-job starts direct:
  - `ready -> active_receipt`

This is **not** static partitioning of jobs across claimers.

If a queue currently allows `2` or `4` active claimers:

- those replicas are the only ones allowed to hit the hot claim path
- they still claim from the same logical queue
- they do **not** each receive a fixed `1/N` share of jobs

So the design is about bounding and rotating **claim authority**, not dividing
jobs into quotas.

That preserves:

- honest `running_depth`
- no early rescue clock
- no second promotion transaction

## Design goals

This design should:

- materially improve `4x8` and `8x4` on one hot queue
- keep `1x32` roughly where it is now
- preserve current short-job and long-running job semantics
- keep hot-path dead tuples low
- prefer strong fairness over time, not globally optimal fairness on every
  single claim

## Non-goals

This design should **not**:

- introduce a per-job `reserved-but-not-started` state
- add a second hot-path transaction between claim and actual worker start
- make one replica the only claimer for the whole system
- claim “exactly once”
- trade safety for microbenchmark wins

## High-level model

Separate:

1. **job lifecycle**
2. **queue claimer lifecycle**

Job lifecycle remains as-is:

```text
ready -> active_receipt -> attempt_state -> active_lease
```

Queue claimer lifecycle is new:

```text
inactive -> active_claimer -> idle_claimer -> expired
```

The key point:

- claimers coordinate access to `claim_ready_runtime(...)`
- jobs are still started directly into the existing active attempt plane

## Queue claimer state machine

### States

```text
[inactive]
   |
   | acquire claimer slot
   v
[active-claimer(instance, slot, epoch, expires_at)]
   |
   | no successful claims for idle_threshold
   v
[idle-claimer(instance, slot, epoch, expires_at)]
   |
   | successful claim by owner
   v
[active-claimer(instance, slot, epoch, expires_at')]
   |
   | expiry / relinquish / steal when idle
   v
[inactive]
```

Derived state:

- `inactive`
  - row absent or `expires_at <= now()`
- `active-claimer`
  - `expires_at > now()`
  - `last_claimed_at > now() - idle_threshold`
- `idle-claimer`
  - `expires_at > now()`
  - `last_claimed_at <= now() - idle_threshold`

### Properties

- at most one live owner per `(queue, claimer_slot)`
- ownership is time-bounded
- ownership controls claim authority, not job state
- stale owners lose claim authority when epoch changes or the slot expires

## Suggested schema

### 1. Queue claimer leases

```sql
CREATE TABLE {schema}.queue_claimer_leases (
    queue TEXT NOT NULL,
    claimer_slot SMALLINT NOT NULL,
    owner_instance_id UUID NOT NULL,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    leased_at TIMESTAMPTZ NOT NULL,
    last_claimed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue, claimer_slot)
);
```

Recommended index:

```sql
CREATE INDEX ON {schema}.queue_claimer_leases (queue, owner_instance_id, expires_at);
```

### 2. Optional queue claimer policy table

This can remain derived from config initially, but if we want runtime override:

```sql
CREATE TABLE {schema}.queue_claimer_policy (
    queue TEXT PRIMARY KEY,
    max_claimers SMALLINT NOT NULL,
    idle_threshold_ms INT NOT NULL,
    lease_ttl_ms INT NOT NULL
);
```

I would not put this on the hot path initially. Use config first.

## Claim algorithm

### Fast path

1. Dispatcher wakes with free local workers.
2. If this runtime already owns one or more active claimer slots for the queue:
   - claim directly using the existing claim path
3. If it owns none:
   - try to acquire an unowned / expired slot
   - or steal an idle slot
4. If it cannot become a claimer:
   - it stays executor-only for that queue until a later wake

### Acquire claimer slot

On demand, the runtime tries to claim one of a small fixed set of slots:

```text
slot_count = max_claimers_per_queue
```

Acquire succeeds if:

- slot absent
- or slot expired
- or slot already owned by self

Update:

- `owner_instance_id = self`
- `lease_epoch = lease_epoch + 1` when taking over
- `leased_at = now()`
- `last_claimed_at = now()`
- `expires_at = now() + lease_ttl`

### Renew

Renew **only on successful claim** from that queue.

Do not renew:

- merely because the dispatcher woke
- merely because the process is alive

That keeps active claimers sticky, but lets unused claim authority decay.

### Idle steal

If a runtime has free capacity and no owned claimer slot:

1. take unowned slots
2. reclaim expired slots
3. steal `idle-claimer` slots

Never steal `active-claimer` slots in the first version.

Steal means:

- replace `owner_instance_id`
- increment `lease_epoch`
- reset `leased_at`, `last_claimed_at`, `expires_at`

### Claiming once active

Once a runtime owns an active claimer slot:

- it uses the existing `claim_ready_runtime(...)`
- jobs still go directly to `active_receipt`
- no intermediate reservation or start token is created

This is the whole point:

- reduce claim contention
- do not add per-job hot-path state transitions

## Job state machine

Unchanged:

```text
ready -> active_receipt -> attempt_state -> active_lease
```

with exits to:

- completed
- deferred/retry
- waiting
- dlq

This is intentional. Bounded claimers change queue-level claim authority, not
job execution semantics.

## Fairness model

This design is explicitly about **strong fairness over time**.

### Within one claimer

- preserve current priority ordering
- preserve current effective priority aging

### Across claimers

- fairness is approximate, not globally optimal on every single claim
- bounded skew is acceptable if:
  - work does not starve
  - aging continues to pull long-waiting lower-priority jobs forward

### Across replicas

- active claimers get locality and throughput
- idle replicas take over expired or idle claimer slots
- fairness happens by:
  - expiry
  - idle steal
  - bounded active claimer count

This is the intended trade:

- locality first
- rebalance second
- fairness over time

## Adaptive bounded-claimers v2

The fixed-cap experiment established that the claimer idea itself is useful,
but a hard cap is too blunt:

- it helps calm phases
- it preserves the good `1x32` shape
- but it can starve a hot queue during `pressure_1` and especially
  `recovery_1`

So the next real design is **queue-global adaptive bounded claimers with
anti-hoarding**, not fixed bounded claimers and not per-replica local
adaptation.

That means:

- the queue has one shared active-claimer target
- replicas do not each privately decide their own target
- one replica should not be allowed to monopolize all claimer slots while
  others are healthy and interested

### Controller states

The queue-level controller should be modeled explicitly:

```text
[calm]
   |
   | sustained backlog + underfilled running depth
   v
[expanding]
   |
   | active claimer target reached
   v
[steady-hot]
   |
   | backlog drains / saturation clears
   v
[contracting]
   |
   | extra claimers released or allowed to expire
   v
[calm]
```

This is still not a new job state machine. It is only a queue-level control
loop over how many replicas are allowed to claim from that queue.

### Queue-global target

For each queue, maintain one shared target:

- `current_target_claimers`

Example:

```text
queue = email

calm            -> target = 1
backlog grows   -> target = 2
steady hot      -> target = 4
backlog drains  -> target = 1
```

That target should be derived from queue-level signals, not one replica's
private claim streak.

### Anti-hoarding

The fixed-cap experiment showed that throughput can look healthy while one
replica still does essentially all the useful claiming/completing work.

So the next version needs an explicit anti-hoarding rule:

- until the queue reaches its current target, prefer assigning claimer slots to
  replicas that do not already own one for that queue
- in the first adaptive version, cap ownership at **one claimer slot per
  instance per queue**

This is still not a `1/N` job split. It only limits who may claim.

### Control variables

The queue should maintain:

- `min_claimers_per_queue`
- `max_claimers_per_queue`
- `current_target_claimers`
- `scale_up_window`
- `scale_down_window`
- `idle_threshold`
- `lease_ttl`

The first version should keep `min_claimers_per_queue = 1` and `max_claimers`
small, likely `4`.

### Inputs to the controller

Use only signals we already trust and can export:

- queue depth
- running depth
- total configured worker capacity for that queue across the cluster
- local free permits
- empty-claim rate
- unused-permit rate
- claim batch size
- claim success rate
- optionally queue lag

### Expansion heuristic

Expand the claimer target when all of these hold over a short rolling window:

- queue depth is above a threshold
- running depth is materially below total configured worker capacity
- claimers are making successful claims
- backlog is not shrinking fast enough

Interpretation:

- there is genuine runnable work
- executors are underfilled
- claim coordination, not lack of jobs, is the limiting factor

### Contraction heuristic

Contract the claimer target when, over a longer window:

- queue depth stays near zero
- running depth is low
- claim success rate drops
- or extra claimers are mostly idle

This keeps the default calm path local and cheap.

### Why this is the right control loop

It matches the principle we agreed on:

- strong fairness over time
- locality when calm
- more coordination only when saturation proves it is necessary

### Example timeline

```text
Queue "email", replicas A B C D

calm:
  target = 1
  slot0 -> A
  B/C/D executor-only

backlog grows while running depth < total worker capacity:
  target = 2
  slot1 -> B

queue remains hot:
  target = 4
  slot2 -> C
  slot3 -> D

backlog drains:
  target = 1
  B/C/D stop renewing and their slots expire
  A remains claimer
```

That is the intended behavior:

- locality while calm
- broader claiming only under sustained saturation
- contraction back to the cheap path once the queue settles

## Safety invariants

These are the non-negotiables.

1. At most one live owner for `(queue, claimer_slot)`.
2. Claimer ownership does not imply job ownership.
3. Unclaimed jobs remain ready until directly claimed.
4. Only active attempts count as running.
5. Crash before claim:
   - loses only claim authority
   - no jobs are lost
6. Crash after claim:
   - existing receipt / attempt / lease rescue handles it
7. Stale claimers cannot renew or continue owning once epoch changes.
8. Idle claimers cannot block progress indefinitely because idle slots are
   stealable.
9. Changing the active claimer target changes only claim authority, not job
   state.
10. Contraction is safe because allowing a claimer lease to expire does not
    revoke already-claimed jobs.

## Measurement plan

This should be judged with the realistic gate, not the direct burst benchmark.

### Primary gate

Single producer only:

- `PRODUCER_ONLY_INSTANCE_ZERO=1`

Replica shapes:

- `1x32`
- `2x16`
- `4x8`
- `8x4`

Phases:

- `warmup=warmup:30s`
- `clean_1=clean:60s`
- `pressure_1=high-load:60s`
- `recovery_1=clean:60s`

Metrics to record:

- throughput
- subscriber p99
- end-to-end p99
- queue depth
- running depth
- dead tuples
- dispatcher wake reasons
- empty claims
- unused permits
- per-replica throughput / latency after warmup
- claimer metrics:
  - active claimer count
  - claimer acquire success/miss
  - claimer renews
  - claimer steals
  - claimer expiries

### Secondary gate

- crash-under-load scenario
- retry/priority semantics scenario

### Robust build/test plan

Do not judge the adaptive version on one lucky run.

The minimum confidence plan should be:

1. **Build + targeted runtime tests**
   - queue claimer acquisition
   - idle/expired takeover
   - adaptive scale-up trigger
   - adaptive scale-down / contraction
   - stale claimer epoch rejection
2. **Three repeated realistic gates**
   - `1x32`
   - `4x8`
   - `8x4`
   - same single-producer workload each time
3. **Crash-under-load**
   - at least one replica kill during `pressure_1`
   - ensure `recovery_1` does not stall
4. **Longer recovery hold**
   - keep `recovery_1` long enough to observe contraction after backlog drains
5. **Canonical comparison**
   - rerun `awa-canonical` in `1x32` and `4x8`
6. **Dead-tuple check**
   - confirm we remain in the current low-hundreds regime
7. **Per-replica distribution**
   - confirm `4x8` does not collapse into one effective claimer after warmup

Only keep it if the *pattern* is stable across repeats.

### Success thresholds

The minimum bar to keep the design:

- `1x32`
  - stays roughly where the current good branch is
- `4x8`
  - `clean_1` materially above current poor shapes
  - `pressure_1` materially above current poor shapes
  - `recovery_1` no longer stalls or collapses
- `8x4`
  - should improve in the same direction, even if it remains worse than `4x8`
- dead tuples remain in the current low-hundreds regime, not canonical-style
  tens of thousands
- `running_depth` remains honest
- crash-under-load remains correct

### Comparison against canonical

Once the queue-storage shape looks good enough, rerun the realistic
single-producer comparison against:

- `awa`
- `awa-canonical`

Especially for:

- `1x32`
- `4x8`

That will tell us whether the new engine is now good enough in the deployment
shape that still blocks shipping confidence.

## Implementation phases

### Phase 1: schema and config

1. add `min_claimers_per_queue`
2. add `max_claimers_per_queue`
3. add adaptive thresholds / windows
4. add `queue_claimer_leases`
5. add metrics fields for queue claimer state

### Phase 2: worker runtime

1. track owned claimer slots per queue
2. renew only on successful claims
3. compute a queue-local target claimer count from observed saturation
4. attempt idle/expired steal only when the active claimer set is below the
   adaptive target
5. let excess claimers expire or relinquish during contraction

### Phase 3: claim path integration

1. gate queue claims on claimer ownership
2. keep existing direct `ready -> active_receipt` path
3. do not add any new per-job pre-start state

### Phase 4: measurement and hardening

1. rerun `1x32 / 2x16 / 4x8 / 8x4`
2. repeat the realistic gate to confirm stable behavior
3. rerun crash-under-load
4. rerun retry/priority semantics
5. compare to canonical in the realistic shape

## Future ideas to keep on the table

These are not the next implementation, but they are worth preserving.

### 1. Queue striping as a hot-queue mode

This is the most important future escape hatch.

For very hot logical queues, support mapping one logical queue to a fixed set of
physical stripes / subqueues.

That would give us:

- a better default engine for normal queues
- an explicit escape hatch for extreme single-queue workloads

This should remain a real option even if bounded claimers work, because some
production workloads will still want an operationally explicit way to spread a
very hot queue across more coordination points.

### 2. Bounded active claimers plus striping

These ideas are complementary, not exclusive.

If one queue remains extremely hot:

- bounded claimers reduce cross-replica contention
- queue striping reduces single-queue coordination pressure further

### 3. More approximate fairness in hot paths

If needed, we should prefer:

- locality
- bounded coordination
- fairness over time

over:

- perfect global ordering on every claim

That principle has repeatedly matched the measurements better than globally
optimal hot-path fairness.

## Current status after the first implementation pass

The first bounded-claimers pass used a **fixed small claimer cap** per queue.

That version was informative but reverted:

- it preserved the healthy `1x32` shape:
  - `clean_1 800/s`
  - `pressure_1 1199/s`
  - `recovery_1 800/s`
- after fixing the multi-replica report aggregation, it became clear that the
  `4x8` shape did **not** collapse to zero throughput:
  - `clean_1 778/s`
  - `pressure_1 829/s`
  - `recovery_1 754/s`
- but the corrected per-replica summary showed the real failure mode:
  - replica `0` did essentially all the work
  - replicas `1-3` remained idle
  - subscriber p99 was still very poor:
    - `clean_1 210 ms`
    - `pressure_1 5583 ms`
    - `recovery_1 17023 ms`

So the next bounded-claimers iteration should **not** keep a rigid cap under
all conditions.

The next variant should be:

- adaptive rather than fixed
- locality-first when the queue is calm
- able to expand active claimers during sustained saturation / recovery

That keeps the same semantic model, but makes the claimer limit part of the
runtime control loop rather than a hard static ceiling.

## Current status after the adaptive MVP

The first adaptive bounded-claimers implementation was also reverted.

What it did:

- kept queue-level claimer leases
- used a local controller that started at `1` claimer target
- expanded toward `4` after repeated full-batch claims
- contracted after repeated empty claims

What held up:

- targeted bounded-claimer runtime tests passed
- the `1x32` realistic gate remained healthy

What failed:

- the realistic `4x8` gate collapsed
- phase summaries showed effectively `0/s` useful throughput across
  `clean_1`, `pressure_1`, and `recovery_1`
- the run emitted:
  - multi-second `COMMIT`s
  - blocked `queue_claimer_leases` updates
  - stale-heartbeat rescues
  - pool timeouts under load

So the useful conclusion is:

- **adaptive bounded claimers is not enough in its naïve form**
- the claimer-lease control plane itself becomes a bottleneck in the hot queue
  shape

That means the next direction should not be “keep tuning the same adaptive
controller.” It should be a broader control-plane rethink, likely one of:

- a cheaper claimer-authority mechanism
- or queue striping as an explicit hot-queue mode

The latter should stay on the table even if a better bounded-claimers variant
is found, because it remains the clearest operational escape hatch for extreme
single-queue workloads.

## Current status after controller tuning

The next adaptive tuning pass kept the same semantic model but moved target
calculation off the hot claim round:

- shared `queue_claimer_state`
- queue-wide target refresh at most every `500ms`
- more aggressive expansion
- slower contraction
- jittered slot probing

That version is the current active bounded-claimers candidate.

Why it is being kept:

- `1x32` remained healthy
- `4x8` stayed genuinely multi-replica after warmup
- `pressure_1` and `recovery_1` both improved relative to the earlier adaptive
  pass

Why it is not done yet:

- `4x8 clean_1` latency is still too high
- `4x8 pressure_1` and `recovery_1` tails are still far above the shipping bar
- crash-under-load still needs to be rerun on this tuned controller

After the crash-under-load rerun, that last point is now concrete:

- the tuned controller still shows recovery-path maintenance errors
- backlog and tails remain too high during restart/recovery

So bounded claimers are still an active line of investigation, but no longer
look like a complete hot-queue answer by themselves.

This strengthens the case that **queue striping should move up from “future
idea” to “real hot-queue mode”** if the next controller pass still cannot make
the realistic and crash gates boring.

## Why this is the next design

This design is attractive because it keeps the good properties of the current
branch:

- direct short-job starts
- honest running state
- no second hot-path transaction
- low queue-plane churn

while attacking the measured remaining problem directly:

- too many small replicas all trying to be claimers on one hot queue

If it works, it is likely the cleanest path left to make the realistic `4x8`
shape good enough to ship with confidence.
