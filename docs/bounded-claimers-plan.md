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
- claimer metrics:
  - active claimer count
  - claimer acquire success/miss
  - claimer renews
  - claimer steals
  - claimer expiries

### Secondary gate

- crash-under-load scenario
- retry/priority semantics scenario

### Success thresholds

The minimum bar to keep the design:

- `1x32`
  - stays roughly where the current good branch is
- `4x8`
  - `clean_1` materially above current poor shapes
  - `pressure_1` materially above current poor shapes
  - `recovery_1` no longer stalls or collapses
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

1. add `max_claimers_per_queue`
2. add `queue_claimer_leases`
3. add metrics fields for queue claimer state

### Phase 2: worker runtime

1. track owned claimer slots per queue
2. renew only on successful claims
3. attempt idle/expired steal only when underfilled
4. cap owned claimer slots per replica

### Phase 3: claim path integration

1. gate queue claims on claimer ownership
2. keep existing direct `ready -> active_receipt` path
3. do not add any new per-job pre-start state

### Phase 4: measurement and hardening

1. rerun `1x32 / 2x16 / 4x8 / 8x4`
2. rerun crash-under-load
3. rerun retry/priority semantics
4. compare to canonical in the realistic shape

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
