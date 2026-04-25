# Sticky Shard Leasing Plan

> **Status: reverted experiment, kept as design context.** Sticky shard
> leasing was prototyped during the 0.6 redesign but not adopted —
> bounded claimers + queue striping (both shipped) cover the same
> coordination pressure with simpler operational semantics. This doc is
> kept for historical context on the design space; do not implement
> from it without revisiting the trade-off against the shipped path.

## Why this exists

The queue-storage engine is now in a good place for:

- single large runtime shapes like `1x32`
- dead-tuple behavior on the hot queue plane
- receipt/attempt/lease correctness for the common short-job and long-running
  paths

The remaining blocker for `0.6` is the realistic many-small-replica shape on
one hot queue:

- `2x16`
- `4x8`
- `8x4`

Benchmarking and reverted spikes have narrowed the problem:

- unsafe receipt-buffer prefetch helped but was wrong
- explicit reserve/promote frontiers were safe but too expensive
- striped claim heads helped somewhat but were not sufficient
- a stateful receipt frontier was also too expensive

The strongest remaining hypothesis is:

- many small replicas need **sticky local claim authority**
- but we must not add a second hot-path transaction per started job
- and we must not lie about `running_depth` or start rescue semantics early

This document is the implementation plan for that next design.

It now describes **v2** of sticky shard leasing.

v1 proved that shard-local ownership is compatible with the hot path and keeps
dead tuples low, but it was too sticky:

- `1x32` stayed healthy
- `4x8` still underutilized workers
- `recovery_1` could stall because shard ownership was not redistributed fast
  enough

So v2 keeps direct starts, but changes the ownership model to be:

- sticky while a replica is actively claiming from a shard
- quickly stealable once that shard becomes idle
- fair over time, not globally optimal on every claim

## Design summary

Use **sticky shard leasing** for the claim plane.

Instead of one global claim cursor per `(queue, priority)`, split each hot lane
into a small fixed set of claim shards. Replicas lease claim authority over
those shards for a short period and claim directly from owned shards into the
existing active-attempt path.

The resulting shape is:

- queue plane remains append-only
- short jobs still go directly `ready -> active_receipt`
- no explicit `reserved-but-not-started` job state is introduced
- no extra per-job promotion transaction is added
- `running_depth` remains honest

## State machines

### Shard lease state machine (v2)

```text
[unowned]
   |
   | acquire(queue, priority, shard, instance)
   v
[owned-active(instance, epoch, expires_at)]
   |
   | no successful claims for idle_threshold
   v
[owned-idle(instance, epoch, expires_at)]
   |
   | successful claim by owner
   v
[owned-active(instance, epoch, expires_at')]
   |
   | expiry / relinquish / steal of idle shard
   v
[unowned]
```

Properties:

- one live owner at most for `(queue, priority, shard)`
- ownership is time-bounded
- lease expiry does not change job state
- `lease_epoch` prevents stale renewals or stale claims from a replaced owner
- ownership alone never means a job is active

Recommended lease row shape:

- `queue`
- `priority`
- `claim_shard`
- `owner_instance_id`
- `lease_epoch`
- `leased_at`
- `last_claimed_at`
- `expires_at`

Derived state:

- `unowned`
  - row absent or `expires_at <= now()`
- `owned-active`
  - `expires_at > now()`
  - `last_claimed_at > now() - idle_threshold`
- `owned-idle`
  - `expires_at > now()`
  - `last_claimed_at <= now() - idle_threshold`

### Job state machine

Unchanged from the current branch:

```text
ready -> active_receipt -> attempt_state -> active_lease
```

with exits to:

- completed
- deferred/retry
- waiting
- dlq

That is intentional. Sticky shard leasing changes **claim authority**, not job
lifecycle semantics.

## Schema sketch

### 1. Shard-local claim heads

Replace one row per `(queue, priority)` with one row per
`(queue, priority, claim_shard)`:

```sql
CREATE TABLE {schema}.queue_claim_heads (
    queue TEXT NOT NULL,
    priority SMALLINT NOT NULL,
    claim_shard SMALLINT NOT NULL,
    claim_seq BIGINT NOT NULL,
    PRIMARY KEY (queue, priority, claim_shard)
);
```

### 2. Shard ownership leases

New bounded coordination table:

```sql
CREATE TABLE {schema}.lane_shard_leases (
    queue TEXT NOT NULL,
    priority SMALLINT NOT NULL,
    claim_shard SMALLINT NOT NULL,
    owner_instance_id UUID NOT NULL,
    lease_epoch BIGINT NOT NULL,
    leased_at TIMESTAMPTZ NOT NULL,
    last_claimed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue, priority, claim_shard)
);
```

Recommended index:

```sql
CREATE INDEX ON {schema}.lane_shard_leases (queue, owner_instance_id, expires_at);
```

### 3. Ready-row shard assignment

Each ready row needs stable shard assignment.

Recommended first version:

- derive `claim_shard = lane_seq % claim_shard_count`

That can be:

- stored physically as a column on `ready_entries`, or
- derived in queries

For the implementation pass, prefer a stored column because it simplifies
indexing and avoids repeated modulo work in the hot claim query.

Suggested index:

```sql
CREATE INDEX ON {schema}.ready_entries (queue, priority, claim_shard, lane_seq);
```

## Claim algorithm

### Fast path

1. Dispatcher wakes with free local workers.
2. It uses or acquires one or more owned shards for `(queue, priority)`.
3. It claims directly from owned shards:
   - lock shard-local `queue_claim_heads`
   - select next ready rows where `claim_shard = owned_shard`
   - create active receipt claims immediately
4. Workers start as they do today.

There is no new reserved job state and no second start transaction.

### Acquire shard

For a replica that needs work:

```text
try acquire shard where:
- shard row absent
or
- expires_at < now()
or
- owner_instance_id = self (renew)
```

Update:

- `owner_instance_id = self`
- `lease_epoch = lease_epoch + 1`
- `leased_at = now()`
- `expires_at = now() + shard_lease_ttl`

### Renew shard

If a replica successfully claims from a shard:

- renew before expiry
- update `last_claimed_at`
- retain locality

Important v2 rule:

- **do not** renew just because the dispatcher wakes
- **do not** renew just because the replica exists
- renew only on successful claim (or a very short post-claim grace window if we
  later need one)

### Steal / reclaim policy (v2)

When a replica has free workers and no useful owned shard:

1. take unowned shards
2. reclaim expired shards
3. steal `owned-idle` shards

Do **not** steal `owned-active` shards in v2.

Mechanically:

- `UPDATE lane_shard_leases`
- guarded by `lease_epoch`
- succeeds only if:
  - the row is expired, or
  - `last_claimed_at < now() - idle_threshold`

On steal:

- replace `owner_instance_id`
- increment `lease_epoch`
- reset `leased_at`, `last_claimed_at`, and `expires_at`

## Fairness model

### Within a shard

- FIFO by `lane_seq`
- claim-time effective priority aging still applies

### Across shards

Ordering becomes approximate rather than globally serialized.

That is acceptable if:

- shard count is modest
- skew is bounded by shard count
- aging still prevents starvation

### Across replicas

Fairness comes from:

- lease expiry
- idle-shard stealing
- bounded shard ownership per replica

Recommended first cap:

- a replica may own at most `1-2` shards per `(queue, priority)` lane in the
  initial implementation

## Safety invariants

These are the non-negotiable ones:

1. At most one live owner exists for `(queue, priority, claim_shard)`.
2. Shard ownership does not itself imply job ownership.
3. Unclaimed jobs remain in `ready_entries` until actually claimed.
4. Only active attempts count as `running`.
5. Crash before claim:
   - no job state changes
   - shard lease expiry restores availability of the shard
6. Crash after claim:
   - existing receipt/attempt/lease rescue applies
7. Stale owners cannot renew or continue claiming after `lease_epoch` changes.
8. Idle owners do not block progress indefinitely because `owned-idle` shards
   are stealable.

## Dead-tuple expectations

This design should:

- reduce repeated contention on one `queue_claim_heads` row
- avoid introducing a new reserved-job frontier
- keep `open_receipt_claims` limited to actually active work

New churn source:

- `lane_shard_leases`

That is acceptable if lease rows remain:

- tiny
- bounded
- renewed less frequently than per-job starts
- updated only on successful claim or ownership transfer

## Implementation plan

### Phase 1: schema and query shape

1. add `claim_shard_count` to `QueueStorageConfig`
2. extend `ready_entries` with `claim_shard`
3. replace `queue_claim_heads` with shard-local rows
4. add `lane_shard_leases` with `last_claimed_at`
5. add/refresh indexes for shard-local claim queries

### Phase 2: claim path

1. add shard acquire/renew helpers in `QueueStorage`
2. update `claim_ready_runtime(...)` to:
   - prefer `owned-active` shards
   - then unowned / expired shards
   - then `owned-idle` shards eligible for steal
   - claim directly from owned or stolen shards
   - continue to create active receipt claims immediately
3. keep direct `ready -> active_receipt`

### Phase 3: dispatcher

1. track owned shards per queue/priority in each dispatcher
2. renew shards only on successful claim
3. steal idle shards only when local capacity is underfilled
4. cap shard ownership per replica
5. no separate reservation buffer

### Phase 4: maintenance / expiry

1. add cleanup of expired `lane_shard_leases`
2. ensure stale owners naturally lose claim authority
3. no job recovery path is needed for shard expiry, because jobs remain ready
4. no shard should remain non-stealable indefinitely if it is not producing
   successful claims

### Phase 5: metrics

Add first-class metrics:

- `owned_shards`
- `shard_acquire_success_total`
- `shard_acquire_miss_total`
- `shard_renew_total`
- `shard_steal_total`
- `shard_expiry_total`
- claim batch size by shard

## Acceptance test matrix

Primary gate:

- `1x32`
- `2x16`
- `4x8`
- `8x4`

with:

- single producer (`PRODUCER_ONLY_INSTANCE_ZERO=1`)
- same phase profile as the current realistic gate

Secondary gate:

- crash-under-load semantics
- retry/priority semantics

### Success criteria

We keep the change only if:

1. `1x32` remains roughly competitive with the current branch
2. `4x8` improves materially in throughput and latency
3. `recovery_1` no longer stalls under `4x8`
4. dead tuples stay low
5. crash-under-load remains correct
5. no new misleading control-plane state is required to explain `running_depth`

## Non-goals for v1

Do not add in the first pass:

- active stealing from healthy shard owners
- a reserved-but-not-started job state
- a second promotion transaction before start
- global fairness stronger than bounded skew + aging

Those can come later if the basic sticky-leasing design proves worthwhile.
