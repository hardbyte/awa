# ADR-031: Partitioned Queues

## Status

Proposed.

## Context

Awa already has the machinery for spreading one hot logical queue across multiple physical queues. `PartitionedQueue` supplies deterministic physical queue names and producer routing, Rust `ClientBuilder::partitioned_queue()` and Python `PartitionedQueue.queue_configs()` register every partition, and the queue-storage COPY path already accepts per-job `InsertParams` and groups internally by `(queue, priority, enqueue_shard)`.

The remaining design work is API stabilization and guardrail work before this surface becomes part of the stable `0.6` contract.

The key defect is correlated hashing across partition and shard levels. Partition selection previously reused `shard_for_ordering_key(key, partitions)`, while queue storage later used `shard_for_ordering_key(key, enqueue_shards)`. Both operations reduced the same hash value modulo different numbers. Whenever `gcd(partitions, enqueue_shards) > 1`, keyed traffic in one partition reached only `enqueue_shards / gcd(partitions, enqueue_shards)` shards. At `partitions = 4` and `enqueue_shards = 4`, keyed traffic in each partition landed on exactly one enqueue shard, silently negating ADR-025 for keyed workloads.

The ergonomics gaps are narrower:

- Python batch COPY exposed one `queue` and one `ordering_key` per call even though the Rust core supports per-row routing.
- The public name should describe partitioned routing rather than messaging fanout.
- The default naming should not strand direct enqueues to the logical queue during a `1 -> N` rollout.
- Docs must distinguish partitions, enqueue shards, internal queue stripes, ordering keys, and claimers.

## Terminology

- **Logical queue**: the application-visible workload name, for example `customer-updates`.
- **Partition / physical queue**: an ordinary Awa queue that carries one slice of the logical queue. This is the ordering scope.
- **Ordering key**: caller-supplied bytes that route related jobs to the same partition and to the same enqueue shard within it.
- **Enqueue shard**: ADR-025's per-`(queue, priority)` head-row shard inside one physical queue.
- **Internal queue stripe**: queue-storage `queue_stripe_count` striping using `queue#N` names. This remains an advanced storage knob, not the public API.

## Decision

Promote the existing routing machinery as **partitioned queues**.

The public type is `PartitionedQueue`. `QueueFanout` is not kept as an alias because the surface has not reached a stable release and two names for the same concept would make the partitioning vocabulary harder to teach. "Fanout" is also misleading in messaging systems because it often means duplicate delivery to every subscriber, while this feature routes each job to exactly one partition.

`PartitionedQueue::new(logical_queue, partitions)` uses the logical queue name itself as partition 0. With four partitions, the default physical queues are:

```text
customer-updates
customer-updates__p1
customer-updates__p2
customer-updates__p3
```

This makes `partitions = 1` literally a plain queue, keeps direct enqueues to the logical name consumable, and makes growing `1 -> N` a normal rolling-deploy operation rather than an orphaned-queue hazard.

Partition routing is domain-separated from enqueue-shard routing. The algorithm keeps ADR-025's portable base ordering-key hash intact for storage shard compatibility, then applies a partition-specific SplitMix64 finalizer before reducing modulo the partition count. Non-Rust producers that hand-roll routing must use that partition hash for physical queue selection and continue to pass the original key as `ordering_key`.

Python `insert_many_copy()` and `enqueue_many_copy()` accept per-job `opts`, where each entry may override `queue` and `ordering_key` for the matching job. Batch-level kwargs remain defaults. This exposes the Rust core's existing per-row COPY shape without adding a partition-specific bulk API.

Capacity remains per partition. `ClientBuilder::partitioned_queue()` applies `QueueConfig` to every physical queue; Python names this explicitly with `max_workers_per_partition`, `min_workers_per_partition`, and `rate_limit_per_partition`. In hard-reserved mode, logical capacity is roughly `partitions * max_workers_per_partition`. Use weighted mode plus `global_max_workers` when a logical total cap matters.

Width changes are repartitioning events. Growing changes key-to-partition routing; per-key FIFO is not preserved across the transition if old jobs for a key remain in the old partition while new jobs route to the new one. Shrinking can leave jobs in removed partitions until they drain or are moved. The safe grow order is workers first, then producers. The safe shrink order is producers first, drain or move removed partitions, then workers.

## Guarantees

Job safety is unchanged. Every partition is an ordinary Awa queue with the ADR-019 queue-storage guarantees, ADR-023 receipt-ring guarantees, and ADR-026 terminal-history guarantees. Transactional enqueue, at-least-once delivery, rescue, and `(job_id, run_lease)` guarded finalization are untouched.

Partitioning changes ordering scope only:

- `partitions = 1`: the queue behaves like a normal queue.
- `partitions > 1`: FIFO is scoped to the selected physical queue and enqueue shard; no cross-partition FIFO is promised.
- Jobs with the same ordering key route to the same partition and carry the same `ordering_key` into queue storage.
- Per-key FIFO is not preserved across a partition-count change unless the old partition has drained before new jobs for that key are produced.

## Relationship to Other ADRs

- **ADR-019: Queue Storage Engine.** Partitions compose as independent ordinary queues. Append-only ready/terminal storage, ready tombstones, receipt paths, and prune safety are unchanged.
- **ADR-023: Receipt Plane Ring Partitioning.** Receipt safety is per partition and composes across the partition set.
- **ADR-025: Sharded Enqueue Heads.** Partitions split a logical workload across physical queues. Enqueue shards split head rows within one physical queue. The domain-separated partition hash is required for the two levels to compose for keyed traffic.
- **ADR-026: Narrow Terminal History.** Terminal counts stay per physical queue. Logical aggregation is a read-side concern.
- **ADR-008: Batch COPY Ingestion.** The Rust queue-storage COPY path is already multi-queue per row; this ADR exposes that shape in Python batch APIs.
- **ADR-011: Weighted Concurrency.** Partition registration multiplies per-queue hard reservations and weights. Global caps remain the way to express a logical total.
- **ADR-022: Descriptor Catalog.** If partition metadata becomes persisted, it belongs in descriptor-style metadata off the dispatch hot path.
- **ADR-030: Durable Batch Operations.** A partitioned queue as a source means the union of partitions. A partitioned queue as a destination is deferred because preserving key routing would require per-job rehashing inside the operation.

## Consequences

Positive:

- Users get a single documented concept for the strongest demonstrated end-to-end throughput lever.
- The keyed-routing skew is fixed before the routing contract stabilizes.
- Python and Rust expose the same conceptual model.
- `queue_stripe_count` stops competing for the public fanout/striping story.

Negative:

- Awa still has three partitioning concepts: partitioned queues, enqueue shards, and internal queue stripes. Docs must keep those distinct.
- Partition-0-as-logical-name makes default physical names asymmetric.
- Logical aggregation in DB-only admin surfaces still needs runtime registration context or future persisted metadata.

## Alternatives Considered

### Keep `QueueFanout`

Rejected. The name is misleading for a feature that routes each job to one partition, and the surface is not stable enough to justify keeping both names.

### Preserve `logical__p0` as partition 0

Rejected. It leaves the bare logical queue name unconsumed at `partitions > 1`, so direct producers, cron schedules, and operator moves can strand work unless every caller is updated at once.

### Add `enqueue_many_grouped_copy(group, jobs, key=...)`

Rejected. The core COPY path already accepts per-row queue and ordering key. Per-job `opts` is simpler and works for every multi-queue batch, not just partitioned queues.

### Promote `queue_stripe_count`

Rejected as the public API. It is global queue-storage config with internal `queue#N` names and producer/worker configuration coupling. It remains an advanced internal storage knob.

### Persist partition metadata first

Deferred. A no-schema API layer proves the public contract. Persisted metadata becomes worthwhile when DB-side aggregation, UI discovery, or producer/worker partition-count mismatch detection needs it.

## Validation

The implementation should have:

- Rust unit tests for default naming, explicit physical queues, routing, validation, and std-trait ergonomics.
- A hash distribution test for `partitions = enqueue_shards = 4` showing keyed traffic inside each partition reaches every enqueue shard.
- Python tests for `PartitionedQueue`, queue config expansion, and per-job COPY opts.
- Integration tests that a worker registered on a partitioned queue can claim from every partition.
- Benchmarks covering partition counts `1`, `2`, `4`, and `8`, plus at least one `partitions x enqueue_shards` keyed-routing cell.
- A focused TLA+ routing model (`AwaPartitionedQueueRouting`) checking that partition routing composes with enqueue-shard routing while lane sequence identity remains scoped to `(partition, shard)`.

The ADR-019 storage lifecycle models do not need a new table or transition while partitioned queues remain client-side routing over independent ordinary queues. Revisit `AwaSegmentedStorage` itself only if routing or persisted partition metadata becomes part of the storage invariants.
