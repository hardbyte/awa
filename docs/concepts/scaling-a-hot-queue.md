# Scaling a hot queue

One busy queue can outgrow the coordination around a single [lane](lanes-and-claiming.md). A lane admits one producer at a time, because a producer holds it until it commits, and one claimer at a time, because a claim locks the lane's cursor. When many producers and workers share one `(queue, priority)`, they queue behind those two locks while the database still has room.

This page is the tuning procedure: what to check first, which lever to try in which order, and how to tell whether it helped. [Queues, lanes and splitting work](queues-lanes-and-splitting-work.md) explains what each lever changes about a queue and what it gives up.

## Check it is short of lanes

Before spreading a queue, check that it is short of lane throughput rather than of workers. A queue whose [utilization](../configuration.md#sizing-workers-from-utilization) stays above about 0.8 needs more workers or a split by service time. Raising `max_workers` adds workers, and so does a `PartitionedQueue`, whose partitions each get their own; enqueue shards and claimers add none.

## Which to try first

1. **Batch.** A larger claim or insert [batch](batching.md) moves more jobs through each lock. Workers sitting idle while the queue has a backlog points at claiming, not at the lane: raise `claim_batch_size` first.
2. **Partition by key.** If the workload has a natural key (customer, account, order) and can live with per-key ordering, use a [partitioned queue](queues-lanes-and-splitting-work.md#one-slice-needs-its-own-limits-partitioned-queues). It also gives each partition its own worker capacity and rate limit.
3. **Shard.** If the queue must stay one queue to operators, raise [`enqueue_shards`](queues-lanes-and-splitting-work.md#producers-waiting-on-one-lane-enqueue-shards).
4. **Add claimers last**, and only with evidence that one claim loop leaves workers idle; see [claimers](queues-lanes-and-splitting-work.md#idle-workers-with-a-backlog-claimers).

## Measured results

- **Enqueue shards.** In a 16-producer benchmark on one queue, 2, 4 and 8 shards gave 1.6, 2.75 and 3.7 times the throughput of one shard. In the hot-queue reference shape, `enqueue_shards = 4` is the change that turned overload into bounded end-to-end throughput.
- **Claimers.** In the same reference shape, raising `claimers` to 4 increased transaction pressure and made tail latency worse.

## Measuring the result

Judge a change by durable completions per second, end-to-end p99 latency, queue depth, and WAL bytes and transactions per completed job. Compare against a run of the same workload before the change. The `awa.job.claimed` counter carries an `awa.enqueue.shard` attribute, so a dashboard can confirm that claims are spread evenly across shards.

## Queue stripes

`queue_stripe_count` also splits queues into more lanes, but for every queue in the fleet at once. It is planned for deprecation in 0.7 in favour of enqueue shards and partitioned queues ([#752](https://github.com/hardbyte/awa/issues/752)); do not adopt it for new deployments. Existing settings are described under [Queue storage tuning](../configuration.md#what-the-knobs-mean).

## Related

- [Queues, lanes and splitting work](queues-lanes-and-splitting-work.md): the problem each tool solves, what it gives up, and which combine.
- [Lanes and claiming](lanes-and-claiming.md): what a lane is and how a claim takes from it.
- [Batching](batching.md): moving more jobs through each statement.
- [Sharding the enqueue head](../configuration.md#sharding-the-enqueue-head-per-queue), [Hot-queue claim control](../configuration.md#hot-queue-claim-control) and [ADR-025](../adr/025-sharded-enqueue-heads.md): settings and design records.
