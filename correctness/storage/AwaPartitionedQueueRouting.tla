---- MODULE AwaPartitionedQueueRouting ----
EXTENDS Naturals, Integers, FiniteSets

\* Focused ADR-031 routing model.
\*
\* Partitioned queues are not a new storage family: each partition is an
\* ordinary Awa queue covered by AwaSegmentedStorage. This model checks the
\* client-side refinement boundary that sits above those queues:
\*
\* - keys are abstract base hash values, not pre-routed partition/shard pairs
\* - every produced job is routed to the partition selected by its key hash
\* - the original ordering key is still available to the storage layer, so
\*   enqueue-shard routing is stable inside the chosen partition
\* - lane sequence identity is scoped to (partition, shard)
\* - domain-separated partition routing reaches every shard from every
\*   partition. The broken spec reuses the base hash modulo both levels and
\*   demonstrates the correlated-hash defect from ADR-031.

CONSTANTS PartitionCount,
          ShardCount,
          MaxHash,
          MaxJobs

VARIABLES jobs,
          nextSeq

vars == <<jobs, nextSeq>>

ASSUME /\ PartitionCount > 0
       /\ ShardCount > 0
       /\ MaxHash >= 0
       /\ MaxJobs > 0

Partitions == 0..(PartitionCount - 1)
Shards == 0..(ShardCount - 1)
KeyHashes == 0..MaxHash

\* ADR-025 enqueue-shard routing uses the portable base ordering-key hash.
BaseHash(h) == h
ShardOf(h) == BaseHash(h) % ShardCount

\* Abstracts the Rust SplitMix64-style finalizer over
\* `ordering_key_hash64(key) ^ PARTITION_HASH_DOMAIN`. The important property
\* for this storage-level model is that the partition hash folds independent
\* hash bits into the low bits before modulo reduction.
PartitionHash(h) == (BaseHash(h) + (BaseHash(h) \div ShardCount)) % (MaxHash + 1)
PartitionOf(h) == PartitionHash(h) % PartitionCount

\* Historical broken shape: partition and shard both reduce the same base hash.
CorrelatedPartitionOf(h) == BaseHash(h) % PartitionCount

JobRows ==
    [id: 1..MaxJobs,
     keyHash: KeyHashes,
     partition: Partitions,
     shard: Shards,
     seq: 1..(MaxJobs + 1)]

Init ==
    /\ jobs = {}
    /\ nextSeq = [p \in Partitions |-> [s \in Shards |-> 1]]

Enqueue(k) ==
    /\ k \in KeyHashes
    /\ Cardinality(jobs) < MaxJobs
    /\ LET p == PartitionOf(k)
           s == ShardOf(k)
           id == Cardinality(jobs) + 1
       IN
       /\ p \in Partitions
       /\ s \in Shards
       /\ jobs' = jobs \cup
            {[id |-> id,
              keyHash |-> k,
              partition |-> p,
              shard |-> s,
              seq |-> nextSeq[p][s]]}
       /\ nextSeq' = [nextSeq EXCEPT ![p][s] = @ + 1]

EnqueueBroken(k) ==
    /\ k \in KeyHashes
    /\ Cardinality(jobs) < MaxJobs
    /\ LET p == CorrelatedPartitionOf(k)
           s == ShardOf(k)
           id == Cardinality(jobs) + 1
       IN
       /\ p \in Partitions
       /\ s \in Shards
       /\ jobs' = jobs \cup
            {[id |-> id,
              keyHash |-> k,
              partition |-> p,
              shard |-> s,
              seq |-> nextSeq[p][s]]}
       /\ nextSeq' = [nextSeq EXCEPT ![p][s] = @ + 1]

Next ==
    \E k \in KeyHashes : Enqueue(k)

NextBroken ==
    \E k \in KeyHashes : EnqueueBroken(k)

Spec ==
    Init /\ [][Next]_vars

SpecBroken ==
    Init /\ [][NextBroken]_vars

TypeOK ==
    /\ jobs \subseteq JobRows
    /\ nextSeq \in [Partitions -> [Shards -> 1..(MaxJobs + 1)]]
    /\ \A k \in KeyHashes :
        /\ PartitionOf(k) \in Partitions
        /\ ShardOf(k) \in Shards

TypeOKBroken ==
    /\ jobs \subseteq JobRows
    /\ nextSeq \in [Partitions -> [Shards -> 1..(MaxJobs + 1)]]
    /\ \A k \in KeyHashes :
        /\ CorrelatedPartitionOf(k) \in Partitions
        /\ ShardOf(k) \in Shards

RowsFollowRouting ==
    \A row \in jobs :
        /\ row.partition = PartitionOf(row.keyHash)
        /\ row.shard = ShardOf(row.keyHash)

RowsFollowCorrelatedRouting ==
    \A row \in jobs :
        /\ row.partition = CorrelatedPartitionOf(row.keyHash)
        /\ row.shard = ShardOf(row.keyHash)

SameKeySamePartition ==
    \A a, b \in jobs :
        a.keyHash = b.keyHash => a.partition = b.partition

SameKeySameShard ==
    \A a, b \in jobs :
        a.keyHash = b.keyHash => a.shard = b.shard

SeqUniqueWithinPartitionShard ==
    \A a, b \in jobs :
        /\ a.id # b.id
        /\ a.partition = b.partition
        /\ a.shard = b.shard
        => a.seq # b.seq

PartitionShardCoverage ==
    \A p \in Partitions :
        {ShardOf(k) : k \in {candidate \in KeyHashes : PartitionOf(candidate) = p}} = Shards

CorrelatedPartitionShardCoverage ==
    \A p \in Partitions :
        {ShardOf(k) : k \in {candidate \in KeyHashes :
            CorrelatedPartitionOf(candidate) = p}} = Shards

ASSUME PartitionShardCoverage

====
