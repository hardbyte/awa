---- MODULE AwaPartitionedQueueRouting ----
EXTENDS Naturals, FiniteSets

\* Focused ADR-031 routing model.
\*
\* Partitioned queues are not a new storage family: each partition is an
\* ordinary Awa queue covered by AwaSegmentedStorage. This model checks the
\* client-side refinement boundary that sits above those queues:
\*
\* - every produced job is routed to the partition selected by its key
\* - the original ordering key is still available to the storage layer, so
\*   enqueue-shard routing is stable inside the chosen partition
\* - lane sequence identity is scoped to (partition, shard)
\* - the chosen routing table for the model reaches every shard from every
\*   partition, which is the property the Rust domain-separated hash test
\*   enforces for representative key sets

CONSTANTS Partitions,
          Shards,
          MaxJobs

VARIABLES jobs,
          nextSeq

vars == <<jobs, nextSeq>>

KeySet == Partitions \X Shards

PartitionOf(k) == k[1]
ShardOf(k) == k[2]

JobRows ==
    [id: 1..MaxJobs,
     key: KeySet,
     partition: Partitions,
     shard: Shards,
     seq: 1..(MaxJobs + 1)]

Init ==
    /\ jobs = {}
    /\ nextSeq = [p \in Partitions |-> [s \in Shards |-> 1]]

Enqueue(k) ==
    /\ k \in KeySet
    /\ Cardinality(jobs) < MaxJobs
    /\ LET p == PartitionOf(k)
           s == ShardOf(k)
           id == Cardinality(jobs) + 1
       IN
       /\ p \in Partitions
       /\ s \in Shards
       /\ jobs' = jobs \cup
            {[id |-> id,
              key |-> k,
              partition |-> p,
              shard |-> s,
              seq |-> nextSeq[p][s]]}
       /\ nextSeq' = [nextSeq EXCEPT ![p][s] = @ + 1]

Next ==
    \E k \in KeySet : Enqueue(k)

Spec ==
    Init /\ [][Next]_vars

TypeOK ==
    /\ jobs \subseteq JobRows
    /\ nextSeq \in [Partitions -> [Shards -> 1..(MaxJobs + 1)]]
    /\ \A k \in KeySet :
        /\ PartitionOf(k) \in Partitions
        /\ ShardOf(k) \in Shards

RowsFollowRouting ==
    \A row \in jobs :
        /\ row.partition = PartitionOf(row.key)
        /\ row.shard = ShardOf(row.key)

SameKeySamePartition ==
    \A a, b \in jobs :
        a.key = b.key => a.partition = b.partition

SameKeySameShard ==
    \A a, b \in jobs :
        a.key = b.key => a.shard = b.shard

SeqUniqueWithinPartitionShard ==
    \A a, b \in jobs :
        /\ a.id # b.id
        /\ a.partition = b.partition
        /\ a.shard = b.shard
        => a.seq # b.seq

PartitionShardCoverage ==
    \A p \in Partitions :
        {ShardOf(k) : k \in {candidate \in KeySet : PartitionOf(candidate) = p}} = Shards

ASSUME PartitionShardCoverage

====
