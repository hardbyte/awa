use crate::job::InsertOpts;
use crate::queue_storage::ordering_key_hash64;
use std::collections::HashSet;

const DEFAULT_PHYSICAL_QUEUE_SUFFIX: &str = "__p";
const PARTITION_HASH_DOMAIN: u64 = 0x9d4d_1b2f_53a7_0c91;

/// Errors returned when constructing a [`PartitionedQueue`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PartitionedQueueError {
    #[error("partitioned queue logical queue must not be empty")]
    EmptyLogicalQueue,
    #[error("partitioned queue partitions must be > 0")]
    ZeroPartitions,
    #[error("partitioned queue supports at most {max} physical queues; got {got}")]
    TooManyPhysicalQueues { got: usize, max: usize },
    #[error("partitioned queue physical queue must not be empty")]
    EmptyPhysicalQueue,
    #[error("partitioned queue physical queue '{queue}' is duplicated")]
    DuplicatePhysicalQueue { queue: String },
}

/// A deterministic set of physical queues for one hot logical queue.
///
/// Awa still stores and executes jobs from ordinary queue names. This helper
/// gives producers and workers the same stable list of physical queues, so an
/// application can partition one logical workload over several queues without
/// hand-rolling naming and routing in every process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedQueue {
    logical_queue: String,
    physical_queues: Vec<String>,
}

impl PartitionedQueue {
    /// Build a partitioned queue using Awa's default physical queue naming.
    ///
    /// One partition maps to the logical queue name itself. With more than one
    /// partition, partition 0 is still the logical queue name and later
    /// partitions are `{logical_queue}__p1`, `{logical_queue}__p2`, and so on.
    /// That keeps direct enqueues to the logical name consumable during a
    /// `1 -> N` rollout.
    pub fn new(
        logical_queue: impl Into<String>,
        partitions: usize,
    ) -> Result<Self, PartitionedQueueError> {
        let logical_queue = logical_queue.into();
        validate_logical_queue(&logical_queue)?;
        validate_physical_count(partitions)?;

        let mut physical_queues = Vec::with_capacity(partitions);
        physical_queues.push(logical_queue.clone());
        physical_queues.extend(
            (1..partitions)
                .map(|idx| format!("{logical_queue}{DEFAULT_PHYSICAL_QUEUE_SUFFIX}{idx}")),
        );

        Ok(Self {
            logical_queue,
            physical_queues,
        })
    }

    /// Build a partitioned queue from explicit physical queue names.
    ///
    /// Use this when an application already has queue names it wants to keep,
    /// or when migrating an existing manually-fanned-out deployment to the
    /// shared helper.
    pub fn from_physical_queues<I, S>(
        logical_queue: impl Into<String>,
        physical_queues: I,
    ) -> Result<Self, PartitionedQueueError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let logical_queue = logical_queue.into();
        validate_logical_queue(&logical_queue)?;

        let physical_queues: Vec<String> = physical_queues.into_iter().map(Into::into).collect();
        validate_physical_count(physical_queues.len())?;
        validate_physical_queues(&physical_queues)?;

        Ok(Self {
            logical_queue,
            physical_queues,
        })
    }

    /// Logical queue name used by the application.
    pub fn logical_queue(&self) -> &str {
        &self.logical_queue
    }

    /// Physical queues that must be declared on worker runtimes.
    pub fn physical_queues(&self) -> &[String] {
        &self.physical_queues
    }

    /// Number of physical queues in the partitioned queue.
    pub fn partitions(&self) -> usize {
        self.physical_queues.len()
    }

    /// Select a physical queue by stable routing key.
    ///
    /// The same key always maps to the same physical queue. Partition routing
    /// domain-separates the hash used for queue-storage enqueue shards so
    /// `partitions == enqueue_shards` does not collapse keyed traffic onto one
    /// shard inside each partition.
    pub fn queue_for_key(&self, key: impl AsRef<[u8]>) -> &str {
        let partition = partition_for_ordering_key(key.as_ref(), self.partitions());
        &self.physical_queues[partition]
    }

    /// Select a physical queue by caller-supplied sequence number.
    ///
    /// This is useful for bulk producers that want round-robin partitioning and
    /// do not need per-key ordering.
    pub fn queue_for_index(&self, index: usize) -> &str {
        &self.physical_queues[index % self.partitions()]
    }

    /// Return insert options routed by key.
    ///
    /// This sets both the physical queue and `ordering_key`, so per-key FIFO is
    /// preserved even if the selected physical queue later uses multiple
    /// queue-storage enqueue shards.
    pub fn route_opts_by_key(&self, mut opts: InsertOpts, key: impl AsRef<[u8]>) -> InsertOpts {
        let key = key.as_ref();
        opts.queue = self.queue_for_key(key).to_string();
        opts.ordering_key = Some(key.to_vec());
        opts
    }

    /// Return insert options routed by round-robin index.
    pub fn route_opts_by_index(&self, mut opts: InsertOpts, index: usize) -> InsertOpts {
        opts.queue = self.queue_for_index(index).to_string();
        opts
    }
}

impl AsRef<[String]> for PartitionedQueue {
    fn as_ref(&self) -> &[String] {
        self.physical_queues()
    }
}

impl<'a> IntoIterator for &'a PartitionedQueue {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.physical_queues.iter()
    }
}

fn validate_logical_queue(logical_queue: &str) -> Result<(), PartitionedQueueError> {
    if logical_queue.is_empty() {
        return Err(PartitionedQueueError::EmptyLogicalQueue);
    }
    Ok(())
}

fn validate_physical_count(count: usize) -> Result<(), PartitionedQueueError> {
    if count == 0 {
        return Err(PartitionedQueueError::ZeroPartitions);
    }

    let max = i16::MAX as usize;
    if count > max {
        return Err(PartitionedQueueError::TooManyPhysicalQueues { got: count, max });
    }

    Ok(())
}

/// Deterministically map an ordering key to a partition in `[0, partitions)`.
///
/// This uses the same portable base hash as `shard_for_ordering_key`, then
/// applies a domain-separated SplitMix64 finalizer before taking the modulo.
/// That keeps partition selection stable and portable without correlating it
/// with the enqueue-shard modulo for the same key bytes.
pub fn partition_for_ordering_key(ordering_key: &[u8], partitions: usize) -> usize {
    if partitions <= 1 {
        return 0;
    }
    (partition_hash64(ordering_key) % partitions as u64) as usize
}

/// Portable partition-routing hash over raw ordering-key bytes.
///
/// This is domain-separated from [`crate::queue_storage::ordering_key_hash64`]
/// so keyed partition routing composes with queue-storage enqueue sharding.
pub fn partition_hash64(ordering_key: &[u8]) -> u64 {
    let mut value = ordering_key_hash64(ordering_key) ^ PARTITION_HASH_DOMAIN;
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate_physical_queues(queues: &[String]) -> Result<(), PartitionedQueueError> {
    let mut seen = HashSet::with_capacity(queues.len());
    for queue in queues {
        if queue.is_empty() {
            return Err(PartitionedQueueError::EmptyPhysicalQueue);
        }
        if !seen.insert(queue.as_str()) {
            return Err(PartitionedQueueError::DuplicatePhysicalQueue {
                queue: queue.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_names_preserve_single_queue_shape() {
        let queue = PartitionedQueue::new("email", 1).expect("partitioned queue should build");

        assert_eq!(queue.logical_queue(), "email");
        assert_eq!(queue.physical_queues(), &["email".to_string()]);
        assert_eq!(queue.partitions(), 1);
        assert_eq!(queue.queue_for_index(42), "email");
    }

    #[test]
    fn default_names_are_stable_for_multiple_queues() {
        let queue = PartitionedQueue::new("email", 4).expect("partitioned queue should build");

        assert_eq!(
            queue.physical_queues(),
            &[
                "email".to_string(),
                "email__p1".to_string(),
                "email__p2".to_string(),
                "email__p3".to_string(),
            ]
        );
        assert_eq!(queue.partitions(), 4);
        assert_eq!(queue.queue_for_index(0), "email");
        assert_eq!(queue.queue_for_index(5), "email__p1");
    }

    #[test]
    fn key_routing_sets_queue_and_ordering_key() {
        let queue =
            PartitionedQueue::new("customer-updates", 4).expect("partitioned queue should build");

        let opts = queue.route_opts_by_key(InsertOpts::default(), b"customer-42");

        assert_eq!(opts.queue, queue.queue_for_key(b"customer-42"));
        assert_eq!(opts.ordering_key.as_deref(), Some(&b"customer-42"[..]));
    }

    #[test]
    fn partition_hash_is_domain_separated_from_enqueue_shard_hash() {
        use crate::queue_storage::shard_for_ordering_key;

        let partitions = 4;
        let shards = 4;
        let mut partition_shards = vec![HashSet::new(); partitions];
        for idx in 0..20_000 {
            let key = format!("customer-{idx}");
            let partition = partition_for_ordering_key(key.as_bytes(), partitions);
            let shard = shard_for_ordering_key(key.as_bytes(), shards);
            partition_shards[partition].insert(shard);
        }

        for hits in partition_shards {
            assert_eq!(hits.len(), shards as usize);
        }
    }

    #[test]
    fn explicit_queues_reject_empty_and_duplicate_names() {
        let empty = PartitionedQueue::from_physical_queues("email", ["email-a", ""]);
        assert!(matches!(
            empty,
            Err(PartitionedQueueError::EmptyPhysicalQueue)
        ));

        let duplicate = PartitionedQueue::from_physical_queues("email", ["email-a", "email-a"]);
        assert!(matches!(
            duplicate,
            Err(PartitionedQueueError::DuplicatePhysicalQueue { queue }) if queue == "email-a"
        ));
    }

    #[test]
    fn explicit_queues_preserve_caller_order() {
        let queue = PartitionedQueue::from_physical_queues(
            "email",
            ["email-fast", "email-bulk", "email-slow"],
        )
        .expect("partitioned queue should build");

        assert_eq!(
            queue.physical_queues(),
            &[
                "email-fast".to_string(),
                "email-bulk".to_string(),
                "email-slow".to_string(),
            ]
        );
        assert_eq!(queue.queue_for_index(0), "email-fast");
        assert_eq!(queue.queue_for_index(4), "email-bulk");
    }

    #[test]
    fn partition_count_is_bounded_by_queue_storage_shard_type() {
        let too_wide = PartitionedQueue::from_physical_queues(
            "email",
            (0..=(i16::MAX as usize)).map(|idx| format!("email-{idx}")),
        );

        assert!(matches!(
            too_wide,
            Err(PartitionedQueueError::TooManyPhysicalQueues { got, max })
                if got == i16::MAX as usize + 1 && max == i16::MAX as usize
        ));
    }
}
