use crate::job::InsertOpts;
use crate::queue_storage::shard_for_ordering_key;
use std::collections::HashSet;

const DEFAULT_PHYSICAL_QUEUE_SUFFIX: &str = "__p";

/// Errors returned when constructing a [`QueueFanout`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueueFanoutError {
    #[error("queue fanout logical queue must not be empty")]
    EmptyLogicalQueue,
    #[error("queue fanout width must be > 0")]
    ZeroWidth,
    #[error("queue fanout supports at most {max} physical queues; got {got}")]
    TooManyPhysicalQueues { got: usize, max: usize },
    #[error("queue fanout physical queue must not be empty")]
    EmptyPhysicalQueue,
    #[error("queue fanout physical queue '{queue}' is duplicated")]
    DuplicatePhysicalQueue { queue: String },
}

/// A deterministic set of physical queues for one hot logical queue.
///
/// Awa still stores and executes jobs from ordinary queue names. This helper
/// gives producers and workers the same stable list of physical queues, so an
/// application can fan one logical workload out over several queues without
/// hand-rolling naming and routing in every process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueFanout {
    logical_queue: String,
    physical_queues: Vec<String>,
}

impl QueueFanout {
    /// Build a fanout using Awa's default physical queue naming.
    ///
    /// Width `1` maps to the logical queue name itself. Widths above `1`
    /// produce `{logical_queue}__p0`, `{logical_queue}__p1`, and so on.
    pub fn new(logical_queue: impl Into<String>, width: usize) -> Result<Self, QueueFanoutError> {
        let logical_queue = logical_queue.into();
        validate_logical_queue(&logical_queue)?;
        validate_physical_count(width)?;

        let physical_queues = if width == 1 {
            vec![logical_queue.clone()]
        } else {
            (0..width)
                .map(|idx| format!("{logical_queue}{DEFAULT_PHYSICAL_QUEUE_SUFFIX}{idx}"))
                .collect()
        };

        Ok(Self {
            logical_queue,
            physical_queues,
        })
    }

    /// Build a fanout from explicit physical queue names.
    ///
    /// Use this when an application already has queue names it wants to keep,
    /// or when migrating an existing manually-fanned-out deployment to the
    /// shared helper.
    pub fn from_physical_queues<I, S>(
        logical_queue: impl Into<String>,
        physical_queues: I,
    ) -> Result<Self, QueueFanoutError>
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

    /// Number of physical queues in the fanout.
    pub fn width(&self) -> usize {
        self.physical_queues.len()
    }

    /// Select a physical queue by stable routing key.
    ///
    /// The same key always maps to the same physical queue, using the same
    /// portable hash Awa uses for queue-storage enqueue shards.
    pub fn queue_for_key(&self, key: impl AsRef<[u8]>) -> &str {
        let shard = shard_for_ordering_key(key.as_ref(), self.width() as i16) as usize;
        &self.physical_queues[shard]
    }

    /// Select a physical queue by caller-supplied sequence number.
    ///
    /// This is useful for bulk producers that want round-robin fanout and do
    /// not need per-key ordering.
    pub fn queue_for_index(&self, index: usize) -> &str {
        &self.physical_queues[index % self.width()]
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

impl AsRef<[String]> for QueueFanout {
    fn as_ref(&self) -> &[String] {
        self.physical_queues()
    }
}

impl<'a> IntoIterator for &'a QueueFanout {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.physical_queues.iter()
    }
}

fn validate_logical_queue(logical_queue: &str) -> Result<(), QueueFanoutError> {
    if logical_queue.is_empty() {
        return Err(QueueFanoutError::EmptyLogicalQueue);
    }
    Ok(())
}

fn validate_physical_count(count: usize) -> Result<(), QueueFanoutError> {
    if count == 0 {
        return Err(QueueFanoutError::ZeroWidth);
    }

    let max = i16::MAX as usize;
    if count > max {
        return Err(QueueFanoutError::TooManyPhysicalQueues { got: count, max });
    }

    Ok(())
}

fn validate_physical_queues(queues: &[String]) -> Result<(), QueueFanoutError> {
    let mut seen = HashSet::with_capacity(queues.len());
    for queue in queues {
        if queue.is_empty() {
            return Err(QueueFanoutError::EmptyPhysicalQueue);
        }
        if !seen.insert(queue.as_str()) {
            return Err(QueueFanoutError::DuplicatePhysicalQueue {
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
        let fanout = QueueFanout::new("email", 1).expect("fanout should build");

        assert_eq!(fanout.logical_queue(), "email");
        assert_eq!(fanout.physical_queues(), &["email".to_string()]);
        assert_eq!(fanout.queue_for_index(42), "email");
    }

    #[test]
    fn default_names_are_stable_for_multiple_queues() {
        let fanout = QueueFanout::new("email", 4).expect("fanout should build");

        assert_eq!(
            fanout.physical_queues(),
            &[
                "email__p0".to_string(),
                "email__p1".to_string(),
                "email__p2".to_string(),
                "email__p3".to_string(),
            ]
        );
        assert_eq!(fanout.queue_for_index(0), "email__p0");
        assert_eq!(fanout.queue_for_index(5), "email__p1");
    }

    #[test]
    fn key_routing_sets_queue_and_ordering_key() {
        let fanout = QueueFanout::new("customer-updates", 4).expect("fanout should build");

        let opts = fanout.route_opts_by_key(InsertOpts::default(), b"customer-42");

        assert_eq!(fanout.queue_for_key(b"customer-42"), "customer-updates__p0");
        assert_eq!(opts.queue, fanout.queue_for_key(b"customer-42"));
        assert_eq!(opts.ordering_key.as_deref(), Some(&b"customer-42"[..]));
    }

    #[test]
    fn explicit_queues_reject_empty_and_duplicate_names() {
        let empty = QueueFanout::from_physical_queues("email", ["email-a", ""]);
        assert!(matches!(empty, Err(QueueFanoutError::EmptyPhysicalQueue)));

        let duplicate = QueueFanout::from_physical_queues("email", ["email-a", "email-a"]);
        assert!(matches!(
            duplicate,
            Err(QueueFanoutError::DuplicatePhysicalQueue { queue }) if queue == "email-a"
        ));
    }
}
