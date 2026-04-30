use awa_model::{QueueStorage, QueueStorageConfig};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct QueueStorageRuntime {
    pub store: Arc<QueueStorage>,
    pub queue_rotate_interval: Duration,
    pub lease_rotate_interval: Duration,
    pub claim_rotate_interval: Duration,
}

impl QueueStorageRuntime {
    pub fn new(
        config: QueueStorageConfig,
        queue_rotate_interval: Duration,
        lease_rotate_interval: Duration,
    ) -> Result<Self, awa_model::AwaError> {
        // ADR-023 claim-ring rotation defaults to the queue-rotate cadence so
        // claim partitions age out roughly in step with the ready / done
        // partitions they reference. Override via
        // `ClientBuilder::claim_rotate_interval`.
        let claim_rotate_interval = queue_rotate_interval;
        Ok(Self {
            store: Arc::new(QueueStorage::new(config)?),
            queue_rotate_interval,
            lease_rotate_interval,
            claim_rotate_interval,
        })
    }

    pub fn with_claim_rotate_interval(mut self, claim_rotate_interval: Duration) -> Self {
        self.claim_rotate_interval = claim_rotate_interval;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) enum RuntimeStorage {
    #[default]
    Canonical,
    QueueStorage(QueueStorageRuntime),
}

impl RuntimeStorage {
    pub fn queue_storage(&self) -> Option<&QueueStorageRuntime> {
        match self {
            RuntimeStorage::Canonical => None,
            RuntimeStorage::QueueStorage(runtime) => Some(runtime),
        }
    }

    pub fn queue_storage_store(&self) -> Option<Arc<QueueStorage>> {
        self.queue_storage().map(|runtime| runtime.store.clone())
    }
}
