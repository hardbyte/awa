use awa_model::{QueueStorage, QueueStorageConfig};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct QueueStorageRuntime {
    pub store: Arc<QueueStorage>,
    pub queue_rotate_interval: Duration,
    pub lease_rotate_interval: Duration,
}

impl QueueStorageRuntime {
    pub fn new(
        config: QueueStorageConfig,
        queue_rotate_interval: Duration,
        lease_rotate_interval: Duration,
    ) -> Result<Self, awa_model::AwaError> {
        Ok(Self {
            store: Arc::new(QueueStorage::new(config)?),
            queue_rotate_interval,
            lease_rotate_interval,
        })
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
