use awa_model::{VacuumAwareConfig, VacuumAwareStore};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct VacuumAwareRuntime {
    pub store: Arc<VacuumAwareStore>,
    pub queue_rotate_interval: Duration,
    pub lease_rotate_interval: Duration,
}

impl VacuumAwareRuntime {
    pub fn new(
        config: VacuumAwareConfig,
        queue_rotate_interval: Duration,
        lease_rotate_interval: Duration,
    ) -> Result<Self, awa_model::AwaError> {
        Ok(Self {
            store: Arc::new(VacuumAwareStore::new(config)?),
            queue_rotate_interval,
            lease_rotate_interval,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) enum RuntimeStorage {
    #[default]
    Canonical,
    VacuumAware(VacuumAwareRuntime),
}

impl RuntimeStorage {
    pub fn vacuum_aware(&self) -> Option<&VacuumAwareRuntime> {
        match self {
            RuntimeStorage::Canonical => None,
            RuntimeStorage::VacuumAware(runtime) => Some(runtime),
        }
    }

    pub fn vacuum_aware_store(&self) -> Option<Arc<VacuumAwareStore>> {
        self.vacuum_aware().map(|runtime| runtime.store.clone())
    }
}
