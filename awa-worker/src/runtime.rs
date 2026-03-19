use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::RwLock;

const IN_FLIGHT_SHARDS: usize = 64;

pub(crate) type RunLease = i64;
pub(crate) type InFlightKey = (i64, RunLease);
pub(crate) type InFlightMap = Arc<InFlightRegistry>;
type CancelFlag = Arc<AtomicBool>;
type InFlightShard = RwLock<HashMap<InFlightKey, CancelFlag>>;

pub(crate) struct InFlightRegistry {
    shards: Box<[InFlightShard]>,
}

impl Default for InFlightRegistry {
    fn default() -> Self {
        let mut shards = Vec::with_capacity(IN_FLIGHT_SHARDS);
        for _ in 0..IN_FLIGHT_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self {
            shards: shards.into_boxed_slice(),
        }
    }
}

impl InFlightRegistry {
    pub fn insert(&self, key: InFlightKey, cancel: CancelFlag) {
        let mut guard = self
            .shard_for(&key)
            .write()
            .expect("in_flight shard poisoned");
        guard.insert(key, cancel);
    }

    pub fn remove(&self, key: InFlightKey) -> Option<CancelFlag> {
        let mut guard = self
            .shard_for(&key)
            .write()
            .expect("in_flight shard poisoned");
        guard.remove(&key)
    }

    pub fn get(&self, key: InFlightKey) -> Option<CancelFlag> {
        let guard = self
            .shard_for(&key)
            .read()
            .expect("in_flight shard poisoned");
        guard.get(&key).cloned()
    }

    pub fn keys(&self) -> Vec<InFlightKey> {
        let mut keys = Vec::new();
        for shard in &*self.shards {
            let guard = shard.read().expect("in_flight shard poisoned");
            keys.extend(guard.keys().copied());
        }
        keys
    }

    pub fn flags(&self) -> Vec<CancelFlag> {
        let mut flags = Vec::new();
        for shard in &*self.shards {
            let guard = shard.read().expect("in_flight shard poisoned");
            flags.extend(guard.values().cloned());
        }
        flags
    }

    fn shard_for(&self, key: &InFlightKey) -> &InFlightShard {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.shards.len();
        &self.shards[idx]
    }
}
