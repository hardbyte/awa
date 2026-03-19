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
type InFlightShard = RwLock<HashMap<InFlightKey, InFlightState>>;

/// Per-job in-flight state: cancellation flag + progress buffer.
pub(crate) struct InFlightState {
    pub cancel: CancelFlag,
    pub progress: Arc<std::sync::Mutex<ProgressState>>,
}

/// Mutable progress buffer shared between handler and heartbeat service.
#[derive(Debug)]
pub struct ProgressState {
    /// Latest assembled progress value visible to handler code.
    pub latest: Option<serde_json::Value>,
    /// Generation counter — bumped on each handler-side mutation.
    pub generation: u64,
    /// Highest generation durably acknowledged by Postgres.
    pub acked_generation: u64,
    /// Snapshot currently being flushed by heartbeat, if any.
    pub in_flight: Option<(u64, serde_json::Value)>,
}

impl ProgressState {
    pub fn new(initial: Option<serde_json::Value>) -> Self {
        Self {
            latest: initial,
            generation: 0,
            acked_generation: 0,
            in_flight: None,
        }
    }

    /// Whether there is a pending update that has not been acked.
    pub fn has_pending(&self) -> bool {
        self.generation > self.acked_generation
    }
}

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
    pub fn insert(&self, key: InFlightKey, state: InFlightState) {
        let mut guard = self
            .shard_for(&key)
            .write()
            .expect("in_flight shard poisoned");
        guard.insert(key, state);
    }

    pub fn remove(&self, key: InFlightKey) -> Option<InFlightState> {
        let mut guard = self
            .shard_for(&key)
            .write()
            .expect("in_flight shard poisoned");
        guard.remove(&key)
    }

    pub fn get_cancel(&self, key: InFlightKey) -> Option<CancelFlag> {
        let guard = self
            .shard_for(&key)
            .read()
            .expect("in_flight shard poisoned");
        guard.get(&key).map(|s| s.cancel.clone())
    }

    #[allow(dead_code)]
    pub fn get_progress(&self, key: InFlightKey) -> Option<Arc<std::sync::Mutex<ProgressState>>> {
        let guard = self
            .shard_for(&key)
            .read()
            .expect("in_flight shard poisoned");
        guard.get(&key).map(|s| s.progress.clone())
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
            flags.extend(guard.values().map(|s| s.cancel.clone()));
        }
        flags
    }

    /// Collect pending progress snapshots for heartbeat flush.
    ///
    /// For each in-flight job with `generation > acked_generation` and no
    /// in-flight snapshot already pending, snapshots `latest` into `in_flight`.
    /// Returns `(job_id, run_lease, generation, progress_json)` tuples.
    pub fn snapshot_pending_progress(&self) -> Vec<(i64, i64, u64, serde_json::Value)> {
        let mut result = Vec::new();
        for shard in &*self.shards {
            let guard = shard.read().expect("in_flight shard poisoned");
            for (&(job_id, run_lease), state) in guard.iter() {
                let mut progress = state.progress.lock().expect("progress lock poisoned");
                if progress.has_pending() && progress.in_flight.is_none() {
                    if let Some(ref value) = progress.latest {
                        let gen = progress.generation;
                        let snapshot = value.clone();
                        progress.in_flight = Some((gen, snapshot.clone()));
                        result.push((job_id, run_lease, gen, snapshot));
                    }
                }
            }
        }
        result
    }

    /// Acknowledge a successful progress flush for a set of jobs.
    pub fn ack_progress(&self, acked: &[(i64, i64, u64)]) {
        for &(job_id, run_lease, generation) in acked {
            let key = (job_id, run_lease);
            let guard = self
                .shard_for(&key)
                .read()
                .expect("in_flight shard poisoned");
            if let Some(state) = guard.get(&key) {
                let mut progress = state.progress.lock().expect("progress lock poisoned");
                if progress
                    .in_flight
                    .as_ref()
                    .is_some_and(|(gen, _)| *gen == generation)
                {
                    progress.acked_generation = generation;
                    progress.in_flight = None;
                }
            }
        }
    }

    /// Clear in-flight snapshot on failed flush (without advancing acked).
    pub fn clear_in_flight_progress(&self, failed: &[(i64, i64, u64)]) {
        for &(job_id, run_lease, generation) in failed {
            let key = (job_id, run_lease);
            let guard = self
                .shard_for(&key)
                .read()
                .expect("in_flight shard poisoned");
            if let Some(state) = guard.get(&key) {
                let mut progress = state.progress.lock().expect("progress lock poisoned");
                if progress
                    .in_flight
                    .as_ref()
                    .is_some_and(|(gen, _)| *gen == generation)
                {
                    progress.in_flight = None;
                }
            }
        }
    }

    fn shard_for(&self, key: &InFlightKey) -> &InFlightShard {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.shards.len();
        &self.shards[idx]
    }
}
