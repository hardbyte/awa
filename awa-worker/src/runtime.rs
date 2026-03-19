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
    pub(crate) latest: Option<serde_json::Value>,
    /// Generation counter — bumped on each handler-side mutation.
    pub(crate) generation: u64,
    /// Highest generation durably acknowledged by Postgres.
    pub(crate) acked_generation: u64,
    /// Snapshot currently being flushed by heartbeat, if any.
    pub(crate) in_flight: Option<(u64, serde_json::Value)>,
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

    /// Get a reference to the latest progress value.
    pub fn latest(&self) -> Option<&serde_json::Value> {
        self.latest.as_ref()
    }

    /// Clone the latest progress value.
    pub fn clone_latest(&self) -> Option<serde_json::Value> {
        self.latest.clone()
    }

    /// Set progress: percent (clamped 0-100), optional message.
    /// Preserves existing metadata sub-object.
    pub fn set_progress(&mut self, percent: u8, message: Option<&str>) {
        let percent = percent.min(100);
        let existing_metadata = self
            .latest
            .as_ref()
            .and_then(|v| v.get("metadata"))
            .cloned();

        let mut value = serde_json::json!({ "percent": percent });
        if let Some(msg) = message {
            value["message"] = serde_json::Value::String(msg.to_string());
        }
        if let Some(meta) = existing_metadata {
            value["metadata"] = meta;
        }
        self.latest = Some(value);
        self.generation += 1;
    }

    /// Shallow-merge keys into the `metadata` sub-object.
    /// Returns false if the existing metadata sub-object is a non-object type (no-op).
    pub fn merge_metadata(&mut self, updates: &serde_json::Map<String, serde_json::Value>) -> bool {
        let progress = self.latest.get_or_insert_with(|| serde_json::json!({}));
        let metadata = progress
            .as_object_mut()
            .expect("progress is always an object")
            .entry("metadata")
            .or_insert_with(|| serde_json::json!({}));

        if let Some(meta_obj) = metadata.as_object_mut() {
            for (k, v) in updates {
                meta_obj.insert(k.clone(), v.clone());
            }
            self.generation += 1;
            true
        } else {
            false
        }
    }

    /// Snapshot for flush: returns (value, generation) if there is a pending update.
    pub fn pending_snapshot(&self) -> Option<(serde_json::Value, u64)> {
        if self.acked_generation >= self.generation {
            return None;
        }
        self.latest.as_ref().map(|v| (v.clone(), self.generation))
    }

    /// Advance acked_generation after a successful flush.
    /// Only advances if `generation` is newer than current acked.
    pub fn ack(&mut self, generation: u64) {
        if generation > self.acked_generation {
            self.acked_generation = generation;
        }
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
                    progress.acked_generation = progress.acked_generation.max(generation);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_progress_does_not_roll_back_newer_explicit_flush() {
        let registry = InFlightRegistry::default();
        let key = (42, 7);
        let progress = Arc::new(std::sync::Mutex::new(ProgressState {
            latest: Some(serde_json::json!({"percent": 90})),
            generation: 2,
            acked_generation: 2,
            in_flight: Some((1, serde_json::json!({"percent": 50}))),
        }));

        registry.insert(
            key,
            InFlightState {
                cancel: Arc::new(AtomicBool::new(false)),
                progress: progress.clone(),
            },
        );

        registry.ack_progress(&[(key.0, key.1, 1)]);

        let state = progress.lock().expect("progress lock poisoned");
        assert_eq!(state.acked_generation, 2);
        assert!(state.in_flight.is_none());
    }
}
