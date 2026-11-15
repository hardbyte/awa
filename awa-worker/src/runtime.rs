use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use awa_model::queue_storage::{ClaimHeartbeatRef, ClaimedEntry, RowClaimHeartbeatRef};

const IN_FLIGHT_SHARDS: usize = 64;

pub(crate) type RunLease = i64;
pub(crate) type InFlightKey = (i64, RunLease);
pub(crate) type InFlightMap = Arc<InFlightRegistry>;
type CancelFlag = Arc<AtomicBool>;
type InFlightShard = RwLock<HashMap<InFlightKey, InFlightState>>;
type CancelledShard = Mutex<HashMap<InFlightKey, Instant>>;

/// How long an operator cancel for an attempt that is not registered here is
/// kept for it. The cancel names an attempt whose claim has committed, so the
/// only attempt it can still meet here is one between claim commit and
/// registration; that gap is bounded by pool waits after the claim.
const UNREGISTERED_CANCEL_TTL: Duration = Duration::from_secs(300);
/// Every runtime hears every cancel in the fleet, nearly all for attempts it
/// does not run, so the kept set is bounded per shard as well as by age.
const UNREGISTERED_CANCELS_PER_SHARD: usize = 256;

/// Per-job in-flight state: cancellation flag + progress buffer.
pub(crate) struct InFlightState {
    pub cancel: CancelFlag,
    pub progress: Arc<std::sync::Mutex<ProgressState>>,
    /// Receipt claim identity, when the attempt was dispatched from queue
    /// storage with one; lets the heartbeat address the claim's slots
    /// directly.
    pub claim: Option<ClaimRef>,
    /// The handler has returned and the attempt waits for its finalize. It is
    /// still heartbeated, but its progress is settled by the finalize.
    pub finalizing: bool,
}

/// How the heartbeat addresses a receipt claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimRef {
    /// A zero-deadline claim, by its claim batch.
    Compact(ClaimHeartbeatRef),
    /// A deadline-backed claim, by its claim and ready slots.
    Row(RowClaimHeartbeatRef),
}

impl ClaimRef {
    pub(crate) fn from_claim(job_id: i64, run_lease: i64, claim: &ClaimedEntry) -> Option<Self> {
        ClaimHeartbeatRef::from_claim(job_id, run_lease, claim)
            .map(Self::Compact)
            .or_else(|| RowClaimHeartbeatRef::from_claim(job_id, run_lease, claim).map(Self::Row))
    }
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

/// Lock a progress buffer, recovering the guard if a handler panicked while
/// holding it. `ProgressState` is plain data with no cross-field invariant a
/// mid-update panic can leave half-applied, and the panic itself is reported
/// through the handler outcome, so a poisoned buffer must not also take down
/// the heartbeat or settle path.
pub(crate) fn lock_progress(
    progress: &std::sync::Mutex<ProgressState>,
) -> std::sync::MutexGuard<'_, ProgressState> {
    progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    /// Operator cancels that arrived before their attempt registered, by
    /// attempt, with when they arrived. Same sharding as `shards`; a shard's
    /// entry here is only read or written under that shard's write lock.
    cancelled: Box<[CancelledShard]>,
}

impl Default for InFlightRegistry {
    fn default() -> Self {
        let mut shards = Vec::with_capacity(IN_FLIGHT_SHARDS);
        let mut cancelled = Vec::with_capacity(IN_FLIGHT_SHARDS);
        for _ in 0..IN_FLIGHT_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
            cancelled.push(Mutex::new(HashMap::new()));
        }
        Self {
            shards: shards.into_boxed_slice(),
            cancelled: cancelled.into_boxed_slice(),
        }
    }
}

impl InFlightRegistry {
    /// Registers an attempt. Returns true when an operator cancel for it
    /// arrived before it registered; its cancel flag is then already set.
    pub fn insert(&self, key: InFlightKey, state: InFlightState) -> bool {
        let index = self.shard_index(&key);
        let mut guard = self.shards[index]
            .write()
            .expect("in_flight shard poisoned");
        let cancelled = self.cancelled[index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key)
            .is_some_and(|at| at.elapsed() <= UNREGISTERED_CANCEL_TTL);
        if cancelled {
            state
                .cancel
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        guard.insert(key, state);
        cancelled
    }

    /// Delivers an operator cancel. A registered attempt has its cancel flag
    /// set; otherwise the cancel is kept so the attempt sees it if it
    /// registers later. Returns whether the attempt was registered.
    pub fn signal_cancel(&self, key: InFlightKey) -> bool {
        let index = self.shard_index(&key);
        // The write lock orders this against `insert`: the attempt either
        // registered first and is flagged here, or registers afterwards and
        // finds the kept cancel.
        let guard = self.shards[index]
            .write()
            .expect("in_flight shard poisoned");
        if let Some(state) = guard.get(&key) {
            state
                .cancel
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return true;
        }
        let mut cancelled = self.cancelled[index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        cancelled.retain(|_, at| now.duration_since(*at) <= UNREGISTERED_CANCEL_TTL);
        if cancelled.len() >= UNREGISTERED_CANCELS_PER_SHARD {
            if let Some(oldest) = cancelled
                .iter()
                .min_by_key(|(_, at)| **at)
                .map(|(key, _)| *key)
            {
                cancelled.remove(&oldest);
            }
        }
        cancelled.insert(key, now);
        false
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

    /// Every attempt to heartbeat, with its receipt claim identity, if any.
    pub fn claim_refs(&self) -> Vec<(InFlightKey, Option<ClaimRef>)> {
        let mut refs = Vec::new();
        for shard in &*self.shards {
            let guard = shard.read().expect("in_flight shard poisoned");
            refs.extend(guard.iter().map(|(key, state)| (*key, state.claim)));
        }
        refs
    }

    /// The handler has returned: keep heartbeating the attempt until its
    /// finalize is done, and stop flushing its progress.
    pub fn mark_finalizing(&self, key: InFlightKey) {
        let mut guard = self
            .shard_for(&key)
            .write()
            .expect("in_flight shard poisoned");
        if let Some(state) = guard.get_mut(&key) {
            state.finalizing = true;
        }
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
                if state.finalizing {
                    continue;
                }
                let mut progress = lock_progress(&state.progress);
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
                let mut progress = lock_progress(&state.progress);
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
                let mut progress = lock_progress(&state.progress);
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
        &self.shards[self.shard_index(key)]
    }

    fn shard_index(&self, key: &InFlightKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
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
                claim: None,
                finalizing: false,
            },
        );

        registry.ack_progress(&[(key.0, key.1, 1)]);

        let state = progress.lock().expect("progress lock poisoned");
        assert_eq!(state.acked_generation, 2);
        assert!(state.in_flight.is_none());
    }

    fn idle_state() -> InFlightState {
        InFlightState {
            cancel: Arc::new(AtomicBool::new(false)),
            progress: Arc::new(std::sync::Mutex::new(ProgressState::new(None))),
            claim: None,
            finalizing: false,
        }
    }

    #[test]
    fn cancel_before_registration_reaches_only_that_attempt() {
        let registry = InFlightRegistry::default();
        assert!(!registry.signal_cancel((7, 3)));

        let other = idle_state();
        let other_flag = other.cancel.clone();
        assert!(!registry.insert((7, 4), other));
        assert!(!other_flag.load(std::sync::atomic::Ordering::SeqCst));

        let cancelled = idle_state();
        let flag = cancelled.cancel.clone();
        assert!(registry.insert((7, 3), cancelled));
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));

        // The kept cancel is consumed by its registration.
        registry.remove((7, 3));
        assert!(!registry.insert((7, 3), idle_state()));
    }

    #[test]
    fn cancel_after_registration_sets_the_flag() {
        let registry = InFlightRegistry::default();
        let state = idle_state();
        let flag = state.cancel.clone();
        assert!(!registry.insert((9, 1), state));
        assert!(registry.signal_cancel((9, 1)));
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn kept_cancels_are_bounded_per_shard() {
        let registry = InFlightRegistry::default();
        for job_id in 0..(IN_FLIGHT_SHARDS * UNREGISTERED_CANCELS_PER_SHARD * 4) as i64 {
            registry.signal_cancel((job_id, 1));
        }
        for shard in &*registry.cancelled {
            assert!(shard.lock().unwrap().len() <= UNREGISTERED_CANCELS_PER_SHARD);
        }
        // The newest cancel is still kept.
        let newest = (IN_FLIGHT_SHARDS * UNREGISTERED_CANCELS_PER_SHARD * 4) as i64 - 1;
        assert!(registry.insert((newest, 1), idle_state()));
    }
}
