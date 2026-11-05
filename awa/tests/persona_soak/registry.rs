//! The set of queues the run owns and the live facts the oracle needs about each:
//! which persona serves it, how many workers can run it, how many are busy, and
//! whether it is intentionally not being served right now.

use super::config::Persona;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

impl QueueInfo {
    /// Jobs enqueued and not yet started, as far as the producers and workers know.
    pub fn backlog_estimate(&self) -> i64 {
        self.enqueued.load(Ordering::Relaxed) - self.started.load(Ordering::Relaxed)
    }
}

pub struct QueueInfo {
    pub name: String,
    pub persona: Persona,
    pub index: u32,
    /// Workers currently able to run this queue across all live runtimes.
    pub capacity: AtomicI64,
    pub in_flight: AtomicI64,
    /// Jobs producers enqueued here and executions begun, for rough backlog estimates.
    pub enqueued: AtomicI64,
    pub started: AtomicI64,
    /// An operator (or a rate limit) is deliberately holding jobs back.
    pub paused: AtomicBool,
    pub rate_limited: bool,
    /// Milliseconds since run start of the most recent operator drain, 0 if never.
    pub drained_at_ms: AtomicI64,
}

#[derive(Default)]
pub struct QueueRegistry {
    queues: RwLock<Vec<Arc<QueueInfo>>>,
    by_name: RwLock<HashMap<String, u32>>,
}

impl QueueRegistry {
    pub fn register(&self, name: &str, persona: Persona, rate_limited: bool) -> Arc<QueueInfo> {
        let mut queues = self.queues.write().expect("registry lock");
        let mut by_name = self.by_name.write().expect("registry lock");
        if let Some(index) = by_name.get(name) {
            return queues[*index as usize].clone();
        }
        let index = queues.len() as u32;
        let info = Arc::new(QueueInfo {
            name: name.to_string(),
            persona,
            index,
            capacity: AtomicI64::new(0),
            in_flight: AtomicI64::new(0),
            enqueued: AtomicI64::new(0),
            started: AtomicI64::new(0),
            paused: AtomicBool::new(false),
            rate_limited,
            drained_at_ms: AtomicI64::new(0),
        });
        by_name.insert(name.to_string(), index);
        queues.push(info.clone());
        info
    }

    /// Looks a queue up by its physical name; striped queues (`name#N`) resolve to
    /// their logical queue.
    pub fn get(&self, name: &str) -> Option<Arc<QueueInfo>> {
        let by_name = self.by_name.read().expect("registry lock");
        let index = by_name.get(name).or_else(|| {
            let (logical, suffix) = name.rsplit_once('#')?;
            suffix.parse::<u32>().ok()?;
            by_name.get(logical)
        })?;
        Some(self.queues.read().expect("registry lock")[*index as usize].clone())
    }

    pub fn by_index(&self, index: u32) -> Option<Arc<QueueInfo>> {
        self.queues
            .read()
            .expect("registry lock")
            .get(index as usize)
            .cloned()
    }

    pub fn all(&self) -> Vec<Arc<QueueInfo>> {
        self.queues.read().expect("registry lock").clone()
    }

    pub fn of(&self, persona: Persona) -> Vec<Arc<QueueInfo>> {
        self.all()
            .into_iter()
            .filter(|queue| queue.persona == persona)
            .collect()
    }
}

/// Counts one running handler against its queue for as long as the guard lives,
/// including when the runtime is killed and the handler future is dropped.
pub struct InFlightGuard {
    queue: Option<Arc<QueueInfo>>,
}

impl InFlightGuard {
    pub fn new(queue: Option<Arc<QueueInfo>>) -> Self {
        if let Some(queue) = &queue {
            queue.in_flight.fetch_add(1, Ordering::Relaxed);
        }
        Self { queue }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Some(queue) = &self.queue {
            queue.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
