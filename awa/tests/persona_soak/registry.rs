//! The set of queues the run owns and the live facts the oracle needs about each:
//! which persona serves it, how many workers can run it, how many are busy, and
//! whether it is intentionally not being served right now.

use super::config::Persona;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

impl QueueInfo {
    pub fn hold(&self) {
        self.pause_holds.fetch_add(1, Ordering::Relaxed);
    }

    pub fn release(&self) {
        self.pause_holds.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn is_held(&self) -> bool {
        self.pause_holds.load(Ordering::Relaxed) > 0
    }

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
    /// Operators currently holding the queue paused; the stall check skips it while non-zero.
    pub pause_holds: AtomicI32,
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
            pause_holds: AtomicI32::new(0),
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
        let index = {
            let by_name = self.by_name.read().expect("registry lock");
            *by_name.get(name).or_else(|| {
                let (logical, suffix) = name.rsplit_once('#')?;
                suffix.parse::<u32>().ok()?;
                by_name.get(logical)
            })?
        };
        Some(self.queues.read().expect("registry lock")[index as usize].clone())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci_timing::scaled_timeout;
    use std::sync::{mpsc, Barrier};
    use std::time::{Duration, Instant};

    #[test]
    fn concurrent_registration_and_striped_lookup_complete() {
        let registry = Arc::new(QueueRegistry::default());
        let seed = registry.register("seed", Persona::Mia, true);
        assert!(Arc::ptr_eq(&seed, &registry.get("seed#7").unwrap()));
        assert!(registry.get("seed#bad").is_none());
        assert!(registry.get("missing").is_none());
        let duplicate = registry.register("seed", Persona::Tom, false);
        assert!(Arc::ptr_eq(&seed, &duplicate));
        assert!(duplicate.persona == Persona::Mia && duplicate.rate_limited);
        let barrier = Arc::new(Barrier::new(9));
        let (done, completed) = mpsc::channel();
        let mut threads = Vec::new();
        for worker in 0..8 {
            let registry = registry.clone();
            let seed = seed.clone();
            let barrier = barrier.clone();
            let done = done.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                if worker < 4 {
                    for number in 0..2_000 {
                        let name = format!("queue-{worker}-{number}");
                        let queue = registry.register(&name, Persona::Tom, false);
                        assert!(Arc::ptr_eq(&queue, &registry.get(&name).unwrap()));
                        assert!(Arc::ptr_eq(
                            &queue,
                            &registry.get(&format!("{name}#2")).unwrap()
                        ));
                        assert!(Arc::ptr_eq(
                            &queue,
                            &registry.by_index(queue.index).unwrap()
                        ));
                    }
                } else {
                    for number in 0..20_000 {
                        assert!(Arc::ptr_eq(&seed, &registry.get("seed#3").unwrap()));
                        let name = format!("queue-{}-{}", number % 4, number % 2_000);
                        if let Some(queue) = registry.get(&name) {
                            assert_eq!(queue.name, name);
                            assert!(Arc::ptr_eq(
                                &queue,
                                &registry.by_index(queue.index).unwrap()
                            ));
                        }
                    }
                }
                done.send(()).unwrap();
            }));
        }
        drop(done);
        barrier.wait();
        let deadline = Instant::now() + scaled_timeout(Duration::from_secs(2));
        for _ in 0..8 {
            completed
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("concurrent register/get must complete before the deadline");
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(registry.all().len(), 8_001);
    }
}
