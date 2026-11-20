//! Coalesced queue wake-ups, sent after commit.
//!
//! A transaction that calls `pg_notify` takes PostgreSQL's notify-queue lock
//! at `COMMIT` and holds it until its commit record is flushed, and every
//! notifying transaction in the cluster waits its turn for that lock. A path
//! that commits its own transaction therefore hands its queues' wake-up
//! channels to [`wake_after_commit`] once `COMMIT` has returned. For each
//! pool, this sends at most one `NOTIFY` per channel per [`WINDOW`], one
//! statement at a time, so a burst of commits costs a few notifications and
//! at most one connection. The first wake-up of a channel that has not been
//! woken within the window goes out at once; later ones go out when the
//! window ends. The window is measured from when a send completes, so a slow
//! database does not shorten it.
//!
//! A wake-up is a hint: claimers also poll, so a wake-up lost to an error or
//! to the process exiting delays a claim until the claimer's next poll. A
//! process that is about to exit or close its pool calls [`flush_wakeups`]
//! first, so its last wake-ups are sent rather than lost.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;

/// Shortest gap between two wake-ups of one channel from one pool.
pub(crate) const WINDOW: Duration = Duration::from_millis(50);

/// Wake-up state of one pool. It holds no pool clone: the sender task owns
/// one, so a pool is never kept alive here.
struct PoolWakeups {
    /// Keeps the address used as this state's key from being reused by
    /// another pool, even if this pool's connect options are replaced.
    _options: Arc<PgConnectOptions>,
    pending: BTreeSet<String>,
    last_sent: HashMap<String, Instant>,
    /// Generation of the live sender task, if one is running.
    sender: Option<u64>,
    /// Interrupts the sender's wait for a window to end, so a channel that
    /// has no window running is sent at once.
    arrived: Arc<tokio::sync::Notify>,
    /// Set by [`flush_wakeups`]: send every pending channel without waiting
    /// for its window.
    flush: bool,
    /// The sender is sending channels it has taken from `pending`.
    sending: bool,
    /// Signalled whenever the sender finishes a send or stops, so a flush
    /// can tell when nothing is pending or being sent.
    settled: Arc<tokio::sync::Notify>,
    /// Sends completed, so tests can tell when the sender is waiting out a
    /// window.
    #[cfg(test)]
    sends_completed: u32,
}

fn pools() -> &'static Mutex<HashMap<usize, PoolWakeups>> {
    static POOLS: OnceLock<Mutex<HashMap<usize, PoolWakeups>>> = OnceLock::new();
    POOLS.get_or_init(Default::default)
}

// Every update leaves the state consistent, so a panic elsewhere while the
// lock was held does not make it unusable.
fn lock_pools() -> MutexGuard<'static, HashMap<usize, PoolWakeups>> {
    pools().lock().unwrap_or_else(PoisonError::into_inner)
}

// Clones of a pool share its connect options, so their address identifies
// the pool. The state holds the options, so the address is not reused while
// the state exists.
fn pool_key(options: &Arc<PgConnectOptions>) -> usize {
    Arc::as_ptr(options) as usize
}

/// Wake the claimers listening on `channels` after the caller's commit.
///
/// Returns at once; the notification is sent from `pool` in the background.
/// Outside a Tokio runtime nothing is sent, and claimers find the rows when
/// they next poll.
pub(crate) fn wake_after_commit(pool: &PgPool, channels: impl IntoIterator<Item = String>) {
    let channels: BTreeSet<String> = channels.into_iter().collect();
    if channels.is_empty() {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    static GENERATIONS: AtomicU64 = AtomicU64::new(0);
    let options = pool.connect_options();
    let key = pool_key(&options);
    let spawn = {
        let mut pools = lock_pools();
        let wakeups = pools.entry(key).or_insert_with(|| PoolWakeups {
            _options: options,
            pending: BTreeSet::new(),
            last_sent: HashMap::new(),
            sender: None,
            arrived: Arc::default(),
            flush: false,
            sending: false,
            settled: Arc::default(),
            #[cfg(test)]
            sends_completed: 0,
        });
        // Only a channel outside its window can be sent before the sender's
        // current wait ends.
        let due_now = channels
            .iter()
            .any(|channel| !wakeups.last_sent.contains_key(channel));
        wakeups.pending.extend(channels);
        if wakeups.sender.is_some() {
            if due_now {
                wakeups.arrived.notify_one();
            }
            None
        } else {
            let generation = GENERATIONS.fetch_add(1, Ordering::Relaxed);
            wakeups.sender = Some(generation);
            Some(generation)
        }
    };
    // Spawned outside the lock: a runtime that is shutting down drops the
    // task at once, and its guard takes the lock to clear the state.
    if let Some(generation) = spawn {
        runtime.spawn(send_wakeups(SenderGuard { key, generation }, pool.clone()));
    }
}

/// Clears a pool's wake-up state when its sender task ends without doing so
/// itself, such as when the task's runtime shuts down. It is an argument of
/// the task, so it is dropped even if the task never runs.
struct SenderGuard {
    key: usize,
    generation: u64,
}

impl Drop for SenderGuard {
    fn drop(&mut self) {
        let mut pools = lock_pools();
        if pools
            .get(&self.key)
            .is_some_and(|wakeups| wakeups.sender == Some(self.generation))
        {
            if let Some(wakeups) = pools.remove(&self.key) {
                wakeups.settled.notify_waiters();
            }
        }
    }
}

async fn send_wakeups(guard: SenderGuard, pool: PgPool) {
    loop {
        let (due, wait, arrived) = {
            let mut pools = lock_pools();
            let Some(wakeups) = pools.get_mut(&guard.key) else {
                return;
            };
            let now = Instant::now();
            wakeups
                .last_sent
                .retain(|_, sent| now.duration_since(*sent) < WINDOW);
            if wakeups.pending.is_empty() && wakeups.last_sent.is_empty() {
                // Idle for a whole window.
                if let Some(wakeups) = pools.remove(&guard.key) {
                    wakeups.settled.notify_waiters();
                }
                return;
            }
            let due: Vec<String> = if wakeups.flush {
                std::mem::take(&mut wakeups.pending).into_iter().collect()
            } else {
                let due: Vec<String> = wakeups
                    .pending
                    .iter()
                    .filter(|channel| !wakeups.last_sent.contains_key(*channel))
                    .cloned()
                    .collect();
                for channel in &due {
                    wakeups.pending.remove(channel);
                }
                due
            };
            for channel in &due {
                wakeups.last_sent.insert(channel.clone(), now);
            }
            if wakeups.pending.is_empty() {
                // A flush is over once what it found pending has been sent.
                wakeups.flush = false;
            }
            wakeups.sending = !due.is_empty();
            let wait = wakeups
                .last_sent
                .values()
                .map(|sent| WINDOW.saturating_sub(now.duration_since(*sent)))
                .min()
                .unwrap_or(WINDOW);
            (due, wait, wakeups.arrived.clone())
        };
        if due.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = arrived.notified() => {}
            }
        } else {
            send(&pool, &due).await;
            // Start the window when the send completes.
            let mut pools = lock_pools();
            if let Some(wakeups) = pools.get_mut(&guard.key) {
                let sent = Instant::now();
                for channel in due {
                    wakeups.last_sent.insert(channel, sent);
                }
                wakeups.sending = false;
                wakeups.settled.notify_waiters();
                #[cfg(test)]
                {
                    wakeups.sends_completed += 1;
                }
            }
        }
    }
}

/// Send `pool`'s pending wake-ups now, ignoring the coalescing window, and
/// wait until they and any send in progress have finished, for at most
/// `timeout`.
///
/// Call this before a process exits or closes `pool`, after work that woke
/// queues, such as an enqueue or a retry, so that claimers in other
/// processes are woken now rather than at their next poll. Returns `true` if
/// nothing was left pending or being sent (a send that failed has been
/// logged), and `false` if `timeout` ran out first; claimers then pick the
/// rows up when they next poll.
pub async fn flush_wakeups(pool: &PgPool, timeout: Duration) -> bool {
    let key = pool_key(&pool.connect_options());
    let settled = {
        let mut pools = lock_pools();
        let Some(wakeups) = pools.get_mut(&key) else {
            return true;
        };
        if wakeups.pending.is_empty() && !wakeups.sending {
            return true;
        }
        wakeups.flush = true;
        wakeups.arrived.notify_one();
        wakeups.settled.clone()
    };
    let is_settled = || {
        lock_pools()
            .get(&key)
            .is_none_or(|wakeups| wakeups.pending.is_empty() && !wakeups.sending)
    };
    let flushed = tokio::time::timeout(timeout, async {
        loop {
            let notified = settled.notified();
            tokio::pin!(notified);
            // Registered before the check, so a signal between the two is
            // not missed.
            notified.as_mut().enable();
            if is_settled() {
                return;
            }
            notified.await;
        }
    })
    .await
    .is_ok();
    if !flushed {
        let pending = lock_pools()
            .get(&key)
            .map_or(0, |wakeups| wakeups.pending.len());
        tracing::warn!(
            pending,
            timeout_ms = timeout.as_millis() as u64,
            "Timed out flushing wake-ups; claimers pick the rows up on their next poll"
        );
    }
    flushed
}

async fn send(pool: &PgPool, channels: &[String]) {
    if let Err(err) =
        sqlx::query("SELECT pg_notify(channel, '') FROM unnest($1::text[]) AS channel")
            .bind(channels)
            .execute(pool)
            .await
    {
        tracing::warn!(
            error = %err,
            channels = channels.len(),
            "Failed to wake claimers after commit; they pick the rows up on their next poll"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    fn has_state(key: usize) -> bool {
        lock_pools().contains_key(&key)
    }

    #[tokio::test]
    async fn a_flush_with_nothing_pending_returns_at_once() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://awa@127.0.0.1:1/awa")
            .unwrap();
        assert!(flush_wakeups(&pool, Duration::ZERO).await);
    }

    #[tokio::test]
    async fn a_flush_sends_a_channel_inside_its_window() {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(1))
            .connect_lazy("postgres://awa@127.0.0.1:1/awa")
            .unwrap();
        let key = pool_key(&pool.connect_options());
        wake_after_commit(&pool, ["awa:q".to_string()]);
        while sends_completed(key) != Some(1) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Woken again inside its window: without a flush this waits for the
        // window to end.
        wake_after_commit(&pool, ["awa:q".to_string()]);
        assert!(flush_wakeups(&pool, Duration::from_secs(5)).await);
        assert_eq!(
            sends_completed(key),
            Some(2),
            "the flush returned after the second send"
        );
    }

    #[tokio::test]
    async fn a_flush_that_outlasts_its_timeout_returns_false() {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy("postgres://awa@127.0.0.1:1/awa")
            .unwrap();
        wake_after_commit(&pool, ["awa:q".to_string()]);
        // The sender keeps retrying the refused connection for 5 s.
        assert!(!flush_wakeups(&pool, Duration::from_millis(20)).await);
    }

    fn sends_completed(key: usize) -> Option<u32> {
        lock_pools()
            .get(&key)
            .map(|wakeups| wakeups.sends_completed)
    }

    #[test]
    fn a_runtime_dropped_before_the_sender_runs_leaves_no_state() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let key = runtime.block_on(async {
            let pool = PgPoolOptions::new()
                .connect_lazy("postgres://awa@127.0.0.1:1/awa")
                .unwrap();
            wake_after_commit(&pool, ["awa:q".to_string()]);
            let key = pool_key(&pool.connect_options());
            assert!(has_state(key));
            key
        });
        drop(runtime);
        assert!(
            !has_state(key),
            "the dropped sender cleared its pool's state"
        );
    }

    #[test]
    fn a_runtime_dropped_during_a_send_leaves_no_state() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let key = runtime.block_on(async {
            let pool = PgPoolOptions::new()
                .acquire_timeout(Duration::from_millis(500))
                .connect_lazy("postgres://awa@127.0.0.1:1/awa")
                .unwrap();
            wake_after_commit(&pool, ["awa:q".to_string()]);
            // The sender is still trying to acquire a connection.
            tokio::time::sleep(Duration::from_millis(10)).await;
            let key = pool_key(&pool.connect_options());
            assert_eq!(sends_completed(key), Some(0));
            key
        });
        drop(runtime);
        assert!(
            !has_state(key),
            "the cancelled sender cleared its pool's state"
        );
    }

    #[test]
    fn a_runtime_dropped_inside_the_window_leaves_no_state() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let key = runtime.block_on(async {
            let pool = PgPoolOptions::new()
                .acquire_timeout(Duration::from_millis(1))
                .connect_lazy("postgres://awa@127.0.0.1:1/awa")
                .unwrap();
            wake_after_commit(&pool, ["awa:q".to_string()]);
            let key = pool_key(&pool.connect_options());
            // Once the first send has completed, the sender waits out the
            // channel's window; the runtime is single-threaded, so it is
            // parked there whenever this task runs.
            while sends_completed(key) != Some(1) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            wake_after_commit(&pool, ["awa:q".to_string()]);
            assert_eq!(sends_completed(key), Some(1));
            key
        });
        drop(runtime);
        assert!(
            !has_state(key),
            "the cancelled sender cleared its pool's state"
        );
    }
}
