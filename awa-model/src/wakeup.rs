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
//! window ends.
//!
//! A wake-up is a hint: claimers also poll, so a wake-up lost to an error or
//! to the process exiting delays a claim by at most one poll interval.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use sqlx::PgPool;

/// Shortest gap between two wake-ups of one channel from one pool.
pub(crate) const WINDOW: Duration = Duration::from_millis(50);

#[derive(Default)]
struct PoolWakeups {
    pending: BTreeSet<String>,
    last_sent: HashMap<String, Instant>,
    pool: Option<PgPool>,
    sender: Option<tokio::task::JoinHandle<()>>,
}

fn pools() -> &'static Mutex<HashMap<usize, PoolWakeups>> {
    static POOLS: OnceLock<Mutex<HashMap<usize, PoolWakeups>>> = OnceLock::new();
    POOLS.get_or_init(Default::default)
}

// Clones of a pool share its connect options, so their address identifies
// the pool for as long as it has wake-up state here.
fn pool_key(pool: &PgPool) -> usize {
    Arc::as_ptr(&pool.connect_options()) as usize
}

/// Wake the claimers listening on `channels` after the caller's commit.
///
/// Returns at once; the notification is sent from `pool` in the background.
/// Outside a Tokio runtime nothing is sent, and claimers find the rows when
/// they next poll.
pub(crate) fn wake_after_commit(pool: &PgPool, channels: impl IntoIterator<Item = String>) {
    let mut channels = channels.into_iter().peekable();
    if channels.peek().is_none() {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let key = pool_key(pool);
    let mut pools = pools().lock().expect("wake-up state poisoned");
    let wakeups = pools.entry(key).or_default();
    wakeups.pending.extend(channels);
    wakeups.pool = Some(pool.clone());
    // A sender whose runtime has shut down reports finished; start another.
    if wakeups
        .sender
        .as_ref()
        .is_none_or(|sender| sender.is_finished())
    {
        wakeups.sender = Some(runtime.spawn(send_wakeups(key)));
    }
}

async fn send_wakeups(key: usize) {
    loop {
        let (due, pool, wait) = {
            let mut pools = pools().lock().expect("wake-up state poisoned");
            let Some(wakeups) = pools.get_mut(&key) else {
                return;
            };
            let now = Instant::now();
            wakeups
                .last_sent
                .retain(|_, sent| now.duration_since(*sent) < WINDOW);
            if wakeups.pending.is_empty() && wakeups.last_sent.is_empty() {
                // Idle for a whole window: drop the state, and with it the
                // pool clone, so a closed pool is not kept alive.
                pools.remove(&key);
                return;
            }
            let due: Vec<String> = wakeups
                .pending
                .iter()
                .filter(|channel| !wakeups.last_sent.contains_key(*channel))
                .cloned()
                .collect();
            for channel in &due {
                wakeups.pending.remove(channel);
                wakeups.last_sent.insert(channel.clone(), now);
            }
            let wait = wakeups
                .last_sent
                .values()
                .map(|sent| WINDOW.saturating_sub(now.duration_since(*sent)))
                .min()
                .unwrap_or(WINDOW);
            (due, wakeups.pool.clone(), wait)
        };
        match pool {
            Some(pool) if !due.is_empty() => send(&pool, &due).await,
            _ => tokio::time::sleep(wait).await,
        }
    }
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
