//! The personas. Each one owns its queues, job kinds, runtimes and producers, and
//! reports what it did through [`Shared::stats`].

pub mod abandon;
pub mod alex;
pub mod chaos;
pub mod fanout;
pub mod mia;
pub mod priya;
pub mod python;
pub mod sam;
pub mod shardops;
pub mod tom;
pub mod ui;

use super::config::Persona;
use super::ledger::{expect, JobRecord};
use super::shared::Shared;
use awa::{ClientBuilder, JobRow, QueueConfig};
use std::sync::Arc;
use std::time::Duration;

/// Registers the lifecycle event hook for each kind so the ledger learns about
/// terminal outcomes as they commit.
pub fn attach_events(
    mut builder: ClientBuilder,
    shared: &Arc<Shared>,
    kinds: &[&str],
) -> ClientBuilder {
    for kind in kinds {
        let shared = shared.clone();
        builder = builder.on_event_kind(*kind, move |event| {
            let shared = shared.clone();
            async move { shared.on_event(&event) }
        });
    }
    builder
}

/// Runtime timings shared by every fleet: fast enough that rescues, promotion and
/// rotation happen many times in a short run, but with realistic ratios between them.
pub fn tune(builder: ClientBuilder, shared: &Shared) -> ClientBuilder {
    // The maintenance leader evaluates only the schedules its own runtime declares, and
    // any runtime can lead, so every fleet declares the same set.
    let builder = if shared.config.has(Persona::Tom) {
        tom::declare_cron(builder)
    } else {
        builder
    };
    let builder = timings(builder);
    match shared.config.rescue_interval {
        Some(interval) => builder
            .heartbeat_rescue_interval(interval)
            .deadline_rescue_interval(interval),
        None => builder,
    }
}

/// The timings of [`tune`] that do not depend on the run, for runtimes that live in a
/// process of their own.
pub fn timings(builder: ClientBuilder) -> ClientBuilder {
    builder
        .heartbeat_interval(Duration::from_secs(2))
        .heartbeat_staleness(Duration::from_secs(HEARTBEAT_STALENESS_SECS))
        .heartbeat_rescue_interval(Duration::from_secs(3))
        .deadline_rescue_interval(Duration::from_secs(2))
        .promote_interval(Duration::from_millis(250))
        .runtime_snapshot_interval(Duration::from_secs(5))
        .dlq_enabled_by_default(true)
}

/// Seconds without a heartbeat after which a claim is eligible for rescue.
pub const HEARTBEAT_STALENESS_SECS: u64 = 12;

pub fn queue(max_workers: u32) -> QueueConfig {
    QueueConfig {
        max_workers,
        ..QueueConfig::default()
    }
}

/// Records freshly enqueued rows in the ledger.
pub fn register_rows(
    shared: &Shared,
    persona: Persona,
    rows: &[JobRow],
    expect_for: impl Fn(usize) -> u8,
    flags: u32,
) {
    let now = shared.now_ms() as u32;
    for (index, row) in rows.iter().enumerate() {
        let queue = shared
            .queues
            .get(&row.queue)
            .map_or(u32::MAX, |queue| queue.index);
        shared.ledger.register(
            row.id,
            JobRecord::new(persona, queue, now, expect_for(index), flags),
        );
    }
}

pub const ANY: u8 = expect::ANY_TERMINAL;

/// Short pause used by producers after an error, so a dead connection does not spin.
pub async fn backoff(shared: &Shared, error: impl std::fmt::Display, persona: Persona) {
    shared.stats(persona).add("enqueue_errors", 1);
    if shared.stats(persona).get("enqueue_errors") <= 5 {
        shared.note(persona.name(), format!("enqueue error: {error}"));
    }
    shared.load_stop.sleep(Duration::from_millis(500)).await;
}
