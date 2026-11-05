//! State shared by every persona, the oracle and the report.

use super::config::{Persona, SoakConfig};
use super::db::{Budget, ServerInfo};
use super::fleet::Fleet;
use super::ledger::{expect, flag, JobRecord, Ledger};
use super::registry::{InFlightGuard, QueueRegistry};
use super::rng::percentile;
use super::stop::Stop;
use awa::{JobRow, UntypedJobEvent};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Serialize)]
pub struct TimelineEvent {
    pub at_ms: i64,
    pub source: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    pub kind: String,
    pub at_ms: i64,
    pub during_chaos: bool,
    pub summary: String,
    /// The violating rows, as JSON text.
    pub rows: Vec<String>,
}

#[derive(Default)]
pub struct Stats {
    counters: Mutex<BTreeMap<String, i64>>,
    samples: Mutex<BTreeMap<String, Vec<i64>>>,
}

impl Stats {
    pub fn add(&self, name: &str, amount: i64) {
        *self
            .counters
            .lock()
            .expect("stats")
            .entry(name.to_string())
            .or_insert(0) += amount;
    }

    pub fn set_max(&self, name: &str, value: i64) {
        let mut counters = self.counters.lock().expect("stats");
        let slot = counters.entry(name.to_string()).or_insert(i64::MIN);
        *slot = (*slot).max(value);
    }

    pub fn get(&self, name: &str) -> i64 {
        self.counters
            .lock()
            .expect("stats")
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    /// Records a latency-style sample; kept bounded so long runs stay small.
    pub fn observe(&self, name: &str, value: i64) {
        let mut samples = self.samples.lock().expect("stats");
        let list = samples.entry(name.to_string()).or_default();
        if list.len() < 400_000 {
            list.push(value);
        }
    }

    pub fn counters(&self) -> BTreeMap<String, i64> {
        self.counters.lock().expect("stats").clone()
    }

    pub fn summaries(&self) -> BTreeMap<String, SampleSummary> {
        self.samples
            .lock()
            .expect("stats")
            .iter()
            .map(|(name, values)| {
                let mut sorted = values.clone();
                sorted.sort_unstable();
                (
                    name.clone(),
                    SampleSummary {
                        count: sorted.len() as i64,
                        p50: percentile(&sorted, 0.5),
                        p99: percentile(&sorted, 0.99),
                        max: sorted.last().copied().unwrap_or(0),
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SampleSummary {
    pub count: i64,
    pub p50: i64,
    pub p99: i64,
    pub max: i64,
}

pub struct Shared {
    pub config: SoakConfig,
    pub info: ServerInfo,
    pub started: Instant,
    pub ledger: Ledger,
    pub queues: QueueRegistry,
    pub budget: Budget,
    pub load_stop: Stop,
    pub drain_stop: Stop,
    pub timeline: Mutex<Vec<TimelineEvent>>,
    pub violations: Mutex<Vec<Violation>>,
    pub stats: HashMap<Persona, Stats>,
    /// Run-wide measurements (resource use, oracle probes).
    pub run: Stats,
    pub fleets: Mutex<HashMap<String, Arc<Fleet>>>,
    chaos_until_ms: AtomicI64,
}

impl Shared {
    pub fn new(config: SoakConfig, info: ServerInfo, budget: Budget) -> Arc<Self> {
        let stats = Persona::ALL
            .into_iter()
            .map(|persona| (persona, Stats::default()))
            .collect();
        Arc::new(Self {
            config,
            info,
            started: Instant::now(),
            ledger: Ledger::new(),
            queues: QueueRegistry::default(),
            budget,
            load_stop: Stop::new(),
            drain_stop: Stop::new(),
            timeline: Mutex::new(Vec::new()),
            violations: Mutex::new(Vec::new()),
            stats,
            run: Stats::default(),
            fleets: Mutex::new(HashMap::new()),
            chaos_until_ms: AtomicI64::new(0),
        })
    }

    pub fn started_load(&self, _at: Instant) {}

    pub fn now_ms(&self) -> i64 {
        self.started.elapsed().as_millis() as i64
    }

    pub fn stats(&self, persona: Persona) -> &Stats {
        &self.stats[&persona]
    }

    pub fn note(&self, source: &str, text: impl Into<String>) {
        let text = text.into();
        let at_ms = self.now_ms();
        eprintln!("[{:>7.1}s] {source:<6} {text}", at_ms as f64 / 1000.0);
        self.timeline.lock().expect("timeline").push(TimelineEvent {
            at_ms,
            source: source.to_string(),
            text,
        });
    }

    /// Marks a disruptive window (kill, failover, connection termination) so that the
    /// time-based checks allow the system its recovery time.
    pub fn chaos_window(&self, duration: std::time::Duration) {
        let until = self.now_ms() + duration.as_millis() as i64;
        self.chaos_until_ms.fetch_max(until, Ordering::Relaxed);
    }

    pub fn chaos_active(&self) -> bool {
        self.now_ms() < self.chaos_until_ms.load(Ordering::Relaxed)
    }

    pub fn violation(&self, kind: &str, summary: impl Into<String>, rows: Vec<String>) {
        let violation = Violation {
            kind: kind.to_string(),
            at_ms: self.now_ms(),
            during_chaos: self.chaos_active(),
            summary: summary.into(),
            rows,
        };
        eprintln!(
            "[{:>7.1}s] VIOLATION {}: {}",
            violation.at_ms as f64 / 1000.0,
            violation.kind,
            violation.summary
        );
        self.violations.lock().expect("violations").push(violation);
    }

    pub fn fleet(&self, name: &str) -> Option<Arc<Fleet>> {
        self.fleets.lock().expect("fleets").get(name).cloned()
    }

    pub fn fleets_of(&self, persona: Persona) -> Vec<Arc<Fleet>> {
        self.fleets
            .lock()
            .expect("fleets")
            .values()
            .filter(|fleet| fleet.persona == persona)
            .cloned()
            .collect()
    }

    /// Records the start of a handler. Hold the returned guard for the duration of
    /// `perform`; dropping it, including when the runtime is killed, ends the execution.
    pub fn begin_execution(
        self: &Arc<Self>,
        persona: Persona,
        job: &JobRow,
        no_events: bool,
    ) -> ExecGuard {
        let queue = self.queues.get(&job.queue);
        let queue_index = queue.as_ref().map_or(u32::MAX, |queue| queue.index);
        let now = self.now_ms() as u32;
        let default_flags = if no_events { flag::NO_EVENTS } else { 0 };
        let (ran_after_cancel, overlapped) = self.ledger.with(
            job.id,
            || {
                JobRecord::new(
                    persona,
                    queue_index,
                    now,
                    expect::ANY_TERMINAL,
                    default_flags,
                )
            },
            |record| {
                let overlapped = record.in_flight > 0;
                record.overlapped |= overlapped;
                record.in_flight = record.in_flight.saturating_add(1);
                record.executions = record.executions.saturating_add(1);
                record.max_attempt = record.max_attempt.max(job.attempt);
                if record.first_start_ms == 0 {
                    record.first_start_ms = now.max(1);
                }
                record.last_start_ms = now;
                (
                    record.operator_cancel_ms != 0 && now > record.operator_cancel_ms,
                    overlapped,
                )
            },
        );
        if ran_after_cancel {
            self.violation(
                "ran_after_cancel",
                format!("job {} started after its operator cancel committed", job.id),
                vec![format!(
                    "{{\"job_id\":{},\"queue\":\"{}\",\"attempt\":{}}}",
                    job.id, job.queue, job.attempt
                )],
            );
        }
        if overlapped {
            self.stats(persona).add("overlapping_executions", 1);
        }
        if let Some(name) = job
            .metadata
            .get("cron_name")
            .and_then(|value| value.as_str())
        {
            let fire = job
                .metadata
                .get("cron_fire_time")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            if !self
                .ledger
                .note_cron_fire(name.to_string(), fire.to_string(), job.id)
            {
                self.violation(
                    "cron_double_fire",
                    format!(
                        "cron {name} fire {fire} produced more than one job (latest {})",
                        job.id
                    ),
                    vec![],
                );
            }
        }
        if let Some(queue) = &queue {
            queue.started.fetch_add(1, Ordering::Relaxed);
        }
        self.stats(persona).add("executions", 1);
        ExecGuard {
            _flight: InFlightGuard::new(queue),
            shared: self.clone(),
            job_id: job.id,
        }
    }

    /// Records a lifecycle event emitted by a runtime after the transition committed.
    pub fn on_event(&self, event: &UntypedJobEvent) {
        let job = event.job();
        let now = self.now_ms() as u32;
        let queue_index = self
            .queues
            .get(&job.queue)
            .map_or(u32::MAX, |queue| queue.index);
        let persona = self
            .queues
            .get(&job.queue)
            .map_or(Persona::Chaos, |queue| queue.persona);
        let mut double_complete = false;
        self.ledger.with(
            job.id,
            || JobRecord::new(persona, queue_index, now, expect::ANY_TERMINAL, 0),
            |record| match event {
                UntypedJobEvent::Completed { .. } => {
                    record.completed_events = record.completed_events.saturating_add(1);
                    record.completed_event_ms = now;
                    record.flags |= flag::COMPLETED_EVENT;
                    double_complete = record.completed_events > 1 && !record.has(flag::DLQ_RETRIED);
                }
                UntypedJobEvent::Exhausted { .. } => {
                    record.exhausted_events = record.exhausted_events.saturating_add(1);
                    record.flags |= flag::EXHAUSTED;
                }
                UntypedJobEvent::Cancelled { .. } => {
                    record.cancel_events = record.cancel_events.saturating_add(1);
                    record.flags |= flag::HANDLER_CANCELLED;
                }
                UntypedJobEvent::Rescued { .. } => record.flags |= flag::RESCUED,
                _ => {}
            },
        );
        match event {
            UntypedJobEvent::Rescued { reason, .. } => {
                self.stats(persona)
                    .add(&format!("rescued_{}", reason.as_str()), 1);
            }
            UntypedJobEvent::Exhausted { .. } => self.stats(persona).add("exhausted", 1),
            UntypedJobEvent::Completed { .. } => self.stats(persona).add("completed_events", 1),
            UntypedJobEvent::Cancelled { .. } => self.stats(persona).add("handler_cancelled", 1),
            UntypedJobEvent::Retried { .. } => self.stats(persona).add("retried", 1),
            _ => {}
        }
        if double_complete {
            self.violation(
                "duplicate_terminal_event",
                format!("job {} reported completed more than once", job.id),
                vec![format!(
                    "{{\"job_id\":{},\"queue\":\"{}\"}}",
                    job.id, job.queue
                )],
            );
        }
    }
}

pub struct ExecGuard {
    _flight: InFlightGuard,
    shared: Arc<Shared>,
    job_id: i64,
}

impl Drop for ExecGuard {
    fn drop(&mut self) {
        self.shared.ledger.with(
            self.job_id,
            || JobRecord::new(Persona::Chaos, u32::MAX, 0, expect::ANY_TERMINAL, 0),
            |record| record.in_flight = record.in_flight.saturating_sub(1),
        );
    }
}
