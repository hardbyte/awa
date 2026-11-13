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
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
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
    pub gate_proof: OnceLock<super::gate_proof::GateProof>,
    pub queues: QueueRegistry,
    pub budget: Budget,
    pub load_stop: Stop,
    pub drain_stop: Stop,
    pub timeline: Mutex<Vec<TimelineEvent>>,
    pub violations: Mutex<Vec<Violation>>,
    /// Required oracle evidence was unavailable; the run cannot establish a pass.
    pub oracle_incomplete: AtomicBool,
    /// Problems that are not invariant violations (slow admin queries); reported, not fatal.
    pub findings: Mutex<Vec<Violation>>,
    pub stats: HashMap<Persona, Stats>,
    pub ui: super::personas::ui::UiMetrics,
    /// Handlers currently running in the Python process, from its own reports.
    pub python_active: AtomicI64,
    /// Set once the post-load drain finished with no live work left.
    pub quiesced: std::sync::atomic::AtomicBool,
    /// The running Python process, so the chaos persona can kill it.
    pub python: Mutex<Option<Arc<super::personas::python::PythonHandle>>>,
    /// Run-wide measurements (resource use, oracle probes).
    pub run: Stats,
    pub fleets: Mutex<HashMap<String, Arc<Fleet>>>,
    /// State of the shard-ops persona and the checks that go with it.
    pub shardops: super::shard_oracle::ShardOpsState,
    /// State of the claim-and-abandon persona and the checks that go with it.
    pub abandon: super::abandon_oracle::AbandonState,
    /// Resident memory of this process over the run, and the growth gate's verdict.
    pub memory: super::memory::MemoryLog,
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
            gate_proof: OnceLock::new(),
            queues: QueueRegistry::default(),
            budget,
            load_stop: Stop::new(),
            drain_stop: Stop::new(),
            timeline: Mutex::new(Vec::new()),
            violations: Mutex::new(Vec::new()),
            oracle_incomplete: AtomicBool::new(false),
            findings: Mutex::new(Vec::new()),
            stats,
            run: Stats::default(),
            ui: Default::default(),
            python_active: AtomicI64::new(0),
            quiesced: std::sync::atomic::AtomicBool::new(false),
            python: Mutex::new(None),
            fleets: Mutex::new(HashMap::new()),
            shardops: Default::default(),
            abandon: Default::default(),
            memory: Default::default(),
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

    /// Recovery time the time-based checks allow during a disruptive window: the configured
    /// slack, plus the completion stall while one has been run, since claiming and promotion
    /// wait on the stalled tables for its whole length.
    pub fn chaos_slack(&self) -> std::time::Duration {
        let stalled = self.memory.stalled_for();
        self.config.chaos_slack + stalled
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

    pub fn incomplete(&self, summary: impl Into<String>, rows: Vec<String>) {
        self.run.add("oracle_incomplete_checks", 1);
        if !self.oracle_incomplete.swap(true, Ordering::Relaxed) {
            self.violation("oracle_incomplete", summary, rows);
        }
    }

    /// Records a non-fatal finding: something worth a bug report that is not one of the
    /// invariants the soak fails on.
    pub fn finding(&self, kind: &str, summary: impl Into<String>, rows: Vec<String>) {
        let finding = Violation {
            kind: kind.to_string(),
            at_ms: self.now_ms(),
            during_chaos: self.chaos_active(),
            summary: summary.into(),
            rows,
        };
        eprintln!(
            "[{:>7.1}s] FINDING {}: {}",
            finding.at_ms as f64 / 1000.0,
            finding.kind,
            finding.summary
        );
        self.findings.lock().expect("findings").push(finding);
    }

    /// Records a successful operator cancel: the job may legitimately end cancelled, a
    /// handler must not start after it, and it must not overwrite a completion.
    pub fn record_operator_cancel(
        &self,
        persona: Persona,
        job_id: i64,
        run_lease: i64,
        completed_before: bool,
        call_started_ms: u32,
    ) {
        let now = self.now_ms() as u32;
        self.stats(persona).add("cancelled", 1);
        self.ledger.with(
            job_id,
            || JobRecord::new(persona, u32::MAX, now, expect::ANY_TERMINAL, 0),
            |record| {
                record.flags |= flag::OPERATOR_REMOVED | flag::OPERATOR_CANCEL_OK;
                record.operator_cancel_ms = now;
                record.operator_cancel_run_lease = Some(run_lease);
            },
        );
        if completed_before {
            self.violation(
                "cancel_overwrote_completed",
                format!("operator cancel of job {job_id} succeeded after it was reported completed (call began at {call_started_ms} ms)"),
                vec![format!("{{\"job_id\":{job_id}}}")],
            );
        }
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
    pub async fn record_completed_return(&self, persona: Persona, job: &JobRow) {
        if let Some(proof) = self.gate_proof.get() {
            let started = Instant::now();
            if let Err(error) = proof.returned_completed(job, persona.name()).await {
                self.incomplete(error, vec![]);
                self.run.add("gate_handler_return_write_failures", 1);
            }
            self.run.add(
                "gate_handler_return_write_us",
                started.elapsed().as_micros().min(i64::MAX as u128) as i64,
            );
        }
        self.ledger.mark(job.id, flag::RETURNED_COMPLETED);
    }

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
                record.latest_run_lease = record.latest_run_lease.max(job.run_lease);
                if record.first_start_ms == 0 {
                    record.first_start_ms = now.max(1);
                }
                record.last_start_ms = now;
                // Cancelling a running handler is cooperative: the attempt
                // the cancel closed was claimed before it and may still reach
                // its handler afterwards. The completion check below and the
                // final verdict make sure that attempt never counts as done.
                // Any other attempt starting after the cancel was claimed
                // after it.
                let ran_after_cancel = record.operator_cancel_ms != 0
                    && now > record.operator_cancel_ms
                    && record.operator_cancel_run_lease != Some(job.run_lease);
                (ran_after_cancel, overlapped)
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
            // Manually triggered jobs carry the schedule's name but no fire time.
            if !fire.is_empty()
                && !self
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
        let mut cancelled_attempt_completed = false;
        self.ledger.with(
            job.id,
            || JobRecord::new(persona, queue_index, now, expect::ANY_TERMINAL, 0),
            |record| match event {
                UntypedJobEvent::Completed { .. } => {
                    record.note_terminal(job.run_lease);
                    record.completed_events = record.completed_events.saturating_add(1);
                    record.completed_event_ms = now;
                    record.flags |= flag::COMPLETED_EVENT;
                    double_complete = record.completed_events > 1 && !record.has(flag::DLQ_RETRIED);
                    cancelled_attempt_completed =
                        record.operator_cancel_run_lease == Some(job.run_lease);
                }
                UntypedJobEvent::Exhausted { .. } => {
                    record.note_terminal(job.run_lease);
                    record.exhausted_events = record.exhausted_events.saturating_add(1);
                    record.flags |= flag::EXHAUSTED;
                }
                UntypedJobEvent::Cancelled { .. } => {
                    record.note_terminal(job.run_lease);
                    record.cancel_events = record.cancel_events.saturating_add(1);
                    record.flags |= flag::HANDLER_CANCELLED;
                }
                UntypedJobEvent::Rescued { .. } => record.flags |= flag::RESCUED,
                _ => {}
            },
        );
        match event {
            UntypedJobEvent::Rescued { reason, .. } => {
                if persona == Persona::ShardOps {
                    self.note(
                        "events",
                        format!(
                            "job {} on {} rescued ({}) at attempt {}",
                            job.id,
                            job.queue,
                            reason.as_str(),
                            job.attempt
                        ),
                    );
                }
                self.stats(persona)
                    .add(&format!("rescued_{}", reason.as_str()), 1);
            }
            UntypedJobEvent::Exhausted { .. } => self.stats(persona).add("exhausted", 1),
            UntypedJobEvent::Completed { .. } => self.stats(persona).add("completed_events", 1),
            UntypedJobEvent::Cancelled { .. } => self.stats(persona).add("handler_cancelled", 1),
            UntypedJobEvent::Retried { .. } => self.stats(persona).add("retried", 1),
            _ => {}
        }
        if cancelled_attempt_completed {
            self.violation(
                "cancelled_attempt_completed",
                format!(
                    "job {} attempt {} was reported completed after an operator cancel of that attempt succeeded",
                    job.id, job.run_lease
                ),
                vec![format!(
                    "{{\"job_id\":{},\"queue\":\"{}\",\"run_lease\":{}}}",
                    job.id, job.queue, job.run_lease
                )],
            );
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
