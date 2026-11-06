//! Checks for the claim-and-abandon persona: a process holding thousands of claims is
//! SIGKILLed again and again. After each kill the rescue backlog has to clear within a
//! bound, the claim ring has to keep rotating, no job whose owner is alive may be
//! rescued, and attempts and the dead-letter queue have to follow `max_attempts`.

use super::config::Persona;
use super::shared::Shared;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::HashMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_ROWS: usize = 25;
/// The ring rotates about once a second; this long without a rotation is a stall.
const RING_STALL_AFTER_MS: i64 = 6_000;
/// Owner of attempts run by the surviving in-process runtime, which is never killed.
pub const FINISHER: u32 = u32::MAX;

pub fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy)]
pub struct Attempt {
    pub attempt: i16,
    pub owner: u32,
    pub at_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Kill {
    pub number: u32,
    pub generation: u32,
    /// Milliseconds since the run started, and since the epoch, when SIGKILL was sent.
    pub at_run_ms: i64,
    pub signalled_epoch_ms: i64,
    /// Handlers the process had started, which is what it held when it died.
    pub started_by_process: i64,
    /// Open claims on the persona's queues right after the kill.
    pub open_after_kill: i64,
    /// Claims of the process that were rescued before it was killed.
    pub rescued_while_alive: i64,
    pub expired_peak: i64,
    pub oldest_expired_peak_s: f64,
    pub cleared_after_s: Option<f64>,
    pub reported_slow: bool,
    pub load_ended: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RingEpisode {
    pub start_ms: i64,
    pub end_ms: Option<i64>,
    pub generation: i64,
    pub slot: i32,
    /// The last time the slot the ring has to enter next held a claim whose owner is
    /// alive, which explains a stall. Starts at the beginning of the episode.
    pub last_live_ms: i64,
    pub expired_in_next_slot: i64,
    pub reported: bool,
}

#[derive(Default)]
struct RingMonitor {
    last: Option<(i32, i64)>,
    last_change_ms: i64,
    episode: Option<RingEpisode>,
    episodes: Vec<RingEpisode>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Sample {
    pub at_ms: i64,
    pub open: i64,
    pub expired: i64,
    pub oldest_expired_s: f64,
    pub ring_generation: i64,
    pub probe_ms: i64,
}

#[derive(Default)]
pub struct AbandonState {
    /// Open claims on the persona's queues at the last probe.
    pub open_claims: AtomicI64,
    /// A child process is running.
    pub child_alive: AtomicBool,
    /// Handlers running in the child process at its last report.
    pub child_active: AtomicI64,
    started_by_process: Mutex<HashMap<u32, i64>>,
    /// The running child process and the epoch milliseconds at which it was started.
    live: Mutex<Option<(u32, i64)>>,
    /// Most claims of a process seen rescued while it was alive, per process.
    live_lost: Mutex<HashMap<u32, i64>>,
    attempts: Mutex<HashMap<i64, Vec<Attempt>>>,
    max_attempts: Mutex<HashMap<i64, i16>>,
    /// Epoch milliseconds at which each process generation was killed.
    killed_at: Mutex<HashMap<u32, i64>>,
    kills: Mutex<Vec<Kill>>,
    samples: Mutex<Vec<Sample>>,
    ring: Mutex<RingMonitor>,
    csv: Mutex<Option<PathBuf>>,
    passes_csv: Mutex<Option<PathBuf>>,
    heartbeat_last: Mutex<Option<f64>>,
}

pub struct Probe {
    pub open: i64,
    pub expired: i64,
    pub oldest_expired_s: f64,
    /// Open claims taken at or after the `since` the probe was given.
    pub open_since: i64,
}

/// The process that is alive, and how many handlers it had started before the probe.
#[derive(Debug, Clone, Copy)]
pub struct LiveCheck {
    pub generation: u32,
    pub started: i64,
}

impl AbandonState {
    pub fn set_csv(&self, path: PathBuf) {
        if let Ok(mut file) = std::fs::File::create(&path) {
            let _ = writeln!(
                file,
                "run_s,open_claims,expired_claims,oldest_expired_s,claim_ring_generation,probe_ms"
            );
        }
        let passes = path.with_file_name("abandon-heartbeat-passes.csv");
        if let Ok(mut file) = std::fs::File::create(&passes) {
            let _ = writeln!(file, "run_s,handlers_running,claims_sampled,pass_ms");
        }
        *self.passes_csv.lock().expect("passes csv") = Some(passes);
        *self.csv.lock().expect("csv") = Some(path);
    }

    /// Folds in one reading of the oldest and newest heartbeat (epoch seconds) among the live
    /// process's claims. While a pass runs, the oldest heartbeat is still one the previous pass
    /// wrote, so it cannot be newer than that pass's newest. The first reading whose oldest
    /// heartbeat is newer was taken just after a pass finished, and the spread from oldest to
    /// newest is its duration (to within the last chunk). The samples are bucketed by handler
    /// count so the report shows how pass time grows with what a process holds. `now` is the
    /// database clock at the reading.
    pub fn note_heartbeat_sample(
        &self,
        shared: &Shared,
        rows: i64,
        oldest: f64,
        newest: f64,
        now: f64,
        active: i64,
    ) {
        let mut last = self.heartbeat_last.lock().expect("heartbeat last");
        let Some(settled_newest) = *last else {
            *last = Some(newest);
            return;
        };
        if oldest <= settled_newest || active <= 0 {
            return;
        }
        *last = Some(newest);
        drop(last);
        // A reading taken long after the pass finished may include the next pass's first
        // chunks, so only a fresh one measures the pass.
        if now - newest > 2.0 {
            shared
                .stats(Persona::ClaimAbandon)
                .add("heartbeat_passes_unmeasured", 1);
            return;
        }
        let ms = ((newest - oldest) * 1000.0).round() as i64;
        let stats = shared.stats(Persona::ClaimAbandon);
        stats.observe("heartbeat_pass_ms", ms);
        let bucket = match active {
            ..=4_999 => "under 5k",
            5_000..=9_999 => "5k-10k",
            10_000..=14_999 => "10k-15k",
            15_000..=19_999 => "15k-20k",
            _ => "20k and over",
        };
        stats.observe(&format!("heartbeat_pass_ms (handlers {bucket})"), ms);
        if let Some(path) = self.passes_csv.lock().expect("passes csv").as_ref() {
            if let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(path) {
                let _ = writeln!(
                    file,
                    "{:.1},{active},{rows},{ms}",
                    shared.now_ms() as f64 / 1000.0
                );
            }
        }
    }

    pub fn register_job(&self, job_id: i64, max_attempts: i16) {
        self.max_attempts
            .lock()
            .expect("max attempts")
            .insert(job_id, max_attempts);
    }

    pub fn note_attempt(&self, job_id: i64, attempt: Attempt) {
        if attempt.owner != FINISHER {
            *self
                .started_by_process
                .lock()
                .expect("started")
                .entry(attempt.owner)
                .or_insert(0) += 1;
        }
        self.attempts
            .lock()
            .expect("attempts")
            .entry(job_id)
            .or_default()
            .push(attempt);
    }

    pub fn note_generation_killed(&self, generation: u32, at_epoch_ms: i64) {
        self.killed_at
            .lock()
            .expect("killed at")
            .insert(generation, at_epoch_ms);
    }

    /// Signal and publish the death before a waiting probe can classify its result.
    /// The live-state lock also fences the probe's post-query check; no await occurs
    /// between a successful signal and publishing its generation and timestamp.
    pub fn signal_generation_killed(
        &self,
        generation: u32,
        signal: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<i64> {
        let mut live = self.live.lock().expect("live");
        signal()?;
        let signalled_at = epoch_ms();
        self.note_generation_killed(generation, signalled_at);
        if live.is_some_and(|(current, _)| current == generation) {
            *live = None;
        }
        Ok(signalled_at)
    }

    pub fn set_live(&self, live: Option<(u32, i64)>) {
        *self.heartbeat_last.lock().expect("heartbeat last") = None;
        *self.live.lock().expect("live") = live;
    }

    pub fn live(&self) -> Option<(u32, i64)> {
        *self.live.lock().expect("live")
    }

    pub fn live_lost(&self, generation: u32) -> i64 {
        self.live_lost
            .lock()
            .expect("live lost")
            .get(&generation)
            .copied()
            .unwrap_or(0)
    }

    pub fn record_kill(&self, kill: Kill) {
        self.kills.lock().expect("kills").push(kill);
    }

    pub fn kills(&self) -> Vec<Kill> {
        self.kills.lock().expect("kills").clone()
    }

    pub fn started_by(&self, generation: u32) -> i64 {
        self.started_by_process
            .lock()
            .expect("started")
            .get(&generation)
            .copied()
            .unwrap_or(0)
    }

    /// Whether the ring has been still for long enough to count as stalled.
    pub fn ring_stalled(&self) -> bool {
        self.ring.lock().expect("ring").episode.is_some()
    }

    pub fn episodes(&self) -> Vec<RingEpisode> {
        let ring = self.ring.lock().expect("ring");
        let mut all = ring.episodes.clone();
        all.extend(ring.episode.clone());
        all
    }

    /// Records one probe and advances the per-kill clocks.
    pub fn observe(
        &self,
        shared: &Shared,
        probe: &Probe,
        live: Option<LiveCheck>,
        ring_generation: i64,
        probe_ms: i64,
    ) {
        let now = shared.now_ms();
        self.open_claims.store(probe.open, Ordering::Relaxed);
        let sample = Sample {
            at_ms: now,
            open: probe.open,
            expired: probe.expired,
            oldest_expired_s: probe.oldest_expired_s,
            ring_generation,
            probe_ms,
        };
        self.samples.lock().expect("samples").push(sample);
        if let Some(path) = self.csv.lock().expect("csv").as_ref() {
            if let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(path) {
                let _ = writeln!(
                    file,
                    "{:.1},{},{},{:.1},{},{}",
                    now as f64 / 1000.0,
                    probe.open,
                    probe.expired,
                    probe.oldest_expired_s,
                    ring_generation,
                    probe_ms
                );
            }
        }
        let stats = shared.stats(Persona::ClaimAbandon);
        stats.set_max("peak_open_claims", probe.open);
        stats.set_max("peak_expired_claims", probe.expired);
        stats.set_max(
            "peak_oldest_expired_s",
            probe.oldest_expired_s.round() as i64,
        );
        stats.observe("probe_ms", probe_ms);

        // A probe can begin while the child is alive and finish after its kill. Only
        // a generation still alive after the query proves missing claims were taken
        // from a live owner. Keep this check serialized with signaling the kill.
        if let Some(live) = live {
            let lost = (live.started - probe.open_since).max(0);
            let current = self.live.lock().expect("live");
            let still_live = current.is_some_and(|(generation, _)| generation == live.generation)
                && !self
                    .killed_at
                    .lock()
                    .expect("killed at")
                    .contains_key(&live.generation);
            if lost > 0 && !still_live {
                shared.incomplete(
                    format!(
                        "claim probe for process {} crossed its kill or replacement; {lost} missing claim(s) cannot be classified as a live-owner rescue",
                        live.generation
                    ),
                    vec![format!(
                        "{{\"process\":{},\"started\":{},\"claims_still_open\":{}}}",
                        live.generation, live.started, probe.open_since
                    )],
                );
            } else {
                let mut peaks = self.live_lost.lock().expect("live lost");
                let peak = peaks.entry(live.generation).or_insert(0);
                let first = lost > 0 && *peak == 0;
                *peak = (*peak).max(lost);
                drop(peaks);
                stats.set_max("live_claims_rescued_peak", lost);
                if first {
                    shared.violation(
                        "live_owner_rescued",
                        format!(
                            "claims of process {} were rescued while it was alive: it had started {} handler(s) and only {} of its claims were still open ({} claim(s) were eligible for rescue)",
                            live.generation, live.started, probe.open_since, probe.expired
                        ),
                        vec![format!(
                            "{{\"process\":{},\"started\":{},\"claims_still_open\":{},\"eligible_for_rescue\":{}}}",
                            live.generation, live.started, probe.open_since, probe.expired
                        )],
                    );
                }
            }
        }
        let staleness_ms = shared.config.heartbeat_staleness.as_millis() as i64 + 3_000;
        let epoch = epoch_ms();
        let bound = shared.config.abandon_bound;
        let mut slow: Vec<String> = Vec::new();
        {
            let mut kills = self.kills.lock().expect("kills");
            for kill in kills
                .iter_mut()
                .filter(|kill| kill.cleared_after_s.is_none())
            {
                kill.expired_peak = kill.expired_peak.max(probe.expired);
                kill.oldest_expired_peak_s = kill.oldest_expired_peak_s.max(probe.oldest_expired_s);
                let since_kill_ms = epoch - kill.signalled_epoch_ms;
                // Claims of the dead process only count as stale after the staleness
                // window, so a probe sooner than that proves nothing.
                if since_kill_ms >= staleness_ms && probe.expired == 0 {
                    kill.cleared_after_s = Some(since_kill_ms as f64 / 1000.0);
                    continue;
                }
                let mut allowed = bound;
                if shared.chaos_active() {
                    allowed += shared.config.chaos_slack;
                }
                if !kill.reported_slow && since_kill_ms as u64 > allowed.as_millis() as u64 {
                    kill.reported_slow = true;
                    slow.push(format!(
                        "{{\"kill\":{},\"seconds_since_kill\":{:.0},\"open_claims\":{},\"expired_claims\":{},\"oldest_expired_s\":{:.0},\"bound_s\":{}}}",
                        kill.number,
                        since_kill_ms as f64 / 1000.0,
                        probe.open,
                        probe.expired,
                        probe.oldest_expired_s,
                        bound.as_secs()
                    ));
                }
            }
        }
        for row in slow {
            shared.violation(
                "rescue_backlog_slow",
                format!(
                    "the rescue backlog left by a killed process was not cleared within {}s of the kill ({} expired claim(s) remain, the oldest eligible for {:.0}s)",
                    bound.as_secs(),
                    probe.expired,
                    probe.oldest_expired_s
                ),
                vec![row],
            );
        }
    }

    /// Watches the claim ring; called about once a second.
    ///
    /// `next_slot` is the number of live and expired open claims in the slot the ring
    /// enters next, known only while the ring is stalled.
    pub fn observe_ring(
        &self,
        shared: &Shared,
        current_slot: i32,
        generation: i64,
        next_slot: Option<(i64, i64)>,
    ) {
        let now = shared.now_ms();
        let mut report: Option<RingEpisode> = None;
        {
            let mut ring = self.ring.lock().expect("ring");
            let moved = ring.last != Some((current_slot, generation));
            if moved {
                ring.last = Some((current_slot, generation));
                ring.last_change_ms = now;
                if let Some(mut episode) = ring.episode.take() {
                    episode.end_ms = Some(now);
                    ring.episodes.push(episode);
                }
                return;
            }
            let mut threshold = crate::ci_timing::scaled_timeout(std::time::Duration::from_millis(
                RING_STALL_AFTER_MS as u64,
            ))
            .as_millis() as i64;
            if shared.chaos_active() {
                threshold += shared.config.chaos_slack.as_millis() as i64;
            }
            if now - ring.last_change_ms >= threshold && ring.episode.is_none() {
                ring.episode = Some(RingEpisode {
                    start_ms: ring.last_change_ms,
                    end_ms: None,
                    generation,
                    slot: current_slot,
                    last_live_ms: now,
                    expired_in_next_slot: 0,
                    reported: false,
                });
            }
            let bound_ms = shared.config.abandon_bound.as_millis() as i64;
            if let Some(episode) = ring.episode.as_mut() {
                if let Some((live, expired)) = next_slot {
                    if live > 0 {
                        episode.last_live_ms = now;
                    }
                    episode.expired_in_next_slot = expired;
                }
                // A stall is expected while a live process holds the next slot; it is a
                // violation once nothing alive holds it and the ring still does not move.
                if !episode.reported && now - episode.last_live_ms > bound_ms {
                    episode.reported = true;
                    report = Some(episode.clone());
                }
            }
        }
        if let Some(episode) = report {
            shared.violation(
                "claim_ring_stalled",
                format!(
                    "the claim ring has not rotated for more than {}s with no live claim in the slot it has to enter next ({} expired claim(s) there); stuck at slot {}, generation {}",
                    shared.config.abandon_bound.as_secs(),
                    episode.expired_in_next_slot,
                    episode.slot,
                    episode.generation
                ),
                vec![format!(
                    "{{\"stalled_since_s\":{:.1},\"slot\":{},\"generation\":{},\"expired_in_next_slot\":{}}}",
                    episode.start_ms as f64 / 1000.0,
                    episode.slot,
                    episode.generation,
                    episode.expired_in_next_slot
                )],
            );
        }
    }

    /// Closes the ring episode left open when the run ends.
    pub fn finish_ring(&self, shared: &Shared) {
        let now = shared.now_ms();
        let mut ring = self.ring.lock().expect("ring");
        if let Some(mut episode) = ring.episode.take() {
            episode.end_ms = Some(now);
            ring.episodes.push(episode);
        }
    }

    /// Stall time and resume time of the ring in each kill's window.
    fn ring_by_kill(&self) -> Vec<(f64, Option<f64>)> {
        let kills = self.kills();
        let episodes = self.episodes();
        kills
            .iter()
            .enumerate()
            .map(|(index, kill)| {
                let from = kill.at_run_ms;
                let until = kills.get(index + 1).map_or(i64::MAX, |next| next.at_run_ms);
                let mut stalled = 0i64;
                let mut resumed: Option<i64> = None;
                for episode in &episodes {
                    let end = episode.end_ms.unwrap_or(i64::MAX);
                    if end <= from || episode.start_ms >= until {
                        continue;
                    }
                    stalled += end.min(until) - episode.start_ms.max(from);
                    if let Some(end) = episode.end_ms {
                        resumed = Some(resumed.map_or(end, |seen| seen.max(end)));
                    }
                }
                (
                    stalled as f64 / 1000.0,
                    resumed.map(|end| (end - from) as f64 / 1000.0),
                )
            })
            .collect()
    }

    /// The per-kill table for the report.
    pub fn render(&self) -> String {
        let kills = self.kills();
        if kills.is_empty() {
            return String::new();
        }
        let ring = self.ring_by_kill();
        let samples = self.samples.lock().expect("samples");
        let mut out = String::new();
        out.push_str("### Kills\n\n");
        out.push_str("| kill | held by the process | rescued while alive | open claims after kill | peak expired | oldest expired (s) | time to clear (s) | ring stalled (s) | ring resumed after (s) |\n");
        out.push_str("| -- | -- | -- | -- | -- | -- | -- | -- | -- |\n");
        for (kill, (stalled, resumed)) in kills.iter().zip(ring) {
            out.push_str(&format!(
                "| {}{} | {} | {} | {} | {} | {:.0} | {} | {:.0} | {} |\n",
                kill.number,
                if kill.load_ended { " (load end)" } else { "" },
                kill.started_by_process,
                kill.rescued_while_alive,
                kill.open_after_kill,
                kill.expired_peak,
                kill.oldest_expired_peak_s,
                kill.cleared_after_s
                    .map_or("not cleared".to_string(), |seconds| format!("{seconds:.0}")),
                stalled,
                resumed.map_or("-".to_string(), |seconds| format!("{seconds:.0}")),
            ));
        }
        let ring_episodes = self.episodes();
        out.push_str(&format!(
            "\nSamples: {}. Ring stall episodes: {}.\n",
            samples.len(),
            ring_episodes.len()
        ));
        out
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "kills": self.kills(),
            "ring_episodes": self.episodes(),
            "samples": self.samples.lock().expect("samples").len(),
        })
    }

    /// Final checks over everything the child processes and the survivors reported.
    pub async fn analyse(&self, shared: &Shared, pool: &PgPool, queues: &[String]) {
        let killed_at = self.killed_at.lock().expect("killed at").clone();
        let max_attempts = self.max_attempts.lock().expect("max attempts").clone();
        let attempts = self.attempts.lock().expect("attempts").clone();
        let stats = shared.stats(Persona::ClaimAbandon);

        let mut live_rescued: Vec<String> = Vec::new();
        let mut live_rescued_count = 0usize;
        let mut duplicates_from_dead = 0i64;
        let mut over_max: Vec<String> = Vec::new();
        let mut gaps: Vec<String> = Vec::new();
        let mut exhausted_by_attempts: Vec<i64> = Vec::new();
        for (job_id, list) in &attempts {
            let mut list = list.clone();
            list.sort_by_key(|attempt| attempt.at_ms);
            for pair in list.windows(2) {
                let (previous, next) = (pair[0], pair[1]);
                let previous_died_at = if previous.owner == FINISHER {
                    None
                } else {
                    killed_at.get(&previous.owner).copied()
                };
                match previous_died_at {
                    Some(died_at) if next.at_ms >= died_at => duplicates_from_dead += 1,
                    _ => {
                        live_rescued_count += 1;
                        if live_rescued.len() < MAX_ROWS {
                            live_rescued.push(format!(
                                "{{\"job_id\":{job_id},\"attempt\":{},\"owner\":\"{}\",\"owner_alive_until_ms\":{},\"next_attempt\":{},\"next_owner\":\"{}\",\"next_started_ms\":{}}}",
                                previous.attempt,
                                owner_name(previous.owner),
                                previous_died_at.map_or("null".to_string(), |at| at.to_string()),
                                next.attempt,
                                owner_name(next.owner),
                                next.at_ms
                            ));
                        }
                    }
                }
                if next.attempt != previous.attempt + 1 && gaps.len() < MAX_ROWS {
                    gaps.push(format!(
                        "{{\"job_id\":{job_id},\"attempt\":{},\"next_attempt\":{}}}",
                        previous.attempt, next.attempt
                    ));
                }
            }
            let highest = list
                .iter()
                .map(|attempt| attempt.attempt)
                .max()
                .unwrap_or(0);
            if let Some(max) = max_attempts.get(job_id) {
                if highest > *max && over_max.len() < MAX_ROWS {
                    over_max.push(format!(
                        "{{\"job_id\":{job_id},\"attempts_seen\":{highest},\"max_attempts\":{max}}}"
                    ));
                }
                let last = list.last().copied();
                if highest == *max
                    && last.is_some_and(|last| last.owner != FINISHER && last.attempt == *max)
                {
                    exhausted_by_attempts.push(*job_id);
                }
            }
        }
        stats.add("duplicate_executions_after_kill", duplicates_from_dead);
        stats.add("live_owner_rescues", live_rescued_count as i64);
        if live_rescued_count > 0 {
            shared.violation(
                "live_owner_rescued",
                format!("{live_rescued_count} job(s) were run again while the process that held them was still alive"),
                live_rescued,
            );
        }
        if !over_max.is_empty() {
            shared.violation(
                "attempts_exceeded_max",
                format!(
                    "{} job(s) ran more attempts than max_attempts",
                    over_max.len()
                ),
                over_max,
            );
        }
        if !gaps.is_empty() {
            shared.finding(
                "abandon_attempt_gap",
                format!(
                    "{} job(s) skipped an attempt number between executions",
                    gaps.len()
                ),
                gaps,
            );
        }

        // Jobs whose last attempt was abandoned at max_attempts have to be dead-lettered.
        let dlq: Result<Vec<(i64, i16, i16)>, _> = sqlx::query_as(&format!(
            "SELECT job_id, attempt, max_attempts FROM {}.dlq_entries WHERE queue = ANY($1)",
            schema(shared)
        ))
        .bind(queues)
        .fetch_all(pool)
        .await;
        match dlq {
            Ok(rows) => {
                stats.add("dead_lettered", rows.len() as i64);
                let early: Vec<String> = rows
                    .iter()
                    .filter(|(_, attempt, max)| attempt < max)
                    .take(MAX_ROWS)
                    .map(|(job, attempt, max)| {
                        format!("{{\"job_id\":{job},\"attempt\":{attempt},\"max_attempts\":{max}}}")
                    })
                    .collect();
                if !early.is_empty() {
                    shared.violation(
                        "dead_lettered_early",
                        format!(
                            "{} job(s) were dead-lettered before reaching max_attempts",
                            early.len()
                        ),
                        early,
                    );
                }
                let in_dlq: std::collections::HashSet<i64> =
                    rows.iter().map(|(job, _, _)| *job).collect();
                let missing: Vec<String> = exhausted_by_attempts
                    .iter()
                    .filter(|job| !in_dlq.contains(job))
                    .take(MAX_ROWS)
                    .map(|job| {
                        format!(
                            "{{\"job_id\":{job},\"max_attempts\":{}}}",
                            max_attempts.get(job).copied().unwrap_or(0)
                        )
                    })
                    .collect();
                stats.add("exhausted_after_kill", exhausted_by_attempts.len() as i64);
                if !missing.is_empty() && !shared.quiesced.load(Ordering::Relaxed) {
                    // The rescue was still working when the run ended.
                    shared.finding(
                        "abandon_rescue_unfinished",
                        format!(
                            "{} job(s) abandoned on their last attempt were not yet dead-lettered when the drain grace ended",
                            missing.len()
                        ),
                        missing,
                    );
                } else if !missing.is_empty() {
                    shared.violation(
                        "exhausted_not_dead_lettered",
                        format!(
                            "{} job(s) abandoned on their last attempt are not in the dead-letter queue",
                            missing.len()
                        ),
                        missing,
                    );
                }
            }
            Err(error) => shared.incomplete(format!("dead-letter check failed: {error}"), vec![]),
        }
    }
}

fn owner_name(owner: u32) -> String {
    if owner == FINISHER {
        "survivor".to_string()
    } else {
        format!("process-{owner}")
    }
}

fn schema(shared: &Shared) -> String {
    shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".to_string())
}

/// Which claims a backlog probe looks at.
pub enum Scope<'a> {
    /// Open receipt claims and running leases of these queues.
    Queues(&'a [String]),
    /// Open receipt identities in one claim-ring slot, including attempts
    /// materialized into running or callback-waiting leases.
    Slot(i32),
}

/// Open claims in `scope`, and the ones the rescue is already entitled to take: claims
/// whose last heartbeat (or claim time, when there never was one) is older than
/// `staleness_secs`, or whose deadline has passed.
///
/// `since_epoch_ms` splits out the open claims taken at or after that time.
pub async fn probe_backlog(
    pool: &PgPool,
    schema: &str,
    scope: Scope<'_>,
    staleness_secs: f64,
    since_epoch_ms: i64,
) -> Result<Probe, sqlx::Error> {
    let (batch_filter, row_filter, receipt_lease_filter, lease_leg) = match &scope {
        Scope::Queues(_) => (
            "b.queue = ANY($1)",
            "c.queue = ANY($1)",
            "l.job_id IS NULL",
            format!(
                "UNION ALL
            SELECT COALESCE(a.heartbeat_at, l.heartbeat_at, l.attempted_at), l.deadline_at,
                   COALESCE(l.attempted_at, l.heartbeat_at), NULL::timestamptz
            FROM {schema}.leases l
            LEFT JOIN {schema}.attempt_state a ON a.job_id = l.job_id AND a.run_lease = l.run_lease
            WHERE l.queue = ANY($1) AND l.state = 'running'"
            ),
        ),
        Scope::Slot(_) => (
            "b.claim_slot = $4",
            "c.claim_slot = $4",
            "l.job_id IS NULL OR l.state IN ('running', 'waiting_external')",
            String::new(),
        ),
    };
    let sql = format!(
        r#"
        WITH bounds AS (
            SELECT clock_timestamp() AS now_ts,
                   clock_timestamp() - make_interval(secs => $2) AS stale_before
        ),
        batch_claims AS (
            SELECT b.claim_slot, i.receipt_id, i.job_id, i.run_lease, b.queue, b.claimed_at, b.deadline_at
            FROM {schema}.lease_claim_batches b
            CROSS JOIN LATERAL unnest(b.job_ids, b.run_leases, b.receipt_ids) AS i(job_id, run_lease, receipt_id)
            WHERE {batch_filter}
        ),
        row_claims AS (
            SELECT c.claim_slot, c.receipt_id, c.job_id, c.run_lease, c.queue, c.claimed_at, c.deadline_at
            FROM {schema}.lease_claims c
            WHERE {row_filter} AND c.closed_at IS NULL
        ),
        open_receipts AS (
            SELECT x.* FROM (SELECT * FROM batch_claims UNION ALL SELECT * FROM row_claims) x
            WHERE NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closures cl
                              WHERE cl.claim_slot = x.claim_slot AND cl.job_id = x.job_id AND cl.run_lease = x.run_lease)
              AND NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closure_batches cb
                              WHERE cb.claim_slot = x.claim_slot AND cb.receipt_ranges @> x.receipt_id)
              AND NOT EXISTS (SELECT 1 FROM {schema}.done_entries d WHERE d.job_id = x.job_id AND d.run_lease = x.run_lease)
              AND NOT EXISTS (SELECT 1 FROM {schema}.deferred_jobs d WHERE d.job_id = x.job_id AND d.run_lease = x.run_lease)
              AND NOT EXISTS (SELECT 1 FROM {schema}.dlq_entries d WHERE d.job_id = x.job_id AND d.run_lease = x.run_lease)
        ),
        holders AS (
            SELECT CASE WHEN l.state = 'waiting_external' THEN NULL
                        ELSE COALESCE(a.heartbeat_at, l.heartbeat_at, l.attempted_at, x.claimed_at)
                   END AS beat_at,
                   CASE WHEN l.job_id IS NULL THEN x.deadline_at
                        WHEN l.state = 'running' THEN l.deadline_at
                   END AS deadline_at,
                   x.claimed_at,
                   CASE WHEN l.state = 'waiting_external' THEN l.callback_timeout_at END
                       AS callback_timeout_at
            FROM open_receipts x
            LEFT JOIN {schema}.leases l ON l.job_id = x.job_id AND l.run_lease = x.run_lease
            LEFT JOIN {schema}.attempt_state a ON a.job_id = x.job_id AND a.run_lease = x.run_lease
            WHERE {receipt_lease_filter}
            {lease_leg}
        ),
        eligible AS (
            SELECT LEAST(
                       CASE WHEN beat_at < stale_before THEN beat_at + make_interval(secs => $2) END,
                       CASE WHEN deadline_at < now_ts THEN deadline_at END,
                       CASE WHEN callback_timeout_at < now_ts THEN callback_timeout_at END
                   ) AS eligible_at,
                   claimed_at >= to_timestamp($3::float8 / 1000.0) AS since_start
            FROM holders, bounds
        )
        SELECT count(*)::bigint,
               count(eligible_at)::bigint,
               COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - min(eligible_at))), 0)::float8,
               count(*) FILTER (WHERE since_start)::bigint
        FROM eligible
        "#
    );
    let query = sqlx::query_as::<_, (i64, i64, f64, i64)>(&sql);
    let query = match scope {
        Scope::Queues(queues) => query
            .bind(queues.to_vec())
            .bind(staleness_secs)
            .bind(since_epoch_ms as f64)
            .bind(0_i32),
        Scope::Slot(slot) => query
            .bind(Vec::<String>::new())
            .bind(staleness_secs)
            .bind(since_epoch_ms as f64)
            .bind(slot),
    };
    let (open, expired, oldest, open_since) = query.fetch_one(pool).await?;
    Ok(Probe {
        open,
        expired,
        oldest_expired_s: oldest.max(0.0),
        open_since,
    })
}

#[cfg(test)]
mod tests {
    use super::{epoch_ms, probe_backlog, LiveCheck, Probe, Scope};
    use crate::persona_soak::config::{replace_database_name, SoakConfig};
    use crate::persona_soak::db::{self, Budget, ServerInfo};
    use crate::persona_soak::shared::Shared;
    use awa::model::{ClaimedRuntimeJob, QueueStorage, QueueStorageConfig};
    use sqlx::PgPool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    fn live_probe_fixture(generation: u32) -> (Arc<Shared>, LiveCheck) {
        let config = SoakConfig::for_test(
            "postgres://postgres:test@localhost/awa_soak_live_probe".to_string(),
        );
        let info = ServerInfo::default();
        let budget = Budget::new(&info, 0);
        let shared = Shared::new(config, info, budget);
        shared.abandon.set_live(Some((generation, epoch_ms())));
        let live = LiveCheck {
            generation: shared.abandon.live().expect("live generation").0,
            started: 1_000,
        };
        (shared, live)
    }

    fn observe_live_probe(shared: &Shared, live: LiveCheck, open: i64) {
        shared.abandon.observe(
            shared,
            &Probe {
                open,
                expired: 0,
                oldest_expired_s: 0.0,
                open_since: open,
            },
            Some(live),
            0,
            1,
        );
    }

    fn live_rescue_reports(shared: &Shared) -> usize {
        shared
            .violations
            .lock()
            .expect("violations")
            .iter()
            .filter(|violation| violation.kind == "live_owner_rescued")
            .count()
    }

    #[test]
    fn live_probe_crossing_its_generation_kill_is_incomplete() {
        let (shared, captured) = live_probe_fixture(1);
        // The query was dispatched with this owner alive, but its snapshot
        // boundary relative to the signal is unknown when the result arrives.
        shared.abandon.note_generation_killed(1, epoch_ms());
        shared.abandon.set_live(None);
        observe_live_probe(&shared, captured, 500);

        assert!(shared.oracle_incomplete.load(Ordering::Relaxed));
        assert_eq!(live_rescue_reports(&shared), 0);
        assert_eq!(shared.abandon.live_lost(1), 0);
        // The usable backlog evidence is retained even though liveness is unknown.
        assert_eq!(shared.abandon.open_claims.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn live_probe_still_alive_missing_claims_remain_a_violation() {
        let (shared, captured) = live_probe_fixture(1);
        observe_live_probe(&shared, captured, 500);

        assert_eq!(live_rescue_reports(&shared), 1);
        assert_eq!(shared.abandon.live_lost(1), 500);
        assert!(!shared.oracle_incomplete.load(Ordering::Relaxed));
    }

    #[test]
    fn live_probe_other_generation_kill_does_not_hide_live_loss() {
        let (shared, captured) = live_probe_fixture(2);
        shared.abandon.note_generation_killed(1, epoch_ms());
        observe_live_probe(&shared, captured, 500);

        assert_eq!(live_rescue_reports(&shared), 1);
        assert_eq!(shared.abandon.live_lost(2), 500);
        assert!(!shared.oracle_incomplete.load(Ordering::Relaxed));
    }

    #[test]
    fn live_probe_kill_overlap_preserves_prior_proven_loss() {
        let (shared, captured) = live_probe_fixture(1);
        observe_live_probe(&shared, captured, 500);
        assert_eq!(shared.abandon.live_lost(1), 500);

        shared.abandon.note_generation_killed(1, epoch_ms());
        shared.abandon.set_live(None);
        observe_live_probe(&shared, captured, 100);

        assert_eq!(live_rescue_reports(&shared), 1);
        assert_eq!(shared.abandon.live_lost(1), 500);
        assert!(shared.oracle_incomplete.load(Ordering::Relaxed));
    }

    #[test]
    fn live_probe_successful_signal_publishes_death_before_returning() {
        let (shared, _) = live_probe_fixture(1);
        let at = shared
            .abandon
            .signal_generation_killed(1, || Ok(()))
            .expect("successful signal");

        assert_eq!(shared.abandon.live(), None);
        assert_eq!(
            shared.abandon.killed_at.lock().expect("killed at").get(&1),
            Some(&at)
        );
    }

    #[test]
    fn live_probe_failed_signal_does_not_publish_a_death() {
        let (shared, _) = live_probe_fixture(1);
        let before = shared.abandon.live();
        let result = shared
            .abandon
            .signal_generation_killed(1, || Err(std::io::Error::other("signal failed")));

        assert!(result.is_err());
        assert_eq!(shared.abandon.live(), before);
        assert!(shared
            .abandon
            .killed_at
            .lock()
            .expect("killed at")
            .is_empty());
    }

    struct SlotProbe {
        config: SoakConfig,
        pool: PgPool,
        store: QueueStorage,
    }

    impl SlotProbe {
        async fn create() -> Self {
            let base = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:test@localhost:15432/awa_test".to_string()
            });
            let name = format!("awa_soak_slot_probe_{}", uuid::Uuid::new_v4().simple());
            let config = SoakConfig::for_test(replace_database_name(&base, &name));
            db::recreate_database(&config)
                .await
                .expect("isolated database");
            let pool = db::connect_pool(&config.database_url, "slot_probe_regression", 4)
                .await
                .expect("connect");
            let store = QueueStorage::new(QueueStorageConfig::default()).expect("store");
            Self {
                config,
                pool,
                store,
            }
        }

        async fn claim(&self, deadline: Duration) -> ClaimedRuntimeJob {
            self.store
                .enqueue_batch(&self.pool, "slot_probe", 2, 1)
                .await
                .expect("enqueue");
            self.store
                .claim_runtime_batch(&self.pool, "slot_probe", 1, deadline)
                .await
                .expect("claim")
                .pop()
                .expect("one claim")
        }

        async fn assert_counts(&self, slot: i32, open: i64, expired: i64) {
            let probe = probe_backlog(&self.pool, "awa", Scope::Slot(slot), 30.0, 0)
                .await
                .expect("slot probe");
            assert_eq!((probe.open, probe.expired), (open, expired));
        }

        async fn age_heartbeat(&self, claim: &ClaimedRuntimeJob) {
            sqlx::query(
                "UPDATE awa.leases
                 SET heartbeat_at = clock_timestamp() - interval '1 minute'
                 WHERE job_id = $1 AND run_lease = $2",
            )
            .bind(claim.job.id)
            .bind(claim.job.run_lease)
            .execute(&self.pool)
            .await
            .expect("age lease heartbeat");
            sqlx::query(
                "INSERT INTO awa.attempt_state (job_id, run_lease, heartbeat_at, updated_at)
                 VALUES ($1, $2, clock_timestamp() - interval '1 minute', clock_timestamp())
                 ON CONFLICT (job_id, run_lease) DO UPDATE
                 SET heartbeat_at = EXCLUDED.heartbeat_at",
            )
            .bind(claim.job.id)
            .bind(claim.job.run_lease)
            .execute(&self.pool)
            .await
            .expect("age attempt heartbeat");
        }

        async fn drop(self) {
            self.pool.close().await;
            let admin = db::connect_pool(&self.config.admin_url(), "slot_probe_cleanup", 1)
                .await
                .expect("cleanup connection");
            sqlx::query(&format!(
                "DROP DATABASE {} WITH (FORCE)",
                self.config.database_name()
            ))
            .execute(&admin)
            .await
            .expect("drop isolated database");
            admin.close().await;
        }
    }

    #[tokio::test]
    async fn slot_probe_keeps_materialized_running_claims_until_durable_completion() {
        let run = SlotProbe::create().await;
        // Zero-deadline receipts are compact; deadline receipts are row-local.
        for deadline in [Duration::ZERO, Duration::from_secs(3600)] {
            let claim = run.claim(deadline).await;
            run.store
                .register_callback(
                    &run.pool,
                    claim.job.id,
                    claim.job.run_lease,
                    Duration::from_secs(3600),
                )
                .await
                .expect("materialize the running receipt");
            let slot = claim.claim.claim_slot;
            run.assert_counts(slot, 1, 0).await;
            run.assert_counts((slot + 1) % 8, 0, 0).await;
            run.age_heartbeat(&claim).await;
            run.assert_counts(slot, 1, 1).await;
            run.store
                .heartbeat_batch(&run.pool, &[(claim.job.id, claim.job.run_lease)])
                .await
                .expect("refresh authoritative attempt heartbeat");
            // The stale leases.heartbeat_at must not override attempt_state.
            run.assert_counts(slot, 1, 0).await;
            run.store
                .complete_runtime_batch(&run.pool, std::slice::from_ref(&claim))
                .await
                .expect("durably complete");
            run.assert_counts(slot, 0, 0).await;
        }
        run.drop().await;
    }

    #[tokio::test]
    async fn slot_probe_uses_callback_timeout_for_waiting_materialized_claims() {
        let run = SlotProbe::create().await;
        for deadline in [Duration::ZERO, Duration::from_secs(3600)] {
            let claim = run.claim(deadline).await;
            let callback = run
                .store
                .register_callback(
                    &run.pool,
                    claim.job.id,
                    claim.job.run_lease,
                    Duration::from_secs(3600),
                )
                .await
                .expect("materialize the receipt");
            assert!(run
                .store
                .enter_callback_wait(&run.pool, claim.job.id, claim.job.run_lease, callback)
                .await
                .expect("wait for callback"));
            run.age_heartbeat(&claim).await;
            sqlx::query(
                "UPDATE awa.leases SET deadline_at = clock_timestamp() - interval '1 second'
                 WHERE job_id = $1 AND run_lease = $2",
            )
            .bind(claim.job.id)
            .bind(claim.job.run_lease)
            .execute(&run.pool)
            .await
            .expect("expire the original job deadline");
            let slot = claim.claim.claim_slot;
            // Waiting ignores the old heartbeat and deadline until its callback times out.
            run.assert_counts(slot, 1, 0).await;
            run.assert_counts((slot + 1) % 8, 0, 0).await;
            sqlx::query(
                "UPDATE awa.leases SET callback_timeout_at = clock_timestamp() - interval '1 second'
                 WHERE job_id = $1 AND run_lease = $2",
            )
            .bind(claim.job.id)
            .bind(claim.job.run_lease)
            .execute(&run.pool)
            .await
            .expect("expire callback");
            run.assert_counts(slot, 1, 1).await;
            run.store
                .complete_external(&run.pool, callback, None, Some(claim.job.run_lease), false)
                .await
                .expect("durably complete callback");
            run.assert_counts(slot, 0, 0).await;
        }
        run.drop().await;
    }
}
