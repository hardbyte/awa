//! Resident memory of the soak process against handler executions.
//!
//! Every runtime the personas start lives in this process, so its resident set is the
//! worker-side memory under test. A sampler records it alongside the executions so far;
//! after the warm-up, resident memory must not keep climbing with executions. Growth is
//! the least-squares slope of resident memory over executions, so a bounded transient
//! barely moves it while memory retained per execution shows up however long the run.
//!
//! An optional completion stall holds completions back while handlers keep running. The
//! window from the stall to as long again after it is left out of the slope and checked on
//! its own: resident memory may rise by at most a fixed amount, however long the stall.
//!
//! The harness's own ledger also lives here and grows with every job enqueued. When the
//! runtimes fall behind the producers, as on a host short of CPU, that growth lands on few
//! executions and would read as a leak, so the slope is then reported as not evaluated.

use super::config::Persona;
use super::shared::Shared;
use super::stop::Stop;
use sqlx::Connection;
use std::fmt::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct MemorySample {
    pub at_ms: i64,
    pub load: bool,
    pub executions: i64,
    pub ledger_jobs: i64,
    pub rss_kib: i64,
    pub hwm_kib: i64,
    pub rss_anon_kib: i64,
}

#[derive(Default)]
pub struct MemoryLog {
    samples: Mutex<Vec<MemorySample>>,
    verdict: Mutex<Option<MemoryVerdict>>,
    stall_ms: Mutex<Option<(i64, i64)>>,
    stall_started: Mutex<Option<std::time::Duration>>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct MemoryVerdict {
    pub samples: usize,
    pub executions_evaluated: i64,
    pub enqueued_evaluated: i64,
    /// Why the slope was not evaluated, when it was not.
    pub not_evaluated: Option<String>,
    pub slope_mib_per_100k: Option<f64>,
    pub limit_mib_per_100k: f64,
    pub start_rss_mib: f64,
    pub end_rss_mib: f64,
    pub peak_hwm_mib: f64,
    pub stall_rise_mib: Option<f64>,
    pub stall_limit_mib: f64,
}

impl MemoryLog {
    pub fn samples(&self) -> Vec<MemorySample> {
        self.samples.lock().expect("memory samples").clone()
    }

    /// Length of the completion stall once it has started, zero otherwise.
    pub fn stalled_for(&self) -> std::time::Duration {
        match *self.stall_started.lock().expect("stall start") {
            Some(hold) => hold,
            None => std::time::Duration::ZERO,
        }
    }

    pub fn verdict(&self) -> Option<MemoryVerdict> {
        self.verdict.lock().expect("memory verdict").clone()
    }

    fn push(&self, sample: MemorySample) {
        self.samples.lock().expect("memory samples").push(sample);
    }
}

fn status_kib(status: &str, field: &str) -> i64 {
    status
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn executions(shared: &Shared) -> i64 {
    Persona::ALL
        .into_iter()
        .filter(|persona| shared.config.has(*persona))
        .map(|persona| shared.stats(persona).get("executions"))
        .sum()
}

/// Samples until `stop`. `/proc/self/status` exists only on Linux; elsewhere nothing is
/// recorded and the gate reports that it could not evaluate.
pub async fn sample(shared: Arc<Shared>, stop: Stop) {
    loop {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            shared.memory.push(MemorySample {
                at_ms: shared.now_ms(),
                load: !shared.load_stop.is_stopped(),
                executions: executions(&shared),
                ledger_jobs: shared.ledger.len() as i64,
                rss_kib: status_kib(&status, "VmRSS:"),
                hwm_kib: status_kib(&status, "VmHWM:"),
                rss_anon_kib: status_kib(&status, "RssAnon:"),
            });
        }
        if stop.sleep(shared.config.memory_sample_every).await {
            return;
        }
    }
}

/// Halfway through the load, holds the queue-storage completion tables in SHARE mode for
/// `completion_stall`: completions cannot commit while claims and handlers carry on, the
/// shape in which a runtime that buffers finished attempts grows without bound.
pub async fn stall_completions(shared: Arc<Shared>) {
    let hold = shared.config.completion_stall;
    if hold.is_zero() {
        return;
    }
    let Some(schema) = shared.info.queue_storage_schema.clone() else {
        shared.note(
            "memory",
            "could not stall completions: no queue-storage schema",
        );
        return;
    };
    if shared.load_stop.sleep(shared.config.load / 2).await {
        return;
    }
    let stalled = async {
        let mut connection = sqlx::PgConnection::connect(&shared.config.database_url).await?;
        let mut transaction = connection.begin().await?;
        sqlx::query(&format!(
            "LOCK TABLE {schema}.lease_claim_closures, {schema}.receipt_completion_batches \
             IN SHARE MODE"
        ))
        .execute(&mut *transaction)
        .await?;
        *shared.memory.stall_started.lock().expect("stall start") = Some(hold);
        shared.chaos_window(hold + shared.config.chaos_slack);
        shared.note(
            "memory",
            format!("completions stalled for {}s", hold.as_secs()),
        );
        let stalled_at = shared.now_ms();
        shared.load_stop.sleep(hold).await;
        let released = transaction.rollback().await;
        *shared.memory.stall_ms.lock().expect("stall window") = Some((stalled_at, shared.now_ms()));
        released
    }
    .await;
    match stalled {
        Ok(()) => shared.note("memory", "completions released"),
        Err(error) => shared.note("memory", format!("could not stall completions: {error}")),
    }
}

/// Least-squares slope of y over x shared by every segment, each with its own intercept,
/// so a step between segments does not count as growth.
fn slope(segments: &[Vec<(f64, f64)>]) -> Option<f64> {
    let mut covariance = 0.0;
    let mut spread = 0.0;
    let mut points = 0;
    for segment in segments.iter().filter(|segment| segment.len() >= 2) {
        let count = segment.len() as f64;
        let mean_x = segment.iter().map(|(x, _)| x).sum::<f64>() / count;
        let mean_y = segment.iter().map(|(_, y)| y).sum::<f64>() / count;
        covariance += segment
            .iter()
            .map(|(x, y)| (x - mean_x) * (y - mean_y))
            .sum::<f64>();
        spread += segment
            .iter()
            .map(|(x, _)| (x - mean_x).powi(2))
            .sum::<f64>();
        points += segment.len();
    }
    (points >= 3 && spread > 0.0).then(|| covariance / spread)
}

/// Records the gate's verdict and raises `memory_growth` when resident memory grows
/// faster than the configured limit per 100k executions after the warm-up.
pub fn evaluate(shared: &Shared, load_started_ms: i64) {
    let samples = shared.memory.samples();
    let (Some(first), Some(last)) = (samples.first().copied(), samples.last().copied()) else {
        shared.note(
            "memory",
            "no memory samples: /proc/self/status is unavailable",
        );
        return;
    };
    let warm_from = load_started_ms + shared.config.memory_warmup.as_millis() as i64;
    let stall = *shared.memory.stall_ms.lock().expect("stall window");
    let in_stall = |at_ms: i64| {
        stall.is_some_and(|(start, end)| at_ms >= start && at_ms <= end + (end - start))
    };
    let steady: Vec<&MemorySample> = samples
        .iter()
        .filter(|sample| sample.load && sample.at_ms >= warm_from && !in_stall(sample.at_ms))
        .collect();
    let stall_rise_mib = stall.and_then(|(start, _)| {
        let before = samples.iter().rev().find(|sample| sample.at_ms <= start)?;
        let peak = samples
            .iter()
            .filter(|sample| in_stall(sample.at_ms))
            .map(|sample| sample.rss_kib)
            .max()?;
        Some((peak - before.rss_kib) as f64 / 1024.0)
    });
    let (before_stall, after_stall): (Vec<&MemorySample>, Vec<&MemorySample>) = steady
        .iter()
        .partition(|sample| stall.is_none_or(|(start, _)| sample.at_ms < start));
    let segments: Vec<Vec<(f64, f64)>> = [&before_stall, &after_stall]
        .iter()
        .map(|segment| {
            segment
                .iter()
                .map(|sample| (sample.executions as f64, sample.rss_kib as f64))
                .collect()
        })
        .collect();
    let executions_evaluated: i64 = [&before_stall, &after_stall]
        .iter()
        .filter_map(|segment| Some(segment.last()?.executions - segment.first()?.executions))
        .sum();
    let enqueued_evaluated: i64 = [&before_stall, &after_stall]
        .iter()
        .filter_map(|segment| Some(segment.last()?.ledger_jobs - segment.first()?.ledger_jobs))
        .sum();
    let not_evaluated = if executions_evaluated < shared.config.memory_min_executions {
        Some(format!(
            "{executions_evaluated} executions after the warm-up, {} needed",
            shared.config.memory_min_executions
        ))
    } else if enqueued_evaluated * 4 > executions_evaluated * 5 {
        Some(format!(
            "the runtimes fell behind the producers ({enqueued_evaluated} jobs enqueued, \
             {executions_evaluated} executions after the warm-up), so the harness ledger's \
             growth would dominate the slope"
        ))
    } else {
        None
    };
    let slope_mib_per_100k = if not_evaluated.is_none() {
        slope(&segments).map(|kib_per_execution| kib_per_execution * 100_000.0 / 1024.0)
    } else {
        None
    };
    let verdict = MemoryVerdict {
        samples: samples.len(),
        executions_evaluated,
        enqueued_evaluated,
        not_evaluated,
        slope_mib_per_100k,
        limit_mib_per_100k: shared.config.memory_slope_limit_mib_per_100k,
        start_rss_mib: first.rss_kib as f64 / 1024.0,
        end_rss_mib: last.rss_kib as f64 / 1024.0,
        peak_hwm_mib: samples
            .iter()
            .map(|sample| sample.hwm_kib)
            .max()
            .unwrap_or(0) as f64
            / 1024.0,
        stall_rise_mib,
        stall_limit_mib: shared.config.completion_stall_memory_limit_mib,
    };
    match verdict.stall_rise_mib {
        // A stall that was asked for but never held would otherwise pass unseen.
        None if !shared.config.completion_stall.is_zero() => shared.violation(
            "memory_growth_under_stall",
            format!(
                "AWA_SOAK_COMPLETION_STALL_SECONDS={} was set but no stall was measured; \
                 see the memory notes in the timeline",
                shared.config.completion_stall.as_secs()
            ),
            vec![],
        ),
        Some(rise) if rise > verdict.stall_limit_mib => shared.violation(
            "memory_growth_under_stall",
            format!(
                "resident memory rose {rise:.0} MiB while completions were stalled \
                 (limit {:.0}); finished attempts are piling up in memory",
                verdict.stall_limit_mib
            ),
            vec![],
        ),
        Some(rise) => shared.note(
            "memory",
            format!(
                "resident memory rose {rise:.0} MiB while completions were stalled (limit {:.0})",
                verdict.stall_limit_mib
            ),
        ),
        None => {}
    }
    match verdict.slope_mib_per_100k {
        Some(slope) if slope > verdict.limit_mib_per_100k => shared.violation(
            "memory_growth",
            format!(
                "resident memory grew {slope:.1} MiB per 100k executions over {} executions \
                 after the warm-up (limit {:.1}); see memory.csv",
                verdict.executions_evaluated, verdict.limit_mib_per_100k
            ),
            vec![],
        ),
        Some(slope) => shared.note(
            "memory",
            format!(
                "resident memory slope {slope:.1} MiB per 100k executions over {} executions \
                 (limit {:.1})",
                verdict.executions_evaluated, verdict.limit_mib_per_100k
            ),
        ),
        None => shared.note(
            "memory",
            format!(
                "memory growth not evaluated: {}",
                verdict.not_evaluated.as_deref().unwrap_or("no slope")
            ),
        ),
    }
    *shared.memory.verdict.lock().expect("memory verdict") = Some(verdict);
}

pub fn csv(samples: &[MemorySample]) -> String {
    let mut out = String::from("at_s,phase,executions,ledger_jobs,rss_kib,hwm_kib,rss_anon_kib\n");
    for sample in samples {
        let _ = writeln!(
            out,
            "{:.1},{},{},{},{},{},{}",
            sample.at_ms as f64 / 1000.0,
            if sample.load { "load" } else { "drain" },
            sample.executions,
            sample.ledger_jobs,
            sample.rss_kib,
            sample.hwm_kib,
            sample.rss_anon_kib
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::slope;

    fn line(from: usize, to: usize, intercept: f64, per_execution: f64) -> Vec<(f64, f64)> {
        (from..to)
            .map(|step| {
                let executions = step as f64 * 1_000.0;
                (executions, intercept + executions * per_execution)
            })
            .collect()
    }

    #[test]
    fn slope_recovers_linear_growth() {
        let kib_per_execution = slope(&[line(0, 10, 50_000.0, 0.17)]).expect("slope");
        assert!((kib_per_execution - 0.17).abs() < 1e-9);
    }

    #[test]
    fn slope_ignores_a_step_between_segments() {
        let segments = [line(0, 10, 50_000.0, 0.0), line(20, 30, 90_000.0, 0.0)];
        assert_eq!(slope(&segments), Some(0.0));
        let growing = [line(0, 10, 50_000.0, 0.17), line(20, 30, 90_000.0, 0.17)];
        assert!((slope(&growing).expect("slope") - 0.17).abs() < 1e-9);
    }

    #[test]
    fn slope_needs_spread_in_executions() {
        assert_eq!(slope(&[vec![(5.0, 1.0), (5.0, 2.0), (5.0, 3.0)]]), None);
        assert_eq!(slope(&[vec![(1.0, 1.0), (2.0, 2.0)]]), None);
    }
}
