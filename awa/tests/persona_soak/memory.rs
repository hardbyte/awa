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
//! its own: resident memory may rise by at most a fixed amount, however long the stall. A
//! stall that was asked for fails the gate unless it held for its whole length and memory
//! samples cover it.
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
    stall: Mutex<Option<Stall>>,
    stall_started: Mutex<Option<std::time::Duration>>,
}

/// What became of a requested completion stall.
#[derive(Clone, Debug, PartialEq)]
enum Stall {
    /// The completion tables were held from `start_ms` to `end_ms`; `cut_short` when the
    /// load ended before the requested length.
    Held {
        start_ms: i64,
        end_ms: i64,
        cut_short: bool,
    },
    /// The stall never took its locks.
    NotStarted(String),
}

impl Stall {
    fn window(&self) -> Option<(i64, i64)> {
        match self {
            Stall::Held {
                start_ms, end_ms, ..
            } => Some((*start_ms, *end_ms)),
            Stall::NotStarted(_) => None,
        }
    }
}

/// The completion-stall half of the gate.
#[derive(Debug, PartialEq)]
enum StallVerdict {
    NotRequested,
    Passed(String),
    Failed(String),
}

/// Judges a requested stall from what became of it and the rise of resident memory over
/// its window, `None` when no sample covers that window.
fn stall_verdict(
    requested: std::time::Duration,
    stall: Option<&Stall>,
    rise_mib: Option<f64>,
    limit_mib: f64,
) -> StallVerdict {
    if requested.is_zero() {
        return StallVerdict::NotRequested;
    }
    let asked = format!("AWA_SOAK_COMPLETION_STALL_SECONDS={}", requested.as_secs());
    match (stall, rise_mib) {
        (None, _) => StallVerdict::Failed(format!(
            "{asked} was set but the stall never ran; see the memory notes in the timeline"
        )),
        (Some(Stall::NotStarted(reason)), _) => StallVerdict::Failed(format!(
            "{asked} was set but the stall could not start: {reason}"
        )),
        (
            Some(Stall::Held {
                start_ms,
                end_ms,
                cut_short: true,
            }),
            _,
        ) => StallVerdict::Failed(format!(
            "{asked} was set but completions were held for only {:.1}s: the load ended \
             first, so the stall did not apply its pressure; shorten it below half of \
             AWA_SOAK_SECONDS",
            (end_ms - start_ms) as f64 / 1000.0
        )),
        (Some(Stall::Held { .. }), None) => StallVerdict::Failed(format!(
            "{asked} was set but no resident memory sample covers the stall, so its memory \
             was not measured; sampling needs a readable /proc/self/status and \
             AWA_SOAK_MEMORY_SAMPLE_SECONDS shorter than the stall"
        )),
        (Some(Stall::Held { .. }), Some(rise)) if rise > limit_mib => {
            StallVerdict::Failed(format!(
                "resident memory rose {rise:.0} MiB while completions were stalled \
                 (limit {limit_mib:.0}); finished attempts are piling up in memory"
            ))
        }
        (Some(Stall::Held { .. }), Some(rise)) => StallVerdict::Passed(format!(
            "resident memory rose {rise:.0} MiB while completions were stalled (limit {limit_mib:.0})"
        )),
    }
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
///
/// Connecting and taking the locks share `completion_stall_acquire_timeout`, enforced by
/// the server's `lock_timeout` and by the client, so a transaction that holds a
/// conflicting lock makes the stall give up instead of hanging the run. The hold ends early
/// when the load does, since the drain needs completions to commit; the gate then fails.
pub async fn stall_completions(shared: Arc<Shared>) {
    let hold = shared.config.completion_stall;
    if hold.is_zero() {
        return;
    }
    let Some(schema) = shared.info.queue_storage_schema.clone() else {
        not_started(&shared, "no queue-storage schema".to_string());
        return;
    };
    if shared.load_stop.sleep(shared.config.load / 2).await {
        not_started(&shared, "the load ended before its midpoint".to_string());
        return;
    }
    let acquire = shared.config.completion_stall_acquire_timeout;
    let deadline = tokio::time::Instant::now() + acquire;
    let connection = tokio::time::timeout_at(
        deadline,
        sqlx::PgConnection::connect(&shared.config.database_url),
    )
    .await;
    let mut connection = match bounded("connect", acquire, connection) {
        Ok(connection) => connection,
        Err(reason) => return not_started(&shared, reason),
    };
    let held = async {
        let mut transaction = bounded(
            "begin",
            acquire,
            tokio::time::timeout_at(deadline, connection.begin()).await,
        )?;
        bounded(
            "set lock_timeout",
            acquire,
            tokio::time::timeout_at(
                deadline,
                sqlx::query(awa::audited_sql(format!(
                    "SET LOCAL lock_timeout = '{}ms'",
                    acquire.as_millis().max(1)
                )))
                .execute(&mut *transaction),
            )
            .await,
        )?;
        bounded(
            "lock the completion tables",
            acquire,
            tokio::time::timeout_at(
                deadline,
                sqlx::query(awa::audited_sql(format!(
                    "LOCK TABLE {schema}.lease_claim_closures, \
                     {schema}.receipt_completion_batches IN SHARE MODE"
                )))
                .execute(&mut *transaction),
            )
            .await,
        )?;
        Ok::<_, String>(transaction)
    }
    .await;
    let transaction = match held {
        Ok(transaction) => transaction,
        Err(reason) => return not_started(&shared, reason),
    };
    *shared.memory.stall_started.lock().expect("stall start") = Some(hold);
    shared.chaos_window(hold + shared.config.chaos_slack);
    shared.note(
        "memory",
        format!("completions stalled for {}s", hold.as_secs()),
    );
    let start_ms = shared.now_ms();
    let cut_short = shared.load_stop.sleep(hold).await;
    let released = transaction.rollback().await;
    *shared.memory.stall.lock().expect("stall") = Some(Stall::Held {
        start_ms,
        end_ms: shared.now_ms(),
        cut_short,
    });
    match released {
        Ok(()) => shared.note("memory", "completions released"),
        Err(error) => shared.note(
            "memory",
            format!("completions released by closing the connection: {error}"),
        ),
    }
}

/// The outcome of one step of taking the stall's locks within `acquire`.
fn bounded<T>(
    what: &str,
    acquire: std::time::Duration,
    result: Result<Result<T, sqlx::Error>, tokio::time::error::Elapsed>,
) -> Result<T, String> {
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(format!("{what}: {error}")),
        Err(_) => Err(format!(
            "{what}: no answer within {}ms",
            acquire.as_millis()
        )),
    }
}

fn not_started(shared: &Shared, reason: String) {
    shared.note("memory", format!("could not stall completions: {reason}"));
    *shared.memory.stall.lock().expect("stall") = Some(Stall::NotStarted(reason));
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
    let stall_record = shared.memory.stall.lock().expect("stall").clone();
    let stall = stall_record.as_ref().and_then(Stall::window);
    let in_stall = |at_ms: i64| {
        stall.is_some_and(|(start, end)| at_ms >= start && at_ms <= end + (end - start))
    };
    let stall_rise_mib = stall.and_then(|(start, _)| {
        let before = samples.iter().rev().find(|sample| sample.at_ms <= start)?;
        let peak = samples
            .iter()
            .filter(|sample| in_stall(sample.at_ms))
            .map(|sample| sample.rss_kib)
            .max()?;
        Some((peak - before.rss_kib) as f64 / 1024.0)
    });
    match stall_verdict(
        shared.config.completion_stall,
        stall_record.as_ref(),
        stall_rise_mib,
        shared.config.completion_stall_memory_limit_mib,
    ) {
        StallVerdict::NotRequested => {}
        StallVerdict::Passed(text) => shared.note("memory", text),
        StallVerdict::Failed(summary) => {
            shared.violation("memory_growth_under_stall", summary, vec![])
        }
    }
    let (Some(first), Some(last)) = (samples.first().copied(), samples.last().copied()) else {
        shared.note(
            "memory",
            "no memory samples: /proc/self/status is unavailable",
        );
        return;
    };
    let warm_from = load_started_ms + shared.config.memory_warmup.as_millis() as i64;
    let steady: Vec<&MemorySample> = samples
        .iter()
        .filter(|sample| sample.load && sample.at_ms >= warm_from && !in_stall(sample.at_ms))
        .collect();
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
    use super::{slope, stall_verdict, Stall, StallVerdict};
    use std::time::Duration;

    const ASKED: Duration = Duration::from_secs(60);
    const HELD: Stall = Stall::Held {
        start_ms: 1_000,
        end_ms: 61_000,
        cut_short: false,
    };

    fn failed(verdict: StallVerdict, expected: &str) {
        match verdict {
            StallVerdict::Failed(summary) => {
                assert!(summary.contains(expected), "{summary:?} lacks {expected:?}")
            }
            other => panic!("expected the stall gate to fail, got {other:?}"),
        }
    }

    #[test]
    fn stall_verdict_is_silent_when_no_stall_was_asked_for() {
        assert_eq!(
            stall_verdict(Duration::ZERO, None, None, 128.0),
            StallVerdict::NotRequested
        );
    }

    #[test]
    fn stall_verdict_fails_a_stall_that_never_held() {
        failed(stall_verdict(ASKED, None, None, 128.0), "never ran");
        let refused = Stall::NotStarted("lock the completion tables: no answer".into());
        failed(
            stall_verdict(ASKED, Some(&refused), None, 128.0),
            "could not start: lock the completion tables",
        );
    }

    #[test]
    fn stall_verdict_fails_a_stall_the_load_cut_short() {
        let cut = Stall::Held {
            start_ms: 1_000,
            end_ms: 3_500,
            cut_short: true,
        };
        failed(
            stall_verdict(ASKED, Some(&cut), Some(1.0), 128.0),
            "held for only 2.5s",
        );
    }

    #[test]
    fn stall_verdict_fails_a_stall_without_memory_samples() {
        failed(
            stall_verdict(ASKED, Some(&HELD), None, 128.0),
            "no resident memory sample covers the stall",
        );
    }

    #[test]
    fn stall_verdict_judges_the_rise_of_a_full_stall() {
        failed(
            stall_verdict(ASKED, Some(&HELD), Some(200.0), 128.0),
            "rose 200 MiB",
        );
        assert!(matches!(
            stall_verdict(ASKED, Some(&HELD), Some(20.0), 128.0),
            StallVerdict::Passed(_)
        ));
    }

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
