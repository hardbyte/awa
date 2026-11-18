//! Latency budgets for the admin reads an operator and the UI depend on: `get_job`,
//! `find_jobs`, `queue_overview` and the state counts. The operator persona's library
//! calls and the UI personas' HTTP requests are both timed; each source is judged
//! against its own p99 budget after a warm-up. Samples taken during a declared disruption
//! are judged too, since chaos runs through most of a default soak, and counted apart.

use super::config::ADMIN_READS;
use super::intent::{Intent, IntentSpan};
use super::rng::percentile;
use super::shared::Shared;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

const MAX_SAMPLES: usize = 200_000;

/// The admin read a timed call stands for, by the operator's call name.
pub fn library_read(name: &str) -> Option<&'static str> {
    match name {
        "admin_get_job" => Some("get_job"),
        "admin_list_jobs" => Some("find_jobs"),
        "admin_queue_overviews" | "admin_queue_overview" => Some("queue_overview"),
        "admin_state_counts" => Some("counts"),
        _ => None,
    }
}

/// The admin read an HTTP endpoint stands for, by the UI's endpoint name.
pub fn http_read(endpoint: &str) -> Option<&'static str> {
    match endpoint {
        "GET /api/jobs/{id}" => Some("get_job"),
        "GET /api/jobs" | "GET /api/jobs (first page)" | "GET /api/jobs (later page)" => {
            Some("find_jobs")
        }
        "GET /api/queues" => Some("queue_overview"),
        "GET /api/stats" => Some("counts"),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub read: &'static str,
    pub source: &'static str,
    pub at_ms: i64,
    pub latency_ms: i64,
}

#[derive(Default)]
pub struct AdminLatency {
    samples: Mutex<Vec<Sample>>,
}

impl AdminLatency {
    pub fn observe(&self, read: &'static str, source: &'static str, at_ms: i64, latency_ms: i64) {
        let mut samples = self.samples.lock().expect("admin latency");
        if samples.len() < MAX_SAMPLES {
            samples.push(Sample {
                read,
                source,
                at_ms,
                latency_ms,
            });
        }
    }

    pub fn samples(&self) -> Vec<Sample> {
        self.samples.lock().expect("admin latency").clone()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyRow {
    pub read: String,
    pub source: String,
    pub samples: usize,
    /// Samples before the warm-up, left out.
    pub excluded: usize,
    /// Judged samples taken while a disruption was declared.
    pub during_disruption: usize,
    pub p50_ms: i64,
    pub p99_ms: i64,
    pub max_ms: i64,
    pub budget_ms: i64,
    /// Whether enough samples remained to judge.
    pub judged: bool,
    pub over_budget: bool,
    /// The slowest judged samples, `(at_s, ms)`.
    pub slowest: Vec<(f64, i64)>,
}

pub fn judge(
    samples: &[Sample],
    spans: &[IntentSpan],
    warm_from_ms: i64,
    min_samples: usize,
    budgets: &BTreeMap<&'static str, &BTreeMap<String, i64>>,
) -> Vec<LatencyRow> {
    let mut rows = Vec::new();
    for (source, source_budgets) in budgets {
        for read in ADMIN_READS {
            let mut excluded = 0;
            let mut during_disruption = 0;
            let mut kept: Vec<&Sample> = Vec::new();
            for sample in samples
                .iter()
                .filter(|sample| sample.read == read && sample.source == *source)
            {
                let disrupted = spans.iter().any(|span| {
                    span.intent == Intent::Disruption
                        && span.queue.is_none()
                        && span.overlaps(sample.at_ms, sample.at_ms + sample.latency_ms, 0)
                });
                if sample.at_ms < warm_from_ms {
                    excluded += 1;
                } else {
                    during_disruption += usize::from(disrupted);
                    kept.push(sample);
                }
            }
            if kept.is_empty() && excluded == 0 {
                continue;
            }
            let mut values: Vec<i64> = kept.iter().map(|sample| sample.latency_ms).collect();
            values.sort_unstable();
            let budget_ms = source_budgets.get(read).copied().unwrap_or(i64::MAX);
            let judged = values.len() >= min_samples;
            let p99_ms = percentile(&values, 0.99);
            let mut slowest: Vec<(f64, i64)> = kept
                .iter()
                .map(|sample| (sample.at_ms as f64 / 1000.0, sample.latency_ms))
                .collect();
            slowest.sort_by_key(|sample| std::cmp::Reverse(sample.1));
            slowest.truncate(5);
            rows.push(LatencyRow {
                read: read.to_string(),
                source: source.to_string(),
                samples: values.len(),
                excluded,
                during_disruption,
                p50_ms: percentile(&values, 0.5),
                p99_ms,
                max_ms: values.last().copied().unwrap_or(0),
                budget_ms,
                judged,
                over_budget: judged && p99_ms > budget_ms,
                slowest,
            });
        }
    }
    rows
}

pub fn evaluate(shared: &Shared) -> Vec<LatencyRow> {
    let checks = &shared.config.checks;
    let budgets = BTreeMap::from([
        ("library", &checks.admin_budgets_ms),
        ("http", &checks.admin_http_budgets_ms),
    ]);
    let rows = judge(
        &shared.admin_latency.samples(),
        &shared.intents.spans(),
        shared.load_started_ms() + checks.admin_warmup.as_millis() as i64,
        checks.admin_min_samples,
        &budgets,
    );
    for row in rows.iter().filter(|row| row.over_budget) {
        shared.violation_at(
            "admin_latency_budget",
            row.slowest
                .first()
                .map_or(0, |slowest| (slowest.0 * 1000.0) as i64),
            format!(
                "{} over {}: p99 {} ms over {} samples (budget {} ms)",
                row.read, row.source, row.p99_ms, row.samples, row.budget_ms
            ),
            vec![serde_json::to_string(row).unwrap_or_default()],
        );
    }
    rows
}

pub fn render(out: &mut String, rows: &[LatencyRow], shared: &Shared) {
    let checks = &shared.config.checks;
    let _ = writeln!(out, "## Admin latency");
    let _ = writeln!(
        out,
        "p99 per admin read after a {} s warm-up, judged from {} samples; samples taken during a declared disruption are judged and counted apart.\n",
        checks.admin_warmup.as_secs(),
        checks.admin_min_samples
    );
    if rows.is_empty() {
        let _ = writeln!(
            out,
            "- no admin reads were timed (needs the alex or a UI persona)\n"
        );
        return;
    }
    let _ = writeln!(
        out,
        "| read | source | n | in disruption | warm-up left out | p50 ms | p99 ms | max ms | budget ms | verdict |"
    );
    let _ = writeln!(
        out,
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for row in rows {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            row.read,
            row.source,
            row.samples,
            row.during_disruption,
            row.excluded,
            row.p50_ms,
            row.p99_ms,
            row.max_ms,
            row.budget_ms,
            if !row.judged {
                "too few samples"
            } else if row.over_budget {
                "OVER"
            } else {
                "ok"
            }
        );
    }
    let _ = writeln!(out);
}

#[cfg(test)]
mod tests {
    use super::super::intent::IntentLog;
    use super::*;

    fn samples(
        read: &'static str,
        source: &'static str,
        latencies: impl Iterator<Item = (i64, i64)>,
    ) -> Vec<Sample> {
        latencies
            .map(|(at_s, latency_ms)| Sample {
                read,
                source,
                at_ms: at_s * 1000,
                latency_ms,
            })
            .collect()
    }

    #[test]
    fn p99_over_budget_fails_and_the_warm_up_is_left_out() {
        let budgets_library = BTreeMap::from([
            ("get_job".to_string(), 200),
            ("find_jobs".to_string(), 1_000),
        ]);
        let budgets = BTreeMap::from([("library", &budgets_library)]);
        let log = IntentLog::default();
        log.window(None, Intent::Disruption, "leader killed", 200_000, 210_000);
        let spans = log.spans();
        // get_job: fast, apart from a slow warm-up and two slow reads during the disruption.
        let mut all = samples("get_job", "library", (0..60).map(|at| (at, 900)));
        all.extend(samples(
            "get_job",
            "library",
            (60..400).map(|at| (at, if (200..=201).contains(&at) { 5_000 } else { 20 })),
        ));
        // find_jobs: one in fifty takes 1.5 s, so p99 is over budget.
        all.extend(samples(
            "find_jobs",
            "library",
            (60..400).map(|at| (at, if at % 50 == 0 { 1_500 } else { 80 })),
        ));
        let rows = judge(&all, &spans, 60_000, 20, &budgets);
        let get_job = rows.iter().find(|row| row.read == "get_job").unwrap();
        assert!(!get_job.over_budget, "{get_job:?}");
        assert_eq!(get_job.excluded, 60);
        assert_eq!(get_job.during_disruption, 11);
        let find_jobs = rows.iter().find(|row| row.read == "find_jobs").unwrap();
        assert!(find_jobs.over_budget, "{find_jobs:?}");
        assert_eq!(find_jobs.p99_ms, 1_500);
    }

    #[test]
    fn too_few_samples_are_reported_not_judged() {
        let budgets_library = BTreeMap::from([("counts".to_string(), 10)]);
        let budgets = BTreeMap::from([("library", &budgets_library)]);
        let all = samples("counts", "library", (0..5).map(|at| (at, 5_000)));
        let rows = judge(&all, &[], 0, 20, &budgets);
        assert_eq!(rows.len(), 1);
        assert!(!rows[0].judged && !rows[0].over_budget);
    }

    #[test]
    fn names_map_to_reads() {
        assert_eq!(library_read("admin_list_jobs"), Some("find_jobs"));
        assert_eq!(http_read("GET /api/jobs/{id}"), Some("get_job"));
        assert_eq!(http_read("POST /api/jobs/{id}/cancel"), None);
    }
}
