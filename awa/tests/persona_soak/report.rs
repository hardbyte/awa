//! The run report and, for a failed run, the reproducer.

use super::config::{Persona, SoakConfig};
use super::pgserver::crash_excerpts;
use super::shared::Shared;
use serde_json::json;
use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;

/// Printed at the top of every report; also lets a built binary be matched to this harness.
pub const HEADER: &str = "AWA PERSONA SOAK HARNESS";

pub fn git_revision() -> String {
    Command::new("git")
        .args(["rev-parse", "--short=10", "HEAD"])
        .output()
        .ok()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

pub struct Outcome {
    pub quiesced: bool,
    pub elapsed_seconds: f64,
    pub crash_excerpts: Vec<String>,
    pub log_tail: Option<String>,
}

pub fn render(shared: &Shared, outcome: &Outcome) -> String {
    let config = &shared.config;
    let info = &shared.info;
    let violations = shared.violations.lock().expect("violations");
    let mut out = String::new();
    let _ = writeln!(out, "# {HEADER}");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "verdict: {}",
        if violations
            .iter()
            .any(|violation| violation.kind != "oracle_incomplete")
        {
            "FAIL"
        } else if shared
            .oracle_incomplete
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            "INCONCLUSIVE"
        } else {
            "PASS"
        }
    );
    let _ = writeln!(
        out,
        "engine: {} (schema {})",
        info.engine,
        info.queue_storage_schema.as_deref().unwrap_or("-")
    );
    let _ = writeln!(out, "server: PostgreSQL {}", info.version);
    let _ = writeln!(
        out,
        "max_connections: {} (usable by personas: {})",
        info.max_connections,
        shared.budget.usable()
    );
    let _ = writeln!(out, "connections at start: {}", info.connections_at_start);
    let _ = writeln!(out, "awa: {} @ {}", info.awa_version, git_revision());
    let _ = writeln!(
        out,
        "seed: {}  personas: {}  load: {}s  scale: {}  elapsed: {:.0}s  quiesced: {}",
        config.seed,
        config
            .personas
            .iter()
            .map(|persona| persona.name())
            .collect::<Vec<_>>()
            .join(","),
        config.load.as_secs(),
        config.scale,
        outcome.elapsed_seconds,
        outcome.quiesced
    );
    let _ = writeln!(out, "ledger jobs: {}", shared.ledger.len());
    let _ = writeln!(out, "NO_EVENTS durable loss detection: job_id % {} = 0; other returned NO_EVENTS jobs are excluded from terminal-commit proof.", super::ledger::TERMINAL_PROOF_STRIDE);
    let _ = writeln!(out);

    let _ = writeln!(out, "## Connection budget");
    for (label, count) in shared.budget.table() {
        let _ = writeln!(out, "- {label}: {count}");
    }
    let _ = writeln!(out);

    for persona in Persona::ALL {
        if !config.has(persona) {
            continue;
        }
        let stats = shared.stats(persona);
        let counters = stats.counters();
        let _ = writeln!(out, "## {}", persona.name());
        for (name, value) in counters {
            let _ = writeln!(out, "- {name}: {value}");
        }
        for (name, summary) in stats.summaries() {
            let _ = writeln!(
                out,
                "- {name}: n={} p50={} p99={} max={}",
                summary.count, summary.p50, summary.p99, summary.max
            );
        }
        if persona == Persona::ClaimAbandon {
            let _ = writeln!(out, "{}", shared.abandon.render());
        }
        let _ = writeln!(out);
    }

    let _ = writeln!(out, "## Run");
    if shared.run.get("claim_ring_lock_capture_records") > 0 {
        let _ = writeln!(
            out,
            "Lock evidence: [claim-ring-locks.jsonl](claim-ring-locks.jsonl)"
        );
    }
    for (name, value) in shared.run.counters() {
        let _ = writeln!(out, "- {name}: {value}");
    }
    for (name, summary) in shared.run.summaries() {
        let _ = writeln!(
            out,
            "- {name}: n={} p50={} p99={} max={}",
            summary.count, summary.p50, summary.p99, summary.max
        );
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Memory");
    match shared.memory.verdict() {
        Some(verdict) => {
            let _ = writeln!(
                out,
                "- resident: start {:.0} MiB, end {:.0} MiB, peak {:.0} MiB ([memory.csv](memory.csv))",
                verdict.start_rss_mib, verdict.end_rss_mib, verdict.peak_hwm_mib
            );
            let _ = match verdict.slope_mib_per_100k {
                Some(slope) => writeln!(
                    out,
                    "- growth after warm-up: {slope:.1} MiB per 100k executions over {} executions (limit {:.1})",
                    verdict.executions_evaluated, verdict.limit_mib_per_100k
                ),
                None => writeln!(
                    out,
                    "- growth after warm-up: not evaluated: {}",
                    verdict.not_evaluated.as_deref().unwrap_or("no slope")
                ),
            };
            if let Some(rise) = verdict.stall_rise_mib {
                let _ = writeln!(
                    out,
                    "- rise while completions were stalled: {rise:.0} MiB (limit {:.0})",
                    verdict.stall_limit_mib
                );
            }
        }
        None => {
            let _ = writeln!(out, "- not sampled");
        }
    }
    let _ = writeln!(out);

    let findings = shared.findings.lock().expect("findings");
    let _ = writeln!(out, "## Findings, not fatal ({})", findings.len());
    for finding in findings.iter() {
        let _ = writeln!(
            out,
            "- [{:.1}s] {}: {}",
            finding.at_ms as f64 / 1000.0,
            finding.kind,
            finding.summary
        );
        for row in finding.rows.iter().take(6) {
            let _ = writeln!(out, "    {row}");
        }
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Violations ({})", violations.len());
    for violation in violations.iter() {
        let _ = writeln!(
            out,
            "- [{:.1}s{}] {}: {}",
            violation.at_ms as f64 / 1000.0,
            if violation.during_chaos {
                ", during chaos"
            } else {
                ""
            },
            violation.kind,
            violation.summary
        );
        for row in violation.rows.iter().take(10) {
            let _ = writeln!(out, "    {row}");
        }
    }
    if !outcome.crash_excerpts.is_empty() {
        let _ = writeln!(out, "\n## Server log: crash lines");
        for excerpt in &outcome.crash_excerpts {
            let _ = writeln!(out, "```\n{excerpt}\n```");
        }
    }
    out
}

/// Writes `report.md` and `report.json`, and for a failed run `reproducer.md`.
pub fn write(shared: &Shared, outcome: &Outcome) -> std::io::Result<()> {
    let dir = &shared.config.report_dir;
    std::fs::create_dir_all(dir)?;
    let markdown = render(shared, outcome);
    println!("\n{markdown}");
    std::fs::write(dir.join("report.md"), &markdown)?;

    let violations = shared.violations.lock().expect("violations").clone();
    let timeline = shared.timeline.lock().expect("timeline").clone();
    let per_persona: serde_json::Map<String, serde_json::Value> = Persona::ALL
        .into_iter()
        .filter(|persona| shared.config.has(*persona))
        .map(|persona| {
            (
                persona.name().to_string(),
                json!({ "counters": shared.stats(persona).counters(), "samples": shared.stats(persona).summaries() }),
            )
        })
        .collect();
    let summary = json!({
        "verdict": if violations.iter().any(|violation| violation.kind != "oracle_incomplete") { "fail" }
            else if shared.oracle_incomplete.load(std::sync::atomic::Ordering::Relaxed) { "inconclusive" } else { "pass" },
        "terminal_proof_sample_stride": super::ledger::TERMINAL_PROOF_STRIDE,
        "oracle_complete": !shared.oracle_incomplete.load(std::sync::atomic::Ordering::Relaxed),
        "seed": shared.config.seed,
        "personas": shared.config.personas.iter().map(|p| p.name()).collect::<Vec<_>>(),
        "engine": shared.info.engine,
        "server_version": shared.info.version,
        "max_connections": shared.info.max_connections,
        "awa_version": shared.info.awa_version,
        "git": git_revision(),
        "elapsed_seconds": outcome.elapsed_seconds,
        "quiesced": outcome.quiesced,
        "personas_stats": per_persona,
        "abandon": shared.abandon.to_json(),
        "run": { "counters": shared.run.counters(), "samples": shared.run.summaries() },
        "memory": shared.memory.verdict(),
        "violations": violations,
        "findings": shared.findings.lock().expect("findings").clone(),
        "timeline": timeline,
    });
    std::fs::write(
        dir.join("memory.csv"),
        super::memory::csv(&shared.memory.samples()),
    )?;
    std::fs::write(
        dir.join("report.json"),
        serde_json::to_string_pretty(&summary).unwrap_or_default(),
    )?;

    if !violations.is_empty() {
        write_reproducer(dir, &shared.config, shared, outcome)?;
    }
    Ok(())
}

fn write_reproducer(
    dir: &Path,
    config: &SoakConfig,
    shared: &Shared,
    outcome: &Outcome,
) -> std::io::Result<()> {
    let violations = shared.violations.lock().expect("violations").clone();
    let timeline = shared.timeline.lock().expect("timeline").clone();
    let mut out = String::new();
    let _ = writeln!(out, "# Reproducer\n");
    let _ = writeln!(out, "Re-run with the same seed, personas and duration:\n");
    let _ = writeln!(
        out,
        "```\nAWA_SOAK_DATABASE_URL=<url of an awa_soak* database> AWA_SOAK_SEED={} AWA_SOAK_PERSONAS={} AWA_SOAK_SECONDS={} AWA_SOAK_SCALE={} \\\n  cargo test --release -p awa --test persona_soak_test -- --ignored --nocapture\n```\n",
        config.seed,
        config.personas.iter().map(|persona| persona.name()).collect::<Vec<_>>().join(","),
        config.load.as_secs(),
        config.scale
    );
    let _ = writeln!(out, "Seed fixes the workload mix and the chaos schedule; timing of races still varies between runs.\n");
    let _ = writeln!(
        out,
        "Engine: {}; PostgreSQL {}; awa {} @ {}\n",
        shared.info.engine,
        shared.info.version,
        shared.info.awa_version,
        git_revision()
    );
    let _ = writeln!(out, "## Violations\n");
    for violation in &violations {
        let persona = violation.kind.as_str();
        let _ = writeln!(
            out,
            "### {persona} at {:.1}s{}\n{}\n",
            violation.at_ms as f64 / 1000.0,
            if violation.during_chaos {
                " (during chaos)"
            } else {
                ""
            },
            violation.summary
        );
        for row in &violation.rows {
            let _ = writeln!(out, "    {row}");
        }
        let nearby: Vec<_> = timeline
            .iter()
            .filter(|event| (event.at_ms - violation.at_ms).abs() <= 60_000)
            .collect();
        if !nearby.is_empty() {
            let _ = writeln!(out, "\nTimeline within 60s:");
            for event in nearby {
                let _ = writeln!(
                    out,
                    "- {:.1}s [{}] {}",
                    event.at_ms as f64 / 1000.0,
                    event.source,
                    event.text
                );
            }
        }
        let _ = writeln!(out);
    }
    let _ = writeln!(out, "## Full timeline\n");
    for event in &timeline {
        let _ = writeln!(
            out,
            "- {:.1}s [{}] {}",
            event.at_ms as f64 / 1000.0,
            event.source,
            event.text
        );
    }
    let _ = writeln!(out, "\n## Server log excerpt\n");
    if !outcome.crash_excerpts.is_empty() {
        for excerpt in &outcome.crash_excerpts {
            let _ = writeln!(out, "```\n{excerpt}\n```");
        }
    } else if let Some(tail) = &outcome.log_tail {
        let interesting: Vec<&str> = tail
            .lines()
            .filter(|line| {
                line.contains("ERROR")
                    || line.contains("FATAL")
                    || line.contains("PANIC")
                    || line.contains("WARNING")
            })
            .collect();
        let start = interesting.len().saturating_sub(80);
        let _ = writeln!(
            out,
            "No crash lines. Last warnings and errors ({} total):\n```",
            interesting.len()
        );
        for line in &interesting[start..] {
            let _ = writeln!(out, "{line}");
        }
        let _ = writeln!(out, "```");
    } else {
        let _ = writeln!(
            out,
            "No server log source configured (set AWA_SOAK_PG_LOG or AWA_SOAK_PG_CONTAINER)."
        );
    }
    std::fs::write(dir.join("reproducer.md"), out)
}

pub fn excerpts_of(log: &str) -> Vec<String> {
    crash_excerpts(log)
}
