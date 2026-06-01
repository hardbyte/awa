//! `awa storage finalize --wait` polling loop and supporting helpers.
//!
//! `finalize --wait` is the operator-friendly counterpart to the one-shot
//! `finalize` command: it polls the same readiness gates that
//! [`awa_model::storage::status_report`] computes, and only invokes the
//! SQL finalize once the blocker list has stayed empty for several
//! consecutive checks. The intent is to give an operator a single
//! command they can leave running during a multi-hour rolling upgrade
//! instead of `watch -n 5 psql …`.
//!
//! Progress output uses the `tracing` macros (the operator may want
//! structured logs); `--check` (in `main.rs`) uses `println!`/`eprintln!`
//! because its output is a one-shot report for a human.

use std::time::{Duration, Instant};

use awa_model::storage::{self, StorageStatusReport};
use sqlx::PgPool;
use tracing::{info, warn};

/// How often to re-fetch the readiness report while waiting.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How many consecutive "ready" observations we want before pulling
/// the trigger. Guards against a single optimistic reading caused by
/// e.g. a runtime momentarily failing to heartbeat between snapshots.
const REQUIRED_READY_STREAK: u32 = 2;

/// Result of `wait_for_finalize`.
pub enum WaitOutcome {
    /// Finalize succeeded; the report is the post-finalize status.
    Finalized(StorageStatusReport),
    /// Wait cap expired before the readiness gates cleared; the report
    /// is the last observation.
    TimedOut(StorageStatusReport),
}

/// Poll readiness, then finalize. `cap` is the optional total wait
/// budget; `None` means wait indefinitely.
pub async fn wait_for_finalize(
    pool: &PgPool,
    cap: Option<Duration>,
) -> Result<WaitOutcome, awa_model::AwaError> {
    let started = Instant::now();
    let mut ready_streak: u32 = 0;
    let mut iteration: u64 = 0;

    info!(
        poll_interval_secs = POLL_INTERVAL.as_secs(),
        required_ready_streak = REQUIRED_READY_STREAK,
        wait_cap_secs = cap.map(|d| d.as_secs()),
        "storage finalize --wait: starting"
    );

    loop {
        iteration += 1;
        let report = storage::status_report(pool).await?;

        if report.can_finalize {
            ready_streak += 1;
            info!(
                iteration,
                ready_streak,
                required_ready_streak = REQUIRED_READY_STREAK,
                state = %report.status.state,
                "storage finalize --wait: ready observation"
            );
            if ready_streak >= REQUIRED_READY_STREAK {
                info!(
                    iteration,
                    elapsed_secs = started.elapsed().as_secs(),
                    "storage finalize --wait: gates clear, invoking finalize"
                );
                storage::finalize(pool).await?;
                let post = storage::status_report(pool).await?;
                return Ok(WaitOutcome::Finalized(post));
            }
        } else {
            // Reset on any blocker so a flap doesn't count toward the
            // streak.
            if ready_streak > 0 {
                warn!(
                    iteration,
                    blocker_count = report.finalize_blockers.len(),
                    "storage finalize --wait: readiness regressed, resetting streak"
                );
            }
            ready_streak = 0;
            info!(
                iteration,
                state = %report.status.state,
                canonical_live_backlog = report.canonical_live_backlog,
                blocker_count = report.finalize_blockers.len(),
                blockers = ?report.finalize_blockers,
                "storage finalize --wait: still blocked"
            );
        }

        if let Some(cap) = cap {
            let elapsed = started.elapsed();
            if elapsed >= cap {
                warn!(
                    iteration,
                    elapsed_secs = elapsed.as_secs(),
                    cap_secs = cap.as_secs(),
                    "storage finalize --wait: cap reached"
                );
                // Re-fetch once more so the caller always reports the
                // freshest observation alongside the timeout message.
                let final_report = storage::status_report(pool).await?;
                return Ok(WaitOutcome::TimedOut(final_report));
            }
            // Don't oversleep past the cap.
            let remaining = cap - elapsed;
            tokio::time::sleep(POLL_INTERVAL.min(remaining)).await;
        } else {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// Parse a tiny subset of duration strings: e.g. `30s`, `5m`, `2h`,
/// `2h30m`, `1500ms`. Numeric-only inputs are rejected to avoid
/// ambiguity between "seconds" and "milliseconds".
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("empty duration".to_string());
    }

    let mut total = Duration::ZERO;
    let mut digits = String::new();
    let mut units = String::new();
    let mut seen_unit = false;

    let mut chars = trimmed.chars().peekable();
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            chars.next();
        } else if ch.is_ascii_alphabetic() {
            units.clear();
            while let Some(&ch) = chars.peek() {
                if ch.is_ascii_alphabetic() {
                    units.push(ch);
                    chars.next();
                } else {
                    break;
                }
            }
            if digits.is_empty() {
                return Err(format!("unit '{units}' has no number before it"));
            }
            let value: u64 = digits
                .parse()
                .map_err(|err| format!("bad number {digits:?}: {err}"))?;
            let multiplier_ms: u64 = match units.as_str() {
                "ms" => 1,
                "s" => 1_000,
                "m" => 60 * 1_000,
                "h" => 3_600 * 1_000,
                other => return Err(format!("unknown unit '{other}' (expected ms/s/m/h)")),
            };
            total = total
                .checked_add(Duration::from_millis(
                    value
                        .checked_mul(multiplier_ms)
                        .ok_or("duration overflow")?,
                ))
                .ok_or("duration overflow")?;
            digits.clear();
            seen_unit = true;
        } else {
            return Err(format!("unexpected character {ch:?}"));
        }
    }

    if !digits.is_empty() {
        return Err(format!(
            "trailing number {digits:?} has no unit (use s/m/h/ms)"
        ));
    }
    if !seen_unit {
        return Err("duration must include a unit (s/m/h/ms)".to_string());
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_units() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(
            parse_duration("1500ms").unwrap(),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn parse_compound_units() {
        assert_eq!(
            parse_duration("2h30m").unwrap(),
            Duration::from_secs(2 * 3600 + 30 * 60)
        );
        assert_eq!(
            parse_duration("1h2m3s").unwrap(),
            Duration::from_secs(3600 + 120 + 3)
        );
    }

    #[test]
    fn rejects_unitless_or_unknown() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("s30").is_err());
    }
}
