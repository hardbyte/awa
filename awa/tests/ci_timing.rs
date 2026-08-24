//! Shared CI contention scaling for the nightly chaos and benchmark suites.
//!
//! The nightly suites run on shared GitHub runners whose CPU allocation
//! varies run to run. Three assertion shapes are sensitive to that and
//! flaked repeatedly through 2026-07 (#399, #434):
//!
//!   1. **Wall-clock waits.** A test waits N seconds for a state the
//!      runtime reaches in milliseconds when it has a core to itself.
//!      Scale with [`scaled_timeout`].
//!
//!   2. **Aggressive rescue cadences.** A chaos client sets
//!      `heartbeat_staleness` a few multiples above `heartbeat_interval`
//!      so its own backdating triggers rescue promptly. Under contention
//!      a *live* worker's heartbeat can miss that window, so the runtime
//!      correctly rescues a healthy attempt — and the test sees a genuine
//!      duplicate completion, which reads as a correctness failure. Scale
//!      with [`scaled_staleness`]: the interval stays fast so rescue is
//!      still exercised, but the staleness window gains the same margin
//!      as the waits around it.
//!
//!   3. **Minimum-progress floors.** A gate asserts "at least N of these
//!      happened" to catch a stalled subsystem. The floor must sit far
//!      below the *observed* operating point, not just below the nominal
//!      one. Scale with [`contention_floor`].
//!
//! Scaling only ever loosens a bound, and only on CI. A local run keeps
//! the strict values, so a real regression still fails fast on a
//! developer machine. Override with `AWA_CHAOS_TIMEOUT_MULTIPLIER`.
#![allow(dead_code)]

use std::time::Duration;

/// How much slack to give contention-sensitive bounds. `1.0` locally,
/// `3.0` on CI, or the `AWA_CHAOS_TIMEOUT_MULTIPLIER` override (clamped
/// to `>= 1.0` so the override can only ever loosen).
pub fn chaos_timeout_multiplier() -> f64 {
    let override_var = std::env::var("AWA_CHAOS_TIMEOUT_MULTIPLIER").ok();
    multiplier_from(override_var.as_deref(), std::env::var_os("CI").is_some())
}

/// The multiplier decision, with the environment passed in so it can be
/// tested without mutating process-global state.
fn multiplier_from(override_var: Option<&str>, is_ci: bool) -> f64 {
    if let Some(parsed) = override_var.and_then(|raw| raw.parse::<f64>().ok()) {
        // Clamped so an override can only ever loosen a bound.
        return parsed.max(1.0);
    }

    if is_ci {
        3.0
    } else {
        1.0
    }
}

/// Grow a wait deadline by the contention multiplier.
pub fn scaled_timeout(timeout: Duration) -> Duration {
    timeout.mul_f64(chaos_timeout_multiplier())
}

/// Grow a heartbeat-staleness window by the contention multiplier.
///
/// Distinct from [`scaled_timeout`] only in intent: this one is passed to
/// `ClientBuilder::heartbeat_staleness`, where the cost of being too tight
/// is a spurious rescue of a live attempt rather than a timeout. Paired
/// heartbeat/rescue *intervals* are deliberately left unscaled so the
/// rescue path still runs at chaos cadence.
pub fn scaled_staleness(staleness: Duration) -> Duration {
    staleness.mul_f64(chaos_timeout_multiplier())
}

/// Shrink a minimum-progress floor by the contention multiplier.
///
/// Use for "this subsystem must have advanced at least N times" gates.
/// The returned floor is at least 1: the regression these gates exist to
/// catch is a fully stalled subsystem, so zero progress must still fail.
pub fn contention_floor(nominal: i64) -> i64 {
    let scaled = (nominal as f64 / chaos_timeout_multiplier()).floor() as i64;
    scaled.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_runs_keep_strict_bounds() {
        assert_eq!(multiplier_from(None, false), 1.0);
    }

    #[test]
    fn ci_runs_get_margin() {
        assert_eq!(multiplier_from(None, true), 3.0);
    }

    #[test]
    fn override_can_only_loosen() {
        assert_eq!(multiplier_from(Some("0.1"), true), 1.0);
        assert_eq!(multiplier_from(Some("-5"), true), 1.0);
        assert_eq!(multiplier_from(Some("6"), false), 6.0);
    }

    #[test]
    fn unparseable_override_falls_back_to_the_environment() {
        assert_eq!(multiplier_from(Some("banana"), true), 3.0);
        assert_eq!(multiplier_from(Some(""), false), 1.0);
    }

    #[test]
    fn scaling_a_timeout_grows_it() {
        // 250ms staleness against a 50ms heartbeat interval is a 5x margin
        // locally; on CI it becomes 15x, which is what stops a contended
        // runner's live worker from being rescued as if it had died.
        assert_eq!(
            Duration::from_millis(250).mul_f64(multiplier_from(None, true)),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn progress_floor_keeps_margin_under_the_observed_rate() {
        // The receipt gate's nominal floor is duration_secs / 4 = 45 for a
        // 180s run. #399 saw a healthy run produce 41, so the CI floor has
        // to sit well under that.
        let ci_floor = (45.0_f64 / multiplier_from(None, true)).floor() as i64;
        assert_eq!(ci_floor, 15);
        assert!(
            ci_floor < 41,
            "floor must clear the observed operating point"
        );
    }

    #[test]
    fn progress_floor_never_reaches_zero() {
        // Whatever the multiplier, a fully pinned ring (zero rotations) has
        // to keep failing — that is the regression the gate exists for.
        assert_eq!(contention_floor(45), contention_floor(45).max(1));
        let absurd = (45.0_f64 / multiplier_from(Some("10000"), true)).floor() as i64;
        assert_eq!(absurd.max(1), 1);
    }
}
