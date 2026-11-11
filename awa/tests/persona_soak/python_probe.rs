//! A migrate probe may hold a normal transaction until its ready marker.

use sqlx::PgPool;
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    #[default]
    Migrating,
    Ready,
    Idle,
    Closed,
}

impl Phase {
    pub fn marker(line: &str) -> Option<Self> {
        if line.starts_with("MIGRATED probe ") {
            Some(Self::Ready)
        } else {
            match line {
                "IDLE" => Some(Self::Idle),
                "CLOSED" => Some(Self::Closed),
                _ => None,
            }
        }
    }

    pub fn from_byte(value: u8) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Idle,
            3 => Self::Closed,
            _ => Self::Migrating,
        }
    }
}

/// The unique application name isolates this process, including backends older
/// than a stalled probe's deadline. A failed query is never a zero-session fact.
pub async fn sessions(pool: &PgPool, application_name: &str) -> Result<(i64, i64), String> {
    tokio::time::timeout(
        crate::ci_timing::scaled_timeout(Duration::from_secs(2)),
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT count(*)::bigint,
                    count(*) FILTER (WHERE state = 'idle in transaction')::bigint
             FROM pg_stat_activity WHERE application_name = $1 AND pid <> pg_backend_pid()",
        )
        .bind(application_name)
        .fetch_one(pool),
    )
    .await
    .map_err(|error| format!("session query timed out: {error}"))?
    .map_err(|error| format!("session query failed: {error}"))
}

#[derive(Debug, Default)]
pub struct Evidence {
    pub peak_sessions: i64,
    pub migrating_idle_transactions: i64,
    pub ready_idle_transactions: i64,
    pub ready_samples: usize,
    pub remaining: Option<i64>,
    pub last_exit_sample_valid: bool,
    pub exit_phase: Option<Phase>,
    pub exit_success: bool,
    pub incomplete: Option<String>,
}

impl Evidence {
    pub fn observe(&mut self, phase_before_query: Phase, result: Result<(i64, i64), String>) {
        match result {
            Ok((sessions, idle)) => {
                self.peak_sessions = self.peak_sessions.max(sessions);
                if phase_before_query == Phase::Migrating {
                    self.migrating_idle_transactions = self.migrating_idle_transactions.max(idle);
                } else {
                    if phase_before_query != Phase::Closed {
                        self.ready_samples += 1;
                    }
                    self.ready_idle_transactions = self.ready_idle_transactions.max(idle);
                }
            }
            Err(error) => self.incomplete(error),
        }
    }

    /// Process exit alone does not prove that an active server query was cancelled.
    /// CLOSED is emitted only after the client acknowledged its graceful close.
    pub fn record_exit(&mut self, phase: Phase, success: bool) {
        self.exit_phase = Some(phase);
        self.exit_success = success;
        if !self.closed_exit_proven() {
            self.incomplete(format!(
                "Python migrate probe exited without successful CLOSED evidence (phase={phase:?}, success={success})"
            ));
        }
    }

    fn closed_exit_proven(&self) -> bool {
        self.exit_success && self.exit_phase == Some(Phase::Closed)
    }

    fn post_exit_observation_complete(&self, phase: Phase, success: bool) -> bool {
        self.last_exit_sample_valid
            && self.remaining == Some(0)
            && (!success || phase == Phase::Closed)
    }

    pub fn after_exit(&mut self, result: Result<(i64, i64), String>) {
        match result {
            Ok((sessions, _)) => {
                self.remaining = Some(sessions);
                self.last_exit_sample_valid = true;
            }
            Err(error) => {
                self.last_exit_sample_valid = false;
                self.incomplete(error);
            }
        }
    }

    /// Reaping may precede stdout consumption. Marker and session observations
    /// share the existing post-exit budget, including failed/slow queries.
    pub async fn finish_exit<F, Fut>(&mut self, phase: &AtomicU8, success: bool, mut sessions: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(i64, i64), String>>,
    {
        let deadline =
            tokio::time::Instant::now() + crate::ci_timing::scaled_timeout(Duration::from_secs(2));
        loop {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let result = tokio::time::timeout_at(deadline, sessions())
                .await
                .unwrap_or_else(|_| {
                    Err("post-exit session observation exceeded its existing deadline".into())
                });
            let failed = result.is_err();
            self.after_exit(result);
            let observed_phase = Phase::from_byte(phase.load(Ordering::Acquire));
            if self.post_exit_observation_complete(observed_phase, success) || failed {
                break;
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + Duration::from_millis(100)).min(deadline),
            )
            .await;
        }
        self.record_exit(Phase::from_byte(phase.load(Ordering::Acquire)), success);
    }

    pub fn incomplete(&mut self, error: impl Into<String>) {
        self.incomplete.get_or_insert_with(|| error.into());
    }

    pub fn violation(&self) -> bool {
        self.peak_sessions > 4
            || self.ready_idle_transactions > 0
            || (self.closed_exit_proven()
                && self.last_exit_sample_valid
                && self.remaining.is_some_and(|sessions| sessions > 0))
    }

    pub fn complete(&self) -> bool {
        self.incomplete.is_none()
            && self.ready_samples > 0
            && self.closed_exit_proven()
            && self.last_exit_sample_valid
            && self.remaining.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn pool(application_name: &str, size: u32) -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".into());
        PgPoolOptions::new()
            .max_connections(size)
            .connect(&format!(
                "{url}{}application_name={application_name}",
                if url.contains('?') { '&' } else { '?' }
            ))
            .await
            .expect("probe test pool")
    }

    fn name() -> String {
        format!("probe_test_{}", uuid::Uuid::new_v4().simple())
    }

    #[tokio::test]
    async fn healthy_migration_transaction_is_not_a_stranded_ready_connection() {
        let application = name();
        let observer = pool(&name(), 1).await;
        let client = pool(&application, 1).await;
        let mut tx = client.begin().await.expect("migration transaction");
        sqlx::query("SELECT 1").execute(tx.as_mut()).await.unwrap();
        let mut evidence = Evidence::default();
        let migrating = sessions(&observer, &application).await.unwrap();
        assert_eq!(
            migrating,
            (1, 1),
            "a legitimate transaction between statements"
        );
        evidence.observe(Phase::Migrating, Ok(migrating));
        tx.commit().await.unwrap();
        evidence.observe(Phase::Ready, sessions(&observer, &application).await);
        // The commit hands the connection back to the pool on a spawned task.
        // If that task is still testing the connection when `close` runs,
        // `close` waits for it but returns without closing the connection it
        // then puts on the idle queue, which stays open while the pool lives.
        // The second `close` closes it.
        client.close().await;
        client.close().await;
        evidence.record_exit(Phase::Closed, true);
        // An acknowledged close can precede the server removing its backend.
        let deadline =
            std::time::Instant::now() + crate::ci_timing::scaled_timeout(Duration::from_secs(2));
        loop {
            let count = sessions(&observer, &application).await.unwrap();
            evidence.after_exit(Ok(count));
            if count.0 == 0 || std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(evidence.complete(), "{evidence:?}");
        assert!(!evidence.violation(), "{evidence:?}");
        observer.close().await;
    }

    #[tokio::test]
    async fn ready_transaction_left_open_is_a_violation() {
        let application = name();
        let observer = pool(&name(), 1).await;
        let client = pool(&application, 1).await;
        let mut tx = client.begin().await.unwrap();
        sqlx::query("SELECT 1").execute(tx.as_mut()).await.unwrap();
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, sessions(&observer, &application).await);
        assert!(evidence.violation(), "{evidence:?}");
        tx.rollback().await.unwrap();
        client.close().await;
        observer.close().await;
    }

    #[tokio::test]
    async fn retained_session_after_exit_is_a_violation_and_other_probes_are_excluded() {
        let application = name();
        let observer = pool(&name(), 1).await;
        let leaked = pool(&application, 1).await;
        let unrelated = pool(&name(), 1).await;
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, sessions(&observer, &application).await);
        evidence.record_exit(Phase::Closed, true);
        evidence.after_exit(sessions(&observer, &application).await);
        assert_eq!(evidence.remaining, Some(1));
        assert!(evidence.complete());
        assert!(evidence.violation(), "{evidence:?}");
        leaked.close().await;
        unrelated.close().await;
        observer.close().await;
    }

    #[tokio::test]
    async fn session_query_failure_is_inconclusive_and_not_zero() {
        let observer = pool(&name(), 1).await;
        observer.close().await;
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, sessions(&observer, &name()).await);
        evidence.after_exit(sessions(&observer, &name()).await);
        assert!(!evidence.complete());
        assert!(evidence.incomplete.is_some());
        assert_eq!(evidence.remaining, None);
    }

    #[test]
    fn probe_exit_forced_during_a_read_retains_incomplete_session_evidence() {
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, Ok((2, 0)));
        evidence.record_exit(Phase::Ready, false);
        evidence.after_exit(Ok((1, 0)));
        assert!(
            !evidence.violation(),
            "a forced exit does not prove graceful-close failure"
        );
        assert!(!evidence.complete());
        assert!(evidence.incomplete.is_some());
        assert_eq!(evidence.remaining, Some(1));
    }

    #[test]
    fn probe_exit_delayed_closed_marker_uses_the_existing_session_window() {
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, Ok((2, 0)));
        evidence.after_exit(Ok((0, 0)));
        assert!(
            !evidence.post_exit_observation_complete(Phase::Ready, true),
            "zero sessions must not stop the reader before its flushed CLOSED marker"
        );
        // The detached reader consumes the marker during the same post-exit window.
        assert!(evidence.post_exit_observation_complete(Phase::Closed, true));
        evidence.record_exit(Phase::Closed, true);
        assert!(evidence.complete());
        assert!(!evidence.violation());
    }

    #[tokio::test]
    async fn probe_exit_finish_waits_for_delayed_stdout_closed_marker() {
        let phase = std::sync::Arc::new(AtomicU8::new(Phase::Ready as u8));
        let reader_phase = phase.clone();
        let reader = tokio::spawn(async move {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            reader_phase.store(Phase::Closed as u8, Ordering::Release);
        });
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, Ok((2, 0)));
        let mut calls = 0;
        evidence
            .finish_exit(&phase, true, || {
                calls += 1;
                async { Ok((0, 0)) }
            })
            .await;
        reader.await.unwrap();
        assert!(evidence.complete(), "{evidence:?}");
        assert!(!evidence.violation());
        assert_eq!(evidence.exit_phase, Some(Phase::Closed));
        assert!(
            calls >= 2,
            "the helper must continue past zero sessions before CLOSED"
        );
    }

    #[tokio::test]
    async fn probe_exit_finish_query_failure_does_not_prove_a_retained_session_leak() {
        let phase = AtomicU8::new(Phase::Closed as u8);
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, Ok((2, 0)));
        let mut calls = 0;
        evidence
            .finish_exit(&phase, true, || {
                calls += 1;
                let result = if calls == 1 {
                    Ok((1, 0))
                } else {
                    Err("injected final query failure".into())
                };
                async move { result }
            })
            .await;
        assert!(
            !evidence.violation(),
            "an earlier positive count is not a successful final observation"
        );
        assert!(!evidence.complete());
        assert!(evidence.incomplete.is_some());
        assert_eq!(
            evidence.remaining,
            Some(1),
            "retain the last successful count for diagnosis"
        );
        assert!(!evidence.last_exit_sample_valid);
    }

    #[test]
    fn probe_exit_success_without_closed_marker_is_incomplete() {
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, Ok((2, 0)));
        evidence.record_exit(Phase::Ready, true);
        evidence.after_exit(Ok((1, 0)));
        assert!(
            !evidence.violation(),
            "an exit code is not an acknowledged pool close"
        );
        assert!(!evidence.complete());
        assert!(evidence.incomplete.is_some());
        assert_eq!(evidence.remaining, Some(1));
    }

    #[test]
    fn probe_exit_acknowledged_close_with_retained_session_is_a_violation() {
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, Ok((2, 0)));
        evidence.record_exit(Phase::Closed, true);
        evidence.after_exit(Ok((1, 0)));
        assert!(evidence.violation());
        assert!(
            evidence.complete(),
            "complete coverage still reports the failure"
        );
        assert_eq!(evidence.remaining, Some(1));
    }

    #[test]
    fn probe_exit_acknowledged_close_and_no_session_is_complete() {
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Ready, Ok((2, 0)));
        evidence.record_exit(Phase::Closed, true);
        evidence.after_exit(Ok((0, 0)));
        assert!(!evidence.violation());
        assert!(evidence.complete());
    }

    #[test]
    fn probe_exit_unproven_close_does_not_hide_pool_or_idle_transaction_violations() {
        for (phase, sample) in [(Phase::Migrating, (5, 0)), (Phase::Ready, (2, 1))] {
            let mut evidence = Evidence::default();
            evidence.observe(phase, Ok(sample));
            evidence.record_exit(phase, false);
            evidence.after_exit(Ok((1, 0)));
            assert!(evidence.violation(), "{evidence:?}");
            assert!(!evidence.complete());
            assert_eq!(evidence.remaining, Some(1));
        }
    }

    #[test]
    fn ready_marker_and_missing_ready_coverage_are_explicit() {
        assert_eq!(
            Phase::marker("MIGRATED probe queues=1 sample=0"),
            Some(Phase::Ready)
        );
        assert_eq!(Phase::marker("IDLE"), Some(Phase::Idle));
        assert_eq!(Phase::marker("CLOSED"), Some(Phase::Closed));
        assert_eq!(Phase::marker("MIGRATED startup queues=1 sample=0"), None);
        let mut evidence = Evidence::default();
        evidence.observe(Phase::Migrating, Ok((4, 1)));
        evidence.after_exit(Ok((0, 0)));
        assert!(
            !evidence.complete(),
            "an exit without ready samples proves no idle invariant"
        );
        evidence.observe(Phase::Migrating, Ok((5, 0)));
        assert!(
            evidence.violation(),
            "the pool budget applies during migration too"
        );
        // Even valid ready/idle/closed markers may all reach the reader before
        // the next snapshot. An already closed pool is not live idle coverage.
        let mut closed_only = Evidence::default();
        closed_only.observe(Phase::Closed, Ok((0, 0)));
        closed_only.record_exit(Phase::Closed, true);
        closed_only.after_exit(Ok((0, 0)));
        assert!(!closed_only.complete());
    }
}
