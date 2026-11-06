//! A migrate probe may hold a normal transaction until its ready marker.

use sqlx::PgPool;
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

    pub fn after_exit(&mut self, result: Result<(i64, i64), String>) {
        match result {
            Ok((sessions, _)) => self.remaining = Some(sessions),
            Err(error) => self.incomplete(error),
        }
    }

    pub fn incomplete(&mut self, error: impl Into<String>) {
        self.incomplete.get_or_insert_with(|| error.into());
    }

    pub fn violation(&self) -> bool {
        self.peak_sessions > 4
            || self.ready_idle_transactions > 0
            || self.remaining.is_some_and(|sessions| sessions > 0)
    }

    pub fn complete(&self) -> bool {
        self.incomplete.is_none() && self.ready_samples > 0 && self.remaining.is_some()
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
        client.close().await;
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
        closed_only.after_exit(Ok((0, 0)));
        assert!(!closed_only.complete());
    }
}
