use awa_model::{QueueStorage, QueueStorageConfig, RingRotationCadence};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct QueueStorageRuntime {
    pub store: Arc<QueueStorage>,
    pub queue_rotate_interval: Duration,
    pub lease_rotate_interval: Duration,
    pub claim_rotate_interval: Duration,
    pub rotation_cadence: RingRotationCadence,
}

impl QueueStorageRuntime {
    pub fn new(
        config: QueueStorageConfig,
        queue_rotate_interval: Duration,
        lease_rotate_interval: Duration,
    ) -> Result<Self, awa_model::AwaError> {
        // ADR-023 claim-ring rotation defaults to the queue-rotate cadence so
        // claim partitions age out roughly in step with the ready / done
        // partitions they reference. Override via
        // `ClientBuilder::claim_rotate_interval`.
        let claim_rotate_interval = queue_rotate_interval;
        Ok(Self {
            store: Arc::new(QueueStorage::new(config)?),
            queue_rotate_interval,
            lease_rotate_interval,
            claim_rotate_interval,
            rotation_cadence: RingRotationCadence::default(),
        })
    }

    /// Set the prune `lock_timeout` on the store. Only valid before the store
    /// is shared, which holds for a runtime still owned by the builder.
    pub fn set_prune_lock_timeout(&mut self, timeout: Duration) -> Result<(), awa_model::AwaError> {
        Arc::get_mut(&mut self.store)
            .ok_or_else(|| {
                awa_model::AwaError::Validation(
                    "queue storage is already shared; set the prune lock timeout before build"
                        .into(),
                )
            })?
            .set_prune_lock_timeout(timeout)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) enum RuntimeStorage {
    #[default]
    Canonical,
    QueueStorage(QueueStorageRuntime),
}

impl RuntimeStorage {
    pub fn queue_storage(&self) -> Option<&QueueStorageRuntime> {
        match self {
            RuntimeStorage::Canonical => None,
            RuntimeStorage::QueueStorage(runtime) => Some(runtime),
        }
    }

    pub fn queue_storage_store(&self) -> Option<Arc<QueueStorage>> {
        self.queue_storage().map(|runtime| runtime.store.clone())
    }
}

/// Server settings that a prune `lock_timeout` must stay below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PruneLockTimeoutRisk {
    /// A prune waiting this long runs PostgreSQL's deadlock detector, which
    /// may cancel a concurrent claim or enqueue instead of the prune.
    DeadlockTimeout { limit_ms: u64 },
    /// `statement_timeout` cancels the statement first, so the prune fails
    /// with an error instead of reporting `blocked`.
    StatementTimeout { limit_ms: u64 },
}

/// Compare a prune lock timeout with the session's `deadlock_timeout` and
/// `statement_timeout` (milliseconds; a zero `statement_timeout` is disabled).
pub(crate) fn prune_lock_timeout_risks(
    timeout: Duration,
    deadlock_timeout_ms: Option<u64>,
    statement_timeout_ms: Option<u64>,
) -> Vec<PruneLockTimeoutRisk> {
    let timeout_ms = timeout.as_millis();
    let mut risks = Vec::new();
    if let Some(limit_ms) = deadlock_timeout_ms.filter(|limit| *limit > 0) {
        if timeout_ms >= u128::from(limit_ms) {
            risks.push(PruneLockTimeoutRisk::DeadlockTimeout { limit_ms });
        }
    }
    if let Some(limit_ms) = statement_timeout_ms.filter(|limit| *limit > 0) {
        if timeout_ms >= u128::from(limit_ms) {
            risks.push(PruneLockTimeoutRisk::StatementTimeout { limit_ms });
        }
    }
    risks
}

/// Read the connection's effective `deadlock_timeout` and `statement_timeout`
/// and report the ones the prune lock timeout reaches.
pub(crate) async fn check_prune_lock_timeout(
    pool: &sqlx::PgPool,
    timeout: Duration,
) -> Result<Vec<PruneLockTimeoutRisk>, sqlx::Error> {
    let settings: Vec<(String, i64)> = sqlx::query_as(
        "SELECT name, setting::bigint FROM pg_settings \
         WHERE name IN ('deadlock_timeout', 'statement_timeout')",
    )
    .fetch_all(pool)
    .await?;
    let setting = |name: &str| {
        settings
            .iter()
            .find(|(setting_name, _)| setting_name == name)
            .and_then(|(_, value)| u64::try_from(*value).ok())
    };
    Ok(prune_lock_timeout_risks(
        timeout,
        setting("deadlock_timeout"),
        setting("statement_timeout"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_lock_timeout_risks_flag_timeouts_at_or_above_server_limits() {
        let below = Duration::from_millis(999);
        let at = Duration::from_millis(1000);
        assert!(prune_lock_timeout_risks(below, Some(1000), Some(0)).is_empty());
        assert_eq!(
            prune_lock_timeout_risks(at, Some(1000), Some(0)),
            vec![PruneLockTimeoutRisk::DeadlockTimeout { limit_ms: 1000 }]
        );
        assert_eq!(
            prune_lock_timeout_risks(Duration::from_secs(30), Some(1000), Some(5000)),
            vec![
                PruneLockTimeoutRisk::DeadlockTimeout { limit_ms: 1000 },
                PruneLockTimeoutRisk::StatementTimeout { limit_ms: 5000 },
            ]
        );
        assert!(prune_lock_timeout_risks(Duration::from_secs(30), None, None).is_empty());
    }
}
