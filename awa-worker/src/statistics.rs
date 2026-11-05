//! Planner-statistics refresh for the queue-storage tables autovacuum never
//! analyses: the two head tables, which stay below the autoanalyze threshold,
//! and the partitioned ring parents, which autovacuum skips entirely.
//!
//! The maintenance leader runs one pass on election and one every interval.
//! Each relation is analysed through `awa.analyze_queue_storage_relation`
//! (the `analyze_queue_storage_relation` schema patch, `SECURITY DEFINER`) in its own short transaction with a
//! bounded `lock_timeout` and `statement_timeout`, so the refresh never holds
//! locks across relations and cannot wait on ring prune for long.

use crate::metrics::AwaMetrics;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// How long one relation's ANALYZE may wait for a lock before the pass moves
/// on. Ring prune holds `ACCESS EXCLUSIVE` on a slot's children only
/// briefly, so a wait this long means something else holds the table.
const LOCK_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound for one relation. A ring parent analyses in under a few
/// seconds even on a loaded host; this only stops a stuck statement.
const STATEMENT_TIMEOUT: Duration = Duration::from_secs(120);

const HEAD_TABLES: [&str; 2] = ["queue_claim_heads", "queue_enqueue_heads"];

const SQLSTATE_INSUFFICIENT_PRIVILEGE: &str = "42501";
const SQLSTATE_UNDEFINED_FUNCTION: &str = "42883";
const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";

/// Which kind of relation an analyse call covered; bounds metric cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationScope {
    Heads,
    RingParent,
}

impl RelationScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RelationScope::Heads => "heads",
            RelationScope::RingParent => "ring_parent",
        }
    }
}

/// Result of analysing one relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnalyzeOutcome {
    Ok,
    /// The function owner (or the runtime role, if it cannot execute the
    /// function) may not analyse the relation.
    SkippedPrivilege,
    /// `lock_timeout` expired waiting on a conflicting lock.
    SkippedLock,
    /// `awa.analyze_queue_storage_relation` is not installed (the schema
    /// patch has not been applied).
    Unavailable,
    Failed,
}

impl AnalyzeOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            AnalyzeOutcome::Ok => "ok",
            AnalyzeOutcome::SkippedPrivilege => "skipped_privilege",
            AnalyzeOutcome::SkippedLock => "skipped_lock",
            AnalyzeOutcome::Unavailable => "unavailable",
            AnalyzeOutcome::Failed => "failed",
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PassSummary {
    pub(crate) ok: usize,
    pub(crate) skipped_privilege: usize,
    pub(crate) skipped_lock: usize,
    pub(crate) unavailable: usize,
    pub(crate) failed: usize,
}

impl PassSummary {
    fn record(&mut self, outcome: AnalyzeOutcome) {
        match outcome {
            AnalyzeOutcome::Ok => self.ok += 1,
            AnalyzeOutcome::SkippedPrivilege => self.skipped_privilege += 1,
            AnalyzeOutcome::SkippedLock => self.skipped_lock += 1,
            AnalyzeOutcome::Unavailable => self.unavailable += 1,
            AnalyzeOutcome::Failed => self.failed += 1,
        }
    }
}

/// Refreshes planner statistics for one queue-storage schema.
pub(crate) struct StatisticsRefresher {
    pool: PgPool,
    schema: String,
    metrics: AwaMetrics,
    warned_privilege: AtomicBool,
    warned_unavailable: AtomicBool,
}

impl StatisticsRefresher {
    pub(crate) fn new(pool: PgPool, schema: String, metrics: AwaMetrics) -> Self {
        Self {
            pool,
            schema,
            metrics,
            warned_privilege: AtomicBool::new(false),
            warned_unavailable: AtomicBool::new(false),
        }
    }

    /// Run a pass now and then every `interval` until `cancel` fires.
    pub(crate) async fn run(&self, interval: Duration, cancel: CancellationToken) {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = timer.tick() => {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = self.run_pass() => {}
                    }
                }
            }
        }
    }

    /// Analyse the head tables, then every partitioned table of the schema.
    pub(crate) async fn run_pass(&self) -> PassSummary {
        let started = Instant::now();
        let mut summary = PassSummary::default();

        match awa_model::storage::queue_storage_schema_ready(&self.pool, &self.schema).await {
            Ok(true) => {}
            Ok(false) => {
                debug!(schema = %self.schema, "Queue-storage schema is not prepared yet; skipping statistics refresh");
                return summary;
            }
            Err(err) => {
                warn!(error = %err, schema = %self.schema, "Could not check the queue-storage schema for statistics refresh");
                self.record(
                    RelationScope::RingParent,
                    AnalyzeOutcome::Failed,
                    started.elapsed(),
                );
                summary.record(AnalyzeOutcome::Failed);
                return summary;
            }
        }

        let ring_parents = match self.ring_parents().await {
            Ok(parents) => parents,
            Err(err) => {
                warn!(error = %err, schema = %self.schema, "Could not list ring tables for statistics refresh");
                self.record(
                    RelationScope::RingParent,
                    AnalyzeOutcome::Failed,
                    started.elapsed(),
                );
                summary.record(AnalyzeOutcome::Failed);
                Vec::new()
            }
        };

        let targets = HEAD_TABLES
            .iter()
            .map(|name| (RelationScope::Heads, (*name).to_string()))
            .chain(
                ring_parents
                    .into_iter()
                    .map(|name| (RelationScope::RingParent, name)),
            );

        for (scope, relation) in targets {
            let relation_started = Instant::now();
            let outcome = self.analyze_relation(&relation).await;
            self.record(scope, outcome, relation_started.elapsed());
            summary.record(outcome);
        }

        debug!(
            schema = %self.schema,
            elapsed_ms = started.elapsed().as_millis() as u64,
            ok = summary.ok,
            skipped_privilege = summary.skipped_privilege,
            skipped_lock = summary.skipped_lock,
            unavailable = summary.unavailable,
            failed = summary.failed,
            "Planner statistics refresh finished"
        );
        summary
    }

    fn record(&self, scope: RelationScope, outcome: AnalyzeOutcome, elapsed: Duration) {
        self.metrics
            .record_statistics_analyze(scope.as_str(), outcome.as_str(), elapsed);
    }

    async fn ring_parents(&self) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar(
            r#"
            SELECT class.relname::text
            FROM pg_class AS class
            JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
            WHERE namespace.nspname = $1 AND class.relkind = 'p'
            ORDER BY class.relname
            "#,
        )
        .bind(&self.schema)
        .fetch_all(&self.pool)
        .await
    }

    async fn analyze_relation(&self, relation: &str) -> AnalyzeOutcome {
        match self.call_analyze_function(relation).await {
            Ok(status) => match status.as_str() {
                "analyzed" => AnalyzeOutcome::Ok,
                "skipped_privilege" => {
                    self.warn_privilege_once(
                        relation,
                        "the owner of awa.analyze_queue_storage_relation() may not ANALYZE this table; \
                         run migrations as the role that owns the queue-storage tables, or grant the \
                         function owner MAINTAIN on them (PostgreSQL 17+)",
                    );
                    AnalyzeOutcome::SkippedPrivilege
                }
                // The relation is not part of this schema's shape; nothing to analyse.
                "missing" => AnalyzeOutcome::Ok,
                other => {
                    warn!(
                        status = other,
                        relation, "Unexpected statistics refresh status"
                    );
                    AnalyzeOutcome::Failed
                }
            },
            Err(err) => self.classify_error(relation, &err),
        }
    }

    async fn call_analyze_function(&self, relation: &str) -> Result<String, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)",
        )
        .bind(format!("{}ms", LOCK_TIMEOUT.as_millis()))
        .bind(format!("{}ms", STATEMENT_TIMEOUT.as_millis()))
        .execute(&mut *tx)
        .await?;
        let status: String =
            sqlx::query_scalar("SELECT awa.analyze_queue_storage_relation($1, $2)")
                .bind(&self.schema)
                .bind(relation)
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        Ok(status)
    }

    fn classify_error(&self, relation: &str, err: &sqlx::Error) -> AnalyzeOutcome {
        let code = match err {
            sqlx::Error::Database(db_err) => db_err.code().map(|code| code.into_owned()),
            _ => None,
        };
        match code.as_deref() {
            Some(SQLSTATE_LOCK_NOT_AVAILABLE) => {
                debug!(
                    relation,
                    "Statistics refresh timed out waiting for a lock; retrying next interval"
                );
                AnalyzeOutcome::SkippedLock
            }
            Some(SQLSTATE_UNDEFINED_FUNCTION) => {
                if !self.warned_unavailable.swap(true, Ordering::Relaxed) {
                    warn!(
                        schema = %self.schema,
                        "Planner statistics refresh is disabled: awa.analyze_queue_storage_relation() \
                         is missing. Run `awa migrate` to apply the analyze_queue_storage_relation schema patch"
                    );
                }
                AnalyzeOutcome::Unavailable
            }
            Some(SQLSTATE_INSUFFICIENT_PRIVILEGE) => {
                self.warn_privilege_once(
                    relation,
                    "the runtime role may not execute awa.analyze_queue_storage_relation(); \
                     GRANT EXECUTE ON FUNCTION awa.analyze_queue_storage_relation(text, text) to it",
                );
                AnalyzeOutcome::SkippedPrivilege
            }
            _ => {
                warn!(relation, error = %err, "Planner statistics refresh failed for relation");
                AnalyzeOutcome::Failed
            }
        }
    }

    fn warn_privilege_once(&self, relation: &str, missing: &str) {
        if self.warned_privilege.swap(true, Ordering::Relaxed) {
            return;
        }
        warn!(
            schema = %self.schema,
            relation,
            "Planner statistics refresh skipped: {missing}. Set the statistics refresh \
             interval to zero to disable the refresh"
        );
    }
}

/// Stops the refresh task when the maintenance leader loses leadership or
/// shuts down.
pub(crate) struct RefreshGuard {
    cancel: CancellationToken,
}

impl RefreshGuard {
    pub(crate) fn spawn(
        refresher: StatisticsRefresher,
        interval: Duration,
        parent: &CancellationToken,
    ) -> Self {
        let cancel = parent.child_token();
        let task_cancel = cancel.clone();
        tokio::spawn(async move {
            debug!(
                schema = %refresher.schema,
                interval_s = interval.as_secs(),
                "Planner statistics refresh started"
            );
            refresher.run(interval, task_cancel).await;
        });
        Self { cancel }
    }
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
