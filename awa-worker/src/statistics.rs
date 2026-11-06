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

/// The partitioned ring parents a queue-storage substrate installs. A schema
/// can hold other partitioned tables (a substrate in an application schema),
/// which this refresh must never analyse.
const RING_PARENTS: [&str; 13] = [
    "done_entries",
    "lease_claim_batches",
    "lease_claim_closure_batches",
    "lease_claim_closures",
    "lease_claims",
    "leases",
    "queue_terminal_count_deltas",
    "ready_claim_attempt_batches",
    "ready_entries",
    "ready_segments",
    "ready_tombstones",
    "receipt_completion_batches",
    "receipt_completion_tombstones",
];

const SQLSTATE_INSUFFICIENT_PRIVILEGE: &str = "42501";
const SQLSTATE_UNDEFINED_FUNCTION: &str = "42883";
const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";

/// Which kind of relation an analyse call covered; bounds metric cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationScope {
    Heads,
    RingParent,
    /// The pass as a whole: a probe or listing failed, or the helper is absent.
    Pass,
}

impl RelationScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RelationScope::Heads => "heads",
            RelationScope::RingParent => "ring_parent",
            RelationScope::Pass => "pass",
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
    /// Set for a leader whose own storage is canonical and whose queue-storage
    /// runtime is only a standby: a pass runs only once routing has flipped to
    /// `schema`.
    require_active_schema: bool,
    warned_privilege: AtomicBool,
    warned_unavailable: AtomicBool,
}

impl StatisticsRefresher {
    pub(crate) fn new(
        pool: PgPool,
        schema: String,
        metrics: AwaMetrics,
        require_active_schema: bool,
    ) -> Self {
        Self {
            pool,
            schema,
            metrics,
            require_active_schema,
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

        if self.require_active_schema {
            match sqlx::query_scalar::<_, Option<String>>(
                "SELECT awa.active_queue_storage_schema()",
            )
            .fetch_one(&self.pool)
            .await
            {
                Ok(active) if active.as_deref() == Some(self.schema.as_str()) => {}
                Ok(_) => {
                    debug!(schema = %self.schema, "Routing has not flipped to this schema yet; skipping statistics refresh");
                    return summary;
                }
                Err(err) => {
                    warn!(error = %err, "Could not resolve the active queue-storage schema for statistics refresh");
                    return summary;
                }
            }
        }

        match awa_model::storage::queue_storage_schema_ready(&self.pool, &self.schema).await {
            Ok(true) => {}
            Ok(false) => {
                debug!(schema = %self.schema, "Queue-storage schema is not prepared yet; skipping statistics refresh");
                return summary;
            }
            Err(err) => {
                warn!(error = %err, schema = %self.schema, "Could not check the queue-storage schema for statistics refresh");
                self.record(
                    RelationScope::Pass,
                    AnalyzeOutcome::Failed,
                    started.elapsed(),
                );
                summary.record(AnalyzeOutcome::Failed);
                return summary;
            }
        }

        match self.helper_installed().await {
            Ok(true) => {}
            Ok(false) => {
                self.warn_unavailable_once();
                self.record(
                    RelationScope::Pass,
                    AnalyzeOutcome::Unavailable,
                    started.elapsed(),
                );
                summary.record(AnalyzeOutcome::Unavailable);
                return summary;
            }
            Err(err) => {
                warn!(error = %err, schema = %self.schema, "Could not probe for the statistics helper");
                self.record(
                    RelationScope::Pass,
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
                    RelationScope::Pass,
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

    /// One catalog lookup per pass. A missing helper (a binary running ahead
    /// of the schema) then costs this query instead of a failing call, and a
    /// server-side ERROR line, per relation; the pass interval is the
    /// re-probe interval.
    async fn helper_installed(&self) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT to_regprocedure('awa.analyze_queue_storage_relation(text,text)') IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await
    }

    async fn ring_parents(&self) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar(
            r#"
            SELECT class.relname::text
            FROM pg_class AS class
            JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
            WHERE namespace.nspname = $1 AND class.relkind = 'p'
              AND class.relname = ANY($2)
            ORDER BY class.relname
            "#,
        )
        .bind(&self.schema)
        .bind(&RING_PARENTS[..])
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
                self.warn_unavailable_once();
                AnalyzeOutcome::Unavailable
            }
            Some(SQLSTATE_INSUFFICIENT_PRIVILEGE) => {
                self.warn_privilege_once(
                    relation,
                    "the runtime role may not execute awa.analyze_queue_storage_relation(), or lacks \
                     INSERT, UPDATE and DELETE on the queue head tables; \
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

    fn warn_unavailable_once(&self) {
        if self.warned_unavailable.swap(true, Ordering::Relaxed) {
            return;
        }
        warn!(
            schema = %self.schema,
            "Planner statistics refresh is disabled: awa.analyze_queue_storage_relation() \
             is missing. Run `awa migrate` to apply the analyze_queue_storage_relation schema patch"
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;

    /// A binary ahead of the schema must probe once and attempt no calls: a
    /// failing call per relation would count as `unavailable` fifteen times
    /// and write fifteen server-side ERROR lines per pass.
    #[tokio::test]
    async fn missing_helper_costs_one_probe_and_no_calls() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string());
        let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await
            .expect("connect admin");
        let database = format!("awa_stats_unit_{}", std::process::id());
        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
            .execute(&admin)
            .await
            .expect("drop leftover database");
        sqlx::raw_sql(&format!("CREATE DATABASE {database}"))
            .execute(&admin)
            .await
            .expect("create database");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options.database(&database))
            .await
            .expect("connect test database");
        awa_model::migrations::run(&pool).await.expect("migrate");

        let refresher = StatisticsRefresher::new(
            pool.clone(),
            "awa".to_string(),
            AwaMetrics::from_global(),
            false,
        );
        let present = refresher.run_pass().await;
        assert_eq!(present.ok, 15, "{present:?}");

        sqlx::raw_sql("DROP FUNCTION awa.analyze_queue_storage_relation(text, text)")
            .execute(&pool)
            .await
            .expect("drop the helper");
        let absent = refresher.run_pass().await;
        assert_eq!(
            absent,
            PassSummary {
                unavailable: 1,
                ..PassSummary::default()
            }
        );
        pool.close().await;
        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
            .execute(&admin)
            .await
            .expect("drop database");
    }
}
