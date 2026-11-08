use awa_model::runtime_control::{self, RuntimeControl};
use chrono::{DateTime, Utc};
use sqlx::pool::PoolConnection;
use sqlx::Postgres;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const FRESHNESS: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    Dispatch,
    Cron,
    Prune,
    Promote,
    Rescue,
}

// Every scheduler branch names a scope; there is no default-allow arm.
macro_rules! branches {
    ($($variant:ident => ($name:literal, $scope:ident)),+ $(,)?) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) enum MaintenanceBranch { $($variant),+ }
        impl MaintenanceBranch {
            pub(crate) fn classification(self) -> (&'static str, Scope) {
                match self { $(Self::$variant => ($name, Scope::$scope)),+ }
            }
        }
    };
}
branches! {
    CronSync => ("cron_sync", Cron),
    RescueHeartbeat => ("rescue_stale_heartbeats", Rescue),
    RescueDeadline => ("rescue_expired_deadlines", Rescue),
    RescueCallback => ("rescue_expired_callbacks", Rescue),
    Promote => ("promote_scheduled", Promote),
    Aging => ("priority_aging", Promote),
    Cleanup => ("cleanup", Prune),
    QueueHealth => ("queue_stats", Prune),
    DirtyMetadata => ("recompute_dirty_admin_metadata", Prune),
    RefreshMetadata => ("refresh_admin_metadata", Prune),
    Batch => ("batch_operations", Dispatch),
    RotateQueue => ("rotate_queue", Prune),
    RotateLease => ("rotate_lease", Prune),
    RotateClaim => ("rotate_claim", Prune),
    TerminalRollup => ("terminal_count_rollup", Prune),
    AuthorityFlip => ("storage_authority_flip", Prune),
    Descriptors => ("publish_descriptors", Prune),
}

#[derive(Default)]
struct Observation {
    value: Option<RuntimeControl>,
    read_started: Option<Instant>,
    observed_at: Option<DateTime<Utc>>,
}

pub(crate) struct PruneControl {
    capability: AtomicU8,
    observation: Mutex<Observation>,
    outstanding: AtomicUsize,
    abandoned: AtomicUsize,
    report_sequence: AtomicU64,
}

impl Default for PruneControl {
    fn default() -> Self {
        Self {
            capability: AtomicU8::new(0),
            observation: Mutex::new(Observation::default()),
            outstanding: AtomicUsize::new(0),
            abandoned: AtomicUsize::new(0),
            report_sequence: AtomicU64::new(0),
        }
    }
}

impl PruneControl {
    pub(crate) async fn publish_pool(
        &self,
        pool: &sqlx::PgPool,
        instance: Uuid,
    ) -> Result<(), sqlx::Error> {
        if self.capability.load(Ordering::SeqCst) != 2 {
            return Ok(());
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut tx = pool.begin().await?;
            sqlx::query("SET LOCAL statement_timeout = '1s'")
                .execute(tx.as_mut())
                .await?;
            write_status(tx.as_mut(), instance, self.status()).await?;
            tx.commit().await
        })
        .await
        .unwrap_or(Err(sqlx::Error::PoolTimedOut))
    }

    fn allowed_observation(&self, observation: &Observation) -> bool {
        if self.capability.load(Ordering::SeqCst) == 1 {
            return observation
                .read_started
                .is_some_and(|time| time.elapsed() < FRESHNESS);
        }
        observation.value.as_ref().is_some_and(|value| {
            value.scope == runtime_control::PRUNE_SCOPE
                && value.mode == "drain"
                && value.revision >= 0
                && value.halt_id.is_none()
        }) && observation
            .read_started
            .is_some_and(|time| time.elapsed() < FRESHNESS)
    }

    pub(crate) fn allowed(&self) -> bool {
        self.allowed_observation(&self.observation.lock().expect("control observation"))
    }

    pub(crate) fn admit(self: &Arc<Self>, branch: MaintenanceBranch) -> Option<Admission> {
        let (_, scope) = branch.classification();
        if scope == Scope::Prune {
            let observation = self.observation.lock().expect("control observation");
            if !self.allowed_observation(&observation) {
                return None;
            }
            self.outstanding.fetch_add(1, Ordering::SeqCst);
            Some(Admission(Some(self.clone())))
        } else {
            Some(Admission(None))
        }
    }

    pub(crate) async fn observe_pool(&self, pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        let read = async {
            let mut tx = pool.begin().await?;
            sqlx::query("SET LOCAL statement_timeout = '1s'")
                .execute(tx.as_mut())
                .await?;
            match self.observe(tx.as_mut()).await {
                Ok(()) => tx.commit().await,
                Err(err) => {
                    let _ = tx.rollback().await;
                    Err(err)
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(1), read)
            .await
            .unwrap_or(Err(sqlx::Error::PoolTimedOut))
    }

    pub(crate) async fn initialize(
        &self,
        conn: &mut sqlx::PgConnection,
    ) -> Result<(), sqlx::Error> {
        self.observe(conn).await
    }

    pub(crate) async fn observe(&self, conn: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
        if self.capability.load(Ordering::SeqCst) != 2 {
            // Catalog presence discovers versionless installation without re-election.
            // Until installed, the existing reclamation policy stays in force.
            let started = Instant::now();
            let installed: bool =
                sqlx::query_scalar("SELECT to_regclass('awa.runtime_controls') IS NOT NULL")
                    .fetch_one(&mut *conn)
                    .await?;
            if installed {
                self.capability.store(2, Ordering::SeqCst);
            } else {
                let _ = self
                    .capability
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst);
                let mut observation = self.observation.lock().expect("control observation");
                if self.capability.load(Ordering::SeqCst) == 1
                    && observation.read_started.is_none_or(|last| started >= last)
                {
                    observation.read_started = Some(started);
                    observation.observed_at = Some(Utc::now());
                }
                return Ok(());
            }
        }
        let started = Instant::now();
        let value = match runtime_control::read(conn).await {
            Ok(value) => value,
            Err(err) => {
                if matches!(
                    &err,
                    sqlx::Error::RowNotFound | sqlx::Error::ColumnDecode { .. }
                ) || matches!(&err, sqlx::Error::Database(db) if db.code().as_deref() == Some("42P01"))
                {
                    self.observation
                        .lock()
                        .expect("control observation")
                        .read_started = None;
                }
                return Err(err);
            }
        };
        if value.scope != runtime_control::PRUNE_SCOPE
            || value.mode != "drain"
            || value.revision < 0
        {
            self.observation
                .lock()
                .expect("control observation")
                .read_started = None;
            return Err(sqlx::Error::Decode(
                "malformed maintenance.prune control".into(),
            ));
        }
        let mut observation = self.observation.lock().expect("control observation");
        // An earlier, slower read cannot renew permission or overwrite a halt.
        if observation.read_started.is_none_or(|last| started >= last)
            && observation
                .value
                .as_ref()
                .is_none_or(|last| value.revision >= last.revision)
        {
            *observation = Observation {
                value: Some(value),
                read_started: Some(started),
                observed_at: Some(Utc::now()),
            };
        }
        Ok(())
    }

    pub(crate) fn record_error(&self, error: &(dyn std::error::Error + 'static)) {
        let mut source = Some(error);
        while let Some(error) = source {
            if let Some(error) = error.downcast_ref::<sqlx::Error>() {
                if matches!(
                    error,
                    sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_)
                ) || matches!(error, sqlx::Error::Database(db) if db.code().is_some_and(|c| c.starts_with("08")))
                {
                    // A transport error cannot establish whether submitted SQL
                    // finished. Keep that uncertainty across re-election.
                    self.abandoned.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            }
            source = error.source();
        }
    }

    pub(crate) fn status(&self) -> serde_json::Value {
        let observation = self.observation.lock().expect("control observation");
        let allowed = self.allowed_observation(&observation);
        let fresh = observation
            .read_started
            .is_some_and(|time| time.elapsed() < FRESHNESS);
        let outstanding = self.outstanding.load(Ordering::SeqCst);
        let abandoned = self.abandoned.load(Ordering::SeqCst);
        let settlement_unknown = abandoned > 0;
        serde_json::json!({
            "scope": runtime_control::PRUNE_SCOPE, "supported": self.capability.load(Ordering::SeqCst) == 2,
            "observed_revision": observation.value.as_ref().map(|v| v.revision),
            "halt_id": observation.value.as_ref().and_then(|v| v.halt_id),
            "observed_at": observation.observed_at, "fresh": fresh, "admission_allowed": allowed,
            "reported_at": Utc::now(), "report_sequence": self.report_sequence.fetch_add(1, Ordering::SeqCst) + 1,
            "outstanding_callbacks": outstanding, "abandoned_callbacks": abandoned,
            "settlement_unknown": settlement_unknown,
            "state": if settlement_unknown || !fresh { "unknown" } else if allowed { "running" } else if outstanding > 0 { "draining" } else { "halted" }
        })
    }
}

async fn write_status(
    conn: &mut sqlx::PgConnection,
    instance: Uuid,
    status: serde_json::Value,
) -> Result<(), sqlx::Error> {
    // Reporter and leader writes can overlap during election. An older
    // snapshot must not replace a newer observation or settlement report.
    sqlx::query("UPDATE awa.runtime_instances SET runtime_control_observation = $2 || jsonb_build_object('received_at', clock_timestamp()) WHERE instance_id = $1 AND (runtime_control_observation->>'report_sequence' IS NULL OR (runtime_control_observation->>'report_sequence')::bigint < ($2->>'report_sequence')::bigint)")
        .bind(instance).bind(status).execute(conn).await?;
    Ok(())
}

/// A locally dropped SQL future does not establish server-side settlement.
/// Keep abandoned callbacks as unknown for this runtime's lifetime. This is
/// evidence for operators, never a prerequisite for drain-mode resume.
pub(crate) struct Admission(Option<Arc<PruneControl>>);
impl Admission {
    pub(crate) async fn run_result<T, E: std::error::Error + 'static>(
        self,
        work: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let control = self.0.clone();
        self.run(async {
            let result = work.await;
            if let (Some(control), Err(error)) = (&control, &result) {
                control.record_error(error);
            }
            result
        })
        .await
    }

    pub(crate) async fn run<T>(mut self, work: impl std::future::Future<Output = T>) -> T {
        let result = work.await;
        if let Some(control) = self.0.take() {
            control.outstanding.fetch_sub(1, Ordering::SeqCst);
        }
        result
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(control) = &self.0 {
            control.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }
}

pub(crate) struct LeaderObserver {
    stop: CancellationToken,
    pub(crate) lost: CancellationToken,
    leader: Arc<AtomicBool>,
}
impl Drop for LeaderObserver {
    fn drop(&mut self) {
        self.leader.store(false, Ordering::SeqCst);
        self.stop.cancel();
    }
}

impl LeaderObserver {
    pub(crate) async fn start(
        mut conn: PoolConnection<Postgres>,
        control: Arc<PruneControl>,
        leader: Arc<AtomicBool>,
        instance: Option<Uuid>,
        fallback_liveness_interval: Duration,
    ) -> Result<Self, sqlx::Error> {
        {
            control
                .observation
                .lock()
                .expect("control observation")
                .read_started = None;
        }
        // Never return a session-scoped leader lock to the pool, even if the
        // owning future is cancelled or unlock cannot be acknowledged.
        conn.close_on_drop();
        tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query("SET statement_timeout = '1s'").execute(&mut *conn),
        )
        .await
        .unwrap_or(Err(sqlx::Error::PoolTimedOut))?;
        match tokio::time::timeout(Duration::from_secs(1), control.initialize(&mut conn)).await {
            Ok(Err(err)) if connection_lost(&err) => return Err(err),
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "Initial prune control read failed; admission stays closed")
            }
            Err(_) => tracing::warn!(
                "Initial prune control read deadline expired; admission stays closed"
            ),
            Ok(Ok(())) => {}
        }
        let stop = CancellationToken::new();
        let lost = CancellationToken::new();
        let result = Self {
            stop: stop.clone(),
            lost: lost.clone(),
            leader: leader.clone(),
        };
        tokio::spawn(async move {
            struct LeaderGuard(Arc<AtomicBool>, CancellationToken);
            impl Drop for LeaderGuard {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                    self.1.cancel();
                }
            }
            let _guard = LeaderGuard(leader, lost);
            let mut last_liveness = Instant::now();
            loop {
                // Stable per-tick entropy without a connection or shared RNG.
                let jitter_ms = 800 + (Uuid::new_v4().as_u128() % 401) as u64;
                tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(jitter_ms)) => {} }
                let read =
                    tokio::time::timeout(Duration::from_secs(1), control.observe(&mut conn)).await;
                match read {
                    Ok(Err(ref err)) if connection_lost(err) => break,
                    Ok(Err(err)) => tracing::warn!(error = %err, "Prune control read failed"),
                    Err(_) => tracing::warn!("Prune control read deadline expired"),
                    Ok(Ok(())) => {
                        last_liveness = Instant::now();
                    }
                }
                // Successful control/schema reads prove liveness. Probe separately
                // only when failed reads have outlasted the configured interval.
                if last_liveness.elapsed() >= fallback_liveness_interval {
                    match tokio::time::timeout(
                        Duration::from_secs(1),
                        sqlx::query("SELECT 1").execute(&mut *conn),
                    )
                    .await
                    {
                        Ok(Err(ref err)) if connection_lost(err) => break,
                        Ok(Err(err)) => {
                            tracing::warn!(error = %err, "Leader liveness read failed without session loss");
                            continue;
                        }
                        Err(_) => continue,
                        Ok(Ok(_)) => {
                            last_liveness = Instant::now();
                        }
                    }
                }
                if let Some(instance) =
                    instance.filter(|_| control.capability.load(Ordering::SeqCst) == 2)
                {
                    let status = control.status();
                    let _ = tokio::time::timeout(
                        Duration::from_secs(1),
                        write_status(&mut conn, instance, status),
                    )
                    .await;
                }
            }
        });
        Ok(result)
    }
}

fn connection_lost(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => true,
        sqlx::Error::Database(err) => err.code().is_some_and(|c| {
            c.starts_with("08") || matches!(c.as_ref(), "57P01" | "57P02" | "57P03")
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use awa_model::migrations;
    use sqlx::{Executor, PgPool};

    async fn wait_for(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("control observation on the established session");
    }

    async fn instance(pool: &PgPool) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO awa.runtime_instances(instance_id,pid,version,started_at,snapshot_interval_ms,healthy,postgres_connected,poll_loop_alive,heartbeat_alive,shutting_down,leader) VALUES ($1,1,'test',now(),1000,true,true,true,true,false,false)")
            .bind(id).execute(pool).await.unwrap();
        id
    }

    #[sqlx::test(migrations = false)]
    async fn versionless_installation_is_discovered_without_reelection(pool: PgPool) {
        for (_, _, sql) in migrations::migration_sql_range(0, 40) {
            pool.execute(sqlx::raw_sql(&sql)).await.unwrap();
        }
        let control = Arc::new(PruneControl::default());
        let leader = Arc::new(AtomicBool::new(true));
        let observer = LeaderObserver::start(
            pool.acquire().await.unwrap(),
            control.clone(),
            leader,
            None,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(control.allowed());
        assert_eq!(control.status()["supported"], false);
        migrations::run(&pool).await.unwrap();
        let halt = runtime_control::halt(
            &pool,
            runtime_control::PRUNE_SCOPE,
            "installed live",
            "drain",
        )
        .await
        .unwrap();
        wait_for(|| control.status()["observed_revision"] == halt.revision).await;
        assert!(!control.allowed());
        pool.execute("DELETE FROM awa.runtime_controls")
            .await
            .unwrap();
        wait_for(|| !control.status()["fresh"].as_bool().unwrap()).await;
        assert!(
            !control.allowed(),
            "an installed missing row must fail closed"
        );
        drop(observer);
    }

    #[sqlx::test(migrations = false)]
    async fn held_session_observes_and_reports_without_pool_or_new_connections(admin: PgPool) {
        migrations::run(&admin).await.unwrap();
        let id = instance(&admin).await;
        let role = format!("prune_observer_{}", Uuid::new_v4().simple());
        admin.execute(sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'phase1'; GRANT USAGE ON SCHEMA awa TO {role}; GRANT SELECT ON awa.runtime_controls TO {role}; GRANT UPDATE (runtime_control_observation) ON awa.runtime_instances TO {role}; GRANT SELECT (instance_id,runtime_control_observation) ON awa.runtime_instances TO {role};"))).await.unwrap();
        let options = (*admin.connect_options())
            .clone()
            .username(&role)
            .password("phase1");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options.clone())
            .await
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(42)")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert!(locked);
        let control = Arc::new(PruneControl::default());
        let leader = Arc::new(AtomicBool::new(true));
        let observer = LeaderObserver::start(
            conn,
            control.clone(),
            leader.clone(),
            Some(id),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let busy = pool.acquire().await.unwrap();
        admin
            .execute(sqlx::raw_sql(&format!(
                "ALTER ROLE {role} CONNECTION LIMIT 0"
            )))
            .await
            .unwrap();
        assert!(sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(1))
            .connect_with(options)
            .await
            .is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), pool.acquire())
                .await
                .is_err()
        );
        let halt = runtime_control::halt(
            &admin,
            runtime_control::PRUNE_SCOPE,
            "held session",
            "drain",
        )
        .await
        .unwrap();
        wait_for(|| control.status()["observed_revision"] == halt.revision).await;
        assert!(!control.allowed());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let value: Option<serde_json::Value> = sqlx::query_scalar("SELECT runtime_control_observation FROM awa.runtime_instances WHERE instance_id=$1").bind(id).fetch_one(&admin).await.unwrap();
                if value.is_some_and(|v| v["observed_revision"] == halt.revision && v["state"] == "halted") { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("publication uses the held session and column-specific grants");
        let resumed =
            runtime_control::resume(&admin, runtime_control::PRUNE_SCOPE, halt.halt_id.unwrap())
                .await
                .unwrap();
        wait_for(|| control.status()["observed_revision"] == resumed.revision).await;
        let mut block = admin.begin().await.unwrap();
        sqlx::query("LOCK TABLE awa.runtime_controls IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *block)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5100)).await;
        assert!(!control.allowed());
        assert!(
            leader.load(Ordering::SeqCst),
            "a control timeout does not lose leadership"
        );
        assert!(control.admit(MaintenanceBranch::Promote).is_some());
        assert!(control.admit(MaintenanceBranch::RescueHeartbeat).is_some());
        assert!(control.admit(MaintenanceBranch::CronSync).is_some());
        block.rollback().await.unwrap();
        wait_for(|| control.allowed()).await;
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .execute(&admin)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), observer.lost.cancelled())
            .await
            .unwrap();
        assert!(!leader.load(Ordering::SeqCst));
        drop(observer);
        drop(busy);
        pool.close().await;
        admin
            .execute(sqlx::raw_sql(&format!(
                "DROP OWNED BY {role}; DROP ROLE {role}"
            )))
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = false)]
    async fn nonleader_reports_drain_and_rejects_delayed_status(pool: PgPool) {
        migrations::run(&pool).await.unwrap();
        let id = instance(&pool).await;
        let control = Arc::new(PruneControl::default());
        control.observe_pool(&pool).await.unwrap();
        let admission = control.admit(MaintenanceBranch::Cleanup).unwrap();
        let halt = runtime_control::halt(&pool, runtime_control::PRUNE_SCOPE, "reporting", "drain")
            .await
            .unwrap();
        control.observe_pool(&pool).await.unwrap();
        control.publish_pool(&pool, id).await.unwrap();
        let newest: serde_json::Value = sqlx::query_scalar(
            "SELECT runtime_control_observation FROM awa.runtime_instances WHERE instance_id=$1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(newest["observed_revision"], halt.revision);
        assert_eq!(newest["state"], "draining");
        let mut obsolete = newest.clone();
        obsolete["report_sequence"] =
            serde_json::json!(newest["report_sequence"].as_u64().unwrap() - 1);
        obsolete["state"] = serde_json::json!("halted");
        write_status(&mut pool.acquire().await.unwrap(), id, obsolete)
            .await
            .unwrap();
        let retained: serde_json::Value = sqlx::query_scalar(
            "SELECT runtime_control_observation FROM awa.runtime_instances WHERE instance_id=$1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(retained, newest);
        admission.run(async {}).await;
        let mut skewed = control.status();
        skewed["reported_at"] = serde_json::json!("1970-01-01T00:00:00Z");
        write_status(&mut pool.acquire().await.unwrap(), id, skewed)
            .await
            .unwrap();
        let recent: bool = sqlx::query_scalar("SELECT (runtime_control_observation->>'received_at')::timestamptz > clock_timestamp() - interval '5 seconds' FROM awa.runtime_instances WHERE instance_id=$1")
            .bind(id).fetch_one(&pool).await.unwrap();
        assert!(
            recent,
            "report recency uses the server receipt, not the runtime clock"
        );
    }

    #[sqlx::test(migrations = false)]
    async fn cancelled_sql_stays_unknown_but_drain_resume_is_unblocked(pool: PgPool) {
        migrations::run(&pool).await.unwrap();
        let control = Arc::new(PruneControl::default());
        control.observe_pool(&pool).await.unwrap();
        let admission = control.admit(MaintenanceBranch::Cleanup).unwrap();
        let worker = pool.clone();
        let task = tokio::spawn(async move {
            admission
                .run(async {
                    let mut tx = worker.begin().await.unwrap();
                    sqlx::query("SELECT pg_sleep(2) /* awa_prune_unknown */")
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                })
                .await;
        });
        tokio::time::timeout(Duration::from_secs(3),async {
            loop {
                let active: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'SELECT pg_sleep(2)%' AND state='active')").fetch_one(&pool).await.unwrap();
                if active {break;}
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(control.status()["state"], "unknown");
        let halt = runtime_control::halt(
            &pool,
            runtime_control::PRUNE_SCOPE,
            "cancelled work",
            "drain",
        )
        .await
        .unwrap();
        runtime_control::resume(&pool, runtime_control::PRUNE_SCOPE, halt.halt_id.unwrap())
            .await
            .unwrap();
        control.observe_pool(&pool).await.unwrap();
        assert!(control.allowed());
        assert_eq!(control.status()["state"], "unknown");
    }

    #[test]
    fn scheduler_registers_branches_with_the_closed_scope_vocabulary() {
        let source = include_str!("maintenance.rs");
        let leader = source
            .split("pub async fn run(&self)")
            .nth(1)
            .unwrap()
            .split("let work = async {")
            .nth(1)
            .unwrap()
            .split("            tokio::select! {\n                _ = observer.lost")
            .next()
            .unwrap();
        // Every select arm except cancellation must register and gate its work.
        let selected = leader.matches("\n                        _ = ").count();
        assert_eq!(
            selected,
            leader.matches("branch_tracker.try_begin").count() + 1
        );
        assert!(!leader.contains("try_begin(\""));
        assert!(!leader.contains("try_begin_without_cooldown(\""));
        assert_eq!(
            leader.matches("branch_tracker.try_begin").count(),
            leader
                .matches("self.control.admit(MaintenanceBranch::")
                .count()
        );
    }
}
