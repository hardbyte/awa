use super::config::Persona;
use super::ledger::{expect, flag, samples_terminal_proof, JobRecord};
use super::shared::Shared;
use awa::JobRow;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

pub const ENV_ENABLED: &str = "AWA_SOAK_GATE_PROOF";
pub const ENV_URL: &str = "AWA_SOAK_GATE_PROOF_URL";
pub const ERROR_MARKER: &str = "AWA_SOAK_GATE_PROOF_INCOMPLETE";
const TIMEOUT: Duration = Duration::from_secs(2);
const WITNESS_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PENDING: usize = 100_000;
const MAX_ROWS: i64 = 100_001;

type WitnessStats = Arc<Mutex<HashMap<&'static str, i64>>>;

pub struct GateProof {
    owner: Handle,
    pool: PgPool,
    write_pool: Option<PgPool>,
    write_stats: WitnessStats,
    reported_lease_duplicates: Mutex<HashSet<(i64, i64)>>,
}

pub fn enabled_from_env() -> Result<bool, String> {
    match std::env::var(ENV_ENABLED).as_deref() {
        Ok("1") => Ok(true),
        Ok("0") | Err(std::env::VarError::NotPresent) => Ok(false),
        _ => Err(format!("{ENV_ENABLED} must be 0 or 1")),
    }
}

struct WitnessCaller {
    abort: tokio::task::AbortHandle,
    stats: WitnessStats,
    pending: bool,
}

impl Drop for WitnessCaller {
    fn drop(&mut self) {
        self.abort.abort();
        if self.pending {
            *self
                .stats
                .lock()
                .expect("proof write stats")
                .entry("caller_cancellations")
                .or_default() += 1;
        }
    }
}

async fn write_on_owner(
    owner: &Handle,
    deadline: tokio::time::Instant,
    stats: WitnessStats,
    write: impl Future<Output = Result<(), String>> + Send + 'static,
) -> Result<(), String> {
    let task = owner.spawn(async move {
        if tokio::time::Instant::now() >= deadline {
            return Err("owner dispatch timeout".to_string());
        }
        write.await
    });
    let mut caller = WitnessCaller {
        abort: task.abort_handle(),
        stats: stats.clone(),
        pending: true,
    };
    let result = tokio::time::timeout_at(deadline, task).await;
    caller.pending = false;
    match result {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            *stats
                .lock()
                .expect("proof write stats")
                .entry("owner_join_errors")
                .or_default() += 1;
            Err(format!("owner task failed: {error}"))
        }
        Err(_) => {
            *stats
                .lock()
                .expect("proof write stats")
                .entry("owner_timeouts")
                .or_default() += 1;
            Err("owner dispatch/ack timeout".to_string())
        }
    }
}

impl GateProof {
    async fn connect(
        url: &str,
        max_connections: u32,
        application: &str,
        statement_timeout: Duration,
    ) -> Result<Self, String> {
        let pool = tokio::time::timeout(
            TIMEOUT,
            super::db::connect_pool_with_timeout(
                url,
                application,
                max_connections,
                Some(statement_timeout),
            ),
        )
        .await
        .map_err(|_| "proof pool connect timeout".to_string())?
        .map_err(|error| format!("proof pool connect: {error}"))?;
        Ok(Self {
            owner: Handle::current(),
            pool,
            write_pool: None,
            write_stats: Arc::new(Mutex::new(HashMap::new())),
            reported_lease_duplicates: Mutex::new(HashSet::new()),
        })
    }

    pub fn owned_url(shared: &Shared) -> String {
        if shared.config.has(Persona::ShardOps) {
            super::roles::url_for_role(&shared.config, super::roles::OWNER)
        } else {
            shared.config.database_url.clone()
        }
    }

    pub async fn install(shared: &Arc<Shared>) -> Result<(), String> {
        // The persona fleets share what is left, so the proof's pools come out of the
        // same budget instead of the headroom kept for the oracle and chaos sessions.
        let audit_connections = shared.budget.reserve("gate-proof-audit", 2, 1);
        let witness_connections = shared.budget.reserve("gate-proof-witness", 16, 1);
        // One witness connection each in the python and claim-and-abandon processes.
        shared.budget.reserve("gate-proof-children", 2, 0);
        let mut proof = Self::connect(
            &Self::owned_url(shared),
            audit_connections,
            "awa_gate_proof_parent_audit",
            TIMEOUT,
        )
        .await?;
        proof.write_pool = Some(
            Self::connect(
                &Self::owned_url(shared),
                witness_connections,
                "awa_gate_proof_parent_witness",
                WITNESS_WRITE_TIMEOUT,
            )
            .await?
            .pool,
        );
        let setup_pool = &proof.pool;
        let schema = shared.info.queue_storage_schema.as_deref().unwrap_or("awa");
        if !schema.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
            || !schema
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err("invalid proof queue schema".into());
        }
        let setup = include_str!("gate_proof.sql").replace("__SCHEMA__", schema);
        tokio::time::timeout(
            Duration::from_secs(10),
            sqlx::raw_sql(&setup).execute(setup_pool),
        )
        .await
        .map_err(|_| "proof setup deadline exceeded".to_string())?
        .map_err(|error| format!("proof setup: {error}"))?;
        for (parent, trigger) in [
            (
                format!("{schema}.done_entries"),
                "awa_soak_audit_done_insert",
            ),
            (
                format!("{schema}.receipt_completion_batches"),
                "awa_soak_audit_receipt_insert",
            ),
        ] {
            let coverage: (i64, i64) = tokio::time::timeout(
                TIMEOUT,
                sqlx::query_as(
                    "SELECT count(*),count(*) FILTER(WHERE EXISTS(
                   SELECT 1 FROM pg_trigger AS installed
                   WHERE installed.tgrelid=tree.relid AND installed.tgname=$2
                     AND installed.tgtype=5 AND installed.tgenabled='O'))
                 FROM pg_partition_tree($1::text::regclass) AS tree",
                )
                .bind(parent)
                .bind(trigger)
                .fetch_one(setup_pool),
            )
            .await
            .map_err(|_| "proof trigger coverage timeout".to_string())?
            .map_err(|error| format!("proof trigger coverage: {error}"))?;
            if coverage.0 < 2 || coverage.0 != coverage.1 {
                return Err(format!("proof trigger coverage incomplete: {coverage:?}"));
            }
        }
        let persistent: i64=tokio::time::timeout(TIMEOUT,sqlx::query_scalar(
            "SELECT count(*) FROM pg_class
             WHERE oid IN ('public.awa_soak_handler_returns'::regclass,'public.awa_soak_completed_inserts'::regclass)
               AND relpersistence='p'"
        ).fetch_one(setup_pool)).await.map_err(|_|"proof persistence timeout".to_string())?
            .map_err(|error|format!("proof persistence: {error}"))?;
        if persistent != 2 {
            return Err("proof tables are not both logged".into());
        }
        let quiet: i64 = tokio::time::timeout(
            TIMEOUT,
            sqlx::query_scalar(
                "SELECT count(*) FROM pg_class AS proof
                 JOIN pg_class AS toast ON toast.oid=proof.reltoastrelid
                 WHERE proof.oid IN ('public.awa_soak_handler_returns'::regclass,
                                     'public.awa_soak_completed_inserts'::regclass)
                   AND toast.relpersistence='p'
                   AND EXISTS (SELECT 1 FROM pg_options_to_table(proof.reloptions)
                               WHERE option_name='autovacuum_enabled' AND option_value='false')
                   AND EXISTS (SELECT 1 FROM pg_options_to_table(toast.reloptions)
                               WHERE option_name='autovacuum_enabled' AND option_value='false')",
            )
            .fetch_one(setup_pool),
        )
        .await
        .map_err(|_| "proof autovacuum validation timeout".to_string())?
        .map_err(|error| format!("proof autovacuum validation: {error}"))?;
        if quiet != 2 {
            return Err("proof heaps and TOAST tables must explicitly disable autovacuum".into());
        }
        let trusted: i64 = tokio::time::timeout(
            TIMEOUT,
            sqlx::query_scalar(
                "SELECT count(*) FROM pg_proc
             WHERE oid IN ('public.awa_soak_audit_done_insert()'::regprocedure,
                           'public.awa_soak_audit_receipt_insert()'::regprocedure)
               AND prosecdef AND proowner=current_user::regrole
               AND proconfig=ARRAY['search_path=pg_catalog']",
            )
            .fetch_one(setup_pool),
        )
        .await
        .map_err(|_| "proof function validation timeout".to_string())?
        .map_err(|error| format!("proof function validation: {error}"))?;
        if trusted != 2 {
            return Err("proof functions lack the fixture owner/fixed search_path".into());
        }

        shared
            .gate_proof
            .set(proof)
            .map_err(|_| "proof installed twice".to_string())?;
        shared.run.add("gate_proof_enabled", 1);
        shared.run.add("gate_proof_parent_max_connections", 18);
        shared
            .run
            .add("gate_proof_parent_witness_max_connections", 16);
        shared.run.add("gate_proof_parent_audit_max_connections", 2);
        shared.note("gate_proof","logged handler-return and completion-insert audit enabled; extra SQL/WAL and up to 18 parent (16 witness + 2 audit) + 1 Rust-child + 1 Python-child connections; product pools,subscriptions,aging and retention unchanged");
        Ok(())
    }

    pub async fn child() -> Result<Option<Self>, String> {
        if !enabled_from_env()? {
            return Ok(None);
        }
        let url = std::env::var(ENV_URL)
            .map_err(|_| "owned proof URL not passed to child".to_string())?;
        Self::connect(&url, 1, "awa_gate_proof_child", WITNESS_WRITE_TIMEOUT)
            .await
            .map(Some)
    }

    pub async fn returned_completed(&self, job: &JobRow, persona: &str) -> Result<(), String> {
        let pool = self.write_pool.as_ref().unwrap_or(&self.pool).clone();
        let started = Instant::now();
        let deadline_at = tokio::time::Instant::now() + WITNESS_WRITE_TIMEOUT;
        let pool_size = pool.size();
        let pool_idle = pool.num_idle();
        let job_id = job.id;
        let run_lease = job.run_lease;
        let queue = job.queue.clone();
        let request_persona = persona.to_string();
        let write_stats = self.write_stats.clone();
        write_on_owner(&self.owner, deadline_at, self.write_stats.clone(), async move {
        let acquired = tokio::time::timeout_at(deadline_at, pool.acquire()).await;
        let acquire_us = started.elapsed().as_micros().min(i64::MAX as u128) as i64;
        let execute_started = Instant::now();
        let (failure_kind, result) = match acquired {
            Err(_) => (Some("acquire_timeouts"), Err("acquire timeout".to_string())),
            Ok(Err(error)) => (Some("acquire_errors"), Err(format!("acquire error: {error}"))),
            Ok(Ok(mut connection)) => {
                match tokio::time::timeout_at(deadline_at, sqlx::query(
                    "INSERT INTO public.awa_soak_handler_returns(job_id,run_lease,queue,persona,outcome)
                     VALUES($1,$2,$3,$4,'completed')"
                ).bind(job_id).bind(run_lease).bind(&queue).bind(&request_persona).execute(&mut *connection)).await {
                    Err(_) => (Some("execute_timeouts"), Err("execute timeout".to_string())),
                    Ok(Err(error)) => (Some("execute_errors"), Err(format!("execute error: {error}"))),
                    Ok(Ok(_)) => (None, Ok(())),
                }
            }
        };
        let execute_us = if matches!(failure_kind, Some("acquire_timeouts" | "acquire_errors")) {
            0
        } else {
            execute_started.elapsed().as_micros().min(i64::MAX as u128) as i64
        };
        {
            let mut stats = write_stats.lock().expect("proof write stats");
            for (name, value) in [
                ("attempts", 1),
                ("acquire_us", acquire_us),
                ("execute_us", execute_us),
                ("pool_size_sum", i64::from(pool_size)),
                ("pool_idle_sum", pool_idle as i64),
            ] {
                *stats.entry(name).or_default() += value;
            }
            if let Some(kind) = failure_kind {
                *stats.entry(kind).or_default() += 1;
            }
        }
        result.map_err(|error| format!("{error}; acquire_us={acquire_us} execute_us={execute_us} pool_size={pool_size} pool_idle={pool_idle}"))
        }).await.map_err(|error| format!("handler return proof {error}; job_id={} run_lease={} persona={persona}",job.id,job.run_lease))
    }

    fn publish_write_stats(&self, shared: &Shared) {
        let stats = std::mem::take(&mut *self.write_stats.lock().expect("proof write stats"));
        for (name, value) in stats {
            shared.run.add(&format!("gate_proof_witness_{name}"), value);
        }
    }

    pub async fn close(&self) -> Result<(), String> {
        tokio::time::timeout(TIMEOUT, async {
            tokio::join!(self.pool.close(), async {
                if let Some(pool) = &self.write_pool {
                    pool.close().await;
                }
            });
        })
        .await
        .map_err(|_| "proof pools close timeout".to_string())
    }
}

pub fn absorb_error(shared: &Shared, line: &str) -> bool {
    if line != ERROR_MARKER {
        return false;
    }
    shared.incomplete(
        "child handler-return proof failed; execution witness incomplete",
        vec![],
    );
    true
}

pub async fn observe(shared: &Shared) {
    let Some(proof) = shared.gate_proof.get() else {
        return;
    };
    proof.publish_write_stats(shared);
    let pool = &proof.pool;
    let mut pending = Vec::new();
    shared.ledger.for_each(|id, record| {
        if ((record.has(flag::RETURNED_COMPLETED) && !record.has(flag::NO_EVENTS))
            || samples_terminal_proof(id))
            && !record.has_durable_terminal()
            && !record.has(flag::OPERATOR_REMOVED)
            && !record.has(flag::DLQ_PURGED)
            && pending.len() <= MAX_PENDING
        {
            pending.push((id, record.latest_run_lease, record.has(flag::NO_EVENTS)));
        }
    });
    if pending.len() > MAX_PENDING {
        shared.incomplete("proof observer pending cap exceeded", vec![]);
        return;
    }
    if pending.is_empty() {
        return;
    }
    let ids: Vec<_> = pending.iter().map(|row| row.0).collect();
    let leases: Vec<_> = pending.iter().map(|row| row.1).collect();
    let no_events: Vec<_> = pending.iter().map(|row| row.2).collect();
    let schema = shared.info.queue_storage_schema.as_deref().unwrap_or("awa");
    let started = Instant::now();
    shared.run.add("gate_proof_observer_queries", 1);
    let rows=tokio::time::timeout(TIMEOUT,sqlx::query_as::<_,(i64,i64,String)>(&format!(
        "WITH pending AS (SELECT * FROM unnest($1::bigint[],$2::bigint[],$3::boolean[]) AS refs(job_id,run_lease,no_events))
         SELECT DISTINCT audit.job_id,audit.run_lease,'completed'::text
         FROM public.awa_soak_completed_inserts AS audit JOIN pending ON audit.job_id=pending.job_id
           AND ((pending.no_events AND audit.run_lease>=pending.run_lease)
             OR (NOT pending.no_events AND audit.run_lease=pending.run_lease))
         UNION ALL SELECT done.job_id,done.run_lease,done.state::text FROM {schema}.done_entries AS done
         JOIN pending ON done.job_id=pending.job_id
           AND ((pending.no_events AND done.run_lease>=pending.run_lease)
             OR (NOT pending.no_events AND done.run_lease=pending.run_lease)) WHERE done.state<>'completed'
         UNION ALL SELECT dlq.job_id,dlq.run_lease,'failed' FROM {schema}.dlq_entries AS dlq
         JOIN pending ON dlq.job_id=pending.job_id
           AND ((pending.no_events AND dlq.run_lease>=pending.run_lease)
             OR (NOT pending.no_events AND dlq.run_lease=pending.run_lease)) LIMIT $4"
    )).bind(&ids).bind(&leases).bind(&no_events).bind(MAX_ROWS).fetch_all(pool)).await;
    shared.run.add(
        "gate_proof_observer_us",
        started.elapsed().as_micros().min(i64::MAX as u128) as i64,
    );
    match rows {
        Ok(Ok(rows)) if rows.len() <= MAX_PENDING => {
            shared
                .run
                .add("gate_proof_observed_rows", rows.len() as i64);
            let now = shared.now_ms() as u32;
            for (id, lease, state) in rows {
                shared.ledger.with(
                    id,
                    || JobRecord::new(Persona::Chaos, u32::MAX, now, expect::ANY_TERMINAL, 0),
                    |record| record.note_observed_terminal(lease, &state),
                );
            }
        }
        failure => shared.incomplete(format!("proof observer incomplete: {failure:?}"), vec![]),
    }
}

pub async fn check_lease_duplicates(shared: &Shared) {
    let Some(proof) = shared.gate_proof.get() else {
        return;
    };
    let pool = &proof.pool;
    let schema = shared.info.queue_storage_schema.as_deref().unwrap_or("awa");
    let started = Instant::now();
    shared.run.add("gate_proof_lease_snapshot_queries", 1);
    let result = tokio::time::timeout(
        TIMEOUT,
        sqlx::query_as::<_, (i64, i64, i64)>(&format!(
            "SELECT job_id,run_lease,count(*) FROM {schema}.leases
         GROUP BY job_id,run_lease HAVING count(*)>1 ORDER BY job_id,run_lease LIMIT 25"
        ))
        .fetch_all(pool),
    )
    .await;
    shared.run.add(
        "gate_proof_lease_snapshot_us",
        started.elapsed().as_micros().min(i64::MAX as u128) as i64,
    );
    match result {
        Ok(Ok(rows)) => {
            let mut reported = proof
                .reported_lease_duplicates
                .lock()
                .expect("lease duplicate proof");
            let rows: Vec<_> = rows
                .into_iter()
                .filter(|row| reported.insert((row.0, row.1)))
                .map(|(id, lease, count)| {
                    serde_json::json!({"job_id":id,"run_lease":lease,"count":count}).to_string()
                })
                .collect();
            if !rows.is_empty() {
                shared.violation(
                    "duplicate_materialized_leases",
                    "snapshot found duplicate materialized lease identities; receipts are excluded",
                    rows,
                );
            }
        }
        failure => shared.incomplete(
            format!("materialized lease duplicate snapshot incomplete: {failure:?}"),
            vec![],
        ),
    }
}

pub async fn reconcile(shared: &Shared) {
    let Some(proof) = shared.gate_proof.get() else {
        return;
    };
    proof.publish_write_stats(shared);
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let counts = tokio::time::timeout_at(
        deadline,
        sqlx::query_as::<_, (i64, i64, i64, i64)>(
            "SELECT handler.rows,handler.pairs,completed.rows,completed.pairs
             FROM (SELECT coalesce(sum(n),0)::bigint AS rows,count(*) AS pairs
                   FROM (SELECT count(*) AS n FROM public.awa_soak_handler_returns
                         GROUP BY job_id,run_lease) AS pairs) AS handler
             CROSS JOIN (SELECT coalesce(sum(n),0)::bigint AS rows,count(*) AS pairs
                   FROM (SELECT count(*) AS n FROM public.awa_soak_completed_inserts
                         GROUP BY job_id,run_lease) AS pairs) AS completed",
        )
        .fetch_one(&proof.pool),
    )
    .await;
    match counts {
        Ok(Ok((returns, return_pairs, inserts, completed_pairs))) => {
            for (name, value) in [
                ("gate_handler_return_rows", returns),
                ("gate_handler_return_unique_pairs", return_pairs),
                ("gate_completed_insert_rows", inserts),
                ("gate_completed_unique_pairs", completed_pairs),
            ] {
                shared.run.add(name, value);
            }
            shared.note("gate_proof",format!("final counts in {:?}; handler rows={returns},handler pairs={return_pairs},committed completed insert rows={inserts},unique completed pairs={completed_pairs}; handler returns are not commits; stale returned pairs need not complete",started.elapsed()));
            if inserts > completed_pairs {
                let details = tokio::time::timeout_at(
                    deadline,
                    sqlx::query_as::<_, (i64, i64, i64)>(
                        "SELECT job_id,run_lease,count(*) FROM public.awa_soak_completed_inserts
                         GROUP BY job_id,run_lease HAVING count(*)>1 ORDER BY job_id,run_lease LIMIT 25",
                    )
                    .fetch_all(&proof.pool),
                )
                .await;
                let rows = match details {
                    Ok(Ok(duplicates)) => duplicates
                        .into_iter()
                        .map(|(id, lease, count)| {
                            serde_json::json!({"job_id":id,"run_lease":lease,"count":count})
                                .to_string()
                        })
                        .collect(),
                    failure => {
                        let message = format!(
                            "full completion proof duplicate details incomplete: {failure:?}"
                        );
                        shared.note("gate_proof", message.clone());
                        shared.incomplete(message, vec![]);
                        vec![]
                    }
                };
                shared.violation("duplicate_completed_inserts",format!("{inserts} completed insert facts for {completed_pairs} distinct attempt pairs"),rows);
            }
        }
        failure => {
            let message = format!("full completion proof counts incomplete: {failure:?}");
            shared.note("gate_proof", message.clone());
            shared.incomplete(message, vec![]);
        }
    }
    shared.run.add(
        "gate_proof_reconcile_us",
        started.elapsed().as_micros().min(i64::MAX as u128) as i64,
    );
    check_lease_duplicates(shared).await;
}

#[cfg(test)]
mod witness_owner_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn stats() -> WitnessStats {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime")
    }

    #[test]
    fn witness_io_survives_caller_runtime_shutdown() {
        let owner = runtime();
        let address = owner.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind echo server");
            let address = listener.local_addr().expect("echo address");
            tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.expect("accept socket");
                while let Ok(value) = socket.read_u8().await {
                    socket.write_u8(value).await.expect("echo byte");
                }
            });
            address
        });
        let socket = Arc::new(tokio::sync::Mutex::new(None::<tokio::net::TcpStream>));
        let first_socket = socket.clone();
        let first_caller = runtime();
        first_caller
            .block_on(write_on_owner(
                owner.handle(),
                tokio::time::Instant::now() + Duration::from_secs(2),
                stats(),
                async move {
                    let mut connection = tokio::net::TcpStream::connect(address)
                        .await
                        .map_err(|error| error.to_string())?;
                    connection
                        .write_u8(41)
                        .await
                        .map_err(|error| error.to_string())?;
                    assert_eq!(
                        connection
                            .read_u8()
                            .await
                            .map_err(|error| error.to_string())?,
                        41
                    );
                    *first_socket.lock().await = Some(connection);
                    Ok(())
                },
            ))
            .expect("first owned socket write");
        first_caller.shutdown_timeout(Duration::from_secs(1));
        let next_caller = runtime();
        next_caller
            .block_on(write_on_owner(
                owner.handle(),
                tokio::time::Instant::now() + Duration::from_secs(2),
                stats(),
                async move {
                    let mut socket = socket.lock().await;
                    let connection = socket.as_mut().expect("reused socket");
                    connection
                        .write_u8(42)
                        .await
                        .map_err(|error| error.to_string())?;
                    assert_eq!(
                        connection
                            .read_u8()
                            .await
                            .map_err(|error| error.to_string())?,
                        42
                    );
                    Ok(())
                },
            ))
            .expect("socket survives original caller runtime");
    }

    struct Dropped(std::sync::mpsc::Sender<()>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn canceling_witness_wait_aborts_owned_task() {
        let owner = runtime();
        let caller = runtime();
        let owner_handle = owner.handle().clone();
        let stats = stats();
        let caller_stats = stats.clone();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (dropped, dropped_rx) = std::sync::mpsc::channel();
        caller.block_on(async move {
            let waiter = tokio::spawn(async move {
                write_on_owner(
                    &owner_handle,
                    tokio::time::Instant::now() + Duration::from_secs(2),
                    caller_stats,
                    async move {
                        let _guard = Dropped(dropped);
                        started.send(()).expect("signal started");
                        std::future::pending::<()>().await;
                        Ok(())
                    },
                )
                .await
            });
            started_rx.await.expect("owned task started");
            waiter.abort();
            assert!(waiter.await.expect_err("caller canceled").is_cancelled());
        });
        dropped_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("owned task canceled instead of detached");
        assert_eq!(
            stats.lock().expect("stats").get("caller_cancellations"),
            Some(&1)
        );
    }

    #[test]
    fn witness_deadline_includes_owner_dispatch() {
        let owner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("undriven owner runtime");
        let caller = runtime();
        let polled = Arc::new(AtomicBool::new(false));
        let operation_polled = polled.clone();
        let stats = stats();
        let started = Instant::now();
        let result = caller.block_on(write_on_owner(
            owner.handle(),
            tokio::time::Instant::now() + Duration::from_millis(100),
            stats.clone(),
            async move {
                operation_polled.store(true, Ordering::Relaxed);
                Ok(())
            },
        ));
        assert!(result.expect_err("dispatch bounded").contains("timeout"));
        assert!(!polled.load(Ordering::Relaxed));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(stats.lock().expect("stats").get("owner_timeouts"), Some(&1));
    }

    #[test]
    fn witness_write_has_one_absolute_deadline() {
        let owner = runtime();
        let caller = runtime();
        let finished = Arc::new(AtomicBool::new(false));
        let operation_finished = finished.clone();
        let started = Instant::now();
        let result = caller.block_on(write_on_owner(
            owner.handle(),
            tokio::time::Instant::now() + Duration::from_millis(200),
            stats(),
            async move {
                tokio::time::sleep(Duration::from_millis(120)).await;
                tokio::time::sleep(Duration::from_millis(120)).await;
                operation_finished.store(true, Ordering::Relaxed);
                Ok(())
            },
        ));
        assert!(result.expect_err("one total budget").contains("timeout"));
        assert!(!finished.load(Ordering::Relaxed));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn witness_owner_panic_is_an_error() {
        let owner = runtime();
        let caller = runtime();
        let stats = stats();
        let result = caller.block_on(write_on_owner(
            owner.handle(),
            tokio::time::Instant::now() + Duration::from_secs(2),
            stats.clone(),
            async { panic!("owned write control") },
        ));
        assert!(result
            .expect_err("join panic surfaced")
            .contains("owner task failed"));
        assert_eq!(
            stats.lock().expect("stats").get("owner_join_errors"),
            Some(&1)
        );
    }
}
