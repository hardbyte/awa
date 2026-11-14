//! Throwaway spike: distinguish startup permission, live admission and a durable halt.
//! `cargo run -p awa --example runtime_control_spike -- --interactive`
//! `cargo run -p awa --example runtime_control_spike -- --database`

#[path = "runtime_control_spike/control.rs"]
mod control;

use awa::{admin, migrations, Client, QueueConfig};
use control::Controls;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

type SpikeResult<T> = Result<T, Box<dyn std::error::Error>>;

fn report(experiment: &str, result: serde_json::Value) {
    println!("{}", json!({"experiment": experiment, "result": result}));
}

fn model_demo() {
    let mut state = Controls::default();
    state.halt();
    state.feature_allowed = false;
    state.feature_allowed = true;
    report(
        "feature_reenable_does_not_clear_halt",
        json!({"admission": state.allows_admission(), "state": state}),
    );
    let old_generation = state.generation;
    state.halt();
    let stale_resume_accepted = state.resume(old_generation);
    report(
        "stale_resume",
        json!({"accepted": stale_resume_accepted, "admission": state.allows_admission()}),
    );
    state.resume(state.generation);
    state.control_fresh = false;
    report(
        "expired_control_permission",
        json!({"admission": state.allows_admission()}),
    );
    state.control_fresh = true;
    state.startup_allowed = false;
    report(
        "startup_ceiling",
        json!({"admission": state.allows_admission(), "state": state}),
    );
    let mut local_state = Controls::default();
    local_state.halt_local();
    local_state.halt();
    local_state.associate_local();
    local_state.resume(local_state.generation);
    report(
        "fleet_resume_releases_associated_local_halt",
        json!({"admission": local_state.allows_admission(), "state": local_state}),
    );
    let token = local_state.halt_local();
    local_state.halt();
    local_state.resume(local_state.generation);
    report(
        "fleet_resume_preserves_unassociated_local_halt",
        json!({"admission": local_state.allows_admission(), "state": local_state}),
    );
    let stale_local_resume = local_state.resume_local(token.saturating_sub(1));
    local_state.feature_allowed = false;
    local_state.resume_local(token);
    report(
        "local_resume_preserves_external_deny",
        json!({"stale_local_resume_accepted": stale_local_resume, "admission": local_state.allows_admission(), "state": local_state}),
    );
}

fn interactive() -> SpikeResult<()> {
    let mut state = Controls::default();
    let mut message = String::new();
    loop {
        println!("\x1b[2J\x1b[H\x1b[1mPROTOTYPE: runtime admission\x1b[0m");
        println!("{}", serde_json::to_string_pretty(&state)?);
        println!("admission_allowed: {}", state.allows_admission());
        println!("{message}\n");
        println!("[h] halt  [r] resume current  [s] stale resume");
        println!("[l] local halt  [p] associate with fleet generation  [u] local resume");
        println!("[f] feature permission  [e] startup permission  [c] freshness  [q] quit");
        io::stdout().flush()?;
        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            return Ok(());
        }
        message.clear();
        match input.trim() {
            "h" => state.halt(),
            "l" => {
                state.halt_local();
            }
            "p" => state.associate_local(),
            "u" => {
                if let Some(local) = &state.local_halt {
                    state.resume_local(local.token);
                }
            }
            "r" => {
                state.resume(state.generation);
            }
            "s" => {
                message = format!(
                    "stale resume accepted: {}",
                    state.resume(state.generation.saturating_sub(1))
                )
            }
            "f" => state.feature_allowed = !state.feature_allowed,
            "e" => state.startup_allowed = !state.startup_allowed,
            "c" => state.control_fresh = !state.control_fresh,
            "q" => return Ok(()),
            _ => message = "Enter one of the keys above.".into(),
        }
    }
}

async fn new_pool(url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_millis(200))
        .connect(url)
        .await
}

async fn database_demo() -> SpikeResult<()> {
    let url = "postgres://postgres:spike@127.0.0.1:15527/awa_runtime_control_spike";
    let admin_pool = new_pool(url, 3).await?;
    migrations::run(&admin_pool).await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS public.awa_spike_control (singleton BOOLEAN PRIMARY KEY CHECK (singleton), generation BIGINT NOT NULL, halted BOOLEAN NOT NULL)")
        .execute(&admin_pool).await?;
    sqlx::query("INSERT INTO public.awa_spike_control VALUES (TRUE, 0, FALSE) ON CONFLICT (singleton) DO UPDATE SET generation = 0, halted = FALSE")
        .execute(&admin_pool).await?;

    let runtime_pool = new_pool(url, 12).await?;
    let client = Arc::new(
        Client::builder(runtime_pool.clone())
            .queue(
                "spike",
                QueueConfig {
                    poll_interval: Duration::from_millis(100),
                    ..Default::default()
                },
            )
            .build()?,
    );
    client.start().await?;
    admin::pause_queue(&admin_pool, "spike", Some("prototype")).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let health = client.health_check().await;
    report(
        "queue_pause",
        json!({"maintenance_alive": health.maintenance_alive, "leader": health.leader, "heartbeat_alive": health.heartbeat_alive}),
    );

    let mut blocker = admin_pool.begin().await?;
    sqlx::query("LOCK TABLE awa.runtime_instances IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;
    let shutdown_client = client.clone();
    let started = Instant::now();
    let mut shutdown = tokio::spawn(async move {
        shutdown_client.shutdown(Duration::from_millis(50)).await;
    });
    let returned_within_250ms = tokio::time::timeout(Duration::from_millis(250), &mut shutdown)
        .await
        .is_ok();
    report(
        "shutdown_with_snapshot_lock",
        json!({"requested_timeout_ms": 50, "returned_within_250ms": returned_within_250ms, "elapsed_ms_before_unlock": started.elapsed().as_millis()}),
    );
    blocker.rollback().await?;
    tokio::time::timeout(Duration::from_secs(5), shutdown).await??;
    runtime_pool.close().await;

    let work_pool = new_pool(url, 1).await?;
    let held_connection = work_pool.acquire().await?;
    let same_pool_read = tokio::time::timeout(
        Duration::from_millis(50),
        sqlx::query_scalar::<_, bool>("SELECT halted FROM public.awa_spike_control")
            .fetch_one(&work_pool),
    )
    .await;
    let control_pool = new_pool(url, 1).await?;
    let first_generation: i64 = sqlx::query_scalar("UPDATE public.awa_spike_control SET generation = generation + 1, halted = TRUE RETURNING generation").fetch_one(&control_pool).await?;
    report(
        "separate_control_connection",
        json!({"shared_pool_read_timed_out": same_pool_read.is_err(), "halt_generation": first_generation}),
    );
    drop(held_connection);
    work_pool.close().await;
    control_pool.close().await;

    let restarted_observer = new_pool(url, 1).await?;
    let durable: (i64, bool) =
        sqlx::query_as("SELECT generation, halted FROM public.awa_spike_control")
            .fetch_one(&restarted_observer)
            .await?;
    report(
        "reconnect_without_notify",
        json!({"generation": durable.0, "halted": durable.1}),
    );
    let current_generation: i64 = sqlx::query_scalar("UPDATE public.awa_spike_control SET generation = generation + 1, halted = TRUE RETURNING generation").fetch_one(&restarted_observer).await?;
    let stale_resume =
        sqlx::query("UPDATE public.awa_spike_control SET halted = FALSE, generation = generation + 1 WHERE generation = $1")
            .bind(first_generation)
            .execute(&restarted_observer)
            .await?;
    report(
        "database_stale_resume",
        json!({"old_generation": first_generation, "current_generation": current_generation, "affected_rows": stale_resume.rows_affected()}),
    );
    sqlx::query("UPDATE public.awa_spike_control SET halted = FALSE, generation = generation + 1 WHERE generation = $1")
        .bind(current_generation)
        .execute(&restarted_observer)
        .await?;
    let queue_still_paused: bool =
        sqlx::query_scalar("SELECT paused FROM awa.queue_meta WHERE queue = 'spike'")
            .fetch_one(&restarted_observer)
            .await?;
    report(
        "global_resume_preserves_queue_pause",
        json!({"queue_paused": queue_still_paused}),
    );
    restarted_observer.close().await;

    let query_pool = new_pool(url, 1).await?;
    let mut query_connection = query_pool.acquire().await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *query_connection)
        .await?;
    let query_task = tokio::spawn(async move {
        sqlx::query("SELECT pg_sleep(10)")
            .execute(&mut *query_connection)
            .await
    });
    for _ in 0..100 {
        let active: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active' AND query LIKE '%pg_sleep(10)%')").bind(pid).fetch_one(&admin_pool).await?;
        if active {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    query_task.abort();
    let _ = query_task.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let sql_still_running: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active' AND query LIKE '%pg_sleep(10)%')").bind(pid).fetch_one(&admin_pool).await?;
    report(
        "abort_rust_query_task",
        json!({"postgres_query_still_running_after_50ms": sql_still_running}),
    );
    if sql_still_running {
        sqlx::query("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .execute(&admin_pool)
            .await?;
    }
    query_pool.close().await;
    admin_pool.close().await;
    Ok(())
}

#[tokio::main]
async fn main() -> SpikeResult<()> {
    match std::env::args().nth(1).as_deref() {
        Some("--interactive") => interactive(),
        Some("--database") => database_demo().await,
        _ => {
            model_demo();
            Ok(())
        }
    }
}
